/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.regex.Pattern;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.scanner.ScanResult;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * ClassLoader for a single JAR file.
 * 
 * @since GemFire 7.0
 */
public class DeployedJar {

  private static final Logger logger = LogService.getLogger();
  private static final MessageDigest messageDigest = getMessageDigest();
  private static final byte[] ZERO_BYTES = new byte[0];
  private static final Pattern PATTERN_SLASH = Pattern.compile("/");

  private final String jarName;
  private final File file;
  private final byte[] md5hash;
  private final Collection<Function> registeredFunctions = new ArrayList<>();

  private static MessageDigest getMessageDigest() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException ignored) {
      // Failure just means we can't do a simple compare for content equality
    }
    return null;
  }

  public File getFile() {
    return this.file;
  }

  public int getVersion() {
    return JarDeployer.extractVersionFromFilename(this.file.getName());
  }

  public DeployedJar(File versionedJarFile, String jarName) throws IOException {
    this(versionedJarFile, jarName, Files.readAllBytes(versionedJarFile.toPath()));
  }

  /**
   * Writes the given jarBytes to versionedJarFile
   */
  public DeployedJar(File versionedJarFile, final String jarName, byte[] jarBytes)
      throws FileNotFoundException {
    Assert.assertTrue(jarBytes != null, "jarBytes cannot be null");
    Assert.assertTrue(jarName != null, "jarName cannot be null");
    Assert.assertTrue(versionedJarFile != null, "versionedJarFile cannot be null");

    this.file = versionedJarFile;
    this.jarName = jarName;

    final byte[] fileContent = getJarContent();
    if (!Arrays.equals(fileContent, jarBytes)) {
      throw new IllegalStateException("JAR file: " + versionedJarFile.getAbsolutePath()
          + ", does not have the expected content.");
    }

    if (!hasValidJarContent(fileContent)) {
      throw new IllegalArgumentException(
          "File does not contain valid JAR content: " + versionedJarFile.getAbsolutePath());
    }

    if (messageDigest != null) {
      this.md5hash = messageDigest.digest(jarBytes);
    } else {
      this.md5hash = null;
    }
  }

  /**
   * Peek into the JAR data and make sure that it is valid JAR content.
   * 
   * @param inputStream InputStream containing data to be validated.
   * @return True if the data has JAR content, false otherwise
   */
  private static boolean hasValidJarContent(final InputStream inputStream) {
    JarInputStream jarInputStream = null;
    boolean valid = false;

    try {
      jarInputStream = new JarInputStream(inputStream);
      valid = jarInputStream.getNextJarEntry() != null;
    } catch (IOException ignore) {
      // Ignore this exception and just return false
    } finally {
      try {
        jarInputStream.close();
      } catch (IOException ignored) {
        // Ignore this exception and just return result
      }
    }

    return valid;
  }

  /**
   * Peek into the JAR data and make sure that it is valid JAR content.
   * 
   * @param jarBytes Bytes of data to be validated.
   * @return True if the data has JAR content, false otherwise
   */
  static boolean hasValidJarContent(final byte[] jarBytes) {
    return hasValidJarContent(new ByteArrayInputStream(jarBytes));
  }

  /**
   * Scan the JAR file and attempt to load all classes and register any function classes found.
   */
  // This method will process the contents of the JAR file as stored in this.jarByteContent
  // instead of reading from the original JAR file. This is done because we can't open up
  // the original file and then close it without releasing the shared lock that was obtained
  // in the constructor. Once this method is finished, all classes will have been loaded and
  // there will no longer be a need to hang on to the original contents so they will be
  // discarded.
  synchronized void loadClassesAndRegisterFunctions() throws ClassNotFoundException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Registering functions with DeployedJar: {}", this);
    }

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(this.getJarContent());

    JarInputStream jarInputStream = null;
    try {
      List<String> functionClasses = findFunctionsInThisJar();

      jarInputStream = new JarInputStream(byteArrayInputStream);
      JarEntry jarEntry = jarInputStream.getNextJarEntry();

      while (jarEntry != null) {
        if (jarEntry.getName().endsWith(".class")) {
          final String className = PATTERN_SLASH.matcher(jarEntry.getName()).replaceAll("\\.")
              .substring(0, jarEntry.getName().length() - 6);

          if (functionClasses.contains(className)) {
            if (isDebugEnabled) {
              logger.debug("Attempting to load class: {}, from JAR file: {}", jarEntry.getName(),
                  this.file.getAbsolutePath());
            }
            try {
              Class<?> clazz = ClassPathLoader.getLatest().forName(className);
              Collection<Function> registerableFunctions = getRegisterableFunctionsFromClass(clazz);
              for (Function function : registerableFunctions) {
                FunctionService.registerFunction(function);
                if (isDebugEnabled) {
                  logger.debug("Registering function class: {}, from JAR file: {}", className,
                      this.file.getAbsolutePath());
                }
                this.registeredFunctions.add(function);
              }
            } catch (ClassNotFoundException | NoClassDefFoundError cnfex) {
              logger.error("Unable to load all classes from JAR file: {}",
                  this.file.getAbsolutePath(), cnfex);
              throw cnfex;
            }
          } else {
            if (isDebugEnabled) {
              logger.debug("No functions found in class: {}, from JAR file: {}", jarEntry.getName(),
                  this.file.getAbsolutePath());
            }
          }
        }
        jarEntry = jarInputStream.getNextJarEntry();
      }
    } catch (IOException ioex) {
      logger.error("Exception when trying to read class from ByteArrayInputStream", ioex);
    } finally {
      if (jarInputStream != null) {
        try {
          jarInputStream.close();
        } catch (IOException ioex) {
          logger.error("Exception attempting to close JAR input stream", ioex);
        }
      }
    }
  }

  synchronized void cleanUp() {
    for (Function function : this.registeredFunctions) {
      FunctionService.unregisterFunction(function.getId());
    }
    this.registeredFunctions.clear();

    try {
      TypeRegistry typeRegistry = ((InternalCache) CacheFactory.getAnyInstance()).getPdxRegistry();
      if (typeRegistry != null) {
        typeRegistry.flushCache();
      }
    } catch (CacheClosedException ignored) {
      // That's okay, it just means there was nothing to flush to begin with
    }
  }

  /**
   * Uses MD5 hashes to determine if the original byte content of this DeployedJar is the same as
   * that past in.
   * 
   * @param compareToBytes Bytes to compare the original content to
   * @return True of the MD5 hash is the same o
   */
  boolean hasSameContentAs(final byte[] compareToBytes) {
    // If the MD5 hash can't be calculated then silently return no match
    if (messageDigest == null || this.md5hash == null) {
      return Arrays.equals(compareToBytes, getJarContent());
    }

    byte[] compareToMd5 = messageDigest.digest(compareToBytes);
    if (logger.isDebugEnabled()) {
      logger.debug("For JAR file: {}, Comparing MD5 hash {} to {}", this.file.getAbsolutePath(),
          new String(this.md5hash), new String(compareToMd5));
    }
    return Arrays.equals(this.md5hash, compareToMd5);
  }

  /**
   * Check to see if the class implements the Function interface. If so, it will be registered with
   * FunctionService. Also, if the functions's class was originally declared in a cache.xml file
   * then any properties specified at that time will be reused when re-registering the function.
   * 
   * @param clazz Class to check for implementation of the Function class
   * @return A collection of Objects that implement the Function interface.
   */
  private Collection<Function> getRegisterableFunctionsFromClass(Class<?> clazz) {
    final List<Function> registerableFunctions = new ArrayList<>();

    try {
      if (Function.class.isAssignableFrom(clazz) && !Modifier.isAbstract(clazz.getModifiers())) {
        boolean registerUninitializedFunction = true;
        if (Declarable.class.isAssignableFrom(clazz)) {
          try {
            final List<Properties> propertiesList = ((InternalCache) CacheFactory.getAnyInstance())
                .getDeclarableProperties(clazz.getName());

            if (!propertiesList.isEmpty()) {
              registerUninitializedFunction = false;
              // It's possible that the same function was declared multiple times in cache.xml
              // with different properties. So, register the function using each set of
              // properties.
              for (Properties properties : propertiesList) {
                @SuppressWarnings("unchecked")
                Function function = newFunction((Class<Function>) clazz, true);
                if (function != null) {
                  ((Declarable) function).init(properties);
                  if (function.getId() != null) {
                    registerableFunctions.add(function);
                  }
                }
              }
            }
          } catch (CacheClosedException ignored) {
            // That's okay, it just means there were no properties to init the function with
          }
        }

        if (registerUninitializedFunction) {
          @SuppressWarnings("unchecked")
          Function function = newFunction((Class<Function>) clazz, false);
          if (function != null && function.getId() != null) {
            registerableFunctions.add(function);
          }
        }
      }
    } catch (Exception ex) {
      logger.error("Attempting to register function from JAR file: {}", this.file.getAbsolutePath(),
          ex);
    }

    return registerableFunctions;
  }

  private List<String> findFunctionsInThisJar() throws IOException {
    URLClassLoader urlClassLoader =
        new URLClassLoader(new URL[] {this.getFile().getCanonicalFile().toURL()});
    FastClasspathScanner fastClasspathScanner = new FastClasspathScanner()
        .removeTemporaryFilesAfterScan(true).overrideClassLoaders(urlClassLoader);
    ScanResult scanResult = fastClasspathScanner.scan();
    return scanResult.getNamesOfClassesImplementing(Function.class);
  }

  private Function newFunction(final Class<Function> clazz, final boolean errorOnNoSuchMethod) {
    try {
      final Constructor<Function> constructor = clazz.getConstructor();
      return constructor.newInstance();
    } catch (NoSuchMethodException nsmex) {
      if (errorOnNoSuchMethod) {
        logger.error("Zero-arg constructor is required, but not found for class: {}",
            clazz.getName(), nsmex);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Not registering function because it doesn't have a zero-arg constructor: {}",
              clazz.getName());
        }
      }
    } catch (Exception ex) {
      logger.error("Error when attempting constructor for function for class: {}", clazz.getName(),
          ex);
    }

    return null;
  }

  private byte[] getJarContent() {
    try {
      InputStream channelInputStream = new FileInputStream(this.file);

      final ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
      final byte[] bytes = new byte[4096];

      int bytesRead;
      while ((bytesRead = channelInputStream.read(bytes)) != -1) {
        byteOutStream.write(bytes, 0, bytesRead);
      }
      channelInputStream.close();
      return byteOutStream.toByteArray();
    } catch (IOException e) {
      logger.error("Error when attempting to read jar contents: ", e);
    }

    return ZERO_BYTES;
  }

  /**
   * @return the unversioned name of this jar file, e.g. myJar.jar
   */
  public String getJarName() {
    return this.jarName;
  }

  public String getFileName() {
    return this.file.getName();
  }

  public String getFileCanonicalPath() throws IOException {
    return this.file.getCanonicalPath();
  }

  public URL getFileURL() {
    try {
      return this.file.toURL();
    } catch (MalformedURLException e) {
      logger.warn(e);
    }
    return null;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (this.jarName == null ? 0 : this.jarName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DeployedJar other = (DeployedJar) obj;
    if (this.jarName == null) {
      if (other.jarName != null) {
        return false;
      }
    } else if (!this.jarName.equals(other.jarName)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append('@').append(System.identityHashCode(this)).append('{');
    sb.append("jarName=").append(this.jarName);
    sb.append(",file=").append(this.file.getAbsolutePath());
    sb.append(",md5hash=").append(Arrays.toString(this.md5hash));
    sb.append(",version=").append(this.getVersion());
    sb.append('}');
    return sb.toString();
  }
}
