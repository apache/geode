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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.deployment.FunctionScanner;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * ClassLoader for a single JAR file.
 *
 * @since GemFire 7.0
 */
public class DeployedJar {

  private static final Logger logger = LogService.getLogger();
  @MakeNotStatic("This object gets updated in the production code")
  private static final MessageDigest messageDigest = getMessageDigest();
  private static final Pattern PATTERN_SLASH = Pattern.compile("/");

  private final String artifactId;
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

  /**
   * Writes the given jarBytes to versionedJarFile
   */
  public DeployedJar(File versionedJarFile) {
    String artifactId = JarDeployer.toArtifactId(versionedJarFile.getName());

    this.file = versionedJarFile;
    this.artifactId = artifactId;

    if (!hasValidJarContent(versionedJarFile)) {
      throw new IllegalArgumentException(
          "File does not contain valid JAR content: " + versionedJarFile.getAbsolutePath());
    }

    byte[] digest = null;
    try {
      if (messageDigest != null) {
        digest = fileDigest(this.file);
      }
    } catch (IOException e) {
      // Ignored
    }
    this.md5hash = digest;
  }

  /**
   * Peek into the JAR data and make sure that it is valid JAR content.
   *
   * @param jarFile Jar containing data to be validated.
   * @return True if the data has JAR content, false otherwise
   */
  public static boolean hasValidJarContent(File jarFile) {
    JarInputStream jarInputStream = null;
    boolean valid = false;

    try {
      jarInputStream = new JarInputStream(new FileInputStream(jarFile));
      valid = jarInputStream.getNextJarEntry() != null;
    } catch (IOException ignore) {
      // Ignore this exception and just return false
    } finally {
      if (jarInputStream != null) {
        try {
          jarInputStream.close();
        } catch (IOException e) {
          // Ignore
        }
      }

    }

    return valid;
  }

  /**
   * Scan the JAR file and attempt to register any function classes found.
   */

  public synchronized void registerFunctions() throws ClassNotFoundException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Registering functions with DeployedJar: {}", this);
    }

    BufferedInputStream bufferedInputStream;
    try {
      bufferedInputStream = new BufferedInputStream(new FileInputStream(this.file));
    } catch (Exception ex) {
      logger.error("Unable to scan jar file for functions");
      return;
    }

    JarInputStream jarInputStream = null;
    try {
      Collection<String> functionClasses = findFunctionsInThisJar();
      jarInputStream = new JarInputStream(bufferedInputStream);
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

  /**
   * Unregisters all functions from this jar if it was undeployed (i.e. newVersion == null), or all
   * functions not present in the new version if it was redeployed.
   *
   * @param newVersion The new version of this jar that was deployed, or null if this jar was
   *        undeployed.
   */
  protected synchronized void cleanUp(DeployedJar newVersion) {
    Stream<String> oldFunctions = this.registeredFunctions.stream().map(Function::getId);

    Stream<String> removedFunctions;
    if (newVersion == null) {
      removedFunctions = oldFunctions;
    } else {
      Predicate<String> isRemoved =
          (String oldFunctionId) -> !newVersion.hasFunctionWithId(oldFunctionId);

      removedFunctions = oldFunctions.filter(isRemoved);
    }

    removedFunctions.forEach(FunctionService::unregisterFunction);
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
   * @param stagedFile File to compare the original content to
   * @return True of the MD5 hash is the same o
   */
  boolean hasSameContentAs(final File stagedFile) {
    // If the MD5 hash can't be calculated then silently return no match
    if (messageDigest == null || this.md5hash == null) {
      return false;
    }

    byte[] compareToMd5;
    try {
      compareToMd5 = fileDigest(stagedFile);
    } catch (IOException ex) {
      return false;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("For JAR file: {}, Comparing MD5 hash {} to {}", this.file.getAbsolutePath(),
          new String(this.md5hash), new String(compareToMd5));
    }
    return Arrays.equals(this.md5hash, compareToMd5);
  }

  private byte[] fileDigest(File file) throws IOException {
    BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file));
    try {
      byte[] data = new byte[8192];
      int read;
      while ((read = fis.read(data)) > 0) {
        messageDigest.update(data, 0, read);
      }
    } finally {
      fis.close();
    }

    return messageDigest.digest();
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
            InternalCache cache = (InternalCache) CacheFactory.getAnyInstance();
            final List<Properties> propertiesList = cache.getDeclarableProperties(clazz.getName());

            if (!propertiesList.isEmpty()) {
              registerUninitializedFunction = false;
              // It's possible that the same function was declared multiple times in cache.xml
              // with different properties. So, register the function using each set of
              // properties.
              for (Properties properties : propertiesList) {
                @SuppressWarnings("unchecked")
                Function function = newFunction((Class<Function>) clazz, true);
                if (function != null) {
                  ((Declarable) function).initialize(cache, properties);
                  ((Declarable) function).init(properties); // for backwards compatibility
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

  protected Collection<String> findFunctionsInThisJar() throws IOException {
    return new FunctionScanner().findFunctionsInJar(this.file);
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

  /**
   * Get this jar's artifact ID, which is the part of the jar file name that precedes the version
   * information.
   *
   * @return the artifact ID for this jar
   */
  public String getArtifactId() {
    return this.artifactId;
  }

  /**
   * @return the filename as user deployed, i.e remove the sequence number
   */
  public String getDeployedFileName() {
    String fileBaseName = JarDeployer.getDeployedFileBaseName(this.file.getName());
    if (fileBaseName == null) {
      throw new IllegalStateException("file name needs to have a sequence number");
    }
    return fileBaseName + ".jar";
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

  private boolean hasFunctionWithId(String id) {
    if (CollectionUtils.isEmpty(this.registeredFunctions)) {
      return false;
    }

    return this.registeredFunctions.stream().map(Function::getId)
        .anyMatch(functionId -> functionId.equals(id));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (this.artifactId == null ? 0 : this.artifactId.hashCode());
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
    if (this.artifactId == null) {
      if (other.artifactId != null) {
        return false;
      }
    } else if (!this.artifactId.equals(other.artifactId)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append('@').append(System.identityHashCode(this)).append('{');
    sb.append("artifactId=").append(this.artifactId);
    sb.append(",file=").append(this.file.getAbsolutePath());
    sb.append(",md5hash=").append(toHex(this.md5hash));
    sb.append(",version=").append(this.getVersion());
    sb.append('}');
    return sb.toString();
  }

  private String toHex(byte[] data) {
    StringBuilder result = new StringBuilder();
    for (byte b : data) {
      result.append(String.format("%02x", b));
    }
    return result.toString();
  }
}
