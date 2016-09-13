/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * ClassLoader for a single JAR file.
 * 
 * @since GemFire 7.0
 */
public class JarClassLoader extends ClassLoader {
  private final static Logger logger = LogService.getLogger();
  private final static MessageDigest messageDigest;

  private final String jarName;
  private final File file;
  private final byte[] md5hash;
  private FileLock fileLock;
  private final Collection<Function> registeredFunctions = new ArrayList<Function>();

  private final ThreadLocal<Boolean> alreadyScanned = new ThreadLocal<Boolean>();

  // Lock used by ChannelInputStream (inner class) to prevent multiple threads from
  // trying to use the channel simultaneously.
  static final ReentrantLock channelLock = new ReentrantLock();

  private byte[] jarByteContent;

  static {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException nsaex) {
      // Failure just means we can't do a simple compare for content equality
    }
    messageDigest = md;
  }

  public JarClassLoader(final File file, final String jarName, byte[] jarBytes) throws IOException {
    Assert.assertTrue(file != null, "file cannot be null");
    Assert.assertTrue(jarName != null, "jarName cannot be null");
    
    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      @SuppressWarnings("resource")
      FileInputStream fileInputStream = new FileInputStream(file);
      this.fileLock = fileInputStream.getChannel().lock(0, file.length(), true);

      if (isDebugEnabled) {
        logger.debug("Acquired shared file lock w/ channel: {}, for JAR: {}", this.fileLock.channel(), file.getAbsolutePath());
      }

      if (file.length() == 0) {
        throw new FileNotFoundException("JAR file was truncated prior to obtaining a lock: " + jarName);
      }

      final byte[] fileContent = getJarContent();
      if (!Arrays.equals(fileContent, jarBytes)) {
        throw new FileNotFoundException("JAR file: " + file.getAbsolutePath() + ", was modified prior to obtaining a lock: "
            + jarName);
      }
        
      if (!isValidJarContent(jarBytes)) {
        if (this.fileLock != null) {
          this.fileLock.release();
          this.fileLock.channel().close();
          if (isDebugEnabled) {
            logger.debug("Prematurely releasing shared file lock due to bad content for JAR file: {}, w/ channel: {}", file.getAbsolutePath(), this.fileLock.channel());
          }
        }
        throw new IllegalArgumentException("File does not contain valid JAR content: " + file.getAbsolutePath());
      }
      
      Assert.assertTrue(jarBytes != null, "jarBytes cannot be null");

      // Temporarily save the contents of the JAR file until they can be processed by the
      // loadClassesandRegisterFunctions() method.
      this.jarByteContent = jarBytes;

      if (messageDigest != null) {
        this.md5hash = messageDigest.digest(this.jarByteContent);
      } else {
        this.md5hash = null;
      }

      this.file = file;
      this.jarName = jarName;

    } catch (FileNotFoundException fnfex) {
      if (this.fileLock != null) {
        this.fileLock.release();
        this.fileLock.channel().close();
        if (isDebugEnabled) {
          logger.debug("Prematurely releasing shared file lock due to file not found for JAR file: {}, w/ channel: {}", file.getAbsolutePath(), this.fileLock.channel());
        }
      }
      throw fnfex;
    }
  }

  /**
   * Peek into the JAR data and make sure that it is valid JAR content.
   * 
   * @param inputStream
   *          InputStream containing data to be validated.
   * 
   * @return True if the data has JAR content, false otherwise
   */
  private static boolean hasValidJarContent(final InputStream inputStream) {
    JarInputStream jarInputStream = null;
    boolean valid = false;

    try {
      jarInputStream = new JarInputStream(inputStream);
      valid = (jarInputStream.getNextJarEntry() != null);
    } catch (IOException ignore) {
      // Ignore this exception and just return false
    } finally {
      try {
        jarInputStream.close();
      } catch (IOException ioex) {
        // Ignore this exception and just return result
      }
    }

    return valid;
  }
  
  /**
   * Peek into the JAR data and make sure that it is valid JAR content.
   * 
   * @param jarBytes
   *          Bytes of data to be validated.
   * 
   * @return True if the data has JAR content, false otherwise
   */
  public static boolean isValidJarContent(final byte[] jarBytes) {
    return hasValidJarContent(new ByteArrayInputStream(jarBytes));
  }
  
  /**
   * Peek into the JAR data and make sure that it is valid JAR content.
   * 
   * @param jarFile
   *          File whose contents should be validated.
   * 
   * @return True if the data has JAR content, false otherwise
   */
  public static boolean hasValidJarContent(final File jarFile) {
    try {
    return hasValidJarContent(new FileInputStream(jarFile));
    } catch (IOException ioex) {
      return false;
    }
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
  public synchronized void loadClassesAndRegisterFunctions() throws ClassNotFoundException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Registering functions with JarClassLoader: {}", this);
    }

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(this.jarByteContent);

    JarInputStream jarInputStream = null;
    try {
      jarInputStream = new JarInputStream(byteArrayInputStream);
      JarEntry jarEntry = jarInputStream.getNextJarEntry();

      while (jarEntry != null) {
        if (jarEntry.getName().endsWith(".class")) {
          if (isDebugEnabled) {
            logger.debug("Attempting to load class: {}, from JAR file: {}", jarEntry.getName(), this.file.getAbsolutePath());
          }

          final String className = jarEntry.getName().replaceAll("/", "\\.").substring(0, (jarEntry.getName().length() - 6));
          try {
            Class<?> clazz = loadClass(className, true, false);
            Collection<Function> registerableFunctions = getRegisterableFunctionsFromClass(clazz);
            for (Function function : registerableFunctions) {
              FunctionService.registerFunction(function);
              if (isDebugEnabled) {
                logger.debug("Registering function class: {}, from JAR file: {}", className, this.file.getAbsolutePath());
              }
              this.registeredFunctions.add(function);
            }
          } catch (ClassNotFoundException cnfex) {
            logger.error("Unable to load all classes from JAR file: {}", this.file.getAbsolutePath(), cnfex);
            throw cnfex;
          } catch (NoClassDefFoundError ncdfex) {
            logger.error("Unable to load all classes from JAR file: {}", this.file.getAbsolutePath(), ncdfex);
            throw ncdfex;
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
    this.jarByteContent = new byte[0];
  }

  synchronized void cleanUp() {
    for (Function function : this.registeredFunctions) {
      FunctionService.unregisterFunction(function.getId());
    }
    this.registeredFunctions.clear();

    try {
      TypeRegistry typeRegistry = ((GemFireCacheImpl) CacheFactory.getAnyInstance()).getPdxRegistry();
      if (typeRegistry != null) {
        typeRegistry.flushCache();
      }
    } catch (CacheClosedException ccex) {
      // That's okay, it just means there was nothing to flush to begin with
    }

    try {
      this.fileLock.release();
      this.fileLock.channel().close();
      if (logger.isDebugEnabled()) {
        logger.debug("Released shared file lock on JAR file: {}, w/ channel: {}", this.file.getAbsolutePath(), this.fileLock.channel());
      }
    } catch (IOException ioex) {
      logger.error("Could not release the shared lock for JAR file: {}", this.file.getAbsolutePath(), ioex);
    }
  }

  /**
   * Uses MD5 hashes to determine if the original byte content of this JarClassLoader is the same as that past in.
   * 
   * @param compareToBytes
   *          Bytes to compare the original content to
   * @return True of the MD5 hash is the same o
   */
  public boolean hasSameContent(final byte[] compareToBytes) {
    // If the MD5 hash can't be calculated then silently return no match
    if (messageDigest == null || this.md5hash == null) {
      return false;
    }

    byte[] compareToMd5 = messageDigest.digest(compareToBytes);
    if (logger.isDebugEnabled()) {
      logger.debug("For JAR file: {}, Comparing MD5 hash {} to {}", this.file.getAbsolutePath(), new String(this.md5hash), new String(compareToMd5));
    }
    return Arrays.equals(this.md5hash, compareToMd5);
  }

  private boolean alreadyScanned() {
    if (this.alreadyScanned.get() == null) {
      this.alreadyScanned.set(Boolean.FALSE);
    }
    return this.alreadyScanned.get();
  }

  @Override
  protected URL findResource(String resourceName) {
    URL returnURL = null;
    JarInputStream jarInputStream = null;
    
    try {
      ChannelInputStream channelInputStream = new ChannelInputStream(this.fileLock.channel());
      jarInputStream = new JarInputStream(channelInputStream);

      JarEntry jarEntry = jarInputStream.getNextJarEntry();
      while (jarEntry != null && !jarEntry.getName().equals(resourceName)) {
        jarEntry = jarInputStream.getNextJarEntry();
      }
      if (jarEntry != null) {
        try {
          returnURL = new URL("jar", "", this.file.toURI().toURL() + "!/" + jarEntry.getName());
        } catch (MalformedURLException muex) {
          logger.error("Could not create resource URL from file URL", muex);
        }
      }
    } catch (IOException ioex) {
      logger.error("Exception when trying to read class from ByteArrayInputStream", ioex);
    } finally {
      if (jarInputStream != null) {
        try {
          jarInputStream.close();
        } catch (IOException ioex) {
          logger.error("Unable to close JAR input stream when finding resource", ioex);
        }
      }
    }

    return returnURL;
  }

  @Override
  protected Enumeration<URL> findResources(final String resourceName) {
    return new Enumeration<URL>() {
      private URL element = findResource(resourceName);

      @Override
      public boolean hasMoreElements() {
        return this.element != null;
      }

      @Override
      public URL nextElement() {
        if (this.element != null) {
          URL element = this.element;
          this.element = null;
          return element;
        }
        throw new NoSuchElementException();
      }
    };
  }

  @Override
  public Class<?> loadClass(final String className) throws ClassNotFoundException {
    return (loadClass(className, true));
  }

  @Override
  public Class<?> loadClass(final String className, final boolean resolveIt) throws ClassNotFoundException {
    return loadClass(className, resolveIt, true);
  }

  Class<?> loadClass(final String className, final boolean resolveIt, final boolean useClassPathLoader)
      throws ClassNotFoundException {
    Class<?> clazz = findLoadedClass(className);
    if (clazz != null) {
      return clazz;
    }

    try {
      clazz = findClass(className);
      if (resolveIt) {
        resolveClass(clazz);
      }
    } catch (ClassNotFoundException cnfex) {
      if (!useClassPathLoader) {
        throw cnfex;
      }
    }

    if (clazz == null) {
      try {
        this.alreadyScanned.set(true);
        return forName(className, ClassPathLoader.getLatest().getClassLoaders());
      } finally {
        this.alreadyScanned.set(false);
      }
    }

    return clazz;
  }

  // When loadClassesAndRegisterFunctions() is called and it starts to load classes, this method
  // may be called multiple times to resolve dependencies on other classes in the same or
  // another JAR file. During this stage, this.jarByteContent will contain the complete contents of
  // the JAR file and will be used when attempting to resolve these dependencies. Once
  // loadClassesAndRegisterFunctions() is complete it discards the data in this.jarByteContent.
  // However, at that point all of the classes available in the JAR file will already have been
  // loaded. Future calls to loadClass(...) will return the cached Class object for any
  // classes available in this JAR and findClass(...) will no longer be needed to find them.
  @Override
  protected Class<?> findClass(String className) throws ClassNotFoundException {
    String formattedClassName = className.replaceAll("\\.", "/") + ".class";

    JarInputStream jarInputStream = null;
    if (this.jarByteContent.length == 0) {
      throw new ClassNotFoundException(className);
    }

    try {
      jarInputStream = new JarInputStream(new ByteArrayInputStream(this.jarByteContent));
      JarEntry jarEntry = jarInputStream.getNextJarEntry();

      while (jarEntry != null && !jarEntry.getName().equals(formattedClassName)) {
        jarEntry = jarInputStream.getNextJarEntry();
      }

      if (jarEntry != null) {
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream(buffer.length);

        int bytesRead = -1;
        while ((bytesRead = jarInputStream.read(buffer)) != -1) {
          byteOutStream.write(buffer, 0, bytesRead);
        }

        // Add the package first if it doesn't already exist
        int lastDotIndex = className.lastIndexOf('.');
        if (lastDotIndex != -1) {
          String pkgName = className.substring(0, lastDotIndex);
          Package pkg = getPackage(pkgName);
          if (pkg == null) {
            definePackage(pkgName, null, null, null, null, null, null, null);
          }
        }

        byte[] classBytes = byteOutStream.toByteArray();

        synchronized (this.file) {
          Class<?> clazz = findLoadedClass(className);
          if (clazz == null) {
            clazz = defineClass(className, classBytes, 0, classBytes.length, null);
          }
          return clazz;
        }
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

    throw new ClassNotFoundException(className);
  }

  /**
   * Check to see if the class implements the Function interface. If so, it will be registered with FunctionService.
   * Also, if the functions's class was originally declared in a cache.xml file then any properties specified at that
   * time will be reused when re-registering the function.
   * 
   * @param clazz
   *          Class to check for implementation of the Function class
   * @return A collection of Objects that implement the Function interface.
   */
  private Collection<Function> getRegisterableFunctionsFromClass(Class<?> clazz) {
    final List<Function> registerableFunctions = new ArrayList<Function>();

    try {
      if (Function.class.isAssignableFrom(clazz) && !Modifier.isAbstract(clazz.getModifiers())) {
        boolean registerUninitializedFunction = true;
        if (Declarable.class.isAssignableFrom(clazz)) {
          try {
            final List<Properties> propertiesList = ((GemFireCacheImpl) CacheFactory.getAnyInstance())
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
          } catch (CacheClosedException ccex) {
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
      logger.error("Attempting to register function from JAR file: " + this.file.getAbsolutePath(), ex);
    }

    return registerableFunctions;
  }

  private Class<?> forName(final String className, final Collection<ClassLoader> classLoaders) throws ClassNotFoundException {
    Class<?> clazz = null;

    for (ClassLoader classLoader : classLoaders) {
      try {
        if (classLoader instanceof JarClassLoader) {
          if (!((JarClassLoader) classLoader).alreadyScanned()) {
            clazz = ((JarClassLoader) classLoader).loadClass(className, true, false);
          }
        } else {
          clazz = Class.forName(className, true, classLoader);
        }
        if (clazz != null) {
          return clazz;
        }
      } catch (SecurityException sex) {
        // Continue to next ClassLoader
      } catch (ClassNotFoundException cnfex) {
        // Continue to next ClassLoader
      }
    }
    throw new ClassNotFoundException(className);
  }

  private Function newFunction(final Class<Function> clazz, final boolean errorOnNoSuchMethod) {
    try {
      final Constructor<Function> constructor = clazz.getConstructor();
      return constructor.newInstance();
    } catch (NoSuchMethodException nsmex) {
      if (errorOnNoSuchMethod) {
        logger.error("Zero-arg constructor is required, but not found for class: {}", clazz.getName(), nsmex);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Not registering function because it doesn't have a zero-arg constructor: {}", clazz.getName());
        }
      }
    } catch (SecurityException sex) {
      logger.error("Zero-arg constructor of function not accessible for class: {}", clazz.getName(), sex);
    } catch (IllegalAccessException iae) {
      logger.error("Zero-arg constructor of function not accessible for class: {}", clazz.getName(), iae);
    } catch (InvocationTargetException ite) {
      logger.error("Error when attempting constructor for function for class: {}", clazz.getName(), ite);
    } catch (InstantiationException ie) {
      logger.error("Unable to instantiate function for class: {}", clazz.getName(), ie);
    } catch (ExceptionInInitializerError eiiex) {
      logger.error("Error during function initialization for class: {}", clazz.getName(), eiiex);
    }
    return null;
  }

  private byte[] getJarContent() throws IOException {
    ChannelInputStream channelInputStream = new ChannelInputStream(this.fileLock.channel());
    final ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
    final byte[] bytes = new byte[4096];

    int bytesRead;
    while (((bytesRead = channelInputStream.read(bytes)) != -1)) {
      byteOutStream.write(bytes, 0, bytesRead);
    }
    channelInputStream.close();
    return byteOutStream.toByteArray();
  }

  public String getJarName() {
    return this.jarName;
  }

  public String getFileName() {
    return this.file.getName();
  }

  public String getFileCanonicalPath() throws IOException {
    return this.file.getCanonicalPath();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.jarName == null) ? 0 : this.jarName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    JarClassLoader other = (JarClassLoader) obj;
    if (this.jarName == null) {
      if (other.jarName != null)
        return false;
    } else if (!this.jarName.equals(other.jarName))
      return false;
    return true;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("jarName=").append(this.jarName);
    sb.append(",file=").append(this.file.getAbsolutePath());
    sb.append(",md5hash=").append(Arrays.toString(this.md5hash));
    sb.append(",fileLock=").append(this.fileLock);
    sb.append("}");
    return sb.toString();
  }

  /**
   * When a lock is acquired it is done so through an open file (FileInputStream, etc.). If for any reason that same
   * file is used to open another input stream, when the second input stream is closed the file lock will not be held
   * (although an OverlappingFileLock exception will be thrown if an attempt is made to acquire the lock again). To get
   * around this problem, this class is used to wrap the original file channel used by the lock with an InputStream.
   * When this class is instantiated a lock is obtained to prevent other threads from attempting to use the file channel
   * at the same time. The file channel can then be read as with any other InputStream. When the input stream is closed,
   * instead of closing the file channel, the lock is released instead.
   * 
   * This class is thread safe. However, multiple instances cannot be created by the same thread. The reason for this is
   * that the lock will be obtained in all cases (it's reentrant), and then the channel position will be modified by
   * both instances causing arbitrary values being returned from the read() method.
   * 
   * @since GemFire 7.0
   */
  private class ChannelInputStream extends InputStream {
    private final FileChannel fileChannel;
    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1);

    ChannelInputStream(final FileChannel fileChannel) throws IOException {
      channelLock.lock();
      this.fileChannel = fileChannel;
      this.fileChannel.position(0);
    }

    @Override
    public int read() throws IOException {
      this.byteBuffer.rewind();
      if (this.fileChannel.read(this.byteBuffer) <= 0) {
        return -1;
      }
      this.byteBuffer.rewind();
      return (this.byteBuffer.get() & 255);
    }

    @Override
    public void close() {
      channelLock.unlock();
    }
  }
}
