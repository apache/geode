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

import static java.util.stream.Collectors.joining;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.CollectionUtils;

/**
 * The delegating <tt>ClassLoader</tt> used by GemFire to load classes and other resources. This
 * <tt>ClassLoader</tt> delegates to any <tt>ClassLoader</tt>s added to the list of custom class
 * loaders, thread context <tt>ClassLoader</tt> s unless they have been excluded}, the
 * <tt>ClassLoader</tt> which loaded the GemFire classes, and finally the system
 * <tt>ClassLoader</tt>.
 * <p>
 * The thread context class loaders can be excluded by setting the system property
 * <tt>gemfire.excludeThreadContextClassLoader</tt>:
 * <ul>
 * <li><tt>-Dgemfire.excludeThreadContextClassLoader=true</tt>
 * <li><tt>System.setProperty("gemfire.excludeThreadContextClassLoader", "true");
 * </tt>
 * </ul>
 * <p>
 * Class loading and resource loading order:
 * <ul>
 * <li>1. Any custom loaders in the order they were added
 * <li>2. <tt>Thread.currentThread().getContextClassLoader()</tt> unless excludeTCCL == true
 * <li>3. <tt>ClassPathLoader.class.getClassLoader()</tt>
 * <li>4. <tt>ClassLoader.getSystemClassLoader()</tt> If the attempt to acquire any of the above
 * class loaders results in either a {@link java.lang.SecurityException SecurityException} or a
 * null, then that class loader is quietly skipped. Duplicate class loaders will be skipped.
 * <p>
 * This class it not an extension of ClassLoader due to #43080. See also
 * http://docs.oracle.com/javase/specs/jvms/se7/html/jvms-5.html
 *
 * @since GemFire 6.5.1.4
 */
public class ClassPathLoader {
  private static final Logger logger = LogService.getLogger();

  static final String EXCLUDE_TCCL_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "excludeThreadContextClassLoader";

  @MakeNotStatic
  private static volatile ClassPathLoader latest;

  public final HashMap<String, DeployJarChildFirstClassLoader> latestJarNamesToClassLoader =
      new HashMap<>();

  private volatile DeployJarChildFirstClassLoader leafLoader;

  private final JarDeployer jarDeployer;

  private boolean excludeTCCL;

  public ClassPathLoader(boolean excludeTCCL) {
    this.excludeTCCL = excludeTCCL;
    this.jarDeployer = new JarDeployer();
    rebuildClassLoaderForDeployedJars();
  }

  public ClassPathLoader(boolean excludeTCCL, File workingDir) {
    this.excludeTCCL = excludeTCCL;
    this.jarDeployer = new JarDeployer(workingDir);
    rebuildClassLoaderForDeployedJars();
  }

  static ClassPathLoader setLatestToDefault() {
    latest = new ClassPathLoader(Boolean.getBoolean(EXCLUDE_TCCL_PROPERTY));
    return latest;
  }

  public static ClassPathLoader setLatestToDefault(File workingDir) {
    latest = new ClassPathLoader(Boolean.getBoolean(EXCLUDE_TCCL_PROPERTY), workingDir);
    return latest;
  }

  public JarDeployer getJarDeployer() {
    return this.jarDeployer;
  }

  /**
   * createWithDefaults is exposed for testing.
   */
  static ClassPathLoader createWithDefaults(final boolean excludeTCCL) {
    return new ClassPathLoader(excludeTCCL);
  }

  synchronized void rebuildClassLoaderForDeployedJars() {
    leafLoader = null;
    Collection<DeployedJar> deployedJars = jarDeployer.getDeployedJars().values();
    for (DeployedJar deployedJar : deployedJars) {
      chainClassloader(deployedJar);
    }
  }

  ClassLoader getLeafLoader() {
    if (leafLoader == null) {
      return ClassPathLoader.class.getClassLoader();
    }
    return leafLoader;
  }

  synchronized void chainClassloader(DeployedJar jar) {
    this.leafLoader = new DeployJarChildFirstClassLoader(latestJarNamesToClassLoader,
        new URL[] {jar.getFileURL()}, jar.getJarName(), getLeafLoader());
  }

  synchronized void unloadClassloaderForJar(String jarName) {
    latestJarNamesToClassLoader.put(jarName, null);
  }

  public URL getResource(final String name) {
    final boolean isDebugEnabled = logger.isTraceEnabled();
    if (isDebugEnabled) {
      logger.trace("getResource({})", name);
    }

    for (ClassLoader classLoader : getClassLoaders()) {
      if (isDebugEnabled) {
        logger.trace("getResource trying: {}", classLoader);
      }
      try {
        URL url = classLoader.getResource(name);

        if (url != null) {
          if (isDebugEnabled) {
            logger.trace("getResource found by: {}", classLoader);
          }
          return url;
        }
      } catch (SecurityException e) {
        // try next classLoader
      }
    }

    return null;
  }

  public Class<?> forName(final String name) throws ClassNotFoundException {
    final boolean isDebugEnabled = logger.isTraceEnabled();
    if (isDebugEnabled) {
      logger.trace("forName({})", name);
    }

    Class<?> clazz = forName(name, isDebugEnabled);
    if (clazz != null)
      return clazz;

    throw new ClassNotFoundException(name);
  }

  private Class<?> forName(String name, boolean isDebugEnabled) {
    for (ClassLoader classLoader : this.getClassLoaders()) {
      if (isDebugEnabled) {
        logger.trace("forName trying: {}", classLoader);
      }
      try {
        // Do not look up class definitions in jars that have been unloaded or are old
        if (classLoader instanceof DeployJarChildFirstClassLoader) {
          if (((DeployJarChildFirstClassLoader) classLoader).thisIsOld()) {
            return null;
          }
        }
        Class<?> clazz = Class.forName(name, true, classLoader);
        if (clazz != null) {
          if (isDebugEnabled) {
            logger.trace("forName found by: {}", classLoader);
          }
          return clazz;
        }
      } catch (SecurityException | ClassNotFoundException e) {
        // try next classLoader
      }
    }
    return null;
  }

  /**
   * See {@link Proxy#getProxyClass(ClassLoader, Class...)}
   */
  Class<?> getProxyClass(final Class<?>... classObjs) {
    IllegalArgumentException ex = null;

    for (ClassLoader classLoader : this.getClassLoaders()) {
      try {
        return Proxy.getProxyClass(classLoader, classObjs);
      } catch (SecurityException sex) {
        // Continue to next classloader
      } catch (IllegalArgumentException iaex) {
        ex = iaex;
        // Continue to next classloader
      }
    }

    if (ex != null) {
      throw ex;
    }
    return null;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append(", excludeTCCL=").append(this.excludeTCCL);
    sb.append(", classLoaders=[");
    sb.append(this.getClassLoaders().stream().map(ClassLoader::toString).collect(joining(", ")));
    sb.append("]}");
    return sb.toString();
  }

  /**
   * Finds the resource with the given name. This method will first search the class loader of the
   * context class for the resource. That failing, this method will invoke
   * {@link #getResource(String)} to find the resource.
   *
   * @param contextClass The class whose class loader will first be searched
   * @param name The resource name
   * @return A <tt>URL</tt> object for reading the resource, or <tt>null</tt> if the resource could
   *         not be found or the invoker doesn't have adequate privileges to get the resource.
   */
  public URL getResource(final Class<?> contextClass, final String name) {
    if (contextClass != null) {
      URL url = contextClass.getResource(name);
      if (url != null) {
        return url;
      }
    }
    return getResource(name);
  }

  /**
   * Returns an input stream for reading the specified resource.
   * <p>
   * The search order is described in the documentation for {@link #getResource(String)}.
   *
   * @param name The resource name
   * @return An input stream for reading the resource, or <tt>null</tt> if the resource could not be
   *         found
   */
  public InputStream getResourceAsStream(final String name) {
    URL url = getResource(name);
    try {
      return url != null ? url.openStream() : null;
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * Returns an input stream for reading the specified resource.
   * <p>
   * The search order is described in the documentation for {@link #getResource(Class, String)}.
   *
   * @param contextClass The class whose class loader will first be searched
   * @param name The resource name
   * @return An input stream for reading the resource, or <tt>null</tt> if the resource could not be
   *         found
   */
  public InputStream getResourceAsStream(final Class<?> contextClass, final String name) {
    if (contextClass != null) {
      InputStream is = contextClass.getResourceAsStream(name);
      if (is != null) {
        return is;
      }
    }
    return getResourceAsStream(name);
  }

  /**
   * Finds all the resources with the given name. This method will first search the class loader of
   * the context class for the resource before searching all other {@link ClassLoader}s.
   *
   * @param contextClass The class whose class loader will first be searched
   * @param name The resource name
   * @return An enumeration of {@link java.net.URL <tt>URL</tt>} objects for the resource. If no
   *         resources could be found, the enumeration will be empty. Resources that the class
   *         loader doesn't have access to will not be in the enumeration.
   * @throws IOException If I/O errors occur
   * @see ClassLoader#getResources(String)
   */
  private Enumeration<URL> getResources(final Class<?> contextClass, final String name)
      throws IOException {
    final LinkedHashSet<URL> urls = new LinkedHashSet<URL>();

    if (contextClass != null) {
      CollectionUtils.addAll(urls, contextClass.getClassLoader().getResources(name));
    }

    for (ClassLoader classLoader : getClassLoaders()) {
      Enumeration<URL> resources = classLoader.getResources(name);
      if (resources != null && resources.hasMoreElements()) {
        CollectionUtils.addAll(urls, resources);
      }
    }

    return Collections.enumeration(urls);
  }

  public Enumeration<URL> getResources(final String name) throws IOException {
    return getResources(null, name);
  }

  private List<ClassLoader> getClassLoaders() {
    ArrayList<ClassLoader> classLoaders = new ArrayList<>();

    if (!excludeTCCL) {
      ClassLoader tccl = Thread.currentThread().getContextClassLoader();
      if (tccl != null) {
        classLoaders.add(tccl);
      }
    }

    classLoaders.add(getLeafLoader());
    return classLoaders;
  }

  /**
   * Wrap this {@link ClassPathLoader} with a {@link ClassLoader} facade.
   *
   * @return {@link ClassLoader} facade.
   * @since GemFire 8.1
   */
  public ClassLoader asClassLoader() {
    return new ClassLoader() {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        return ClassPathLoader.this.forName(name);
      }

      @Override
      protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        return ClassPathLoader.this.forName(name);
      }

      @Override
      public URL getResource(String name) {
        return ClassPathLoader.this.getResource(name);
      }

      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        return ClassPathLoader.this.getResources(name);
      }

      @Override
      public InputStream getResourceAsStream(String name) {
        return ClassPathLoader.this.getResourceAsStream(name);
      }
    };
  }

  public static ClassPathLoader getLatest() {
    if (latest == null) {
      synchronized (ClassPathLoader.class) {
        if (latest == null)
          setLatestToDefault();
      }
    }

    return latest;
  }

  /**
   * Helper method equivalent to <code>ClassPathLoader.getLatest().asClassLoader();</code>.
   *
   * @return {@link ClassLoader} for current {@link ClassPathLoader}.
   * @since GemFire 8.1
   */
  public static ClassLoader getLatestAsClassLoader() {
    return getLatest().asClassLoader();
  }

}
