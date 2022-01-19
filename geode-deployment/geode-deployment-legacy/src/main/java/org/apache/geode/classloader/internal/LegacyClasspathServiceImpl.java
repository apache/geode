/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.classloader.internal;

import static java.util.stream.Collectors.joining;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.internal.classloader.ClasspathService;
import org.apache.geode.internal.classloader.DeployJarChildFirstClassLoader;
import org.apache.geode.internal.deployment.DeploymentServiceFactory;
import org.apache.geode.internal.deployment.JarDeploymentService;
import org.apache.geode.internal.util.CollectionUtils;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.utils.JarFileUtils;

/**
 * This implementation of {@link ClasspathService} will be used by {@link ClassPathLoader} when the
 * system is started in a non-modular fashion. An instance can be retrieved from
 * {@link DeploymentServiceFactory}
 */
public class LegacyClasspathServiceImpl implements ClasspathService {

  private static final Logger logger = LogService.getLogger();

  private final Map<String, DeployJarChildFirstClassLoader> artifactIdsToClassLoader =
      new HashMap<>();

  private volatile DeployJarChildFirstClassLoader leafLoader;

  private boolean excludeTCCL;

  private JarDeploymentService jarDeploymentService;

  public void init(boolean excludeTCCL, JarDeploymentService jarDeploymentService) {
    this.excludeTCCL = excludeTCCL;
    this.jarDeploymentService = jarDeploymentService;
    rebuildClassLoaderForDeployedJars();
  }

  public synchronized void chainClassloader(File jar) {
    try {
      leafLoader = new DeployJarChildFirstClassLoader(artifactIdsToClassLoader,
          new URL[] {jar.toURI().toURL()}, JarFileUtils.toArtifactId(jar.getName()),
          getLeafLoader());
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }
  }

  public synchronized void unloadClassloaderForArtifact(String artifactId) {
    artifactIdsToClassLoader.put(artifactId, null);
  }

  @Override
  public void close() {
    leafLoader = null;
    artifactIdsToClassLoader.clear();
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

  public Enumeration<URL> getResources(final String name) throws IOException {
    final LinkedHashSet<URL> urls = new LinkedHashSet<>();

    for (ClassLoader classLoader : getClassLoaders()) {
      Enumeration<URL> resources = classLoader.getResources(name);
      if (resources != null && resources.hasMoreElements()) {
        CollectionUtils.addAll(urls, resources);
      }
    }

    return Collections.enumeration(urls);
  }

  public URL getResource(final String name) {
    final boolean isTraceEnabled = logger.isTraceEnabled();
    if (isTraceEnabled) {
      logger.trace("getResource({})", name);
    }

    for (ClassLoader classLoader : getClassLoaders()) {
      if (isTraceEnabled) {
        logger.trace("getResource trying: {}", classLoader);
      }
      try {
        URL url = classLoader.getResource(name);

        if (url != null) {
          if (isTraceEnabled) {
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
    final boolean isTraceEnabled = logger.isTraceEnabled();
    if (isTraceEnabled) {
      logger.trace("forName({})", name);
    }

    Class<?> clazz = forName(name, isTraceEnabled);
    if (clazz != null) {
      return clazz;
    }

    throw new ClassNotFoundException(name);
  }

  private Class<?> forName(String name, boolean isTraceEnabled) {
    for (ClassLoader classLoader : getClassLoaders()) {
      if (isTraceEnabled) {
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
        if (isTraceEnabled) {
          logger.trace("forName found by: {}", classLoader);
        }
        return clazz;
      } catch (SecurityException | ClassNotFoundException e) {
        // try next classLoader
      }
    }
    return null;
  }

  /**
   * See {@link Proxy#getProxyClass(ClassLoader, Class...)}
   */
  public Class<?> getProxyClass(final Class<?>... classObjs) {
    IllegalArgumentException ex = null;

    for (ClassLoader classLoader : getClassLoaders()) {
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

  private synchronized void rebuildClassLoaderForDeployedJars() {
    leafLoader = null;
    List<Deployment> deployments = jarDeploymentService.listDeployed();
    for (Deployment deployment : deployments) {
      chainClassloader(deployment.getFile());
    }
  }

  private ClassLoader getLeafLoader() {
    if (leafLoader == null) {
      return ClassPathLoader.class.getClassLoader();
    }
    return leafLoader;
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

  @Override
  public String toString() {
    return getClass().getName() + "@" + System.identityHashCode(this) + "{"
        + "excludeTCCL=" + excludeTCCL
        + ", jarDeployer=" + jarDeploymentService
        + ", classLoaders=["
        + getClassLoaders().stream().map(ClassLoader::toString).collect(joining(", "))
        + "]}";
  }
}
