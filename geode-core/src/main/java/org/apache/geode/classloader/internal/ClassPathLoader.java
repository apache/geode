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
package org.apache.geode.classloader.internal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.deployment.internal.DeploymentServiceFactory;
import org.apache.geode.deployment.internal.JarDeploymentService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * The delegating <tt>ClassLoader</tt> used by GemFire to load classes and other resources. This
 * <tt>ClassLoader</tt> delegates to any <tt>ClassLoader</tt>s added to the list of custom class
 * loaders, thread context <tt>ClassLoader</tt> s unless they have been excluded}, the
 * <tt>ClassLoader</tt> which loaded the GemFire classes, and finally the system
 * <tt>ClassLoader</tt>.
 *
 * <p>
 * The thread context class loaders can be excluded by setting the system property
 * <tt>gemfire.excludeThreadContextClassLoader</tt>:
 * <ul>
 * <li><tt>-Dgemfire.excludeThreadContextClassLoader=true</tt>
 * <li><tt>System.setProperty("gemfire.excludeThreadContextClassLoader", "true");</tt>
 * </ul>
 *
 * <p>
 * Class loading and resource loading order:
 * <ul>
 * <li>1. Any custom loaders in the order they were added
 * <li>2. <tt>Thread.currentThread().getContextClassLoader()</tt> unless excludeTCCL == true
 * <li>3. <tt>ClassPathLoader.class.getClassLoader()</tt>
 * <li>4. <tt>ClassLoader.getSystemClassLoader()</tt> If the attempt to acquire any of the above
 * class loaders results in either a {@code SecurityException} or a null, then that class loader is
 * quietly skipped. Duplicate class loaders will be skipped.
 * </ul>
 *
 * <p>
 * See http://docs.oracle.com/javase/specs/jvms/se7/html/jvms-5.html for more information about
 * {@code ClassLoader}s.
 *
 * @since GemFire 6.5.1.4
 */
public class ClassPathLoader {
  static final String EXCLUDE_TCCL_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "excludeThreadContextClassLoader";
  @MakeNotStatic
  private static volatile ClassPathLoader latest;

  private final JarDeploymentService jarDeploymentService;
  private final ClasspathService classPathService;


  private ClassPathLoader(boolean excludeTCCL) {
    jarDeploymentService = DeploymentServiceFactory.getJarDeploymentServiceInstance();
    classPathService = DeploymentServiceFactory.getClasspathServiceInstance();
    classPathService.init(excludeTCCL, jarDeploymentService);

  }

  private ClassPathLoader(boolean excludeTCCL, File workingDir) {
    jarDeploymentService = DeploymentServiceFactory.getJarDeploymentServiceInstance();
    jarDeploymentService.reinitializeWithWorkingDirectory(workingDir);
    classPathService = DeploymentServiceFactory.getClasspathServiceInstance();
    classPathService.init(excludeTCCL, jarDeploymentService);
  }

  public static ClassPathLoader setLatestToDefault(File workingDir) {
    latest = new ClassPathLoader(Boolean.getBoolean(EXCLUDE_TCCL_PROPERTY), workingDir);
    return latest;
  }

  /**
   * createWithDefaults is exposed for testing.
   */
  @VisibleForTesting
  static ClassPathLoader createWithDefaults(final boolean excludeTCCL) {
    return new ClassPathLoader(excludeTCCL);
  }

  public static ClassPathLoader getLatest() {
    if (latest == null) {
      synchronized (ClassPathLoader.class) {
        if (latest == null) {
          setLatestToDefault(null);
        }
      }
    }

    return latest;
  }

  /**
   * Helper method equivalent to {@code ClassPathLoader.getLatest().asClassLoader();}.
   *
   * @return a ClassLoader for current ClassPathLoader.
   * @since GemFire 8.1
   */
  public static ClassLoader getLatestAsClassLoader() {
    return getLatest().asClassLoader();
  }

  @Override
  public String toString() {
    return classPathService.toString();
  }

  /**
   * Wrap this {@code ClassPathLoader} with a {@code ClassLoader} facade.
   *
   * @return a ClassLoader facade.
   * @since GemFire 8.1
   */
  public ClassLoader asClassLoader() {
    return new ClassLoader() {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        return forName(name);
      }

      @Override
      protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        return forName(name);
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

  public void chainClassloader(File jar) {
    classPathService.chainClassloader(jar);
  }

  public void unloadClassloaderForArtifact(String artifactId) {
    classPathService.unloadClassloaderForArtifact(artifactId);
  }

  public Class<?> forName(String name) throws ClassNotFoundException {
    return classPathService.forName(name);
  }

  public Class<?> getProxyClass(Class<?>... classObjs) {
    return classPathService.getProxyClass(classObjs);
  }

  public URL getResource(Class<?> contextClass, String name) {
    return classPathService.getResource(contextClass, name);
  }

  public URL getResource(String name) {
    return classPathService.getResource(name);
  }

  public InputStream getResourceAsStream(Class<?> contextClass, String name) {
    return classPathService.getResourceAsStream(contextClass, name);
  }

  public InputStream getResourceAsStream(String name) {
    return classPathService.getResourceAsStream(name);
  }

  public Enumeration<URL> getResources(String name) throws IOException {
    return classPathService.getResources(name);
  }

  public JarDeploymentService getJarDeploymentService() {
    return jarDeploymentService;
  }
}
