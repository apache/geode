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
package org.apache.geode.classloader.internal.modular;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

import org.jboss.modules.Module;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.classloader.internal.ClassPathLoader;
import org.apache.geode.classloader.internal.ClasspathService;
import org.apache.geode.deployment.internal.DeploymentServiceFactory;
import org.apache.geode.deployment.internal.JarDeploymentService;
import org.apache.geode.deployment.internal.modules.loader.GeodeModuleLoader;


/**
 * This implementation of {@link ClasspathService} will be used by {@link ClassPathLoader} when the
 * system is started in a modular fashion. An instance can be retrieved from
 * {@link DeploymentServiceFactory}
 */
public class ModularClasspathService implements ClasspathService {
  private boolean excludeTCCL = false;
  private GeodeModuleLoader rootModuleLoader = (GeodeModuleLoader) Module.getBootModuleLoader();

  @Override
  public void init(boolean excludeTCCL, JarDeploymentService jarDeploymentService) {
    this.excludeTCCL = excludeTCCL;
  }

  @Override
  public void chainClassloader(File jar) {

  }

  @Override
  public void unloadClassloaderForArtifact(String artifactId) {

  }

  @Override
  public void close() {

  }

  @Override
  public Class<?> forName(String name) throws ClassNotFoundException {
    String normalizedName = name;
    if (name.contains("/")) {
      normalizedName = name.replaceAll("/", ".");
    }

    Throwable tempThrowable = null;

    for (ClassLoader classLoader : getClassLoaders()) {
      try {
        return classLoader.loadClass(normalizedName);
      } catch (ClassNotFoundException | NoClassDefFoundError e) {
        tempThrowable = e;
      }
    }
    throw new ClassNotFoundException(name, tempThrowable);
  }

  @Override
  public Class<?> forName(final Class<?> contextClass, String name) throws ClassNotFoundException {
    String normalizedName = name;
    if (name.contains("/")) {
      normalizedName = name.replaceAll("/", ".");
    }

    try {
      return contextClass.getClassLoader().loadClass(normalizedName);
    } catch (ClassNotFoundException e) {
      return forName(name);
    }
  }

  @Override
  public Class<?> getProxyClass(Class<?>... classObjs) {
    for (ClassLoader classLoader : getClassLoaders()) {
      try {
        return Proxy.getProxyClass(classLoader, classObjs);
      } catch (NoClassDefFoundError e) {
        // ignore
      }
    }
    return null;
  }

  @Override
  public URL getResource(Class<?> contextClass, String name) {
    return contextClass.getResource(name);
  }

  @Override
  public URL getResource(String name) {
    for (ClassLoader classLoader : getClassLoaders()) {
      try {
        return classLoader.getResource(name);
      } catch (NoClassDefFoundError e) {
        // ignore
      }
    }
    return null;
  }

  @Override
  public InputStream getResourceAsStream(Class<?> contextClass, String name) {
    return contextClass.getResourceAsStream(name);
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    for (ClassLoader classLoader : getClassLoaders()) {
      try {
        return classLoader.getResourceAsStream(name);
      } catch (NoClassDefFoundError e) {
        // ignore
      }
    }
    return null;
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    for (ClassLoader classLoader : getClassLoaders()) {
      try {
        return classLoader.getResources(name);
      } catch (NoClassDefFoundError e) {
        // ignore
      }
    }
    return null;
  }

  private List<ClassLoader> getClassLoaders() {
    List<ClassLoader> classLoaders = new LinkedList<>();
    classLoaders.add(GemFireCache.class.getClassLoader());
    if (!excludeTCCL) {
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      if (contextClassLoader != null) {
        classLoaders.add(contextClassLoader);
      }
    }
    return classLoaders;
  }
}
