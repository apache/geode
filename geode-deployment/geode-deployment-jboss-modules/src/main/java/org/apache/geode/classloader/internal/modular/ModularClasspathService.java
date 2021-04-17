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

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.classloader.internal.ClassPathLoader;
import org.apache.geode.classloader.internal.ClasspathService;
import org.apache.geode.deployment.internal.DeploymentServiceFactory;
import org.apache.geode.deployment.internal.JarDeploymentService;

/**
 * This implementation of {@link ClasspathService} will be used by {@link ClassPathLoader} when the
 * system is started in a modular fashion. An instance can be retrieved from
 * {@link DeploymentServiceFactory}
 */
public class ModularClasspathService implements ClasspathService {
  @Override
  public void init(boolean excludeTCCL, JarDeploymentService jarDeploymentService) {

  }

  @Override
  public void chainClassloader(File jar, String deploymentName) {

  }

  @Override
  public void unloadClassloaderForArtifact(String artifactId) {

  }

  @Override
  public void close() {

  }

  @Override
  public Class<?> forName(String name) throws ClassNotFoundException {
    return GemFireCache.class.getClassLoader().loadClass(name);
  }

  @Override
  public Class<?> getProxyClass(Class<?>... classObjs) {
    return Proxy.getProxyClass(GemFireCache.class.getClassLoader(), classObjs);
  }

  @Override
  public URL getResource(Class<?> contextClass, String name) {
    return contextClass.getResource(name);
  }

  @Override
  public URL getResource(String name) {
    return getResource(GemFireCache.class, name);
  }

  @Override
  public InputStream getResourceAsStream(Class<?> contextClass, String name) {
    return contextClass.getResourceAsStream(name);
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    return getResourceAsStream(GemFireCache.class, name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    return GemFireCache.class.getClassLoader().getResources(name);
  }
}
