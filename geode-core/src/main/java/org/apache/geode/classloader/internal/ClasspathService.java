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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;

import org.apache.geode.deployment.internal.DeploymentServiceFactory;
import org.apache.geode.deployment.internal.JarDeploymentService;

/**
 * This interface replaces the guts of {@link ClassPathLoader} to allow differences in classloading
 * behavior between the modular and legacy behavior.
 * <p>
 * Only one instance should exist at a time. The current instance can be retrieved from
 * {@link DeploymentServiceFactory#getClasspathServiceInstance()}
 */
public interface ClasspathService {

  void init(boolean excludeTCCL, JarDeploymentService jarDeploymentService);

  void chainClassloader(File jar, String deploymentName);

  void unloadClassloaderForArtifact(String artifactId);

  void close();

  Class<?> forName(final String name) throws ClassNotFoundException;

  Class<?> getProxyClass(final Class<?>... classObjs);

  URL getResource(final Class<?> contextClass, final String name);

  URL getResource(final String name);

  InputStream getResourceAsStream(final Class<?> contextClass, final String name);

  InputStream getResourceAsStream(final String name);

  Enumeration<URL> getResources(final String name) throws IOException;
}
