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
package org.apache.geode.services.classloader;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.services.result.ServiceResult;

/**
 * This interface should be used for loading classes and resources. You can obtain an instance of
 * {@link ClassLoaderService} from ClassLoaderServiceInstance. {@link ClassLoaderService}
 * should be used instead of...
 * - ClassPathLoader
 * - {@link ServiceLoader}
 * - {@link Class#forName(String)}
 * - {@link Thread#getContextClassLoader()}
 * - {@link ClassLoader}
 * and any other method of classloading.
 */
@Experimental
public interface ClassLoaderService {

  /**
   * Loads and returns a {@link List} of service instances.
   *
   * @param service interface type to load and instantiate an implementation of.
   * @return {@link ServiceResult}. containing a {@link List<T>>} containing the loaded services.
   */
  <T> ServiceResult<List<T>> loadService(Class<T> service);

  /**
   * Returns the Class for the provided name.
   *
   * @param className the name fo the class to load.
   * @return {@link ServiceResult} containing the {@link Class>} found for the given name.
   */
  ServiceResult<Class<?>> forName(String className);

  /**
   * Finds the resource represented by the given resource file and returns an {@link InputStream}
   * for the found resources.
   *
   * @param resourceFilePath the name of the resource to be found.
   * @return a {@link ServiceResult} containing an {@link InputStream} representing the desired
   *         resource.
   */
  ServiceResult<InputStream> getResourceAsStream(String resourceFilePath);

  /**
   * Finds the resource represented by the given resource file and returns an {@link InputStream}
   * for the found resources.
   *
   * @param clazz the class to find the resource relative to.
   * @param resourceFilePath the name of the resource to be found.
   * @return a {@link ServiceResult} containing an {@link InputStream} representing the desired
   *         resource.
   */
  ServiceResult<InputStream> getResourceAsStream(Class<?> clazz, String resourceFilePath);

  /**
   * Finds the resource represented by the given resource file and returns a {@link URL}
   * for the found resources.
   *
   * @param resourceFilePath the name of the resource to be found.
   * @return a {@link ServiceResult} containing an {@link URL} representing the desired
   *         resource.
   */
  ServiceResult<URL> getResource(String resourceFilePath);

  /**
   * Finds the resource represented by the given resource file and returns a {@link URL}
   * for the found resources.
   *
   * @param clazz the class to find the resource relative to.
   * @param resourceFilePath the name of the resource to be found.
   * @return a {@link ServiceResult} containing a {@link URL} representing the desired resource.
   */
  ServiceResult<URL> getResource(Class<?> clazz, String resourceFilePath);

  /**
   * Gets a proxy class for the given class(es)
   *
   * @param clazz the {@link Class} or {@link Class}es for which to get the proxy class
   * @return a proxy {@link Class} that implements all the {@link Class}es specified by clazz.
   *
   * @see Proxy
   */
  ServiceResult<Class<?>> getProxyClass(Class<?>... clazz);

  /**
   * Sets the {@link Logger} to be used by the {@link ClassLoaderService}.
   *
   * @param logger the {@link Logger} to be used.
   */
  void setLogger(Logger logger);

  /**
   * Sets the directory to use for deploying jars.
   *
   * @param deployWorkingDir a {@link File} representing the directory to use when deploying jars.
   */
  void setWorkingDirectory(File deployWorkingDir);

  /**
   * get a {@link ClassLoader} backed by the {@link ClassLoaderService}.
   *
   * @return a {@link ClassLoader} backed by the {@link ClassLoaderService}.
   */
  ClassLoader asClassLoader();
}
