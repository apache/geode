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
import java.net.URL;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.services.registry.ServiceRegistryInstance;
import org.apache.geode.services.result.ServiceResult;

@Experimental
public interface ClassLoaderService {

  /**
   * Loads and returns a service instance from any loaded module.
   *
   * @param service interface type to load and instantiate an implementation of.
   * @return {@link ServiceResult}. This type represents either Success or Failure. Using
   *         {@link ServiceResult#isSuccessful()} returns a {@literal Map<String,Set<T>>} in
   *         the case of success.
   *         The result is a Map of {@code <ModuleName,Set<ServiceInstance>>}. Where moduleName
   *         corresponds
   *         to the serviceInstance from the module.
   *         Upon success use {@link ServiceResult#getMessage()} to get the result and upon
   *         failure
   *         used {@link ServiceResult#getErrorMessage()} to get the error message of the
   *         failure.
   */
  <T> ServiceResult<Set<T>> loadService(Class<T> service);

  /**
   * Returns the Class for the provided name for all loaded module.
   *
   * @param className the classname that is to be loaded
   * @return {@link ServiceResult}. This type represents either Success or Failure. Using
   *         {@link ServiceResult#isSuccessful()} returns a {@literal Map<String,Class<T>>} in
   *         the case of success.
   *         The resultant map returns a list of modules where the class can be loaded from.
   *         Upon success use {@link ServiceResult#getMessage()} to get the result and upon
   *         failure
   *         used {@link ServiceResult#getErrorMessage()} to get the error message of the
   *         failure.
   */
  ServiceResult<Class<?>> forName(String className);

  /**
   * Finds the resource represented by the given resource file and returns a collection of
   * {@link InputStream}s for the found resources.
   *
   * @param resourceFilePath the name of the resource to be found.
   * @return a {@link ServiceResult} containing a {@link List} of {@link InputStream}s representing
   *         the desired resource.
   */
  ServiceResult<InputStream> getResourceAsStream(String resourceFilePath);

  ServiceResult<InputStream> getResourceAsStream(Class<?> clazz, String resourceFilePath);

  ServiceResult<URL> getResource(String resourceFilePath);

  ServiceResult<URL> getResource(Class<?> clazz, String resourceFilePath);

  ServiceResult<Class<?>> getProxyClass(Class<?>... clazz);

  /**
   * Sets the {@link Logger} to be used by the {@link ClassLoaderService}.
   *
   * @param logger the {@link Logger} to be used.
   */
  void setLogger(Logger logger);

  void setWorkingDirectory(File deployWorkingDir);

  static ClassLoaderService getClassLoaderService() {
    ServiceResult<ClassLoaderService> result =
        ServiceRegistryInstance.getService(ClassLoaderService.class);
    if (result.isFailure()) {
      throw new RuntimeException("No ClassLoaderService registered in ServiceRegistry");
    }
    return result.getMessage();
  }

  ClassLoader asClassLoader();
}
