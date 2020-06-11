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

package org.apache.geode.services.module;

import java.util.Map;
import java.util.Set;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.services.result.ModuleServiceResult;

/**
 * Loads and unloads modules and services in a classloader-isolated manner.
 *
 * @since Geode 1.14.0
 */
@Experimental
public interface ModuleService {

  /**
   * Loads a module from a resource.
   *
   * @param moduleDescriptor description of the module to be loaded and information necessary to
   *        load it.
   * @return {@link ModuleServiceResult}. This type represents either Success or Failure. Using
   *         {@link ModuleServiceResult#isSuccessful()} returns a {@literal Boolean} in the case of
   *         success.
   *         Upon success use {@link ModuleServiceResult#getMessage()} to get the result and upon
   *         failure
   *         used {@link ModuleServiceResult#getErrorMessage()} to get the error message of the
   *         failure.
   */
  ModuleServiceResult<Boolean> loadModule(ModuleDescriptor moduleDescriptor);

  /**
   * Registers a module from the {@link ModuleDescriptor}.
   *
   * @param moduleDescriptor description of the module to be loaded and information necessary to
   *        register it.
   * @return {@link ModuleServiceResult}. This type represents either Success or Failure. Using
   *         {@link ModuleServiceResult#isSuccessful()} returns a {@literal Boolean} in the case of
   *         success.
   *         Upon success use {@link ModuleServiceResult#getMessage()} to get the result and upon
   *         failure
   *         used {@link ModuleServiceResult#getErrorMessage()} to get the error message of the
   *         failure.
   */
  ModuleServiceResult<Boolean> registerModule(ModuleDescriptor moduleDescriptor);

  /**
   * Unloads a previously loaded module.
   * unloadModule does not check for dependent modules, therefor you may only use it to unload
   * modules that are not dependencies of other modules.
   *
   * @param moduleName name of the module to be unloaded.
   * @return {@link ModuleServiceResult}. This type represents either Success or Failure. Using
   *         {@link ModuleServiceResult#isSuccessful()} returns a {@literal Boolean} in the case of
   *         success.
   *         Upon success use {@link ModuleServiceResult#getMessage()} to get the result and upon
   *         failure
   *         used {@link ModuleServiceResult#getErrorMessage()} to get the error message of the
   *         failure.
   */
  ModuleServiceResult<Boolean> unloadModule(String moduleName);

  /**
   * Loads and returns a service instance from any loaded module.
   *
   * @param service interface type to load and instantiate an implementation of.
   * @return {@link ModuleServiceResult}. This type represents either Success or Failure. Using
   *         {@link ModuleServiceResult#isSuccessful()} returns a {@literal Map<String,Set<T>>} in
   *         the case of success.
   *         The result is a Map of {@code <ModuleName,Set<ServiceInstance>>}. Where moduleName
   *         corresponds
   *         to the serviceInstance from the module.
   *         Upon success use {@link ModuleServiceResult#getMessage()} to get the result and upon
   *         failure
   *         used {@link ModuleServiceResult#getErrorMessage()} to get the error message of the
   *         failure.
   */
  <T> ModuleServiceResult<Map<String, Set<T>>> loadService(Class<T> service);

  /**
   * Returns the Class for the provided name for a specific module.
   *
   * @param className the classname that is to be loaded
   * @param moduleDescriptor the ${@link ModuleDescriptor} used to lookup the module in question
   * @return {@link ModuleServiceResult}. This type represents either Success or Failure. Using
   *         {@link ModuleServiceResult#isSuccessful()} returns a {@literal Class<T>} in the case of
   *         success.
   *         Upon success use {@link ModuleServiceResult#getMessage()} to get the result and upon
   *         failure
   *         used {@link ModuleServiceResult#getErrorMessage()} to get the error message of the
   *         failure.
   */
  ModuleServiceResult<Class<?>> loadClass(String className, ModuleDescriptor moduleDescriptor);

  /**
   * Returns the Class for the provided name for all loaded module.
   *
   * @param className the classname that is to be loaded
   * @return {@link ModuleServiceResult}. This type represents either Success or Failure. Using
   *         {@link ModuleServiceResult#isSuccessful()} returns a {@literal Map<String,Class<T>>} in
   *         the case of success.
   *         The resultant map returns a list of modules where the class can be loaded from.
   *         Upon success use {@link ModuleServiceResult#getMessage()} to get the result and upon
   *         failure
   *         used {@link ModuleServiceResult#getErrorMessage()} to get the error message of the
   *         failure.
   */
  ModuleServiceResult<Map<String, Class<?>>> loadClass(String className);
}
