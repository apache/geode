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

import java.util.List;

import org.apache.geode.annotations.Experimental;

/**
 * Loads and unloads modules and services in a classloader-isolated manner.
 *
 * @since Geode 1.13.0
 */
@Experimental
public interface ModuleService {

  /**
   * Loads a module from a resource.
   *
   * @param moduleDescriptor description of the module to be loaded and information necessary to
   *        load it.
   * @return true on success, false if the module could not be loaded.
   */
  boolean loadModule(ModuleDescriptor moduleDescriptor);

  /**
   * Unloads a previously loaded module.
   *
   * @param moduleName name of the module to be unloaded.
   * @return true on success, false if the module could not be unloaded.
   */
  boolean unloadModule(String moduleName);

  /**
   * Loads and returns a service instance for an interface.
   *
   * @param service interface type to load and instantiate an implementation of.
   * @return An instance of an implementation of service
   */
  <T> List<T> loadService(Class<T> service);
}
