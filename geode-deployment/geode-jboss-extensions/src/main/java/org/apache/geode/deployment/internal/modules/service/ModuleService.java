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
package org.apache.geode.deployment.internal.modules.service;


/**
 * This interface will be implemented to register and unregister jars with the JBoss modular system.
 */
public interface ModuleService {

  /**
   * Links a new module into the classloader isolation system.
   *
   * @param moduleName the name of the module.
   * @param moduleDependee name of dependee module.
   * @return true if the module was registered, false otherwise.
   */
  boolean linkModule(String moduleName, String moduleDependee, boolean export);

  /**
   * Unregisters a module from the classloader isolation system.
   *
   * @param moduleName name of the module to unregister.
   * @return true if the module was unregistered, false if it was not found or could not be removed.
   */
  boolean unregisterModule(String moduleName);

  boolean moduleExists(String moduleName);
}
