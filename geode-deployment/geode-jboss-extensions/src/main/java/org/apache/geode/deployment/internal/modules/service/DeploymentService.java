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

import java.util.List;

import org.apache.geode.deployment.internal.modules.extensions.impl.Application;
import org.apache.geode.deployment.internal.modules.extensions.impl.GeodeExtension;

/**
 * This interface will be implemented to register and unregister jars with the JBoss modular system.
 */
public interface DeploymentService {

  /**
   * Registers a new module with the modular system, associating it with the given application.
   *
   * @param moduleName the name of the module.
   * @param filePath path to the file to load as a module.
   * @param moduleDependencyNames existing modules that this module will depend on.
   * @return true if the module was registered, false otherwise.
   */
  boolean registerModule(String moduleName, String filePath,
      List<String> moduleDependencyNames);

  /**
   * Registers a new module with the modular system, associating it with the given application.
   *
   * @param moduleName the name of the module.
   * @param applicationName name of the {@link Application} to associate the module with.
   * @param filePath path to the file to load as a module.
   * @param moduleDependencyNames existing modules that this module will depend on.
   * @return true if the module was registered, false otherwise.
   */
  boolean registerModule(String moduleName, String applicationName, String filePath,
      List<String> moduleDependencyNames);

  /**
   * Unregisters a module from te modular system.
   *
   * @param moduleName name of the module to unregister.
   * @return true if the module was unregistered, false if it was not found or could not be removed.
   */
  boolean unregisterModule(String moduleName);

  /**
   * Registers an {@link Application}.
   *
   * @param applicationName name of the {@link Application} to register.
   * @return true if the {@link Application} was registered, false if it was not.
   */
  boolean registerApplication(String applicationName);

  /**
   * Registers an {@link Application}.
   *
   * @param extensionName name of the {@link GeodeExtension} to register.
   * @return true if the {@link GeodeExtension} was registered, false if it was not.
   */
  boolean registerGeodeExtension(String extensionName);

  /**
   * Unregisters a module from te modular system.
   *
   * @param extensionName name of the extension to unregister.
   * @return true if the extension was unregistered, false if it was not found or could not be
   *         removed.
   */
  boolean unregisterExtension(String extensionName);

  void loadGeodeExtensionsFromPropertiesFile(ClassLoader classLoader);
}
