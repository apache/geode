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

import org.jboss.modules.Module;
import org.jboss.modules.ModuleLoadException;

import org.apache.geode.deployment.internal.modules.loader.GeodeModuleLoader;

/**
 * This class is essentially a wrapper around {@link GeodeModuleLoader} that is used to load and
 * unload modules at runtime.
 */
public class GeodeJBossModuleService implements ModuleService {

  private final GeodeModuleLoader geodeModuleLoader;

  public GeodeJBossModuleService() {
    this((GeodeModuleLoader) Module.getBootModuleLoader());
  }

  protected GeodeJBossModuleService(GeodeModuleLoader geodeModuleLoader) {
    this.geodeModuleLoader = geodeModuleLoader;
  }

  @Override
  public boolean linkModule(String moduleName, String moduleDependee, boolean export) {
    validate(moduleName, moduleDependee);
    try {
      geodeModuleLoader.linkModules(moduleDependee, moduleName, export);
    } catch (ModuleLoadException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  @Override
  public boolean unregisterModule(String moduleName) {
    if (moduleName == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
    try {
      geodeModuleLoader.unregisterModule(moduleName);
    } catch (ModuleLoadException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  @Override
  public boolean moduleExists(String moduleName) {
    try {
      geodeModuleLoader.moduleExists(moduleName);
    } catch (ModuleLoadException e) {
      return false;
    }
    return true;
  }

  private void validate(String moduleName, String dependeeName) {
    if (moduleName == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }

    if (dependeeName == null) {
      throw new IllegalArgumentException("Dependee name cannot be null");
    }
  }
}
