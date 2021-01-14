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

import java.util.Collections;
import java.util.List;

import org.jboss.modules.Module;
import org.jboss.modules.ModuleLoadException;

import org.apache.geode.deployment.internal.modules.loader.GeodeModuleLoader;

/**
 * This class is essentially a wrapper around {@link GeodeModuleLoader} that is used to load and
 * unload modules at runtime.
 */
public class GeodeJBossDeploymentService implements DeploymentService {

  private static final String customJarDeploymentModuleName = "geode-custom-jar-deployments";
  private final GeodeModuleLoader geodeModuleLoader;

  public GeodeJBossDeploymentService() {
    this((GeodeModuleLoader) Module.getBootModuleLoader());
  }

  public GeodeJBossDeploymentService(GeodeModuleLoader geodeModuleLoader) {
    this.geodeModuleLoader = geodeModuleLoader;
  }

  public boolean registerModule(String moduleName, String filePath,
      List<String> moduleDependencyNames) {
    validate(moduleName, filePath);
    if (moduleDependencyNames == null) {
      moduleDependencyNames = Collections.emptyList();
    }
    try {
      geodeModuleLoader.registerModule(moduleName, filePath, moduleDependencyNames);
      geodeModuleLoader.registerModuleAsDependencyOfModule(
          customJarDeploymentModuleName, moduleName);
    } catch (ModuleLoadException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  public boolean unregisterModule(String moduleName) {
    if (moduleName == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
    try {
      geodeModuleLoader.unregisterModuleDependencyFromModules(moduleName);
      geodeModuleLoader.unregisterModule(moduleName);
    } catch (ModuleLoadException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  private void validate(String moduleName, String filePath) {
    if (moduleName == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }

    if (filePath == null) {
      throw new IllegalArgumentException("File path cannot be null");
    }
  }
}
