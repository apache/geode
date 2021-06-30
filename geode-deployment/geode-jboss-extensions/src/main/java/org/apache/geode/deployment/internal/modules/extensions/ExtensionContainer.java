/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.deployment.internal.modules.extensions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.filter.PathFilter;
import org.jboss.modules.filter.PathFilters;

import org.apache.geode.deployment.internal.modules.loader.GeodeModuleLoader;
import org.apache.geode.deployment.internal.modules.service.DeploymentService;
import org.apache.geode.deployment.internal.modules.service.GeodeJBossDeploymentService;
import org.apache.geode.deployment.internal.modules.utils.ModuleUtils;

public class ExtensionContainer {
  private static final String
      EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME = "external-library-dependencies";
  private final GeodeModuleLoader moduleLoader;
  private final DeploymentService deploymentService;
  private final Map<String, Extension> extensions;
  private PathFilter externalLibraryPathFilter;

  public ExtensionContainer(GeodeModuleLoader moduleLoader) {
    this.moduleLoader = moduleLoader;
    this.deploymentService = new GeodeJBossDeploymentService(moduleLoader);
    this.extensions = new HashMap<>();
  }

  private synchronized PathFilter getExternalLibraryPathFilter() throws ModuleLoadException {
    if(externalLibraryPathFilter == null) {
      Set<String> exportedPaths =
          moduleLoader.loadModule(EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME).getExportedPaths();
      externalLibraryPathFilter = ModuleUtils.createPathFilter(exportedPaths);
    }
    return externalLibraryPathFilter;
  }

  public Extension getExtensionByName(String name) {
    return extensions.get(name);
  }

  public void registerApplication(String name) {
    try {
      extensions.put(name, new Application(name, getExternalLibraryPathFilter()));
    } catch (ModuleLoadException e) {
      throw new RuntimeException(e);
    }
  }

  public void registerGeodeExtension(String name, List<String> moduleDependencies) {
    extensions.put(name, new GeodeExtension(name, PathFilters.acceptAll(), moduleDependencies));
    deploymentService.registerModule(name, null, )
  }

  public void initialize() {

  }
}
