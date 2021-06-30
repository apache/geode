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
package org.apache.geode.deployment.internal.modules.extensions;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.filter.PathFilter;
import org.jboss.modules.filter.PathFilters;

import org.apache.geode.deployment.internal.modules.loader.GeodeModuleLoader;
import org.apache.geode.deployment.internal.modules.utils.ModuleUtils;

public class ExtensionContainer {
  private static final String EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME =
      "external-library-dependencies";
  private final GeodeModuleLoader moduleLoader;
  private final Map<String, Extension> extensions;
  private PathFilter externalLibraryPathFilter;

  public ExtensionContainer(GeodeModuleLoader moduleLoader) {
    this.moduleLoader = moduleLoader;
    this.extensions = new ConcurrentHashMap<>();
  }

  private synchronized PathFilter getExternalLibraryPathFilter() throws ModuleLoadException {
    if (externalLibraryPathFilter == null) {
      Set<String> exportedPaths =
          moduleLoader.loadModule(EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME).getExportedPaths();
      externalLibraryPathFilter = ModuleUtils.createPathFilter(exportedPaths);
    }
    return externalLibraryPathFilter;
  }

  public Extension getExtensionByName(String name) {
    return extensions.get(name);
  }

  public boolean registerApplication(String name) {
    try {
      Application application = new Application(name, getExternalLibraryPathFilter());
      moduleLoader.registerExtensionModule(application);
      extensions.put(name, application);
    } catch (ModuleLoadException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean registerGeodeExtension(String name, List<String> moduleDependencies) {
    GeodeExtension extension =
        new GeodeExtension(name, PathFilters.acceptAll(), moduleDependencies);
    extensions.put(name, extension);
    try {
      moduleLoader.registerExtensionModule(extension);
    } catch (ModuleLoadException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean unregisterExtension(String extensionName) {
    Extension extension = extensions.get(extensionName);
    if (extension == null) {
      return false;
    }
    try {
      moduleLoader.unregisterModule(extension.getName());
    } catch (ModuleLoadException e) {
      e.printStackTrace();
      return false;
    }
    extensions.remove(extensionName);
    return true;
  }

  public void initialize() {
    registerApplication("geode-custom-jar-deployments");
    // TODO: we need to add things or Dunit here.
    registerGeodeExtension("geode-extensions", Arrays.asList(
        "geode-cq",
        "geode-gfsh",
        "geode-lucene",
        "geode-apis-compatible-with-redis",
        "geode-memcached",
        "geode-wan",
        "geode-connectors",
        "geode-deployment-jboss-modules",
        "geode-log4j"));
  }

  public Collection<Extension> getAllExtensions() {
    return extensions.values();
  }

  public Collection<GeodeExtension> getGeodeExtensions() {
    return extensions.values().stream().filter(extension -> extension instanceof GeodeExtension)
        .map(extension -> (GeodeExtension) extension).collect(
            Collectors.toList());
  }

  public Collection<Application> getApplications() {
    return extensions.values().stream().filter(extension -> extension instanceof Application)
        .map(extension -> (Application) extension).collect(
            Collectors.toList());
  }
}
