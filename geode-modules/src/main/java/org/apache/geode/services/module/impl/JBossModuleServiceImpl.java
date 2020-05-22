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

package org.apache.geode.services.module.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;

import org.apache.logging.log4j.Logger;
import org.jboss.modules.DependencySpec;
import org.jboss.modules.LocalDependencySpecBuilder;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleDependencySpecBuilder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleSpec;
import org.jboss.modules.PathUtils;
import org.jboss.modules.ResourceLoader;
import org.jboss.modules.ResourceLoaderSpec;
import org.jboss.modules.ResourceLoaders;
import org.jboss.modules.Version;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.ModuleDescriptor;
import org.apache.geode.services.module.ModuleService;

/**
 * Implementation of {@link ModuleService} using JBoss-Modules.
 */
@Experimental
public class JBossModuleServiceImpl implements ModuleService {

  private final Map<String, Module> modules = new HashMap<>();

  private final GeodeModuleLoader moduleLoader = new GeodeModuleLoader();

  private final Logger logger;

  public JBossModuleServiceImpl() {
    this(LogService.getLogger());
  }

  public JBossModuleServiceImpl(Logger logger) {
    this.logger = logger;
  }

  public Module getModule(String name) {
    return modules.get(name);
  }

  @Override
  public boolean loadModule(ModuleDescriptor moduleDescriptor) {
    logger.debug(String.format("Beginning to load module %s", moduleDescriptor.getVersionedName()));

    if (modules.containsKey(moduleDescriptor.getVersionedName())) {
      logger
          .warn(String.format("Module %s is already loaded.", moduleDescriptor.getVersionedName()));
      return false;
    }

    // Setting up new module.
    ModuleSpec.Builder builder = ModuleSpec.build(moduleDescriptor.getVersionedName());
    builder.setVersion(Version.parse(moduleDescriptor.getVersion()));
    builder.addDependency(new LocalDependencySpecBuilder()
        .setImportServices(true)
        .setExport(true)
        .build());

    // Add dependencies to the module.
    moduleDescriptor.getDependedOnModules().forEach(dependency -> {
      logger.debug(String.format("Adding dependency on module %s", dependency));
      builder.addDependency(new ModuleDependencySpecBuilder()
          .setExport(true)
          .setImportServices(true)
          .setName(dependency)
          .build());
    });

    // Add resources to the module.
    try {
      for (String source : moduleDescriptor.getSources()) {
        logger.debug(String.format("Adding resource %s to module", source));
        ResourceLoader resourceLoader =
            ResourceLoaders.createJarResourceLoader(new JarFile(source));
        builder.addResourceRoot(ResourceLoaderSpec.createResourceLoaderSpec(resourceLoader));
      }
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      return false;
    }

    // Add dependency on the system classloader so modules can access classes that aren't in
    // modules.
    builder.addDependency(DependencySpec.createSystemDependencySpec(PathUtils.getPathSet(null)));

    // Build and register the ModuleSpec
    ModuleSpec moduleSpec = builder.create();
    moduleLoader.addModuleSpec(moduleSpec);

    // Load the module and add it to the modules map.
    try {
      modules.put(moduleDescriptor.getVersionedName(),
          moduleLoader.loadModule(moduleSpec.getName()));
    } catch (ModuleLoadException e) {
      logger.error(e.getMessage(), e);
      return false;
    }

    logger
        .debug(String.format("Module %s successfully loaded", moduleDescriptor.getVersionedName()));

    return true;
  }

  @Override
  public boolean unloadModule(String moduleName) {
    return false;
  }

  @Override
  public <T> List<T> loadService(Class<T> service) {
    List<T> serviceImpls = new LinkedList<>();

    // Iterate over all the modules looking for implementations of service.
    modules.values().forEach((module) -> {
      module.loadService(service).forEach((impl) -> {
        // Check if class is already loaded.
        // Modules with dependencies can cause duplicates without this check.
        boolean duplicate = false;
        for (T serviceImpl : serviceImpls) {
          if (serviceImpl.getClass() == impl.getClass()) {
            duplicate = true;
            break;
          }
        }

        // If impl is not a duplicate, add it to the list to return.
        if (!duplicate) {
          serviceImpls.add(impl);
        }
      });
    });

    return serviceImpls;
  }
}
