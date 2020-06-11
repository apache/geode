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

package org.apache.geode.services.module.internal.loader;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.jboss.modules.DelegatingModuleLoader;
import org.jboss.modules.GeodeJarModuleFinder;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.services.module.ModuleDescriptor;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.module.impl.JBossModuleServiceImpl;
import org.apache.geode.services.module.internal.finder.DelegatingModuleFinder;
import org.apache.geode.services.result.ModuleServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

/**
 * A custom implementation of {@link ModuleLoader} for use by {@link JBossModuleServiceImpl}.
 * This implementation has reference to a {@link DelegatingModuleFinder} of type
 * {@link ModuleFinder}
 * to store a dynamically sized list of {@link ModuleFinder}.
 *
 * It also keeps track of registered {@link ModuleSpec} which have been created from
 * {@link ModuleDescriptor}
 * on a per module basis.
 *
 * @see ModuleLoader
 * @see JBossModuleServiceImpl
 * @see ModuleService
 * @see ModuleServiceResult
 * @see ModuleSpec
 *
 * @since 1.14.0
 */
@Experimental
public class GeodeModuleLoader extends DelegatingModuleLoader {
  private final DelegatingModuleFinder moduleFinder;
  private final Map<String, ModuleSpec> moduleSpecs = new ConcurrentHashMap<>();
  private final Logger logger;

  public GeodeModuleLoader(Logger logger) {
    this(new DelegatingModuleFinder(logger), logger);
  }

  private GeodeModuleLoader(DelegatingModuleFinder moduleFinder, Logger logger) {
    super(Module.getSystemModuleLoader(), moduleFinder);
    this.moduleFinder = moduleFinder;
    this.logger = logger;
  }

  public ModuleServiceResult<Boolean> unloadModule(Module module) {
    if (module != null && !StringUtils.isEmpty(module.getName())) {
      return unloadModuleAndRemoveSpec(module);
    } else {
      String errorMessage =
          "Module could not be unloaded because either the module or module name is null";
      logger.debug(errorMessage);
      return Failure.of(errorMessage);
    }
  }

  private ModuleServiceResult<Boolean> unloadModuleAndRemoveSpec(Module module) {
    try {
      if (unloadModuleLocal(module.getName(), module)) {
        moduleSpecs.remove(module.getName());
        return Success.of(true);
      } else {
        String errorMessage = "Module could not be unloaded because it does not exist";
        logger.debug(errorMessage);
        return Failure.of(errorMessage);
      }
    } catch (SecurityException e) {
      logger.error(e);
      return Failure.of(String.format("Unloading of module: %s  failed due to exception: %s",
          module.getName(), e.getMessage()));
    }
  }

  public ModuleServiceResult<Boolean> registerModuleDescriptor(ModuleDescriptor descriptor) {
    try {
      moduleFinder.addModuleFinder(new GeodeJarModuleFinder(logger, descriptor));
    } catch (IOException e) {
      logger.error(e);
      return Failure.of(String.format("Registering module: %s failed with error: %s",
          descriptor.getName(), e.getMessage()));
    }
    return Success.of(true);
  }

  @Override
  protected ModuleSpec findModule(String name) throws ModuleLoadException {
    ModuleSpec moduleSpec = moduleSpecs.get(name);
    if (moduleSpec == null) {
      moduleSpec = moduleFinder.findModule(name, this);
      if (moduleSpec != null) {
        moduleSpecs.put(name, moduleSpec);
      }
    }
    return moduleSpec;
  }

  public boolean hasRegisteredModule(String moduleName) {
    return moduleSpecs.get(moduleName) != null;
  }
}
