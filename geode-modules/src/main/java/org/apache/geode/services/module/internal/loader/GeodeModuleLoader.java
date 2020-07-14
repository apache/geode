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

import static org.apache.geode.services.result.impl.Success.SUCCESS_TRUE;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.jboss.modules.DelegatingModuleLoader;
import org.jboss.modules.GeodeModuleFinder;
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
import org.apache.geode.services.result.ServiceResult;
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
 * @see ServiceResult
 * @see ModuleSpec
 *
 * @since 1.14.0
 */
@Experimental
public class GeodeModuleLoader extends DelegatingModuleLoader {
  private final DelegatingModuleFinder moduleFinder;
  private final Map<String, ModuleSpec> moduleSpecs = new ConcurrentHashMap<>();
  private final Logger logger;

  /**
   * Construct a {@link GeodeModuleLoader} with a {@link Logger}
   *
   */
  public GeodeModuleLoader(Logger logger) {
    this(new DelegatingModuleFinder(logger), logger);
  }

  private GeodeModuleLoader(DelegatingModuleFinder moduleFinder, Logger logger) {
    super(Module.getSystemModuleLoader(), moduleFinder);
    this.moduleFinder = moduleFinder;
    this.logger = logger;
  }

  /**
   * Unload a previously loaded {@link Module}.
   *
   * @param module - the {@link Module} to be unloaded
   * @return {@link Success} if the {@link Module} was successfully unloaded. Returns
   *         {@link Failure<String>} if the module was not unloaded.
   */
  public ServiceResult<Boolean> unloadModule(Module module) {
    if (module != null && !StringUtils.isBlank(module.getName())) {
      return unloadModuleAndRemoveSpec(module);
    } else {
      String errorMessage =
          "Module could not be unloaded because either the module or module name is null";
      logger.debug(errorMessage);
      return Failure.of(errorMessage);
    }
  }

  /**
   * Removes the {@link ModuleSpec} and unloads the {@link Module}.
   *
   * @param module - the {@link Module} that needs to be unregistered and unloaded
   * @return {@link Success} in the case of successfully unregistering and unloading of the
   *         module. In the case of an {@link SecurityException} being thrown, the {@link Module} is
   *         not unloaded or unregistered and a {@link Failure<String>} is returned.
   */
  private ServiceResult<Boolean> unloadModuleAndRemoveSpec(Module module) {
    try {
      if (unloadModuleLocal(module.getName(), module)) {
        moduleSpecs.remove(module.getName());
      } else {
        logger.debug("Module could not be unloaded because it does not exist");
      }
    } catch (SecurityException e) {
      logger.error(e);
      return Failure.of("Unloading of module: " + module.getName() + "  failed due to exception",
          e);
    }
    return SUCCESS_TRUE;
  }

  /**
   * Register the {@link ModuleDescriptor} with the modular system.
   *
   * @param moduleDescriptor - the {@link ModuleDescriptor} to be registered
   * @return {@link Success} when the {@link ModuleDescriptor} was successfully registered.
   *         {@link Failure<String>} is returned in the failure case.
   */
  public ServiceResult<Boolean> registerModuleDescriptor(ModuleDescriptor moduleDescriptor) {
    try {
      moduleFinder.addModuleFinder(moduleDescriptor.getName(),
          new GeodeModuleFinder(logger, moduleDescriptor));
    } catch (IOException e) {
      logger.error(e);
      return Failure.of("Registering module: " + moduleDescriptor.getName() + " failed with error",
          e);
    }
    return SUCCESS_TRUE;
  }

  /**
   * {@inheritDoc}
   */
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

  /**
   * Has a {@link ModuleSpec} for the name been registered.
   *
   * @param moduleName - the name of the {@link ModuleSpec} to be checked
   */
  public boolean hasRegisteredModule(String moduleName) {
    return moduleSpecs.get(moduleName) != null;
  }

  /**
   * Unregister a {@link ModuleDescriptor}. The current implementation does not have a failure
   * case, thus {@link Failure<String>} is never returned and always successfull.
   *
   * @param moduleDescriptor - the {@link ModuleDescriptor} to be unregistered
   * @return {@link Success} on successfully unregistering the {@link ModuleDescriptor}.
   */
  public ServiceResult<Boolean> unregisterModuleDescriptor(
      ModuleDescriptor moduleDescriptor) {
    moduleFinder.removeModuleFinderForName(moduleDescriptor.getName());
    moduleSpecs.remove(moduleDescriptor.getName());
    return SUCCESS_TRUE;
  }
}
