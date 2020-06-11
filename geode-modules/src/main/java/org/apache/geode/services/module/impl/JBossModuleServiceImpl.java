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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleClassLoader;
import org.jboss.modules.ModuleLoadException;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.services.module.ModuleDescriptor;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.module.internal.loader.GeodeModuleLoader;
import org.apache.geode.services.result.ModuleServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

/**
 * Implementation of {@link ModuleService} using JBoss-Modules. This implementation uses
 * JBossModules
 * to load classes in a ClassLoader isolated manner.
 *
 * @see <a href="https://github.com/jboss-modules/jboss-modules">JBoss Modules</a>
 *
 * @since 1.14.0
 */
@Experimental
public class JBossModuleServiceImpl implements ModuleService {

  private final Map<String, Module> modules = new ConcurrentHashMap<>();

  private final GeodeModuleLoader moduleLoader;

  private final Logger logger;

  public JBossModuleServiceImpl(Logger logger) {
    this.logger = logger;
    this.moduleLoader = new GeodeModuleLoader(logger);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ModuleServiceResult<Boolean> registerModule(ModuleDescriptor moduleDescriptor) {
    return moduleLoader.registerModuleDescriptor(moduleDescriptor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ModuleServiceResult<Boolean> loadModule(ModuleDescriptor moduleDescriptor) {
    if (moduleDescriptor == null) {
      return Failure.of("Load module failed due to moduleDescriptor being null");
    }

    String versionedName = moduleDescriptor.getName();
    logger.debug(String.format("Beginning to load module %s", versionedName));

    if (modules.containsKey(versionedName)) {
      String errorMessage = String.format("Module %s is already loaded.", versionedName);
      logger.warn(errorMessage);
      return Failure.of(errorMessage);
    }

    return loadRegisteredModule(moduleDescriptor);
  }

  /**
   * Tries to load a module.
   *
   * @param moduleDescriptor the {@link ModuleDescriptor} describing the module.
   * @return {@link Success} in the event of success and {@link Failure} in case of failure. Failure
   *         to load a module usually happens due to the {@link ModuleDescriptor} not having been
   *         registered.
   *         {@link ModuleDescriptor} can be registered using
   *         {@link ModuleService#registerModule(ModuleDescriptor)}
   */
  private ModuleServiceResult<Boolean> loadRegisteredModule(ModuleDescriptor moduleDescriptor) {
    String versionedName = moduleDescriptor.getName();
    try {
      modules.put(versionedName, moduleLoader.loadModule(versionedName));
      return Success.of(true);
    } catch (ModuleLoadException e) {
      logger.error(e.getMessage(), e);
      return Failure.of(e.getMessage());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ModuleServiceResult<Boolean> unloadModule(String moduleName) {
    logger.debug(String.format("Unloading module %s", moduleName));
    if (!modules.containsKey(moduleName)) {
      String errorMessage =
          String.format("Module %s could not be unloaded because it is not loaded", moduleName);
      logger.warn(errorMessage);
      return Failure.of(errorMessage);
    }

    ModuleServiceResult<Boolean> unloadModuleResult =
        moduleLoader.unloadModule(modules.get(moduleName));
    if (unloadModuleResult.isSuccessful()) {
      modules.remove(moduleName);
      logger.debug(String.format("Module %s was successfully unloaded", moduleName));
    }

    return unloadModuleResult;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> ModuleServiceResult<Map<String, Set<T>>> loadService(Class<T> service) {
    Map<String, Set<T>> serviceImpls = new HashMap<>();

    // Iterate over all the modules looking for implementations of service.
    modules.values().forEach((module) -> {
      module.loadService(service).forEach((serviceImpl) -> {
        String moduleName = ((ModuleClassLoader) serviceImpl.getClass().getClassLoader()).getName();
        Set<T> listOfServices = Optional.ofNullable(serviceImpls.get(moduleName))
            .orElseGet(() -> createTreeSetWithClassLoaderComparator());
        listOfServices.add(serviceImpl);
        serviceImpls.put(moduleName, listOfServices);
      });
    });

    return Success.of(serviceImpls);
  }

  /**
   *
   * Create a Set<T> with a comparator on classname and ClassLoader to remove duplicates.
   *
   * @return empty set of type T
   */
  private <T> Set<T> createTreeSetWithClassLoaderComparator() {
    return new TreeSet<>((o1, o2) -> {
      if (o1 == null && o2 == null) {
        return 0;
      }
      if (o1 == null) {
        return 1;
      }
      if (o2 == null) {
        return -1;
      }
      int classLoaderCompare = ((ModuleClassLoader) o1.getClass().getClassLoader()).getName()
          .compareTo(((ModuleClassLoader) o2.getClass().getClassLoader()).getName());
      if (classLoaderCompare == 0) {
        return o1.getClass().getName().compareTo(o2.getClass().getName());
      }
      return classLoaderCompare;
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ModuleServiceResult<Class<?>> loadClass(String className,
      ModuleDescriptor moduleDescriptor) {
    Module module = modules.get(moduleDescriptor.getName());

    if (module != null) {
      return loadClassFromModule(className, module);
    }
    if (moduleLoader.hasRegisteredModule(moduleDescriptor.getName())) {
      try {
        module = moduleLoader.loadModule(moduleDescriptor.getName());
        return loadClassFromModule(className, module);
      } catch (ModuleLoadException e) {
        logger.error(e);
        return Failure.of(e.getMessage());
      }
    }
    return Failure
        .of(String.format("Module named: %s could be found. Please ensure it is registered",
            moduleDescriptor.getName()));
  }

  /**
   * Tries to load a class for name from a given module.
   *
   * @param className the classname that is to be loaded
   * @param module the module from which the class is to be loaded from.
   * @return {@link Success} with result type {@literal Class} or {@link Failure} with
   *         {@literal String}
   *         error message describing the reason for failure.
   */
  private ModuleServiceResult<Class<?>> loadClassFromModule(String className, Module module) {
    try {
      Class<?> loadedClass = module.getClassLoader().loadClass(className);
      return Success.of(loadedClass);
    } catch (ClassNotFoundException e) {
      String errorMessage =
          String.format("Could not find class for name: %s in module: %s", className,
              module.getName());
      logger.debug(errorMessage);
      return Failure.of(errorMessage);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ModuleServiceResult<Map<String, Class<?>>> loadClass(String className) {
    Map<String, Class<?>> classes = new HashMap<>();
    modules.values().forEach((module) -> {
      try {
        Class<?> loadedClass = module.getClassLoader().loadClass(className);
        classes.put(module.getName(), loadedClass);
      } catch (ClassNotFoundException e) {
        logger.debug(String.format("Could not find class for name: %s in module: %s", className,
            module.getName()));
      }
    });

    return Success.of(classes);
  }
}
