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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  private Logger logger;

  public JBossModuleServiceImpl() {
    this.moduleLoader = new GeodeModuleLoader();
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
    logDebug(String.format("Beginning to load module %s", versionedName));

    if (modules.containsKey(versionedName)) {
      String errorMessage = String.format("Module %s is already loaded.", versionedName);
      logWarn(errorMessage);
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
      logError(e.getMessage());
      return Failure.of(e.toString());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ModuleServiceResult<Boolean> unloadModule(String moduleName) {
    logDebug(String.format("Unloading module %s", moduleName));
    if (!modules.containsKey(moduleName)) {
      String errorMessage =
          String.format("Module %s could not be unloaded because it is not loaded", moduleName);
      logWarn(errorMessage);
      return Failure.of(errorMessage);
    }

    ModuleServiceResult<Boolean> unloadModuleResult =
        moduleLoader.unloadModule(modules.get(moduleName));
    if (unloadModuleResult.isSuccessful()) {
      modules.remove(moduleName);
      logDebug(String.format("Module %s was successfully unloaded", moduleName));
    }

    return unloadModuleResult;
  }

  /**
   * {@inheritDoc}
   *
   */
  @Override
  public <T> ModuleServiceResult<Set<T>> loadService(Class<T> service) {
    Set<T> result = createTreeSetWithClassLoaderComparator();

    // Iterate over all the modules looking for implementations of service.
    modules.values().forEach((module) -> {
      Iterator<T> loadedServices = module.loadService(service).iterator();
      while (loadedServices.hasNext()) {
        try {
          result.add(loadedServices.next());
        } catch (Error e) {
          logError(e.getMessage());
        }
      }
    });

    return Success.of(result);
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
        logError(e.getMessage());
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
      logDebug(errorMessage);
      return Failure.of(errorMessage);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ModuleServiceResult<List<Class<?>>> loadClass(String className) {
    List<Class<?>> result = new ArrayList<>();
    modules.values().forEach((module) -> {
      try {
        result.add(module.getClassLoader().loadClass(className));
      } catch (ClassNotFoundException e) {
        logDebug(String.format("Could not find class for name: %s in module: %s", className,
            module.getName()));
      }
    });

    return Success.of(result);
  }

  @Override
  public ModuleServiceResult<List<InputStream>> findResourceAsStream(String resourceFile) {
    List<InputStream> results = new ArrayList<>();
    modules.values().forEach(module -> {
      InputStream resourceAsStream =
          module.getClassLoader().findResourceAsStream(resourceFile, false);

      if (resourceAsStream != null) {
        results.add(resourceAsStream);
      }
    });

    return results.isEmpty()
        ? Failure.of(String.format("No resource for path: %s could be found", resourceFile))
        : Success.of(results);
  }

  @Override
  public void setLogger(Logger logger) {
    this.logger = logger;
  }

  private void logWarn(String message) {
    if (logger != null) {
      logger.warn(message);
    }
  }

  private void logInfo(String message) {
    if (logger != null) {
      logger.info(message);
    }
  }

  private void logDebug(String message) {
    if (logger != null) {
      logger.debug(message);
    }
  }

  private void logError(String message) {
    if (logger != null) {
      logger.error(message);
    }
  }
}
