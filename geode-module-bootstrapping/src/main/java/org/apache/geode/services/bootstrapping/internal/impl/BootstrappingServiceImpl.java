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
package org.apache.geode.services.bootstrapping.internal.impl;

import static org.apache.geode.services.result.impl.Success.SUCCESS_TRUE;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.services.bootstrapping.BootstrappingService;
import org.apache.geode.services.management.impl.ComponentIdentifier;
import org.apache.geode.services.module.ModuleDescriptor;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

/**
 * An implementation of {@link BootstrappingService} that uses the {@link ModuleService} to load the
 * relevant modules for a given {@link ComponentIdentifier}. This implementation will process the
 * {@link Manifest} file from the {@link JarFile} and ensure all dependent modules are registered.
 * Additionally, this implementation will employ the concept of a linking module, which is
 * functionally similar to a mapping table from the relational database world.
 *
 * This linking modules is intended to maintain the bidirectional relationships that currently exist
 * between modules in Geode. The linking module also enables the ability to dynamically add/remove
 * modules at runtime and avoid costly reloading of existing modules.
 *
 * @since Geode 1.14.0
 *
 * @see BootstrappingService
 * @see ComponentIdentifier
 * @see ModuleService
 * @see JarFile
 * @see Manifest
 * @see ModuleDescriptor
 */
public class BootstrappingServiceImpl implements BootstrappingService {

  private static final String[] EMPTY_STRING_ARRAY = {};
  private final ModuleDescriptor.Builder geodeModuleDescriptorBuilder =
      new ModuleDescriptor.Builder("geode")
          .requiresJDKPaths(true);
  private Logger logger;
  private ModuleService moduleService;

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(ModuleService moduleService, Logger logger) {
    this.moduleService = moduleService;
    this.logger = logger;
    ModuleDescriptor geodeModuleDescriptor = geodeModuleDescriptorBuilder.build();
    moduleService.registerModule(geodeModuleDescriptor);
    moduleService.loadModule(geodeModuleDescriptor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdown() {}

  /**
   * {@inheritDoc}
   */
  @Override
  public ModuleService getModuleService() {
    return moduleService;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ServiceResult<Boolean> bootStrapModule(
      ComponentIdentifier componentIdentifier) {
    Optional<String> path = componentIdentifier.getPath();
    if (path.isPresent() && !StringUtils.isBlank(path.get())) {
      return processComponentIdentifier(componentIdentifier);
    } else {
      return Failure
          .of("Invalid path for the component: " + componentIdentifier.getComponentName());
    }
  }

  /**
   * Processes and registers all modules required by the given {@link JarFile} and adds them to the
   * {@link Set} of registered modules.
   *
   * @param jarFile the {@link JarFile} to process the dependent modules of.
   * @param registeredModules a {@link Set} of the names of all the currently registered modules.
   * @return {@link Success} containing an updated {@link Set} of all registered modules.
   */
  private ServiceResult<Set<String>> processDependentModulesFromManifestFile(
      JarFile jarFile, Set<String> registeredModules) {

    // Get list of dependent modules names from manifest file
    ServiceResult<String[]> dependentModulesFromJarFile =
        getDependentModulesFromJarFile(jarFile);

    if (dependentModulesFromJarFile.isSuccessful()) {
      for (String moduleName : dependentModulesFromJarFile.getMessage()) {

        ServiceResult<String> jarPathLookupResult =
            lookupOrResolvePathForModuleName(moduleName, jarFile);

        if (jarPathLookupResult.isSuccessful()) {
          ServiceResult<Boolean> registerModuleResult =
              registerModule(registeredModules, moduleName, jarPathLookupResult);

          if (registerModuleResult.isFailure()) {
            return Failure.of(registerModuleResult.getErrorMessage());
          }
          ServiceResult<Set<String>> processDependentModule =
              processDependentModule(jarPathLookupResult.getMessage(), registeredModules);
          if (processDependentModule.isFailure()) {
            return Failure.of(processDependentModule.getErrorMessage());
          }
        } else {
          return Failure.of(jarPathLookupResult.getErrorMessage());
        }
      }
    } else {
      return Failure.of(dependentModulesFromJarFile.getErrorMessage());
    }
    return Success.of(registeredModules);
  }

  /**
   * Creates a {@link ModuleDescriptor} for the module represented by the given module name,
   * registers it, and adds it to the {@link Set} of registered modules.
   *
   * @param registeredModules a {@link Set} of all currently registered modules.
   * @param moduleName the name of the module to be registered.
   * @param jarPathLookupResult a {@link ServiceResult} containing the resource paths for the
   *        module to be registered.
   * @return {@link Success} upon registering the module and {@link Failure} on failure.
   */
  private ServiceResult<Boolean> registerModule(Set<String> registeredModules,
      String moduleName, ServiceResult<String> jarPathLookupResult) {
    ModuleDescriptor moduleDescriptor = new ModuleDescriptor.Builder(moduleName)
        .fromResourcePaths(jarPathLookupResult.getMessage())
        .dependsOnModules(geodeModuleDescriptorBuilder.getName())
        .build();
    ServiceResult<Boolean> registerModuleResult =
        moduleService.registerModule(moduleDescriptor);
    if (registerModuleResult.isFailure()) {
      return registerModuleResult;
    }
    registerModuleResult
        .ifSuccessful(aBoolean -> registeredModules.add(moduleDescriptor.getName()));
    return SUCCESS_TRUE;
  }

  /**
   * Gets the dependent modules of a given {@link JarFile} from the "Dependent-Modules" attribute in
   * the {@link Manifest} file in the {@link JarFile}.
   *
   * @param jarFile the {@link JarFile} to find dependent modules for.
   * @return {@link Success} containing a {@link String[]} of names of the dependent modules for the
   *         given {@link JarFile} or {@link Failure} if the {@link Manifest} cannot be processed.
   */
  private ServiceResult<String[]> getDependentModulesFromJarFile(JarFile jarFile) {
    Optional<Manifest> manifest = null;
    try {
      manifest = Optional.ofNullable(jarFile.getManifest());
    } catch (IOException e) {
      return Failure.of(e);
    }
    String attributesValue = null;
    if (manifest.isPresent()) {
      Attributes attributes = manifest.get().getMainAttributes();
      attributesValue = attributes.getValue("Dependent-Modules");
    }

    return Success.of(!StringUtils.isBlank(attributesValue)
        ? attributesValue.split(" ")
        : EMPTY_STRING_ARRAY);
  }

  /**
   * Processes the dependent modules of a {@link JarFile} represented by the given path.
   *
   * @param path the path to the {@link JarFile} to be processed.
   * @param registeredModules a {@link Set} of all currently registered modules.
   * @return {@link Success} containing an updated {@link Set} of registered modules or
   *         {@link Failure} on failure.
   */
  private ServiceResult<Set<String>> processDependentModule(
      String path, Set<String> registeredModules) {
    try {
      return processDependentModulesFromManifestFile(
          new JarFile(path), registeredModules);
    } catch (IOException e) {
      return Failure.of(e);
    }
  }

  /**
   * Finds the path to a {@link JarFile} for the given module using the provided {@link JarFile} to
   * determine the containing directory.
   *
   * @param moduleName the name of the module to find the path for.
   * @param jarFile a {@link JarFile} used to find the directory portion of the path.
   * @return {@link Success} containing the path to the {@link JarFile} for the specified module or
   *         {@link Failure} on failure.
   */
  private ServiceResult<String> lookupOrResolvePathForModuleName(String moduleName,
      JarFile jarFile) {
    String name = jarFile.getName();
    String path;
    try {
      path = name.substring(0, name.lastIndexOf(File.separator) + 1);
    } catch (Exception e) {
      return Failure.of(e);
    }
    return Success.of(path + moduleName + ".jar");
  }

  /**
   * Resolves a {@link JarFile} from the provided path, registers and loads the "Dependent-Modules"
   * from the {@link Manifest} file and then registers and loads the module for the
   * provided {@link ComponentIdentifier}. After that, the linking module is updated with the newly
   * registered modules and relinked.
   *
   * @param componentIdentifier the {@link ComponentIdentifier} to process.
   * @return {@link Success} if the {@link ComponentIdentifier} was successfully processed, and
   *         {@link Failure} on failure.
   */
  private ServiceResult<Boolean> processComponentIdentifier(
      ComponentIdentifier componentIdentifier) {

    JarFile jarFile;
    try {
      jarFile = new JarFile(componentIdentifier.getPath().get());
    } catch (IOException e) {
      return Failure.of(e);
    }

    ServiceResult<Set<String>> loadedDependentModuleNamesResult =
        processDependentModulesFromManifestFile(jarFile, new TreeSet<>());

    if (loadedDependentModuleNamesResult.isSuccessful()) {
      Set<String> loadedDependentModuleNames = loadedDependentModuleNamesResult.getMessage();
      registerAndLoadModuleForComponent(componentIdentifier, jarFile).ifSuccessful(success -> {
        loadedDependentModuleNames.add(componentIdentifier.getComponentName());
        updateGeodeModule(loadedDependentModuleNames).ifFailure(errorMessage -> {
          throw new IllegalStateException(errorMessage);
        });
      });

      loadedDependentModuleNames.forEach(
          moduleName -> moduleService.loadModule(new ModuleDescriptor.Builder(moduleName).build()));
      return SUCCESS_TRUE;
    } else {
      return Failure.of(loadedDependentModuleNamesResult.getErrorMessage());
    }
  }

  /**
   * Registers and loads a module for the provided {@link ComponentIdentifier} and {@link JarFile}.
   *
   * @param componentIdentifier the {@link ComponentIdentifier} containing the name of the module to
   *        be registered and loaded.
   * @param jarFile the {@link JarFile} to be used as the resource for the module being registered
   *        and loaded.
   * @return {@link Success} when the module is registered and loaded and {@link Failure} on
   *         failure.
   */
  private ServiceResult<Boolean> registerAndLoadModuleForComponent(
      ComponentIdentifier componentIdentifier, JarFile jarFile) {
    ModuleDescriptor moduleDescriptor =
        new ModuleDescriptor.Builder(componentIdentifier.getComponentName())
            .fromResourcePaths(jarFile.getName())
            .dependsOnModules(geodeModuleDescriptorBuilder.getName())
            .build();

    ServiceResult<Boolean> registerModuleResult =
        moduleService.registerModule(moduleDescriptor);
    if (registerModuleResult.isFailure()) {
      return registerModuleResult;
    }

    return moduleService.loadModule(moduleDescriptor);
  }

  /**
   * Unlinks and re-links the geode linking module. First it unloads the module, then unregisters
   * the
   * module, then re-registers the module with updated dependencies, and then reloads the module.
   * This is a critical process and failure could force the system to be restarted.
   *
   * @param newlyRegisteredModules the updated list of modules to be linked against.
   * @return {@link Success} when the module is successfully updated and {@link Failure} on failure.
   */
  private ServiceResult<Boolean> updateGeodeModule(Set<String> newlyRegisteredModules) {
    geodeModuleDescriptorBuilder.dependsOnModules(newlyRegisteredModules);
    ModuleDescriptor geodeModuleDescriptor = geodeModuleDescriptorBuilder.build();
    return moduleService.unloadModule(geodeModuleDescriptorBuilder.getName())
        .ifSuccessful(aBoolean -> moduleService.unregisterModule(geodeModuleDescriptor)
            .ifSuccessful(aBoolean1 -> moduleService.registerModule(geodeModuleDescriptor)
                .ifSuccessful(aBoolean2 -> moduleService.loadModule(geodeModuleDescriptor)
                    .ifFailure(logger::warn))
                .ifFailure(logger::warn))
            .ifFailure(logger::warn))
        .ifFailure(logger::warn);
  }
}
