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
package org.apache.geode.deployment.internal.modules.loader;

import java.util.Arrays;
import java.util.List;

import org.jboss.modules.AliasModuleSpec;
import org.jboss.modules.ConcreteModuleSpec;
import org.jboss.modules.DelegatingModuleLoader;
import org.jboss.modules.JDKModuleFinder;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;
import org.jboss.modules.filter.MultiplePathFilterBuilder;
import org.jboss.modules.filter.PathFilter;
import org.jboss.modules.filter.PathFilters;

import org.apache.geode.deployment.internal.modules.extensions.impl.Application;
import org.apache.geode.deployment.internal.modules.extensions.impl.GeodeExtension;
import org.apache.geode.deployment.internal.modules.finder.GeodeApplicationModuleFinder;
import org.apache.geode.deployment.internal.modules.finder.GeodeCompositeModuleFinder;
import org.apache.geode.deployment.internal.modules.finder.GeodeDelegatingLocalModuleFinder;
import org.apache.geode.deployment.internal.modules.finder.GeodeJarModuleFinder;

/**
 * This {@link ModuleLoader} will be used to bootstrap the JBoss system when the server starts. It
 * allows us access to the internals of JBoss so we can load and unload modules at runtime.
 */
public class GeodeModuleLoader extends DelegatingModuleLoader implements AutoCloseable {

  private static final String GEODE_BASE_PACKAGE_PATH = "org/apache/geode";
  private final GeodeCompositeModuleFinder compositeModuleFinder;
  private static final ModuleLoader JDK_MODULE_LOADER =
      new ModuleLoader(JDKModuleFinder.getInstance());
  private static final String CORE_MODULE_NAME = "geode-core";

  public GeodeModuleLoader(GeodeCompositeModuleFinder compositeModuleFinder) {
    super(JDK_MODULE_LOADER, new ModuleFinder[] {compositeModuleFinder});
    compositeModuleFinder.addModuleFinder("__default__",
        new GeodeDelegatingLocalModuleFinder());
    this.compositeModuleFinder = compositeModuleFinder;
  }

  public GeodeModuleLoader() {
    this(new GeodeCompositeModuleFinder());
  }

  public void registerModule(String moduleName, String path,
      List<String> moduleDependencyNames) throws ModuleLoadException {
    validate(moduleName);
    if (findLoadedModuleLocal(moduleName) != null) {
      unregisterModule(moduleName);
    }
    compositeModuleFinder.addModuleFinder(moduleName,
        new GeodeJarModuleFinder(moduleName, path, moduleDependencyNames));
  }

  public void unregisterModule(String moduleName) throws ModuleLoadException {
    validate(moduleName);
    unregisterModuleDependencyFromModules(moduleName);
    compositeModuleFinder.removeModuleFinder(moduleName);
    Module loadedModuleLocal = findLoadedModuleLocal(moduleName);
    if (loadedModuleLocal != null) {
      unloadModuleLocal(moduleName, loadedModuleLocal);
    }
    relinkModule(CORE_MODULE_NAME);
  }

  private void validate(String moduleName) {
    if (moduleName == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
    // if (moduleName.startsWith("geode-")) {
    // throw new RuntimeException("Deployment: "+moduleName+" starting with \"geode-\" are not
    // allowed.");
    // }
  }

  @Override
  protected Module preloadModule(String name) throws ModuleLoadException {
    if (name == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
    if (name.contains(CORE_MODULE_NAME) && findLoadedModuleLocal(name) == null) {
      Module coreModule = super.preloadModule(name);
      unloadModuleLocal(name, coreModule);
    }
    return super.preloadModule(name);
  }

  @Override
  public void close() throws Exception {

  }

  public boolean registerApplication(Application application) {
    try {
      if (findLoadedModuleLocal(application.getName()) == null) {
        compositeModuleFinder.addModuleFinder(application.getName(),
            new GeodeApplicationModuleFinder(application));
      }
      registerModulesAsDependencyOfModule(CORE_MODULE_NAME, application.getName());
    } catch (ModuleLoadException e) {
      return false;
    }
    return true;
  }

  public boolean registerGeodeExtension(GeodeExtension extension) {
    try {
      if (findLoadedModuleLocal(extension.getName()) != null) {
        unregisterModule(extension.getName());
      }
      preloadModule(extension.getName());
      // compositeModuleFinder.addModuleFinder(extension.getName(),
      // new LocalModuleFinder(
      // GeodeExtensionModuleFinder(extension));
      registerModulesAsDependencyOfModule(CORE_MODULE_NAME, extension.getName());
    } catch (ModuleLoadException e) {
      return false;
    }
    return true;
  }

  public void registerModulesAsDependencyOfModule(String moduleName, String... modulesToDependOn)
      throws ModuleLoadException {
    compositeModuleFinder.addDependencyToModule(moduleName, createDefaultExportFilter(),
        modulesToDependOn);
    relinkModule(moduleName);
  }

  public void registerModulesAsDependencyOfModule(String moduleName, PathFilter exportFilter,
      String... modulesToDependOn)
      throws ModuleLoadException {
    compositeModuleFinder.addDependencyToModule(moduleName, exportFilter, modulesToDependOn);
    relinkModule(moduleName);
  }

  public void relinkModule(String moduleName) throws ModuleLoadException {
    ModuleSpec moduleSpec = findModule(moduleName);
    if (moduleSpec instanceof AliasModuleSpec) {
      AliasModuleSpec aliasModuleSpec = (AliasModuleSpec) moduleSpec;
      relinkModule(aliasModuleSpec.getAliasName());
    } else {
      setAndRelinkDependencies(findLoadedModuleLocal(moduleName),
          Arrays.asList(((ConcreteModuleSpec) moduleSpec).getDependencies()));
    }
  }

  public void unregisterModuleDependencyFromModules(String moduleDependencyToRemove)
      throws ModuleLoadException {
    List<String> modulesToRelink =
        compositeModuleFinder.removeDependencyFromModules(moduleDependencyToRemove);
    for (String moduleName : modulesToRelink) {
      relinkModule(moduleName);
    }
  }

  private PathFilter createDefaultExportFilter() {
    final MultiplePathFilterBuilder exportBuilder = PathFilters.multiplePathFilterBuilder(true);
    exportBuilder.addFilter(PathFilters.getMetaInfServicesFilter(), true);
    exportBuilder.addFilter(PathFilters.getMetaInfSubdirectoriesFilter(), true);
    exportBuilder.addFilter(PathFilters.getMetaInfFilter(), true);
    return exportBuilder.create();
  }
}
