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
import java.util.Set;
import java.util.stream.Collectors;

import org.jboss.modules.AliasModuleSpec;
import org.jboss.modules.ConcreteModuleSpec;
import org.jboss.modules.DelegatingModuleLoader;
import org.jboss.modules.JDKModuleFinder;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;

import org.apache.geode.deployment.internal.modules.finder.GeodeCompositeModuleFinder;
import org.apache.geode.deployment.internal.modules.finder.GeodeDelegatingLocalModuleFinder;
import org.apache.geode.deployment.internal.modules.finder.GeodeJarModuleFinder;

/**
 * This {@link ModuleLoader} will be used to bootstrap the JBoss system when the server starts. It
 * allows us access to the internals of JBoss so we can load and unload modules at runtime.
 */
public class GeodeModuleLoader extends DelegatingModuleLoader implements AutoCloseable {

  private final GeodeCompositeModuleFinder compositeModuleFinder;
  private static final ModuleLoader JDK_MODULE_LOADER =
      new ModuleLoader(JDKModuleFinder.getInstance());
  private static final String THIRD_PARTY_MODULE_NAME = "thirdParty";
  private static final String CUSTOM_JAR_DEPLOYMENT_MODULE_NAME = "geode-custom-jar-deployments";
  private static final String CORE_MODULE_NAME = "geode-core";
  private static final String GEODE_BASE_PACKAGE_PATH = "org/apache/geode";

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
    if (moduleName.startsWith("geode-")) {
      throw new RuntimeException("Deployments starting with \"geode-\" are not allowed.");
    }
  }


  @Override
  protected Module preloadModule(String name) throws ModuleLoadException {
    if (name == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
    if (name.contains(CORE_MODULE_NAME) && findLoadedModuleLocal(name) == null
        && findModule(THIRD_PARTY_MODULE_NAME) != null
        && findModule(CUSTOM_JAR_DEPLOYMENT_MODULE_NAME) != null) {
      Module coreModule = super.preloadModule(name);

      excludeThirdPartyPathsFromModule(name, CUSTOM_JAR_DEPLOYMENT_MODULE_NAME);

      unloadModuleLocal(name, coreModule);
    }
    return super.preloadModule(name);
  }

  @Override
  public void close() throws Exception {

  }

  public void registerModuleAsDependencyOfModule(String moduleName, String moduleToDependOn)
      throws ModuleLoadException {
    compositeModuleFinder.addDependencyToModule(moduleName, moduleToDependOn);
    relinkModule(moduleName);
    relinkModule(CORE_MODULE_NAME);
  }

  private void relinkModule(String moduleName) throws ModuleLoadException {
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

  private void excludeThirdPartyPathsFromModule(String moduleToPutExcludeFilterOn,
      String moduleToExcludeFrom)
      throws ModuleLoadException {
    Module thirdPartyModule = loadModule(THIRD_PARTY_MODULE_NAME);
    Set<String> exportedPaths = thirdPartyModule.getExportedPaths();
    List<String> restrictPaths =
        exportedPaths.stream().filter(packageName -> packageName.split("/").length <= 2)
            .collect(Collectors.toList());
    List<String> restrictPathsAndChildren =
        exportedPaths.stream().filter(packageName -> packageName.split("/").length == 3)
            .collect(Collectors.toList());

    restrictPathsAndChildren.add(GEODE_BASE_PACKAGE_PATH);

    compositeModuleFinder.addExcludeFilterToModule(moduleToPutExcludeFilterOn, moduleToExcludeFrom,
        restrictPaths, restrictPathsAndChildren);
  }
}
