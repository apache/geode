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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.modules.AliasModuleSpec;
import org.jboss.modules.ConcreteModuleSpec;
import org.jboss.modules.DelegatingModuleLoader;
import org.jboss.modules.JDKModuleFinder;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;

import org.apache.geode.deployment.internal.modules.dependency.InboundModuleDependency;
import org.apache.geode.deployment.internal.modules.finder.GeodeCompositeModuleFinder;
import org.apache.geode.deployment.internal.modules.finder.GeodeJarModuleFinder;

/**
 * This {@link ModuleLoader} will be used to bootstrap the JBoss system when the server starts. It
 * allows us access to the internals of JBoss so we can load and unload modules at runtime.
 */
public class GeodeModuleLoader extends DelegatingModuleLoader implements AutoCloseable {

  private final GeodeCompositeModuleFinder compositeModuleFinder;
  private static final ModuleLoader JDK_MODULE_LOADER =
      new ModuleLoader(JDKModuleFinder.getInstance());
  private static final String CORE_MODULE_NAME = "geode";
  private final Map<String, List<InboundModuleDependency>> moduleDependencyInfoMap =
      new ConcurrentHashMap<>();

  public GeodeModuleLoader(GeodeCompositeModuleFinder compositeModuleFinder) {
    super(JDK_MODULE_LOADER, new ModuleFinder[] {compositeModuleFinder});
    this.compositeModuleFinder = compositeModuleFinder;
  }

  public GeodeModuleLoader() {
    this(new GeodeCompositeModuleFinder());
  }

  public void registerModule(String moduleName, String path,
      List<String> moduleDependencyNames) throws ModuleLoadException {
    validate(moduleName);
    Module module = findLoadedModuleLocal(moduleName);
    if (module != null) {
      unregisterModule(moduleName);
    }
    compositeModuleFinder.addModuleFinder(moduleName,
        new GeodeJarModuleFinder(moduleName, path, moduleDependencyNames));
    restoreDependencies(moduleName);
  }

  public void unregisterModule(String moduleName) throws ModuleLoadException {
    validate(moduleName);
    // store dependency info for later use
    moduleDependencyInfoMap.put(moduleName,
        compositeModuleFinder.findInboundDependenciesForModule(moduleName));

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
  }

  @Override
  protected Module preloadModule(String name) throws ModuleLoadException {
    if (name == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
    return super.preloadModule(name);
  }

  @Override
  public void close() throws Exception {

  }

  private void restoreDependencies(String moduleName) throws ModuleLoadException {
    List<InboundModuleDependency> inboundModuleDependencies =
        moduleDependencyInfoMap.get(moduleName);
    if (inboundModuleDependencies != null) {
      for (InboundModuleDependency inboundDependency : inboundModuleDependencies) {
        if (findLoadedModuleLocal(inboundDependency.getInboundDependencyModuleName()) != null) {
          registerModulesAsDependencyOfModule(inboundDependency.getInboundDependencyModuleName(),
              moduleName);
        }
      }
    }
  }

  public void registerModulesAsDependencyOfModule(String moduleName, String... modulesToDependOn)
      throws ModuleLoadException {
    if (modulesToDependOn == null) {
      throw new IllegalArgumentException("Modules to depend on cannot be null");
    }

    for (String moduleToDependOn : modulesToDependOn) {
      compositeModuleFinder.addDependencyToModule(moduleName, moduleToDependOn);
    }
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
}
