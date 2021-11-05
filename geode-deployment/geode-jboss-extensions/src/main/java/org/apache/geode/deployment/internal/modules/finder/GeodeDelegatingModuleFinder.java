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
package org.apache.geode.deployment.internal.modules.finder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.modules.AliasModuleSpec;
import org.jboss.modules.ConcreteModuleSpec;
import org.jboss.modules.DependencySpec;
import org.jboss.modules.GeodeLocalModuleFinderExt;
import org.jboss.modules.ModuleDependencySpec;
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;
import org.jboss.modules.ModuleSpecUtils;

/**
 * This {@link ModuleFinder} will delegate to a local {@link ModuleFinder} and hold
 * {@link ModuleSpec}s,
 * for previously loaded modules.
 *
 * @since Geode 1.16
 */
public class GeodeDelegatingModuleFinder implements ModuleFinder {
  private final ModuleFinder delegatingLocalModuleFinder = new GeodeLocalModuleFinderExt();
  private final Map<String, ModuleSpec> moduleSpecs = new ConcurrentHashMap<>();

  @Override
  public ModuleSpec findModule(String name, ModuleLoader delegateLoader)
      throws ModuleLoadException {
    ModuleSpec moduleSpec = moduleSpecs.get(name);
    if (moduleSpec == null) {
      moduleSpec = delegatingLocalModuleFinder.findModule(name, delegateLoader);
      if (moduleSpec != null) {
        moduleSpec = ModuleSpecUtils.expandClasspath(moduleSpec);
        addModuleSpec(moduleSpec);
        return ModuleSpecUtils.addSystemClasspathDependency(moduleSpec);
      }
    }
    return moduleSpec;
  }

  /**
   * Remove a previously added {@link ModuleFinder}.
   *
   * @param moduleName name used when adding the {@link ModuleFinder} to be removed.
   */
  public void removeModuleSpec(String moduleName) {
    if (moduleName != null) {
      moduleSpecs.remove(moduleName);
    }
  }

  /**
   * Add a module dependency on module represented by moduleToDependOn to the module represented by
   * moduleName. The module you are trying to add the dependency to must have been loaded by
   * findModule before adding a dependency to it.
   *
   * @param moduleName the module to add a dependency to.
   * @param moduleToDependOn the module to add the dependency on.
   */
  public void addDependencyToModule(String moduleName, String moduleToDependOn, boolean export) {
    if (moduleToDependOn == null) {
      throw new IllegalArgumentException("Module to depend on cannot be null");
    }
    ModuleSpec moduleSpec = getConcreteModuleSpec(moduleName);
    if (moduleSpec != null) {
      moduleSpec = ModuleSpecUtils.addModuleDependencyToSpec(moduleSpec, export, moduleToDependOn);
      addModuleSpec(moduleSpec);
    } else {
      throw new IllegalArgumentException("No such module: " + moduleName);
    }
  }

  /**
   * Remove the dependency on moduleDependencyToRemove from all modules that depend on it.
   *
   * @param moduleDependencyToRemove name of the module that will be removed as a dependency from
   *        all modules that depend on it.
   * @return a {@link List} of names of all the modules that had depended on
   *         moduleDependencyToRemove.
   */
  public List<String> removeDependencyFromModules(String moduleDependencyToRemove) {
    if (moduleDependencyToRemove == null) {
      throw new IllegalArgumentException("Module dependency name cannot be null");
    }
    List<String> modulesThatDependOn = findModulesThatDependOn(moduleDependencyToRemove);
    for (String moduleName : modulesThatDependOn) {
      ModuleSpec moduleSpec = getConcreteModuleSpec(moduleName);
      if (moduleSpec != null) {
        moduleSpec = ModuleSpecUtils.removeDependencyFromSpec(moduleSpec, moduleDependencyToRemove);
        addModuleSpec(moduleSpec);
      }
    }
    return modulesThatDependOn;
  }

  private ModuleSpec getConcreteModuleSpec(String moduleName) {
    if (moduleName == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
    ModuleSpec moduleSpec = moduleSpecs.get(moduleName);
    if (moduleSpec instanceof AliasModuleSpec) {
      AliasModuleSpec aliasSpec = (AliasModuleSpec) moduleSpec;
      return getConcreteModuleSpec(aliasSpec.getAliasName());
    } else {
      return moduleSpec;
    }
  }

  public List<String> findModulesThatDependOn(String moduleName) {
    return findModulesThatDependOn(moduleName, new HashMap<>(moduleSpecs));
  }

  private List<String> findModulesThatDependOn(String moduleName,
      Map<String, ModuleSpec> moduleSpecsToCheck) {
    moduleSpecsToCheck.remove(moduleName);
    HashMap<String, ModuleSpec> modulesToCheckClone = new HashMap<>(moduleSpecsToCheck);
    List<String> dependentModuleNames = new LinkedList<>();
    Set<String> subModuleDependencies = new HashSet<>();
    for (Map.Entry<String, ModuleSpec> entry : moduleSpecsToCheck.entrySet()) {
      modulesToCheckClone.remove(entry.getKey());
      Boolean dependentsToExport =
          ModuleSpecUtils.moduleExportsModuleDependency(entry.getValue(), moduleName);
      if (dependentsToExport != null) {
        dependentModuleNames.add(entry.getKey());
        if (dependentsToExport) {
          subModuleDependencies
              .addAll(findModulesThatDependOn(entry.getKey(), modulesToCheckClone));
        }
      }
    }
    dependentModuleNames.addAll(subModuleDependencies);
    return dependentModuleNames;
  }

  public List<String> findInboundDependenciesForModule(String moduleName) {
    List<String> inboundModuleDependencies = new LinkedList<>();
    for (ModuleSpec moduleSpec : moduleSpecs.values()) {
      if (moduleSpec instanceof ConcreteModuleSpec) {
        ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpec;
        for (DependencySpec dependencySpec : concreteModuleSpec.getDependencies()) {
          if (dependencySpec instanceof ModuleDependencySpec) {
            ModuleDependencySpec spec = (ModuleDependencySpec) dependencySpec;
            if (spec.getName().equals(moduleName)) {
              inboundModuleDependencies.add(concreteModuleSpec.getName());
            }
          }
        }
      }
    }
    return inboundModuleDependencies;
  }

  public void addModuleSpec(ModuleSpec moduleSpec) {
    if (moduleSpec != null) {
      moduleSpecs.put(moduleSpec.getName(), moduleSpec);
    } else {
      throw new RuntimeException("ModuleSpec cannot be null");
    }
  }
}
