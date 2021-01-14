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
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;
import org.jboss.modules.ModuleSpecUtil;

/**
 * This {@link ModuleFinder} will hold multiple other {@link ModuleFinder}s, mostly {@link
 * GeodeJarModuleFinder}, that it will delegate to to find modules. There should only be one
 * instance at a time.
 */
public class GeodeCompositeModuleFinder implements ModuleFinder {
  private final Map<String, ModuleFinder> moduleFinders = new ConcurrentHashMap<>();
  private final Map<String, ModuleSpec> moduleSpecs = new ConcurrentHashMap<>();

  /**
   * Adds a {@link ModuleFinder} to the composite to be delegated to.
   *
   * @param moduleName name of the module that cna be found by the {@link ModuleFinder}.
   * @param moduleFinder a {@link ModuleFinder} to be searched when looking for modules.
   */
  public synchronized void addModuleFinder(String moduleName, ModuleFinder moduleFinder) {
    if (moduleName == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
    if (moduleFinder == null) {
      throw new IllegalArgumentException("ModuleFinder cannot be null");
    }
    moduleFinders.put(moduleName, moduleFinder);
  }

  @Override
  public ModuleSpec findModule(String name, ModuleLoader delegateLoader)
      throws ModuleLoadException {
    ModuleSpec moduleSpec = moduleSpecs.get(name);
    if (moduleSpec == null) {
      for (ModuleFinder moduleFinder : moduleFinders.values()) {
        moduleSpec = moduleFinder.findModule(name, delegateLoader);
        if (moduleSpec != null) {
          moduleSpecs.put(name, moduleSpec);
          return moduleSpec;
        }
      }
    }
    return moduleSpec;
  }

  /**
   * Remove a previously added {@link ModuleFinder}.
   *
   * @param moduleName name used when adding the {@link ModuleFinder} to be removed.
   */
  public synchronized void removeModuleFinder(String moduleName) {
    if (moduleName != null) {
      moduleFinders.remove(moduleName);
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
  public void addDependencyToModule(String moduleName, String moduleToDependOn) {
    if (moduleToDependOn == null) {
      throw new IllegalArgumentException("Module to depend on cannot be null");
    }
    ModuleSpec moduleSpec = getConcreteModuleSpec(moduleName);
    if (moduleSpec != null) {
      moduleSpec = ModuleSpecUtil.addModuleDependencyToSpec(moduleSpec, moduleToDependOn);
      moduleSpecs.put(moduleSpec.getName(), moduleSpec);
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
        moduleSpec = ModuleSpecUtil.removeDependencyFromSpec(moduleSpec, moduleDependencyToRemove);
        moduleSpecs.put(moduleSpec.getName(), moduleSpec);
      }
    }
    return modulesThatDependOn;
  }

  /**
   * Excludes the given paths of a specified module dependency from a specified module.
   *
   * @param moduleToPutExcludeFilterOn the module that will have the filter put on its dependency.
   * @param moduleToExcludeFrom the module dependency to exclude things from.
   * @param restrictPaths paths to exclude from the moduleToExcludeFrom.
   * @param restrictPathsAndChildren paths to exclude the children of from the
   *        moduleToExcludeFrom.
   */
  public void addExcludeFilterToModule(String moduleToPutExcludeFilterOn,
      String moduleToExcludeFrom, List<String> restrictPaths,
      List<String> restrictPathsAndChildren) {
    ModuleSpec moduleSpec = getConcreteModuleSpec(moduleToPutExcludeFilterOn);
    if (moduleSpec != null) {
      moduleSpec = ModuleSpecUtil.addExcludeFilter(moduleSpec, restrictPaths,
          restrictPathsAndChildren, moduleToExcludeFrom);
      moduleSpecs.put(moduleSpec.getName(), moduleSpec);
    } else {
      throw new RuntimeException("No such module: " + moduleToPutExcludeFilterOn);
    }
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

  private List<String> findModulesThatDependOn(String moduleName) {
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
          ModuleSpecUtil.moduleExportsModuleDependency(entry.getValue(), moduleName);
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
}
