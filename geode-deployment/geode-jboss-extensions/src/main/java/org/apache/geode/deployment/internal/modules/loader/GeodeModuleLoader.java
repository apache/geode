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
import org.jboss.modules.ModuleNotFoundException;
import org.jboss.modules.ModuleSpec;

import org.apache.geode.deployment.internal.modules.finder.GeodeDelegatingModuleFinder;

/**
 * This {@link ModuleLoader} will be used to bootstrap the JBoss system when the server starts. It
 * allows us access to the internals of JBoss, so we can load and unload modules at runtime.
 *
 * @since Geode 1.16
 */
public class GeodeModuleLoader extends DelegatingModuleLoader {

  private final GeodeDelegatingModuleFinder delegatingModuleFinder;
  private static final ModuleLoader JDK_MODULE_LOADER =
      new ModuleLoader(JDKModuleFinder.getInstance());
  private static final String CORE_MODULE_NAME = "geode";

  public GeodeModuleLoader(GeodeDelegatingModuleFinder delegatingModuleFinder) {
    super(JDK_MODULE_LOADER, new ModuleFinder[] {delegatingModuleFinder});
    this.delegatingModuleFinder = delegatingModuleFinder;
  }

  public GeodeModuleLoader() {
    this(new GeodeDelegatingModuleFinder());
  }

  public synchronized void unregisterModule(String moduleName) throws ModuleLoadException {
    validate(moduleName);

    List<String> inboundDependenciesForModule =
        delegatingModuleFinder.findInboundDependenciesForModule(moduleName);

    delegatingModuleFinder.removeModuleSpec(moduleName);

    unregisterModuleDependencyFromModules(moduleName);
    Module loadedModuleLocal = findLoadedModuleLocal(moduleName);
    if (loadedModuleLocal != null) {
      unloadModuleLocal(moduleName, loadedModuleLocal);
    }
    for (String inboundModuleName : inboundDependenciesForModule) {
      relinkModule(inboundModuleName);
    }
  }

  private void validate(String moduleName) {
    if (moduleName == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
  }

  public void moduleExists(String moduleName) throws ModuleLoadException {
    if (moduleName != null && preloadModule(moduleName) == null) {
      throw new ModuleNotFoundException(moduleName);
    }
  }

  @Override
  protected Module preloadModule(String name) throws ModuleLoadException {
    if (name == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
    return super.preloadModule(name);
  }

  public void linkModules(String moduleName, String moduleToDependOn, boolean export)
      throws ModuleLoadException {
    validate(moduleName, moduleToDependOn);

    delegatingModuleFinder.addDependencyToModule(moduleName, moduleToDependOn, export);
    relinkModule(moduleName);
    relinkModule(CORE_MODULE_NAME);
  }

  private void validate(String moduleName, String moduleToDependOn) throws ModuleLoadException {
    if (moduleName == null) {
      throw new IllegalArgumentException("Module name cannot be null");
    }
    if (moduleToDependOn == null) {
      throw new IllegalArgumentException("Module to depend on cannot be null");
    }

    moduleExists(moduleName);
    moduleExists(moduleToDependOn);
  }

  public void relinkModule(String moduleName) throws ModuleLoadException {
    ModuleSpec moduleSpec = findModule(moduleName);
    if (moduleSpec instanceof AliasModuleSpec) {
      AliasModuleSpec aliasModuleSpec = (AliasModuleSpec) moduleSpec;
      relinkModule(aliasModuleSpec.getAliasName());
    } else {
      Module loadedModuleLocal = findLoadedModuleLocal(moduleName);
      if (loadedModuleLocal != null) {
        setAndRelinkDependencies(loadedModuleLocal,
            Arrays.asList(((ConcreteModuleSpec) moduleSpec).getDependencies()));
      } else {
        throw new ModuleNotFoundException(moduleName);
      }
    }
  }

  public void unregisterModuleDependencyFromModules(String moduleDependencyToRemove)
      throws ModuleLoadException {
    List<String> modulesToRelink =
        delegatingModuleFinder.removeDependencyFromModules(moduleDependencyToRemove);
    for (String moduleName : modulesToRelink) {
      relinkModule(moduleName);
    }

    relinkModule(CORE_MODULE_NAME);
  }
}
