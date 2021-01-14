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
package org.apache.geode.modules.finder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;

public class GeodeCompositeModuleFinder implements ModuleFinder {

  private final Map<String, ModuleFinder> moduleFinders = new ConcurrentHashMap<>();
  private final Map<String, ModuleSpec> moduleSpecs = new ConcurrentHashMap<>();

  public GeodeCompositeModuleFinder(String name, ModuleFinder moduleFinder) {
    addModuleFinder(name, moduleFinder);
  }

  public void addModuleFinder(String name, ModuleFinder moduleFinder) {
    moduleFinders.put(name, moduleFinder);
  }

  public void removeModuleFinder(String moduleName) {

  }

  @Override
  public ModuleSpec findModule(String name, ModuleLoader delegateLoader)
      throws ModuleLoadException {
    ModuleSpec moduleSpec = moduleSpecs.get(name);
    if (moduleSpec == null) {
      for (ModuleFinder finder : moduleFinders.values()) {
        moduleSpec = finder.findModule(name, delegateLoader);
        if (moduleSpec != null) {
          addSpecToLinkingModule(moduleSpec);
          moduleSpecs.put(name, moduleSpec);
        }
      }
    }
    return moduleSpec;
  }

  private void addSpecToLinkingModule(ModuleSpec moduleSpec) {

  }
}
