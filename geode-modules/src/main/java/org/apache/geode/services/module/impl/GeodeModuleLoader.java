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

import org.jboss.modules.DelegatingModuleLoader;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;

import org.apache.geode.annotations.Experimental;

/**
 * {@link ModuleLoader} for use by {@link JBossModuleService}.
 */
@Experimental
public class GeodeModuleLoader extends DelegatingModuleLoader {
  private Map<String, ModuleSpec> moduleSpecs = new HashMap<>();

  public GeodeModuleLoader() {
    super(Module.getSystemModuleLoader(), ModuleLoader.NO_FINDERS);
  }

  public void addModuleSpec(ModuleSpec moduleSpec) {
    moduleSpecs.put(moduleSpec.getName(), moduleSpec);
  }

  @Override
  protected ModuleSpec findModule(String name) throws ModuleLoadException {
    ModuleSpec moduleSpec = moduleSpecs.get(name);
    if (moduleSpec == null) {
      throw new ModuleLoadException(
          String.format("ModuleSpec for Module %s could not be found", name));
    }
    return moduleSpecs.get(name);
  }
}
