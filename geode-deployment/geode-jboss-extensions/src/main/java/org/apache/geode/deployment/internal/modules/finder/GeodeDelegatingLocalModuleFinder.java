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

import org.jboss.modules.LocalModuleFinder;
import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;
import org.jboss.modules.ModuleSpecUtils;

/**
 * This {@link ModuleLoader} is responsible for loading the Geode modules from module.xml files. It
 * delegates to another {@link ModuleFinder}, adding a dependency on the classpath before returning
 * the spec. This classes exists because {@link LocalModuleFinder} does not allow modules to access
 * the classpath and cannot be extended to do so because it is final.
 */
public class GeodeDelegatingLocalModuleFinder implements ModuleFinder {

  protected final ModuleFinder moduleFinder;

  public GeodeDelegatingLocalModuleFinder() {
    this(new LocalModuleFinder());
  }

  protected GeodeDelegatingLocalModuleFinder(ModuleFinder moduleFinder) {
    if (moduleFinder == null) {
      throw new IllegalArgumentException("ModuleFinder cannot be null");
    }
    this.moduleFinder = moduleFinder;
  }

  @Override
  public ModuleSpec findModule(String name, ModuleLoader delegateLoader)
      throws ModuleLoadException {
    ModuleSpec moduleSpec = moduleFinder.findModule(name, delegateLoader);
    return ModuleSpecUtils.addSystemClasspathDependency(moduleSpec);
  }
}
