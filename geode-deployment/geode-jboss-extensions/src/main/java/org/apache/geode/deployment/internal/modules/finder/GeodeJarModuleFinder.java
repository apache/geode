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

import java.io.File;
import java.util.List;

import org.jboss.modules.ModuleFinder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleLoader;
import org.jboss.modules.ModuleSpec;
import org.jboss.modules.ModuleSpecUtils;

import org.apache.geode.deployment.internal.modules.utils.ModuleSpecBuilderUtils;

/**
 * A {@link ModuleFinder} used to find modules need by Geode. There wll be an instance of this class
 * for each modules loaded into Geode; this includes geode-core, geode-gfsh, geode-* as well as any
 * modules for jars deployed into Geode.
 */
public class GeodeJarModuleFinder implements ModuleFinder {
  private final String moduleName;
  private final String path;
  private final String[] moduleDependencies;

  public GeodeJarModuleFinder(String moduleName, String path,
      List<String> moduleDependencyNames) {
    this.moduleName = moduleName;
    this.path = path;
    if (moduleDependencyNames != null) {
      this.moduleDependencies = moduleDependencyNames.toArray(new String[] {});
    } else {
      this.moduleDependencies = new String[0];
    }
    validate();
  }

  private void validate() {
    if (moduleName == null) {
      throw new RuntimeException("Module name cannot be null");
    }

    if (path == null || !new File(path).exists()) {
      throw new RuntimeException("Unable to resolve path to jar: " + path);
    }
  }

  @Override
  public ModuleSpec findModule(String name, ModuleLoader delegateLoader)
      throws ModuleLoadException {
    if (name.equals(moduleName)) {
      ModuleSpec.Builder builder = ModuleSpecUtils.createBuilder(moduleName, true);
      builder = ModuleSpecBuilderUtils.addLocalDependencySpec(builder);
      builder = ModuleSpecBuilderUtils.addJarResourceToBuilder(builder, path);
      builder = ModuleSpecBuilderUtils.addModuleDependencies(builder, false, moduleDependencies);

      ModuleSpec moduleSpec = ModuleSpecUtils.addSystemClasspathDependency(builder.create());
      return moduleSpec;
    } else {
      return null;
    }
  }
}
