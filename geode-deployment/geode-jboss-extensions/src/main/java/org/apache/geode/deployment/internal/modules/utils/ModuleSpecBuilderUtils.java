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
package org.apache.geode.deployment.internal.modules.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.jar.JarFile;

import org.jboss.modules.LocalDependencySpecBuilder;
import org.jboss.modules.ModuleDependencySpecBuilder;
import org.jboss.modules.ModuleLoadException;
import org.jboss.modules.ModuleSpec;
import org.jboss.modules.ResourceLoaderSpec;
import org.jboss.modules.ResourceLoaders;
import org.jboss.modules.filter.MultiplePathFilterBuilder;
import org.jboss.modules.filter.PathFilter;
import org.jboss.modules.filter.PathFilters;

/**
 * A set of utilities that simplify working with {@link ModuleSpec.Builder}s.
 */
public class ModuleSpecBuilderUtils {

  private static final PathFilter importServicesPathFilter = createImportFilterWithServices();

  public static ModuleSpec.Builder addModuleDependencies(ModuleSpec.Builder builder, boolean export,
      String... moduleDependencies) {
    return addModuleDependencies(builder, export, Arrays.asList(moduleDependencies));
  }

  public static ModuleSpec.Builder addModuleDependencies(ModuleSpec.Builder builder, boolean export,
      List<String> moduleDependencies) {
    if (builder != null && moduleDependencies != null) {
      for (String moduleDependency : new HashSet<>(moduleDependencies)) {
        if (moduleDependency != null) {
          builder.addDependency(new ModuleDependencySpecBuilder()
              .setName(moduleDependency)
              .setImportFilter(importServicesPathFilter)
              .setExport(export)
              .build());
        }
      }
    }
    return builder;
  }

  public static ModuleSpec.Builder addLocalDependencySpec(ModuleSpec.Builder builder) {
    if (builder != null) {
      builder.addDependency(new LocalDependencySpecBuilder()
          .setExport(true)
          .setImportFilter(importServicesPathFilter)
          .build());
    }
    return builder;
  }

  public static ModuleSpec.Builder addJarResourceToBuilder(ModuleSpec.Builder builder,
      String jarPath)
      throws ModuleLoadException {
    if (builder != null && jarPath != null) {
      try {
        builder.addResourceRoot(ResourceLoaderSpec
            .createResourceLoaderSpec(
                ResourceLoaders.createJarResourceLoader(new JarFile(jarPath))));
      } catch (IOException e) {
        throw new ModuleLoadException(e);
      }
    }

    return builder;
  }

  private static PathFilter createImportFilterWithServices() {
    MultiplePathFilterBuilder multiplePathFilterBuilder =
        PathFilters.multiplePathFilterBuilder(true);
    multiplePathFilterBuilder.addFilter(PathFilters.getMetaInfServicesFilter(), true);
    multiplePathFilterBuilder.addFilter(PathFilters.getMetaInfSubdirectoriesFilter(), true);
    multiplePathFilterBuilder.addFilter(PathFilters.getMetaInfFilter(), true);
    return multiplePathFilterBuilder.create();
  }
}
