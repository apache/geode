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
package org.jboss.modules;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jboss.modules.filter.MultiplePathFilterBuilder;
import org.jboss.modules.filter.PathFilter;
import org.jboss.modules.filter.PathFilters;

/**
 * A set of utilities that simplify working with {@link ModuleSpec}s.
 */
public class ModuleSpecUtil {

  /**
   * Creates a {@link ModuleSpec.Builder} given for a module of the given name.
   *
   * @param name name of the module to create a builder for.
   * @param addJavaBaseDeps if true, the module will depend on java, if false, it will not.
   * @return a {@link ModuleSpec.Builder} based on the passed arguments.
   */
  public static ModuleSpec.Builder createBuilder(String name, boolean addJavaBaseDeps) {
    return ModuleSpec.build(name, addJavaBaseDeps);
  }

  /**
   * Removes the given dependencies from the given {@link ModuleSpec}
   *
   * @param moduleSpec the {@link ModuleSpec} to remove dependencies from.
   * @param dependenciesToRemove names of dependencies to remove.
   * @return a {@link ModuleSpec} which is a copy of moduleSpec, minus the specified dependencies.
   */
  public static ModuleSpec removeDependencyFromSpec(ModuleSpec moduleSpec,
      String... dependenciesToRemove) {
    validate(moduleSpec);
    if (dependenciesToRemove == null || dependenciesToRemove.length == 0) {
      return moduleSpec;
    }
    ModuleSpec.Builder builder =
        createBuilderAndRemoveDependencies(moduleSpec, dependenciesToRemove);
    return builder.create();
  }

  /**
   * Adds a dependency on the system classpath allowing the module to access classes from the
   * classpath.
   * If moduleSpec is null, null will be returned.
   *
   * @param moduleSpec the {@link ModuleSpec} to add a classpath dependency to.
   * @return a {@link ModuleSpec} which a classpath dependency.
   */
  public static ModuleSpec addSystemClasspathDependency(ModuleSpec moduleSpec) {
    if (moduleSpec instanceof ConcreteModuleSpec) {
      return createBuilder(moduleSpec).addDependency(new LocalDependencySpecBuilder()
          .setImportFilter(PathFilters.acceptAll())
          .setExport(true)
          .setLocalLoader(ClassLoaderLocalLoader.SYSTEM)
          .setLoaderPaths(JDKPaths.JDK)
          .build())
          .create();
    }
    return moduleSpec;
  }

  private static ModuleSpec.Builder createBuilder(ModuleSpec moduleSpec) {
    return createBuilderAndRemoveDependencies(moduleSpec);
  }

  private static ModuleSpec.Builder createBuilderAndRemoveDependencies(ModuleSpec moduleSpec,
      String... dependenciesToRemove) {
    List<String> listOfDependenciesToRemove = Arrays.asList(dependenciesToRemove);
    ModuleSpec.Builder builder = ModuleSpec.build(moduleSpec.getName(), false);
    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpec;

    for (ResourceLoaderSpec resourceLoader : concreteModuleSpec.getResourceLoaders()) {
      builder.addResourceRoot(resourceLoader);
    }

    Map<String, DependencySpec> dependencies = new HashMap<>();
    for (DependencySpec dependency : concreteModuleSpec.getDependencies()) {
      boolean addDependency = true;
      String name = dependency.toString();
      if (dependency instanceof ModuleDependencySpec) {
        ModuleDependencySpec moduleDependencySpec = (ModuleDependencySpec) dependency;
        name = moduleDependencySpec.getName();
        if (listOfDependenciesToRemove.contains(name)) {
          addDependency = false;
        }
      }
      if (addDependency) {
        dependencies.put(name, dependency);
      }
    }
    dependencies.forEach((k, v) -> builder.addDependency(v));

    builder.setMainClass(concreteModuleSpec.getMainClass());
    builder.setAssertionSetting(concreteModuleSpec.getAssertionSetting());
    builder.setFallbackLoader(concreteModuleSpec.getFallbackLoader());
    builder.setModuleClassLoaderFactory(concreteModuleSpec.getModuleClassLoaderFactory());
    builder.setClassFileTransformer(concreteModuleSpec.getClassFileTransformer());
    builder.setPermissionCollection(concreteModuleSpec.getPermissionCollection());
    builder.setVersion(concreteModuleSpec.getVersion());
    concreteModuleSpec.getProperties().forEach(builder::addProperty);

    return builder;
  }

  /**
   * Adds a dependency on dependencyName to the given {@link ModuleSpec}
   *
   * @param moduleSpec the {@link ModuleSpec} to add the dependency to.
   * @param dependencyName the name of the module to depend on.
   * @return a {@link ModuleSpec} that is dependent on the specified module.
   */
  public static ModuleSpec addModuleDependencyToSpec(ModuleSpec moduleSpec, String dependencyName) {
    validate(moduleSpec);

    final MultiplePathFilterBuilder exportBuilder = PathFilters.multiplePathFilterBuilder(true);
    exportBuilder.addFilter(PathFilters.getMetaInfServicesFilter(), true);
    exportBuilder.addFilter(PathFilters.getMetaInfSubdirectoriesFilter(), true);
    exportBuilder.addFilter(PathFilters.getMetaInfFilter(), true);

    ModuleSpec.Builder builder = createBuilder(moduleSpec);
    builder.addDependency(new ModuleDependencySpecBuilder()
        .setName(dependencyName)
        .setImportFilter(PathFilters.getDefaultImportFilterWithServices())
        .setExportFilter(exportBuilder.create())
        .build());

    return builder.create();
  }

  /**
   * Excludes the given paths from being imported from the given module.
   *
   * @param moduleSpec the {@link ModuleSpec} to add the filter to.
   * @param pathsToExclude paths to be excluded from the given module.
   * @param pathsToExcludeChildrenOf paths whose children should be excluded from the given module.
   * @param moduleToPutExcludeOn name of the module to exclude the paths from.
   * @return a {@link ModuleSpec} which will exclude the given paths from the given module.
   */
  public static ModuleSpec addExcludeFilter(ModuleSpec moduleSpec, List<String> pathsToExclude,
      List<String> pathsToExcludeChildrenOf,
      String moduleToPutExcludeOn) {
    validate(moduleSpec);
    if (moduleToPutExcludeOn == null) {
      throw new IllegalArgumentException("Module to exclude from cannot be null");
    }

    ModuleSpec.Builder builder =
        createBuilderAndRemoveDependencies(moduleSpec, moduleToPutExcludeOn);

    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpec;
    Optional<ModuleDependencySpec> dependencySpecOptional =
        Arrays.stream(concreteModuleSpec.getDependencies()).map(it -> (ModuleDependencySpec) it)
            .filter(it -> it.getName().equals(moduleToPutExcludeOn)).findFirst();
    if (!dependencySpecOptional.isPresent()) {
      return moduleSpec;
    }

    MultiplePathFilterBuilder pathFilterBuilder = PathFilters.multiplePathFilterBuilder(true);

    if (pathsToExclude != null) {
      for (String path : pathsToExclude) {
        pathFilterBuilder.addFilter(PathFilters.is(path), false);
      }
    }

    if (pathsToExcludeChildrenOf != null) {
      for (String path : pathsToExcludeChildrenOf) {
        pathFilterBuilder.addFilter(PathFilters.isOrIsChildOf(path), false);
      }
    }

    builder.addDependency(new ModuleDependencySpecBuilder()
        .setName(moduleToPutExcludeOn)
        .setImportFilter(pathFilterBuilder.create())
        .setExportFilter(pathFilterBuilder.create())
        .build());

    return builder.create();
  }

  /**
   * Checks if the moduleSpec is dependent on the module represented by moduleName.
   * <p>
   *
   * @return a {@link Boolean} which is null if the ModuleSpec does does not depend on the module,
   *         true if it does depend on the module and the module is exported, and false if the
   *         module is not
   *         exported.
   */
  public static Boolean moduleExportsModuleDependency(ModuleSpec moduleSpec, String moduleName) {
    validate(moduleSpec);
    if (moduleSpec instanceof ConcreteModuleSpec) {
      ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpec;
      for (DependencySpec dependencySpec : concreteModuleSpec.getDependencies()) {
        if (dependencySpec instanceof ModuleDependencySpec) {
          ModuleDependencySpec spec = (ModuleDependencySpec) dependencySpec;
          if (spec.getName().equals(moduleName)) {
            PathFilter exportFilter = spec.getExportFilter();
            // We assume that anything besides a 'Reject' is possibly an export
            return !exportFilter.toString().equals("Reject");
          }
        }
      }
    }
    return null;
  }

  private static void validate(ModuleSpec moduleSpec) {
    if (moduleSpec == null) {
      throw new IllegalArgumentException("ModuleSpec cannot be null");
    }
  }
}
