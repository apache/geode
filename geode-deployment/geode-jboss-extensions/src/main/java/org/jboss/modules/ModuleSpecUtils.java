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


import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.jboss.modules.filter.PathFilter;
import org.jboss.modules.filter.PathFilters;

/**
 * A set of utilities that simplify working with {@link ModuleSpec}s.
 */
public class ModuleSpecUtils {

  /**
   * Adds resources deined by the Class-Path attribute of the manifest inside the modules resources.
   *
   * @param moduleSpec spec for the module to expand the classpath of.
   * @return a {@link ModuleSpec} with expanded classpath.
   */
  public static ModuleSpec expandClasspath(ModuleSpec moduleSpec) {
    if (!(moduleSpec instanceof ConcreteModuleSpec)) {
      return moduleSpec;
    }
    ModuleSpec.Builder builder = ModuleSpecUtils.createBuilder(moduleSpec);
    try {
      for (ResourceLoaderSpec resourceLoader : ((ConcreteModuleSpec) moduleSpec)
          .getResourceLoaders()) {
        URLConnection urlConnection = resourceLoader.getResourceLoader().getLocation()
            .toURL().openConnection();
        if (urlConnection instanceof JarURLConnection) {
          File file = new File(((JarURLConnection) urlConnection).getJarFileURL().toURI());
          JarFile jarFile = new JarFile(file);
          Manifest manifest = jarFile.getManifest();
          if (manifest != null) {
            Attributes mainAttributes = manifest.getMainAttributes();
            if (mainAttributes != null) {
              addClassPathDependencies(builder, file.toPath(), mainAttributes);
            }
          }
        }
      }
      return builder.create();
    } catch (IOException | URISyntaxException e) {
      e.printStackTrace();
    }
    return moduleSpec;
  }

  private static void addClassPathDependencies(final ModuleSpec.Builder builder,
      final Path path, final Attributes mainAttributes) {
    final String classPath = mainAttributes.getValue(Attributes.Name.CLASS_PATH);
    final String[] classPathEntries =
        classPath == null ? Utils.NO_STRINGS : classPath.split("\\s+");
    for (String entry : classPathEntries) {
      if (!entry.isEmpty()) {
        try {
          File resourceJar = path.resolveSibling(Paths.get(entry)).normalize().toFile();
          if (resourceJar.exists()) {
            builder.addResourceRoot(ResourceLoaderSpec
                .createResourceLoaderSpec(
                    ResourceLoaders.createJarResourceLoader(new JarFile(resourceJar))));
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

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
    dependencies.forEach((name, dependency) -> builder.addDependency(dependency));

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
   * @param dependencyNames the names of the modules to depend on.
   * @return a {@link ModuleSpec} that is dependent on the specified module.
   */
  public static ModuleSpec addModuleDependencyToSpec(ModuleSpec moduleSpec, PathFilter importFilter,
      PathFilter exportFilter, String... dependencyNames) {
    validate(moduleSpec);
    if (dependencyNames == null) {
      throw new IllegalArgumentException("Dependency names cannot be null");
    }

    ModuleSpec.Builder builder = createBuilder(moduleSpec);
    for (String dependencyName : dependencyNames) {
      builder.addDependency(new ModuleDependencySpecBuilder()
          .setName(dependencyName)
          .setImportFilter(importFilter)
          .setExportFilter(exportFilter)
          .build());
    }

    return builder.create();
  }

  /**
   * Excludes the given paths from being imported from the given module.
   *
   * @param moduleSpec the {@link ModuleSpec} to add the filter to.
   * @param pathFilter {@link PathFilter} representing the paths to exclude.
   * @param moduleToPutExcludeOn name of the module to exclude the paths from.
   * @return a {@link ModuleSpec} which will exclude the given paths from the given module.
   */
  public static ModuleSpec addExcludeFilter(ModuleSpec moduleSpec, String moduleToPutExcludeOn,
      PathFilter pathFilter) {
    validate(moduleSpec);
    if (moduleToPutExcludeOn == null) {
      throw new IllegalArgumentException("Module to exclude from cannot be null");
    }

    ConcreteModuleSpec concreteModuleSpec = (ConcreteModuleSpec) moduleSpec;
    Optional<ModuleDependencySpec> dependencySpecOptional =
        Arrays.stream(concreteModuleSpec.getDependencies())
            .filter(it -> it instanceof ModuleDependencySpec)
            .map(it -> (ModuleDependencySpec) it)
            .filter(it -> it.getName().equals(moduleToPutExcludeOn)).findFirst();
    if (!dependencySpecOptional.isPresent()) {
      return moduleSpec;
    }

    ModuleSpec.Builder builder =
        createBuilderAndRemoveDependencies(moduleSpec, moduleToPutExcludeOn);

    builder.addDependency(new ModuleDependencySpecBuilder()
        .setName(moduleToPutExcludeOn)
        .setImportFilter(pathFilter)
        .setExportFilter(pathFilter)
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
