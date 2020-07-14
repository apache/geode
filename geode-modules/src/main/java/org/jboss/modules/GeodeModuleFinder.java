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

package org.jboss.modules;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.logging.log4j.Logger;
import org.jboss.modules.filter.PathFilters;

import org.apache.geode.services.module.ModuleDescriptor;
import org.apache.geode.services.module.internal.GeodeJDKPaths;
import org.apache.geode.util.internal.JarFileUtils;

/**
 * A custom implementation of a {@link ModuleFinder}. This implementation is based on {@link
 * JarModuleFinder} which creates a {@link ModuleSpec} from a {@link ModuleDescriptor} The {@link
 * ModuleDescriptor} defines the module name, a set of {@link JarFile}s and/or dependent module
 * names for Module.
 * <p>
 * This {@link ModuleFinder} has the ability to set both resource paths and dependent module names
 * from either the {@link ModuleDescriptor} or from a {@link Manifest} file within the {@link
 * JarFile}.
 * <p>
 * {@link Manifest} file must contain the attribute {@link Attributes.Name#CLASS_PATH} for resource
 * paths and "Dependent-Modules" for a list of modules this modules depends on.
 * <p>
 * In the case of both values being set on the {@link ModuleDescriptor} and within the {@link
 * Manifest} file, the rule of addition is followed rather than failure.
 *
 * @see ModuleDescriptor
 * @see ModuleSpec
 * @see ModuleFinder
 * @see JarModuleFinder
 * @see JarFile
 * @see Manifest
 * @since 1.14.0
 */
public class GeodeModuleFinder implements ModuleFinder, Comparable {

  private final ModuleDescriptor moduleDescriptor;
  private final String moduleName;
  private final List<JarFile> sourceJarFiles;
  private final Logger logger;

  private static final String DEPENDENT_MODULES = "Dependent-Modules";

  /**
   * Constructs a {@link GeodeModuleFinder} using a {@link Logger} and {@link ModuleDescriptor}.
   * In the the case of incorrect/missing resource paths an {@link IOException} will be thrown.
   *
   * @param moduleDescriptor the {@link ModuleDescriptor} describing the module
   * @throws IOException is thrown in the case of incorrect/missing resource paths.
   */
  public GeodeModuleFinder(final Logger logger, final ModuleDescriptor moduleDescriptor)
      throws IOException {
    this.moduleName = moduleDescriptor.getName();
    this.sourceJarFiles = parseSourcesIntoJarFiles(moduleDescriptor);
    this.logger = logger;
    this.moduleDescriptor = moduleDescriptor;
  }

  /**
   * Processes the {@link ModuleDescriptor} resource paths into a collection of {@link JarFile}
   *
   * @param moduleDescriptor the {@link ModuleDescriptor} for the module
   * @return a collection of {@link JarFile} from the {@link ModuleDescriptor}
   * @throws IOException is thrown in the case of incorrect/missing resources
   */
  private List<JarFile> parseSourcesIntoJarFiles(ModuleDescriptor moduleDescriptor)
      throws IOException {
    List<JarFile> results = new LinkedList<>();
    for (String sourcePath : moduleDescriptor.getResourceJarPaths()) {
      if (sourcePath.endsWith(".jar")) {
        results.add(new JarFile(sourcePath, true));
      }
    }
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ModuleSpec findModule(String name, ModuleLoader delegateLoader)
      throws ModuleLoadException {
    if (this.moduleName.equals(name)) {
      return createModuleSpecForRegisteredJars(name);
    }
    return null;
  }

  /**
   * Create a {@link ModuleSpec} for the registered Module. It creates a {@link ModuleSpec} for the
   * name, processes each of the {@link JarFile} in the {@link #sourceJarFiles} collection and adds
   * dependencies to other modules.
   *
   * @param name the module name
   * @return a {@link ModuleSpec} from the corresponding {@link ModuleDescriptor}
   * @throws ModuleLoadException in case of failure loading resource paths
   */
  private ModuleSpec createModuleSpecForRegisteredJars(String name) throws ModuleLoadException {
    ModuleSpec.Builder moduleSpecBuilder = getModuleSpec(name);
    for (JarFile jarFile : sourceJarFiles) {
      registerJarFile(moduleSpecBuilder, jarFile);
    }

    createDependenciesForModules(moduleSpecBuilder, moduleDescriptor.getDependedOnModules());

    createJDKPathDependencyForModule(moduleSpecBuilder);

    moduleSpecBuilder
        .addDependency(DependencySpec.createSystemDependencySpec(GeodeJDKPaths.JDK));
    return moduleSpecBuilder.create();
  }

  private void createJDKPathDependencyForModule(ModuleSpec.Builder moduleSpecBuilder) {
    if (moduleDescriptor.requiresJDKPaths()) {
      moduleSpecBuilder.addDependency(new LocalDependencySpecBuilder()
          .setImportFilter(PathFilters.acceptAll())
          .setExport(true)
          .setLocalLoader(ClassLoaderLocalLoader.SYSTEM)
          .setLoaderPaths(JDKPaths.JDK)
          .build());
    }
  }

  /**
   * Processes a single {@link JarFile}. Processes the optionally included {@link Manifest} and adds
   * itself to the {@link ModuleSpec} as a source resource.
   *
   * @param moduleSpecBuilder the builder for the {@link ModuleSpec}
   * @param jarFile the {@link JarFile} to be processed
   * @throws ModuleLoadException in case of failure loading resource paths
   */
  private void registerJarFile(ModuleSpec.Builder moduleSpecBuilder, JarFile jarFile)
      throws ModuleLoadException {
    processManifestFileEntries(moduleSpecBuilder, jarFile);

    moduleSpecBuilder.addResourceRoot(ResourceLoaderSpec
        .createResourceLoaderSpec(ResourceLoaders.createJarResourceLoader(jarFile)));
  }

  /**
   * Processes a collection of modules and adds them to the dependent modules list for the {@link
   * ModuleSpec}
   *
   * @param moduleSpecBuilder the builder for the {@link ModuleSpec}
   * @param modulesDependencies a list of module names on which this module depends on
   */
  private void createDependenciesForModules(ModuleSpec.Builder moduleSpecBuilder,
      String[] modulesDependencies) {
    for (String moduleDependency : modulesDependencies) {
      moduleSpecBuilder.addDependency(new ModuleDependencySpecBuilder()
          .setExport(true)
          .setImportServices(true)
          .setName(moduleDependency)
          .build());
    }
  }

  /**
   * Processes a collection of modules and adds them to the dependent modules list for the {@link
   * ModuleSpec}
   *
   * @param moduleSpecBuilder the builder for the {@link ModuleSpec}
   * @param modulesDependencies a list of module names on which this module depends on
   */
  private void createDependenciesForModules(ModuleSpec.Builder moduleSpecBuilder,
      Collection<String> modulesDependencies) {
    for (String moduleDependency : modulesDependencies) {
      moduleSpecBuilder.addDependency(new ModuleDependencySpecBuilder()
          .setExport(true)
          .setImportServices(true)
          .setName(moduleDependency)
          .build());
    }
  }

  /**
   * Processes the {@link Manifest} and populates both the resource paths and dependent modules from
   * it.
   * The {@link Manifest} file optionally includes a {@link Attributes.Name#CLASS_PATH} attribute,
   * describing the resource paths to be included
   * or an attribute {@link #DEPENDENT_MODULES} describing a list of dependent module names.
   *
   * @param moduleSpecBuilder the builder for the {@link ModuleSpec}
   * @param jarFile the {@link JarFile} which contains a {@link Manifest} file.
   * @throws ModuleLoadException in case of failure loading resource paths
   */
  private void processManifestFileEntries(ModuleSpec.Builder moduleSpecBuilder, JarFile jarFile)
      throws ModuleLoadException {
    Optional<Manifest> manifestFromJar = getManifestFromJar(jarFile);
    if (manifestFromJar.isPresent()) {
      Manifest manifest = manifestFromJar.get();
      processClasspathFromManifest(moduleSpecBuilder, JarFileUtils.getRootPathFromJarFile(jarFile),
          manifest);

      processManifestDependents(moduleSpecBuilder, manifest);
    }

  }

  /**
   * Process the {@link Manifest} file for dependent module names. The {@link Manifest} file must
   * contain
   * attribute {@link #DEPENDENT_MODULES} entries for this to be successful.
   *
   * @param moduleSpecBuilder the builder for the {@link ModuleSpec}
   * @param manifest the manifest file where the attribute {@link #DEPENDENT_MODULES} is looked up
   *        from
   */
  private void processManifestDependents(ModuleSpec.Builder moduleSpecBuilder, Manifest manifest) {
    createDependenciesForModules(moduleSpecBuilder,
        JarFileUtils.getAttributesFromManifest(manifest, DEPENDENT_MODULES));
  }

  /**
   * Process the {@link Manifest} file for resource paths to include. The {@link Manifest} file must
   * contain
   * attribute {@link Attributes.Name#CLASS_PATH} entries for this to be successful.
   *
   * @param builder the builder for the {@link ModuleSpec}
   * @param rootPath the root path for the source {@link JarFile}.
   * @param manifest the manifest file where the attribute {@link Attributes.Name#CLASS_PATH} is
   *        looked up from
   * @throws ModuleLoadException in the case of incorrect/missing resource
   */
  private void processClasspathFromManifest(ModuleSpec.Builder builder, String rootPath,
      Manifest manifest) throws ModuleLoadException {
    String[] classpathEntries =
        JarFileUtils.getAttributesFromManifest(manifest, Attributes.Name.CLASS_PATH);
    for (String classpathEntry : classpathEntries) {
      try {
        builder.addResourceRoot(ResourceLoaderSpec
            .createResourceLoaderSpec(
                ResourceLoaders
                    .createJarResourceLoader(getJarFileForPath(rootPath, classpathEntry))));
      } catch (IOException e) {
        logger.error(String.format(
            "File for name: %s could not be loaded as a dependent jar file at location: %s",
            classpathEntry, (rootPath + File.separator + classpathEntry)));
        throw new ModuleLoadException(e);
      }
    }
  }

  /**
   * Returns a {@link File} for the path = rootPath + fileName. Checks if the fileName includes
   * ".jar" and adds it if it is not included.
   *
   * @param rootPath the root path where the file is to be located
   * @param fileName the filename of the {@link JarFile} within the rootPath
   * @return file represented by the rootPath + fileName + ".jar".
   */
  private JarFile getJarFileForPath(String rootPath, String fileName) throws IOException {
    if (fileName.endsWith(".jar")) {
      return new JarFile(rootPath + File.separator + fileName);
    } else {
      return new JarFile(rootPath + File.separator + fileName + ".jar");
    }
  }

  /**
   * Creates and returns a {@link ModuleSpec.Builder} for the name specified
   *
   * @param name the name of the module
   * @return a {@link ModuleSpec.Builder}
   */
  private ModuleSpec.Builder getModuleSpec(String name) {

    // PathFilter metainfChild = PathFilters.isChildOf("META-INF");
    // PathFilter metainf = PathFilters.is("META-INF");
    // PathFilter metainfServices = PathFilters.is("META-INF/services");
    // PathFilter metainfServicesChild = PathFilters.isChildOf("META-INF/services");
    // PathFilter childOfSlash = PathFilters.isChildOf("/");
    // MultiplePathFilterBuilder multiplePathFilterBuilder =
    // PathFilters.multiplePathFilterBuilder(true);
    // multiplePathFilterBuilder.addFilter(metainfChild, true);
    // multiplePathFilterBuilder.addFilter(metainf, true);
    // multiplePathFilterBuilder.addFilter(metainfServicesChild, true);
    // multiplePathFilterBuilder.addFilter(metainfServices, true);
    // multiplePathFilterBuilder.addFilter(childOfSlash, true);

    ModuleSpec.Builder builder = ModuleSpec.build(name);
    builder.addDependency(new LocalDependencySpecBuilder()
        .setImportServices(true)
        // .setImportFilter(multiplePathFilterBuilder.create())
        .setExport(true)
        .build());

    return builder;
  }

  /**
   * Returns a {@link Manifest} file from the {@link JarFile}
   *
   * @param jarFile the {@link JarFile} from which to retrieve the {@link Manifest} file
   * @return an {@link Optional} containing a {@link Manifest} file from the {@link JarFile}. In the
   *         case of no {@link Manifest} file being present, an {@link Optional#empty()} shall be
   *         returned.
   */
  private Optional<Manifest> getManifestFromJar(JarFile jarFile) {
    try {
      return JarFileUtils.getManifestFromJarFile(jarFile);
    } catch (IOException e) {
      logger.info(
          String.format("Unable to find manifest file for jar ile name: %s", jarFile.getName()));
    }
    return Optional.empty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GeodeModuleFinder that = (GeodeModuleFinder) o;
    return moduleName.equals(that.moduleName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(moduleName);
  }

  @Override
  public int compareTo(Object o) {
    return this.moduleName.compareTo(((GeodeModuleFinder) o).moduleName);
  }
}
