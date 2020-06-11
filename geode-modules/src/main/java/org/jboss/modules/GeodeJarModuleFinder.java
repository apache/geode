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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.services.module.ModuleDescriptor;

/**
 * A custom implementation of a {@link ModuleFinder}. This implementation is based on
 * {@link JarModuleFinder}
 * which creates a {@link ModuleSpec} from a {@link ModuleDescriptor}
 * The {@link ModuleDescriptor} defines the module name, a set of {@link JarFile}s and/or dependent
 * module names for Module.
 *
 * This {@link ModuleFinder} has the ability to set both resource paths and dependent module names
 * from either
 * the {@link ModuleDescriptor} or from a {@link Manifest} file within the {@link JarFile}.
 *
 * {@link Manifest} file must contain the attribute {@link Attributes.Name#CLASS_PATH} for resource
 * paths and "Dependent-Modules"
 * for a list of modules this modules depends on.
 *
 * In the case of both values being set on the {@link ModuleDescriptor} and within the
 * {@link Manifest} file,
 * the rule of addition is followed rather than failure.
 *
 * @see ModuleDescriptor
 * @see ModuleSpec
 * @see ModuleFinder
 * @see JarModuleFinder
 * @see JarFile
 * @see Manifest
 *
 * @since 1.14.0
 *
 */
public class GeodeJarModuleFinder implements ModuleFinder {

  private final ModuleDescriptor moduleDescriptor;
  private final String moduleName;
  private final List<JarFile> sourceJarFiles;
  private final Logger logger;

  private static final String[] EMPTY_STRING_ARRAY = new String[0];
  private static final String DEPENDENT_MODULES = "Dependent-Modules";
  private static final List<String> EMPTY_LIST = new ArrayList<>();

  /**
   * Constructs a {@link GeodeJarModuleFinder} using a {@link Logger} and {@link ModuleDescriptor}.
   * In the the case of incorrect/missing resource paths an {@link IOException} will be thrown.
   *
   * @param logger a Logger to log messages
   * @param moduleDescriptor the {@link ModuleDescriptor} describing the module
   * @throws IOException is thrown in the case of incorrect/missing resource paths.
   */
  public GeodeJarModuleFinder(final Logger logger, final ModuleDescriptor moduleDescriptor)
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
      results.add(new JarFile(sourcePath, true));
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
      return findModuleOfRegisteredJars(name);
    }
    return null;
  }

  /**
   * Create a {@link ModuleSpec} for the registered Module.
   * It creates a {@link ModuleSpec} for the name, processes each of the {@link JarFile} in the
   * {@link #sourceJarFiles} collection and adds dependencies to other modules.
   *
   * @param name the module name
   * @return a {@link ModuleSpec} from the corresponding {@link ModuleDescriptor}
   * @throws ModuleLoadException in case of failure loading resource paths
   */
  private ModuleSpec findModuleOfRegisteredJars(String name) throws ModuleLoadException {
    ModuleSpec.Builder moduleSpecBuilder = getModuleSpec(name);
    for (JarFile jarFile : sourceJarFiles) {
      registerJarFile(moduleSpecBuilder, jarFile);
    }
    createDependenciesForModules(moduleSpecBuilder, moduleDescriptor.getDependedOnModules());
    moduleSpecBuilder.addDependency(DependencySpec.createSystemDependencySpec(JDKPaths.JDK));
    return moduleSpecBuilder.create();
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
   * Processes a collection of modules and adds them to the dependent modules list for the
   * {@link ModuleSpec}
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
      String rootPath =
          jarFile.getName().substring(0, jarFile.getName().lastIndexOf(File.separator));
      Attributes mainAttributes = manifestFromJar.get().getMainAttributes();
      processClasspathFromManifest(moduleSpecBuilder, rootPath,
          mainAttributes.getValue(Attributes.Name.CLASS_PATH));

      processManifestDependents(moduleSpecBuilder, mainAttributes.getValue(DEPENDENT_MODULES));
    }
  }

  /**
   * Process the {@link Manifest} file for dependent module names. The {@link Manifest} file must
   * contain
   * attribute {@link #DEPENDENT_MODULES} entries for this to be successful.
   *
   * @param moduleSpecBuilder the builder for the {@link ModuleSpec}
   * @param dependentModules a String (' ') single space delimited of dependent module names
   */
  private void processManifestDependents(ModuleSpec.Builder moduleSpecBuilder,
      String dependentModules) {
    List<String> dependentModulesEntries = !StringUtils.isEmpty(dependentModules) ? Arrays
        .asList(dependentModules.split(" ")) : EMPTY_LIST;
    createDependenciesForModules(moduleSpecBuilder, dependentModulesEntries);
  }

  /**
   * Process the {@link Manifest} file for resource paths to include. The {@link Manifest} file must
   * contain
   * attribute {@link Attributes.Name#CLASS_PATH} entries for this to be successful.
   *
   * @param builder the builder for the {@link ModuleSpec}
   * @param rootPath the root path for the source {@link JarFile}.
   * @param classpath a String (' ') single space delimited list of resource paths.
   * @throws ModuleLoadException in the case of incorrect/missing resource
   */
  private void processClasspathFromManifest(ModuleSpec.Builder builder, String rootPath,
      String classpath) throws ModuleLoadException {
    String[] classpathEntries =
        !StringUtils.isEmpty(classpath) ? classpath.split(" ") : EMPTY_STRING_ARRAY;
    for (String classpathEntry : classpathEntries) {
      if (!classpathEntry.isEmpty()) {

        File file = getFileForPath(rootPath, classpathEntry);
        try {
          builder.addResourceRoot(ResourceLoaderSpec
              .createResourceLoaderSpec(
                  ResourceLoaders.createJarResourceLoader(new JarFile(file, true))));
        } catch (IOException e) {
          logger.error(String.format(
              "File for name: %s could not be loaded as a dependent jar file at location: %s",
              classpathEntry, file.getName()));
          throw new ModuleLoadException(e);
        }
      }
    }
  }

  /**
   * Returns a {@link File} for the path = rootPath + fileName.
   * Checks if the fileName includes ".jar" and adds it if it is not included.
   *
   * @param rootPath the root path where the file is to be located
   * @param fileName the filename of the {@link JarFile} within the rootPath
   * @return file represented by the rootPath + fileName.
   */
  private File getFileForPath(String rootPath, String fileName) {
    if (fileName.endsWith(".jar")) {
      return new File(rootPath + File.separator + fileName);
    } else {
      return new File(rootPath + File.separator + fileName + ".jar");
    }
  }

  /**
   * Creates and returns a {@link ModuleSpec.Builder} for the name specified
   *
   * @param name the name of the module
   * @return a {@link ModuleSpec.Builder}
   */
  private ModuleSpec.Builder getModuleSpec(String name) {
    ModuleSpec.Builder builder = ModuleSpec.build(name);
    builder.addDependency(new LocalDependencySpecBuilder()
        .setImportServices(true)
        .setExport(true)
        .build());

    return builder;
  }

  /**
   * Returns a {@link Manifest} file from the {@link JarFile}
   *
   * @param jarFile the {@link JarFile} from which to retrieve the {@link Manifest} file
   * @return an {@link Optional} containing a {@link Manifest} file from the {@link JarFile}. In the
   *         case of no {@link Manifest} file
   *         being present, an {@link Optional#empty()} shall be returned.
   */
  private Optional<Manifest> getManifestFromJar(JarFile jarFile) {
    try {
      return Optional.ofNullable(jarFile.getManifest());
    } catch (IOException e) {
      logger.info(
          String.format("Unable to find manifest file for jar ile name: %s", jarFile.getName()));
    }
    return Optional.empty();
  }
}
