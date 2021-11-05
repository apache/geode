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
package org.apache.geode.gradle.jboss.modules.plugins.services;

import static org.apache.geode.gradle.jboss.modules.plugins.utils.ProjectUtils.getProjectDependenciesForConfiguration;
import static org.apache.geode.gradle.jboss.modules.plugins.utils.ProjectUtils.getTargetConfigurations;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.DependencyConstraint;
import org.gradle.api.artifacts.ModuleIdentifier;
import org.gradle.api.artifacts.ModuleVersionIdentifier;
import org.gradle.api.artifacts.MutableVersionConstraint;
import org.gradle.api.artifacts.ResolvedArtifact;
import org.gradle.api.artifacts.SelfResolvingDependency;
import org.gradle.api.attributes.AttributeContainer;
import org.gradle.api.attributes.Category;
import org.gradle.api.internal.artifacts.dependencies.DefaultExternalModuleDependency;
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependency;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.geode.gradle.jboss.modules.plugins.config.ModulesGeneratorConfig;
import org.apache.geode.gradle.jboss.modules.plugins.domain.DependencyWrapper;
import org.apache.geode.gradle.jboss.modules.plugins.generator.ModuleDescriptorGenerator;
import org.apache.geode.gradle.jboss.modules.plugins.generator.domain.ModuleDependency;
import org.apache.geode.gradle.jboss.modules.plugins.generator.xml.JBossModuleDescriptorGenerator;
import org.apache.geode.gradle.jboss.modules.plugins.utils.ProjectUtils;

public class GeodeJBossModuleDescriptorService implements GeodeModuleDescriptorService {

  private static final String API = "api";
  private static final String IMPLEMENTATION = "implementation";
  private static final String RUNTIME_ONLY = "runtimeOnly";
  private static final String RUNTIME_CLASSPATH = "runtimeClasspath";
  private static final String JBOSS_MODULAR = "jbossModular";
  private static final String EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME =
      "external-library-dependencies";
  private static final String COMBINED_EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME =
      "combined-external-library-dependencies";
  private static final String LIB_PATH_PREFIX = "../../../../../lib/";
  private static final String PLUGIN_ID = "geode.jboss-modules-plugin";
  private final ModuleDescriptorGenerator moduleDescriptorGenerator =
      new JBossModuleDescriptorGenerator();
  private final Map<String, Set<ResolvedArtifact>> resolvedProjectRuntimeArtifacts =
      new HashMap<>();
  private final Map<String, List<DependencyWrapper>> apiProjectDependencies = new HashMap<>();
  private final Map<String, List<DependencyWrapper>> runtimeProjectDependencies = new HashMap<>();
  private final Map<String, List<DependencyWrapper>> jbossProjectDependencies = new HashMap<>();
  private final Map<String, List<String>> embeddedArtifactNames = new HashMap<>();
  private List<Project> allProjects;

  @Override
  public void createModuleDescriptor(Project project, ModulesGeneratorConfig config,
      File projectJarFile) {

    List<ModuleDependency> moduleDependencies = generateModuleDependencies(project, config);

    Set<String> resourceRoots = generateResourceRoots(project, projectJarFile, config);

    String moduleVersion = project.getVersion().toString();

    generateNewModuleDescriptor(project, config, moduleDependencies, resourceRoots, moduleVersion);
  }


  private void generateNewModuleDescriptor(Project project, ModulesGeneratorConfig config,
      List<ModuleDependency> moduleDependencies, Set<String> resourceRoots, String moduleVersion) {
    List<DependencyWrapper> apiProjectDependencies = getApiProjectDependencies(project, config);
    Set<ResolvedArtifact> resolvedProjectRuntimeArtifacts =
        getResolvedRuntimeArtifactsForProject(project, config);

    List<String> apiDependencies = apiProjectDependencies.stream()
        .filter(
            dependencyWrapper -> !dependencyWrapper.getDependency().getName().contains("geode-"))
        .map(dependencyWrapper -> dependencyWrapper.getDependency().getName())
        .collect(Collectors.toList());


    moduleDescriptorGenerator.generate(config.outputRoot.resolve(config.name), project.getName(),
        moduleVersion, resourceRoots, moduleDependencies, config.mainClass, Collections.EMPTY_LIST,
        config.packagesToExport, config.packagesToExclude);

    moduleDescriptorGenerator
        .generateAlias(config.outputRoot.resolve(config.name), project.getName(), moduleVersion);
  }

  private List<ModuleDependency> generateModuleDependencies(Project project,
      ModulesGeneratorConfig config) {
    List<String> projectNames =
        getAllProjects(project).stream().filter(proj -> proj.getPluginManager().hasPlugin(
            PLUGIN_ID)).map(Project::getName).collect(Collectors.toList());

    List<ModuleDependency> moduleDependencies = getModuleDependencies(projectNames,
        getApiProjectDependencies(project, config),
        getRuntimeProjectDependencies(project, config),
        getJBossProjectDependencies(project, config));

    if(config.parentModule != null){
      if(config.parentModule.getExtensions().getExtraProperties().has("isGeodeExtension") &&
              (boolean) config.parentModule.getExtensions().getExtraProperties().get("isGeodeExtension")){
        moduleDependencies.add(new ModuleDependency(config.parentModule.getName() + "-external-library",
                true, false));
      }
    }

    moduleDependencies.add(new ModuleDependency(EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME,
        false, false));

    if (project.getExtensions().getExtraProperties().has("isGeodeExtension") &&
            (boolean) project.getExtensions().getExtraProperties().get("isGeodeExtension")) {
      moduleDependencies.add(new ModuleDependency(project.getName() + "-external-library",
          true, false));
    }

    moduleDependencies.addAll(getJBossJDKModuleDependencies(config));
    return moduleDependencies;
  }

  private List<DependencyWrapper> getRuntimeProjectDependencies(Project project,
      ModulesGeneratorConfig config) {
    return getOrPopulateCachedDependencies(runtimeProjectDependencies, project, config,
        IMPLEMENTATION, RUNTIME_ONLY);
  }

  private List<DependencyWrapper> getJBossProjectDependencies(Project project,
      ModulesGeneratorConfig config) {
    return getOrPopulateCachedDependencies(jbossProjectDependencies, project, config,
        JBOSS_MODULAR);
  }

  private List<DependencyWrapper> getApiProjectDependencies(Project project,
      ModulesGeneratorConfig config) {

    return getOrPopulateCachedDependencies(apiProjectDependencies, project, config, API);
  }

  private List<DependencyWrapper> getOrPopulateCachedDependencies(
      Map<String, List<DependencyWrapper>> dependenciesMap, Project project,
      ModulesGeneratorConfig config, String... gradleConfigurations) {
    String key = config.name + project.getName();
    return Collections.unmodifiableList(
            getProjectDependenciesForConfiguration(project, getTargetConfigurations(
                    config.name, gradleConfigurations)));
//    if (dependenciesMap.get(key) == null) {
//      dependenciesMap.put(key, Collections.unmodifiableList(
//          getProjectDependenciesForConfiguration(project, getTargetConfigurations(
//              config.name, gradleConfigurations))));
//    }
//    return dependenciesMap.get(key);
  }

  @Override
  public void createExternalLibraryDependenciesModuleDescriptor(Project project,
      ModulesGeneratorConfig config) {

    Set<ResolvedArtifact> resolvedDependencyArtifacts =
        getResolvedRuntimeArtifactsForProject(project, config);

    List<String> targetConfigurations = getTargetConfigurations(config.name, RUNTIME_CLASSPATH);
    Set<ResolvedArtifact> resolvedClassPathArtifacts = project.getConfigurations()
        .getByName(targetConfigurations.get(0)).getResolvedConfiguration().getResolvedArtifacts();

    Set<ResolvedArtifact> resolvedArtifacts = resolvedDependencyArtifacts.stream()
        .filter(resolvedArtifact -> resolvedClassPathArtifacts.contains(resolvedArtifact))
        .collect(Collectors.toSet());

    Set<String> embeddedProjectArtifacts =
        getEmbeddedArtifactsFromDependencyProjects(project, config, resolvedArtifacts);

    Set<String> resourceRoots = getResourceRoots(project, config, embeddedProjectArtifacts, true);

    ExtraPropertiesExtension extraProperties = project.getExtensions().getExtraProperties();
    boolean isGeodeExtension = extraProperties.has("isGeodeExtension")
        && (boolean) extraProperties.get("isGeodeExtension");

    List<ModuleDependency> moduleDependencies =
        getExternalDependencyJBossJDKModuleDependencies(config);
    if (isGeodeExtension) {
      moduleDependencies.add(new ModuleDependency(EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME,
          false, false));
    }

    String moduleName = project.getName() + "-external-library";

    Path basePath =
        config.outputRoot.resolve(config.name).resolve(EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME);

    if(config.parentModule != null){
      if(config.parentModule.getExtensions().getExtraProperties().has("isGeodeExtension") &&
              (boolean) config.parentModule.getExtensions().getExtraProperties().get("isGeodeExtension")){
        moduleDependencies.add(new ModuleDependency(config.parentModule.getName() + "-external-library",
                true, false));
      }
    }

    moduleDescriptorGenerator
        .generate(basePath, moduleName,
            project.getVersion().toString(), resourceRoots, moduleDependencies);

    moduleDescriptorGenerator
        .generateAlias(basePath, moduleName, project.getVersion().toString());
  }

  private Set<String> getEmbeddedArtifactsFromDependencyProjects(Project project,
      ModulesGeneratorConfig config, Set<ResolvedArtifact> resolvedArtifacts) {
    Set<String> embeddedProjectArtifacts =
        new HashSet<>(getEmbeddedArtifactsNames(project, config));
    for (ResolvedArtifact resolvedArtifact : resolvedArtifacts) {
      // Iterate over all subprojects and exclude all "embedded" libraries from the subprojects
      for (Project innerProject : getAllProjects(project)) {
        if (innerProject.getName().equals(resolvedArtifact.getName())) {
          embeddedProjectArtifacts.addAll(getEmbeddedArtifactsNames(innerProject, config));
        }
      }
    }
    return embeddedProjectArtifacts;
  }

  private List<ModuleDependency> getJBossJDKModuleDependencies(
      ModulesGeneratorConfig config) {
    return config.jbossJdkModulesDependencies == null ? Collections.emptyList()
        : config.jbossJdkModulesDependencies.stream()
            .map(jbossJdkModule -> new ModuleDependency(jbossJdkModule, false, false))
            .collect(Collectors.toList());
  }

  private List<ModuleDependency> getExternalDependencyJBossJDKModuleDependencies(
      ModulesGeneratorConfig config) {
    return config.externalLibraryJbossJDKModules == null ? Collections.emptyList()
        : config.externalLibraryJbossJDKModules.stream()
            .map(jbossJdkModule -> new ModuleDependency(jbossJdkModule, true, false))
            .collect(Collectors.toList());
  }

  @Override
  public void combineModuleDescriptors(Project project, ModulesGeneratorConfig config,
      List<File> externalLibraryModuleDescriptors, Path rootPath) {
    Set<String> externalLibraries = new TreeSet<>();
    for (File inputFile : externalLibraryModuleDescriptors) {
      externalLibraries.addAll(getResourceRootsFromModuleXml(inputFile));
    }

    moduleDescriptorGenerator
        .generate(rootPath, EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME,
            project.getVersion().toString(), new HashSet<>(externalLibraries),
            getExternalDependencyJBossJDKModuleDependencies(config));

    moduleDescriptorGenerator.generateAlias(rootPath, EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME,
        project.getVersion().toString());
  }

  private Set<ResolvedArtifact> getResolvedRuntimeArtifactsForProject(Project project,
      ModulesGeneratorConfig config) {

    String key = config.name + project.getName();
//    if (resolvedProjectRuntimeArtifacts.get(key) == null) {
      Set<ResolvedArtifact> artifacts = new HashSet<>();
      List<String> targetConfigurations = getTargetConfigurations(config.name, API, IMPLEMENTATION);
      Set<DependencyConstraint> constraints =
          getDependencyConstraints(project, targetConfigurations);

      for (String targetConfiguration : targetConfigurations) {
        Configuration configuration = project.getConfigurations().findByName(targetConfiguration);

        if (configuration != null) {
          constraints.addAll(configuration.getAllDependencyConstraints());
          configuration.getDependencies().stream()
              .filter(dependency -> dependency instanceof DefaultExternalModuleDependency)
              .forEach(dependency -> {
                ModuleIdentifier moduleIdentifier =
                    ((DefaultExternalModuleDependency) dependency).getModule();
                Optional<DependencyConstraint> constraint =
                    findConstraintForModuleIdentifier(constraints, moduleIdentifier);

                Dependency dependencyToProcess;
                if (constraint.isPresent()) {
                  dependencyToProcess = createModuleDependency(moduleIdentifier, constraint.get(),
                      (DefaultExternalModuleDependency) dependency);
                } else {
                  dependencyToProcess = dependency;
                }
                Map<ModuleIdentifier, ResolvedArtifact> resolvedArtifacts =
                    getLatestResolvedArtifacts(project, constraints,
                        project.getConfigurations().detachedConfiguration(dependencyToProcess)
                            .getResolvedConfiguration().getResolvedArtifacts(),
                        new HashSet<>());

                artifacts.addAll(resolvedArtifacts.values());
              });
        }
      }

      List<String> targetConfigurations1 = getTargetConfigurations(config.name, RUNTIME_CLASSPATH);
      Set<String> resolvedClassPathArtifacts = new HashSet<>();
      targetConfigurations1.forEach(configuration -> {
        project.getConfigurations().getByName(configuration)
            .getResolvedConfiguration().getResolvedArtifacts().stream()
            .forEach(resolvedArtifact -> resolvedClassPathArtifacts
                .add(resolvedArtifact.getFile().getName()));
      });

      Set<ResolvedArtifact> resolvedArtifacts =
          artifacts.stream().filter(resolvedArtifact -> resolvedClassPathArtifacts
              .contains(resolvedArtifact.getFile().getName())).collect(Collectors.toSet());

//      resolvedProjectRuntimeArtifacts.put(key, resolvedArtifacts);
//    }
//    return resolvedProjectRuntimeArtifacts.get(key);
    return resolvedArtifacts;
  }

  private DefaultExternalModuleDependency createModuleDependency(ModuleIdentifier moduleIdentifier,
      DependencyConstraint constraint) {
    return this.createModuleDependency(moduleIdentifier, constraint, null);
  }

  private DefaultExternalModuleDependency createModuleDependency(ModuleIdentifier moduleIdentifier,
      DependencyConstraint constraint,
      DefaultExternalModuleDependency originalDependency) {
    DefaultExternalModuleDependency newDependency =
        new DefaultExternalModuleDependency(moduleIdentifier.getGroup(), moduleIdentifier.getName(),
            constraint.getVersion());
    if (originalDependency != null) {
      Map<String, String> exclusions = new TreeMap<>();
      originalDependency.getExcludeRules()
          .forEach(excludeRule -> {
            if (excludeRule.getGroup() == null) {
              exclusions.put("module", excludeRule.getModule());
            } else {
              exclusions.put("group", excludeRule.getGroup());
            }
          });
      if (exclusions.size() > 0) {
        newDependency.exclude(exclusions);
      }
    }
    return newDependency;
  }

  private Map<ModuleIdentifier, ResolvedArtifact> getLatestResolvedArtifacts(Project project,
      Set<DependencyConstraint> constraints,
      Set<ResolvedArtifact> artifacts, Set<ModuleIdentifier> processedModuleIdentifiers) {

    Map<ModuleIdentifier, ResolvedArtifact> outputArtifacts = new HashMap<>();
    artifacts.forEach(resolvedArtifact -> {
      ModuleVersionIdentifier versionIdentifier = resolvedArtifact.getModuleVersion().getId();
      ModuleIdentifier moduleIdentifier = versionIdentifier.getModule();
      if (!processedModuleIdentifiers.contains(moduleIdentifier)) {

        Optional<DependencyConstraint> constraintForModuleIdentifier =
            findConstraintForModuleIdentifier(constraints, moduleIdentifier);

        if (constraintForModuleIdentifier.isPresent()) {
          if (!constraintForModuleIdentifier.get().getVersion()
              .equals(resolvedArtifact.getModuleVersion().getId().getVersion())) {
            DefaultExternalModuleDependency dependency =
                createModuleDependency(moduleIdentifier, constraintForModuleIdentifier.get());

            Set<ResolvedArtifact> resolvedArtifacts =
                project.getConfigurations().detachedConfiguration(dependency)
                    .getResolvedConfiguration().getResolvedArtifacts();
            outputArtifacts.putAll(getLatestResolvedArtifacts(project, constraints,
                resolvedArtifacts, processedModuleIdentifiers));
            processedModuleIdentifiers.add(moduleIdentifier);
          } else {
            outputArtifacts.put(moduleIdentifier, resolvedArtifact);
            processedModuleIdentifiers.add(moduleIdentifier);
          }
        } else {
          outputArtifacts.put(moduleIdentifier, resolvedArtifact);
          processedModuleIdentifiers.add(moduleIdentifier);
        }
      }
    });
    return outputArtifacts;
  }

  private Set<DependencyConstraint> getDependencyConstraints(Project project,
      List<String> targetConfigurations) {
    Set<DependencyConstraint> constraints = new HashSet<>();
    for (String targetConfiguration : targetConfigurations) {
      Configuration configuration = project.getConfigurations().findByName(targetConfiguration);
      if (configuration != null) {
        configuration.getDependencies().stream()
            .filter(dependency -> (dependency instanceof DefaultProjectDependency))
            .forEach(dependency -> {
              DefaultProjectDependency innerProject = (DefaultProjectDependency) dependency;
              AttributeContainer attributes = innerProject.getAttributes();
              attributes.keySet().forEach(attribute -> {
                if (attribute.getName().equals("org.gradle.category")) {
                  if (((Category) attributes.getAttribute(attribute)).getName()
                      .equals("platform")) {
                    constraints.addAll(
                        innerProject.getDependencyProject().getConfigurations().getByName("api")
                            .getAllDependencyConstraints());
                    constraints.addAll(innerProject.getDependencyProject().getConfigurations()
                        .getByName("runtime")
                        .getAllDependencyConstraints());
                  }
                }
              });
            });
      }
    }
    return constraints;
  }

  private Optional<DependencyConstraint> findConstraintForModuleIdentifier(
      Set<DependencyConstraint> constraints, ModuleIdentifier moduleIdentifier) {
    Optional<DependencyConstraint> constraint = constraints.stream()
        .filter(
            dependencyConstraint -> dependencyConstraint.getModule()
                .equals(moduleIdentifier))
        .findFirst();
    return constraint;
  }

  private List<String> getAdditionalSourceSets(Project project,
      ModulesGeneratorConfig config) {
    List<DependencyWrapper> wrappers = new LinkedList<>();
    wrappers.addAll(getApiProjectDependencies(project, config));
    wrappers.addAll(getRuntimeProjectDependencies(project, config));
    List<String> resourcePaths = new LinkedList<>();
    for (DependencyWrapper wrapper : wrappers) {
      if (wrapper.getDependency() instanceof SelfResolvingDependency) {
        resourcePaths.addAll((((SelfResolvingDependency) wrapper.getDependency()).resolve().stream()
            .map(File::getAbsolutePath)
            .filter(path -> new File(path).exists())
            .collect(Collectors.toList())));
      }
    }
    return resourcePaths;
  }

  private Set<String> generateResourceRoots(Project project, File resourceFile,
      ModulesGeneratorConfig config) {
    List<String> embeddedArtifactNames = getEmbeddedArtifactsNames(project, config);

    Set<String> resourceRoots = getResourceRoots(project, config, embeddedArtifactNames, false);

    if (config.shouldAssembleFromSource()) {
      resourceRoots.addAll(getAdditionalSourceSets(project, config));

      if (!config.name.equals("main")) {
        validateAndAddResourceRoot(resourceRoots,
            project.getBuildDir().toPath().resolve("classes").resolve("java").resolve(config.name)
                .toString());
        validateAndAddResourceRoot(resourceRoots,
            project.getBuildDir().toPath().resolve("resources").resolve(config.name).toString());
        validateAndAddResourceRoot(resourceRoots,
            project.getBuildDir().toPath().resolve("generated-resources").resolve(config.name)
                .toString());
      }
      validateAndAddResourceRoot(resourceRoots,
          project.getBuildDir().toPath().resolve("classes").resolve("java").resolve("main")
              .toString());
      validateAndAddResourceRoot(resourceRoots,
          project.getBuildDir().toPath().resolve("resources").resolve("main").toString());
      validateAndAddResourceRoot(resourceRoots,
          project.getBuildDir().toPath().resolve("generated-resources").resolve("main").toString());
    } else if (resourceFile != null) {
      resourceRoots.add(LIB_PATH_PREFIX + resourceFile.getName());
    }

    return resourceRoots;
  }

  private Set<String> getResourceRoots(Project project, ModulesGeneratorConfig config,
      Collection<String> embeddedArtifactNames, boolean forExternalLibraries) {
    Set<String> resourceRoots = new HashSet<>();
    Set<String> resourceToFilterOut = new HashSet<>();

    Set<ResolvedArtifact> resolvedRuntimeArtifactsForProject =
        getResolvedRuntimeArtifactsForProject(project, config);
    if (forExternalLibraries) {
      if (config.parentModule != null) {
        getResolvedRuntimeArtifactsForProject(config.parentModule, config)
                .forEach(resolvedArtifact -> resourceToFilterOut.add(resolvedArtifact.getName()));

        getResolvedRuntimeArtifactsForProject(project, config).stream()
            .filter(artifact -> !resourceToFilterOut.contains(artifact.getName()))
            .filter(artifact -> !embeddedArtifactNames.contains(artifact.getName()))
            .forEach(artifact -> resourceRoots.add(LIB_PATH_PREFIX + artifact.getFile().getName()));
      } else {
        getResolvedRuntimeArtifactsForProject(project, config).stream()
            .filter(artifact -> !embeddedArtifactNames.contains(artifact.getName()))
            .forEach(artifact -> resourceRoots.add(LIB_PATH_PREFIX + artifact.getFile().getName()));
      }
    } else {
      resolvedRuntimeArtifactsForProject.stream()
          .filter(artifact -> embeddedArtifactNames.contains(artifact.getName()))
          .forEach(artifact -> resourceRoots.add(LIB_PATH_PREFIX + artifact.getFile().getName()));
    }

    return resourceRoots;
  }

  private void validateAndAddResourceRoot(Set<String> resourceRoots,
      String resourceRootToValidateAndAdd) {
    if (new File(resourceRootToValidateAndAdd).exists()) {
      resourceRoots.add(resourceRootToValidateAndAdd);
    }
  }

  private List<String> getEmbeddedArtifactsNames(Project project, ModulesGeneratorConfig config) {
    String key = config.name + project.getName();
//    if (embeddedArtifactNames.get(key) == null) {
      TreeSet<String> artifacts = new TreeSet<>();
      artifacts.addAll(
          getModuleNameForEmbeddedDependency(getApiProjectDependencies(project, config)));
      artifacts
          .addAll(
              getModuleNameForEmbeddedDependency(getRuntimeProjectDependencies(project, config)));
      embeddedArtifactNames
          .put(key, Collections.unmodifiableList(new LinkedList<>(artifacts)));
      return Collections.unmodifiableList(new LinkedList<>(artifacts));
//    }
//    return embeddedArtifactNames.get(key);
  }

  private List<String> getModuleNameForEmbeddedDependency(
      List<DependencyWrapper> projectDependencies) {
    return projectDependencies.stream().filter(DependencyWrapper::isEmbedded)
        .map(dependencyWrapper -> dependencyWrapper.getDependency().getName())
        .collect(Collectors.toList());
  }

  private List<String> getResourceRootsFromModuleXml(File moduleXml) {
    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

    List<String> resourceRoots = new LinkedList<>();
    try {
      documentBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
      Document document = documentBuilder.parse(moduleXml);

      NodeList resourceRootNodes = document.getElementsByTagName("resource-root");
      for (int i = 0; i < resourceRootNodes.getLength(); i++) {
        Element resourceElement = (Element) resourceRootNodes.item(i);
        resourceRoots.add(resourceElement.getAttribute("path"));
      }
    } catch (ParserConfigurationException | SAXException | IOException e) {
      throw new RuntimeException(e);
    }
    return resourceRoots;
  }

  private List<ModuleDependency> getModuleDependencies(List<String> allProjectNames,
      List<DependencyWrapper> apiProjectDependencies,
      List<DependencyWrapper> runtimeProjectDependencies,
      List<DependencyWrapper> jbossProjectDependencies) {

    Set<ModuleDependency> moduleDependencies = new HashSet<>(
        getModuleDependencies(apiProjectDependencies, allProjectNames, true));

    moduleDependencies.addAll(
        getModuleDependencies(runtimeProjectDependencies, allProjectNames, false));

    moduleDependencies.addAll(
        getModuleDependencies(jbossProjectDependencies, allProjectNames, false));

    return new LinkedList<>(moduleDependencies);
  }

  private List<ModuleDependency> getModuleDependencies(List<DependencyWrapper> dependencyWrappers,
      List<String> allProjectNames,
      boolean export) {
    return dependencyWrappers.stream()
        .filter(dependencyWrapper -> allProjectNames
            .contains(dependencyWrapper.getDependency().getName()))
        .map(dependencyWrapper -> {
          Dependency dependency = dependencyWrapper.getDependency();
          boolean optional = ProjectUtils.invokeGroovyCode(
              "hasExtension", "optional", dependency);
          return new ModuleDependency(dependency.getName(), export, optional);
        }).collect(Collectors.toList());
  }

  private List<Project> getAllProjects(Project project) {
    if (allProjects == null) {
      allProjects =
          Collections.unmodifiableList(new LinkedList<>(project.getRootProject().getSubprojects()));
    }
    return allProjects;
  }

  private static Set<String> extractPackagesFromJarFile(String filePath) {
    try {
      ZipFile zipFile = new ZipFile(filePath);
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      Set<String> packages = new HashSet<>();
      while (entries.hasMoreElements()) {
        ZipEntry zipEntry = entries.nextElement();
        if (!zipEntry.isDirectory() && zipEntry.getName().endsWith(".class")
            && !zipEntry.getName().startsWith("META-INF")) {
          String[] strings = zipEntry.getName().split("/");
          if (strings.length > 1) {
            StringJoiner stringJoiner = new StringJoiner("/");
            for (int i = 0; i < strings.length - 1; i++) {
              stringJoiner.add(strings[i]);
            }
            packages.add(stringJoiner.toString());
          }
        }
      }
      zipFile.close();
      return packages;
    } catch (IOException e) {
      return Collections.emptySet();
    }
  }
}
