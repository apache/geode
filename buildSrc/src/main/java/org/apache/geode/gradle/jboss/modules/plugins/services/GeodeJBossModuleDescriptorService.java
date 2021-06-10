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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ResolvedArtifact;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.geode.gradle.jboss.modules.plugins.config.GeodeJBossModulesGeneratorConfig;
import org.apache.geode.gradle.jboss.modules.plugins.domain.DependencyWrapper;
import org.apache.geode.gradle.jboss.modules.plugins.generator.ModuleDescriptorGenerator;
import org.apache.geode.gradle.jboss.modules.plugins.generator.domain.ModuleDependency;
import org.apache.geode.gradle.jboss.modules.plugins.generator.xml.JBossModuleDescriptorGenerator;
import org.apache.geode.gradle.jboss.modules.plugins.utils.ProjectUtils;

public class GeodeJBossModuleDescriptorService extends GeodeCommonModuleDescriptorService {

  private static final String API = "api";
  private static final String IMPLEMENTATION = "implementation";
  private static final String RUNTIME_ONLY = "runtimeOnly";
  private static final String RUNTIME_CLASSPATH = "runtimeClasspath";
  private static final String JBOSS_MODULAR = "jbossModular";
  private static final String EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME =
      "external-library-dependencies";
  private static final String LIB_PATH_PREFIX = "../../../../lib/";
  private final ModuleDescriptorGenerator moduleDescriptorGenerator =
      new JBossModuleDescriptorGenerator();
  private final Map<String, List<ResolvedArtifact>> resolvedProjectRuntimeArtifacts =
      new HashMap<>();
  private final Map<String, List<DependencyWrapper>> apiProjectDependencies = new HashMap<>();
  private final Map<String, List<DependencyWrapper>> runtimeProjectDependencies = new HashMap<>();
  private final Map<String, List<String>> embeddedArtifactNames = new HashMap<>();
  private List<Project> allProjects;

  @Override
  public void createModuleDescriptor(Project project, GeodeJBossModulesGeneratorConfig config,
      File projectJarFile) {

    List<ModuleDependency> moduleDependencies = generateModuleDependencies(project, config);

    List<String> resourceRoots = generateResourceRoots(project, projectJarFile, config);

    String moduleVersion = project.getVersion().toString();

    moduleDescriptorGenerator.generate(config.outputRoot, project.getName(),
        moduleVersion, resourceRoots, moduleDependencies, config.mainClass);

    moduleDescriptorGenerator.generateAlias(config.outputRoot, project.getName(), moduleVersion);
  }

  private List<ModuleDependency> generateModuleDependencies(Project project,
      GeodeJBossModulesGeneratorConfig config) {
    List<ModuleDependency> moduleDependencies =
        getModuleDependencies(getApiProjectDependencies(project, config),
            getRuntimeProjectDependencies(project, config), config.projectsToInclude);

    moduleDependencies.add(new ModuleDependency(EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME,
        false, false));

    moduleDependencies.addAll(getJBossJDKModuleDependencies());
    return moduleDependencies;
  }

  private synchronized List<DependencyWrapper> getRuntimeProjectDependencies(Project project,
      GeodeJBossModulesGeneratorConfig config) {
    String projectName = project.getName();
    if (runtimeProjectDependencies.get(projectName) == null) {
      runtimeProjectDependencies.put(projectName, Collections.unmodifiableList(
          getProjectDependenciesForConfiguration(project, getTargetConfigurations(
              config.facetName, IMPLEMENTATION, RUNTIME_ONLY, JBOSS_MODULAR))));

    }
    return runtimeProjectDependencies.get(projectName);
  }

  private synchronized List<DependencyWrapper> getApiProjectDependencies(Project project,
      GeodeJBossModulesGeneratorConfig config) {
    String projectName = project.getName();
    if (apiProjectDependencies.get(projectName) == null) {
      apiProjectDependencies.put(projectName, Collections.unmodifiableList(
          getProjectDependenciesForConfiguration(project, getTargetConfigurations(
              config.facetName, API))));
    }
    return apiProjectDependencies.get(projectName);
  }

  @Override
  public void createExternalLibraryDependenciesModuleDescriptor(Project project,
      GeodeJBossModulesGeneratorConfig config) {
    Set<String> embeddedProjectArtifacts = new TreeSet<>();
    embeddedProjectArtifacts.addAll(getEmbeddedArtifactsNames(project, config));
    List<ResolvedArtifact> resolvedArtifacts = getResolvedProjectRuntimeArtifacts(project, config);
    for (ResolvedArtifact resolvedArtifact : resolvedArtifacts) {
      for (Project innerProject : getAllProjects(project)) {
        if (innerProject.getName().equals(resolvedArtifact.getName())) {
          embeddedProjectArtifacts.addAll(getEmbeddedArtifactsNames(innerProject, config));
        }
      }
    }

    Set<String> projectNames = getAllProjects(project).stream().map(Project::getName).collect(
        Collectors.toSet());

    List<String> artifactNames = resolvedArtifacts.stream()
        .filter(artifact -> !projectNames.contains(artifact.getName()))
        .filter(artifact -> !embeddedProjectArtifacts.contains(artifact.getName()))
        .map(artifact -> LIB_PATH_PREFIX + artifact.getFile().getName())
        .collect(Collectors.toList());

    List<ModuleDependency> moduleDependencies = getJBossJDKModuleDependencies();

    moduleDescriptorGenerator.generate(config.outputRoot, EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME,
        project.getVersion().toString(), artifactNames, moduleDependencies);
  }

  private List<ModuleDependency> getJBossJDKModuleDependencies() {
    return Arrays.asList(new ModuleDependency("java.se", false, false),
        new ModuleDependency("jdk.unsupported", false, false),
        new ModuleDependency("jdk.scripting.nashorn", false, false));
  }

  @Override
  public void combineModuleDescriptors(Project project, GeodeJBossModulesGeneratorConfig config,
      List<File> externalLibraryModuleDescriptors) {
    Set<String> externalLibraries = new TreeSet<>();
    for (File inputFile : externalLibraryModuleDescriptors) {
      externalLibraries.addAll(getResourceRootsFromModuleXml(inputFile));
    }

    moduleDescriptorGenerator.generate(config.outputRoot, EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME,
        project.getVersion().toString(), new ArrayList<>(externalLibraries), getJBossJDKModuleDependencies());

    moduleDescriptorGenerator.generateAlias(config.outputRoot, EXTERNAL_LIBRARY_DEPENDENCIES_MODULE_NAME, project.getVersion().toString());
  }

  private synchronized List<ResolvedArtifact> getResolvedProjectRuntimeArtifacts(Project project,
      GeodeJBossModulesGeneratorConfig config) {
    String projectName = project.getName();
    if (resolvedProjectRuntimeArtifacts.get(projectName) == null) {
      resolvedProjectRuntimeArtifacts.put(projectName,
          ProjectUtils.getResolvedProjectRuntimeArtifacts(project,
              getTargetConfigurations(config.facetName, RUNTIME_CLASSPATH)));
    }
    return resolvedProjectRuntimeArtifacts.get(projectName);
  }

  private List<String> generateResourceRoots(Project project, File resourceFile,
      GeodeJBossModulesGeneratorConfig config) {
    List<String> resourceRoots = new ArrayList<>();
    resourceRoots.add(LIB_PATH_PREFIX + resourceFile.getName());

    List<String> embeddedArtifactNames = getEmbeddedArtifactsNames(project, config);

    getResolvedProjectRuntimeArtifacts(project, config).stream()
        .filter(artifact -> embeddedArtifactNames.contains(artifact.getName()))
        .forEach(artifact -> resourceRoots.add(LIB_PATH_PREFIX + artifact.getFile().getName()));
    return resourceRoots;
  }

  private List<String> getEmbeddedArtifactsNames(Project project,
      GeodeJBossModulesGeneratorConfig config) {
    String projectName = project.getName();
    if (embeddedArtifactNames.get(projectName) == null) {
      LinkedList<String> artifacts = new LinkedList<>();
      artifacts.addAll(getModuleNameFromDependency(getApiProjectDependencies(project, config)));
      artifacts
          .addAll(getModuleNameFromDependency(getRuntimeProjectDependencies(project, config)));
      embeddedArtifactNames.put(projectName, Collections.unmodifiableList(artifacts));
    }
    return embeddedArtifactNames.get(projectName);
  }

  private List<String> getModuleNameFromDependency(List<DependencyWrapper> projectDependencies) {
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

  private List<ModuleDependency> getModuleDependencies(
      List<DependencyWrapper> apiProjectDependencies,
      List<DependencyWrapper> runtimeProjectDependencies, List<String> allProjectNames) {

    List<ModuleDependency> moduleDependencies =
        apiProjectDependencies.stream()
            .filter(dependencyWrapper -> allProjectNames
                .contains(dependencyWrapper.getDependency().getName()))
            .map(dependencyWrapper -> {
              Dependency dependency = dependencyWrapper.getDependency();
              boolean optional = ProjectUtils.invokeGroovyCode(
                  (org.gradle.api.artifacts.ModuleDependency) dependency, "optional");
              return new ModuleDependency(dependency.getName(), true,
                  optional);
            }).collect(Collectors.toList());

    moduleDependencies.addAll(
        runtimeProjectDependencies.stream()
            .filter(dependencyWrapper -> allProjectNames
                .contains(dependencyWrapper.getDependency().getName()))
            .map(dependencyWrapper -> {
              Dependency dependency = dependencyWrapper.getDependency();
              boolean optional = ProjectUtils.invokeGroovyCode(
                  (org.gradle.api.artifacts.ModuleDependency) dependency, "optional");
              return new ModuleDependency(dependency.getName(), false,
                  optional);
            }).collect(Collectors.toList()));

    return moduleDependencies;
  }

  private synchronized List<Project> getAllProjects(Project project) {
    if (allProjects == null) {
      allProjects =
          Collections.unmodifiableList(new LinkedList<>(project.getRootProject().getSubprojects()));
    }
    return allProjects;
  }
}
