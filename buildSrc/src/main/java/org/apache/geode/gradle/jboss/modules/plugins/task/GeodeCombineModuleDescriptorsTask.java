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
package org.apache.geode.gradle.jboss.modules.plugins.task;

import org.apache.geode.gradle.jboss.modules.plugins.config.GeodeJBossModulesGeneratorConfig;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeModuleDescriptorService;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectories;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.OutputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;
import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class GeodeCombineModuleDescriptorsTask extends DefaultTask {

  @Internal
  public List<GeodeJBossModulesGeneratorConfig> configurations;

  @Internal
  public List<String> projectInclusions;

  @Internal
  public GeodeModuleDescriptorService descriptorService;

  @Internal
  public Path assemblyRoot;

  @Inject
  public GeodeCombineModuleDescriptorsTask(
      List<String> projectInclusions,
      List<GeodeJBossModulesGeneratorConfig> configurations, Path assemblyRoot,
      GeodeModuleDescriptorService descriptorService) {
    this.configurations = configurations;
    this.projectInclusions = projectInclusions;
    this.descriptorService = descriptorService;
    this.assemblyRoot = assemblyRoot;

    dependsOn(getProject().getRootProject().getSubprojects().stream()
        .filter(
            project -> projectInclusions.isEmpty() || projectInclusions.contains(project.getName()))
        .map(project -> project.getTasks().named("generateExternalDependenciesModule"))
        .collect(Collectors.toSet()));
  }

  @InputFiles
  @PathSensitive(PathSensitivity.ABSOLUTE)
  public List<File> getInputFiles() {
    return getProject().getRootProject().getSubprojects().stream()
        .filter(
            project -> projectInclusions.isEmpty() || projectInclusions.contains(project.getName()))
        .map(project -> project.getTasks().named("generateExternalDependenciesModule").get()
            .getOutputs()
            .getFiles().getSingleFile())
        .collect(Collectors.toList());
  }

  @OutputDirectory
  public File getOutputFile() {
    return resolveFileFromConfiguration();
  }

  private File resolveFileFromConfiguration() {
    return assemblyRoot.resolve("moduleDescriptors")
        .resolve("external-library-dependencies").toFile();
  }

  @TaskAction
  public void run() {
    configurations.forEach(config -> descriptorService
        .combineModuleDescriptors(getProject(), config, getInputFiles()));
  }

  public List<GeodeJBossModulesGeneratorConfig> getConfigurations() {
    return configurations;
  }

  public GeodeModuleDescriptorService getDescriptorService() {
    return descriptorService;
  }

  public List<String> getProjectInclusions() {
    return projectInclusions;
  }

  public Path getAssemblyRoot() {
    return assemblyRoot;
  }
}
