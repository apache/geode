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

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependency;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import org.apache.geode.gradle.jboss.modules.plugins.config.ModulesGeneratorConfig;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeModuleDescriptorService;
import org.gradle.api.tasks.TaskProvider;

public class CombineExternalLibraryModuleDescriptorsTask extends GeodeJBossTask {

  @Internal
  public GeodeModuleDescriptorService descriptorService;

  @Internal
  public String facetToAssemble;

  private Set<TaskProvider<Task>> tasks;

  @Inject
  public CombineExternalLibraryModuleDescriptorsTask(
      String facetToAssemble,
      ModulesGeneratorConfig configuration,
      GeodeModuleDescriptorService descriptorService) {
    this.descriptorService = descriptorService;
    this.modulesGeneratorConfig = configuration;
    this.facetToAssemble = facetToAssemble;

    String facetTaskName = getFacetTaskName("generateLibraryModuleDescriptors", facetToAssemble);

    Configuration geodeArchives = getProject().getConfigurations().getByName("geodeArchives");
    Set<Project> geodeDependencies = geodeArchives.getDependencies().stream()
        .filter(dependency -> dependency instanceof DefaultProjectDependency)
        .map(dependency -> ((DefaultProjectDependency) dependency).getDependencyProject())
        .collect(Collectors.toSet());

    tasks = getProject().getRootProject().getSubprojects().stream()
//        .filter(project -> geodeDependencies.contains(project))
        .filter(
            project -> project.getTasks().findByName(facetTaskName) != null)
        .filter(project -> !project.getExtensions().getExtraProperties().has("isGeodeExtension")
            || !(boolean) project.getExtensions().getExtraProperties().get("isGeodeExtension"))
        .map(project -> project.getTasks().named(facetTaskName))
        .collect(Collectors.toSet());
    this.dependsOn(tasks);
  }

  @InputFiles
  @PathSensitive(PathSensitivity.ABSOLUTE)
  public List<File> getInputFiles() {
    List<File> collect = tasks.stream()
        .map(taskTaskProvider -> taskTaskProvider.get().getOutputs().getFiles().getSingleFile())
        .collect(Collectors.toList());
    return collect;
  }

  @OutputDirectory
  public File getOutputFile() {
    return resolveFileFromConfiguration().toFile();
  }

  @TaskAction
  public void run() {
    descriptorService
        .combineModuleDescriptors(getProject(), getModulesGeneratorConfig(), getInputFiles(),resolveFileFromConfiguration());
  }

  private Path resolveFileFromConfiguration() {
    return getProject().getBuildDir().toPath()
        .resolve("moduleDescriptors").resolve(getModulesGeneratorConfig().name)
        .resolve("combined-external-library-dependencies");
  }

  public GeodeModuleDescriptorService getDescriptorService() {
    return descriptorService;
  }

  public String getFacetToAssemble() {
    return facetToAssemble;
  }
}
