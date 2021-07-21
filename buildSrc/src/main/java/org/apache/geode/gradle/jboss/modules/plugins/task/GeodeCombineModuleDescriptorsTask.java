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
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class GeodeCombineModuleDescriptorsTask extends GeodeJBossTask {

  @Internal
  public GeodeModuleDescriptorService descriptorService;

  @Internal
  public String facetToAssemble;

  @Inject
  public GeodeCombineModuleDescriptorsTask(
      String facetToAssemble,
      GeodeJBossModulesGeneratorConfig configuration,
      GeodeModuleDescriptorService descriptorService) {
    this.descriptorService = descriptorService;
    this.configuration = configuration;
    this.facetToAssemble = facetToAssemble;

    String facetTaskName = getFacetTaskName("generateLibraryModuleDescriptors", facetToAssemble);

    dependsOn(getProject().getRootProject().getSubprojects().stream()
        .filter(
            project -> project.getTasks().findByName(facetTaskName) != null)
        .map(project -> project.getTasks().named(facetTaskName))
        .collect(Collectors.toSet()));
  }

  @InputFiles
  @PathSensitive(PathSensitivity.ABSOLUTE)
  public List<File> getInputFiles() {
    String facetTaskName = getFacetTaskName("generateLibraryModuleDescriptors", facetToAssemble);

    return getProject().getRootProject().getSubprojects().stream()
        .filter(
            project -> project.getTasks().findByName(facetTaskName) != null)
        .map(project -> project.getTasks().getByName(facetTaskName)
            .getOutputs()
            .getFiles().getSingleFile()).collect(Collectors.toList());
  }

  @OutputDirectory
  public File getOutputFile() {
    return resolveFileFromConfiguration();
  }

  @TaskAction
  public void run() {
    descriptorService
        .combineModuleDescriptors(getProject(), configuration, getInputFiles());
  }

  private File resolveFileFromConfiguration() {
    return getProject().getBuildDir().toPath()
        .resolve("moduleDescriptors").resolve(configuration.name).resolve("external-library-dependencies").toFile();
  }

  public GeodeModuleDescriptorService getDescriptorService() {
    return descriptorService;
  }

  public String getFacetToAssemble() {
    return facetToAssemble;
  }
}
