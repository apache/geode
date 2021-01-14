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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectories;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.OutputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import org.apache.geode.gradle.jboss.modules.plugins.config.ModulesGeneratorConfig;

public class AggregateModuleDescriptorsTask extends GeodeJBossTask {

  @Internal
  public String facetToAssemble;

  @Inject
  public AggregateModuleDescriptorsTask(String facetToAssemble, ModulesGeneratorConfig config) {
    this.facetToAssemble = facetToAssemble;
    this.modulesGeneratorConfig = config;
    String facetTaskName = getFacetTaskName("generateModuleDescriptors", facetToAssemble);
    dependsOn(getProject().getRootProject().getSubprojects().stream()
        .filter(
            project -> project.getTasks().findByName(facetTaskName) != null)
        .map(project -> project.getTasks().named(facetTaskName))
        .collect(Collectors.toSet()));
  }

  @InputFiles
  @PathSensitive(PathSensitivity.ABSOLUTE)
  public Set<File> getInputFiles() {
    String facetTaskName = getFacetTaskName("generateModuleDescriptors", facetToAssemble);
    return getProject().getRootProject().getSubprojects().stream()
        .filter(project -> project.getTasks().findByName(facetTaskName) != null)
        .map(project -> project.getTasks().getByName(facetTaskName).getOutputs()
            .getFiles().getSingleFile())
        .collect(Collectors.toSet());
  }

  @OutputDirectories
  public List<File> getOutputFiles() {
    String facetTaskName = getFacetTaskName("generateModuleDescriptors", facetToAssemble);
    return getProject().getRootProject().getSubprojects().stream()
        .filter(project -> project.getTasks().findByName(facetTaskName) != null)
        .map(project -> project.getTasks().getByName(facetTaskName).getOutputs()
            .getFiles().getSingleFile().getParentFile().getParentFile())
        .collect(Collectors.toList());
  }

  @TaskAction
  public void run() {
  }

  public String getFacetToAssemble() {
    return facetToAssemble;
  }
}
