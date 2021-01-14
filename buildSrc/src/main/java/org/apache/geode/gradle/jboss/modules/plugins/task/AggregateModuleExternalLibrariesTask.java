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
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.gradle.api.Task;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectories;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;

import org.apache.geode.gradle.jboss.modules.plugins.config.ModulesGeneratorConfig;

public class AggregateModuleExternalLibrariesTask extends GeodeJBossTask {

  @Internal
  public String facetToAssemble;

  private Set<TaskProvider<Task>> tasks;

  @Inject
  public AggregateModuleExternalLibrariesTask(String facetToAssemble,
      ModulesGeneratorConfig config) {
    this.facetToAssemble = facetToAssemble;
    this.modulesGeneratorConfig = config;
    String facetTaskName = getFacetTaskName("generateLibraryModuleDescriptors", facetToAssemble);
     this.tasks = getProject().getRootProject().getSubprojects().stream()
     .filter(project -> project.getTasks().findByName(facetTaskName) != null)
     .filter(project ->
     project.getExtensions().getExtraProperties().has("isGeodeExtension")
     && (boolean) project.getExtensions().getExtraProperties().get("isGeodeExtension"))
     .map(project -> project.getTasks().named(facetTaskName))
     .collect(Collectors.toSet());
     this.dependsOn(tasks);
  }

  @OutputDirectories
  public Set<File> getOutputFiles() {
     Set<File> files = this.tasks.stream().map(task ->
     task.get().getOutputs().getFiles().getSingleFile().getParentFile().getParentFile().getParentFile())
     .collect(Collectors.toSet());
     return files;
  }

  @TaskAction
  public void run() {}

  public String getFacetToAssemble() {
    return facetToAssemble;
  }
}
