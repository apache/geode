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

import org.apache.commons.lang3.StringUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.Task;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectories;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;

public class AggregateTestModuleDescriptorsTask extends DefaultTask {

  @Internal
  public String facetToAssemble;

  @Inject
  public AggregateTestModuleDescriptorsTask(String facetToAssemble) {
    this.facetToAssemble = facetToAssemble;
    String facetTaskName = getFacetTaskName("generateTestModuleDescriptors", facetToAssemble);
    Set<TaskProvider<Task>> tasks = getProject().getRootProject().getSubprojects().stream()
            .filter(
                    project -> project.getTasks().findByName(facetTaskName) != null)
            .map(project -> project.getTasks().named(facetTaskName))
            .collect(Collectors.toSet());
    System.out.println("tasks = " + tasks);
    dependsOn(tasks);
  }

  @InputFiles
  @PathSensitive(PathSensitivity.ABSOLUTE)
  public Set<File> getInputFiles() {
    String facetTaskName = getFacetTaskName("generateTestModuleDescriptors", facetToAssemble);
    return getProject().getRootProject().getSubprojects().stream()
        .filter(project -> project.getTasks().findByName(facetTaskName) != null)
        .map(project -> project.getTasks().getByName(facetTaskName).getOutputs()
            .getFiles().getSingleFile())
        .collect(Collectors.toSet());
  }

  @OutputDirectories
  public List<File> getOutputFiles() {
    String facetTaskName = getFacetTaskName("generateTestModuleDescriptors", facetToAssemble);
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

  protected String getFacetTaskName(String baseTaskName, String facet) {
    return facet.equals("main") ? baseTaskName : facet + StringUtils.capitalize(baseTaskName);
  }
}
