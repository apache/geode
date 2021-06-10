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

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.OutputDirectories;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

public class GeodeJBossModulesCombinerTask extends DefaultTask {
  public List<String> getProjectsToInclude() {
    return projectsToInclude;
  }

  @Inject
  public GeodeJBossModulesCombinerTask(List<String> projectsToInclude) {
    this.projectsToInclude = projectsToInclude;
  }

  @OutputDirectories
  public List<File> getOutputFiles() {
    List<File> testGenerate = getProject().getRootProject().getSubprojects().stream()
            .filter(
                    project -> projectsToInclude.isEmpty() || projectsToInclude.contains(project.getName()))
            .map(project -> {
              File parentDirectory = project.getTasks().named("testGenerate").get().getOutputs().getFiles().getSingleFile().getParentFile().getParentFile();
              return parentDirectory;
            }).collect(Collectors.toList());
    return testGenerate;
  }

  @Input
  public List<String> projectsToInclude;

  @TaskAction
  public void run() {
  }

}
