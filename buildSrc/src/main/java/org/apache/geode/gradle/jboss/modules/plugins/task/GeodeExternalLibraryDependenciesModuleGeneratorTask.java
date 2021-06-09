/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import org.apache.geode.gradle.jboss.modules.plugins.config.GeodeJBossModulesGeneratorConfig;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeModuleDescriptorService;

public class GeodeExternalLibraryDependenciesModuleGeneratorTask extends DefaultTask {
  @Internal
  public List<GeodeJBossModulesGeneratorConfig> configurations;

  @Internal
  public GeodeModuleDescriptorService descriptorService;

  public GeodeExternalLibraryDependenciesModuleGeneratorTask() {
    dependsOn(getProject().getTasks().named("testGenerate"));
  }

  @InputFiles
  @PathSensitive(PathSensitivity.ABSOLUTE)
  public Set<File> getInputFiles() {
    return getProject().getTasks().named("testGenerate").get().getOutputs().getFiles().getFiles();
  }

  @OutputFiles
  public List<File> getOutputFile() {
    return configurations.stream().map(this::resolveFileFromConfiguration)
            .collect(Collectors.toList());
  }

  private File resolveFileFromConfiguration(GeodeJBossModulesGeneratorConfig config) {
    return config.outputRoot.resolve("external-library-dependencies").resolve(getProject().getVersion().toString())
        .resolve("module.xml").toFile();
  }

  @TaskAction
  public void run() {
    configurations.forEach(config -> descriptorService
        .createExternalLibraryDependenciesModuleDescriptor(getProject(), config));
  }

  public List<GeodeJBossModulesGeneratorConfig> getConfigurations() {
    return configurations;
  }

  public GeodeModuleDescriptorService getDescriptorService() {
    return descriptorService;
  }
}
