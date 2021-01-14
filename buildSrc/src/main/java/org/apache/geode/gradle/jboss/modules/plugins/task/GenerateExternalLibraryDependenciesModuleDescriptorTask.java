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

import javax.inject.Inject;

import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import org.apache.geode.gradle.jboss.modules.plugins.config.ModulesGeneratorConfig;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeModuleDescriptorService;

public class GenerateExternalLibraryDependenciesModuleDescriptorTask extends GeodeJBossTask {
  @Internal
  public GeodeModuleDescriptorService descriptorService;

  @Inject
  public GenerateExternalLibraryDependenciesModuleDescriptorTask(ModulesGeneratorConfig config,
      GeodeModuleDescriptorService descriptorService) {
    this.modulesGeneratorConfig = config;
    this.descriptorService = descriptorService;
  }

  @OutputFile
  public File getOutputFile() {
    return resolveFileFromConfiguration(getModulesGeneratorConfig());
  }

  @TaskAction
  public void run() {
    descriptorService
        .createExternalLibraryDependenciesModuleDescriptor(getProject(),
            getModulesGeneratorConfig());
  }

  private File resolveFileFromConfiguration(ModulesGeneratorConfig config) {
    Path basePath = config.outputRoot.resolve(config.name)
            .resolve("external-library-dependencies");
    return basePath.resolve(getProject().getName() + "-external-library")
        .resolve(getProject().getVersion().toString())
        .resolve("module.xml").toFile();
  }

  public GeodeModuleDescriptorService getDescriptorService() {
    return descriptorService;
  }
}
