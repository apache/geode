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
package org.apache.geode.gradle.jboss.modules.plugins;

import org.apache.geode.gradle.jboss.modules.plugins.config.GeodeJBossModulesGeneratorConfig;
import org.apache.geode.gradle.jboss.modules.plugins.extension.GeodeJBossModulesExtension;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeJBossModuleDescriptorService;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeModuleDescriptorService;
import org.apache.geode.gradle.jboss.modules.plugins.task.GeodeCombineModuleDescriptorsTask;
import org.apache.geode.gradle.jboss.modules.plugins.task.GeodeExternalLibraryDependenciesModuleGeneratorTask;
import org.apache.geode.gradle.jboss.modules.plugins.task.GeodeJBossModuleGeneratorTask;
import org.apache.geode.gradle.jboss.modules.plugins.task.GeodeJBossModulesCombinerTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GeodeJBossModulesGeneratorPlugin implements Plugin<Project> {

//  private final List<String> projectInclusions = Collections.emptyList();
  private final List<String> projectInclusions = Arrays.asList("geode-custom-jar-deployments",
      "geode-extensions", "geode-apis-compatible-with-redis", "geode-common", "geode-connectors",
      "geode-core", "geode-cq", "geode-deployment-jboss-modules", "geode-gfsh",
      "geode-http-service", "geode-log4j", "geode-logging", "geode-lucene", "geode-management",
      "geode-membership", "geode-memcached", "geode-rebalancer", "geode-serialization",
      "geode-tcp-server", "geode-unsafe", "geode-wan");

  private Project assemblyRootProject;
  private final GeodeModuleDescriptorService moduleDescriptorService;

  public GeodeJBossModulesGeneratorPlugin() {
    moduleDescriptorService = new GeodeJBossModuleDescriptorService();
  }

  @Override
  public void apply(Project project) {
    GeodeJBossModulesExtension jbossModulesExtension = project.getExtensions()
        .create("jbossModulesExtension", GeodeJBossModulesExtension.class, project);

    if (projectInclusions.isEmpty() || projectInclusions.contains(project.getName())) {
      project.getConfigurations().create("jbossModular");

      // register task to create module descriptor for each project
      registerModuleDescriptorGenerationTask(project, jbossModulesExtension,
          moduleDescriptorService);

      // register task to create external library dependency module or each project
      registerExternalLibraryDescriptorGenerationTask(project, jbossModulesExtension,
          moduleDescriptorService);

      // register task to combine all module descriptors in root project
    }

    if (assemblyRootProject == null) {
//      assemblyRootProject = project.getRootProject();
      assemblyRootProject = project.getRootProject().project(":geode-assembly");
    }

    if (project.equals(assemblyRootProject)) {
      project.afterEvaluate(project1 -> {
        registerLibraryCombinerTask(project, jbossModulesExtension, moduleDescriptorService);
      });
    }
  }

  private void registerLibraryCombinerTask(Project project,
      GeodeJBossModulesExtension jbossModulesExtension,
      GeodeModuleDescriptorService generator) {
    Class geodeCombineModuleDescriptorsTaskClass = GeodeCombineModuleDescriptorsTask.class;

    project.getTasks()
        .create("combineExternalModuleDescriptors", geodeCombineModuleDescriptorsTaskClass,
            projectInclusions, Collections.singletonList(new GeodeJBossModulesGeneratorConfig(null,
                project.getBuildDir().toPath().resolve("moduleDescriptors"), "",
                projectInclusions)),
            project.getBuildDir().toPath(), generator);
    Class geodeJBossModulesCombinerTaskClass = GeodeJBossModulesCombinerTask.class;
    project.getTasks()
        .create("combineModuleDescriptors", geodeJBossModulesCombinerTaskClass,
            projectInclusions);
  }

  private void registerExternalLibraryDescriptorGenerationTask(Project project,
      GeodeJBossModulesExtension jbossModulesExtension,
      GeodeModuleDescriptorService generator) {
    Class thirdPartyJBossModuleGeneratorTaskClass =
        GeodeExternalLibraryDependenciesModuleGeneratorTask.class;
    project.getTasks()
        .create("generateExternalDependenciesModule", thirdPartyJBossModuleGeneratorTaskClass,
            task -> {
              GeodeExternalLibraryDependenciesModuleGeneratorTask thirdPartyJBossModuleGeneratorTask =
                  (GeodeExternalLibraryDependenciesModuleGeneratorTask) task;
              thirdPartyJBossModuleGeneratorTask.configurations =
                  Collections.singletonList(new GeodeJBossModulesGeneratorConfig("org.apache.geode.distributed.ServerLauncher",
                      project.getBuildDir().toPath().resolve("moduleDescriptors"), "",
                      projectInclusions));
              // jbossModulesExtension.getConfigList();
              thirdPartyJBossModuleGeneratorTask.descriptorService = generator;
            });
  }

  private void registerModuleDescriptorGenerationTask(Project project,
      GeodeJBossModulesExtension jbossModulesExtension,
      GeodeModuleDescriptorService generator) {
    Class geodeJBossModuleGeneratorTaskClass = GeodeJBossModuleGeneratorTask.class;
    project.getTasks().create("testGenerate", geodeJBossModuleGeneratorTaskClass, task -> {
      GeodeJBossModuleGeneratorTask geodeJBossModuleGeneratorTask =
          (GeodeJBossModuleGeneratorTask) task;
      geodeJBossModuleGeneratorTask.configurations = Collections.singletonList(new GeodeJBossModulesGeneratorConfig("org.apache.geode.distributed.ServerLauncher",
          project.getBuildDir().toPath().resolve("moduleDescriptors"), "",
          projectInclusions));
    //jbossModulesExtension.getConfigList();
      geodeJBossModuleGeneratorTask.descriptorService = generator;
    });
  }
}
