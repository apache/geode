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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.geode.gradle.jboss.modules.plugins.task.AggregateModuleExternalLibrariesTask;
import org.apache.geode.gradle.jboss.modules.plugins.utils.ModuleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskProvider;

import org.apache.geode.gradle.jboss.modules.plugins.config.ModulesGeneratorConfig;
import org.apache.geode.gradle.jboss.modules.plugins.extension.GeodeJBossModulesExtension;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeJBossModuleDescriptorService;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeModuleDescriptorService;
import org.apache.geode.gradle.jboss.modules.plugins.task.AggregateModuleDescriptorsTask;
import org.apache.geode.gradle.jboss.modules.plugins.task.CombineExternalLibraryModuleDescriptorsTask;
import org.apache.geode.gradle.jboss.modules.plugins.task.GenerateExternalLibraryDependenciesModuleDescriptorTask;
import org.apache.geode.gradle.jboss.modules.plugins.task.GenerateModuleDescriptorsTask;

public class GeodeJBossModulesGeneratorPlugin implements Plugin<Project> {

  private final Map<String, GeodeModuleDescriptorService> configModuleDescriptorService =
      new HashMap<>();

  @Override
  public void apply(Project project) {

    NamedDomainObjectContainer<ModulesGeneratorConfig> configurationContainer =
        project.container(ModulesGeneratorConfig.class,
            ModulesGeneratorConfig::new);
    GeodeJBossModulesExtension jbossModulesExtension = project.getExtensions()
        .create("jbossModulesExtension", GeodeJBossModulesExtension.class,
            configurationContainer);

    project.getConfigurations().create("jbossModular");

    project.afterEvaluate(project1 -> {
      NamedDomainObjectContainer<ModulesGeneratorConfig> geodeConfigurations =
          jbossModulesExtension.geodeConfigurations;

      Map<String, ModulesGeneratorConfig> configurations =
          geodeConfigurations.getAsMap();

      for (ModulesGeneratorConfig config : configurations.values()) {
        GeodeModuleDescriptorService moduleDescriptorService =
            getGeodeModuleDescriptorService(config.name);
        ModulesGeneratorConfig globalConfig =
            configurations.get("main") == null ? config : configurations.get("main");

        // register task to create module descriptor for each project
        ModulesGeneratorConfig defaultedConfig =
            ModuleUtils.defaultConfigFromGlobal(globalConfig, config);
        registerModuleDescriptorGenerationTask(project1, defaultedConfig, moduleDescriptorService);

        registerExternalLibraryDescriptorGenerationTask(project1, defaultedConfig,
            moduleDescriptorService);


        if (jbossModulesExtension.isAssemblyProject) {
          jbossModulesExtension.facetsToAssemble.forEach(facetName -> {
            registerExternalLibraryCombinerTask(project1, facetName, moduleDescriptorService,
                defaultedConfig);
            registerModuleCombinerTask(project1, facetName);
            registerModuleExternalLibraryCombinerTask(project1, facetName);
          });
        }
      }
    });
  }

  private synchronized GeodeModuleDescriptorService getGeodeModuleDescriptorService(
      String configName) {
    if (!configModuleDescriptorService.containsKey(configName)) {
      configModuleDescriptorService.put(configName, new GeodeJBossModuleDescriptorService());
    }
    return configModuleDescriptorService.get(configName);
  }

  private TaskProvider<?> registerExternalLibraryCombinerTask(Project project,
      String facetToAssemble,
      GeodeModuleDescriptorService descriptorService,
      ModulesGeneratorConfig config) {
    Class geodeCombineModuleDescriptorsTaskClass =
        CombineExternalLibraryModuleDescriptorsTask.class;
    return project.getTasks()
        .register(getFacetTaskName("combineLibraryModuleDescriptors", facetToAssemble),
            geodeCombineModuleDescriptorsTaskClass, facetToAssemble, config, descriptorService);
  }

  private TaskProvider<?> registerModuleCombinerTask(Project project, String facetToAssemble) {
    Class geodeJBossModulesCombinerTaskClass = AggregateModuleDescriptorsTask.class;
    ModulesGeneratorConfig config =
        new ModulesGeneratorConfig(facetToAssemble,
            project.getBuildDir().toPath().resolve("moduleDescriptors"));
    return project.getTasks()
        .register(getFacetTaskName("combineModuleDescriptors", facetToAssemble),
            geodeJBossModulesCombinerTaskClass, facetToAssemble, config);

  }

  private TaskProvider<?> registerModuleExternalLibraryCombinerTask(Project project, String facetToAssemble) {
    Class geodeJBossModulesCombinerTaskClass = AggregateModuleExternalLibrariesTask.class;
    ModulesGeneratorConfig config =
        new ModulesGeneratorConfig(facetToAssemble,
            project.getBuildDir().toPath().resolve("moduleDescriptors"));
    return project.getTasks()
        .register(getFacetTaskName("combineModuleExternalLibraryDescriptors", facetToAssemble),
            geodeJBossModulesCombinerTaskClass, facetToAssemble, config);
  }

  private TaskProvider<?> registerExternalLibraryDescriptorGenerationTask(Project project,
      ModulesGeneratorConfig configuration,
      GeodeModuleDescriptorService descriptorService) {
    Class thirdPartyJBossModuleGeneratorTaskClass =
        GenerateExternalLibraryDependenciesModuleDescriptorTask.class;
    return project.getTasks()
        .register(getFacetTaskName("generateLibraryModuleDescriptors", configuration.name),
            thirdPartyJBossModuleGeneratorTaskClass, configuration, descriptorService);
  }

  private TaskProvider<?> registerModuleDescriptorGenerationTask(Project project,
      ModulesGeneratorConfig configuration,
      GeodeModuleDescriptorService descriptorService) {
    Class geodeJBossModuleGeneratorTaskClass = GenerateModuleDescriptorsTask.class;

    TaskProvider generateModuleDescriptors = project.getTasks()
        .register(getFacetTaskName("generateModuleDescriptors", configuration.name),
            geodeJBossModuleGeneratorTaskClass, configuration, descriptorService);
    return generateModuleDescriptors;
  }

  private String getFacetTaskName(String baseTaskName, String facet) {
    return facet.equals("main") ? baseTaskName : facet + StringUtils.capitalize(baseTaskName);
  }
}
