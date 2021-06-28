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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskProvider;

import org.apache.geode.gradle.jboss.modules.plugins.config.GeodeJBossModulesGeneratorConfig;
import org.apache.geode.gradle.jboss.modules.plugins.extension.GeodeJBossModulesExtension;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeJBossModuleDescriptorService;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeModuleDescriptorService;
import org.apache.geode.gradle.jboss.modules.plugins.task.GeodeCombineModuleDescriptorsTask;
import org.apache.geode.gradle.jboss.modules.plugins.task.GeodeExternalLibraryDependenciesModuleGeneratorTask;
import org.apache.geode.gradle.jboss.modules.plugins.task.GeodeJBossModuleGeneratorTask;
import org.apache.geode.gradle.jboss.modules.plugins.task.GeodeJBossModulesCombinerTask;

public class GeodeJBossModulesGeneratorPlugin implements Plugin<Project> {

  private final GeodeModuleDescriptorService moduleDescriptorService;

  public GeodeJBossModulesGeneratorPlugin() {
    moduleDescriptorService = new GeodeJBossModuleDescriptorService();
  }

  private static GeodeJBossModulesGeneratorConfig defaultConfigFromGlobal(
      GeodeJBossModulesGeneratorConfig globalConfig, GeodeJBossModulesGeneratorConfig config) {
    GeodeJBossModulesGeneratorConfig newConfig = new GeodeJBossModulesGeneratorConfig(config.name);
    newConfig.outputRoot = config.outputRoot == null ? globalConfig.outputRoot : config.outputRoot;
    newConfig.mainClass = config.mainClass == null ? globalConfig.mainClass : config.mainClass;
    newConfig.assembleFromSource = config.assembleFromSource == null ?
        (globalConfig.assembleFromSource != null && globalConfig.assembleFromSource)
        : config.assembleFromSource;
    newConfig.jbossJdkModules = combineLists(globalConfig.jbossJdkModules, config.jbossJdkModules);
    return newConfig;
  }

  private static List<String> combineLists(final List<String> list1, final List<String> list2) {
    if (list1 == null) {
      return list2 != null ? list2 : Collections.emptyList();
    }
    if (list2 == null) {
      return list1;
    }
    Set<String> strings = new HashSet<>(list1);
    strings.addAll(list2);
    return new LinkedList<>(strings);
  }

  @Override
  public void apply(Project project) {
    NamedDomainObjectContainer<GeodeJBossModulesGeneratorConfig> configurationContainer =
        project.container(GeodeJBossModulesGeneratorConfig.class,
            GeodeJBossModulesGeneratorConfig::new);
    GeodeJBossModulesExtension jbossModulesExtension = project.getExtensions()
        .create("jbossModulesExtension", GeodeJBossModulesExtension.class,
            configurationContainer);

    project.getConfigurations().create("jbossModular");

    project.afterEvaluate(project1 -> {
      NamedDomainObjectContainer<GeodeJBossModulesGeneratorConfig> geodeConfigurations =
          jbossModulesExtension.geodeConfigurations;
      Map<String, GeodeJBossModulesGeneratorConfig> configurations =
          geodeConfigurations.getAsMap();

      for (GeodeJBossModulesGeneratorConfig config : configurations.values()) {
        GeodeJBossModulesGeneratorConfig globalConfig = configurations.get("main");
        if(globalConfig == null) {
          globalConfig = config;
        }

        // register task to create module descriptor for each project
        GeodeJBossModulesGeneratorConfig defaultedConfig = defaultConfigFromGlobal(globalConfig, config);
        registerModuleDescriptorGenerationTask(project1, defaultedConfig,
            moduleDescriptorService);

        // register task to create external library dependency module or each project
        registerExternalLibraryDescriptorGenerationTask(project1, defaultedConfig,
            moduleDescriptorService);

        if (!config.name.equals("main") && project.getTasksByName(config.name, false).size() == 1) {

        }
      }

      if (jbossModulesExtension.isAssemblyProject) {
        jbossModulesExtension.facetsToAssemble.forEach(facetName -> {
          registerLibraryCombinerTask(project1, facetName, moduleDescriptorService);
          registerModuleCombinerTask(project1, facetName);
        });
      }
    });
  }

  private TaskProvider<?> registerLibraryCombinerTask(Project project, String facetToAssemble,
                                                      GeodeModuleDescriptorService descriptorService) {
    Class geodeCombineModuleDescriptorsTaskClass = GeodeCombineModuleDescriptorsTask.class;
    GeodeJBossModulesGeneratorConfig
        config =
        new GeodeJBossModulesGeneratorConfig(facetToAssemble, null,
            project.getBuildDir().toPath().resolve("moduleDescriptors"));
    return project.getTasks()
        .register(getFacetTaskName("combineLibraryModuleDescriptors", facetToAssemble), geodeCombineModuleDescriptorsTaskClass,
            facetToAssemble, config, descriptorService);
  }

  private TaskProvider<?> registerModuleCombinerTask(Project project, String facetToAssemble) {
    Class geodeJBossModulesCombinerTaskClass = GeodeJBossModulesCombinerTask.class;
    GeodeJBossModulesGeneratorConfig
        config =
        new GeodeJBossModulesGeneratorConfig(facetToAssemble, null,
            project.getBuildDir().toPath().resolve("moduleDescriptors"));
    return project.getTasks()
        .register(getFacetTaskName("combineModuleDescriptors", facetToAssemble), geodeJBossModulesCombinerTaskClass, facetToAssemble,
            config);
  }

  private TaskProvider<?> registerExternalLibraryDescriptorGenerationTask(Project project,
                                                                          GeodeJBossModulesGeneratorConfig configuration,
                                                                          GeodeModuleDescriptorService descriptorService) {
    Class thirdPartyJBossModuleGeneratorTaskClass =
        GeodeExternalLibraryDependenciesModuleGeneratorTask.class;
    return project.getTasks()
        .register(getFacetTaskName("generateLibraryModuleDescriptors", configuration.name),
            thirdPartyJBossModuleGeneratorTaskClass, configuration, descriptorService);
  }

  private TaskProvider<?> registerModuleDescriptorGenerationTask(Project project,
                                                                 GeodeJBossModulesGeneratorConfig configuration,
                                                                 GeodeModuleDescriptorService descriptorService) {
    Class geodeJBossModuleGeneratorTaskClass = GeodeJBossModuleGeneratorTask.class;
    return project.getTasks()
        .register(getFacetTaskName("generateModuleDescriptors", configuration.name),
            geodeJBossModuleGeneratorTaskClass, configuration, descriptorService);
  }

  private String getFacetTaskName(String baseTaskName, String facet) {
    return facet.equals("main") ? baseTaskName : facet + StringUtils.capitalize(baseTaskName);
  }
}
