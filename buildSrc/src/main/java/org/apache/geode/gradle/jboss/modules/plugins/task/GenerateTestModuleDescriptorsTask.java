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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import org.apache.geode.gradle.jboss.modules.plugins.generator.domain.ModuleDependency;
import org.apache.geode.gradle.jboss.modules.plugins.generator.xml.JBossModuleDescriptorGenerator;

public class GenerateTestModuleDescriptorsTask extends DefaultTask {

  // path to libs directory relative to module.xml location inside geode-assembly install directory
  private static final String LIB_PATH_PREFIX = "../../../../../../lib/";
  private static final String GEODE = "geode";

  @Inject
  public GenerateTestModuleDescriptorsTask() {
    if (getProject().getPluginManager().hasPlugin("java-library")) {
      addDependencies("compileJava");
      addDependencies("processResources");
      addDependencies("compile" + StringUtils.capitalize("distributedTest") + "Java");
      addDependencies("process" + StringUtils.capitalize("distributedTest") + "Resources");
    }
  }

  private void addDependencies(String taskName) {
    Task facetProcessResourcesTask = getProject().getTasks().findByName(taskName);
    if (facetProcessResourcesTask != null) {
      dependsOn(facetProcessResourcesTask);
    }
  }

  @OutputFile
  public File getOutputFile() {
    return getRootPath().resolve(GEODE).resolve(getProject().getVersion().toString())
        .resolve("module.xml").toFile();
  }

  private Path getRootPath() {
    return getProject().getBuildDir().toPath().resolve("moduleDescriptors").resolve("dunit")
        .resolve(getProject().getName());
  }

  @TaskAction
  public void run() {
    JBossModuleDescriptorGenerator jBossModuleDescriptorGenerator =
        new JBossModuleDescriptorGenerator();

    Collection<String> resources = generateResourceRoots(getProject());
    resources.addAll(getProject().getConfigurations()
        .getByName("distributedTestRuntimeClasspath").getResolvedConfiguration()
        .getResolvedArtifacts().stream()
        .map(resolvedArtifact -> {
          String artifactFileName = resolvedArtifact.getFile().getAbsolutePath();
          if (artifactFileName.contains("geode-deployment-chained-classloader")) {
            return artifactFileName.replace("geode-deployment-chained-classloader",
                "geode-deployment-jboss-modules");
          }
          return artifactFileName;
        })
        .sorted()
        .collect(Collectors.toList()));

    if (getProject().getTasks().findByName("war") != null) {
      String war =
          LIB_PATH_PREFIX + getProject().getTasks().findByName("war").getOutputs().getFiles()
              .getSingleFile().getName();
      resources.add(war);
    }

    List<ModuleDependency> modules = new LinkedList<>();
    modules.add(new ModuleDependency("java.se", false, false));
    modules.add(new ModuleDependency("jdk.unsupported", false, false));
    modules.add(new ModuleDependency("jdk.scripting.nashorn", false, false));

    jBossModuleDescriptorGenerator.generate(getRootPath(), GEODE,
        getProject().getVersion().toString(), resources, modules,
        "org.apache.geode.test.dunit.internal.ChildVM", Collections.EMPTY_LIST,
        Collections.EMPTY_LIST, Collections.EMPTY_LIST);

    jBossModuleDescriptorGenerator.generateAlias(getRootPath(), GEODE,
        getProject().getVersion().toString());
  }

  private Collection<String> generateResourceRoots(Project project) {

    Collection<String> resourceRoots = new LinkedList<>();
    String[] facets = new String[] {"commonTest", "distributedTest", "main"};

    for (String facet : facets) {
      validateAndAddResourceRoot(resourceRoots,
          project.getBuildDir().toPath().resolve("classes").resolve("java").resolve(facet)
              .toString());
      validateAndAddResourceRoot(resourceRoots,
          project.getBuildDir().toPath().resolve("resources").resolve(facet).toString());
      validateAndAddResourceRoot(resourceRoots,
          project.getBuildDir().toPath().resolve("generated-resources").resolve(facet)
              .toString());
    }
    return resourceRoots;
  }

  private void validateAndAddResourceRoot(Collection<String> resourceRoots,
      String resourceRootToValidateAndAdd) {
    if (new File(resourceRootToValidateAndAdd).exists()) {
      resourceRoots.add(resourceRootToValidateAndAdd);
    }
  }
}
