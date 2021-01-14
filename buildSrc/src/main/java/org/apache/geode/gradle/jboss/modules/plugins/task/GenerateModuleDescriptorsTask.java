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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.jvm.tasks.Jar;

import org.apache.geode.gradle.jboss.modules.plugins.config.ModulesGeneratorConfig;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeModuleDescriptorService;

public class GenerateModuleDescriptorsTask extends GeodeJBossTask {

  @Internal
  public GeodeModuleDescriptorService descriptorService;

  @Inject
  public GenerateModuleDescriptorsTask(ModulesGeneratorConfig configuration,
      GeodeModuleDescriptorService descriptorService) {
    this.modulesGeneratorConfig = configuration;
    this.descriptorService = descriptorService;
    if (getProject().getPluginManager().hasPlugin("java-library")) {
      if (configuration.shouldAssembleFromSource()) {
        addDependencies("compile" + StringUtils.capitalize(configuration.name) + "Java");
        addDependencies("process" + StringUtils.capitalize(configuration.name) + "Resources");
        addDependencies("createVersionPropertiesFile");
      } else {
        dependsOn(getProject().getTasks().withType(Jar.class).named("jar"));
      }
    }
  }

  private void addDependencies(String taskName) {
    Task facetProcessResourcesTask = getProject().getTasks().findByName(taskName);
    if (facetProcessResourcesTask != null) {
      dependsOn(facetProcessResourcesTask);
    }
  }

  @InputFiles
  @PathSensitive(PathSensitivity.ABSOLUTE)
  public Set<File> getInputFiles() {
    if (getProject().getPluginManager().hasPlugin("java-library")) {
      if (modulesGeneratorConfig.shouldAssembleFromSource()) {
        String taskName = "compile" + getTaskNameFacet() + "Java";
        Task compileFacetTask = getProject().getTasks().findByName(taskName);
        if (compileFacetTask != null) {
          return getProject().getLayout().files(compileFacetTask).getFiles();
        }
      } else {
        return getProject().getLayout()
            .files(getProject().getTasks().withType(Jar.class).named("jar")).getFiles();
      }
    }
    return Collections.emptySet();
  }

  private String getTaskNameFacet() {
    return modulesGeneratorConfig.name.equals("main") ? "" : StringUtils.capitalize(modulesGeneratorConfig.name);
  }

  @OutputFile
  public File getOutputFile() {
    return resolveFileFromConfiguration(getModulesGeneratorConfig());
  }

  @TaskAction
  public void run() {
    if (containsExistingDescriptor(getProject(), getModulesGeneratorConfig())) {
      copyExistingDescriptorFile(getProject(), getModulesGeneratorConfig());
    } else {
      Set<File> inputFiles = getInputFiles();
      if (modulesGeneratorConfig.shouldAssembleFromSource() || inputFiles.isEmpty()) {
        descriptorService.createModuleDescriptor(getProject(), getModulesGeneratorConfig(), null);
      } else {
        descriptorService.createModuleDescriptor(getProject(), getModulesGeneratorConfig(),
            inputFiles.iterator().next());
      }
    }
  }

  private void copyExistingDescriptorFile(Project project, ModulesGeneratorConfig config) {
    Path predefineModuleDescriptor =
        config.alternativeDescriptorRoot.resolve(config.name).resolve(project.getName());
    Path targetPath = config.outputRoot.resolve(config.name).resolve(project.getName());
    recursiveDelete(targetPath);
    copyCursively(predefineModuleDescriptor, targetPath);
  }

  private void copyCursively(Path predefineModuleDescriptor, Path targetPath) {
    try {
      // create a stream
      Stream<Path> files = Files.walk(predefineModuleDescriptor);

      // delete directory including files and sub-folders
      files.map(Path::toFile)
          .forEach(file -> {
            if (file.isDirectory()) {
              targetPath.resolve(file.getName()).toFile().mkdirs();
            } else {
              Path target = targetPath;
              for (String path : file.getParent()
                  .substring(predefineModuleDescriptor.toString().length() + 1).split("/")) {
                target = targetPath.resolve(path);
              }
              try {
                Files.copy(file.toPath(), target.resolve(file.getName()));
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          });

      // close the stream
      files.close();

      targetPath.toFile().delete();

    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  private void recursiveDelete(Path targetPath) {
    try {
      // create a stream
      Stream<Path> files = Files.walk(targetPath);

      // delete directory including files and sub-folders
      files.sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);

      // close the stream
      files.close();

      targetPath.toFile().delete();

    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  private boolean containsExistingDescriptor(Project project, ModulesGeneratorConfig config) {
    if (config.alternativeDescriptorRoot == null) {
      return false;
    }
    Path existingDescriptor = config.alternativeDescriptorRoot.resolve(config.name)
        .resolve(project.getName());
    File possibleDescriptorLocation = existingDescriptor.toFile();
    return possibleDescriptorLocation.exists();
  }

  private File resolveFileFromConfiguration(ModulesGeneratorConfig config) {
    return config.outputRoot.resolve(config.name)
        .resolve(getProject().getName())
        .resolve(getProject().getVersion().toString()).resolve("module.xml").toFile();
  }

  public GeodeModuleDescriptorService getDescriptorService() {
    return descriptorService;
  }
}
