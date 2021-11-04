package org.apache.geode.gradle.jboss.modules.plugins.task;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.geode.gradle.jboss.modules.plugins.generator.domain.ModuleDependency;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import org.apache.geode.gradle.jboss.modules.plugins.generator.xml.JBossModuleDescriptorGenerator;

public class GenerateTestModuleDescriptorsTask extends DefaultTask {

  private static final String LIB_PATH_PREFIX = "../../../../../lib/";

  @Inject
  public GenerateTestModuleDescriptorsTask() {
    if (getProject().getPluginManager().hasPlugin("java-library")) {
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
    return getRootPath()
        .resolve("module.xml").toFile();
  }

  private Path getRootPath() {
    return getProject().getBuildDir().toPath().resolve("moduleDescriptors").resolve("dunit");
  }

  @TaskAction
  public void run() {
    JBossModuleDescriptorGenerator jBossModuleDescriptorGenerator =
        new JBossModuleDescriptorGenerator();

    Set<String> resources = getProject().getConfigurations()
        .getByName("distributedTestRuntimeClasspath").getResolvedConfiguration()
        .getResolvedArtifacts().stream()
        .map(resolvedArtifact -> LIB_PATH_PREFIX + resolvedArtifact.getFile().getName())
        .collect(Collectors.toSet());

    resources.addAll(generateResourceRoots(getProject()));

    List<ModuleDependency> modules =
        Collections.singletonList(new ModuleDependency("java.se", false, false));

    jBossModuleDescriptorGenerator.generate(getRootPath(), getProject().getName(),
        getProject().getVersion().toString(), resources, modules,
        "org.apache.geode.test.dunit.internal.ChildVM", Collections.EMPTY_LIST,
        Collections.EMPTY_LIST, Collections.EMPTY_LIST);
  }

  private Set<String> generateResourceRoots(Project project) {

    Set<String> resourceRoots = new TreeSet<>();

    String distributedTest = "distributedTest";

    validateAndAddResourceRoot(resourceRoots,
        project.getBuildDir().toPath().resolve("classes").resolve("java").resolve(distributedTest)
            .toString());
    validateAndAddResourceRoot(resourceRoots,
        project.getBuildDir().toPath().resolve("resources").resolve(distributedTest).toString());
    validateAndAddResourceRoot(resourceRoots,
        project.getBuildDir().toPath().resolve("generated-resources").resolve(distributedTest)
            .toString());
    return resourceRoots;
  }

  private void validateAndAddResourceRoot(Set<String> resourceRoots,
      String resourceRootToValidateAndAdd) {
    if (new File(resourceRootToValidateAndAdd).exists()) {
      resourceRoots.add(resourceRootToValidateAndAdd);
    }
  }
}
