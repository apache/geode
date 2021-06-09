package org.apache.geode.gradle.jboss.modules.plugins.extension;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import groovy.lang.Closure;
import org.gradle.api.Project;

import org.apache.geode.gradle.jboss.modules.plugins.config.GeodeJBossModulesGeneratorConfig;

public class GeodeJBossModulesExtension {
  private final Project project;
  public List<Closure<GeodeJBossModulesGeneratorConfig>> geodeConfigurations = new ArrayList<>();
  public List<String> projectsToExclude = new ArrayList<>();
  public Path assemblyRoot;

  public GeodeJBossModulesExtension(Project project) {
    this.project = project;
  }

  public List<GeodeJBossModulesGeneratorConfig> getConfigList() {
    return geodeConfigurations.stream()
        .map(config -> (GeodeJBossModulesGeneratorConfig)project.configure(new GeodeJBossModulesGeneratorConfig(), config))
            .collect(Collectors.toList());
  }
}