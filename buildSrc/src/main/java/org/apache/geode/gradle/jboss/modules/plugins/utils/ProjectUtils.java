package org.apache.geode.gradle.jboss.modules.plugins.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.apache.commons.lang3.StringUtils;
import org.gradle.api.Project;
import org.gradle.api.UnknownDomainObjectException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ModuleDependency;
import org.gradle.api.artifacts.ResolvedArtifact;

import org.apache.geode.gradle.jboss.modules.plugins.domain.DependencyWrapper;

public class ProjectUtils {
    private static final Script hasExtensionScript;

    static {
        try {
            hasExtensionScript = new GroovyShell().parse(Objects.requireNonNull(org.apache.geode.gradle.jboss.modules.plugins.utils.ProjectUtils.class.getClassLoader().getResource("ProjectUtilGroovy.groovy")).toURI());
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean invokeGroovyCode(ModuleDependency moduleDependency, String key) {
        return (boolean) hasExtensionScript.invokeMethod("hasExtension", new Object[]{moduleDependency, key});
    }

    public static List<DependencyWrapper> getProjectDependenciesForConfiguration(Project project, List<String> configurations) {
        List<DependencyWrapper> dependencies = new LinkedList<>();
        for (String configuration : configurations) {
            try {
                project.getConfigurations()
                        .named(configuration).get()
                        .getDependencies()
                        .forEach(dependency -> {
                            boolean embed = invokeGroovyCode((ModuleDependency) dependency, "embed");
                            dependencies.add(new DependencyWrapper(dependency, embed));
                        });
            } catch (UnknownDomainObjectException exception) {
                //ignore the exception
                return Collections.emptyList();
            }
        }
        return dependencies;
    }

    public static List<String> getTargetConfigurations(String facet, String... configurations) {
        List<String> targetConfigurations = new LinkedList<>(Arrays.asList(configurations));
        if (facet != null && !facet.isEmpty()) {
            for (String configuration : configurations) {
                targetConfigurations.add(facet + StringUtils.capitalize(configuration));
            }
        }
        return targetConfigurations;
    }

    public static List<ResolvedArtifact> getResolvedProjectRuntimeArtifacts(Project project, List<String> configurations) {
        List<ResolvedArtifact> resolvedArtifacts = new LinkedList<>();
        for (String configToLookUp : configurations) {
            Configuration configuration = project.getConfigurations().named(configToLookUp).get();
            resolvedArtifacts.addAll(configuration.getResolvedConfiguration().getResolvedArtifacts());
        }
        return resolvedArtifacts;
    }
}
