/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.gradle.jboss.modules.plugins.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.apache.commons.lang3.StringUtils;
import org.gradle.api.Project;
import org.gradle.api.UnknownDomainObjectException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ResolvedArtifact;

import org.apache.geode.gradle.jboss.modules.plugins.domain.DependencyWrapper;

public class ProjectUtils {
    private static final Script groovyExtensionScript;

    static {
        try {
            groovyExtensionScript = new GroovyShell().parse(Objects.requireNonNull(ProjectUtils.class.getClassLoader().getResource("ProjectUtilGroovy.groovy")).toURI());
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T invokeGroovyCode(String functionName, String key, Dependency moduleDependency) {
        return (T) groovyExtensionScript.invokeMethod(functionName, new Object[]{moduleDependency, key});
    }

    public static List<DependencyWrapper> getProjectDependenciesForConfiguration(Project project, List<String> configurations) {
        List<DependencyWrapper> dependencies = new LinkedList<>();
        for (String configuration : configurations) {
            try {
                project.getConfigurations()
                        .named(configuration).get()
                        .getDependencies()
                        .forEach(dependency -> {
                            boolean embed = false;
                            boolean hasEmbedExtension = invokeGroovyCode("hasExtension", "embed",
                                dependency);
                            if(hasEmbedExtension) {
                                embed = invokeGroovyCode("getExtension", "embed", dependency);
                            }
                            dependencies.add(new DependencyWrapper(dependency, embed));
                        });
            } catch (UnknownDomainObjectException exception) {
                //ignore the exception
            }
        }
        return dependencies;
    }

    public static List<String> getTargetConfigurations(String facet, String... configurations) {
        List<String> targetConfigurations = new LinkedList<>(Arrays.asList(configurations));
        if (facet != null && !facet.isEmpty()) {
            for (String configuration : configurations) {
                if(!facet.equals("main")){
                    targetConfigurations.add(facet + StringUtils.capitalize(configuration));
                }
            }
        }
        return targetConfigurations;
    }

    public static List<ResolvedArtifact> getResolvedProjectRuntimeArtifacts(Project project, List<String> configurations) {
        Set<ResolvedArtifact> resolvedArtifacts = new HashSet<>();
        for (String configToLookUp : configurations) {
            Configuration configuration = project.getConfigurations().findByName(configToLookUp);
            if(configuration != null) {
                resolvedArtifacts
                    .addAll(configuration.getResolvedConfiguration().getResolvedArtifacts());
            }
        }
        return new LinkedList<>(resolvedArtifacts);
    }
}
