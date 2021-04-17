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
import java.util.Collections;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.gradle.api.Task;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.jvm.tasks.Jar;

import org.apache.geode.gradle.jboss.modules.plugins.config.GeodeJBossModulesGeneratorConfig;
import org.apache.geode.gradle.jboss.modules.plugins.services.GeodeModuleDescriptorService;

public class GeodeJBossModuleGeneratorTask extends GeodeJBossTask {

    @Internal
    public GeodeModuleDescriptorService descriptorService;

    @Inject
    public GeodeJBossModuleGeneratorTask(GeodeJBossModulesGeneratorConfig configuration, GeodeModuleDescriptorService descriptorService) {
        this.configuration = configuration;
        this.descriptorService = descriptorService;
        if(getProject().getPluginManager().hasPlugin("java-library")) {
            if(configuration.shouldAssembleFromSource()) {
                Task facetCompileTask = getProject().getTasks()
                        .findByName("compile" + StringUtils.capitalize(configuration.name) + "Java");
                if(facetCompileTask != null) {
                    dependsOn(facetCompileTask);
                }
            } else {
                dependsOn(getProject().getTasks().withType(Jar.class).named("jar"));
            }
        }
    }

    @InputFiles
    @PathSensitive(PathSensitivity.ABSOLUTE)
    public Set<File> getInputFiles() {
        if (getProject().getPluginManager().hasPlugin("java-library")) {
            if (configuration.shouldAssembleFromSource()) {
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
        return configuration.name.equals("main")? "" : StringUtils.capitalize(configuration.name);
    }

    @OutputFile
    public File getOutputFile() {
        return resolveFileFromConfiguration(getConfiguration());
    }

    @TaskAction
    public void run() {
        Set<File> inputFiles = getInputFiles();
        if(configuration.shouldAssembleFromSource() || inputFiles.isEmpty()) {
            descriptorService.createModuleDescriptor(getProject(), getConfiguration(), null);
        } else {
            descriptorService.createModuleDescriptor(getProject(), getConfiguration(),
                inputFiles.iterator().next());
        }
    }

    private File resolveFileFromConfiguration(GeodeJBossModulesGeneratorConfig config) {
        return config.outputRoot.resolve(config.name)
            .resolve(getProject().getName())
            .resolve(getProject().getVersion().toString()).resolve("module.xml").toFile();
    }

    public GeodeModuleDescriptorService getDescriptorService() {
        return descriptorService;
    }
}
