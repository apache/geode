/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.geode.gradle.jboss.modules.plugins.config;

import java.nio.file.Path;
import java.util.List;

import org.gradle.api.Project;

public class ModulesGeneratorConfig {
    public String mainClass;

    public Path outputRoot;

    public String name;

    public List<String> jbossJdkModulesDependencies;

    public List<String> externalLibraryJbossJDKModules;

    public Boolean assembleFromSource;

    public Path alternativeDescriptorRoot;

    public Project parentModule;

    public List<String> packagesToExport;

    public List<String> packagesToExclude;

    public ModulesGeneratorConfig(String name) {
        this.name = name;
    }

    public ModulesGeneratorConfig(String name, Path outputRoot) {
        this(name);
        this.outputRoot = outputRoot;
    }

    //This is for testing purposes only
    public ModulesGeneratorConfig(String name, String mainClass, Path outputRoot) {
        this(name,outputRoot);
        this.mainClass = mainClass;
    }

    public boolean shouldAssembleFromSource() {
        return assembleFromSource != null && assembleFromSource;
    }

    @Override
    public String toString() {
        return "ModulesGeneratorConfig{" +
                "mainClass='" + mainClass + '\'' +
                ", outputRoot=" + outputRoot +
                ", name='" + name + '\'' +
                ", jbossJdkModulesDependencies=" + jbossJdkModulesDependencies +
                ", externalLibraryJbossJDKModules=" + externalLibraryJbossJDKModules +
                ", assembleFromSource=" + assembleFromSource +
                ", alternativeDescriptorRoot=" + alternativeDescriptorRoot +
                ", parentModule=" + parentModule +
                ", packagesToExport=" + packagesToExport +
                ", packagesToExclude=" + packagesToExclude +
                '}';
    }
}