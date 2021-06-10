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

import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class GeodeJBossModulesGeneratorConfig {

    @Input
    @Optional
    public String mainClass = null;

    @Input
    public Path outputRoot = new File("").toPath();

    @Input
    @Optional
    public String facetName = "";

    @Input
    @Optional
    public List<String> projectsToInclude = new ArrayList<>();

    @Input
    public Path assemblyRoot;

    public GeodeJBossModulesGeneratorConfig() {
    }

    public GeodeJBossModulesGeneratorConfig(String mainClass, Path outputRoot, String facetName, List<String> projectsToInclude) {
        this.mainClass = mainClass;
        this.outputRoot = outputRoot;
        this.facetName = facetName;
        this.projectsToInclude = projectsToInclude;
    }
}