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
import java.util.Arrays;
import java.util.List;

public class GeodeJBossModulesGeneratorConfig {
    public String mainClass;

    public Path outputRoot;

    public String name;

    public List<String> jbossJdkModules;

    public Boolean assembleFromSource;

    public GeodeJBossModulesGeneratorConfig(String name) {
        this.name = name;
    }

    public GeodeJBossModulesGeneratorConfig(String name, Path outputRoot) {
        this.outputRoot = outputRoot;
        this.name = name;
    }

    //This is for testing purposes only
    public GeodeJBossModulesGeneratorConfig(String name, String mainClass, Path outputRoot) {
        this(name);
        this.mainClass = mainClass;
        this.outputRoot = outputRoot;
        this.jbossJdkModules = Arrays.asList("java.se","jdk.unsupported", "jdk.scripting.nashorn",
            "java.desktop");
    }

    public boolean shouldAssembleFromSource() {
        return assembleFromSource != null && assembleFromSource;
    }
}