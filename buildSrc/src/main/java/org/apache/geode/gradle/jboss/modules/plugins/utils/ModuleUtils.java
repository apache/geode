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
package org.apache.geode.gradle.jboss.modules.plugins.utils;

import org.apache.geode.gradle.jboss.modules.plugins.config.ModulesGeneratorConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class ModuleUtils {
  public static ModulesGeneratorConfig defaultConfigFromGlobal(
      ModulesGeneratorConfig globalConfig, ModulesGeneratorConfig config) {
    ModulesGeneratorConfig newConfig = new ModulesGeneratorConfig(config.name);
    newConfig.outputRoot = config.outputRoot == null ? globalConfig.outputRoot : config.outputRoot;
    newConfig.mainClass = config.mainClass == null ? globalConfig.mainClass : config.mainClass;
    newConfig.assembleFromSource = config.assembleFromSource == null
        ? (globalConfig.assembleFromSource != null && globalConfig.assembleFromSource)
        : config.assembleFromSource;
    newConfig.jbossJdkModulesDependencies =
        combineLists(globalConfig.jbossJdkModulesDependencies, config.jbossJdkModulesDependencies);
    newConfig.externalLibraryJbossJDKModules = combineLists(
        globalConfig.externalLibraryJbossJDKModules, config.externalLibraryJbossJDKModules);
    newConfig.alternativeDescriptorRoot =
        config.alternativeDescriptorRoot == null ? globalConfig.alternativeDescriptorRoot
            : config.alternativeDescriptorRoot;
    newConfig.parentModule =
        config.parentModule == null ? globalConfig.parentModule : config.parentModule;
    newConfig.packagesToExport =
        combineLists(globalConfig.packagesToExport, config.packagesToExport);
    newConfig.packagesToExclude =
        combineLists(globalConfig.packagesToExclude, config.packagesToExclude);
    return newConfig;
  }

  private static List<String> combineLists(final List<String> list1, final List<String> list2) {
    if (list1 == null) {
      return list2 != null ? list2 : Collections.emptyList();
    }
    if (list2 == null) {
      return list1;
    }
    Set<String> strings = new HashSet<>(list1);
    strings.addAll(list2);
    return new LinkedList<>(strings);
  }
}
