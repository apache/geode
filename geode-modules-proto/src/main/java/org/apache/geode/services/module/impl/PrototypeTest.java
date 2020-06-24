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

package org.apache.geode.services.module.impl;

import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.services.bootstrapping.BootstrappingService;
import org.apache.geode.services.module.ModuleDescriptor;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ModuleServiceResult;

public class PrototypeTest {

  private static final String rootPath =
      System.getProperty("user.dir") + "/geode-assembly/build/install/apache-geode/lib/";
  private final String gemFireVersion = "1.14.0-build.0";
  private ModuleService moduleService;
  private BootstrappingService bootstrappingService;

  private void setup() {
    // String property = System.getProperty("sun.boot.class.path");
    Set<String> packages =
        // GeodeJDKPaths.getListPackagesFromJars(property);
        new TreeSet<>();
    packages.add("javax.management");
    packages.add("java.lang.management");

    System.setProperty("jboss.modules.system.pkgs",
        processPackagesIntoJBossPackagesNames(packages));

    moduleService = new JBossModuleServiceImpl();

    ModuleDescriptor moduleManagementBootStrappingDescriptor =
        new ModuleDescriptor.Builder("bootStrapping", gemFireVersion)
            .fromResourcePaths(rootPath + "geode-module-bootstrapping-" + gemFireVersion + ".jar")
            .build();

    ModuleServiceResult<Boolean> registerModule =
        moduleService.registerModule(moduleManagementBootStrappingDescriptor);
    registerModule.ifSuccessful(result -> {
      ModuleServiceResult<Boolean> loadModule =
          moduleService.loadModule(moduleManagementBootStrappingDescriptor);
      loadModule.ifSuccessful(loadResult -> {
        ModuleServiceResult<Set<BootstrappingService>> serviceLoadResult =
            moduleService.loadService(BootstrappingService.class);
        serviceLoadResult.ifSuccessful(serviceLoad -> {
          for (BootstrappingService service : serviceLoadResult.getMessage()) {
            bootstrappingService = service;
            break;
          }
        });
      });
      loadModule.ifFailure(System.err::println);
    });
    registerModule.ifFailure(System.err::println);
  }

  private String processPackagesIntoJBossPackagesNames(Set<String> packages) {
    StringBuilder stringBuilder = new StringBuilder();
    packages.forEach(packageName -> stringBuilder.append(packageName + ","));
    if (stringBuilder.length() > 0) {
      stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    }
    return stringBuilder.toString();
  }

  public static void main(String[] args) {
    PrototypeTest prototypeTest = new PrototypeTest();

    prototypeTest.setup();
    Properties properties = new Properties();
    properties.setProperty("name", "whatever");
    properties.setProperty("mcast-port", "0");
    properties.setProperty("start-locator", "localhost[10334]");
    properties.setProperty("jmx-manager-start", "true");
    properties.setProperty("jmx-manager", "true");

    if (prototypeTest.bootstrappingService != null) {
      prototypeTest.bootstrappingService.init(prototypeTest.moduleService, properties);
    }
  }
}
