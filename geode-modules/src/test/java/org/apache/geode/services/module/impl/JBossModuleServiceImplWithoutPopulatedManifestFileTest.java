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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.jboss.modules.ModuleClassLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.InvalidService;
import org.apache.geode.TestService;
import org.apache.geode.services.module.ModuleDescriptor;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ModuleServiceResult;

public class JBossModuleServiceImplWithoutPopulatedManifestFileTest {

  private static final String MODULE1_PATH =
      System.getProperty("user.dir") + "/../libs/module1WithoutManifest-1.0.jar";
  private static final String MODULE2_PATH =
      System.getProperty("user.dir") + "/../libs/module2WithoutManifest-1.0.jar";
  private static final String MODULE3_PATH =
      System.getProperty("user.dir") + "/../libs/module3WithoutManifest-1.0.jar";
  private static final String MODULE4_PATH =
      System.getProperty("user.dir") + "/../libs/module4WithoutManifest-1.0.jar";

  private static final String MODULE1_MESSAGE = "Hello from Module1!";
  private static final String MODULE2_MESSAGE = "Hello from Module2!";

  private ModuleService moduleService;

  @Before
  public void setup() {
    moduleService = new JBossModuleServiceImpl(LogManager.getLogger());
  }

  @After
  public void teardown() {
    moduleService = null;
  }

  @Test
  public void modulesNotAccessibleFromSystemClassloaderNoModulesLoaded() {
    ModuleServiceResult<Map<String, Class<?>>> mapModuleServiceResult = moduleService
        .loadClass("org.apache.geode.ModuleService1");
    assertThat(mapModuleServiceResult.isSuccessful()).isEqualTo(true);
    assertThat(mapModuleServiceResult.getMessage().size()).isEqualTo(0);

    mapModuleServiceResult = moduleService
        .loadClass("org.apache.geode.ModuleService2");
    assertThat(mapModuleServiceResult.isSuccessful()).isEqualTo(true);
    assertThat(mapModuleServiceResult.getMessage().size()).isEqualTo(0);
  }

  @Test
  public void modulesNotAccessibleFromSystemClassloaderWithModulesLoaded() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    assertThatExceptionOfType(ClassNotFoundException.class)
        .isThrownBy(
            () -> this.getClass().getClassLoader().loadClass("org.apache.geode.ModuleService1"));
    assertThatExceptionOfType(ClassNotFoundException.class)
        .isThrownBy(
            () -> this.getClass().getClassLoader().loadClass("org.apache.geode.ModuleService2"));
  }

  @Test
  public void loadSingleModuleFromSingleJarNoDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService1", module1Descriptor);
  }

  @Test
  public void loadSingleModuleFromMultipleJarsNoDependencies() {
    ModuleDescriptor moduleDescriptor = new ModuleDescriptor.Builder("multiJarModule", "1.0")
        .fromResourcePaths(MODULE1_PATH, MODULE2_PATH)
        .build();

    assertThat(moduleService.registerModule(moduleDescriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(moduleDescriptor).isSuccessful()).isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService1", moduleDescriptor);

    loadClassAndAssert("org.apache.geode.ModuleService2", moduleDescriptor);
  }

  private void loadClassAndAssert(String className, ModuleDescriptor loadFromModule,
      ModuleDescriptor moduleClassLoader) {
    ModuleServiceResult<Class<?>> loadClassResult =
        moduleService.loadClass(className, loadFromModule);
    assertThat(loadClassResult.isSuccessful()).isTrue();
    String moduleNameFromClassLoader =
        ((ModuleClassLoader) loadClassResult.getMessage().getClassLoader()).getName();
    assertThat(moduleNameFromClassLoader).isEqualTo(moduleClassLoader.getName());
  }

  private void loadClassAndAssert(String className, ModuleDescriptor moduleDescriptor) {
    loadClassAndAssert(className, moduleDescriptor, moduleDescriptor);
  }

  private void loadClassAndAssertFailure(String className, ModuleDescriptor moduleDescriptor,
      String expectedErrorMessage) {
    ModuleServiceResult<Class<?>> loadClassResult =
        moduleService.loadClass(className, moduleDescriptor);
    assertThat(loadClassResult.isSuccessful()).isFalse();
    assertThat(loadClassResult.getErrorMessage()).isEqualTo(expectedErrorMessage);
  }

  @Test
  public void loadMultipleModulesFromMultipleJarsNoDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH, MODULE2_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE3_PATH, MODULE4_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService1", module1Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService2", module1Descriptor);

    loadClassAndAssert("org.apache.geode.ModuleService3", module2Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService4", module2Descriptor);
  }

  @Test
  public void modulesCannotAccessOtherModulesMultipleModulesFromMultipleJarsNoDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH, MODULE2_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE3_PATH, MODULE4_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    loadClassAndAssertFailure("org.apache.geode.ModuleService3", module1Descriptor,
        "Could not find class for name: org.apache.geode.ModuleService3 in module: module1WithoutManifest-1.0");
    loadClassAndAssertFailure("org.apache.geode.ModuleService4", module1Descriptor,
        "Could not find class for name: org.apache.geode.ModuleService4 in module: module1WithoutManifest-1.0");
    loadClassAndAssertFailure("org.apache.geode.ModuleService1", module2Descriptor,
        "Could not find class for name: org.apache.geode.ModuleService1 in module: module2WithoutManifest-1.0");
    loadClassAndAssertFailure("org.apache.geode.ModuleService2", module2Descriptor,
        "Could not find class for name: org.apache.geode.ModuleService2 in module: module2WithoutManifest-1.0");
  }

  @Test
  public void loadMultipleModulesFromMultipleJarsWithDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH, MODULE2_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE3_PATH, MODULE4_PATH)
            .dependsOnModules(module1Descriptor.getName())
            .build();

    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService1", module2Descriptor, module1Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService2", module2Descriptor, module1Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService3", module2Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService4", module2Descriptor);
  }

  @Test
  public void dependenciesDoNotGoBothWaysMultipleModulesFromMultipleJars() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH, MODULE2_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE3_PATH, MODULE4_PATH)
            .dependsOnModules(module1Descriptor.getName())
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService1", module2Descriptor, module1Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService2", module2Descriptor, module1Descriptor);

    loadClassAndAssertFailure("org.apache.geode.ModuleService3", module1Descriptor,
        "Could not find class for name: org.apache.geode.ModuleService3 in module: module1WithoutManifest-1.0");
    loadClassAndAssertFailure("org.apache.geode.ModuleService4", module1Descriptor,
        "Could not find class for name: org.apache.geode.ModuleService4 in module: module1WithoutManifest-1.0");
  }

  @Test
  public void loadMultipleModulesFromSingleJarNoDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService1", module1Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService2", module2Descriptor);
  }

  @Test
  public void modulesCannotAccessOtherModulesMultipleModulesFromSingleJarNoDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    loadClassAndAssertFailure("org.apache.geode.ModuleService2", module1Descriptor,
        "Could not find class for name: org.apache.geode.ModuleService2 in module: module1WithoutManifest-1.0");
    loadClassAndAssertFailure("org.apache.geode.ModuleService1", module2Descriptor,
        "Could not find class for name: org.apache.geode.ModuleService1 in module: module2WithoutManifest-1.0");
  }

  @Test
  public void loadMultipleModulesWithDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .dependsOnModules(module1Descriptor.getName())
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService1", module1Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService2", module2Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService1", module2Descriptor, module1Descriptor);
  }

  @Test
  public void loadMultipleModulesFromSingleSourceWithDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .dependsOnModules(module1Descriptor.getName())
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService1", module1Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService2", module2Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService1", module2Descriptor, module1Descriptor);
  }

  @Test
  public void dependenciesDoNotGoBothWaysMultipleModulesFromSingleJar() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .dependsOnModules(module1Descriptor.getName())
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    loadClassAndAssertFailure("org.apache.geode.ModuleService2", module1Descriptor,
        "Could not find class for name: org.apache.geode.ModuleService2 in module: module1WithoutManifest-1.0");
    loadClassAndAssert("org.apache.geode.ModuleService1", module2Descriptor, module1Descriptor);
  }

  @Test
  public void loadModuleMultipleTimes() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isFalse();

    loadClassAndAssert("org.apache.geode.ModuleService1", module1Descriptor);
  }

  @Test
  public void loadModulesWithSameNameAndDifferentVersions() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();
    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();

    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "2.0")
            .fromResourcePaths(MODULE2_PATH)
            .build();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService1", module1Descriptor);
    loadClassAndAssertFailure("org.apache.geode.ModuleService2", module1Descriptor,
        "Could not find class for name: org.apache.geode.ModuleService2 in module: module1WithoutManifest-1.0");
    loadClassAndAssert("org.apache.geode.ModuleService2", module2Descriptor);
    loadClassAndAssertFailure("org.apache.geode.ModuleService1", module2Descriptor,
        "Could not find class for name: org.apache.geode.ModuleService1 in module: module1WithoutManifest-2.0");
  }

  @Test
  public void registerModuleFromInvalidSource() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths("/there/is/nothing/here.jar")
            .build();

    ModuleServiceResult<Boolean> moduleServiceResult =
        moduleService.registerModule(module1Descriptor);
    assertThat(moduleServiceResult.isSuccessful()).isFalse();
    assertThat(moduleServiceResult.getErrorMessage()).contains(
        "Registering module: module1WithoutManifest-1.0 failed with error: /there/is/nothing/here.jar");
  }

  @Test
  public void loadModuleFromMixOfValidAndInvalidSources() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths("/there/is/nothing/here.jar", MODULE1_PATH)
            .build();
    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isFalse();
  }

  @Test
  public void loadModuleWithInvalidDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .dependsOnModules("this_is_invalid")
            .build();
    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isFalse();
  }

  @Test
  public void loadModuleWithMixOfValidAndInvalidDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .dependsOnModules("this_is_invalid", module1Descriptor.getName())
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isFalse();
  }

  @Test
  public void loadServiceNoModulesLoaded() {
    ModuleServiceResult<Map<String, Set<TestService>>> loadServiceResult =
        moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage()).isEmpty();
  }

  @Test
  public void loadServiceNoModulesImplementService() {
    ModuleServiceResult<Map<String, Set<InvalidService>>> loadServiceResult =
        moduleService.loadService(InvalidService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage()).isEmpty();
  }

  @Test
  public void loadServiceFromSingleModule() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();

    ModuleServiceResult<Map<String, Set<TestService>>> serviceList =
        moduleService.loadService(TestService.class);
    assertThat(serviceList.isSuccessful()).isTrue();
    assertThat(serviceList.getMessage().size()).isEqualTo(1);
    assertThat(serviceList.getMessage().get(module1Descriptor.getName()).stream()
        .map(TestService::sayHello).findFirst().orElse("Error")).isEqualTo(MODULE1_MESSAGE);
  }

  @Test
  public void loadServicesFromMultipleModules() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    ModuleServiceResult<Map<String, Set<TestService>>> serviceList =
        moduleService.loadService(TestService.class);
    assertThat(serviceList.isSuccessful()).isTrue();
    assertThat(serviceList.getMessage().size()).isEqualTo(2);
    Collection<Set<TestService>> values = serviceList.getMessage().values();
    List<String> results = new ArrayList<>();
    for (Set<TestService> services : values) {
      results.addAll(services.stream().map(TestService::sayHello).collect(Collectors.toList()));
    }
    assertThat(results).contains(MODULE1_MESSAGE, MODULE2_MESSAGE);
  }

  @Test
  public void loadServicesFromCompositeModule() {
    ModuleDescriptor moduleDescriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH, MODULE2_PATH)
            .build();

    assertThat(moduleService.registerModule(moduleDescriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(moduleDescriptor).isSuccessful()).isTrue();

    ModuleServiceResult<Map<String, Set<TestService>>> serviceList =
        moduleService.loadService(TestService.class);
    assertThat(serviceList.isSuccessful()).isTrue();
    assertThat(serviceList.getMessage().size()).isEqualTo(1);
    assertThat(serviceList.getMessage().get(moduleDescriptor.getName()).size())
        .isEqualTo(2);

    Collection<Set<TestService>> values = serviceList.getMessage().values();
    List<String> results = new ArrayList<>();
    for (Set<TestService> services : values) {
      results.addAll(services.stream().map(TestService::sayHello).collect(Collectors.toList()));
    }
    assertThat(results).contains(MODULE1_MESSAGE, MODULE2_MESSAGE);
  }

  @Test
  public void loadServiceFromModulesWithDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();

    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .dependsOnModules(module1Descriptor.getName())
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();

    ModuleServiceResult<Map<String, Set<TestService>>> serviceList =
        moduleService.loadService(TestService.class);
    assertThat(serviceList.isSuccessful()).isTrue();
    assertThat(serviceList.getMessage().size()).isEqualTo(2);
    assertThat(serviceList.getMessage().get(module1Descriptor.getName()).size())
        .isEqualTo(1);
    assertThat(serviceList.getMessage().get(module2Descriptor.getName()).size())
        .isEqualTo(1);

    Collection<Set<TestService>> values = serviceList.getMessage().values();
    List<String> results = new ArrayList<>();
    for (Set<TestService> services : values) {
      results.addAll(services.stream().map(TestService::sayHello).collect(Collectors.toList()));
    }
    assertThat(results).contains(MODULE1_MESSAGE, MODULE2_MESSAGE);
  }

  @Test
  public void loadServiceFromModuleWithDuplicateContents() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH, MODULE1_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();

    ModuleServiceResult<Map<String, Set<TestService>>> serviceList =
        moduleService.loadService(TestService.class);
    assertThat(serviceList.isSuccessful()).isTrue();
    assertThat(serviceList.getMessage().size()).isEqualTo(1);
    assertThat(serviceList.getMessage().get(module1Descriptor.getName()).stream()
        .map(TestService::sayHello).findFirst().orElse("Error")).isEqualTo(MODULE1_MESSAGE);
  }

  @Test
  public void unloadModule() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();

    ModuleServiceResult<Map<String, Set<TestService>>> loadServiceResult =
        moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage().size()).isEqualTo(1);

    assertThat(moduleService.unloadModule(module1Descriptor.getName()).isSuccessful())
        .isTrue();

    loadServiceResult = moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage()).isEmpty();
  }

  @Test
  public void unloadModuleFromMultipleJars() {
    ModuleDescriptor moduleDescriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH, MODULE2_PATH)
            .build();

    assertThat(moduleService.registerModule(moduleDescriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(moduleDescriptor).isSuccessful()).isTrue();

    ModuleServiceResult<Map<String, Set<TestService>>> loadServiceResult =
        moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage().size()).isEqualTo(1);

    assertThat(moduleService.unloadModule(moduleDescriptor.getName()).isSuccessful())
        .isTrue();

    loadServiceResult = moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage()).isEmpty();
  }

  @Test
  public void unloadOneOfMultipleModules() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();
    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    ModuleServiceResult<Map<String, Set<TestService>>> loadServiceResult =
        moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage().size()).isEqualTo(2);
    assertThat(loadServiceResult.getMessage().get(module1Descriptor.getName()).size())
        .isEqualTo(1);
    assertThat(loadServiceResult.getMessage().get(module2Descriptor.getName()).size())
        .isEqualTo(1);

    assertThat(moduleService.unloadModule(module1Descriptor.getName()).isSuccessful())
        .isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService2", module2Descriptor);

    loadServiceResult = moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage().size()).isEqualTo(1);
    assertThat(loadServiceResult.getMessage().get(module2Descriptor.getName()).size())
        .isEqualTo(1);
  }

  @Test
  public void unloadInvalidModuleName() {
    assertThat(moduleService.unloadModule("invalidModuleName").isSuccessful()).isFalse();
  }

  @Test
  public void reloadUnloadedModule() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();

    ModuleServiceResult<Map<String, Set<TestService>>> loadServiceResult =
        moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage().size()).isEqualTo(1);
    assertThat(loadServiceResult.getMessage().get(module1Descriptor.getName()).size())
        .isEqualTo(1);

    assertThat(moduleService.unloadModule(module1Descriptor.getName()).isSuccessful())
        .isTrue();

    loadServiceResult = moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage()).isEmpty();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();

    loadServiceResult = moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage().size()).isEqualTo(1);
    assertThat(loadServiceResult.getMessage().get(module1Descriptor.getName()).size())
        .isEqualTo(1);
  }

  @Test
  public void unloadModuleWithDependencies() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();

    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH)
            .dependsOnModules(module1Descriptor.getName())
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();

    ModuleServiceResult<Map<String, Set<TestService>>> loadServiceResult =
        moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage().size()).isEqualTo(2);
    assertThat(loadServiceResult.getMessage().get(module1Descriptor.getName()).size())
        .isEqualTo(1);
    assertThat(loadServiceResult.getMessage().get(module2Descriptor.getName()).size())
        .isEqualTo(1);

    assertThat(moduleService.unloadModule(module2Descriptor.getName()).isSuccessful())
        .isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService1", module1Descriptor);

    loadServiceResult = moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage().size()).isEqualTo(1);
    assertThat(loadServiceResult.getMessage().get(module1Descriptor.getName()).size())
        .isEqualTo(1);
  }

  @Test
  public void unloadModuleTwice() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.unloadModule(module1Descriptor.getName()).isSuccessful())
        .isTrue();
    assertThat(moduleService.unloadModule(module1Descriptor.getName()).isSuccessful())
        .isFalse();
  }

  @Test
  public void unloadModuleWithSourceSharedByOtherModule() {
    ModuleDescriptor module1Descriptor =
        new ModuleDescriptor.Builder("module1WithoutManifest", "1.0")
            .fromResourcePaths(MODULE1_PATH, MODULE2_PATH)
            .build();

    ModuleDescriptor module2Descriptor =
        new ModuleDescriptor.Builder("module2WithoutManifest", "1.0")
            .fromResourcePaths(MODULE2_PATH, MODULE3_PATH)
            .build();

    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.loadModule(module2Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.unloadModule(module1Descriptor.getName()).isSuccessful())
        .isTrue();

    loadClassAndAssert("org.apache.geode.ModuleService2", module2Descriptor);
    loadClassAndAssert("org.apache.geode.ModuleService3", module2Descriptor);

    ModuleServiceResult<Map<String, Set<TestService>>> loadServiceResult =
        moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage().size()).isEqualTo(1);
    assertThat(loadServiceResult.getMessage().keySet().toArray()[0])
        .isEqualTo(module2Descriptor.getName());

    Set<TestService> testServices =
        loadServiceResult.getMessage().get(module2Descriptor.getName());
    assertThat(testServices.size()).isEqualTo(2);
    assertThat(testServices.stream().map(service -> service.getClass().getName()))
        .containsExactlyInAnyOrder("org.apache.geode.ModuleService2",
            "org.apache.geode.ModuleService3");
  }
}
