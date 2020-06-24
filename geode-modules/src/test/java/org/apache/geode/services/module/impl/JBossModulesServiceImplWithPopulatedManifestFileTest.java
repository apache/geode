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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.services.module.ModuleDescriptor;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ModuleServiceResult;
import org.apache.geode.services.result.impl.Failure;

public class JBossModulesServiceImplWithPopulatedManifestFileTest {

  private static String gemFireVersion = GemFireVersion.getGemFireVersion();

  private static final String MODULE1_PATH =
      System.getProperty("user.dir") + "/../libs/module1WithManifest-1.0.jar";
  private static final String MODULE2_PATH =
      System.getProperty("user.dir") + "/../libs/module2WithManifest-1.0.jar";
  private static final String MODULE3_PATH =
      System.getProperty("user.dir") + "/../libs/module3WithManifest-1.0.jar";
  private static final String MODULE4_PATH =
      System.getProperty("user.dir") + "/../libs/module4WithManifest-1.0.jar";
  private static final String MODULE5_PATH =
      System.getProperty("user.dir") + "/../libs/module5WithManifest-1.0.jar";
  private static final String GEODE_COMMONS_SERVICES_PATH =
      System.getProperty("user.dir") + "/../libs/geode-common-services-" + gemFireVersion + ".jar";
  private static final String GEODE_COMMONS_PATH =
      System.getProperty("user.dir") + "/../libs/geode-common-" + gemFireVersion + ".jar";

  private ModuleService moduleService;
  private ModuleDescriptor geodeCommonsServiceDescriptor;
  private ModuleDescriptor geodeCommonDescriptor;

  @Before
  public void setup() {
    moduleService = new JBossModuleServiceImpl(LogManager.getLogger());
    geodeCommonsServiceDescriptor =
        new ModuleDescriptor.Builder("geode-common-services", gemFireVersion)
            .fromResourcePaths(GEODE_COMMONS_SERVICES_PATH)
            .build();

    geodeCommonDescriptor = new ModuleDescriptor.Builder("geode-common", gemFireVersion)
        .fromResourcePaths(GEODE_COMMONS_PATH)
        .build();
  }

  @Test
  public void loadJarWithManifestAndClasspathAttribute() {
    ModuleDescriptor module1Descriptor = new ModuleDescriptor.Builder("module1WithManifest", "1.0")
        .fromResourcePaths(MODULE1_PATH)
        .build();

    ModuleDescriptor module2Descriptor = new ModuleDescriptor.Builder("module2WithManifest", "1.0")
        .fromResourcePaths(MODULE2_PATH)
        .build();

    assertThat(moduleService.registerModule(geodeCommonsServiceDescriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(geodeCommonDescriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module2Descriptor).isSuccessful()).isTrue();

    loadModuleAndAssert(geodeCommonsServiceDescriptor);
    loadModuleAndAssert(module1Descriptor);
    loadModuleAndAssert(module2Descriptor);

    ModuleServiceResult<Map<String, Class<?>>> loadClassResult =
        moduleService.loadClass("com.google.common.base.Strings");

    assertThat(loadClassResult.isSuccessful()).isTrue();

    Map<String, Class<?>> message = loadClassResult.getMessage();
    assertThat(message.size()).isEqualTo(1);
    assertThat(message.keySet().toArray()[0]).isEqualTo(module1Descriptor.getName());
  }

  private void loadModuleAndAssert(ModuleDescriptor descriptor) {
    ModuleServiceResult<Boolean> loadModuleResult = moduleService.loadModule(descriptor);
    assertThat(loadModuleResult.isSuccessful()).isTrue();
    assertThat(loadModuleResult.getMessage()).isEqualTo(true);
  }

  @Test
  public void loadJarWithManifestAndClasspathAttributeInvalidClassName() {
    ModuleDescriptor module1Descriptor = new ModuleDescriptor.Builder("module1WithManifest", "1.0")
        .fromResourcePaths(MODULE1_PATH)
        .build();

    assertThat(moduleService.registerModule(geodeCommonDescriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(geodeCommonsServiceDescriptor).isSuccessful()).isTrue();
    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
    loadModuleAndAssert(module1Descriptor);

    ModuleServiceResult<Map<String, Class<?>>> loadClassResult =
        moduleService.loadClass(".ocm.this.should.not.Exist");
    assertThat(loadClassResult.isSuccessful()).isTrue();
    assertThat(loadClassResult.getMessage().size()).isEqualTo(0);
  }

  @Test
  public void loadJarWithManifestWithInvalidClasspathLocation() {
    ModuleDescriptor descriptor = new ModuleDescriptor.Builder("module5", "1.0")
        .fromResourcePaths(MODULE5_PATH)
        .build();

    assertThat(moduleService.registerModule(descriptor).isSuccessful()).isTrue();

    ModuleServiceResult<Boolean> loadModuleResult = moduleService.loadModule(descriptor);
    assertThat(loadModuleResult.isSuccessful()).isFalse();
    assertThat(loadModuleResult).isExactlyInstanceOf(Failure.class);

    String[] errorMessageSnippet =
        new String[] {"java.io.FileNotFoundException:", "java.nio.file.NoSuchFileException:"};
    assertMessageContains(loadModuleResult.getErrorMessage(), errorMessageSnippet);
    assertThat(loadModuleResult.getErrorMessage())
        .contains("libs/invalidjar.jar");
  }

  private void assertMessageContains(String errorMessage, String[] errorMessageSnippet) {
    AtomicBoolean containsString = new AtomicBoolean();
    Arrays.stream(errorMessageSnippet)
        .forEach(errorSnippet -> containsString.set(errorMessage.contains(errorMessage)));
    assertThat(containsString.get()).isTrue();
  }
}
