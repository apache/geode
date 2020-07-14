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

package org.apache.geode.services.module.internal.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.services.module.ModuleDescriptor;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ServiceResult;

public class ServiceLoaderModuleServiceTest {

  private static final String resourceFile = "TestResourceFile.txt";
  private static ModuleService moduleService;
  private static ModuleDescriptor module1Descriptor;

  @BeforeClass
  public static void setup() {
    moduleService = new ServiceLoaderModuleService(LogManager.getLogger());
    module1Descriptor = new ModuleDescriptor.Builder("module1", "1.0")
        .fromResourcePaths("path/to/jar.jar")
        .dependsOnModules("module2", "module3")
        .build();
  }

  @Test
  public void registerModule() {
    assertThat(moduleService.registerModule(module1Descriptor).isSuccessful()).isTrue();
  }

  @Test
  public void loadModule() {
    assertThat(moduleService.loadModule(module1Descriptor).isSuccessful()).isTrue();
  }

  @Test
  public void unregisterModule() {
    assertThat(moduleService.unregisterModule(module1Descriptor).isSuccessful()).isTrue();
  }

  @Test
  public void unloadModule() {
    assertThat(moduleService.unloadModule(module1Descriptor.getName()).isSuccessful()).isTrue();
  }

  @Test
  public void findResourceAsStreamExists() {
    ServiceResult<List<InputStream>> resourceAsStream =
        moduleService.findResourceAsStream(resourceFile);

    assertThat(resourceAsStream.isSuccessful()).isTrue();
    assertThat(resourceAsStream.getMessage().size()).isEqualTo(1);
  }

  @Test
  public void findResourceAsStreamNotExists() {
    ServiceResult<List<InputStream>> resourceAsStream =
        moduleService.findResourceAsStream("invalidReSource.file");

    assertThat(resourceAsStream.isSuccessful()).isFalse();
  }

  @Test
  public void loadClassExists() {
    ServiceResult<List<Class<?>>> loadedClassResult =
        moduleService.loadClass("org.apache.geode.services.module.ModuleDescriptor");

    assertThat(loadedClassResult.isSuccessful()).isTrue();
    assertThat(loadedClassResult.getMessage().size()).isEqualTo(1);
    assertThat(loadedClassResult.getMessage().get(0)).isEqualTo(ModuleDescriptor.class);
  }

  @Test
  public void loadClassExistsMissingPackage() {
    ServiceResult<List<Class<?>>> loadedClassResult = moduleService.loadClass("ModuleDescriptor");

    assertThat(loadedClassResult.isSuccessful()).isFalse();
  }

  @Test
  public void loadClassNotExists() {
    ServiceResult<List<Class<?>>> loadedClassResult =
        moduleService.loadClass("org.apache.geode.services.moduleFakeClassThatIsNotReal");

    assertThat(loadedClassResult.isSuccessful()).isFalse();
  }

  @Test
  public void loadClassFromModule() {
    assertThat(moduleService
        .loadClass("org.apache.geode.services.module.ModuleDescriptor", module1Descriptor)
        .isSuccessful()).isTrue();
  }

  @Test
  public void loadClassMissingPackageFromModule() {
    assertThat(moduleService
        .loadClass("ModuleDescriptor", module1Descriptor)
        .isSuccessful()).isFalse();
  }

  @Test
  public void loadClassMissingNotExistsFromModule() {
    assertThat(moduleService
        .loadClass("org.apache.geode.services.moduleFakeClassThatIsNotReal", module1Descriptor)
        .isSuccessful()).isFalse();
  }

  @Test
  public void loadService() {
    ServiceResult<Set<TestService>> loadServiceResult =
        moduleService.loadService(TestService.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage().size()).isEqualTo(1);
    assertThat(loadServiceResult.getMessage().iterator().next())
        .isInstanceOf(TestServiceImpl.class);
  }

  @Test
  public void loadInvalidService() {
    ServiceResult<Set<TestServiceImpl>> loadServiceResult =
        moduleService.loadService(TestServiceImpl.class);
    assertThat(loadServiceResult.isSuccessful()).isTrue();
    assertThat(loadServiceResult.getMessage().size()).isEqualTo(0);
  }
}
