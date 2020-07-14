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

package org.apache.geode.services.management.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.services.bootstrapping.BootstrappingService;
import org.apache.geode.services.management.ComponentManagementService;
import org.apache.geode.services.management.ManagementService;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

public class ManagementServiceImplTest {

  private static ManagementService managementService;
  private static BootstrappingService bootstrappingService;
  private static ModuleService moduleService;
  private static ComponentManagementService componentManagementService;

  @Before
  public void setup() {
    componentManagementService = Mockito.mock(ComponentManagementService.class);

    moduleService = Mockito.mock(ModuleService.class);

    bootstrappingService = Mockito.mock(BootstrappingService.class);
    when(bootstrappingService.getModuleService()).thenReturn(moduleService);

    managementService = new ManagementServiceImpl(bootstrappingService, LogManager.getLogger());
  }

  @Test
  public void createComponentHappyCase() {
    when(bootstrappingService.bootStrapModule(any())).thenReturn(Success.SUCCESS_TRUE);

    when(moduleService.loadService(any())).thenReturn(Success.of(Collections.singleton(
        componentManagementService)));

    when(componentManagementService.canCreateComponent(any())).thenReturn(true);
    when(componentManagementService.init(any(), any(), any())).thenReturn(Success.SUCCESS_TRUE);

    ComponentIdentifier componentIdentifier = new ComponentIdentifier("TestComponent");
    assertThat(managementService.createComponent(componentIdentifier).isSuccessful()).isTrue();
  }

  @Test
  public void createComponentNoComponentManagementServiceFound() {
    when(bootstrappingService.bootStrapModule(any())).thenReturn(Success.SUCCESS_TRUE);

    when(moduleService.loadService(any())).thenReturn(Success.of(Collections.emptySet()));

    ComponentIdentifier componentIdentifier = new ComponentIdentifier("TestComponent");
    ServiceResult<Boolean> serviceResult = managementService.createComponent(componentIdentifier);
    assertThat(serviceResult.isFailure()).isTrue();
    assertThat(serviceResult.getErrorMessage())
        .isEqualTo("Could not find ComponentManagementService for component: TestComponent");
  }

  @Test
  public void createComponent_ComponentManagementServiceFound_WrongType() {
    when(bootstrappingService.bootStrapModule(any())).thenReturn(Success.SUCCESS_TRUE);

    when(moduleService.loadService(any())).thenReturn(Success.of(Collections.singleton(
        componentManagementService)));

    when(componentManagementService.canCreateComponent(any())).thenReturn(false);

    ComponentIdentifier componentIdentifier = new ComponentIdentifier("TestComponent");
    ServiceResult<Boolean> serviceResult = managementService.createComponent(componentIdentifier);
    assertThat(serviceResult.isFailure()).isTrue();
    assertThat(serviceResult.getErrorMessage())
        .isEqualTo("Could not find ComponentManagementService for component: TestComponent");
  }

  @Test
  public void createComponent_ComponentManagementServiceFound_CorrectType_CreateFailed() {
    when(bootstrappingService.bootStrapModule(any())).thenReturn(Success.SUCCESS_TRUE);

    when(moduleService.loadService(any())).thenReturn(Success.of(Collections.singleton(
        componentManagementService)));

    when(componentManagementService.canCreateComponent(any())).thenReturn(true);
    when(componentManagementService.init(any(), any(), any()))
        .thenReturn(Failure.of("We want to see this failure message"));

    ComponentIdentifier componentIdentifier = new ComponentIdentifier("TestComponent");
    ServiceResult<Boolean> serviceResult = managementService.createComponent(componentIdentifier);
    assertThat(serviceResult.isFailure()).isTrue();
    assertThat(serviceResult.getErrorMessage()).isEqualTo("We want to see this failure message");
  }

  @Test
  public void createExistingComponent() {
    when(bootstrappingService.bootStrapModule(any())).thenReturn(Success.SUCCESS_TRUE);

    when(moduleService.loadService(any())).thenReturn(Success.of(Collections.singleton(
        componentManagementService)));

    when(componentManagementService.canCreateComponent(any())).thenReturn(true);
    when(componentManagementService.init(any(), any(), any())).thenReturn(Success.of(true));

    ComponentIdentifier componentIdentifier = new ComponentIdentifier("TestComponent");
    assertThat(managementService.createComponent(componentIdentifier).isSuccessful()).isTrue();

    ServiceResult<Boolean> serviceResult2 = managementService.createComponent(componentIdentifier);

    assertThat(serviceResult2.isFailure()).isTrue();
    assertThat(serviceResult2.getErrorMessage())
        .isEqualTo("Component for name: TestComponent, has already been created");
  }

  @Test
  public void createWithNullComponentIdentifier() {
    ServiceResult<Boolean> serviceResult = managementService.createComponent(null);
    assertThat(serviceResult.isFailure()).isTrue();
    assertThat(serviceResult.getErrorMessage()).isEqualTo("Component Identifier cannot be null");
  }

  @Test
  public void destroyExistingComponent() {
    when(bootstrappingService.bootStrapModule(any())).thenReturn(Success.SUCCESS_TRUE);

    when(moduleService.loadService(any())).thenReturn(Success.of(Collections.singleton(
        componentManagementService)));

    when(componentManagementService.canCreateComponent(any())).thenReturn(true);
    when(componentManagementService.init(any(), any(), any())).thenReturn(Success.of(true));
    when(componentManagementService.close(any())).thenReturn(Success.of(true));

    ComponentIdentifier componentIdentifier = new ComponentIdentifier("TestComponent");
    assertThat(managementService.createComponent(componentIdentifier).isSuccessful()).isTrue();
    assertThat(managementService.closeComponent(componentIdentifier).isSuccessful()).isTrue();
  }

  @Test
  public void destroyComponentWhichDoesNotExist() {
    when(bootstrappingService.bootStrapModule(any())).thenReturn(Success.SUCCESS_TRUE);

    when(moduleService.loadService(any())).thenReturn(Success.of(Collections.singleton(
        componentManagementService)));

    when(componentManagementService.canCreateComponent(any())).thenReturn(true);
    when(componentManagementService.init(any(), any(), any())).thenReturn(Success.of(true));

    assertThat(
        managementService.createComponent(new ComponentIdentifier("TestComponent")).isSuccessful())
            .isTrue();
    ServiceResult<Boolean> serviceResult =
        managementService.closeComponent(new ComponentIdentifier("TestComponent2"));
    assertThat(serviceResult.isSuccessful()).isTrue();
  }

  @Test
  public void destroyComponentWhichDoesNotExist2() {
    assertThat(
        managementService.closeComponent(new ComponentIdentifier("TestComponent")).isSuccessful())
            .isTrue();
  }

  @Test
  public void destroyComponentFailureToDestroyComponentManagementService() {
    when(bootstrappingService.bootStrapModule(any())).thenReturn(Success.SUCCESS_TRUE);

    when(moduleService.loadService(any())).thenReturn(Success.of(Collections.singleton(
        componentManagementService)));

    when(componentManagementService.canCreateComponent(any())).thenReturn(true);
    when(componentManagementService.init(any(), any(), any())).thenReturn(Success.of(true));

    when(componentManagementService.close(any()))
        .thenReturn(Failure.of("Underlying Component destroy failure"));

    assertThat(
        managementService.createComponent(new ComponentIdentifier("TestComponent")).isSuccessful())
            .isTrue();

    ServiceResult<Boolean> serviceResult =
        managementService.closeComponent(new ComponentIdentifier("TestComponent"));

    assertThat(serviceResult.getErrorMessage()).isEqualTo("Underlying Component destroy failure");
  }
}
