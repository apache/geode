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
package org.apache.geode.internal.services.registry.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.services.registry.ServiceRegistry;
import org.apache.geode.services.result.ServiceResult;

public class ServiceRegistryImplTest {

  private ServiceRegistry serviceRegistry;

  @Before
  public void setup() {
    serviceRegistry = new ServiceRegistryImpl();
  }

  @Test
  public void getServiceNotExists() {
    ServiceResult<TestService> serviceResult = serviceRegistry.getService(TestService.class);
    assertThat(serviceResult.isFailure()).isTrue();
    assertThat(serviceResult.getErrorMessage()).isEqualTo("No service found for type: "
        + "org.apache.geode.internal.services.registry.impl.ServiceRegistryImplTest$TestService");
  }

  @Test
  public void getServiceNotExistsUsingName() {
    ServiceResult<TestService> serviceResult =
        serviceRegistry.getService("somename", TestService.class);
    assertThat(serviceResult.isFailure()).isTrue();
    assertThat(serviceResult.getErrorMessage()).isEqualTo("No service found for name: somename");
  }

  @Test
  public void getServiceWithMultipleInstances() {
    serviceRegistry.addService(new TestServiceImpl());
    serviceRegistry.addService(new TestService2Impl());
    ServiceResult<TestService> serviceResult = serviceRegistry.getService(TestService.class);
    assertThat(serviceResult.isFailure()).isTrue();
    assertThat(serviceResult.getErrorMessage()).contains(Arrays.asList(
        "Multiple services for type: org.apache.geode.internal.services.registry.impl.ServiceRegistryImplTest$TestService were found:",
        "org.apache.geode.internal.services.registry.impl.ServiceRegistryImplTest$TestServiceImpl",
        "org.apache.geode.internal.services.registry.impl.ServiceRegistryImplTest$TestService2Impl"));
  }

  @Test
  public void getServiceWithMultipleInstancesUsingName() {
    serviceRegistry.addService("testService", new TestServiceImpl());
    serviceRegistry.addService(new TestService2Impl());
    assertThat(serviceRegistry.getService("testService", TestService.class).isSuccessful())
        .isTrue();
  }

  @Test
  public void getServiceForNameWrongType() {
    serviceRegistry.addService("testService", new TestServiceImpl());
    ServiceResult<WrongService> serviceResult =
        serviceRegistry.getService("testService", WrongService.class);
    assertThat(serviceResult.isFailure()).isTrue();
    assertThat(serviceResult.getErrorMessage())
        .isEqualTo("Service for name: testService is not of type: "
            + "org.apache.geode.internal.services.registry.impl.ServiceRegistryImplTest$WrongService");
  }

  @Test
  public void addService() {
    assertThat(serviceRegistry.addService(new TestServiceImpl()).isSuccessful()).isTrue();
  }

  @Test
  public void addServiceMultipleTimes() {
    assertThat(serviceRegistry.addService(new TestServiceImpl()).isSuccessful()).isTrue();
    ServiceResult<Boolean> serviceResult = serviceRegistry.addService(new TestServiceImpl());
    assertThat(serviceResult.isSuccessful()).isFalse();
    assertThat(serviceResult.getErrorMessage()).isEqualTo(
        "Service for name: org.apache.geode.internal.services.registry.impl.ServiceRegistryImplTest$TestServiceImpl already added with type: org.apache.geode.internal.services.registry.impl.ServiceRegistryImplTest$TestServiceImpl");
  }

  @Test
  public void addMultipleDifferentServices() {
    assertThat(serviceRegistry.addService(new TestServiceImpl()).isSuccessful()).isTrue();
    assertThat(serviceRegistry.addService(new TestService2Impl()).isSuccessful()).isTrue();
  }

  @Test
  public void addServiceForName() {
    assertThat(serviceRegistry.addService("testService", new TestServiceImpl()).isSuccessful())
        .isTrue();
  }

  @Test
  public void addMultipleServicesForName() {
    assertThat(serviceRegistry.addService("testService", new TestServiceImpl()).isSuccessful())
        .isTrue();
    ServiceResult<Boolean> serviceResult =
        serviceRegistry.addService("testService", new TestService2Impl());
    assertThat(serviceResult.isSuccessful()).isFalse();
    assertThat(serviceResult.getErrorMessage()).isEqualTo(
        "Service for name: testService already added with type: org.apache.geode.internal.services.registry.impl.ServiceRegistryImplTest$TestService2Impl");
  }

  @Test
  public void removeService() {
    serviceRegistry.addService("testService", new TestServiceImpl());
    assertThat(serviceRegistry.removeService("testService").isSuccessful()).isTrue();
    assertThat(serviceRegistry.getService(TestService.class).isFailure()).isTrue();
  }

  @Test
  public void removeServiceNotExists() {
    assertThat(serviceRegistry.removeService("testService").isSuccessful()).isTrue();
  }

  interface TestService {

  }

  interface WrongService {

  }

  class TestServiceImpl implements TestService {

  }

  class TestService2Impl implements TestService {

  }
}
