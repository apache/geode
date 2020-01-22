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
package org.apache.geode.management.internal.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.internal.operation.OperationHistoryManager.OperationInstance;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class OperationManagerTest {
  private OperationManager executorManager;
  private OperationHistoryPersistenceService operationHistoryPersistenceService;

  @Before
  public void setUp() throws Exception {
    operationHistoryPersistenceService = mock(OperationHistoryPersistenceService.class);
    executorManager = new OperationManager(null,
        new OperationHistoryManager(1, TimeUnit.MINUTES, operationHistoryPersistenceService));
    executorManager.registerOperation(TestOperation.class, OperationManagerTest::perform);
  }

  @Test
  public void submitAndComplete() throws Exception {
    TestOperation operation = new TestOperation();
    OperationInstance<TestOperation, TestOperationResult> inst = executorManager.submit(operation);
    String id = inst.getId();
    assertThat(id).isNotBlank();

    doReturn(inst).when(operationHistoryPersistenceService).getOperationInstance(id);

    assertThat(executorManager.getOperationInstance(id)).isNotNull();
    assertThat(executorManager.getOperationInstance(id).getOperationStart()).isNotNull();
    assertThat(executorManager.getOperationInstance(id).getOperationEnd()).isNull();

    TestOperation operation2 = new TestOperation();
    OperationInstance<TestOperation, TestOperationResult> inst2 =
        executorManager.submit(operation2);
    String id2 = inst2.getId();
    assertThat(id2).isNotBlank();
    doReturn(inst2).when(operationHistoryPersistenceService).getOperationInstance(id2);

    assertThat(executorManager.getOperationInstance(id2)).isNotNull();
    assertThat(executorManager.getOperationInstance(id2).getOperationStart()).isNotNull();
    assertThat(executorManager.getOperationInstance(id2).getOperationEnd()).isNull();

    operation.latch.countDown();
    GeodeAwaitility.await().untilAsserted(() -> {
      assertThat(executorManager.getOperationInstance(id).getOperationEnd()).isNotNull();
    });

    operation2.latch.countDown();
    GeodeAwaitility.await().untilAsserted(() -> {
      assertThat(executorManager.getOperationInstance(id2).getOperationEnd()).isNotNull();
    });
  }

  @Test
  public void submit() {
    TestOperation operation = new TestOperation();
    OperationInstance<TestOperation, TestOperationResult> inst = executorManager.submit(operation);
    String id = inst.getId();
    assertThat(id).isNotBlank();

    doReturn(inst).when(operationHistoryPersistenceService).getOperationInstance(id);


    assertThat(executorManager.getOperationInstance(id)).isNotNull();

    TestOperation operation2 = new TestOperation();
    OperationInstance<TestOperation, TestOperationResult> inst2 = executorManager.submit(operation2);
    String id2 = inst2.getId();
    assertThat(id2).isNotBlank();
    doReturn(inst2).when(operationHistoryPersistenceService).getOperationInstance(id2);

    // all are still in progress
    assertThat(executorManager.getOperationInstance(id)).isNotNull();
    assertThat(executorManager.getOperationInstance(id2)).isNotNull();

    operation.latch.countDown();
    operation2.latch.countDown();
  }

  @Test
  public void submitRogueOperation() {
    TestOperation operation = mock(TestOperation.class);
    assertThatThrownBy(() -> executorManager.submit(operation))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(" is not supported.");
  }

  static class TestOperation implements ClusterManagementOperation<TestOperationResult> {
    CountDownLatch latch = new CountDownLatch(1);

    @Override
    public String getEndpoint() {
      return "/operations/test";
    }
  }

  static class TestOperationResult implements OperationResult {
  }

  static TestOperationResult perform(Cache cache, TestOperation operation) {
    try {
      operation.latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }
}
