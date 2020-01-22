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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.internal.operation.OperationHistoryManager.OperationInstance;
import org.apache.geode.management.runtime.OperationResult;

public class OperationHistoryManagerTest {
  private OperationHistoryManager history;
  private OperationHistoryPersistenceService operationHistoryPersistenceService;
  private Executor executor;

  @Before
  public void setUp() throws Exception {
    operationHistoryPersistenceService = mock(OperationHistoryPersistenceService.class);
    history = new OperationHistoryManager(2, TimeUnit.HOURS, operationHistoryPersistenceService);
    executor = LoggingExecutors.newThreadOnEachExecute("OHM_test");
  }

  @Test
  public void idNotFound() {
    assertThat(history.getOperationInstance("foo")).isNull();
  }

  @Test
  public void saveSetsId() {
    BiFunction<Cache, TestOperation1, TestOperationResult> performer = (cache, testOpType) -> null;

    OperationInstance<TestOperation1, TestOperationResult> operationInstance = history.save(
        null, performer, null, mock(Executor.class));

    assertThat(operationInstance).isNotNull();
    assertThat(operationInstance.getId()).isNotNull();
  }

  @Test
  public void saveSetsStartDate() {
    BiFunction<Cache, TestOperation1, TestOperationResult> performer = (cache, testOpType) -> null;

    OperationInstance<TestOperation1, TestOperationResult> operationInstance = history.save(
        null, performer, null, mock(Executor.class));

    assertThat(operationInstance).isNotNull();
    assertThat(operationInstance.getOperationStart()).isNotNull();
  }

  @Test
  public void saveSetsOperation() {
    TestOperation1 testOperation1 = new TestOperation1();
    BiFunction<Cache, TestOperation1, TestOperationResult> performer = (cache, testOpType) -> null;

    OperationInstance<TestOperation1, TestOperationResult> operationInstance = history.save(
        testOperation1, performer, null, mock(Executor.class));

    assertThat(operationInstance).isNotNull();
    assertThat(operationInstance.getOperation()).isEqualTo(testOperation1);
  }

  @Test
  public void savePassesCacheAndOperationToPerformer() {
    Cache cache = mock(Cache.class);
    TestOperation1 testOperation1 = new TestOperation1();
    BiFunction<Cache, TestOperation1, TestOperationResult> performer = mock(BiFunction.class);

    history.save(testOperation1, performer, cache, executor);

    await().untilAsserted(() -> verify(performer).apply(same(cache), same(testOperation1)));
  }

  @Test
  public void saveUpdatesOperationStateWhenOperationCompletesSuccessfully() {
    Cache cache = mock(Cache.class);
    TestOperation1 testOperation1 = new TestOperation1();
    OperationInstance<TestOperation1, TestOperationResult> operationInstance =
        mock(OperationInstance.class);
    TestOperationResult testOperationResult = new TestOperationResult();
    BiFunction<Cache, TestOperation1, TestOperationResult> performer = mock(BiFunction.class);

    when(performer.apply(any(), any())).thenReturn(testOperationResult);
    doReturn(operationInstance).when(operationHistoryPersistenceService)
        .getOperationInstance(any());
    history.save(testOperation1, performer, cache, executor);

    await().untilAsserted(() -> {
      verify(operationInstance).setOperationEnd(any(), same(testOperationResult), isNull());
      verify(operationHistoryPersistenceService).update(same(operationInstance));
    });
  }

  @Test
  public void saveUpdatesOperationStateWhenOperationCompletesExceptionally() {
    Cache cache = mock(Cache.class);
    TestOperation1 testOperation1 = new TestOperation1();
    OperationInstance<TestOperation1, TestOperationResult> operationInstance =
        mock(OperationInstance.class);
    BiFunction<Cache, TestOperation1, TestOperationResult> performer = mock(BiFunction.class);

    RuntimeException thrownByPerformer = new RuntimeException();
    doThrow(thrownByPerformer).when(performer).apply(any(), any());
    doReturn(operationInstance).when(operationHistoryPersistenceService)
        .getOperationInstance(any());

    history.save(testOperation1, performer, cache, executor);

    await().untilAsserted(() -> {
      verify(operationInstance).setOperationEnd(any(), isNull(), same(thrownByPerformer));
      verify(operationHistoryPersistenceService).update(same(operationInstance));
    });
  }

  @Test
  public void callsUpdateOnlyAfterPerformerCompletes() throws InterruptedException {
    CountDownLatch performerIsInProgress = new CountDownLatch(1);
    CountDownLatch performerHasTestPermissionToComplete = new CountDownLatch(1);

    when(operationHistoryPersistenceService.getOperationInstance(any()))
        .thenReturn(mock(OperationInstance.class));

    BiFunction<Cache, TestOperation1, TestOperationResult> performer = (cache, operation) -> {
      try {
        performerIsInProgress.countDown();
        performerHasTestPermissionToComplete.await(10, SECONDS);
      } catch (InterruptedException e) {
        System.out.println("Countdown interrupted");
      }
      return null;
    };

    history.save(null, performer, null, executor);

    performerIsInProgress.await(10, SECONDS);

    verify(operationHistoryPersistenceService, never()).update(any());

    performerHasTestPermissionToComplete.countDown();

    await().untilAsserted(() -> verify(operationHistoryPersistenceService).update(any()));
  }

  @Test
  public void retainsHistoryForAllInProgressOperations() {
    TestOperation1 testOperation1 = new TestOperation1();
    List<OperationInstance<TestOperation1, TestOperationResult>> sampleOps = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      OperationInstance<TestOperation1, TestOperationResult> operationInstance =
          new OperationInstance<>("op-" + i, testOperation1, new Date());
      // Have not called operation end, so operation is still in progress.
      sampleOps.add(operationInstance);
    }

    doReturn(sampleOps).when(operationHistoryPersistenceService).listOperationInstances();

    List<OperationInstance<TestOperation1, TestOperationResult>> opList =
        history.listOperationInstances(testOperation1);

    assertThat(opList.size()).isEqualTo(3);

    verify(operationHistoryPersistenceService, never()).remove(any());
  }

  @Test
  public void expiresHistoryForCompletedOperation() {
    TestOperation1 opType1 = new TestOperation1();
    TestOperationResult testOperationResult = new TestOperationResult();
    long now = System.currentTimeMillis();
    long threeHoursAgo = now - (3600 * 3 * 1000);
    long twoAndAHalfHoursAgo = new Double(now - (3600 * 2.5 * 1000)).longValue();

    List<OperationInstance<TestOperation1, TestOperationResult>> sampleOps = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      sampleOps.add(new OperationInstance<>("op-" + i, opType1, new Date(threeHoursAgo)));
      if (i % 2 != 0) {
        sampleOps.get(i).setOperationEnd(new Date(twoAndAHalfHoursAgo), testOperationResult, null);
      }
    }

    doReturn(sampleOps).when(operationHistoryPersistenceService).listOperationInstances();

    List<OperationInstance<TestOperation1, TestOperationResult>> opList =
        history.listOperationInstances(opType1);

    assertThat(opList.size()).isEqualTo(5);

    verify(operationHistoryPersistenceService, times(2)).remove(any());
  }

  @Test
  public void listOperationsFiltersByType() {
    TestOperation1 opType1 = new TestOperation1();
    TestOperation2 opType2 = new TestOperation2();

    List<OperationInstance<?, TestOperationResult>> sampleOps = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      if (i % 2 == 0) {
        sampleOps.add(new OperationInstance<>("op-" + i, opType1, new Date()));
      } else {
        sampleOps.add(new OperationInstance<>("op-" + i, opType2, new Date()));
      }
    }

    doReturn(sampleOps).when(operationHistoryPersistenceService).listOperationInstances();

    List<OperationInstance<TestOperation1, TestOperationResult>> opList1 =
        history.listOperationInstances(opType1);
    List<OperationInstance<TestOperation2, TestOperationResult>> opList2 =
        history.listOperationInstances(opType2);

    assertThat(opList1.size()).isEqualTo(5);
    assertThat(opList2.size()).isEqualTo(4);

    verify(operationHistoryPersistenceService, never()).remove(any());
  }

  static class TestOperation1 implements ClusterManagementOperation<TestOperationResult> {
    @Override
    public String getEndpoint() {
      return null;
    }
  }

  static class TestOperation2 implements ClusterManagementOperation<TestOperationResult> {
    @Override
    public String getEndpoint() {
      return null;
    }
  }

  private static class TestOperationResult implements OperationResult {
  }
}
