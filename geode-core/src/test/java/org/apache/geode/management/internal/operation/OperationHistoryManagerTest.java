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

import static org.apache.geode.test.awaitility.GeodeAwaitility.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
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
import org.apache.geode.test.awaitility.GeodeAwaitility;

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

    await().untilAsserted(() -> {
      verify(performer).apply(same(cache), same(testOperation1));
    });
  }

  @Test
  public void saveUpdatesOperationStateWhenOperationCompletes() {
    Cache cache = mock(Cache.class);
    TestOperation1 testOperation1 = new TestOperation1();
    OperationInstance<TestOperation1, TestOperationResult> operationInstance = mock(OperationInstance.class);
    TestOperationResult testOperationResult = new TestOperationResult();
    BiFunction<Cache, TestOperation1, TestOperationResult> performer = mock(BiFunction.class);

    when(performer.apply(any(), any())).thenReturn(testOperationResult);
    doReturn(operationInstance).when(operationHistoryPersistenceService).getOperationInstance(any());
    history.save(testOperation1, performer, cache, executor);

    await().untilAsserted(() -> {
      verify(operationInstance).setOperationEnd(any(), same(testOperationResult), isNull());
      verify(operationHistoryPersistenceService).update(same(operationInstance));
    });
  }

  @Test
  public void statusIsConsistentOnSuccess() {
    TestOperationResult testOperationResult = new TestOperationResult();
    TestOperation1 testOperation1 = new TestOperation1();
    BiFunction<Cache, TestOperation1, TestOperationResult> testOperation = (cache, testOpType) -> {
      try {
        testOpType.countDownLatch.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        System.out.println("Countdown interrupted");
      }
      return testOperationResult;
    };

    OperationInstance<TestOperation1, TestOperationResult> operationInstance = history.save(
        testOperation1, testOperation, null, executor);

    assertSoftly(softly -> {
      softly.assertThat(operationInstance.getId()).as("id pre-completion").isNotNull();

      doReturn(operationInstance).when(operationHistoryPersistenceService)
          .getOperationInstance(operationInstance.getId());

      softly.assertThat(operationInstance.getOperationEnd()).as("operationEnd pre-completion")
          .isNull();
      assertThat(operationInstance.getResult()).as("result pre-completion").isNull();

      testOperation1.downLatch();

      await().untilAsserted(
          () -> softly.assertThat(operationInstance.getOperationEnd())
              .as("operationEnd post-completion").isNotNull());
      await().untilAsserted(
          () -> softly.assertThat(operationInstance.getResult()).as("result post-completion")
              .isEqualTo(testOperationResult));
    });

    verify(operationHistoryPersistenceService, times(1))
        .getOperationInstance(operationInstance.getId());
  }

  @Test
  public void statusIsConsistentOnException() {
    TestOperationResult testOperationResult = new TestOperationResult();
    TestOperation1 testOperation1 = new TestOperation1();
    BiFunction<Cache, TestOperation1, TestOperationResult> testOperation = (cache, testOpType) -> {
      try {
        testOpType.countDownLatch.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        System.out.println("Countdown interrupted");
      }
      if (testOpType.exceptionLatch.getCount() == 0) {
        throw new RuntimeException("Long running operation failed");
      }
      return testOperationResult;
    };

    OperationInstance<TestOperation1, TestOperationResult> operationInstance = history.save(
        testOperation1, testOperation, null, executor);

    assertSoftly(softly -> {
      softly.assertThat(operationInstance.getId()).as("id pre-completion").isNotNull();

      doReturn(operationInstance).when(operationHistoryPersistenceService)
          .getOperationInstance(operationInstance.getId());

      softly.assertThat(operationInstance.getOperationEnd()).as("operationEnd pre-completion")
          .isNull();
      assertThat(operationInstance.getResult()).as("result pre-completion").isNull();

      testOperation1.downException();
      testOperation1.downLatch();

      await().untilAsserted(
          () -> softly.assertThat(operationInstance.getOperationEnd())
              .as("operationEnd post-completion").isNotNull());
      await().untilAsserted(
          () -> softly.assertThat(operationInstance.getResult()).as("result post-completion")
              .isNull());
      await().untilAsserted(
          () -> softly.assertThat(operationInstance.getThrowable()).as("throwable post-completion")
              .isNotNull());
      await().untilAsserted(
          () -> softly.assertThat(operationInstance.getThrowable().getMessage())
              .as("throwable message post-completion")
              .contains("operation failed"));
    });

    verify(operationHistoryPersistenceService, times(1))
        .getOperationInstance(operationInstance.getId());
  }

  @Test
  public void completedStatusIsConsistentEvenWhenReallyFast() {
    TestOperationResult testOperationResult = new TestOperationResult();
    TestOperation1 testOperation1 = new TestOperation1();
    BiFunction<Cache, TestOperation1, TestOperationResult> performer = (cache, testOpType) -> {
      try {
        testOpType.countDownLatch.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        System.out.println("Countdown interrupted");
      }
      return testOperationResult;
    };

    OperationInstance<TestOperation1, TestOperationResult> operationInstance = history.save(
        testOperation1, performer, null, executor);
    doReturn(operationInstance).when(operationHistoryPersistenceService)
        .getOperationInstance(operationInstance.getId());
    testOperation1.downLatch();

    assertSoftly(softly -> {
      softly.assertThat(operationInstance.getId()).as("id post-completion").isNotNull();
      softly.assertThat(operationInstance.getOperationStart()).as("start post-completion")
          .isNotNull();

      await().untilAsserted(
          () -> softly.assertThat(operationInstance.getOperationEnd())
              .as("operationEnd post-completion").isNotNull());
      await().untilAsserted(
          () -> softly.assertThat(operationInstance.getResult()).as("result post-completion")
              .isNotNull());
      await().untilAsserted(
          () -> softly.assertThat(operationInstance.getThrowable()).as("throwable post-completion")
              .isNull());
    });

    verify(operationHistoryPersistenceService, times(1))
        .getOperationInstance(operationInstance.getId());
    verify(operationHistoryPersistenceService, times(1))
        .update(operationInstance);
  }

  @Test
  public void retainsHistoryForAllInProgressOperations() {
    TestOperation1 testOperation1 = new TestOperation1();
    List<OperationInstance<TestOperation1, TestOperationResult>> sampleOps = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      sampleOps.add(new OperationInstance<>("op-" + i, testOperation1, new Date()));
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
      if (i%2 == 0) {
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
    private CountDownLatch exceptionLatch = new CountDownLatch(1);
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public String getEndpoint() {
      return null;
    }

    public void downException() {
      exceptionLatch.countDown();
    }

    public void downLatch() {
      countDownLatch.countDown();
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
