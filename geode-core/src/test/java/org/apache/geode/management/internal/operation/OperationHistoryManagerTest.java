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
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
  private Cache cache;
  private Executor executor;

  @Before
  public void setUp() throws Exception {
    operationHistoryPersistenceService = mock(OperationHistoryPersistenceService.class);
    cache = mock(Cache.class);
    history = new OperationHistoryManager(2, TimeUnit.HOURS, operationHistoryPersistenceService);
    executor = LoggingExecutors.newThreadOnEachExecute("OHM_test");
  }

  @Test
  public void idNotFound() {
    assertThat(history.getOperationInstance("foo")).isNull();
  }

  @Test
  public void statusIsConsistentOnSuccess() {
    TestOperationResult testOperationResult = new TestOperationResult();
    TestOpType1 testOpType1 = new TestOpType1();
    BiFunction<Cache, TestOpType1, TestOperationResult> testOperation = (cache, testOpType) -> {
      try {
        testOpType.countDownLatch.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        System.out.println("Countdown interrupted");
      }
      return testOperationResult;
    };

    OperationInstance<TestOpType1, TestOperationResult> operationInstance = history.save(
        testOpType1, testOperation, cache, executor);

    assertSoftly(softly -> {
      softly.assertThat(operationInstance.getId()).as("id pre-completion").isNotNull();

      doReturn(operationInstance).when(operationHistoryPersistenceService)
          .getOperationInstance(operationInstance.getId());

      softly.assertThat(operationInstance.getOperationEnd()).as("operationEnd pre-completion")
          .isNull();
      assertThat(operationInstance.getResult()).as("result pre-completion").isNull();

      testOpType1.downLatch();

      GeodeAwaitility.await().untilAsserted(
          () -> softly.assertThat(operationInstance.getOperationEnd())
              .as("operationEnd post-completion").isNotNull());
      GeodeAwaitility.await().untilAsserted(
          () -> softly.assertThat(operationInstance.getResult()).as("result post-completion")
              .isEqualTo(testOperationResult));
    });

    verify(operationHistoryPersistenceService, times(1))
        .getOperationInstance(operationInstance.getId());
  }

  @Test
  public void statusIsConsistentOnException() {
    TestOperationResult testOperationResult = new TestOperationResult();
    TestOpType1 testOpType1 = new TestOpType1();
    BiFunction<Cache, TestOpType1, TestOperationResult> testOperation = (cache, testOpType) -> {
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

    OperationInstance<TestOpType1, TestOperationResult> operationInstance = history.save(
        testOpType1, testOperation, cache, executor);

    assertSoftly(softly -> {
      softly.assertThat(operationInstance.getId()).as("id pre-completion").isNotNull();

      doReturn(operationInstance).when(operationHistoryPersistenceService)
          .getOperationInstance(operationInstance.getId());

      softly.assertThat(operationInstance.getOperationEnd()).as("operationEnd pre-completion")
          .isNull();
      assertThat(operationInstance.getResult()).as("result pre-completion").isNull();

      testOpType1.downException();
      testOpType1.downLatch();

      GeodeAwaitility.await().untilAsserted(
          () -> softly.assertThat(operationInstance.getOperationEnd())
              .as("operationEnd post-completion").isNotNull());
      GeodeAwaitility.await().untilAsserted(
          () -> softly.assertThat(operationInstance.getResult()).as("result post-completion")
              .isNull());
      GeodeAwaitility.await().untilAsserted(
          () -> softly.assertThat(operationInstance.getThrowable()).as("throwable post-completion")
              .isNotNull());
      GeodeAwaitility.await().untilAsserted(
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
    TestOpType1 testOpType1 = new TestOpType1();
    BiFunction<Cache, TestOpType1, TestOperationResult> testOperation = (cache, testOpType) -> {
      try {
        testOpType.countDownLatch.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        System.out.println("Countdown interrupted");
      }
      return testOperationResult;
    };

    OperationInstance<TestOpType1, TestOperationResult> operationInstance = history.save(
        testOpType1, testOperation, cache, executor);
    doReturn(operationInstance).when(operationHistoryPersistenceService)
        .getOperationInstance(operationInstance.getId());
    testOpType1.downLatch();

    assertSoftly(softly -> {
      softly.assertThat(operationInstance.getId()).as("id post-completion").isNotNull();
      softly.assertThat(operationInstance.getOperationStart()).as("start post-completion")
          .isNotNull();

      GeodeAwaitility.await().untilAsserted(
          () -> softly.assertThat(operationInstance.getOperationEnd())
              .as("operationEnd post-completion").isNotNull());
      GeodeAwaitility.await().untilAsserted(
          () -> softly.assertThat(operationInstance.getResult()).as("result post-completion")
              .isNotNull());
      GeodeAwaitility.await().untilAsserted(
          () -> softly.assertThat(operationInstance.getThrowable()).as("throwable post-completion")
              .isNull());
    });

    verify(operationHistoryPersistenceService, times(1))
        .getOperationInstance(operationInstance.getId());
  }

  @Test
  public void retainsHistoryForAllInProgressOperations() {
    TestOpType1 opType1 = new TestOpType1();
    List<OperationInstance<TestOpType1, TestOperationResult>> sampleOps = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      sampleOps.add(new OperationInstance<>("op-" + i, opType1, new Date()));
    }

    doReturn(sampleOps).when(operationHistoryPersistenceService).listOperationInstances();

    List<OperationInstance<TestOpType1, TestOperationResult>> opList =
        history.listOperationInstances(opType1);

    assertThat(opList.size()).isEqualTo(3);

    verify(operationHistoryPersistenceService, never()).remove(any());
  }

  @Test
  public void expiresHistoryForCompletedOperation() {
    TestOpType1 opType1 = new TestOpType1();
    TestOperationResult testOperationResult = new TestOperationResult();
    long now = System.currentTimeMillis();
    long threeHoursAgo = now - (3600 * 3 * 1000);
    long twoAndAHalfHoursAgo = new Double(now - (3600 * 2.5 * 1000)).longValue();

    List<OperationInstance<TestOpType1, TestOperationResult>> sampleOps = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      sampleOps.add(new OperationInstance<>("op-" + i, opType1, new Date(threeHoursAgo)));
      if (i % 2 != 0) {
        sampleOps.get(i).setOperationEnd(new Date(twoAndAHalfHoursAgo), testOperationResult, null);
      }
    }

    doReturn(sampleOps).when(operationHistoryPersistenceService).listOperationInstances();

    List<OperationInstance<TestOpType1, TestOperationResult>> opList =
        history.listOperationInstances(opType1);

    assertThat(opList.size()).isEqualTo(5);

    verify(operationHistoryPersistenceService, times(2)).remove(any());
  }

  @Test
  public void listOperationsFiltersByType() {
    TestOpType1 opType1 = new TestOpType1();
    TestOpType2 opType2 = new TestOpType2();

    List<OperationInstance<?, TestOperationResult>> sampleOps = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      if (i%2 == 0) {
        sampleOps.add(new OperationInstance<>("op-" + i, opType1, new Date()));
      } else {
        sampleOps.add(new OperationInstance<>("op-" + i, opType2, new Date()));
      }
    }

    doReturn(sampleOps).when(operationHistoryPersistenceService).listOperationInstances();

    List<OperationInstance<TestOpType1, TestOperationResult>> opList1 =
        history.listOperationInstances(opType1);
    List<OperationInstance<TestOpType2, TestOperationResult>> opList2 =
        history.listOperationInstances(opType2);

    assertThat(opList1.size()).isEqualTo(5);
    assertThat(opList2.size()).isEqualTo(4);

    verify(operationHistoryPersistenceService, never()).remove(any());
  }

  static class TestOpType1 implements ClusterManagementOperation<TestOperationResult> {
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

  static class TestOpType2 implements ClusterManagementOperation<TestOperationResult> {
    @Override
    public String getEndpoint() {
      return null;
    }
  }

  private static class TestOperationResult implements OperationResult {
  }
}
