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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
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
  public void inProgressStatusIsConsistent() {
    TestOperationResult testOperationResult = new TestOperationResult();
    TestOpType1 testOpType1 = new TestOpType1();
    BiFunction<Cache, TestOpType1, TestOperationResult> testOperation = (cache, testOpType) -> {
      while (!testOpType.done) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          System.out.println(e.getMessage());
        }
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

      testOpType1.setDone(true);

      GeodeAwaitility.await().untilAsserted(() -> {
        softly.assertThat(operationInstance.getOperationEnd()).as("operationEnd post-completion")
            .isNotNull();
      });
      GeodeAwaitility.await().untilAsserted(() -> {
        softly.assertThat(operationInstance.getResult()).as("result post-completion")
            .isEqualTo(testOperationResult);
      });
    });

    verify(operationHistoryPersistenceService, times(1))
        .getOperationInstance(operationInstance.getId());
  }

  @Test
  public void endDateIsNotSetBeforeOperationCompletedFires() {
    CompletableFuture<TestOperationResult> future = new CompletableFuture<>();
    Date operationStart = new Date();
    String opId = "1";

    // history.save(opId, new TestOpType1(), operationStart, future);

    future.whenComplete(
        (r, e) -> {
          OperationInstance<TestOpType1, TestOperationResult> op =
              history.getOperationInstance(opId);
          assertThat(op).isNotNull();
          assertThat(op.getOperationEnd()).isNull();
        });
    future.complete(null);
  }

  @Test
  public void completedStatusIsConsistentOnSuccess() {
    CompletableFuture<TestOperationResult> future = new CompletableFuture<>();
    Date operationStart = new Date();
    String opId = "1";
    TestOperationResult testOperationResult = new TestOperationResult();

    // OperationInstance<TestOpType1, TestOperationResult> operationInstance = history.save(
    // opId, new TestOpType1(), operationStart, future);

    // doReturn(operationInstance).when(operationHistoryPersistenceService).getOperationInstance(opId);

    future.complete(testOperationResult);

    // assertThat(operationInstance.getOperationEnd()).isNotNull();
    // assertThat(operationInstance.getResult()).isEqualTo(testOperationResult);
  }

  @Test
  public void completedStatusIsConsistentOnException() {
    CompletableFuture<TestOperationResult> future = new CompletableFuture<>();
    Date operationStart = new Date();
    String opId = "1";

    // OperationInstance<TestOpType1, TestOperationResult> operationInstance = history.save(
    // opId, new TestOpType1(), operationStart, future);

    // doReturn(operationInstance).when(operationHistoryPersistenceService).getOperationInstance(opId);

    future.completeExceptionally(new Exception("An exceptional end to the operation"));
    //
    // assertThat(operationInstance.getOperationEnd()).isNotNull();
    // assertThat(operationInstance.getResult()).isNull();
    // assertThat(operationInstance.getThrowable()).isNotNull();
    // assertThat(operationInstance.getThrowable().getMessage()).contains("exceptional end");
  }

  @Test
  public void completedStatusIsConsistentEvenWhenReallyFast() {
    CompletableFuture<TestOperationResult> future = new CompletableFuture<>();
    Date operationStart = new Date();
    String opId = "1";
    TestOperationResult testOperationResult = new TestOperationResult();

    future.complete(testOperationResult);

    // OperationInstance<TestOpType1, TestOperationResult> operationInstance = history.save(
    // opId, new TestOpType1(), operationStart, future);

    // doReturn(operationInstance).when(operationHistoryPersistenceService).getOperationInstance(opId);
    //
    // assertThat(operationInstance.getOperationEnd()).isNotNull();
    // assertThat(operationInstance.getResult()).isEqualTo(testOperationResult);
  }

  @Test
  public void retainsHistoryForAllInProgressOperations() {
    // history = new OperationHistoryManager(0, TimeUnit.MILLISECONDS);
    // history.save(op("1", new CompletableFuture<>()));
    // history.save(op("2", new CompletableFuture<>()));
    assertThat(history.getOperationInstance("1")).isNotNull();
    assertThat(history.getOperationInstance("2")).isNotNull();
  }

  @Test
  public void expiresHistoryForCompletedOperation() {
    // history = new OperationHistoryManager(0, TimeUnit.MILLISECONDS);
    // history.save(op("1", new CompletableFuture<>())).getFutureResult().complete(null);
    assertThat(history.getOperationInstance("1")).isNull();
  }

  @Test
  public void timestampsAreCorrectWhenFutureIsAlreadyCompleteBeforeSave() throws Exception {
    CompletableFuture<OperationResult> future1 = new CompletableFuture<>();
    future1.complete(null);
    Date start = new Date();
    // history.save(op("1", future1, start));
    assertThat(history.getOperationInstance("1").getOperationStart()).isEqualTo(start);
    // assertThat(history.getOperationInstance("1").getFutureOperationEnded().isDone()).isTrue();
    // assertThat(history.getOperationInstance("1").getFutureOperationEnded().get().getTime())
    // .isGreaterThanOrEqualTo(start.getTime());
  }

  @Test
  public void timestampsAreCorrectWhenFutureCompletesAfterSave() throws Exception {
    CompletableFuture<OperationResult> future2 = new CompletableFuture<>();
    Date start = new Date();
    // history.save(op("2", future2, start));
    // assertThat(history.getOperationInstance("2").getFutureOperationEnded().isDone()).isFalse();
    // future2.complete(null);
    // assertThat(history.getOperationInstance("2").getOperationStart()).isEqualTo(start);
    // assertThat(history.getOperationInstance("2").getFutureOperationEnded().isDone()).isTrue();
    // assertThat(history.getOperationInstance("2").getFutureOperationEnded().get().getTime())
    // .isGreaterThanOrEqualTo(start.getTime());
  }

  @Test
  public void onlyExpiresOldOperations() {
    // make op1 one ended yesterday
    // OperationInstance<?, ?> op1 = history.save(op("1", new CompletableFuture<>()));
    // op1.getFutureOperationEnded().complete(new Date(System.currentTimeMillis() - 86400000));
    // op1.getFutureResult().complete(null);

    // op2 ended just now
    // history.save(op("2", new CompletableFuture<>())).getFutureResult().complete(null);

    assertThat(history.getOperationInstance("1")).isNull();
    assertThat(history.getOperationInstance("2")).isNotNull();
  }

  @Test
  public void listOperationsFiltersByType() {
    // OperationInstance<OpType1, OperationResult> op1a = history.save(op("1a", new OpType1()));
    // OperationInstance<OpType1, OperationResult> op1b = history.save(op("1b", new OpType1()));
    // OperationInstance<OpType2, OperationResult> op2a = history.save(op("2a", new OpType2()));
    assertThat(history.listOperationInstances(new TestOpType1()).size()).isEqualTo(2);
    assertThat(history.listOperationInstances(new TestOpType2()).size()).isEqualTo(1);
  }

  static class TestOpType1 implements ClusterManagementOperation<TestOperationResult> {
    private boolean done = false;
    private boolean failed = false;

    @Override
    public String getEndpoint() {
      return null;
    }

    public void setDone(boolean done) {
      this.done = done;
    }

    public void setFailed(boolean failed) {
      this.failed = failed;
    }
  }

  static class TestOpType2 implements ClusterManagementOperation<TestOperationResult> {
    @Override
    public String getEndpoint() {
      return null;
    }
  }

  private static <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationInstance<A, V> op(
      String id) {
    return op(id, null, new Date());
  }

  private static <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationInstance<A, V> op(
      String id, A op, Date operationStart) {
    return new OperationInstance<>(id, op, operationStart);
  }

  private static <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationInstance<A, V> op(
      String id, A op) {
    return new OperationInstance<>(id, op, new Date());
  }

  private static class TestOperationResult implements OperationResult {
  }
}
