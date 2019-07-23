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

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.api.JsonSerializable;
import org.apache.geode.management.internal.operation.OperationHistoryManager.OperationInstance;

public class OperationHistoryManagerTest {
  private OperationHistoryManager history;

  @Before
  public void setUp() throws Exception {
    history = new OperationHistoryManager();
  }

  @Test
  public void idNotFound() {
    assertThat(history.getOperationInstance("foo")).isNull();
  }

  @Test
  public void inProgressStatusIsConsistent() {
    CompletableFuture<JsonSerializable> future = new CompletableFuture<>();
    CompletableFuture<JsonSerializable> future2 = history.save(op("1", future)).getFutureResult();
    assertThat(future2.isDone()).isFalse();
    assertThat(history.getOperationInstance("1").getFutureResult().isDone()).isFalse();
    assertThat(history.getOperationInstance("1").getFutureOperationEnded().isDone()).isFalse();
  }

  @Test
  public void endDateIsSetBeforeOperationCompletedFires() {
    CompletableFuture<JsonSerializable> future = new CompletableFuture<>();
    future.whenComplete(
        (r, e) -> assertThat(history.getOperationInstance("1").getFutureOperationEnded().isDone())
            .isTrue());
    future.complete(null);
  }

  @Test
  public void completedStatusIsConsistent() {
    CompletableFuture<JsonSerializable> future = new CompletableFuture<>();
    CompletableFuture<JsonSerializable> future2 = history.save(op("1", future)).getFutureResult();
    future.complete(null);
    assertThat(future2.isDone()).isTrue();
    assertThat(history.getOperationInstance("1").getFutureResult().isDone()).isTrue();
    assertThat(history.getOperationInstance("1").getFutureOperationEnded().isDone()).isTrue();
  }

  @Test
  public void completedStatusIsConsistentWhenResultOfSaveIsCompleted() {
    CompletableFuture<JsonSerializable> future = new CompletableFuture<>();
    CompletableFuture<JsonSerializable> future2 = history.save(op("1", future)).getFutureResult();
    future2.complete(null);
    assertThat(future.isDone()).isTrue();
    assertThat(history.getOperationInstance("1").getFutureResult().isDone()).isTrue();
    assertThat(history.getOperationInstance("1").getFutureOperationEnded().isDone()).isTrue();
  }

  @Test
  public void completedStatusIsConsistentEvenWhenReallyFast() {
    CompletableFuture<JsonSerializable> future = new CompletableFuture<>();
    future.complete(null);
    CompletableFuture<JsonSerializable> future2 = history.save(op("1", future)).getFutureResult();
    assertThat(future2.isDone()).isTrue();
    assertThat(history.getOperationInstance("1").getFutureResult().isDone()).isTrue();
    assertThat(history.getOperationInstance("1").getFutureOperationEnded().isDone()).isTrue();
  }

  @Test
  public void retainsHistoryForAllInProgressOperations() {
    history = new OperationHistoryManager(0, TimeUnit.MILLISECONDS);
    history.save(op("1", new CompletableFuture<>()));
    history.save(op("2", new CompletableFuture<>()));
    assertThat(history.getOperationInstance("1")).isNotNull();
    assertThat(history.getOperationInstance("2")).isNotNull();
  }

  @Test
  public void expiresHistoryForCompletedOperation() {
    history = new OperationHistoryManager(0, TimeUnit.MILLISECONDS);
    history.save(op("1", new CompletableFuture<>())).getFutureResult().complete(null);
    assertThat(history.getOperationInstance("1")).isNull();
  }

  @Test
  public void timestampsAreCorrectWhenFutureIsAlreadyCompleteBeforeSave() throws Exception {
    CompletableFuture<JsonSerializable> future1 = new CompletableFuture<>();
    future1.complete(null);
    Date start = new Date();
    history.save(op("1", future1, start));
    assertThat(history.getOperationInstance("1").getOperationStart()).isEqualTo(start);
    assertThat(history.getOperationInstance("1").getFutureOperationEnded().isDone()).isTrue();
    assertThat(history.getOperationInstance("1").getFutureOperationEnded().get().getTime())
        .isGreaterThanOrEqualTo(start.getTime());
  }

  @Test
  public void timestampsAreCorrectWhenFutureCompletesAfterSave() throws Exception {
    CompletableFuture<JsonSerializable> future2 = new CompletableFuture<>();
    Date start = new Date();
    history.save(op("2", future2, start));
    assertThat(history.getOperationInstance("2").getFutureOperationEnded().isDone()).isFalse();
    future2.complete(null);
    assertThat(history.getOperationInstance("2").getOperationStart()).isEqualTo(start);
    assertThat(history.getOperationInstance("2").getFutureOperationEnded().isDone()).isTrue();
    assertThat(history.getOperationInstance("2").getFutureOperationEnded().get().getTime())
        .isGreaterThanOrEqualTo(start.getTime());
  }

  @Test
  public void onlyExpiresOldOperations() {
    // make op1 one ended yesterday
    OperationInstance<?, ?> op1 = history.save(op("1", new CompletableFuture<>()));
    op1.getFutureOperationEnded().complete(new Date(System.currentTimeMillis() - 86400000));
    op1.getFutureResult().complete(null);

    // op2 ended just now
    history.save(op("2", new CompletableFuture<>())).getFutureResult().complete(null);

    assertThat(history.getOperationInstance("1")).isNull();
    assertThat(history.getOperationInstance("2")).isNotNull();
  }

  private static <A extends ClusterManagementOperation<V>, V extends JsonSerializable> OperationInstance<A, V> op(
      String id, CompletableFuture<V> future) {
    return op(id, future, new Date());
  }

  private static <A extends ClusterManagementOperation<V>, V extends JsonSerializable> OperationInstance<A, V> op(
      String id, CompletableFuture<V> future, Date startDate) {
    return new OperationInstance<>(future, id, null, startDate);
  }
}
