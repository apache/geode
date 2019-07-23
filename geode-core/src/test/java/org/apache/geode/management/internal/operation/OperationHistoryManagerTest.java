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
    history = new OperationHistoryManager(500, TimeUnit.MILLISECONDS);
  }

  @Test
  public void getNoStatus() {
    assertThat(history.getOperationInstance("foo")).isNull();
  }

  @Test
  public void getInProgStatus() {
    CompletableFuture<JsonSerializable> future = new CompletableFuture<>();
    CompletableFuture<JsonSerializable> future2 = history.save(op("1", future)).getFutureResult();
    assertThat(history.getOperationInstance("1").getFutureResult().isDone()).isFalse();
    assertThat(history.getOperationInstance("1").getFutureResult().isDone()).isFalse();
    assertThat(history.getOperationInstance("1").getFutureOperationEnded().isDone()).isFalse();
    future.complete(null);
    assertThat(history.getOperationInstance("1").getFutureResult().isDone()).isTrue();
    assertThat(history.getOperationInstance("1").getFutureResult().isDone()).isTrue();
    assertThat(history.getOperationInstance("1").getFutureOperationEnded().isDone()).isTrue();
  }

  @Test
  public void getCompletedBeforeStatus() {
    CompletableFuture<JsonSerializable> future = new CompletableFuture<>();
    future.complete(null);
    history.save(op("1", future));
    assertThat(history.getOperationInstance("1").getFutureResult()).isSameAs(future);
  }


  @Test
  public void getCompletedAfterStatus() {
    CompletableFuture<JsonSerializable> future = new CompletableFuture<>();
    history.save(op("1", future));
    future.complete(null);
    assertThat(history.getOperationInstance("1").getFutureResult()).isSameAs(future);
  }

  @Test
  public void getLotsOfInProgStatus() {
    history.save(op("1", new CompletableFuture<>()));
    history.save(op("2", new CompletableFuture<>()));
    history.save(op("3", new CompletableFuture<>()));
    assertThat(history.getOperationInstance("1")).isNotNull();
    assertThat(history.getOperationInstance("2")).isNotNull();
    assertThat(history.getOperationInstance("3")).isNotNull();
  }

  @Test
  public void getLotsOfCompleted() throws Exception {
    CompletableFuture<JsonSerializable> future1 = new CompletableFuture<>();
    future1.complete(null);
    history.save(op("1", future1));
    CompletableFuture<JsonSerializable> future2 = new CompletableFuture<>();
    history.save(op("2", future2));
    future2.complete(null);
    CompletableFuture<JsonSerializable> future3 = new CompletableFuture<>();
    history.save(op("3", future3));
    assertThat(history.getOperationInstance("1")).isNotNull();
    assertThat(history.getOperationInstance("2")).isNotNull();
    assertThat(history.getOperationInstance("3")).isNotNull();
    Thread.sleep(1000); // for test, completed expiry is 500ms so this should trigger expiry
    future3.complete(null);
    assertThat(history.getOperationInstance("1")).isNull();
    assertThat(history.getOperationInstance("2")).isNull();
    assertThat(history.getOperationInstance("3")).isNotNull();
  }

  @Test
  public void getSomeCompleted() {
    CompletableFuture<JsonSerializable> future1 = new CompletableFuture<>();
    future1.complete(null);
    history.save(op("1", future1));
    CompletableFuture<JsonSerializable> future2 = new CompletableFuture<>();
    history.save(op("2", future2));
    future2.complete(null);
    CompletableFuture<JsonSerializable> future3 = new CompletableFuture<>();
    history.save(op("3", future3));
    assertThat(history.getOperationInstance("1")).isNotNull();
    assertThat(history.getOperationInstance("2")).isNotNull();
    assertThat(history.getOperationInstance("3")).isNotNull();
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

  private static <A extends ClusterManagementOperation<V>, V extends JsonSerializable> OperationInstance<A, V> op(
      String id, CompletableFuture<V> future) {
    return op(id, future, new Date());
  }

  private static <A extends ClusterManagementOperation<V>, V extends JsonSerializable> OperationInstance<A, V> op(
      String id, CompletableFuture<V> future, Date startDate) {
    return new OperationInstance<>(future, id, null, startDate);
  }
}
