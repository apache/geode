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

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.api.JsonSerializable;
import org.apache.geode.management.internal.operation.OperationHistoryManager.OperationInstance;

public class OperationHistoryManagerTest {
  private OperationHistoryManager history;

  @Before
  public void setUp() throws Exception {
    history = new OperationHistoryManager(2);
  }

  @Test
  public void getNoStatus() {
    assertThat(history.getStatus("foo")).isNull();
  }

  @Test
  public void getInProgStatus() {
    CompletableFuture<JsonSerializable> future = new CompletableFuture<>();
    CompletableFuture<JsonSerializable> future2 = history.save(op("1", future)).getFuture();
    assertThat(history.getStatus("1")).isSameAs(future2);
  }

  @Test
  public void getCompletedBeforeStatus() {
    CompletableFuture<JsonSerializable> future = new CompletableFuture<>();
    future.complete(null);
    history.save(op("1", future));
    assertThat(history.getStatus("1")).isSameAs(future);
  }


  @Test
  public void getCompletedAfterStatus() {
    CompletableFuture<JsonSerializable> future = new CompletableFuture<>();
    history.save(op("1", future));
    future.complete(null);
    assertThat(history.getStatus("1")).isSameAs(future);
  }

  @Test
  public void getLotsOfInProgStatus() {
    history.save(op("1", new CompletableFuture<>()));
    history.save(op("2", new CompletableFuture<>()));
    history.save(op("3", new CompletableFuture<>()));
    assertThat(history.getStatus("1")).isNotNull();
    assertThat(history.getStatus("2")).isNotNull();
    assertThat(history.getStatus("3")).isNotNull();
  }

  @Test
  public void getLotsOfCompleted() {
    CompletableFuture<JsonSerializable> future1 = new CompletableFuture<>();
    future1.complete(null);
    history.save(op("1", future1));
    CompletableFuture<JsonSerializable> future2 = new CompletableFuture<>();
    history.save(op("2", future2));
    future2.complete(null);
    CompletableFuture<JsonSerializable> future3 = new CompletableFuture<>();
    history.save(op("3", future3));
    future3.complete(null);
    assertThat(history.getStatus("1")).isNull();
    assertThat(history.getStatus("2")).isNotNull();
    assertThat(history.getStatus("3")).isNotNull();
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
    assertThat(history.getStatus("1")).isNotNull();
    assertThat(history.getStatus("2")).isNotNull();
    assertThat(history.getStatus("3")).isNotNull();
  }

  @Test
  public void timestampsAreCorrect() throws Exception {
    CompletableFuture<JsonSerializable> future1 = new CompletableFuture<>();
    future1.complete(null);
    long now = System.currentTimeMillis();
    history.save(op("1", future1));
    assertThat(history.getOperationStart("1").getTime()).isBetween(now - 10000, now);
    assertThat(history.getOperationEnded("1").isDone()).isTrue();
    assertThat(history.getOperationEnded("1").get().getTime())
        .isGreaterThanOrEqualTo(history.getOperationStart("1").getTime());
    CompletableFuture<JsonSerializable> future2 = new CompletableFuture<>();
    history.save(op("2", future2));
    assertThat(history.getOperationEnded("2").isDone()).isFalse();
    future2.complete(null);
    now = System.currentTimeMillis();
    assertThat(history.getOperationStart("2").getTime()).isBetween(now - 10000, now);
    assertThat(history.getOperationEnded("2").isDone()).isTrue();
    assertThat(history.getOperationEnded("2").get().getTime())
        .isGreaterThanOrEqualTo(history.getOperationStart("2").getTime());
    CompletableFuture<JsonSerializable> future3 = new CompletableFuture<>();
    history.save(op("3", future3));
    assertThat(history.getOperationEnded("3").isDone()).isFalse();
  }

  private static <A extends ClusterManagementOperation<V>, V extends JsonSerializable> OperationInstance<A, V> op(
      String id, CompletableFuture<V> future) {
    A op = null;
    return new OperationInstance<>(future, id, op, new Date());
  }
}
