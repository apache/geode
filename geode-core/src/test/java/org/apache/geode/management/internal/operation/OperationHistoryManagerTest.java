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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementOperation;

public class OperationHistoryManagerTest {
  OperationHistoryManager history;

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
    CompletableFuture future = new CompletableFuture();
    history.save(op("1"), future);
    assertThat(history.getStatus("1")).isSameAs(future);
  }

  @Test
  public void getCompletedBeforeStatus() {
    CompletableFuture future = new CompletableFuture();
    future.complete(null);
    history.save(op("1"), future);
    assertThat(history.getStatus("1")).isSameAs(future);
  }


  @Test
  public void getCompletedAfterStatus() {
    CompletableFuture future = new CompletableFuture();
    history.save(op("1"), future);
    future.complete(null);
    assertThat(history.getStatus("1")).isSameAs(future);
  }

  @Test
  public void getLotsOfInProgStatus() {
    history.save(op("1"), new CompletableFuture());
    history.save(op("2"), new CompletableFuture());
    history.save(op("3"), new CompletableFuture());
    assertThat(history.getStatus("1")).isNotNull();
    assertThat(history.getStatus("2")).isNotNull();
    assertThat(history.getStatus("3")).isNotNull();
  }

  @Test
  public void getLotsOfCompleted() {
    CompletableFuture future1 = new CompletableFuture();
    future1.complete(null);
    history.save(op("1"), future1);
    CompletableFuture future2 = new CompletableFuture();
    history.save(op("2"), future2);
    future2.complete(null);
    CompletableFuture future3 = new CompletableFuture();
    history.save(op("3"), future3);
    future3.complete(null);
    assertThat(history.getStatus("1")).isNull();
    assertThat(history.getStatus("2")).isNotNull();
    assertThat(history.getStatus("3")).isNotNull();
  }

  @Test
  public void getSomeCompleted() {
    CompletableFuture future1 = new CompletableFuture();
    future1.complete(null);
    history.save(op("1"), future1);
    CompletableFuture future2 = new CompletableFuture();
    history.save(op("2"), future2);
    future2.complete(null);
    CompletableFuture future3 = new CompletableFuture();
    history.save(op("3"), future3);
    assertThat(history.getStatus("1")).isNotNull();
    assertThat(history.getStatus("2")).isNotNull();
    assertThat(history.getStatus("3")).isNotNull();
  }

  private static ClusterManagementOperation op(String id) {
    ClusterManagementOperation op = mock(ClusterManagementOperation.class);
    when(op.getId()).thenReturn(id);
    return op;
  }
}
