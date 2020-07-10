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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

public class OperationManagerTest {
  private OperationManager executorManager;
  private OperationHistoryManager operationHistoryManager;
  private InternalCache cache;

  @Before
  public void setUp() throws Exception {
    operationHistoryManager = mock(OperationHistoryManager.class);
    cache = mock(InternalCache.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(cache.getMyId()).thenReturn(member);
    executorManager = new OperationManager(cache, operationHistoryManager);
  }

  @Test
  public void submitPassesCacheAndOperationToPerformer() {
    OperationPerformer<ClusterManagementOperation<OperationResult>, OperationResult> performer =
        mock(OperationPerformer.class);
    ClusterManagementOperation<OperationResult> operation = mock(ClusterManagementOperation.class);
    executorManager.registerOperation(
        (Class<ClusterManagementOperation<OperationResult>>) operation.getClass(), performer);

    executorManager.submit(operation);

    await().untilAsserted(() -> verify(performer).perform(same(cache), same(operation)));
  }

  @Test
  public void submitReturnsOperationState() {
    OperationPerformer<ClusterManagementOperation<OperationResult>, OperationResult> performer =
        mock(OperationPerformer.class);
    ClusterManagementOperation<OperationResult> operation = mock(ClusterManagementOperation.class);
    String opId = "opId";
    OperationState<ClusterManagementOperation<OperationResult>, OperationResult> expectedOpState =
        mock(OperationState.class);
    executorManager.registerOperation(
        (Class<ClusterManagementOperation<OperationResult>>) operation.getClass(), performer);

    when(operationHistoryManager.recordStart(eq(operation), any())).thenReturn(opId);
    when(operationHistoryManager.get(opId)).thenReturn(expectedOpState);

    OperationState<ClusterManagementOperation<OperationResult>, OperationResult> operationState =
        executorManager.submit(operation);

    assertThat(operationState).isSameAs(expectedOpState);
  }

  @Test
  public void submitUpdatesOperationStateWhenOperationCompletesSuccessfully() {
    OperationPerformer<ClusterManagementOperation<OperationResult>, OperationResult> performer =
        mock(OperationPerformer.class);
    OperationResult operationResult = mock(OperationResult.class);
    ClusterManagementOperation<OperationResult> operation = mock(ClusterManagementOperation.class);
    executorManager.registerOperation(
        (Class<ClusterManagementOperation<OperationResult>>) operation.getClass(), performer);

    when(performer.perform(any(), any())).thenReturn(operationResult);
    String opId = "my-op-id";
    when(operationHistoryManager.recordStart(any(), any())).thenReturn(opId);

    executorManager.submit(operation);

    await().untilAsserted(() -> verify(operationHistoryManager)
        .recordEnd(eq(opId), same(operationResult), isNull()));
  }

  @Test
  public void submitUpdatesOperationStateWhenOperationCompletesExceptionally() {
    OperationPerformer<ClusterManagementOperation<OperationResult>, OperationResult> performer =
        mock(OperationPerformer.class);
    ClusterManagementOperation<OperationResult> operation = mock(ClusterManagementOperation.class);
    executorManager.registerOperation(
        (Class<ClusterManagementOperation<OperationResult>>) operation.getClass(), performer);

    RuntimeException thrownByPerformer = new RuntimeException();
    doThrow(thrownByPerformer).when(performer).perform(any(), any());
    String opId = "my-op-id";
    when(operationHistoryManager.recordStart(any(), any())).thenReturn(opId);

    executorManager.submit(operation);

    await().untilAsserted(() -> verify(operationHistoryManager)
        .recordEnd(eq(opId), isNull(), same(thrownByPerformer)));
  }

  @Test
  public void submitCallsRecordEndOnlyAfterPerformerCompletes() throws InterruptedException {
    ClusterManagementOperation<OperationResult> operation = mock(ClusterManagementOperation.class);
    CountDownLatch performerIsInProgress = new CountDownLatch(1);
    CountDownLatch performerHasTestPermissionToComplete = new CountDownLatch(1);

    String opId = "my-op-id";
    when(operationHistoryManager.recordStart(any(), any())).thenReturn(opId);

    OperationResult operationResult = mock(OperationResult.class);

    OperationPerformer<ClusterManagementOperation<OperationResult>, OperationResult> performer =
        (cache, op) -> {
          try {
            performerIsInProgress.countDown();
            performerHasTestPermissionToComplete.await(10, SECONDS);
          } catch (InterruptedException e) {
            System.out.println("Countdown interrupted");
          }
          return operationResult;
        };

    executorManager.registerOperation(
        (Class<ClusterManagementOperation<OperationResult>>) operation.getClass(), performer);

    executorManager.submit(operation);

    performerIsInProgress.await(10, SECONDS);

    verify(operationHistoryManager, never()).recordEnd(any(), any(), any());

    performerHasTestPermissionToComplete.countDown();

    await().untilAsserted(() -> verify(operationHistoryManager)
        .recordEnd(eq(opId), same(operationResult), isNull()));
  }

  @Test
  public void submitRogueOperation() {
    ClusterManagementOperation<?> operation = mock(ClusterManagementOperation.class);
    assertThatThrownBy(() -> executorManager.submit(operation))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(" is not supported.");
  }
}
