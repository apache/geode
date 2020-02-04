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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

public class OperationHistoryManagerTest {
  private OperationHistoryManager history;
  private OperationHistoryPersistenceService operationHistoryPersistenceService;

  @Before
  public void setUp() throws Exception {
    operationHistoryPersistenceService = mock(OperationHistoryPersistenceService.class);
    history = new OperationHistoryManager(2, TimeUnit.HOURS, operationHistoryPersistenceService);
  }

  @Test
  public void idNotFound() {
    assertThat(history.get("foo")).isNull();
  }

  @Test
  public void recordStartReturnsExpectedOpId() {
    ClusterManagementOperation<?> op = mock(ClusterManagementOperation.class);
    String expectedOpId = "12345";
    when(operationHistoryPersistenceService.recordStart(same(op))).thenReturn(expectedOpId);

    String opId = history.recordStart(op);
    assertThat(opId).isSameAs(expectedOpId);
  }

  @Test
  public void recordStartDelegatesToPersistenceService() {
    ClusterManagementOperation<?> op = mock(ClusterManagementOperation.class);

    String opId = history.recordStart(op);
    verify(operationHistoryPersistenceService).recordStart(same(op));
  }

  @Test
  public void recordEndDelegatesToPersistenceService() {
    String opId = "opId";
    OperationResult result = mock(OperationResult.class);
    Throwable cause = new Throwable();

    history.recordEnd(opId, result, cause);
    verify(operationHistoryPersistenceService).recordEnd(same(opId), same(result), same(cause));
  }

  @Test
  public void expireHistoryRetainsHistoryInProgressOperations() {
    List<OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> sampleOps =
        new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      OperationState<ClusterManagementOperation<OperationResult>, OperationResult> operationInstance =
          new OperationState<>("op-" + i, null, new Date());
      // Have not called operation end, so operation is still in progress.
      sampleOps.add(operationInstance);
    }
    doReturn(sampleOps).when(operationHistoryPersistenceService).list();

    history.expireHistory();

    verify(operationHistoryPersistenceService, never()).remove(any());
  }

  @Test
  public void expireHistoryRetainsUnexpiredCompletedOperations() {
    List<OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> sampleOps =
        new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      sampleOps.add(new OperationState<>("op-" + i, null, new Date()));
      sampleOps.get(i).setOperationEnd(new Date(), null, null);
    }

    doReturn(sampleOps).when(operationHistoryPersistenceService).list();

    history.expireHistory();

    verify(operationHistoryPersistenceService, never()).remove(any());
  }

  @Test
  public void expireHistoryRemovesExpiredCompletedOperations() {
    long now = System.currentTimeMillis();
    long twoAndAHalfHoursAgo = new Double(now - (3600 * 2.5 * 1000)).longValue();

    List<OperationState<?, ?>> sampleOps = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      sampleOps.add(new OperationState<>("op-" + i, null, new Date()));
      sampleOps.get(i).setOperationEnd(new Date(twoAndAHalfHoursAgo), null, null);
    }

    doReturn(sampleOps).when(operationHistoryPersistenceService).list();

    history.expireHistory();

    verify(operationHistoryPersistenceService, times(5)).remove(any());
  }

  @Test
  public void listFiltersByType() {
    ClusterManagementOperation<OperationResult> opType1 = mock(ClusterManagementOperation.class);
    ClusterManagementOperation<OperationResult> opType2 = new CmOperation();

    List<OperationState<?, ?>> sampleOps = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      if (i % 2 == 0) {
        sampleOps.add(new OperationState<>("op-" + i, opType1, new Date()));
      } else {
        sampleOps.add(new OperationState<>("op-" + i, opType2, new Date()));
      }
    }

    doReturn(sampleOps).when(operationHistoryPersistenceService).list();

    List<OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> opList1 =
        history.list(opType1);
    List<OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> opList2 =
        history.list(opType2);

    assertThat(opList1.size()).isEqualTo(5);
    assertThat(opList2.size()).isEqualTo(4);
  }

  @Test
  public void listCallsDelegatesToPersistenceService() {
    ClusterManagementOperation<OperationResult> op = mock(ClusterManagementOperation.class);

    history.list(op);

    // once for expireHistory, once directly
    verify(operationHistoryPersistenceService, times(2)).list();
  }

  @Test
  public void recordStartCallsExpireHistory() {
    OperationHistoryManager historySpy = spy(history);

    historySpy.recordStart(null);
    verify(historySpy).expireHistory();
  }

  @Test
  public void listCallsExpireHistory() {
    OperationHistoryManager historySpy = spy(history);

    historySpy.list(null);
    verify(historySpy).expireHistory();
  }

  @Test
  public void getCallsExpireHistory() {
    OperationHistoryManager historySpy = spy(history);

    historySpy.get(null);
    verify(historySpy).expireHistory();
  }

  private static class CmOperation implements ClusterManagementOperation<OperationResult> {

    @Override
    public String getEndpoint() {
      return null;
    }
  }
}
