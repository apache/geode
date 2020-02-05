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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

public class RegionOperationHistoryPersistenceServiceTest {
  private OperationHistoryPersistenceService historyPersistenceService;
  private Supplier<String> uniqueIdSupplier;
  private Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> region;
  private InternalCache cache;

  @Before
  public void init() {
    uniqueIdSupplier = mock(Supplier.class);
    when(uniqueIdSupplier.get()).thenReturn("defaultId");
    region = mock(Region.class);
    historyPersistenceService =
        new RegionOperationHistoryPersistenceService(uniqueIdSupplier, region);
    cache = mock(InternalCache.class);
  }

  @Test
  public void recordStartReturnsAnIdFromProvidedSupplier() {
    ClusterManagementOperation<OperationResult> operation = mock(ClusterManagementOperation.class);
    String uniqueId = ";lkajdfa;ldkjfppoiuqe.,.,mnavc098";
    when(uniqueIdSupplier.get()).thenReturn(uniqueId);
    String opId = historyPersistenceService.recordStart(operation);

    assertThat(opId).isSameAs(uniqueId);
    verify(uniqueIdSupplier).get();
  }

  @Test
  public void recordStartRecordsOperationStatusInGivenRegion() {
    ClusterManagementOperation<OperationResult> operation = mock(ClusterManagementOperation.class);

    String opId = historyPersistenceService.recordStart(operation);

    ArgumentCaptor<OperationState> capturedOperationInstance = ArgumentCaptor.forClass(
        OperationState.class);
    verify(region).put(eq(opId), capturedOperationInstance.capture());
    OperationState operationInstance = capturedOperationInstance.getValue();

    assertThat(operationInstance).as("operationInstance").isNotNull();

    assertThat(operationInstance.getId()).as("id").isEqualTo(opId);
    assertThat(operationInstance.getOperation()).as("operation").isSameAs(operation);
    assertThat(operationInstance.getOperationStart()).as("start").isNotNull();
    assertThat(operationInstance.getThrowable()).as("throwable").isNull();
    assertThat(operationInstance.getOperationEnd()).as("end").isNull();
    assertThat(operationInstance.getResult()).as("result").isNull();
  }

  @Test
  public void recordEndRecordsSuccessfulCompletion() {
    String opId = "my-id";
    OperationState operationState = mock(OperationState.class);
    when(region.get(opId)).thenReturn(operationState);

    OperationResult operationResult = mock(OperationResult.class);
    Throwable thrownByOperation = new RuntimeException();

    historyPersistenceService.recordEnd(opId, operationResult, thrownByOperation);

    verify(operationState).setOperationEnd(notNull(), same(operationResult),
        same(thrownByOperation));
    verifyNoMoreInteractions(operationState);

    verify(region).put(eq(opId), same(operationState));
  }

  @Test
  public void removeRemovesIdentifiedOperationStateFromRegion() {
    String opId = "doomed-operation";

    historyPersistenceService.remove(opId);

    verify(region).remove(opId);
  }

  @Test
  public void getReturnsOperationFromRegion() {
    String opId = "doomed-operation";
    OperationState recordedOperationState = mock(OperationState.class);

    when(region.get(opId)).thenReturn(recordedOperationState);

    OperationState operationState = historyPersistenceService.get(opId);

    assertThat(operationState).isSameAs(recordedOperationState);
  }

  @Test
  public void listReturnsOperationsFromRegion() {
    ArrayList list = new ArrayList<>();
    list.add(new OperationState("op1", null, null));
    list.add(new OperationState("op2", null, null));
    when(region.values()).thenReturn(list);

    List result = historyPersistenceService.list();

    assertThat(result).containsExactlyElementsOf(list);
  }

  @Test
  public void cacheConstructorUsesExistingRegion() {
    Region region = mock(Region.class);
    when(cache.getRegion(RegionOperationHistoryPersistenceService.OPERATION_HISTORY_REGION_NAME))
        .thenReturn(region);

    RegionOperationHistoryPersistenceService result =
        new RegionOperationHistoryPersistenceService(cache);

    assertThat(result.getRegion()).isSameAs(region);
  }
}
