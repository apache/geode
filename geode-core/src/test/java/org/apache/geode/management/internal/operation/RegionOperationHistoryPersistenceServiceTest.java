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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

public class RegionOperationHistoryPersistenceServiceTest {
  private OperationHistoryPersistenceService historyPersistenceService;
  private Supplier<String> uniqueIdSupplier;
  private Region<String, OperationState> region;
  private Supplier<Region<String, OperationState>> regionSupplier;

  @Before
  public void init() {
    uniqueIdSupplier = mock(Supplier.class);
    when(uniqueIdSupplier.get()).thenReturn("defaultId");
    region = mock(Region.class);
    historyPersistenceService =
        new RegionOperationHistoryPersistenceService(uniqueIdSupplier, region);
  }

  @Test
  public void createReturnsAnIdFromProvidedSupplier() {
    ClusterManagementOperation<OperationResult> operation = mock(ClusterManagementOperation.class);
    String uniqueId = ";lkajdfa;ldkjfppoiuqe.,.,mnavc098";
    when(uniqueIdSupplier.get()).thenReturn(uniqueId);
    String opId = historyPersistenceService.create(operation);

    assertThat(opId).isSameAs(uniqueId);
    verify(uniqueIdSupplier).get();
  }

  @Test
  public void createStoresOperationStatusInGivenRegion() {
    ClusterManagementOperation<OperationResult> operation = mock(ClusterManagementOperation.class);
    String opId = historyPersistenceService.create(operation);

    ArgumentCaptor<OperationState> capturedOperationInstance = ArgumentCaptor.forClass(
        OperationState.class);
    verify(region).put(eq(opId), capturedOperationInstance.capture());
    OperationState operationInstance = capturedOperationInstance.getValue();

    assertThat(operationInstance).as("operationInstance").isNotNull();

    assertSoftly(softly -> {
      softly.assertThat(operationInstance.getId()).as("id").isEqualTo(opId);
      softly.assertThat(operationInstance.getOperation()).as("operation").isSameAs(operation);
      softly.assertThat(operationInstance.getOperationStart()).as("start").isNotNull();
      softly.assertThat(operationInstance.getOperator()).as("operator").isNull();
      softly.assertThat(operationInstance.getThrowable()).as("throwable").isNull();
      softly.assertThat(operationInstance.getOperationEnd()).as("end").isNull();
      softly.assertThat(operationInstance.getResult()).as("result").isNull();
    });
  }
}
