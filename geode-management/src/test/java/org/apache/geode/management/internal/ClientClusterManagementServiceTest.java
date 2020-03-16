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

package org.apache.geode.management.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.NotImplementedException;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementListOperationsResult;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementServiceTransport;
import org.apache.geode.management.api.CommandType;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.management.runtime.RebalanceResult;
import org.apache.geode.management.runtime.RuntimeInfo;

public class ClientClusterManagementServiceTest {
  private ClientClusterManagementService service;
  private ClusterManagementServiceTransport serviceTransport;
  private AbstractConfiguration<RuntimeInfo> configuration;
  private ClusterManagementRealizationResult successRealizationResult;
  private ClusterManagementOperationResult<RebalanceOperation, RebalanceResult> successOperationResult;
  private ClusterManagementOperation<OperationResult> operation;

  @SuppressWarnings("unchecked")
  @Before
  public void init() {
    serviceTransport = mock(ClusterManagementServiceTransport.class);
    configuration = mock(AbstractConfiguration.class);
    service = new ClientClusterManagementService(serviceTransport);

    successRealizationResult = mock(ClusterManagementRealizationResult.class);
    when(successRealizationResult.isSuccessful()).thenReturn(true);

    successOperationResult = mock(ClusterManagementOperationResult.class);
    when(successOperationResult.isSuccessful()).thenReturn(true);

    operation = mock(ClusterManagementOperation.class);
  }

  @Test
  public void createCallsSubmitMessageAndReturnsResult() {
    when(serviceTransport.submitMessage(any(), any())).thenReturn(successRealizationResult);
    when(configuration.getCreationCommandType()).thenReturn(CommandType.CREATE);

    ClusterManagementRealizationResult realizationResult = service.create(configuration);

    assertThat(realizationResult).isSameAs(successRealizationResult);
    verify(serviceTransport).submitMessage(same(configuration), same(CommandType.CREATE));
  }

  @Test
  public void deleteCallsSubmitMessageAndReturnsResult() {
    when(serviceTransport.submitMessage(any(), any())).thenReturn(successRealizationResult);

    ClusterManagementRealizationResult realizationResult = service.delete(configuration);

    assertThat(realizationResult).isSameAs(successRealizationResult);
    verify(serviceTransport).submitMessage(same(configuration), same(CommandType.DELETE));
  }

  @Test
  public void updateNotImplemented() {
    assertThatThrownBy(() -> service.update(null))
        .isInstanceOf(NotImplementedException.class)
        .hasMessageContaining("Not Implemented");
  }

  @Test
  public void listCallsSubmitMessageAndReturnsResult() {
    @SuppressWarnings("unchecked")
    ClusterManagementListResult<AbstractConfiguration<RuntimeInfo>, RuntimeInfo> successListResult =
        mock(ClusterManagementListResult.class);
    when(successListResult.isSuccessful()).thenReturn(true);
    when(serviceTransport.submitMessageForList(any())).thenReturn(successListResult);

    ClusterManagementListResult<AbstractConfiguration<RuntimeInfo>, RuntimeInfo> listResult =
        service.list(configuration);

    assertThat(listResult).isSameAs(successListResult);
    verify(serviceTransport).submitMessageForList(same(configuration));
  }

  @Test
  public void getCallsSubmitMessageAndReturnsResult() {
    @SuppressWarnings("unchecked")
    ClusterManagementGetResult<AbstractConfiguration<RuntimeInfo>, RuntimeInfo> successGetResult =
        mock(ClusterManagementGetResult.class);
    when(successGetResult.isSuccessful()).thenReturn(true);
    when(serviceTransport.submitMessageForGet(any())).thenReturn(successGetResult);

    ClusterManagementGetResult<AbstractConfiguration<RuntimeInfo>, RuntimeInfo> getResult =
        service.get(configuration);

    assertThat(getResult).isSameAs(successGetResult);
    verify(serviceTransport).submitMessageForGet(same(configuration));
  }

  @Test
  public void startCallsSubmitMessageAndReturnsResult() {
    RebalanceOperation rebalanceOperation = new RebalanceOperation();
    doReturn(successOperationResult).when(serviceTransport)
        .submitMessageForStart(any(RebalanceOperation.class));

    ClusterManagementOperationResult<RebalanceOperation, RebalanceResult> operationResult =
        service.start(rebalanceOperation);

    assertThat(operationResult).isSameAs(successOperationResult);
  }

  @Test
  public void getOperationCallsSubmitMessageAndReturnsResult() {
    String opId = "opId";
    RebalanceOperation opType = new RebalanceOperation();
    doReturn(successOperationResult).when(serviceTransport)
        .submitMessageForGetOperation(same(opType), same(opId));

    ClusterManagementOperationResult<RebalanceOperation, RebalanceResult> operationResult =
        service.get(opType, opId);

    assertThat(operationResult).isSameAs(successOperationResult);
    verify(serviceTransport).submitMessageForGetOperation(same(opType), same(opId));
  }

  @Test
  public void getOperationCallsSubmitMessageAndReturnsFuture() {
    String opId = "opId";
    RebalanceOperation opType = new RebalanceOperation();
    doReturn(successOperationResult).when(serviceTransport)
        .submitMessageForGetOperation(same(opType), same(opId));

    CompletableFuture<ClusterManagementOperationResult<RebalanceOperation, RebalanceResult>> future =
        service.getFuture(opType, opId);

    await().untilAsserted(
        () -> verify(serviceTransport).submitMessageForGetOperation(same(opType), same(opId)));
    assertThat(future.isDone()).isFalse();

    future.cancel(true);
  }

  @Test
  public void getOperationCallsSubmitMessageAndReturnsFutureThatCompletes() throws Exception {
    String opId = "opId";
    RebalanceOperation opType = new RebalanceOperation();
    doReturn(successOperationResult).when(serviceTransport)
        .submitMessageForGetOperation(same(opType), same(opId));

    CompletableFuture<ClusterManagementOperationResult<RebalanceOperation, RebalanceResult>> future =
        service.getFuture(opType, opId);

    await().untilAsserted(
        () -> verify(serviceTransport).submitMessageForGetOperation(same(opType), same(opId)));
    assertThat(future.isDone()).isFalse();

    when(successOperationResult.getOperationEnd()).thenReturn(new Date());
    await().untilAsserted(future::isDone);

    assertThat(future.get()).isSameAs(successOperationResult);
  }

  @Test
  public void listOperationCallsSubmitMessageAndReturnsResult() {
    @SuppressWarnings("unchecked")
    ClusterManagementListOperationsResult<ClusterManagementOperation<OperationResult>, OperationResult> successListOperationsResult =
        mock(ClusterManagementListOperationsResult.class);
    when(successListOperationsResult.isSuccessful()).thenReturn(true);
    doReturn(successListOperationsResult).when(serviceTransport)
        .submitMessageForListOperation(any());

    ClusterManagementListOperationsResult<ClusterManagementOperation<OperationResult>, OperationResult> operationResult =
        service.list(operation);

    assertThat(operationResult).isSameAs(successListOperationsResult);
    verify(serviceTransport).submitMessageForListOperation(same(operation));
  }

  @Test
  public void createWithNullResultThrows() {
    when(serviceTransport.submitMessage(any(), any())).thenReturn(null);

    assertThatThrownBy(() -> service.create(configuration))
        .hasMessageContaining("Unable to parse server response.");
  }

  @Test
  public void createWithFailedResult() {
    ClusterManagementRealizationResult realizationResult =
        mock(ClusterManagementRealizationResult.class);
    when(realizationResult.isSuccessful()).thenReturn(false);
    when(serviceTransport.submitMessage(any(), any())).thenReturn(realizationResult);

    Throwable throwable = catchThrowable(() -> service.create(configuration));

    assertThat(throwable).isInstanceOf(ClusterManagementException.class);
    assertThat(((ClusterManagementException) throwable).getResult()).isSameAs(realizationResult);
  }
}
