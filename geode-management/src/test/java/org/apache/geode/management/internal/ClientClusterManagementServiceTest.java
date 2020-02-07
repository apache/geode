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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.lang3.NotImplementedException;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementListOperationsResult;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementServiceTransport;
import org.apache.geode.management.api.CommandType;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.runtime.RuntimeInfo;

public class ClientClusterManagementServiceTest {
  private ClientClusterManagementService service;
  private ClusterManagementServiceTransport serviceTransport;
  private AbstractConfiguration<RuntimeInfo> configuration;
  private ClusterManagementRealizationResult successRealizationResult;
  private ClusterManagementOperationResult successOperationResult;
  private ClusterManagementOperation operation;

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
    when(serviceTransport.submitMessage(any(), any(), any())).thenReturn(successRealizationResult);

    ClusterManagementRealizationResult realizationResult = service.create(configuration);

    assertThat(realizationResult).isSameAs(successRealizationResult);
    verify(serviceTransport).submitMessage(same(configuration), same(CommandType.CREATE), same(
        ClusterManagementRealizationResult.class));
  }

  @Test
  public void deleteCallsSubmitMessageAndReturnsResult() {
    when(serviceTransport.submitMessage(any(), any(), any())).thenReturn(successRealizationResult);

    ClusterManagementRealizationResult realizationResult = service.delete(configuration);

    assertThat(realizationResult).isSameAs(successRealizationResult);
    verify(serviceTransport).submitMessage(same(configuration), same(CommandType.DELETE), same(
        ClusterManagementRealizationResult.class));
  }

  @Test
  public void updateNotImplemented() {
    assertThatThrownBy(() -> service.update(null))
        .isInstanceOf(NotImplementedException.class)
        .hasMessageContaining("Not Implemented");
  }

  @Test
  public void listCallsSubmitMessageAndReturnsResult() {
    ClusterManagementListResult successListResult = mock(ClusterManagementListResult.class);
    when(successListResult.isSuccessful()).thenReturn(true);
    when(serviceTransport.submitMessageForList(any(), any())).thenReturn(successListResult);

    ClusterManagementListResult listResult = service.list(configuration);

    assertThat(listResult).isSameAs(successListResult);
    verify(serviceTransport).submitMessageForList(same(configuration),
        same(ClusterManagementListResult.class));
  }

  @Test
  public void getCallsSubmitMessageAndReturnsResult() {
    ClusterManagementGetResult successGetResult = mock(ClusterManagementGetResult.class);
    when(successGetResult.isSuccessful()).thenReturn(true);
    when(serviceTransport.submitMessageForGet(any(), any())).thenReturn(successGetResult);

    ClusterManagementGetResult getResult = service.get(configuration);

    assertThat(getResult).isSameAs(successGetResult);
    verify(serviceTransport).submitMessageForGet(same(configuration),
        same(ClusterManagementGetResult.class));
  }

  @Test
  public void startCallsSubmitMessageAndReturnsResult() {
    when(serviceTransport.submitMessageForStart(any())).thenReturn(successOperationResult);

    ClusterManagementOperationResult operationResult = service.start(operation);

    assertThat(operationResult).isSameAs(successOperationResult);
    verify(serviceTransport).submitMessageForStart(same(operation));
  }

  @Test
  public void checkStatusCallsSubmitMessageAndReturnsResult() {
    String opId = "opId";
    // when transport then do

    ClusterManagementOperationResult operationResult = service.checkStatus(opId);

    assertThat(operationResult).isNotNull();
    // verify call to transport
  }

  @Test
  public void listOperationCallsSubmitMessageAndReturnsResult() {
    ClusterManagementListOperationsResult successListOperationsResult =
        mock(ClusterManagementListOperationsResult.class);
    when(successListOperationsResult.isSuccessful()).thenReturn(true);
    when(serviceTransport.submitMessageForListOperation(any(), any()))
        .thenReturn(successListOperationsResult);

    ClusterManagementListOperationsResult operationResult = service.list(operation);

    assertThat(operationResult).isSameAs(successListOperationsResult);
    verify(serviceTransport).submitMessageForListOperation(same(operation),
        same(ClusterManagementListOperationsResult.class));
  }
}
