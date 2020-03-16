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

package org.apache.geode.management.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.configuration.Links;
import org.apache.geode.management.operation.RebalanceOperation;

public class RestTemplateClusterManagementServiceTransportTest {
  private RestTemplateClusterManagementServiceTransport restServiceTransport;
  private RestTemplate restTemplate;
  private ResponseEntity<?> responseEntity;
  private AbstractConfiguration<?> configuration;
  private Links links;

  @Before
  public void init() {
    restTemplate = mock(RestTemplate.class);
    restServiceTransport = new RestTemplateClusterManagementServiceTransport(restTemplate);

    responseEntity = mock(ResponseEntity.class);
    configuration = mock(AbstractConfiguration.class);
    links = mock(Links.class);
    when(configuration.getLinks()).thenReturn(links);

    ClusterManagementRealizationResult realizationSuccess =
        mock(ClusterManagementRealizationResult.class);
    when(realizationSuccess.isSuccessful()).thenReturn(true);
    ClusterManagementListResult<?, ?> listResult = mock(ClusterManagementListResult.class);
    when(listResult.isSuccessful()).thenReturn(true);
    ClusterManagementGetResult<?, ?> getResult = mock(ClusterManagementGetResult.class);
    when(getResult.isSuccessful()).thenReturn(true);
  }

  @Test
  public void submitMessageCallsRestTemplateExchange() {
    when(links.getList()).thenReturn("/operations");
    doReturn(responseEntity).when(restTemplate).exchange(any(String.class), same(HttpMethod.POST),
        any(HttpEntity.class), same(ClusterManagementRealizationResult.class));

    restServiceTransport.submitMessage(configuration, CommandType.CREATE);

    verify(restTemplate).exchange(eq("/v1/operations"), eq(HttpMethod.POST), any(HttpEntity.class),
        eq(ClusterManagementRealizationResult.class));
  }

  @Test
  public void submitMessageRestCallFails() {
    when(links.getList()).thenReturn("/operations");
    doThrow(new RestClientException("Rest call failed")).when(restTemplate).exchange(
        any(String.class), same(HttpMethod.POST),
        any(HttpEntity.class), same(ClusterManagementRealizationResult.class));

    Throwable throwable =
        catchThrowable(() -> restServiceTransport.submitMessage(configuration, CommandType.CREATE));

    assertThat(throwable).isInstanceOf(RestClientException.class);
    assertThat(throwable.getMessage()).isEqualTo("Rest call failed");
  }

  @Test
  public void submitMessageForListCallsRestTemplateExchange() {
    String opId = "opId";
    String groupId = "groupId";
    when(links.getList()).thenReturn("/operations");
    doReturn(responseEntity).when(restTemplate).exchange(any(String.class), same(HttpMethod.GET),
        any(HttpEntity.class), same(ClusterManagementListResult.class), any(String.class),
        any(String.class));
    when(configuration.getId()).thenReturn(opId);
    when(configuration.getGroup()).thenReturn(groupId);

    restServiceTransport.submitMessageForList(configuration);

    verify(restTemplate).exchange(eq("/v1/operations?id={id}&group={group}"), eq(HttpMethod.GET),
        any(HttpEntity.class),
        eq(ClusterManagementListResult.class), eq(opId), eq(groupId));
  }

  @Test
  public void submitMessageForGetOperationCallsRestTemplateExchange() {
    String opId = "opId";
    doReturn(responseEntity).when(restTemplate).exchange(any(String.class), same(HttpMethod.GET),
        any(HttpEntity.class), same(ClusterManagementOperationResult.class));

    restServiceTransport.submitMessageForGetOperation(new RebalanceOperation(), opId);

    verify(restTemplate).exchange(eq("/v1/operations/rebalances/opId"), eq(HttpMethod.GET),
        any(HttpEntity.class),
        eq(ClusterManagementOperationResult.class));
  }
}
