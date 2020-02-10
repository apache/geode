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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.configuration.Links;

public class RestTemplateClusterManagementServiceTransportTest {
  private RestTemplateClusterManagementServiceTransport restServiceTransport;
  private RestTemplate restTemplate;
  private ResponseEntity responseEntity;
  private AbstractConfiguration configuration;
  private Links links;
  private ClusterManagementRealizationResult realizationSuccess;
  private ClusterManagementRealizationResult realizationFailure;

  @Before
  public void init() {
    restTemplate = mock(RestTemplate.class);
    restServiceTransport = new RestTemplateClusterManagementServiceTransport(restTemplate);

    responseEntity = mock(ResponseEntity.class);
    configuration = mock(AbstractConfiguration.class);
    links = mock(Links.class);
    when(configuration.getLinks()).thenReturn(links);

    realizationSuccess = mock(ClusterManagementRealizationResult.class);
    when(realizationSuccess.isSuccessful()).thenReturn(true);
    realizationFailure = mock(ClusterManagementRealizationResult.class);
    when(realizationFailure.isSuccessful()).thenReturn(false);
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
  public void submitMessageFails() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void submitMessageForGetSucceeds() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void submitMessageThrowsRestException() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void submitMessageForGetFails() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void submitMessageForListSucceeds() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void submitMessageForListFails() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void submitMessageForListOperationSucceeds() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void submitMessageForListOperationFails() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void submitMessageForGetOperationSucceeds() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void submitMessageForGetOperationFails() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void submitMessageForStartOperationSucceeds() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void submitMessageForStartOperationFails() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void reAnimateSucceeds() {
    assertThat(true).as("not implemented").isFalse();
  }

  @Test
  public void reAnimateFails() {
    assertThat(true).as("not implemented").isFalse();
  }
}
