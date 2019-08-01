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

package org.apache.geode.management.internal.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.api.ClusterManagementListOperationsResult;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceResult;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
public class RebalanceIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any LocatorContextLoader
  private LocatorWebContext context;

  private ClusterManagementService client;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
    client = ClusterManagementServiceBuilder.buildWithRequestFactory()
        .setRequestFactory(context.getRequestFactory()).build();
  }

  @Test
  public void start() throws Exception {
    String json = "{}";
    context.perform(post("/experimental/operations/rebalance").content(json))
        .andExpect(status().isAccepted())
        .andExpect(
            jsonPath("$.uri",
                Matchers.containsString("/management/experimental/operations/rebalance/")))
        .andExpect(jsonPath("$.statusMessage", Matchers.containsString("Operation started")));
  }

  @Test
  public void checkStatus() throws Exception {
    context.perform(get("/experimental/operations/rebalance/abc"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.statusCode", Matchers.is("ENTITY_NOT_FOUND")))
        .andExpect(
            jsonPath("$.statusMessage", Matchers.containsString("Operation id = abc not found")));
  }

  @Test
  public void list() throws Exception {
    String json = "{}";
    context.perform(post("/experimental/operations/rebalance").content(json));
    context.perform(get("/experimental/operations/rebalance"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result[0].statusCode", Matchers.isOneOf("IN_PROGRESS", "ERROR")))
        .andExpect(jsonPath("$.result[0].uri", Matchers.containsString("rebalance/")))
        .andExpect(jsonPath("$.statusCode", Matchers.is("OK")));
  }

  @Test
  public void doOperation() {
    RebalanceOperation rebalance = new RebalanceOperation();
    ClusterManagementOperationResult<RebalanceResult> result = client.start(rebalance);
    // note: the "java.lang.RuntimeException: " prefix is appended by CompletableFuture itself when
    // you call get() and it had completed exceptionally.
    assertThatThrownBy(result.getFutureResult()::get).hasMessage(
        "java.lang.RuntimeException: ERROR: rebalance returned info: Distributed system has no regions that can be rebalanced");
  }

  @Test
  public void doListOperations() {
    client.start(new RebalanceOperation());
    ClusterManagementListOperationsResult<RebalanceResult> listResult =
        client.list(new RebalanceOperation());
    assertThat(listResult.getResult().size()).isGreaterThanOrEqualTo(1);
    assertThat(listResult.getResult().get(0).getOperationStart()).isNotNull();
    assertThat(listResult.getResult().get(0).getStatusCode().toString()).isIn("IN_PROGRESS",
        "ERROR");
    assertThat(listResult.getResult().get(0).getUri()).contains("rebalance/");
  }
}
