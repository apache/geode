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
  public void startOperation() throws Exception {
    String json = "{}";
    context.perform(post("/v2/operations/rebalance").content(json))
        .andExpect(status().isAccepted())
        .andExpect(
            jsonPath("$.uri", Matchers.containsString("/management/v2/operations/rebalance/")))
        .andExpect(jsonPath("$.statusMessage",
            Matchers.containsString("async operation started")));
  }

  @Test
  public void checkStatus() throws Exception {
    context.perform(get("/v2/operations/rebalance/abc"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.statusCode", Matchers.is("ENTITY_NOT_FOUND")))
        .andExpect(jsonPath("$.statusMessage",
            Matchers.containsString("Operation id = abc not found")));
  }

  @Test
  public void doOperation() {
    RebalanceOperation rebalance = new RebalanceOperation();
    ClusterManagementOperationResult<RebalanceResult> result = client.startOperation(rebalance);
    assertThatThrownBy(result.getResult()::get).hasMessage(
        "java.lang.RuntimeException: ERROR: java.util.concurrent.ExecutionException: java.lang.RuntimeException: rebalance returned info: Distributed system has no regions that can be rebalanced");
  }
}
