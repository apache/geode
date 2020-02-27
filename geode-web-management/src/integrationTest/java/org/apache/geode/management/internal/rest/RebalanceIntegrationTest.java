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

import static org.apache.geode.management.configuration.Links.URI_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.StringContains.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.concurrent.CompletableFuture;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.api.ClusterManagementListOperationsResult;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.ClusterManagementServiceTransport;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceResult;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class RebalanceIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any LocatorContextLoader
  private LocatorWebContext context;

  private ClusterManagementService client;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);

    RestTemplate template = new RestTemplate();
    template.setRequestFactory(context.getRequestFactory());
    ClusterManagementServiceTransport transport =
        new RestTemplateClusterManagementServiceTransport(template);
    client = new ClusterManagementServiceBuilder().setTransport(transport).build();
  }

  @Test
  public void start() throws Exception {
    String json = "{}";
    context.perform(post("/v1/operations/rebalances").content(json))
        .andExpect(status().isAccepted())
        .andExpect(content().string(not(containsString("\"class\""))))
        .andExpect(
            jsonPath("$.links.self",
                Matchers.containsString("/v1/operations/rebalances/")))
        .andExpect(jsonPath("$.statusMessage", Matchers.containsString("Operation started")));
  }

  @Test
  public void getStatus() throws Exception {
    String json = "{}";
    CompletableFuture<String> futureUri = new CompletableFuture<>();
    context.perform(post("/v1/operations/rebalances").content(json))
        .andExpect(status().isAccepted())
        .andExpect(new ResponseBodyMatchers().containsObjectAsJson(futureUri))
        .andExpect(jsonPath("$.statusMessage", Matchers.containsString("Operation started")));
    while (true) {
      try {
        context.perform(get(futureUri.get()))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.statusCode", Matchers.is("IN_PROGRESS")));
      } catch (AssertionError t) {
        context.perform(get(futureUri.get()))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.operationResult.statusMessage",
                Matchers.containsString("has no regions")))
            .andExpect(jsonPath("$.operationResult.success", Matchers.is(false)));
        return;
      }
    }
  }

  static class ResponseBodyMatchers {
    ResultMatcher containsObjectAsJson(CompletableFuture<String> futureUri) {
      return mvcResult -> {
        String json = mvcResult.getResponse().getContentAsString();
        String uri =
            json.replaceFirst(".*\"self\":\"[^\"]*" + URI_VERSION, URI_VERSION).replaceFirst("\".*",
                "");
        futureUri.complete(uri);
      };
    }
  }

  @Test
  public void checkStatusOperationDoesNotExist() throws Exception {
    context.perform(get("/v1/operations/rebalances/abc"))
        .andExpect(status().isNotFound())
        .andExpect(content().string(not(containsString("\"class\""))))
        .andExpect(jsonPath("$.statusCode", Matchers.is("ENTITY_NOT_FOUND")))
        .andExpect(
            jsonPath("$.statusMessage",
                Matchers.containsString("Operation 'abc' does not exist.")));
  }

  @Test
  public void list() throws Exception {
    String json = "{}";
    context.perform(post("/v1/operations/rebalances").content(json));
    context.perform(get("/v1/operations/rebalances"))
        .andExpect(status().isOk())
        .andExpect(content().string(not(containsString("\"class\""))))
        .andExpect(
            jsonPath("$.result[0].statusCode", Matchers.isOneOf("IN_PROGRESS", "ERROR", "OK")))
        .andExpect(jsonPath("$.result[0].links.self", Matchers.containsString("rebalances/")))
        .andExpect(jsonPath("$.statusCode", Matchers.is("OK")));
  }

  @Test
  public void doOperation() throws Exception {
    RebalanceOperation rebalance = new RebalanceOperation();
    ClusterManagementOperationResult<RebalanceOperation, RebalanceResult> result =
        client.start(rebalance);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusMessage())
        .isEqualTo("Operation started.  Use the URI to check its status.");
  }

  @Test
  public void doListOperations() {
    client.start(new RebalanceOperation());
    ClusterManagementListOperationsResult<RebalanceOperation, RebalanceResult> listResult =
        client.list(new RebalanceOperation());
    assertThat(listResult.getResult().size()).isGreaterThanOrEqualTo(1);
    assertThat(listResult.getResult().get(0).getOperationStart()).isNotNull();
    assertThat(listResult.getResult().get(0).getStatusCode().toString()).isIn("IN_PROGRESS",
        "ERROR", "OK");
  }
}
