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

import static org.hamcrest.Matchers.is;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.configuration.PdxType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.util.internal.GeodeJsonMapper;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = SecuredLocatorContextLoader.class)
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class ClusterManagementSecurityRestIntegrationTest {

  private static final String REGION = "products";
  @Autowired
  private WebApplicationContext webApplicationContext;

  private LocatorWebContext context;

  private static final List<TestContext> testContexts = new ArrayList<>();
  private static ObjectMapper mapper;

  @BeforeClass
  public static void beforeClass() throws JsonProcessingException {
    mapper = GeodeJsonMapper.getMapper();
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName(REGION);
    regionConfig.setType(RegionType.REPLICATE);

    testContexts.add(new TestContext(post("/v1/regions"), "DATA:MANAGE")
        .setContent(mapper.writeValueAsString(regionConfig)));

    // additional credentials needed to create persistent regions
    regionConfig.setType(RegionType.REPLICATE_PERSISTENT);
    testContexts.add(new TestContext(post("/v1/regions"), "CLUSTER:WRITE:DISK")
        .setCredentials("dataManage", "dataManage")
        .setContent(mapper.writeValueAsString(regionConfig)));

    testContexts.add(new TestContext(get("/v1/regions"), "CLUSTER:READ"));
    testContexts.add(new TestContext(get("/v1/regions/regionA"), "CLUSTER:READ:regionA"));
    testContexts.add(new TestContext(delete("/v1/regions/regionA"), "DATA:MANAGE"));
    testContexts
        .add(new TestContext(get("/v1/regions/regionA/indexes"), "CLUSTER:READ:QUERY"));
    testContexts
        .add(new TestContext(get("/v1/regions/regionA/indexes"), "CLUSTER:READ:QUERY"));
    testContexts
        .add(new TestContext(get("/v1/regions/regionA/indexes/index1"),
            "CLUSTER:READ:QUERY"));
    testContexts
        .add(new TestContext(post("/v1/regions/regionA/indexes/"),
            "CLUSTER:MANAGE:QUERY").setContent(mapper.writeValueAsString(new Index())));
    testContexts
        .add(new TestContext(delete("/v1/regions/regionA/indexes/index1"),
            "CLUSTER:MANAGE:QUERY"));

    testContexts.add(new TestContext(get("/v1/gateways/receivers"), "CLUSTER:READ"));
    testContexts
        .add(new TestContext(get("/v1/gateways/receivers/receiver1"), "CLUSTER:READ"));
    testContexts.add(new TestContext(post("/v1/gateways/receivers"), "CLUSTER:MANAGE")
        .setContent(mapper.writeValueAsString(new GatewayReceiverConfig())));

    testContexts.add(new TestContext(get("/v1/members"), "CLUSTER:READ"));
    testContexts.add(new TestContext(get("/v1/members/server1"), "CLUSTER:READ"));

    testContexts.add(new TestContext(post("/v1/configurations/pdx"), "CLUSTER:MANAGE")
        .setContent(mapper.writeValueAsString(new PdxType())));
    testContexts.add(new TestContext(get("/v1/configurations/pdx"), "CLUSTER:READ"));

    testContexts.add(new TestContext(post("/v1/operations/rebalances"), "DATA:MANAGE")
        .setContent(mapper.writeValueAsString(new RebalanceOperation())));
    testContexts
        .add(new TestContext(get("/v1/operations/rebalances/123"), "DATA:MANAGE"));
  }

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
  }


  @Test
  public void notAuthorized() throws Exception {
    for (TestContext testContext : testContexts) {
      MockHttpServletRequestBuilder requestBuilder = testContext.request
          .with(httpBasic(testContext.username, testContext.password));
      if (testContext.content != null) {
        requestBuilder.content(testContext.content);
      }
      context.perform(requestBuilder)
          .andExpect(status().isForbidden())
          .andExpect(jsonPath("$.statusCode", is("UNAUTHORIZED")))
          .andExpect(jsonPath("$.statusMessage",
              is(sentenceCase(testContext.username) + " not authorized for "
                  + testContext.permission + ".")));
    }
  }

  private static String sentenceCase(String s) {
    return s.substring(0, 1).toUpperCase() + s.substring(1);
  }

  @Test
  public void noCredentials() throws Exception {
    context.perform(post("/v1/regions"))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHENTICATED")))
        .andExpect(jsonPath("$.statusMessage",
            is("Full authentication is required to access this resource.")));
  }

  @Test
  public void wrongCredentials() throws Exception {
    context.perform(post("/v1/regions")
        .with(httpBasic("user", "wrong_password")))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHENTICATED")))
        .andExpect(jsonPath("$.statusMessage",
            is("Invalid username/password.")));
  }

  @Test
  public void successful() throws Exception {
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName(REGION);
    regionConfig.setType(RegionType.REPLICATE);
    context.perform(post("/v1/regions")
        .with(httpBasic("dataManage", "dataManage"))
        .content(mapper.writeValueAsString(regionConfig)))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.statusCode", is("OK")))
        .andExpect(jsonPath("$.statusMessage",
            is("Successfully updated configuration for cluster.")));
    // cleanup in order to pass stressNew
    context.perform(delete("/v1/regions/" + REGION)
        .with(httpBasic("dataManage", "dataManage")))
        .andExpect(status().is2xxSuccessful());
  }

  private static class TestContext {
    MockHttpServletRequestBuilder request;
    String content;
    String permission;
    String username = "user";
    String password = "user";

    public TestContext(MockHttpServletRequestBuilder request, String permission) {
      this.request = request;
      this.permission = permission;
    }

    public TestContext setContent(String content) {
      this.content = content;
      return this;
    }

    public TestContext setCredentials(String username, String password) {
      this.username = username;
      this.password = password;
      return this;
    }
  }
}
