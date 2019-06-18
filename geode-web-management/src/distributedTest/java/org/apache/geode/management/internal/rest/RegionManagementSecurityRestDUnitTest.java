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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.management.internal.api.LocatorClusterManagementService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = SecuredLocatorContextLoader.class)
@WebAppConfiguration
public class RegionManagementSecurityRestDUnitTest {

  private static final String REGION = "products";

  @Autowired
  private WebApplicationContext webApplicationContext;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(1);

  private LocatorWebContext context;

  private RegionConfig regionConfig;
  private String json;

  @Before
  public void before() throws JsonProcessingException {
    regionConfig = new RegionConfig();
    regionConfig.setName(REGION);
    regionConfig.setType(RegionType.REPLICATE);
    ObjectMapper mapper = new ObjectMapper();
    json = mapper.writeValueAsString(regionConfig);
    context = new LocatorWebContext(webApplicationContext);

    cluster.setSkipLocalDistributedSystemCleanup(true);
    int locatorPort = context.getLocator().getPort();

    cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withSecurityManager(SimpleSecurityManager.class)
        .withCredential("cluster", "cluster"));
  }

  @After
  public void after() {
    LocatorClusterManagementService client =
        (LocatorClusterManagementService) context.getLocator().getClusterManagementService();

    ClusterManagementResult<RuntimeRegionConfig> result = client.list(new RegionConfig());
    List<RuntimeRegionConfig> regions = result.getResult();

    regions.forEach(r -> client.delete(new RegionConfig(r)));
  }

  @Test
  public void createRegionFails_when_notAuthorized() throws Exception {
    context.perform(post("/v2/regions")
        .with(httpBasic("user", "user"))
        .content(json))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHORIZED")))
        .andExpect(jsonPath("$.statusMessage",
            is("user not authorized for DATA:MANAGE")));
  }

  @Test
  public void createRegionFails_when_noCredentials() throws Exception {
    context.perform(post("/v2/regions")
        .content(json))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHENTICATED")))
        .andExpect(jsonPath("$.statusMessage",
            is("Full authentication is required to access this resource")));
  }

  @Test
  public void createRegionFails_when_wrongCredentials() throws Exception {
    context.perform(post("/v2/regions")
        .with(httpBasic("user", "wrong_password"))
        .content(json))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHENTICATED")))
        .andExpect(jsonPath("$.statusMessage",
            is("Authentication error. Please check your credentials.")));
  }

  @Test
  public void createRegion_succeeds() throws Exception {
    createRegion();
  }

  @Test
  public void deleteRegionFails_when_notAuthorized() throws Exception {
    createRegion();

    context.perform(delete("/v2/regions/" + REGION)
        .with(httpBasic("user", "user"))
        .content(json))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHORIZED")))
        .andExpect(jsonPath("$.statusMessage",
            is("user not authorized for DATA:MANAGE")));
  }

  @Test
  public void deleteRegionFails_when_noCredentials() throws Exception {
    createRegion();

    context.perform(delete("/v2/regions/" + REGION)
        .content(json))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHENTICATED")))
        .andExpect(jsonPath("$.statusMessage",
            is("Full authentication is required to access this resource")));
  }

  @Test
  public void deleteRegionFails_when_wrongCredentials() throws Exception {
    createRegion();

    context.perform(delete("/v2/regions/" + REGION)
        .with(httpBasic("user", "wrong_password"))
        .content(json))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHENTICATED")))
        .andExpect(jsonPath("$.statusMessage",
            is("Authentication error. Please check your credentials.")));
  }

  @Test
  public void deleteRegion_succeeds() throws Exception {
    createRegion();

    context.perform(delete("/v2/regions/" + REGION)
        .with(httpBasic("dataManage", "dataManage"))
        .content(json))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.statusCode", is("OK")))
        .andExpect(jsonPath("$.statusMessage",
            is("Successfully removed config for [cluster]")));
  }

  private void createRegion() throws Exception {
    context.perform(post("/v2/regions")
        .with(httpBasic("dataManage", "dataManage"))
        .content(json))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.statusCode", is("OK")))
        .andExpect(jsonPath("$.statusMessage",
            is("Successfully updated config for cluster")));
  }

}
