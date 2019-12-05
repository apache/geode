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

import static org.apache.geode.test.junit.assertions.ClusterManagementRealizationResultAssert.assertManagementResult;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.Region.Expiration;
import org.apache.geode.management.configuration.Region.ExpirationAction;
import org.apache.geode.management.configuration.Region.ExpirationType;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.util.internal.GeodeJsonMapper;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class RegionManagementIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any BaseLocatorContextLoader
  private LocatorWebContext context;

  private ClusterManagementService client;

  private Index index;
  private Region region;
  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
    client = ClusterManagementServiceBuilder.buildWithRequestFactory()
        .setRequestFactory(context.getRequestFactory()).build();
    region = new Region();
    index = new Index();
  }

  @Test
  @WithMockUser
  public void sanityCheck() {
    region.setName("customers");
    region.setType(RegionType.PARTITION);
    region.setDiskStoreName("diskStore");
    region.setKeyConstraint("keyConstraint");
    region.setValueConstraint("valueConstraint");
    region.setRedundantCopies(1);
    Expiration expiration = new Expiration();
    expiration.setType(ExpirationType.ENTRY_IDLE_TIME);
    expiration.setAction(ExpirationAction.DESTROY);
    expiration.setTimeInSeconds(1);
    region.setExpirations(Collections.singletonList(expiration));

    assertManagementResult(client.create(region))
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);
    assertManagementResult(client.delete(region))
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);
  }

  @Test
  public void invalidGroup() {
    region.setName("customers");
    region.setGroup("cluster");

    assertThatThrownBy(() -> client.create(region))
        .hasMessageContaining("ILLEGAL_ARGUMENT: 'cluster' is a reserved group name");
  }

  @Test
  @WithMockUser
  public void ping() throws Exception {
    context.perform(get("/v1/ping"))
        .andExpect(content().string("pong"));
  }

  @Test
  public void createIndexOnNonExistentRegion() throws Exception {
    index.setName("index");
    index.setRegionPath("/regionA");
    index.setExpression("id");
    String postUrl = index.getLinks().getList();
    context.perform(post("/v1" + postUrl).content(mapper.writeValueAsString(index)))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.statusCode", Matchers.is("ENTITY_NOT_FOUND")))
        .andExpect(jsonPath("$.statusMessage",
            Matchers.containsString("Region regionA not found.")));
  }

  @Test
  public void createDuplicateIndex() throws Exception {
    region.setName("customers");
    region.setType(RegionType.PARTITION);
    client.create(region);
    index.setName("index");
    index.setRegionPath("/customers");
    index.setExpression("id");
    client.create(index);

    // trying to create a duplicate index
    index.setName("index");
    index.setRegionPath("/customers");
    index.setExpression("key");
    String postUrl = index.getLinks().getList();
    context.perform(post("/v1" + postUrl).content(mapper.writeValueAsString(index)))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.statusCode", Matchers.is("ENTITY_EXISTS")))
        .andExpect(jsonPath("$.statusMessage",
            Matchers.containsString("Index 'index' already exists in group cluster.")));

    assertManagementResult(client.delete(region))
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);
  }

  @Test
  public void postToRegionEndPoint() throws Exception {
    index.setName("index");
    index.setRegionPath("/customers");
    index.setExpression("id");
    context.perform(post("/v1/regions/products/indexes").content(mapper.writeValueAsString(index)))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.statusCode", Matchers.is("ILLEGAL_ARGUMENT")))
        .andExpect(jsonPath("$.statusMessage",
            Matchers
                .containsString("Region name in path must match Region name in configuration.")));

    index.setRegionPath(null);
    context.perform(post("/v1/regions/products/indexes").content(mapper.writeValueAsString(index)))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.statusCode", Matchers.is("ENTITY_NOT_FOUND")))
        .andExpect(jsonPath("$.statusMessage",
            Matchers.containsString("Region products not found.")));

  }
}
