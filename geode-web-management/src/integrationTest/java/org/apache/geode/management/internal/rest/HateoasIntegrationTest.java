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
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
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

import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
public class HateoasIntegrationTest {

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
  public void listRegionHateoas() throws Exception {
    Region regionConfig = new Region();
    regionConfig.setName("customers");
    regionConfig.setType(RegionType.REPLICATE);

    try {
      // if run multiple times, this could either be OK or ENTITY_EXISTS
      assertManagementResult(client.create(regionConfig))
          .hasStatusCode(ClusterManagementResult.StatusCode.OK);
    } catch (ClusterManagementException cme) {
      assertThat(cme.getResult().getStatusCode())
          .isEqualTo(ClusterManagementResult.StatusCode.ENTITY_EXISTS);
    }

    context.perform(get("/experimental/regions"))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("\"_links\"")))
        .andExpect(
            jsonPath("$.result[0]._links.self",
                Matchers.endsWith("/experimental/regions/customers")))
        .andExpect(
            jsonPath("$.result[0]._links.indexes",
                Matchers.endsWith("/experimental/regions/customers/indexes")))
        .andExpect(
            jsonPath("$.result[0]._links.diskstores",
                Matchers.endsWith("/experimental/regions/customers/diskstores")))
        .andExpect(
            jsonPath("$.result[0]._links.self",
                Matchers.containsString("http://")));
  }

  @Test
  public void getRegionHateoas() throws Exception {
    Region regionConfig = new Region();
    regionConfig.setName("customers");
    regionConfig.setType(RegionType.REPLICATE);

    try {
      // if run multiple times, this could either be OK or ENTITY_EXISTS
      assertManagementResult(client.create(regionConfig))
          .hasStatusCode(ClusterManagementResult.StatusCode.OK);
    } catch (ClusterManagementException cme) {
      assertThat(cme.getResult().getStatusCode())
          .isEqualTo(ClusterManagementResult.StatusCode.ENTITY_EXISTS);
    }

    context.perform(get("/experimental/regions/customers"))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("\"_links\"")))
        .andExpect(
            jsonPath("$.result._links.self",
                Matchers.endsWith("/experimental/regions/customers")))
        .andExpect(
            jsonPath("$.result._links.indexes",
                Matchers.endsWith("/experimental/regions/customers/indexes")))
        .andExpect(
            jsonPath("$.result._links.diskstores",
                Matchers.endsWith("/experimental/regions/customers/diskstores")))
        .andExpect(
            jsonPath("$._links.['api root']]",
                Matchers.endsWith("/experimental/")));
  }

  @Test
  public void listRootLinks() throws Exception {
    context.perform(get("/experimental/"))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("\"_links\"")))
        .andExpect(
            jsonPath("$._links.swagger",
                Matchers.containsString("swagger-ui.html")))
        .andExpect(
            jsonPath("$._links.docs",
                Matchers.containsString("/docs")))
        .andExpect(
            jsonPath("$._links.wiki",
                Matchers.containsString("cwiki")))
        .andExpect(
            jsonPath("$._links.['list regions']",
                Matchers.containsString("/regions")));
  }
}
