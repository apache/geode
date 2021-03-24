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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.api.ClusterManagementException;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class HateoasIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any LocatorContextLoader
  private LocatorWebContext context;

  private ClusterManagementService client;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
    client = new ClusterManagementServiceBuilder().setTransport(
        new RestTemplateClusterManagementServiceTransport(
            new RestTemplate(context.getRequestFactory())))
        .build();
  }

  @Test
  public void listRegionHateoas() throws Exception {
    prepRegion();

    context.perform(get("/v1/regions"))
        .andExpect(status().isOk())
        .andExpect(
            jsonPath("$.result[0].links.self",
                Matchers.endsWith("/regions/customers")))
        .andExpect(
            jsonPath("$.result[0].links.indexes",
                Matchers.endsWith("/regions/customers/indexes")))
        .andExpect(
            jsonPath("$.result[0].links.self",
                Matchers.containsString("http://")));
  }

  @Test
  public void getRegionHateoas() throws Exception {
    prepRegion();

    context.perform(get("/v1/regions/customers"))
        .andExpect(status().isOk())
        .andExpect(
            jsonPath("$.result.links.self",
                Matchers.endsWith("/regions/customers")))
        .andExpect(
            jsonPath("$.result.links.indexes",
                Matchers.endsWith("/regions/customers/indexes")));
  }

  private void prepRegion() {
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
  }
}
