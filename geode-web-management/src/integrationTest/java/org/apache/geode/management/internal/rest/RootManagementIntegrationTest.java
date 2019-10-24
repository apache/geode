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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
public class RootManagementIntegrationTest {
  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any BaseLocatorContextLoader
  private LocatorWebContext context;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
  }

  @Test
  public void getRootLinks() throws Exception {
    context.perform(get("/experimental/"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.links").isNotEmpty())
        .andExpect(jsonPath("$.links.length()").value(21))
        .andExpect(
            jsonPath("$.links.swagger").value("http://localhost/swagger-ui.html"))
        .andExpect(jsonPath("$.links.docs").value("https://geode.apache.org/docs"))
        .andExpect(jsonPath("$.links.wiki")
            .value("https://cwiki.apache.org/confluence/display/pages/viewpage.action?pageId=115511910"))
        .andExpect(jsonPath("$.links.['get gateway-receiver']")
            .value("http://localhost/experimental/gateways/receivers/{id}"))
        .andExpect(jsonPath("$.links.['create gateway-receiver']")
            .value("http://localhost/experimental/gateways/receivers"))
        .andExpect(jsonPath("$.links.['list gateway-receivers']")
            .value("http://localhost/experimental/gateways/receivers"))
        .andExpect(jsonPath("$.links.['get member']")
            .value("http://localhost/experimental/members/{id}"))
        .andExpect(jsonPath("$.links.['list members']")
            .value("http://localhost/experimental/members"))
        .andExpect(jsonPath("$.links.['configure pdx']")
            .value("http://localhost/experimental/configurations/pdx"))
        .andExpect(jsonPath("$.links.ping").value("http://localhost/experimental/ping"))
        .andExpect(jsonPath("$.links.['start rebalance']")
            .value("http://localhost/experimental/operations/rebalances"))
        .andExpect(jsonPath("$.links.['list rebalances']")
            .value("http://localhost/experimental/operations/rebalances"))
        .andExpect(jsonPath("$.links.['check rebalance']")
            .value("http://localhost/experimental/operations/rebalances/{id}"))
        .andExpect(jsonPath("$.links.['get region']")
            .value("http://localhost/experimental/regions/{id}"))
        .andExpect(jsonPath("$.links.['create region']")
            .value("http://localhost/experimental/regions"))
        .andExpect(jsonPath("$.links.['get index']")
            .value("http://localhost/experimental/regions/{regionName}/indexes/{id}"))
        .andExpect(jsonPath("$.links.['list region indexes']")
            .value("http://localhost/experimental/regions/{regionName}/indexes"))
        .andExpect(jsonPath("$.links.['list regions']")
            .value("http://localhost/experimental/regions"))
        .andExpect(jsonPath("$.links.['delete region']")
            .value("http://localhost/experimental/regions/{id}"))
        .andExpect(jsonPath("$.links.['list indexes']")
            .value("http://localhost/experimental/indexes"))
        .andExpect(
            jsonPath("$.links.['api root']").value("http://localhost/experimental/"));
  }
}
