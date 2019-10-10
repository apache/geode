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
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
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
        .andExpect(jsonPath("$._links").isNotEmpty())
        .andExpect(jsonPath("$._links.length()").value(21))
        .andExpect(
            jsonPath("$._links.swagger").value("http://localhost/management/swagger-ui.html"))
        .andExpect(jsonPath("$._links.docs").value("https://geode.apache.org/docs"))
        .andExpect(jsonPath("$._links.wiki")
            .value("https://cwiki.apache.org/confluence/display/GEODE/Management+REST+API"))
        .andExpect(jsonPath("$._links.['get gateway-receiver']")
            .value("http://localhost/management/experimental/gateways/receivers/{id}"))
        .andExpect(jsonPath("$._links.['create gateway-receiver']")
            .value("http://localhost/management/experimental/gateways/receivers"))
        .andExpect(jsonPath("$._links.['list gateway-receivers']")
            .value("http://localhost/management/experimental/gateways/receivers"))
        .andExpect(jsonPath("$._links.['get member']")
            .value("http://localhost/management/experimental/members/{id}"))
        .andExpect(jsonPath("$._links.['list members']")
            .value("http://localhost/management/experimental/members"))
        .andExpect(jsonPath("$._links.['configure pdx']")
            .value("http://localhost/management/experimental/configurations/pdx"))
        .andExpect(jsonPath("$._links.ping").value("http://localhost/management/experimental/ping"))
        .andExpect(jsonPath("$._links.['start rebalance']")
            .value("http://localhost/management/experimental/operations/rebalances"))
        .andExpect(jsonPath("$._links.['list rebalances']")
            .value("http://localhost/management/experimental/operations/rebalances"))
        .andExpect(jsonPath("$._links.['check rebalance']")
            .value("http://localhost/management/experimental/operations/rebalances/{id}"))
        .andExpect(jsonPath("$._links.['get region']")
            .value("http://localhost/management/experimental/regions/{id}"))
        .andExpect(jsonPath("$._links.['create region']")
            .value("http://localhost/management/experimental/regions"))
        .andExpect(jsonPath("$._links.['get index']")
            .value("http://localhost/management/experimental/regions/{regionName}/indexes/{id}"))
        .andExpect(jsonPath("$._links.['list region indexes']")
            .value("http://localhost/management/experimental/regions/{regionName}/indexes"))
        .andExpect(jsonPath("$._links.['list regions']")
            .value("http://localhost/management/experimental/regions"))
        .andExpect(jsonPath("$._links.['delete region']")
            .value("http://localhost/management/experimental/regions/{id}"))
        .andExpect(jsonPath("$._links.['list indexes']")
            .value("http://localhost/management/experimental/indexes"))
        .andExpect(
            jsonPath("$._links.['api root']").value("http://localhost/management/experimental/"));
  }
}
