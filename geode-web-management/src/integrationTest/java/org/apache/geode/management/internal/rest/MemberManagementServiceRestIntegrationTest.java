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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = LocatorLauncherContextLoader.class)
@WebAppConfiguration
public class MemberManagementServiceRestIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(1);

  private LocatorWebContext webContext;

  @Before
  public void before() {
    webContext = new LocatorWebContext(webApplicationContext);

    cluster.setSkipLocalDistributedSystemCleanup(true);
    int locatorPort = webContext.getLocator().getPort();

    cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withProperty("groups", "group-1,group-2")
        .withLogFile());
  }

  @Test
  public void listLocator() throws Exception {
    webContext.perform(get("/v2/members")
        .param("id", "locator-0"))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.memberStatuses").doesNotExist())
        .andExpect(jsonPath("$.statusCode", is("OK")))
        .andExpect(jsonPath("$.result[*].id", contains("locator-0")))
        .andExpect(jsonPath("$.result[0].port", greaterThan(0)))
        .andExpect(jsonPath("$.result[0].locator", is(true)))
        .andExpect(jsonPath("$.result[0].status", is("online")))
        .andExpect(jsonPath("$.result[0].cacheServers").doesNotExist())
        .andExpect(jsonPath("$.result[0].logFile", endsWith("locator-0.log")))
        .andExpect(jsonPath("$.result[0].workingDirectory", notNullValue()))
        .andExpect(jsonPath("$.result[0].usedHeap", greaterThan(0)));
  }

  @Test
  public void listServer() throws Exception {
    webContext.perform(get("/v2/members")
        .param("id", "server-1"))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.memberStatuses").doesNotExist())
        .andExpect(jsonPath("$.statusCode", is("OK")))
        .andExpect(jsonPath("$.result[*].id", contains("server-1")))
        .andExpect(jsonPath("$.result[0].port").doesNotExist())
        .andExpect(jsonPath("$.result[0].locator", is(false)))
        .andExpect(jsonPath("$.result[0].cacheServers[0].port", greaterThan(0)))
        .andExpect(jsonPath("$.result[0].cacheServers[0].maxConnections", equalTo(800)))
        .andExpect(jsonPath("$.result[0].cacheServers[0].maxThreads", equalTo(0)))
        .andExpect(jsonPath("$.result[0].groups", containsInAnyOrder("group-1", "group-2")))
        .andExpect(jsonPath("$.result[0].logFile", endsWith("server-1.log")))
        .andExpect(jsonPath("$.result[0].workingDirectory", endsWith("vm1")))
        .andExpect(jsonPath("$.result[0].clientConnections", equalTo(0)))
        .andExpect(jsonPath("$.result[0].usedHeap", greaterThan(0)));
  }

  @Test
  public void listAllMembers() throws Exception {
    webContext.perform(get("/v2/members"))
        .andDo(print())
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.memberStatuses").doesNotExist())
        .andExpect(jsonPath("$.statusCode", is("OK")))
        .andExpect(jsonPath("$.result[*].id", containsInAnyOrder("locator-0", "server-1")));
  }
}
