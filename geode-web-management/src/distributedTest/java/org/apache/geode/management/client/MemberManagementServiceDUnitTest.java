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

package org.apache.geode.management.client;


import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.internal.rest.LocatorWebContext;
import org.apache.geode.management.internal.rest.PlainLocatorContextLoader;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
public class MemberManagementServiceDUnitTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(1);

  private ClusterManagementService client;
  private LocatorWebContext webContext;

  @Before
  public void before() {
    cluster.setSkipLocalDistributedSystemCleanup(true);
    webContext = new LocatorWebContext(webApplicationContext);
    client = ClusterManagementServiceProvider.getService(webContext.getRequestFactory());
    cluster.startServerVM(0, webContext.getLocator().getPort());
  }

  @Test
  @WithMockUser
  public void listMember() {
    MemberConfig memberConfig = new MemberConfig();
    ClusterManagementResult result = client.list(memberConfig);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getResult().size()).isEqualTo(2);
  }

  @Test
  @WithMockUser
  public void getOneMember() throws Exception {
    MemberConfig config = new MemberConfig();
    config.setId("server-0");
    ClusterManagementResult result = client.list(config);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getResult().size()).isEqualTo(1);
  }

  @Test
  @WithMockUser
  public void noMatchWithJavaAPI() throws Exception {
    MemberConfig config = new MemberConfig();
    // look for a member with a non-existent id
    config.setId("server");
    ClusterManagementResult result = client.list(config);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode())
        .isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getResult().size()).isEqualTo(0);
  }

  @Test
  @WithMockUser
  public void noMatchWithFilter() throws Exception {
    webContext.perform(get("/v2/members?id=server"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.statusCode", is("OK")))
        .andExpect(jsonPath("$.result", iterableWithSize(0)));
  }

  @Test
  @WithMockUser
  public void noMatchWithUriVariable() throws Exception {
    webContext.perform(get("/v2/members/server"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.statusCode", is("ENTITY_NOT_FOUND")))
        .andExpect(jsonPath("$.statusMessage",
            is("Unable to find the member with id = server")));
  }
}
