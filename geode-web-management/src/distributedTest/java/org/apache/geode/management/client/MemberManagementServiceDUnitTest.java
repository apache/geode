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


import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.stream.Collectors;

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
import org.apache.geode.management.api.ClusterManagementServiceConfig;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.management.configuration.RuntimeCacheElement;
import org.apache.geode.management.internal.ClientClusterManagementService;
import org.apache.geode.management.internal.rest.LocatorLauncherContextLoader;
import org.apache.geode.management.internal.rest.LocatorWebContext;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = LocatorLauncherContextLoader.class)
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

    ClusterManagementServiceConfig config = JavaClientClusterManagementServiceConfig.builder()
        .setRequestFactory(webContext.getRequestFactory())
        .build();
    client = new ClientClusterManagementService(config);

    cluster.startServerVM(1, webContext.getLocator().getPort());
  }

  @Test
  @WithMockUser
  public void listAllMembers() {
    MemberConfig memberConfig = new MemberConfig();
    ClusterManagementResult result = client.list(memberConfig);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);

    List<MemberConfig> members = result.getResult(MemberConfig.class);
    assertThat(members.size()).isEqualTo(2);
    assertThat(members.stream().map(MemberConfig::getId).collect(Collectors.toList()))
        .containsExactlyInAnyOrder("locator-0", "server-1");
    for (MemberConfig oneMember : members) {
      if (oneMember.isLocator()) {
        assertThat(oneMember.getPort())
            .as("port for locator member should not be null").isNotNull().isGreaterThan(0);
        assertThat(oneMember.getCacheServers().size())
            .as("locators should not have cache servers").isEqualTo(0);
      } else {
        assertThat(oneMember.getPort()).as("port for server member should be null").isNull();
        assertThat(oneMember.getCacheServers().size())
            .as("server should have one cache server").isEqualTo(1);
        assertThat(oneMember.getCacheServers().get(0).getPort()).isGreaterThan(0);
        assertThat(oneMember.getCacheServers().get(0).getMaxConnections()).isGreaterThan(0);
        assertThat(oneMember.getCacheServers().get(0).getMaxThreads()).isEqualTo(0);
      }
    }
  }

  @Test
  @WithMockUser
  public void getOneMember() throws Exception {
    MemberConfig config = new MemberConfig();
    config.setId("server-1");
    ClusterManagementResult result = client.list(config);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);

    List<MemberConfig> memberConfig = result.getResult(MemberConfig.class);
    assertThat(memberConfig.size()).isEqualTo(1);
  }

  @Test
  @WithMockUser
  public void getMemberStatus() throws Exception {
    MemberConfig config = new MemberConfig();
    config.setId("locator-0");
    ClusterManagementResult result = client.list(config);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);

    List<MemberConfig> members = result.getResult(MemberConfig.class);
    assertThat(members.size()).isEqualTo(1);

    MemberConfig memberConfig = members.get(0);
    assertThat(memberConfig.getInitialHeap()).isGreaterThan(0);
    assertThat(memberConfig.getMaxHeap()).isGreaterThan(0);
    assertThat(memberConfig.getStatus()).isEqualTo("online");
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
    assertThat(result.getResult(RuntimeCacheElement.class).size()).isEqualTo(0);
  }

  @Test
  @WithMockUser
  public void noMatchWithFilter() throws Exception {
    webContext.perform(get("/v2/members?id=server"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.statusCode", is("OK")));
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
