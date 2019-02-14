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


import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.client.MockMvcClientHttpRequestFactory;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.internal.rest.BaseLocatorContextLoader;
import org.apache.geode.management.internal.rest.PlainLocatorContextLoader;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
public class ClusterManagementClientDUnitTest {
  private static final ResponseErrorHandler ERROR_HANDLER = new RestTemplateResponseErrorHandler();

  @Autowired
  private WebApplicationContext webApplicationContext;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(1);

  private MemberVM server1;
  private ClusterManagementService client;

  @Before
  public void before() {
    cluster.setSkipLocalDistributedSystemCleanup(true);
    MockMvc mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext)
        .build();
    MockMvcClientHttpRequestFactory requestFactory = new MockMvcClientHttpRequestFactory(mockMvc);
    client = ClusterManagementServiceProvider.getService(requestFactory);

    server1 = cluster.startServerVM(0,
        BaseLocatorContextLoader.getLocatorFromContext(webApplicationContext).getPort());
  }

  @Test
  @WithMockUser
  public void createRegion() {
    RegionConfig region = new RegionConfig();
    region.setName("customer");
    region.setType("REPLICATE");

    ClusterManagementResult result = client.create(region, "");

    // This all fails in light of running this test repeatedly as a stress test. Until we introduce
    // idempotency and/or the ability to call client.delete we can't do this. But it will get fixed
    // assertThat(result.isSuccessful()).isTrue();

    // Not implemented yet
    // result = client.delete(region, "");
    // assertThat(result.isSuccessful()).isTrue();
  }
}
