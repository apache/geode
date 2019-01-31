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

package org.apache.geode.management.internal.rest.security;

import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class RegionManagementIntegrationTest extends BaseManagementIntegrationTest {

  static {
    locator = new LocatorStarterRule().withHttpService();
    locator.startLocator();
  }

  private MockMvc mockMvc;

  @Before
  public void before() {
    mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext)
        .build();
  }

  @Test
  @WithMockUser
  public void sanityCheck() throws Exception {
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    regionConfig.setType("REPLICATE");

    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(regionConfig);

    mockMvc.perform(post("/v2/regions")
        .with(POST_PROCESSOR)
        .content(json))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.persistenceStatus.status", is("FAILURE")))
        .andExpect(jsonPath("$.persistenceStatus.message",
            is("no members found to create cache element")));
  }

  @Test
  @WithMockUser
  public void ping() throws Exception {
    mockMvc.perform(get("/v2/ping")
        .with(POST_PROCESSOR))
        .andExpect(content().string("pong"));
  }
}
