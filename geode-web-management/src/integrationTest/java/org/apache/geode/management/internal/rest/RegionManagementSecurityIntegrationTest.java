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

import static org.hamcrest.Matchers.is;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class RegionManagementSecurityIntegrationTest extends BaseManagementIntegrationTest {

  private RegionConfig regionConfig;
  private String json;
  private MockMvc mockMvc;

  @BeforeClass
  public static void beforeClass() {
    locator = new LocatorStarterRule()
        .withSecurityManager(SimpleTestSecurityManager.class)
        .withAutoStart();
    BaseManagementIntegrationTest.beforeClass();
  }

  @Before
  public void before() throws JsonProcessingException {
    regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    regionConfig.setType("REPLICATE");
    ObjectMapper mapper = new ObjectMapper();
    json = mapper.writeValueAsString(regionConfig);
    mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext)
        .apply(springSecurity())
        .build();
  }

  @Test
  public void sanityCheck_not_authorized() throws Exception {
    mockMvc.perform(post("/v2/regions")
        .with(httpBasic("user", "user"))
        .with(POST_PROCESSOR)
        .content(json))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.persistenceStatus.status", is("FAILURE")))
        .andExpect(jsonPath("$.persistenceStatus.message",
            is("user not authorized for DATA:MANAGE")));
  }

  @Test
  public void sanityCheckWithNoCredentials() throws Exception {
    mockMvc.perform(post("/v2/regions")
        .with(POST_PROCESSOR)
        .content(json))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.persistenceStatus.status", is("FAILURE")))
        .andExpect(jsonPath("$.persistenceStatus.message",
            is("Full authentication is required to access this resource")));
  }

  @Test
  public void sanityCheckWithWrongCredentials() throws Exception {
    mockMvc.perform(post("/v2/regions")
        .with(httpBasic("user", "wrong_password"))
        .with(POST_PROCESSOR)
        .content(json))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.persistenceStatus.status", is("FAILURE")))
        .andExpect(jsonPath("$.persistenceStatus.message",
            is("Authentication error. Please check your credentials.")));
  }

  @Test
  public void sanityCheck_success() throws Exception {
    mockMvc.perform(post("/v2/regions")
        .with(httpBasic("dataManage", "dataManage"))
        .with(POST_PROCESSOR)
        .content(json))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.persistenceStatus.status", is("FAILURE")))
        .andExpect(jsonPath("$.persistenceStatus.message",
            is("no members found to create cache element")));
  }

}
