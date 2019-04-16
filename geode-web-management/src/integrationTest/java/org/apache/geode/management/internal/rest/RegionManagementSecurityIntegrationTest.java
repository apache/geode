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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = LocatorWithSecurityManagerContextLoader.class)
@WebAppConfiguration
public class RegionManagementSecurityIntegrationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  private LocatorWebContext context;

  private RegionConfig regionConfig;
  private String json;

  @Before
  public void before() throws JsonProcessingException {
    regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    regionConfig.setType(RegionType.REPLICATE);
    ObjectMapper mapper = new ObjectMapper();
    json = mapper.writeValueAsString(regionConfig);
    context = new LocatorWebContext(webApplicationContext);
  }

  @Test
  public void sanityCheck_not_authorized() throws Exception {
    context.perform(post("/v2/regions")
        .with(httpBasic("user", "user"))
        .content(json))
        .andExpect(status().isForbidden())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHORIZED")))
        .andExpect(jsonPath("$.statusMessage",
            is("user not authorized for DATA:MANAGE")));
  }

  @Test
  public void sanityCheckWithNoCredentials() throws Exception {
    context.perform(post("/v2/regions")
        .content(json))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHENTICATED")))
        .andExpect(jsonPath("$.statusMessage",
            is("Full authentication is required to access this resource")));
  }

  @Test
  public void sanityCheckWithWrongCredentials() throws Exception {
    context.perform(post("/v2/regions")
        .with(httpBasic("user", "wrong_password"))
        .content(json))
        .andExpect(status().isUnauthorized())
        .andExpect(jsonPath("$.statusCode", is("UNAUTHENTICATED")))
        .andExpect(jsonPath("$.statusMessage",
            is("Authentication error. Please check your credentials.")));
  }

  @Test
  public void sanityCheck_success() throws Exception {
    context.perform(post("/v2/regions")
        .with(httpBasic("dataManage", "dataManage"))
        .content(json))
        .andExpect(status().isInternalServerError())
        .andExpect(jsonPath("$.statusCode", is("ERROR")))
        .andExpect(jsonPath("$.statusMessage",
            is("no members found in cluster to create cache element")));
  }

}
