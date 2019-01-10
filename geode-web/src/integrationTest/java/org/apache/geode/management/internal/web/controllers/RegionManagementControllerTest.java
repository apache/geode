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

package org.apache.geode.management.internal.web.controllers;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.internal.rule.AdminRestStartupRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-mgmt-servlet.xml"})
@WebAppConfiguration
public class RegionManagementControllerTest {

  @ClassRule
  public static ServerStarterRule rule = new ServerStarterRule()
      .withProperty("log-level", "warn")
      .withPDXReadSerialized()
      .withRegion(RegionShortcut.REPLICATE, "customers");

  @Autowired
  private WebApplicationContext webApplicationContext;

  @Rule
  public AdminRestStartupRule adminRest = new AdminRestStartupRule(() -> webApplicationContext,
      () -> rule.getCache().getSecurityService());

  @Test
  @WithMockUser
  public void sanityCheck() throws Exception {
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName("customers");
    regionConfig.setRefid("REPLICATE");

    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(regionConfig);
    adminRest.perform(post("/v2/regions")
        .content(json))
        .andExpect(status().isOk())
        .andExpect(content().string("customers"));
  }

}
