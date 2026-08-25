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
package org.apache.geode.tools.pulse.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.MediaType.parseMediaType;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.security.Principal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Covers the region-detail error message end to end, from the {@code /pulseUpdate} request the
 * Pulse UI posts through to the JSON it receives back.
 */
@Category({PulseTest.class})
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration("classpath*:WEB-INF/pulse-servlet.xml")
@ActiveProfiles({"pulse.controller.test"})
public class RegionDetailErrorMessageIntegrationTest {

  private static final String PATH_WITH_SPECIAL_CHARACTERS = "/orders<2026>&archive";
  private static final String ENCODED_MESSAGE =
      "Region [/orders&lt;2026&gt;&amp;archive] is not available";

  private static final MediaType JSON_MEDIA_TYPE = parseMediaType(APPLICATION_JSON_VALUE);
  private static final Principal PRINCIPAL = () -> "test-user";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(LENIENT);

  @Autowired
  private WebApplicationContext wac;

  @Autowired
  private Repository repository;

  @Mock
  Cluster cluster;

  private MockMvc mockMvc;

  @Before
  public void setup() {
    when(repository.getCluster()).thenReturn(cluster);
    when(cluster.getServerName()).thenReturn("mock-cluster");
    // The requested path resolves to no region, so the services take the error branch.
    when(cluster.getClusterRegion(anyString())).thenReturn(null);

    mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
  }

  @Test
  public void pulseUpdateEncodesSpecialCharactersForClusterSelectedRegion() throws Exception {
    MvcResult result = mockMvc
        .perform(post("/pulseUpdate")
            .param("pulseData", pulseData("ClusterSelectedRegion", PATH_WITH_SPECIAL_CHARACTERS))
            .principal(PRINCIPAL)
            .accept(JSON_MEDIA_TYPE))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.errorOnRegion")
            .value(ENCODED_MESSAGE))
        .andReturn();

    assertThat(result.getResponse().getContentAsString())
        .contains("/orders&lt;2026&gt;&amp;archive");
  }

  @Test
  public void pulseUpdateEncodesSpecialCharactersForClusterSelectedRegionsMember()
      throws Exception {
    MvcResult result = mockMvc
        .perform(post("/pulseUpdate")
            .param("pulseData",
                pulseData("ClusterSelectedRegionsMember", PATH_WITH_SPECIAL_CHARACTERS))
            .principal(PRINCIPAL)
            .accept(JSON_MEDIA_TYPE))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterSelectedRegionsMember.selectedRegionsMembers.errorOnRegion")
            .value(ENCODED_MESSAGE))
        .andReturn();

    assertThat(result.getResponse().getContentAsString())
        .contains("/orders&lt;2026&gt;&amp;archive");
  }

  @Test
  public void pulseUpdateLeavesOrdinaryRegionPathUnchanged() throws Exception {
    mockMvc
        .perform(post("/pulseUpdate")
            .param("pulseData", pulseData("ClusterSelectedRegion", "/mock-region"))
            .principal(PRINCIPAL)
            .accept(JSON_MEDIA_TYPE))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ClusterSelectedRegion.selectedRegion.errorOnRegion")
            .value("Region [/mock-region] is not available"));
  }

  /** Builds the {@code pulseData} body the Pulse frontend posts for the region-detail page. */
  private static String pulseData(String service, String regionFullPath) {
    ObjectNode parameters = MAPPER.createObjectNode();
    parameters.put("regionFullPath", regionFullPath);
    ObjectNode root = MAPPER.createObjectNode();
    root.set(service, parameters);
    return root.toString();
  }
}
