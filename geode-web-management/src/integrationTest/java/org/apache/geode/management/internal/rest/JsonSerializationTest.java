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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = MockLocatorContextLoader.class)
@WebAppConfiguration
public class JsonSerializationTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  // needs to be used together with any LocatorContextLoader
  private LocatorWebContext context;

  private MockLocatorContextLoader mockLocator;
  private ClusterManagementService cms;

  private ArgumentCaptor<RegionConfig> regionConfigCaptor;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
    regionConfigCaptor = ArgumentCaptor.forClass(RegionConfig.class);
    mockLocator = (MockLocatorContextLoader) context.getLocator();
    cms = mockLocator.getClusterManagementService();
  }

  @Test
  public void invalidAttributes() throws Exception {
    String json = "{'name':'test','Group1':'group1','Group2':'group2'}";
    when(cms.create(any())).thenReturn(new ClusterManagementResult<>());
    context.perform(post("/v2/regions").content(json))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.statusCode", Matchers.is("ILLEGAL_ARGUMENT")))
        .andExpect(jsonPath("$.statusMessage",
            Matchers.containsString("JSON parse error: Unrecognized field \"Group1\"")))
        .andExpect(jsonPath("$.statusMessage", Matchers.not(Matchers.containsString("Group2"))));
  }

  @Test
  public void validAttributes() throws Exception {
    String json = "{'name':'test','group':'group1'}";
    when(cms.create(any())).thenReturn(new ClusterManagementResult<>());
    context.perform(post("/v2/regions").content(json))
        .andExpect(status().isCreated());
    verify(cms, atLeastOnce()).create(regionConfigCaptor.capture());
    RegionConfig value = regionConfigCaptor.getValue();
    assertThat(value.getGroup()).isEqualTo("group1");
  }
}
