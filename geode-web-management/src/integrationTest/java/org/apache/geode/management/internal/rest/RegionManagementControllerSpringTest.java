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
 *
 */

package org.apache.geode.management.internal.rest;

import static org.apache.geode.management.configuration.Index.INDEXES;
import static org.apache.geode.management.configuration.Links.URI_VERSION;
import static org.apache.geode.management.configuration.Region.REGION_CONFIG_ENDPOINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.management.api.ClusterManagementGetResult;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.Region;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/management-servlet.xml"},
    loader = MockLocatorContextLoader.class)
@WebAppConfiguration
public class RegionManagementControllerSpringTest {

  @Autowired
  private WebApplicationContext webApplicationContext;

  private LocatorWebContext context;
  private MockLocatorContextLoader mockLocator;
  private ClusterManagementService cms;

  @Before
  public void before() {
    context = new LocatorWebContext(webApplicationContext);
    mockLocator = (MockLocatorContextLoader) context.getLocator();
    cms = mockLocator.getClusterManagementService();
  }

  @After
  public void clearMocks() {
    Mockito.clearInvocations(cms);
  }

  @Test
  public void getIndexMappingRecognizesIndexIdWithDot() throws Exception {
    String regionName = "myregion";
    String indexNameWithDot = "index.name";
    String requestPath = URI_VERSION + REGION_CONFIG_ENDPOINT
        + "/" + regionName + INDEXES + "/" + indexNameWithDot;

    when(cms.get(any())).thenReturn(new ClusterManagementGetResult<>());

    context.perform(get(requestPath))
        .andExpect(status().is2xxSuccessful());

    ArgumentCaptor<Index> indexCaptor = ArgumentCaptor.forClass(Index.class);
    verify(cms).get(indexCaptor.capture());
    Index indexPassedToGet = indexCaptor.getValue();

    assertThat(indexPassedToGet.getId())
        .isEqualTo(indexNameWithDot);
  }

  @Test
  public void getRegionMappingRecognizesRegionNameWithDot() throws Exception {
    String regionNameWithDot = "region.name";
    String requestPath = URI_VERSION + REGION_CONFIG_ENDPOINT + "/" + regionNameWithDot;

    when(cms.get(any())).thenReturn(new ClusterManagementGetResult<>());

    context.perform(get(requestPath))
        .andExpect(status().is2xxSuccessful());

    ArgumentCaptor<Region> regionCaptor = ArgumentCaptor.forClass(Region.class);
    verify(cms).get(regionCaptor.capture());
    Region regionPassedToGet = regionCaptor.getValue();

    assertThat(regionPassedToGet.getName())
        .isEqualTo(regionNameWithDot);
  }

  @Test
  public void deleteRegionMappingRecognizesRegionNameWithDot() throws Exception {
    String regionNameWithDot = "region.name";
    String requestPath = URI_VERSION + REGION_CONFIG_ENDPOINT + "/" + regionNameWithDot;

    when(cms.delete(any())).thenReturn(new ClusterManagementRealizationResult());

    context.perform(delete(requestPath))
        .andExpect(status().is2xxSuccessful());

    ArgumentCaptor<Region> regionCaptor = ArgumentCaptor.forClass(Region.class);
    verify(cms).delete(regionCaptor.capture());
    Region regionPassedToGet = regionCaptor.getValue();

    assertThat(regionPassedToGet.getName())
        .isEqualTo(regionNameWithDot);
  }

  @Test
  public void deleteRegionMappingRecognizesIndexNameWithDot() throws Exception {
    String regionName = "regionName";
    String indexNameWithDot = "index.name";
    String requestPath =
        URI_VERSION + REGION_CONFIG_ENDPOINT + "/" + regionName + INDEXES + "/" + indexNameWithDot;

    when(cms.delete(any())).thenReturn(new ClusterManagementRealizationResult());

    context.perform(delete(requestPath))
        .andExpect(status().is2xxSuccessful());

    ArgumentCaptor<Index> indexCaptor = ArgumentCaptor.forClass(Index.class);
    verify(cms).delete(indexCaptor.capture());
    Index indexPassedToGet = indexCaptor.getValue();

    assertThat(indexPassedToGet.getName())
        .isEqualTo(indexNameWithDot);
  }
}
