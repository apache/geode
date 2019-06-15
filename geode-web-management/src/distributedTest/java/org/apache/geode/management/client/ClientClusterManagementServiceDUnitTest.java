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


import static org.apache.geode.test.junit.assertions.ClusterManagementResultAssert.assertManagementResult;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.After;
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

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.management.internal.rest.LocatorWebContext;
import org.apache.geode.management.internal.rest.PlainLocatorContextLoader;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

@RunWith(SpringRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = PlainLocatorContextLoader.class)
@WebAppConfiguration
public class ClientClusterManagementServiceDUnitTest {

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

    client = ClusterManagementServiceBuilder.buildWithRequestFactory()
        .setRequestFactory(webContext.getRequestFactory()).build();
    cluster.startServerVM(1, "group1", webContext.getLocator().getPort());
    cluster.startServerVM(2, "group2", webContext.getLocator().getPort());
    cluster.startServerVM(3, "group1,group2", webContext.getLocator().getPort());
  }

  @After
  public void deleteAllRegions() {
    List<RuntimeRegionConfig> regions = client.list(new RegionConfig())
        .getResult();

    regions.forEach(r -> {
      r.setGroup(null);
      client.delete(r);
    });

    List<RuntimeRegionConfig> moreRegions = client.list(new RegionConfig())
        .getResult();
    assertThat(moreRegions).isEmpty();
  }

  @Test
  @WithMockUser
  public void createAndDeleteRegion() {
    RegionConfig region = new RegionConfig();
    region.setName("customer");
    region.setType(RegionType.REPLICATE);

    ClusterManagementResult<RegionConfig> createResult = client.create(region);
    assertManagementResult(createResult).hasStatusCode(ClusterManagementResult.StatusCode.OK);

    ClusterManagementResult<RegionConfig> deleteResult = client.delete(region);
    assertManagementResult(deleteResult)
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);

    ClusterManagementResult<RuntimeRegionConfig> listResult = client.list(new RegionConfig());
    assertManagementResult(listResult)
        .hasStatusCode(ClusterManagementResult.StatusCode.OK)
        .hasListResult()
        .isEmpty();
  }

  @Test
  @WithMockUser
  public void createAndDeleteRegionOnGroup() {
    RegionConfig region = new RegionConfig();
    region.setName("customer");
    region.setGroup("group1");
    region.setType(RegionType.REPLICATE);

    ClusterManagementResult<RegionConfig> createResult = client.create(region);
    assertManagementResult(createResult).hasStatusCode(ClusterManagementResult.StatusCode.OK);

    region.setGroup(null);
    ClusterManagementResult<RegionConfig> deleteResult = client.delete(region);
    assertManagementResult(deleteResult)
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);

    ClusterManagementResult<RuntimeRegionConfig> listResult = client.list(new RegionConfig());
    assertManagementResult(listResult)
        .hasStatusCode(ClusterManagementResult.StatusCode.OK)
        .hasListResult()
        .isEmpty();
  }

  @Test
  @WithMockUser
  public void createSameRegionOnDifferentGroup() {
    RegionConfig region = new RegionConfig();
    region.setName("test");
    region.setGroup("group1");
    region.setType(RegionType.REPLICATE);

    ClusterManagementResult<RegionConfig> result = client.create(region);
    assertManagementResult(result).isSuccessful().hasMemberStatus().containsOnlyKeys("server-1",
        "server-3");

    // creating the same region on group2 will not be successful because they have a common member
    region.setGroup("group2");
    assertManagementResult(client.create(region)).failed().hasStatusCode(
        ClusterManagementResult.StatusCode.ENTITY_EXISTS)
        .containsStatusMessage("Member(s) server-3 already has this element created");
  }

  @Test
  @WithMockUser
  public void deleteRegionCreatedOnMultipleGroups() {
    RegionConfig region = new RegionConfig();
    region.setName("test");
    region.setGroup("group1");
    region.setType(RegionType.REPLICATE);

    ClusterManagementResult<RegionConfig> result = client.create(region);
    assertManagementResult(result).isSuccessful().hasMemberStatus().containsOnlyKeys("server-1",
        "server-3");

    // creating the same region on group2 will not be successful because they have a common member
    region.setGroup("group2");
    assertManagementResult(client.create(region)).failed().hasStatusCode(
        ClusterManagementResult.StatusCode.ENTITY_EXISTS);

    region.setGroup(null);
    ClusterManagementResult<RegionConfig> deleteResult = client.delete(region);

    assertManagementResult(deleteResult).isSuccessful();

    List<RuntimeRegionConfig> listResult = client.list(new RegionConfig())
        .getResult();

    assertThat(listResult).hasSize(0);
  }


  @Test
  @WithMockUser
  public void deleteRegionCreatedOnPartialGroups() {
    RegionConfig region = new RegionConfig();
    region.setName("test");
    region.setGroup("group1");
    region.setType(RegionType.REPLICATE);

    ClusterManagementResult<RegionConfig> result = client.create(region);
    assertManagementResult(result).isSuccessful().hasMemberStatus().containsOnlyKeys("server-1",
        "server-3");

    assertManagementResult(client.delete(region)).failed()
        .hasStatusCode(ClusterManagementResult.StatusCode.ILLEGAL_ARGUMENT)
        .containsStatusMessage("group is an invalid option when deleting region");
  }

  @Test
  @WithMockUser
  public void deleteUnknownRegion() {
    RegionConfig region = new RegionConfig();
    region.setName("unknown");

    ClusterManagementResult<RegionConfig> result = client.delete(region);
    assertManagementResult(result).failed()
        .hasStatusCode(ClusterManagementResult.StatusCode.ENTITY_NOT_FOUND);
  }

  @Test
  @WithMockUser
  public void deleteRegionOnSpecificGroup() {
    RegionConfig region = new RegionConfig();
    region.setName("region1");
    region.setGroup("group1");
    region.setType(RegionType.REPLICATE);

    ClusterManagementResult<RegionConfig> result = client.create(region);
    assertManagementResult(result).isSuccessful().hasMemberStatus().containsOnlyKeys("server-1",
        "server-3");

    region.setName("region2");
    region.setGroup("group2");
    result = client.create(region);
    assertManagementResult(result).isSuccessful().hasMemberStatus().containsOnlyKeys("server-2",
        "server-3");

    region.setName("region2");
    region.setGroup(null);
    result = client.delete(region);
    assertManagementResult(result)
        .hasStatusCode(ClusterManagementResult.StatusCode.OK);

    ClusterManagementResult<RuntimeRegionConfig> listResult = client.list(new RegionConfig());
    assertManagementResult(listResult)
        .hasStatusCode(ClusterManagementResult.StatusCode.OK)
        .hasListResult()
        .extracting(RegionConfig::getId)
        .containsExactly("region1");
  }

  @Test
  @WithMockUser
  public void sanityCheck() {
    assertThat(client.isConnected()).isTrue();
  }
}
