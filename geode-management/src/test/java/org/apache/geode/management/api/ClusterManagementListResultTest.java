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

package org.apache.geode.management.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.runtime.RuntimeRegionInfo;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class ClusterManagementListResultTest {

  private ClusterManagementListResult<Region, RuntimeRegionInfo> list;
  private ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Before
  public void before() {
    list = new ClusterManagementListResult<>();
  }

  @Test
  public void serialization() throws Exception {
    List<EntityGroupInfo<Region, RuntimeRegionInfo>> groupInfos = new ArrayList<>();
    EntityGroupInfo<Region, RuntimeRegionInfo> group1 = new EntityGroupInfo<>();
    Region group1Region = new Region();
    group1Region.setName("regionA");
    group1Region.setGroup("group1");
    group1Region.setType(RegionType.REPLICATE);
    group1.setConfiguration(group1Region);

    EntityGroupInfo<Region, RuntimeRegionInfo> group2 = new EntityGroupInfo<>();
    Region group2Region = new Region();
    group2Region.setName("regionA");
    group2Region.setGroup("group2");
    group2Region.setType(RegionType.REPLICATE);
    group2.setConfiguration(group2Region);

    EntityGroupInfo<Region, RuntimeRegionInfo> group3 = new EntityGroupInfo<>();
    Region group3Region = new Region();
    group3Region.setName("regionB");
    group3Region.setGroup("group3");
    group3Region.setType(RegionType.REPLICATE);
    group3.setConfiguration(group3Region);

    groupInfos.add(group1);
    groupInfos.add(group2);
    groupInfos.add(group3);

    list.setEntityGroupInfo(groupInfos);

    // make sure it has 3 region definition by groups
    assertThat(list.getEntityGroupInfo()).hasSize(3);
    // make sure it only has 2 regions defined (one region exists in 2 groups)
    assertThat(list.getResult()).hasSize(2);

    String json = mapper.writeValueAsString(list);
    System.out.println(json);

    // make sure each region's link only appears once in the json
    assertThat(json).containsOnlyOnce("\"self\":\"#HREF/management/v1/regions/regionA\"")
        .containsOnlyOnce("\"self\":\"#HREF/management/v1/regions/regionB\"");

    ClusterManagementListResult<?, ?> result =
        mapper.readValue(json, ClusterManagementListResult.class);

    assertThat(result.getEntityGroupInfo()).hasSize(3);
    assertThat(result.getResult()).hasSize(2);
  }
}
