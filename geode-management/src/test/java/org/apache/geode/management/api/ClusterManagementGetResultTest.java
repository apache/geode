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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.runtime.RuntimeRegionInfo;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class ClusterManagementGetResultTest {

  @Test
  public void serialization() throws JsonProcessingException {
    Region region = createRegion();
    RuntimeRegionInfo runtimeRegionInfo = new RuntimeRegionInfo();

    EntityGroupInfo<Region, RuntimeRegionInfo> entityGroupInfo =
        new EntityGroupInfo<>(region, singletonList(runtimeRegionInfo));

    EntityInfo<Region, RuntimeRegionInfo> configurationinfo =
        new EntityInfo<>("my.element", singletonList(entityGroupInfo));

    ClusterManagementGetResult<Region, RuntimeRegionInfo> original =
        new ClusterManagementGetResult<>(configurationinfo);

    ObjectMapper mapper = GeodeJsonMapper.getMapper();
    String json = mapper.writeValueAsString(original);

    assertThat(json).containsOnlyOnce("result").containsOnlyOnce("statusCode");

    System.out.println(json);

    @SuppressWarnings("unchecked")
    ClusterManagementGetResult<Region, RuntimeRegionInfo> deserialized =
        mapper.readValue(json, ClusterManagementGetResult.class);

    System.out.println(mapper.writeValueAsString(deserialized));

    assertThat(deserialized).isEqualTo(original);
  }

  public static Region createRegion() {
    Region region = new Region();
    region.setName("my.region");
    region.setGroup("my.group");
    return region;
  }
}
