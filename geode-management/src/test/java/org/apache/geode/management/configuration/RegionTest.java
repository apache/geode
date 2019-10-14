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

package org.apache.geode.management.configuration;

import static org.apache.geode.management.api.Links.URI_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.util.internal.GeodeJsonMapper;

public class RegionTest {

  private Region regionConfig;
  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();

  @Before
  public void before() throws Exception {
    regionConfig = new Region();
  }

  @Test
  public void regionNameSwallowsSlash() {
    regionConfig.setName("/regionA");
    assertThat(regionConfig.getName()).isEqualTo("regionA");
  }

  @Test
  public void subRegionsUnsupported() {
    regionConfig = new Region();
    assertThatThrownBy(() -> regionConfig.setName("/Parent/Child"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> regionConfig.setName("Parent/Child"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void correctJson() throws Exception {
    String json = "{\"name\":\"test\", \"type\":\"REPLICATE\"}";
    regionConfig = mapper.readValue(json, Region.class);
    assertThat(regionConfig.getName()).isEqualTo("test");
    assertThat(regionConfig.getType()).isEqualTo(RegionType.REPLICATE);

    String json2 = mapper.writeValueAsString(regionConfig);
    assertThat(json2).contains("\"type\":\"REPLICATE\"");
    assertThat(json2).contains("\"name\":\"test\"");
  }

  @Test
  public void getUri() {
    regionConfig.setName("regionA");
    assertThat(regionConfig.getLinks().getList()).isEqualTo("/regions");

    assertThat(regionConfig.getLinks().getSelf())
        .isEqualTo(URI_CONTEXT + "/experimental/regions/regionA");
  }
}
