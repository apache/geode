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

package org.apache.geode.cache.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.MemberConfig;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class CacheElementJsonMappingTest {
  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();

  private static MemberConfig member;
  private static RegionConfig region;

  @BeforeClass
  public static void beforeClass() throws Exception {
    member = new MemberConfig();
    member.setId("server");
    member.setPid("123");

    region = new RegionConfig();
    region.setName("test");
  }

  @Test
  public void serializeRegion() throws Exception {
    String json = mapper.writeValueAsString(region);
    System.out.println(json);
    assertThat(json).contains("class").contains("\"name\":\"test\"");

    RegionConfig config = mapper.readValue(json, RegionConfig.class);
    assertThat(config.getName()).isEqualTo(region.getName());
  }

  @Test
  public void serializeMember() throws Exception {
    String json = mapper.writeValueAsString(member);
    System.out.println(json);
    assertThat(json).contains("class").contains("\"id\":\"server\"");

    MemberConfig config = mapper.readValue(json, MemberConfig.class);
    assertThat(config.getId()).isEqualTo(member.getId());
  }

  @Test
  public void serializeResult() throws Exception {
    ClusterManagementResult result = new ClusterManagementResult();
    List<CacheElement> elements = new ArrayList<>();
    elements.add(region);
    elements.add(member);
    result.setResult(elements);

    String json = mapper.writeValueAsString(result);
    System.out.println(json);

    ClusterManagementResult result1 = mapper.readValue(json, ClusterManagementResult.class);
    assertThat(result1.getResult()).hasSize(2);
  }

  @Test
  public void deserializeWithoutTypeInfo() throws Exception {
    String json = "{'name':'test'}";
    RegionConfig config = mapper.readValue(json, RegionConfig.class);
    assertThat(config.getName()).isEqualTo("test");
  }
}
