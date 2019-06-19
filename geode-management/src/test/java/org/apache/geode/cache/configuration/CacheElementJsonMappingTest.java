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
import org.apache.geode.management.configuration.RuntimeIndex;
import org.apache.geode.management.configuration.RuntimeMemberConfig;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class CacheElementJsonMappingTest {
  private static ObjectMapper mapper = GeodeJsonMapper.getMapper();

  private static RuntimeMemberConfig member;
  private static RuntimeRegionConfig region;

  @BeforeClass
  public static void beforeClass() {
    member = new RuntimeMemberConfig();
    member.setId("server");
    member.setPid(123);

    region = new RuntimeRegionConfig();
    region.setName("test");
  }

  @Test
  public void serializeBasicRegion() throws Exception {
    String json = mapper.writeValueAsString(region);
    System.out.println(json);
    assertThat(json).contains("class").contains("\"name\":\"test\"");

    RegionConfig config = mapper.readValue(json, RegionConfig.class);
    assertThat(config.getName()).isEqualTo(region.getName());
  }

  @Test
  public void serializeRegion() throws Exception {
    RegionConfig value = new RegionConfig();
    value.setName("test");
    String json = mapper.writeValueAsString(value);
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
    ClusterManagementResult<CacheElement> result = new ClusterManagementResult<>();
    List<CacheElement> elements = new ArrayList<>();
    elements.add(region);
    elements.add(member);
    result.setResult(elements);

    String json = mapper.writeValueAsString(result);
    System.out.println(json);

    ClusterManagementResult<?> result1 =
        mapper.readValue(json, ClusterManagementResult.class);
    assertThat(result1.getResult()).hasSize(2);
    assertThat(result1.getResult().get(0))
        .isInstanceOf(RegionConfig.class);
    assertThat(result1.getResult().get(1))
        .isInstanceOf(MemberConfig.class);
  }

  @Test
  public void deserializeWithoutTypeInfo() throws Exception {
    String json = "{'name':'test'}";
    RegionConfig config = mapper.readValue(json, RegionConfig.class);
    assertThat(config.getName()).isEqualTo("test");
  }

  @Test
  public void getGroup() throws Exception {
    assertThat(region.getGroup()).isNull();
    assertThat(region.getConfigGroup()).isEqualTo("cluster");

    String json = mapper.writeValueAsString(region);
    assertThat(json).doesNotContain("\"group\"");
  }

  @Test
  public void group() throws Exception {
    String json = "{'name':'test','group':'group1'}";
    RegionConfig regionConfig = mapper.readValue(json, RegionConfig.class);
    assertThat(regionConfig.getGroup()).isEqualTo("group1");
  }

  @Test
  public void groups() throws Exception {
    String json = "{'name':'test','groups':['group1','group2']}";
    RuntimeRegionConfig regionConfig = mapper.readValue(json, RuntimeRegionConfig.class);
    assertThat(regionConfig.getGroups()).containsExactlyInAnyOrder("group1", "group2");
  }

  @Test
  public void serializeGroup() throws Exception {
    RegionConfig config = new RegionConfig();
    config.setName("test");
    config.setGroup("group1");
    String json = mapper.writeValueAsString(config);
    System.out.println(json);
    assertThat(json)
        .contains("\"group\":\"group1\"");
  }

  @Test
  public void serializeMultipleGroup() throws Exception {
    RuntimeRegionConfig config = new RuntimeRegionConfig();
    config.setName("test");
    config.getGroups().add("group1");
    config.getGroups().add("group2");
    String json = mapper.writeValueAsString(config);
    System.out.println(json);
    assertThat(json).contains("\"groups\":[\"group1\",\"group2\"]").doesNotContain("\"group\"");
  }

  @Test
  public void serializeRuntimeRegionConfigWithIndex() throws Exception {
    RegionConfig config = new RegionConfig();
    config.setName("region1");
    config.setType(RegionType.REPLICATE);
    config.setGroup("group1");
    RegionConfig.Index index = new RegionConfig.Index();
    index.setName("index1");
    index.setFromClause("/region1 r");
    index.setRegionName("region1");
    index.setExpression("id");
    config.getIndexes().add(index);
    RuntimeRegionConfig runtimeConfig = new RuntimeRegionConfig(config);
    String json = mapper.writeValueAsString(runtimeConfig);
    System.out.println(json);

    runtimeConfig = mapper.readValue(json, RuntimeRegionConfig.class);
    assertThat(runtimeConfig.getGroups()).containsExactly("group1");
    List<RuntimeIndex> runtimeIndexes = runtimeConfig.getRuntimeIndexes(null);
    assertThat(runtimeIndexes).hasSize(1);
    assertThat(runtimeIndexes.get(0).getRegionName()).isEqualTo("region1");
  }

  @Test
  public void serializeGroupCluster() throws Exception {
    RegionConfig config = new RegionConfig();
    config.setName("test");
    config.setGroup("cluster");
    String json = mapper.writeValueAsString(config);
    System.out.println(json);
    assertThat(json).contains("\"group\":\"cluster\"");
  }
}
