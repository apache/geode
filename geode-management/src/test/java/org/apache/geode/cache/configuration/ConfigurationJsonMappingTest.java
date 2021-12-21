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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.runtime.MemberInformation;
import org.apache.geode.management.runtime.RuntimeRegionInfo;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class ConfigurationJsonMappingTest {
  private static final ObjectMapper mapper = GeodeJsonMapper.getMapper();

  private static MemberInformation member;
  private static Region region;
  private static RuntimeRegionInfo runtimeRegionInfo;

  @BeforeClass
  public static void beforeClass() {
    member = new MemberInformation();
    member.setId("server");
    member.setProcessId(123);

    region = new Region();
    region.setName("test");

    runtimeRegionInfo = new RuntimeRegionInfo();
    runtimeRegionInfo.setEntryCount(100);
  }

  @Test
  public void serializeBasicRegion() throws Exception {
    String json = mapper.writeValueAsString(region);
    System.out.println(json);
    assertThat(json).contains("class").contains("\"name\":\"test\"");

    Region config = mapper.readValue(json, Region.class);
    assertThat(config.getName()).isEqualTo(region.getName());
  }

  @Test
  public void serializeRegion() throws Exception {
    Region value = new Region();
    value.setName("test");
    String json = mapper.writeValueAsString(value);
    System.out.println(json);
    assertThat(json).contains("class").contains("\"name\":\"test\"");

    Region config = mapper.readValue(json, Region.class);
    assertThat(config.getName()).isEqualTo(region.getName());
  }

  @Test
  public void serializeMember() throws Exception {
    String json = mapper.writeValueAsString(member);
    System.out.println(json);
    assertThat(json).contains("class").contains("\"id\":\"server\"");

    MemberInformation config = mapper.readValue(json, MemberInformation.class);
    assertThat(config.getId()).isEqualTo(member.getId());
  }

  @Test
  public void deserializeWithoutTypeInfo() throws Exception {
    String json = "{'name':'test'}";
    Region config = mapper.readValue(json, Region.class);
    assertThat(config.getName()).isEqualTo("test");
  }

  @Test
  public void getGroup() throws Exception {
    assertThat(region.getGroup()).isNull();

    String json = mapper.writeValueAsString(region);
    assertThat(json).doesNotContain("\"group\"");
  }

  @Test
  public void group() throws Exception {
    String json = "{'name':'test','group':'group1'}";
    Region regionConfig = mapper.readValue(json, Region.class);
    assertThat(regionConfig.getGroup()).isEqualTo("group1");
  }

  @Test
  public void serializeGroup() throws Exception {
    Region config = new Region();
    config.setName("test");
    config.setGroup("group1");
    String json = mapper.writeValueAsString(config);
    System.out.println(json);
    assertThat(json)
        .contains("\"group\":\"group1\"");
  }

  @Test
  public void serializeGroupCluster() throws Exception {
    Region config = new Region();
    config.setName("test");
    config.setGroup("cluster");
    String json = mapper.writeValueAsString(config);
    System.out.println(json);
    assertThat(json).contains("\"group\":\"cluster\"");
  }

  @Test
  public void serializeNullGroup() throws Exception {
    Region config = new Region();
    config.setName("test");
    config.setGroup(null);
    String json = mapper.writeValueAsString(config);
    System.out.println(json);
    assertThat(json).doesNotContain("group");
  }

  @Test
  public void serializeEmptyGroup() throws Exception {
    Region config = new Region();
    config.setName("test");
    config.setGroup("");
    String json = mapper.writeValueAsString(config);
    System.out.println(json);
    assertThat(json).doesNotContain("group");
  }
}
