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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.management.configuration.GroupableConfiguration;
import org.apache.geode.management.configuration.Links;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.runtime.RuntimeInfo;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class GroupableConfigurationTest {

  private GroupableConfiguration<?> element;

  private static ObjectMapper mapper;
  private String json;

  @BeforeClass
  public static void beforeClass() {
    mapper = GeodeJsonMapper.getMapper();
  }

  @Before
  public void before() throws Exception {
    element = new Region();
  }

  @Test
  public void plainRegionConfig() throws Exception {
    assertThat(element.getGroup()).isNull();
    json = mapper.writeValueAsString(element);
    System.out.println(json);
    assertThat(json).doesNotContain("group").doesNotContain("configGroup");
  }

  @Test
  public void setterRegionConfigGroup() throws Exception {
    element.setGroup("group1");
    assertThat(element.getGroup()).isEqualTo("group1");
    json = mapper.writeValueAsString(element);
    System.out.println(json);
    assertThat(json).contains("\"group\":\"group1\"");
  }

  @Test
  public void setGroup() throws Exception {
    element.setGroup("group1");
    assertThat(element.getGroup()).isEqualTo("group1");
    element.setGroup(null);
    assertThat(element.getGroup()).isNull();
    element.setGroup("");
    assertThat(element.getGroup()).isEqualTo("");
    element.setGroup("CLUSTER");
    assertThat(element.getGroup()).isEqualTo("CLUSTER");
    element.setGroup("cluster");
    assertThat(element.getGroup()).isEqualTo("cluster");
    element.setGroup("ClUsTeR");
    assertThat(element.getGroup()).isEqualTo("ClUsTeR");
  }

  private static class TestGroupableConfiguration extends GroupableConfiguration<TestRuntimeInfo> {
    @Override
    public String getId() {
      return null;
    }

    @Override
    public Links getLinks() {
      return null;
    }
  }

  @Test
  public void equality() {
    TestGroupableConfiguration configuration1 = new TestGroupableConfiguration();
    TestGroupableConfiguration configuration2 = new TestGroupableConfiguration();

    configuration1.setGroup("group");
    configuration2.setGroup("group");

    assertThat(configuration1).as("when equal").isEqualTo(configuration2);

    configuration2.setGroup("different-group");
    assertThat(configuration1).as("when not equal").isNotEqualTo(configuration2);
  }

  private static class TestRuntimeInfo extends RuntimeInfo {
  }
}
