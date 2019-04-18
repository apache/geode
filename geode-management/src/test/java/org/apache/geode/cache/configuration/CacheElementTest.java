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

import org.apache.geode.management.configuration.RuntimeCacheElement;
import org.apache.geode.management.configuration.RuntimeRegionConfig;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class CacheElementTest {

  private CacheElement element;
  private RuntimeCacheElement runtime;

  private static ObjectMapper mapper;
  private String json;

  @BeforeClass
  public static void beforeClass() {
    mapper = GeodeJsonMapper.getMapper();
  }

  @Before
  public void before() throws Exception {
    element = new RegionConfig();
    runtime = new RuntimeRegionConfig();
  }

  @Test
  public void plainRegionConfig() throws Exception {
    assertThat(element.getGroup()).isNull();
    assertThat(element.getConfigGroup()).isEqualTo("cluster");
    json = mapper.writeValueAsString(element);
    System.out.println(json);
    assertThat(json).doesNotContain("group").doesNotContain("groups");
  }

  @Test
  public void plainRuntimeRegionConfig() throws Exception {
    assertThat(runtime.getGroup()).isNull();
    assertThat(runtime.getGroups()).isNotNull().isEmpty();
    json = mapper.writeValueAsString(runtime);
    System.out.println(json);
    assertThat(json).doesNotContain("group").doesNotContain("groups");
  }


  @Test
  public void setterRegionConfigGroup() throws Exception {
    element.setGroup("group1");
    assertThat(element.getGroup()).isEqualTo("group1");
    assertThat(element.getConfigGroup()).isEqualTo("group1");
    json = mapper.writeValueAsString(element);
    System.out.println(json);
    assertThat(json).contains("\"group\":\"group1\"").doesNotContain("groups");
  }

  @Test
  public void setterRuntimeRegionConfig() throws Exception {
    runtime.getGroups().add("group1");
    assertThat(runtime.getGroup()).isEqualTo("group1");
    json = mapper.writeValueAsString(runtime);
    System.out.println(json);
    assertThat(json).contains("\"groups\":[\"group1\"]").doesNotContain("\"group\"");
  }

  @Test
  public void copy() throws Exception {
    RegionConfig config = new RegionConfig();
    config.setName("test");
    runtime = new RuntimeRegionConfig(config);
    assertThat(runtime.getGroup()).isNull();
    assertThat(runtime.getGroups()).hasSize(0);
  }

  @Test
  public void setGroup() throws Exception {
    element.setGroup("group1");
    assertThat(element.getGroup()).isEqualTo("group1");
    element.setGroup("");
    assertThat(element.getGroup()).isNull();
  }
}
