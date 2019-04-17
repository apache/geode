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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

public class CacheElementTest {

  private CacheElement element;

  @Before
  public void before() throws Exception {
    element = new RegionConfig();
  }

  @Test
  public void noGroup() throws Exception {
    assertThat(element.getGroup()).isNull();
    assertThat(element.getConfigGroup()).isEqualTo("cluster");
    assertThat(element.getGroups()).isNotNull().isEmpty();
  }

  @Test
  public void setter() throws Exception {
    element.setGroup("group1");
    assertThat(element.getGroup()).isEqualTo("group1");
    assertThat(element.getConfigGroup()).isEqualTo("group1");
    assertThat(element.getGroups()).containsExactly("group1");
  }

  @Test
  public void setter_cluster() throws Exception {
    element.setGroup("cluster");
    assertThat(element.getGroup()).isEqualTo("cluster");
    assertThat(element.getConfigGroup()).isEqualTo("cluster");
    assertThat(element.getGroups()).isNotNull().isEmpty();
  }

  @Test
  public void multipleGroup() {
    element.getGroupList().add("cluster");
    element.getGroupList().add("group1");
    assertThat(element.getGroups()).containsExactlyInAnyOrder("cluster", "group1");
    assertThatThrownBy(() -> element.getGroup()).isInstanceOf(IllegalArgumentException.class);
  }
}
