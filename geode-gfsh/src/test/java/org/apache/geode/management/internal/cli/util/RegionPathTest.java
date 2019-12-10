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
package org.apache.geode.management.internal.cli.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.management.internal.util.RegionPath;

public class RegionPathTest {

  @Test
  public void rootName() {
    RegionPath path = new RegionPath("test");
    assertThat(path.getParent()).isNull();
    assertThat(path.getName()).isEqualTo("test");
    assertThat(path.isRoot()).isTrue();
    assertThat(path.getRegionsOnParentPath()).isEmpty();
    assertThat(path.getRootRegionName()).isEqualTo("test");
  }

  @Test
  public void rootNameWithSlash() {
    RegionPath path = new RegionPath("/test");
    assertThat(path.getParent()).isNull();
    assertThat(path.getName()).isEqualTo("test");
    assertThat(path.isRoot()).isTrue();
    assertThat(path.getRegionsOnParentPath()).isEmpty();
    assertThat(path.getRootRegionName()).isEqualTo("test");
  }

  @Test
  public void subRegionName() {
    RegionPath path = new RegionPath("test1/test2");
    assertThat(path.getParent()).isEqualTo("/test1");
    assertThat(path.getName()).isEqualTo("test2");
    assertThat(path.isRoot()).isFalse();
    assertThat(path.getRegionsOnParentPath()).containsExactly("test1");
    assertThat(path.getRootRegionName()).isEqualTo("test1");
  }

  @Test
  public void subRegionNameWithSlash() {
    RegionPath path = new RegionPath("/test1/test2");
    assertThat(path.getParent()).isEqualTo("/test1");
    assertThat(path.getName()).isEqualTo("test2");
    assertThat(path.isRoot()).isFalse();
    assertThat(path.getRegionsOnParentPath()).containsExactly("test1");
    assertThat(path.getRootRegionName()).isEqualTo("test1");
  }

  @Test
  public void subSubRegionName() {
    RegionPath path = new RegionPath("test1/test2/test3");
    assertThat(path.getParent()).isEqualTo("/test1/test2");
    assertThat(path.getName()).isEqualTo("test3");
    assertThat(path.isRoot()).isFalse();
    assertThat(path.getRegionsOnParentPath()).containsExactly("test1", "test2");
    assertThat(path.getRootRegionName()).isEqualTo("test1");
  }

  @Test
  public void subSubRegionNameWithSlash() {
    RegionPath path = new RegionPath("/test1/test2/test3");
    assertThat(path.getParent()).isEqualTo("/test1/test2");
    assertThat(path.getName()).isEqualTo("test3");
    assertThat(path.isRoot()).isFalse();
    assertThat(path.getRegionsOnParentPath()).containsExactly("test1", "test2");
    assertThat(path.getRootRegionName()).isEqualTo("test1");
  }
}
