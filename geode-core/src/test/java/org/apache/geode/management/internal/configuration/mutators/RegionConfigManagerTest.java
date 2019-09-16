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

package org.apache.geode.management.internal.configuration.mutators;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;

public class RegionConfigManagerTest {

  private RegionConfigManager manager;
  private Region config1, config2;

  @Before
  public void before() throws Exception {
    manager = spy(new RegionConfigManager());
    config1 = new Region();
    config1.setName("test");
    config2 = new Region();
    config2.setName("test");
  }

  @Test
  public void compatibleWithItself() throws Exception {
    config1.setType(RegionType.REPLICATE);
    manager.checkCompatibility(config1, "group", config1);
  }

  @Test
  public void compatibleWithSameType() throws Exception {
    config1.setType(RegionType.PARTITION_PERSISTENT);
    config2.setType(RegionType.PARTITION_PERSISTENT);
    manager.checkCompatibility(config1, "group", config2);
  }

  @Test
  public void compatibleWithSamePROXYType() throws Exception {
    config1.setType(RegionType.PARTITION_PROXY);
    config2.setType(RegionType.PARTITION_PROXY);
    manager.checkCompatibility(config1, "group", config2);
  }

  @Test
  public void compatibleWithPROXYType() throws Exception {
    config1.setType(RegionType.PARTITION);
    config2.setType(RegionType.PARTITION_PROXY);
    manager.checkCompatibility(config1, "group", config2);
  }

  @Test
  public void compatibleWithPROXYType2() throws Exception {
    config1.setType(RegionType.PARTITION_PROXY);
    config2.setType(RegionType.PARTITION);
    manager.checkCompatibility(config1, "group", config2);
  }

  @Test
  public void compatibleWithPROXYType3() throws Exception {
    config1.setType(RegionType.PARTITION_PROXY);
    config2.setType(RegionType.PARTITION_PERSISTENT);
    manager.checkCompatibility(config1, "group", config2);
  }

  @Test
  public void incompatible() throws Exception {
    config1.setType(RegionType.PARTITION_PROXY);
    config2.setType(RegionType.REPLICATE);
    assertThatThrownBy(() -> manager.checkCompatibility(config1, "group", config2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Region 'test' of type 'PARTITION_PROXY' is not compatible with group's existing Region 'test' of type 'REPLICATE'");
  }

  @Test
  public void incompatible2() throws Exception {
    config1.setType(RegionType.PARTITION);
    config2.setType(RegionType.REPLICATE);
    assertThatThrownBy(() -> manager.checkCompatibility(config1, "group", config2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Region 'test' of type 'PARTITION' is not compatible with group's existing Region 'test' of type 'REPLICATE'");
  }

  @Test
  public void incompatible3() throws Exception {
    config1.setType(RegionType.PARTITION);
    config2.setType(RegionType.PARTITION_PERSISTENT);
    assertThatThrownBy(() -> manager.checkCompatibility(config1, "group", config2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Region 'test' of type 'PARTITION' is not compatible with group's existing Region 'test' of type 'PARTITION_PERSISTENT'");
  }
}
