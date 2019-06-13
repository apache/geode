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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.configuration.RuntimeRegionConfig;

public class RegionConfigManagerTest {

  private RegionConfigManager manager;
  private RegionConfig config1, config2;

  @Before
  public void before() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    manager = spy(new RegionConfigManager(cache));
    config1 = new RegionConfig();
    config1.setName("test");
    config2 = new RegionConfig();
    config2.setName("test");
  }

  @Test
  public void managerCountIsMinusOneIfMbeanNotReady() throws Exception {
    CacheConfig existing = mock(CacheConfig.class);
    List<RegionConfig> staticRegionConfigs = new ArrayList<>();
    config1.setName("region1");
    staticRegionConfigs.add(config1);
    when(existing.getRegions()).thenReturn(staticRegionConfigs);


    ManagementService managementService = mock(ManagementService.class);
    doReturn(managementService).when(manager).getManagementService();
    List<RuntimeRegionConfig> result = manager.list(new RegionConfig(), existing);
    assertThat(result.get(0).getEntryCount()).isEqualTo(-1);
  }

  @Test
  public void compatibleWithItself() throws Exception {
    config1.setType(RegionType.REPLICATE);
    manager.checkCompatibility(config1, "group", config1);
  }

  @Test
  public void compatibleWithSameType() throws Exception {
    config1.setType(RegionType.PARTITION_HEAP_LRU);
    config2.setType(RegionType.PARTITION_HEAP_LRU);
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
            "Region test of type PARTITION_PROXY is not compatible with group's existing Region test of type REPLICATE");
  }

  @Test
  public void incompatible2() throws Exception {
    config1.setType(RegionType.PARTITION);
    config2.setType(RegionType.REPLICATE);
    assertThatThrownBy(() -> manager.checkCompatibility(config1, "group", config2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Region test of type PARTITION is not compatible with group's existing regionConfig Region test of type REPLICATE");
  }

  @Test
  public void incompatible3() throws Exception {
    config1.setType(RegionType.PARTITION);
    config2.setType(RegionType.PARTITION_PERSISTENT);
    assertThatThrownBy(() -> manager.checkCompatibility(config1, "group", config2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Region test of type PARTITION is not compatible with group's existing regionConfig Region test of type PARTITION_PERSISTEN");
  }
}
