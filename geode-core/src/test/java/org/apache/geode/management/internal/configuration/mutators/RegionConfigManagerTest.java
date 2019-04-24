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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.configuration.RuntimeRegionConfig;

public class RegionConfigManagerTest {
  @Test
  public void managerCountIsMinusOneIfMbeanNotReady() throws Exception {
    RegionConfig filter = new RegionConfig();
    CacheConfig existing = mock(CacheConfig.class);
    List<RegionConfig> staticRegionConfigs = new ArrayList<>();
    RegionConfig region1 = new RegionConfig();
    region1.setName("region1");
    staticRegionConfigs.add(region1);
    when(existing.getRegions()).thenReturn(staticRegionConfigs);

    InternalCache cache = mock(InternalCache.class);
    RegionConfigManager manager = spy(new RegionConfigManager(cache));
    ManagementService managementService = mock(ManagementService.class);
    doReturn(managementService).when(manager).getManagementService();

    List<RuntimeRegionConfig> result = manager.list(filter, existing);
    assertThat(result.get(0).getEntryCount()).isEqualTo(-1);
  }
}
