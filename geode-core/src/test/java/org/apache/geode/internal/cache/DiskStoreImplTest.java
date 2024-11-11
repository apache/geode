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
package org.apache.geode.internal.cache;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.SetSystemProperty;

import org.apache.geode.Statistics;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.statistics.StatisticsManager;

class DiskStoreImplTest {

  @Test
  @SetSystemProperty(key = "gemfire.disk.drfHashMapOverflowThreshold", value = "10")
  public void testDrfHashMapOverflowThresholdSystemPropertyIsUsed(@TempDir File dir1,
      @TempDir File dir2) {
    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    DiskStoreAttributes diskStoreAttributes = mock(DiskStoreAttributes.class);
    StatisticsManager statisticsManager = mock(StatisticsManager.class);

    when(internalDistributedSystem.getStatisticsManager()).thenReturn(statisticsManager);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(diskStoreAttributes.getDiskDirs()).thenReturn(
        new File[] {dir1, dir2});
    when(diskStoreAttributes.getDiskDirSizes()).thenReturn(new int[] {1, 1});
    when(diskStoreAttributes.getDiskDirSizesUnit()).thenReturn(DiskDirSizesUnit.MEGABYTES);
    when(statisticsManager.createStatistics(any(), any())).thenReturn(mock(Statistics.class));

    DiskStoreImpl diskStore = new DiskStoreImpl(cache, diskStoreAttributes);

    Assertions.assertThat(diskStore.DRF_HASHMAP_OVERFLOW_THRESHOLD).isEqualTo(10);
  }
}
