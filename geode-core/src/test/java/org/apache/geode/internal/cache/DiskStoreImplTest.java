/*
 * Copyright (c) 2023. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package org.apache.geode.internal.cache;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;

import org.apache.geode.Statistics;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.statistics.StatisticsManager;

class DiskStoreImplTest {

  @Test
  @SetSystemProperty(key = "gemfire.disk.drfHashMapOverflowThreshold", value = "10")
  @Disabled
  public void testDrfHashMapOverflowThresholdSystemPropertyIsUsed() {
    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    DiskStoreAttributes diskStoreAttributes = mock(DiskStoreAttributes.class);
    StatisticsManager statisticsManager = mock(StatisticsManager.class);

    when(internalDistributedSystem.getStatisticsManager()).thenReturn(statisticsManager);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(diskStoreAttributes.getDiskDirs()).thenReturn(new File[] {mock(File.class), mock(File.class)});
    when(diskStoreAttributes.getDiskDirSizes()).thenReturn(new int[] {1, 1});
    when(diskStoreAttributes.getDiskDirSizesUnit()).thenReturn(DiskDirSizesUnit.MEGABYTES);
    when(statisticsManager.createStatistics(any(), any())).thenReturn(mock(Statistics.class));

    DiskStoreImpl diskStore = new DiskStoreImpl(cache, diskStoreAttributes);

    Assertions.assertThat(diskStore.DRF_HASHMAP_OVERFLOW_THRESHOLD).isEqualTo(10);
  }
}