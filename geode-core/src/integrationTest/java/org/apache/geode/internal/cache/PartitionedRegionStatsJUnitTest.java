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

import static org.apache.geode.internal.cache.PartitionedRegionStats.bucketClearsId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This test verifies that stats are collected properly for the SingleNode and Single
 * PartitionedRegion
 *
 */
public class PartitionedRegionStatsJUnitTest {
  private static final File DISK_DIR = new File("PRStatsTest");
  Logger logger = null;

  @Before
  public void setUp() {
    logger = LogService.getLogger();
  }

  @After
  public void tearDown() throws IOException {
    PartitionedRegionTestHelper.closeCache();
    FileUtils.deleteDirectory(DISK_DIR);
  }

  private PartitionedRegion createPR(String name, int lmax) {
    PartitionAttributesFactory<Object, Object> paf = new PartitionAttributesFactory<>();
    paf.setLocalMaxMemory(lmax).setRedundantCopies(0).setTotalNumBuckets(13); // set low to
                                                                              // reduce
                                                                              // logging
    Cache cache = PartitionedRegionTestHelper.createCache();
    PartitionedRegion pr;
    try {
      RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
      regionFactory.setPartitionAttributes(paf.create());
      pr = (PartitionedRegion) regionFactory.create(name);
    } catch (RegionExistsException rex) {
      pr = (PartitionedRegion) cache.getRegion(name);
    }
    return pr;
  }

  private PartitionedRegion createPRWithEviction(String name, int lmax,
      boolean diskSync,
      boolean persistent) {
    PartitionAttributesFactory<Object, Object> paf = new PartitionAttributesFactory<>();
    paf.setLocalMaxMemory(lmax).setRedundantCopies(0).setTotalNumBuckets(13); // set low to
                                                                              // reduce
                                                                              // logging
    Cache cache = PartitionedRegionTestHelper.createCache();
    RegionFactory<Object, Object> regionFactory = cache.createRegionFactory();
    regionFactory.setPartitionAttributes(paf.create());
    if (persistent) {
      regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    }
    regionFactory.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    regionFactory.setDiskStoreName("diskstore");
    regionFactory.setDiskSynchronous(diskSync);
    regionFactory.setPartitionAttributes(paf.create());
    PartitionedRegion pr;

    assertThat(DISK_DIR.mkdir()).isTrue();
    cache.createDiskStoreFactory().setDiskDirs(new File[] {DISK_DIR}).create("diskstore");

    try {
      pr = (PartitionedRegion) regionFactory.create(name);

    } catch (RegionExistsException rex) {
      pr = (PartitionedRegion) cache.getRegion(name);
    }
    return pr;
  }

  /**
   * This test verifies that PR statistics are working properly for single/multiple
   * PartitionedRegions on single node.
   *
   */
  @Test
  public void testStats() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPR(regionName + 1, localMaxMemory);
    validateStats(pr);
    pr = createPR(regionName + 2, localMaxMemory);
    validateStats(pr);

    if (logger.isDebugEnabled()) {
      logger.debug("PartitionedRegionStatsJUnitTest -  testStats() Completed successfully ... ");
    }
  }

  /**
   * This method verifies that PR statistics are working properly for a PartitionedRegion.
   * putsCompleted, getsCompleted, createsCompleted, destroysCompleted, containsKeyCompleted,
   * containsValueForKeyCompleted, invalidatesCompleted, totalBucketSize and temporarily commented
   * avgRedundantCopies, maxRedundantCopies, minRedundantCopies are validated in this method.
   */
  private void validateStats(PartitionedRegion pr) {
    Statistics stats = pr.getPrStats().getStats();
    int bucketCount = stats.get("bucketCount").intValue();
    int putsCompleted = stats.get("putsCompleted").intValue();
    int totalBucketSize = stats.get("dataStoreEntryCount").intValue();

    assertEquals(0, bucketCount);
    assertEquals(0, putsCompleted);
    assertEquals(0, totalBucketSize);
    int totalGets = 0;

    final int bucketMax = pr.getTotalNumberOfBuckets();
    for (int i = 0; i < bucketMax + 1; i++) {
      Long val = (long) i;
      try {
        pr.put(val, val);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warn(ex);
      }
    }
    for (int i = 0; i < bucketMax + 1; i++) {
      Long val = (long) i;
      try {
        pr.get(val);
        totalGets++;
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warn(ex);
      }
    }


    bucketCount = stats.get("bucketCount").intValue();
    putsCompleted = stats.get("putsCompleted").intValue();
    totalBucketSize = stats.get("dataStoreEntryCount").intValue();

    assertEquals(bucketMax, bucketCount);
    assertEquals(bucketMax + 1, putsCompleted);
    assertEquals(bucketMax + 1, totalBucketSize);

    pr.destroy((long) bucketMax);

    putsCompleted = stats.get("putsCompleted").intValue();
    totalBucketSize = stats.get("dataStoreEntryCount").intValue();

    assertEquals(bucketMax, bucketCount);
    assertEquals(bucketMax + 1, putsCompleted);
    assertEquals(bucketMax, totalBucketSize);

    for (int i = 200; i < 210; i++) {
      Long key = (long) i;
      String val = "" + i;
      try {
        pr.create(key, val);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warn(ex);
      }
    }
    for (int i = 200; i < 210; i++) {
      Long key = (long) i;
      try {
        pr.get(key);
        totalGets++;
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warn(ex);
      }
    }


    for (int i = 200; i < 210; i++) {
      Long key = (long) i;
      try {
        // noinspection ResultOfMethodCallIgnored
        pr.containsKey(key);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warn(ex);
      }
    }

    for (int i = 200; i < 210; i++) {
      Long key = (long) i;
      try {
        pr.containsValueForKey(key);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warn(ex);
      }
    }

    for (int i = 200; i < 210; i++) {
      Long key = (long) i;
      try {
        pr.invalidate(key);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warn(ex);
      }
    }
    int getsCompleted = stats.get("getsCompleted").intValue();
    int createsCompleted = stats.get("createsCompleted").intValue();
    int containsKeyCompleted = stats.get("containsKeyCompleted").intValue();
    int containsValueForKeyCompleted = stats.get("containsValueForKeyCompleted").intValue();
    int invalidatesCompleted = stats.get("invalidatesCompleted").intValue();
    int destroysCompleted = stats.get("destroysCompleted").intValue();

    assertEquals(totalGets, getsCompleted);
    assertEquals(10, createsCompleted);
    assertEquals(10, containsKeyCompleted);
    assertEquals(10, containsValueForKeyCompleted);
    assertEquals(10, invalidatesCompleted);
    assertEquals(1, destroysCompleted);

    // Redundant copies related statistics
    /*
     * int maxRedundantCopies = stats.get("maxRedundantCopies").intValue(); int minRedundantCopies =
     * stats.get("minRedundantCopies").intValue(); int avgRedundantCopies =
     * stats.get("avgRedundantCopies").intValue();
     *
     * assertIndexDetailsEquals(minRedundantCopies, 2); assertIndexDetailsEquals(maxRedundantCopies,
     * 2); assertIndexDetailsEquals(avgRedundantCopies, 2);
     */
  }

  @Test
  public void testOverflowStatsAsync() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPRWithEviction(regionName + 1, localMaxMemory, false, false);
    validateOverflowStats(pr);
  }

  /**
   * This test verifies that PR statistics are working properly for single/multiple
   * PartitionedRegions on single node.
   *
   */
  @Test
  public void testOverflowStats() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPRWithEviction(regionName + 1, localMaxMemory, true, false);
    validateOverflowStats(pr);
  }

  @Test
  public void testPersistOverflowStatsAsync() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPRWithEviction(regionName + 1, localMaxMemory, false, true);
    validateOverflowStats(pr);
  }

  /**
   * This test verifies that PR statistics are working properly for single/multiple
   * PartitionedRegions on single node.
   *
   */
  @Test
  public void testPersistOverflowStats() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPRWithEviction(regionName + 1, localMaxMemory, true, true);
    validateOverflowStats(pr);
  }

  private void validateOverflowStats(PartitionedRegion pr) {
    Statistics stats = pr.getPrStats().getStats();
    DiskRegionStats diskStats = pr.getDiskRegionStats();

    assertEquals(0, stats.getLong("dataStoreBytesInUse"));
    assertEquals(0, stats.getLong("dataStoreEntryCount"));
    assertEquals(0, diskStats.getNumOverflowBytesOnDisk());
    assertEquals(0, diskStats.getNumEntriesInVM());
    assertEquals(0, diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));


    int numEntries = 0;

    pr.put(0, 0);
    numEntries++;
    pr.getDiskStore().flush();

    long singleEntryMemSize = stats.getLong("dataStoreBytesInUse");
    assertEquals(1, stats.getLong("dataStoreEntryCount"));
    assertEquals(0, diskStats.getNumOverflowBytesOnDisk());
    assertEquals(1, diskStats.getNumEntriesInVM());
    assertEquals(0, diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));

    pr.put(1, 1);
    numEntries++;
    pr.getDiskStore().flush();

    assertEquals(singleEntryMemSize, stats.getLong("dataStoreBytesInUse"));
    assertEquals(2, stats.getLong("dataStoreEntryCount"));
    long entryOverflowSize = diskStats.getNumOverflowBytesOnDisk();
    assertEquals(1, diskStats.getNumEntriesInVM());
    assertEquals(1, diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));

    assertTrue(entryOverflowSize > 0);

    for (; numEntries < pr.getTotalNumberOfBuckets() * 5; numEntries++) {
      pr.put(numEntries, numEntries);
    }
    pr.getDiskStore().flush();

    assertEquals(singleEntryMemSize, stats.getLong("dataStoreBytesInUse"));
    assertEquals(numEntries, stats.getLong("dataStoreEntryCount"));
    assertEquals((numEntries - 1) * entryOverflowSize, diskStats.getNumOverflowBytesOnDisk());
    assertEquals(1, diskStats.getNumEntriesInVM());
    assertEquals((numEntries - 1), diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));


    // Update some entries
    for (int i = 0; i < numEntries / 2; i++) {
      pr.put(i, i * 2);
    }
    pr.getDiskStore().flush();

    assertEquals(singleEntryMemSize, stats.getLong("dataStoreBytesInUse"));
    assertEquals(numEntries, stats.getLong("dataStoreEntryCount"));
    assertEquals((numEntries - 1) * entryOverflowSize, diskStats.getNumOverflowBytesOnDisk());
    assertEquals(1, diskStats.getNumEntriesInVM());
    assertEquals((numEntries - 1), diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));

    // Get some entries to trigger evictions
    for (int i = 0; i < numEntries / 2; i++) {
      pr.get(i);
    }
    pr.getDiskStore().flush();

    assertEquals(singleEntryMemSize, stats.getLong("dataStoreBytesInUse"));
    assertEquals(numEntries, stats.getLong("dataStoreEntryCount"));
    assertEquals((numEntries - 1) * entryOverflowSize, diskStats.getNumOverflowBytesOnDisk());
    assertEquals(1, diskStats.getNumEntriesInVM());
    assertEquals((numEntries - 1), diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));


    // Remove some entries
    for (; numEntries > 100; numEntries--) {
      pr.remove(numEntries);
    }
    pr.getDiskStore().flush();

    assertEquals(singleEntryMemSize, stats.getLong("dataStoreBytesInUse"));
    assertEquals(numEntries, stats.getLong("dataStoreEntryCount"));
    assertEquals((numEntries - 1) * entryOverflowSize, diskStats.getNumOverflowBytesOnDisk());
    assertEquals(1, diskStats.getNumEntriesInVM());
    assertEquals((numEntries - 1), diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));

    // Update the same entry twice
    // noinspection OverwrittenKey
    pr.put(5, 5);
    // noinspection OverwrittenKey
    pr.put(5, 6);
    pr.getDiskStore().flush();

    assertEquals(singleEntryMemSize, stats.getLong("dataStoreBytesInUse"));
    assertEquals(numEntries, stats.getLong("dataStoreEntryCount"));
    assertEquals((numEntries - 1) * entryOverflowSize, diskStats.getNumOverflowBytesOnDisk());
    assertEquals(1, diskStats.getNumEntriesInVM());
    assertEquals((numEntries - 1), diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));

    // Put get put - seems to leave entry in memory?
    pr.put(10, 11);
    pr.get(10);
    pr.put(10, 12);

    pr.getDiskStore().flush();

    int entriesInMem = 1;

    assertEquals(singleEntryMemSize * entriesInMem, stats.getLong("dataStoreBytesInUse"));
    assertEquals(numEntries, stats.getLong("dataStoreEntryCount"));
    assertEquals((numEntries - entriesInMem) * entryOverflowSize,
        diskStats.getNumOverflowBytesOnDisk());
    assertEquals(entriesInMem, diskStats.getNumEntriesInVM());
    assertEquals((numEntries - entriesInMem), diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));

    // Do some random operations

    System.out.println("----Doing random operations");
    Random rand = new Random(12345L);
    for (int i = 0; i < 1000; i++) {
      int key = rand.nextInt(numEntries);
      int op = rand.nextInt(3);
      switch (op) {
        case 0:
          pr.put(key, rand.nextInt());
          break;
        case 1:
          pr.get(key);
          break;
        case 2:
          pr.remove(key);
          break;
      }
    }

    pr.getDiskStore().flush();

    System.out.println("----Done with random operations");

    numEntries = pr.entryCount();

    assertEquals(singleEntryMemSize * entriesInMem, stats.getLong("dataStoreBytesInUse"));
    assertEquals(numEntries, stats.getLong("dataStoreEntryCount"));
    assertEquals((numEntries - entriesInMem) * entryOverflowSize,
        diskStats.getNumOverflowBytesOnDisk());
    assertEquals(entriesInMem, diskStats.getNumEntriesInVM());
    assertEquals((numEntries - entriesInMem), diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));
  }

  private Object getDiskBytes(PartitionedRegion pr) {
    Set<BucketRegion> brs = pr.getDataStore().getAllLocalBucketRegions();

    long bytes = 0;
    for (BucketRegion br : brs) {
      bytes += br.getNumOverflowBytesOnDisk();
    }

    return bytes;
  }

  private long getMemBytes(PartitionedRegion pr) {
    Set<BucketRegion> brs = pr.getDataStore().getAllLocalBucketRegions();

    long bytes = 0;
    for (BucketRegion br : brs) {
      bytes += br.getBytesInMemory();
    }

    return bytes;
  }

  @Test
  public void incBucketClearCountIncrementsClears() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPR(regionName + 1, localMaxMemory);

    final long startTime = pr.getPrStats().startBucketClear();
    pr.getPrStats().endBucketClear(startTime);

    assertThat(pr.getPrStats().getStats().getLong(bucketClearsId)).isEqualTo(1L);
    assertThat(pr.getCachePerfStats().getClearCount()).isEqualTo(0L);
  }

  @Test
  public void bucketClearsWrapsFromMaxLongToNegativeValue() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPR(regionName + 1, localMaxMemory);
    PartitionedRegionStats partitionedRegionStats = pr.getPrStats();
    partitionedRegionStats.getStats().incLong(bucketClearsId, Long.MAX_VALUE);

    final long startTime = 1L;
    partitionedRegionStats.endBucketClear(startTime);
    assertThat(partitionedRegionStats.getBucketClearCount()).isNegative();
  }

  @Test
  public void testPartitionedRegionClearStats() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPR(regionName + 1, localMaxMemory);

    final int bucketMax = pr.getTotalNumberOfBuckets();
    for (long i = 0L; i < 10000; i++) {
      try {
        pr.put(i, i);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warn(ex);
      }
    }

    assertThat(pr.getPrStats().getTotalBucketCount()).isEqualTo(bucketMax);
    assertThat(pr.size()).isEqualTo(10000);
    pr.clear();
    assertThat(pr.size()).isEqualTo(0);
    assertThat(pr.getPrStats().getStats().getLong(bucketClearsId)).isEqualTo(bucketMax);
  }

  @Test
  public void testBasicPartitionedRegionClearTimeStat() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPR(regionName + 1, localMaxMemory);
    assertThat(pr.getPrStats().getBucketClearTime()).isEqualTo(0L);

    long startTime = pr.getPrStats().startBucketClear();
    startTime -= 137L;
    pr.getPrStats().endBucketClear(startTime);
    assertThat(pr.getPrStats().getBucketClearTime()).isGreaterThanOrEqualTo(137L);
  }

  @Test
  public void testFullPartitionedRegionClearTimeStat() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPR(regionName + 1, localMaxMemory);

    for (long i = 0L; i < 10000; i++) {
      try {
        pr.put(i, i);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warn(ex);
      }
    }

    assertThat(pr.size()).isEqualTo(10000);
    assertThat(pr.getPrStats().getBucketClearCount()).isEqualTo(0L);

    assertThat(pr.getPrStats().getBucketClearTime()).isEqualTo(0L);
    pr.clear();
    assertThat(pr.getPrStats().getBucketClearCount()).isGreaterThan(0L);

    assertThat(pr.getPrStats().getBucketClearTime()).isGreaterThan(0L);
  }

  @Test
  public void testBasicPartitionedRegionClearsInProgressStat() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPR(regionName + 1, localMaxMemory);
    assertThat(pr.getPrStats().getBucketClearsInProgress()).isEqualTo(0L);

    final long startTime = pr.getPrStats().startBucketClear();
    assertThat(pr.getPrStats().getBucketClearsInProgress()).isEqualTo(1L);
    pr.getPrStats().endBucketClear(startTime);
    assertThat(pr.getPrStats().getBucketClearsInProgress()).isEqualTo(0L);
  }

  @Test
  public void testFullPartitionedRegionClearsInProgressStat() {
    String regionName = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = spy(createPR(regionName + 1, localMaxMemory));
    for (long i = 0L; i < 100; i++) {
      try {
        pr.put(i, i);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warn(ex);
      }
    }
    PartitionedRegionStats partitionedRegionStats = spy(pr.getPrStats());
    when(pr.getPrStats()).thenReturn(partitionedRegionStats);

    BucketRegion actualBucketRegion = pr.getBucketRegion(0L);
    assertThat((Object) actualBucketRegion).isNotNull();
    InternalRegionArguments arguments = mock(InternalRegionArguments.class);
    when(arguments.getPartitionedRegion()).thenReturn(pr);
    when(arguments.getBucketAdvisor()).thenReturn(actualBucketRegion.getBucketAdvisor());
    when(arguments.getPartitionedRegionBucketRedundancy())
        .thenReturn(actualBucketRegion.getRedundancyLevel());
    when(arguments.isUsedForPartitionedRegionBucket()).thenReturn(true);

    BucketRegion bucketRegion =
        new BucketRegion(pr.getName(), pr.getBucketRegion(0L).getAttributes(), pr.getRoot(),
            PartitionedRegionTestHelper.getCache(), arguments,
            pr.getStatisticsClock());
    bucketRegion = spy(bucketRegion);


    assertThat(pr.size()).isEqualTo(100);
    RegionEventImpl event = new RegionEventImpl(bucketRegion, Operation.REGION_CLEAR, null,
        false, bucketRegion.getMyId(), bucketRegion.generateEventID());
    bucketRegion.basicClear(event);
    assertThat(bucketRegion.getPartitionedRegion().getPrStats().getBucketClearCount())
        .isEqualTo(1L);
    verify(partitionedRegionStats, times(1)).startBucketClear();
    verify(partitionedRegionStats, times(1)).endBucketClear(anyLong());
  }
}
