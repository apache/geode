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
/**
 * This test verifies that stats are collected properly for the SingleNode and Single
 * PartitionedRegion
 *
 */
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.cache.PartitionedRegionStats.regionClearLocalDurationId;
import static org.apache.geode.internal.cache.PartitionedRegionStats.regionClearTotalDurationId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.LogWriter;
import org.apache.geode.Statistics;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.RegionExistsException;

public class PartitionedRegionStatsJUnitTest {
  private static final File DISK_DIR = new File("PRStatsTest");
  LogWriter logger = null;

  @Before
  public void setUp() {
    logger = PartitionedRegionTestHelper.getLogger();
  }

  @After
  public void tearDown() throws IOException {
    PartitionedRegionTestHelper.closeCache();
    FileUtils.deleteDirectory(DISK_DIR);
  }

  private PartitionedRegion createPR(String name, int lmax, int redundancy) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(lmax).setRedundantCopies(redundancy).setTotalNumBuckets(13); // set low to
                                                                                       // reduce
                                                                                       // logging
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    Cache cache = PartitionedRegionTestHelper.createCache();
    PartitionedRegion pr = null;
    try {
      pr = (PartitionedRegion) cache.createRegion(name, af.create());
    } catch (RegionExistsException rex) {
      pr = (PartitionedRegion) cache.getRegion(name);
    }
    return pr;
  }

  private PartitionedRegion createPRWithEviction(String name, int lmax, int redundancy,
      int evictionCount, boolean diskSync, boolean persistent) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(lmax).setRedundantCopies(redundancy).setTotalNumBuckets(13); // set low to
                                                                                       // reduce
                                                                                       // logging
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    if (persistent) {
      af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    }
    af.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    af.setDiskStoreName("diskstore");
    af.setDiskSynchronous(diskSync);
    Cache cache = PartitionedRegionTestHelper.createCache();
    DISK_DIR.mkdir();
    cache.createDiskStoreFactory().setDiskDirs(new File[] {DISK_DIR}).create("diskstore");
    PartitionedRegion pr = null;
    try {
      pr = (PartitionedRegion) cache.createRegion(name, af.create());
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
  public void testStats() throws Exception {
    String regionname = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPR(regionname + 1, localMaxMemory, 0);
    validateStats(pr);
    pr = createPR(regionname + 2, localMaxMemory, 0);
    validateStats(pr);

    if (logger.fineEnabled()) {
      logger.fine("PartitionedRegionStatsJUnitTest -  testStats() Completed successfully ... ");
    }
  }

  /**
   * This method verifies that PR statistics are working properly for a PartitionedRegion.
   * putsCompleted, getsCompleted, createsCompleted, destroysCompleted, containsKeyCompleted,
   * containsValueForKeyCompleted, invalidatesCompleted, totalBucketSize and temporarily commented
   * avgRedundantCopies, maxRedundantCopies, minRedundantCopies are validated in this method.
   */
  private void validateStats(PartitionedRegion pr) throws Exception {
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
      Long val = new Long(i);
      try {
        pr.put(val, val);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warning(ex);
      }
    }
    for (int i = 0; i < bucketMax + 1; i++) {
      Long val = new Long(i);
      try {
        pr.get(val);
        totalGets++;
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warning(ex);
      }
    }


    bucketCount = stats.get("bucketCount").intValue();
    putsCompleted = stats.get("putsCompleted").intValue();
    totalBucketSize = stats.get("dataStoreEntryCount").intValue();

    assertEquals(bucketMax, bucketCount);
    assertEquals(bucketMax + 1, putsCompleted);
    assertEquals(bucketMax + 1, totalBucketSize);

    pr.destroy(new Long(bucketMax));

    putsCompleted = stats.get("putsCompleted").intValue();
    totalBucketSize = stats.get("dataStoreEntryCount").intValue();

    assertEquals(bucketMax, bucketCount);
    assertEquals(bucketMax + 1, putsCompleted);
    assertEquals(bucketMax, totalBucketSize);

    for (int i = 200; i < 210; i++) {
      Long key = new Long(i);
      String val = "" + i;
      try {
        pr.create(key, val);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warning(ex);
      }
    }
    for (int i = 200; i < 210; i++) {
      Long key = new Long(i);
      try {
        pr.get(key);
        totalGets++;
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warning(ex);
      }
    }


    for (int i = 200; i < 210; i++) {
      Long key = new Long(i);
      try {
        pr.containsKey(key);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warning(ex);
      }
    }

    for (int i = 200; i < 210; i++) {
      Long key = new Long(i);
      try {
        pr.containsValueForKey(key);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warning(ex);
      }
    }

    for (int i = 200; i < 210; i++) {
      Long key = new Long(i);
      try {
        pr.invalidate(key);
      } catch (PartitionedRegionStorageException ex) {
        this.logger.warning(ex);
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
  public void testOverflowStatsAsync() throws Exception {
    String regionname = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPRWithEviction(regionname + 1, localMaxMemory, 0, 1, false, false);
    validateOverflowStats(pr);
  }

  /**
   * This test verifies that PR statistics are working properly for single/multiple
   * PartitionedRegions on single node.
   *
   */
  @Test
  public void testOverflowStats() throws Exception {
    String regionname = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPRWithEviction(regionname + 1, localMaxMemory, 0, 1, true, false);
    validateOverflowStats(pr);
  }

  @Test
  public void testPersistOverflowStatsAsync() throws Exception {
    String regionname = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPRWithEviction(regionname + 1, localMaxMemory, 0, 1, false, true);
    validateOverflowStats(pr);
  }

  /**
   * This test verifies that PR statistics are working properly for single/multiple
   * PartitionedRegions on single node.
   *
   */
  @Test
  public void testPersistOverflowStats() throws Exception {
    String regionname = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPRWithEviction(regionname + 1, localMaxMemory, 0, 1, true, true);
    validateOverflowStats(pr);
  }

  private void validateOverflowStats(PartitionedRegion pr) throws Exception {
    Statistics stats = pr.getPrStats().getStats();
    DiskRegionStats diskStats = pr.getDiskRegionStats();

    assertEquals(0, stats.getLong("dataStoreBytesInUse"));
    assertEquals(0, stats.getInt("dataStoreEntryCount"));
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
    assertEquals(1, stats.getInt("dataStoreEntryCount"));
    assertEquals(0, diskStats.getNumOverflowBytesOnDisk());
    assertEquals(1, diskStats.getNumEntriesInVM());
    assertEquals(0, diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));

    pr.put(1, 1);
    numEntries++;
    pr.getDiskStore().flush();

    assertEquals(singleEntryMemSize, stats.getLong("dataStoreBytesInUse"));
    assertEquals(2, stats.getInt("dataStoreEntryCount"));
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
    assertEquals(numEntries, stats.getInt("dataStoreEntryCount"));
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
    assertEquals(numEntries, stats.getInt("dataStoreEntryCount"));
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
    assertEquals(numEntries, stats.getInt("dataStoreEntryCount"));
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
    assertEquals(numEntries, stats.getInt("dataStoreEntryCount"));
    assertEquals((numEntries - 1) * entryOverflowSize, diskStats.getNumOverflowBytesOnDisk());
    assertEquals(1, diskStats.getNumEntriesInVM());
    assertEquals((numEntries - 1), diskStats.getNumOverflowOnDisk());
    assertEquals(stats.getLong("dataStoreBytesInUse"), getMemBytes(pr));
    assertEquals(diskStats.getNumOverflowBytesOnDisk(), getDiskBytes(pr));

    // Update the same entry twice
    pr.put(5, 5);
    pr.put(5, 6);
    pr.getDiskStore().flush();

    assertEquals(singleEntryMemSize, stats.getLong("dataStoreBytesInUse"));
    assertEquals(numEntries, stats.getInt("dataStoreEntryCount"));
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
    assertEquals(numEntries, stats.getInt("dataStoreEntryCount"));
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
    assertEquals(numEntries, stats.getInt("dataStoreEntryCount"));
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
    for (Iterator<BucketRegion> itr = brs.iterator(); itr.hasNext();) {
      BucketRegion br = itr.next();
      bytes += br.getNumOverflowBytesOnDisk();
    }

    return bytes;
  }

  private long getMemBytes(PartitionedRegion pr) {
    Set<BucketRegion> brs = pr.getDataStore().getAllLocalBucketRegions();

    long bytes = 0;
    for (Iterator<BucketRegion> itr = brs.iterator(); itr.hasNext();) {
      BucketRegion br = itr.next();
      bytes += br.getBytesInMemory();
    }

    return bytes;
  }

  @Test
  public void incPartitionedRegionClearLocalDurationIncrementsPartitionedRegionClearLocalDuration() {
    String regionname = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPR(regionname + 1, localMaxMemory, 0);
    PartitionedRegionStats partitionedRegionStats = pr.getPrStats();
    partitionedRegionStats.incPartitionedRegionClearLocalDuration(100L);
    assertThat(partitionedRegionStats.getStats().getLong(regionClearLocalDurationId))
        .isEqualTo(100L);
  }

  @Test
  public void incPartitionedRegionClearTotalDurationIncrementsPartitionedRegionClearTotalDuration() {
    String regionname = "testStats";
    int localMaxMemory = 100;
    PartitionedRegion pr = createPR(regionname + 1, localMaxMemory, 0);
    PartitionedRegionStats partitionedRegionStats = pr.getPrStats();
    partitionedRegionStats.incPartitionedRegionClearTotalDuration(100L);

    assertThat(partitionedRegionStats.getStats().getLong(regionClearTotalDurationId))
        .isEqualTo(100L);
  }
}
