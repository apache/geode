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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskWriteAttributesFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystem;

/**
 * Tests the old disk apis to make sure they do the correct thing. Once we drop these old deprecated
 * disk apis then this unit test can be removed.
 */
public class DiskOldAPIsJUnitTest {

  protected static Cache cache = null;

  protected static DistributedSystem ds = null;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, "config"); // to keep diskPerf logs smaller
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(STATISTIC_ARCHIVE_FILE, "stats.gfs");

    cache = new CacheFactory(props).create();
    ds = cache.getDistributedSystem();
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
  }

  /**
   * Make sure that if diskWriteAttributes sets sync then it shows up in the new apis.
   */
  @Test
  public void testSyncBit() throws Exception {
    doSyncBitTest(true);
    doSyncBitTest(false);
    doSyncBitTest(true);
  }

  private void doSyncBitTest(boolean destroyRegion) throws Exception {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setSynchronous(true);
    AttributesFactory af = new AttributesFactory();
    af.setDiskWriteAttributes(dwaf.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    assertEquals(true, r.getAttributes().isDiskSynchronous());
    if (destroyRegion) {
      r.localDestroyRegion();
    } else {
      r.close();
    }

    Thread.sleep(1);
    dwaf.setSynchronous(false);
    af.setDiskWriteAttributes(dwaf.create());
    r = cache.createRegion("r", af.create());
    assertEquals(false, r.getAttributes().isDiskSynchronous());
    if (destroyRegion) {
      r.localDestroyRegion();
    } else {
      r.close();
    }

    // now try it with a persistent pr
    dwaf.setSynchronous(true);
    af.setDiskWriteAttributes(dwaf.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    r = cache.createRegion("r2", af.create());
    assertEquals(true, r.getAttributes().isDiskSynchronous());
    r.put("key", "value");
    {
      PartitionedRegion pr = (PartitionedRegion) r;
      PartitionedRegionDataStore prds = pr.getDataStore();
      Set<BucketRegion> s = prds.getAllLocalBucketRegions();
      assertTrue(s.size() > 0);
      for (BucketRegion br : s) {
        assertEquals(true, br.getAttributes().isDiskSynchronous());
      }
    }
    if (destroyRegion) {
      r.localDestroyRegion();
    } else {
      r.close();
    }

    dwaf.setSynchronous(false);
    af.setDiskWriteAttributes(dwaf.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    r = cache.createRegion("r2", af.create());
    assertEquals(false, r.getAttributes().isDiskSynchronous());
    r.put("key", "value");
    {
      PartitionedRegion pr = (PartitionedRegion) r;
      PartitionedRegionDataStore prds = pr.getDataStore();
      Set<BucketRegion> s = prds.getAllLocalBucketRegions();
      assertTrue(s.size() > 0);
      for (BucketRegion br : s) {
        assertEquals(false, br.getAttributes().isDiskSynchronous());
      }
    }
    if (destroyRegion) {
      r.localDestroyRegion();
    } else {
      r.close();
    }

    // now try it with an overflow pr
    dwaf.setSynchronous(true);
    af.setDiskWriteAttributes(dwaf.create());
    af.setDataPolicy(DataPolicy.PARTITION);
    af.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    r = cache.createRegion("r3", af.create());
    assertEquals(true, r.getAttributes().isDiskSynchronous());
    {
      for (int i = 0; i < 300; i++) {
        r.put("key" + i, "value" + i);
      }
      PartitionedRegion pr = (PartitionedRegion) r;
      PartitionedRegionDataStore prds = pr.getDataStore();
      Set<BucketRegion> s = prds.getAllLocalBucketRegions();
      assertTrue(s.size() > 0);
      for (BucketRegion br : s) {
        assertEquals(true, br.getAttributes().isDiskSynchronous());
      }
    }
    if (destroyRegion) {
      r.localDestroyRegion();
    } else {
      r.close();
    }

    dwaf.setSynchronous(false);
    af.setDiskWriteAttributes(dwaf.create());
    af.setDataPolicy(DataPolicy.PARTITION);
    af.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    r = cache.createRegion("r3", af.create());
    assertEquals(false, r.getAttributes().isDiskSynchronous());
    {
      for (int i = 0; i < 300; i++) {
        r.put("key" + i, "value" + i);
      }
      PartitionedRegion pr = (PartitionedRegion) r;
      PartitionedRegionDataStore prds = pr.getDataStore();
      Set<BucketRegion> s = prds.getAllLocalBucketRegions();
      assertTrue(s.size() > 0);
      for (BucketRegion br : s) {
        assertEquals(false, br.getAttributes().isDiskSynchronous());
      }
    }
    if (destroyRegion) {
      r.localDestroyRegion();
    } else {
      r.close();
    }
  }

  /**
   * Make sure that if diskWriteAttributes are used that the diskStore that is created will use
   * them. Note that the isSync bit is tested by another method.
   */
  @Test
  public void testDWA_1() throws Exception {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setMaxOplogSize(1);
    dwaf.setTimeInterval(333);
    dwaf.setBytesThreshold(666);
    AttributesFactory af = new AttributesFactory();
    af.setDiskWriteAttributes(dwaf.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    {
      LocalRegion lr = (LocalRegion) r;
      DiskStoreImpl ds = lr.getDiskStore();
      assertEquals(1, ds.getMaxOplogSize());
      assertEquals(333, ds.getTimeInterval());
      // byteThreshold > 0 --> queueSize == 1
      assertEquals(1, ds.getQueueSize());
    }
    r.localDestroyRegion();

    // now try it with a pr
    af.setDiskWriteAttributes(dwaf.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    r = cache.createRegion("r", af.create());
    {
      LocalRegion lr = (LocalRegion) r;
      DiskStoreImpl ds = lr.getDiskStore();
      assertEquals(1, ds.getMaxOplogSize());
      assertEquals(333, ds.getTimeInterval());
      // byteThreshold > 0 --> queueSize == 1
      assertEquals(1, ds.getQueueSize());
    }
    r.put("key", "value");
    {
      PartitionedRegion pr = (PartitionedRegion) r;
      PartitionedRegionDataStore prds = pr.getDataStore();
      Set<BucketRegion> s = prds.getAllLocalBucketRegions();
      assertTrue(s.size() > 0);
      for (BucketRegion br : s) {
        LocalRegion lr = (LocalRegion) br;
        DiskStoreImpl ds = lr.getDiskStore();
        assertEquals(1, ds.getMaxOplogSize());
        assertEquals(333, ds.getTimeInterval());
        // byteThreshold > 0 --> queueSize == 1
        assertEquals(1, ds.getQueueSize());
      }
    }
    r.localDestroyRegion();
  }

  @Test
  public void testDWA_2() throws Exception {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setMaxOplogSize(2);
    dwaf.setTimeInterval(1);
    dwaf.setBytesThreshold(0);
    AttributesFactory af = new AttributesFactory();
    af.setDiskWriteAttributes(dwaf.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    {
      LocalRegion lr = (LocalRegion) r;
      DiskStoreImpl ds = lr.getDiskStore();
      assertEquals(2, ds.getMaxOplogSize());
      assertEquals(1, ds.getTimeInterval());
      assertEquals(0, ds.getQueueSize());
    }
    r.localDestroyRegion();

    // now try it with a pr
    af.setDiskWriteAttributes(dwaf.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    r = cache.createRegion("r", af.create());
    {
      LocalRegion lr = (LocalRegion) r;
      DiskStoreImpl ds = lr.getDiskStore();
      assertEquals(2, ds.getMaxOplogSize());
      assertEquals(1, ds.getTimeInterval());
      assertEquals(0, ds.getQueueSize());
    }
    r.put("key", "value");
    {
      PartitionedRegion pr = (PartitionedRegion) r;
      PartitionedRegionDataStore prds = pr.getDataStore();
      Set<BucketRegion> s = prds.getAllLocalBucketRegions();
      assertTrue(s.size() > 0);
      for (BucketRegion br : s) {
        LocalRegion lr = (LocalRegion) br;
        DiskStoreImpl ds = lr.getDiskStore();
        assertEquals(2, ds.getMaxOplogSize());
        assertEquals(1, ds.getTimeInterval());
        assertEquals(0, ds.getQueueSize());
      }
    }
    r.localDestroyRegion();
  }

  /**
   * Make sure the old diskDirs apis get mapped onto the diskStore.
   */
  @Test
  public void testDiskDirs() throws Exception {
    File f1 = new File("testDiskDir1");
    f1.mkdir();
    File f2 = new File("testDiskDir2");
    f2.mkdir();
    try {
      AttributesFactory af = new AttributesFactory();
      af.setDiskDirs(new File[] {f1, f2});
      af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      Region r = cache.createRegion("r", af.create());
      {
        LocalRegion lr = (LocalRegion) r;
        DiskStoreImpl ds = lr.getDiskStore();
        File[] dirs = ds.getDiskDirs();
        assertEquals(2, dirs.length);
        assertEquals(f1, dirs[0]);
        assertEquals(f2, dirs[1]);
      }
      r.localDestroyRegion();

      // now try it with a pr
      af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      r = cache.createRegion("r", af.create());
      {
        LocalRegion lr = (LocalRegion) r;
        DiskStoreImpl ds = lr.getDiskStore();
        File[] dirs = ds.getDiskDirs();
        assertEquals(2, dirs.length);
        assertEquals(f1, dirs[0]);
        assertEquals(f2, dirs[1]);
      }
      r.put("key", "value");
      {
        PartitionedRegion pr = (PartitionedRegion) r;
        PartitionedRegionDataStore prds = pr.getDataStore();
        Set<BucketRegion> s = prds.getAllLocalBucketRegions();
        assertTrue(s.size() > 0);
        for (BucketRegion br : s) {
          LocalRegion lr = (LocalRegion) br;
          DiskStoreImpl ds = lr.getDiskStore();
          File[] dirs = ds.getDiskDirs();
          assertEquals(2, dirs.length);
          assertEquals(f1, dirs[0]);
          assertEquals(f2, dirs[1]);
        }
      }
      r.localDestroyRegion();
    } finally {
      cache.close();
      removeDir(f1);
      removeDir(f2);
    }
  }

  /**
   * Make sure the old diskDirs apis get mapped onto the diskStore.
   */
  @Test
  public void testDiskDirsAndSizes() throws Exception {
    File f1 = new File("testDiskDir1");
    f1.mkdir();
    File f2 = new File("testDiskDir2");
    f2.mkdir();
    try {
      AttributesFactory af = new AttributesFactory();
      af.setDiskDirsAndSizes(new File[] {f1, f2}, new int[] {1, 2});
      af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      Region r = cache.createRegion("r", af.create());
      {
        LocalRegion lr = (LocalRegion) r;
        DiskStoreImpl ds = lr.getDiskStore();
        File[] dirs = ds.getDiskDirs();
        assertEquals(2, dirs.length);
        assertEquals(f1, dirs[0]);
        assertEquals(f2, dirs[1]);
        int[] sizes = ds.getDiskDirSizes();
        assertEquals(2, sizes.length);
        assertEquals(1, sizes[0]);
        assertEquals(2, sizes[1]);
      }
      r.localDestroyRegion();

      // now try it with a pr
      af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      r = cache.createRegion("r", af.create());
      {
        LocalRegion lr = (LocalRegion) r;
        DiskStoreImpl ds = lr.getDiskStore();
        File[] dirs = ds.getDiskDirs();
        assertEquals(2, dirs.length);
        assertEquals(f1, dirs[0]);
        assertEquals(f2, dirs[1]);
        int[] sizes = ds.getDiskDirSizes();
        assertEquals(2, sizes.length);
        assertEquals(1, sizes[0]);
        assertEquals(2, sizes[1]);
      }
      r.put("key", "value");
      {
        PartitionedRegion pr = (PartitionedRegion) r;
        PartitionedRegionDataStore prds = pr.getDataStore();
        Set<BucketRegion> s = prds.getAllLocalBucketRegions();
        assertTrue(s.size() > 0);
        for (BucketRegion br : s) {
          LocalRegion lr = (LocalRegion) br;
          DiskStoreImpl ds = lr.getDiskStore();
          File[] dirs = ds.getDiskDirs();
          assertEquals(2, dirs.length);
          assertEquals(f1, dirs[0]);
          assertEquals(f2, dirs[1]);
          int[] sizes = ds.getDiskDirSizes();
          assertEquals(2, sizes.length);
          assertEquals(1, sizes[0]);
          assertEquals(2, sizes[1]);
        }
      }
      r.localDestroyRegion();
    } finally {
      cache.close();
      removeDir(f1);
      removeDir(f2);
    }
  }

  private static void removeDir(File dir) {
    File[] files = dir.listFiles();
    for (int i = 0; i < files.length; i++) {
      files[i].delete();
    }
    dir.delete();
  }
}
