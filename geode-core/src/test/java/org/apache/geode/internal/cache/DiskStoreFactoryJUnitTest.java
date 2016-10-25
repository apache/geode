/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Tests DiskStoreFactory
 */
@Category(IntegrationTest.class)
public class DiskStoreFactoryJUnitTest {

  private static Cache cache = null;

  private static DistributedSystem ds = null;
  private static Properties props = new Properties();

  static {
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, "config"); // to keep diskPerf logs smaller
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(STATISTIC_ARCHIVE_FILE, "stats.gfs");
  }

  @Before
  public void setUp() throws Exception {
    createCache();
  }

  private Cache createCache() {
    cache = new CacheFactory(props).create();
    ds = cache.getDistributedSystem();
    return cache;
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  /**
   * Test method for
   * 'org.apache.geode.cache.DiskWriteAttributes.getDefaultInstance()'
   */
  @Test
  public void testGetDefaultInstance() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testGetDefaultInstance";
    assertEquals(null, cache.findDiskStore(name));
    DiskStore ds = dsf.create(name);
    assertEquals(ds, cache.findDiskStore(name));
    assertEquals(name, ds.getName());
    assertEquals(DiskStoreFactory.DEFAULT_AUTO_COMPACT, ds.getAutoCompact());
    assertEquals(DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD, ds.getCompactionThreshold());
    assertEquals(DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION, ds.getAllowForceCompaction());
    assertEquals(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE, ds.getMaxOplogSize());
    assertEquals(DiskStoreFactory.DEFAULT_TIME_INTERVAL, ds.getTimeInterval());
    assertEquals(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE, ds.getWriteBufferSize());
    assertEquals(DiskStoreFactory.DEFAULT_QUEUE_SIZE, ds.getQueueSize());
    if (!Arrays.equals(DiskStoreFactory.DEFAULT_DISK_DIRS, ds.getDiskDirs())) {
      fail("expected=" + Arrays.toString(DiskStoreFactory.DEFAULT_DISK_DIRS)
           + " had=" + Arrays.toString(ds.getDiskDirs()));
    }
    if (!Arrays.equals(DiskStoreFactory.DEFAULT_DISK_DIR_SIZES, ds.getDiskDirSizes())) {
      fail("expected=" + Arrays.toString(DiskStoreFactory.DEFAULT_DISK_DIR_SIZES)
           + " had=" + Arrays.toString(ds.getDiskDirSizes()));
    }
  }

  @Test
  public void testNonDefaults() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testNonDefaults";
    DiskStore ds = dsf
      .setAutoCompact(!DiskStoreFactory.DEFAULT_AUTO_COMPACT)
      .setCompactionThreshold(DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD/2)
      .setAllowForceCompaction(!DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION)
      .setMaxOplogSize(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE+1)
      .setTimeInterval(DiskStoreFactory.DEFAULT_TIME_INTERVAL+1)
      .setWriteBufferSize(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE+1)
      .setQueueSize(DiskStoreFactory.DEFAULT_QUEUE_SIZE+1)
      .create(name);
    assertEquals(!DiskStoreFactory.DEFAULT_AUTO_COMPACT, ds.getAutoCompact());
    assertEquals(DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD/2, ds.getCompactionThreshold());
    assertEquals(!DiskStoreFactory.DEFAULT_ALLOW_FORCE_COMPACTION, ds.getAllowForceCompaction());
    assertEquals(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE+1, ds.getMaxOplogSize());
    assertEquals(DiskStoreFactory.DEFAULT_TIME_INTERVAL+1, ds.getTimeInterval());
    assertEquals(DiskStoreFactory.DEFAULT_WRITE_BUFFER_SIZE+1, ds.getWriteBufferSize());
    assertEquals(DiskStoreFactory.DEFAULT_QUEUE_SIZE+1, ds.getQueueSize());
  }

  @Test
  public void testCompactionThreshold() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testCompactionThreshold1";
    DiskStore ds = dsf
      .setCompactionThreshold(0)
      .create(name);
    assertEquals(0, ds.getCompactionThreshold());
    name = "testCompactionThreshold2";
    ds = dsf
      .setCompactionThreshold(100)
      .create(name);
    assertEquals(100, ds.getCompactionThreshold());
    // check illegal stuff
    try {
      dsf.setCompactionThreshold(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      dsf.setCompactionThreshold(101);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testQueueSize() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testQueueSize";
    DiskStore ds = dsf
      .setQueueSize(0)
      .create(name);
    assertEquals(0, ds.getQueueSize());
    name = "testQueueSize2";
    ds = dsf
      .setQueueSize(Integer.MAX_VALUE)
      .create(name);
    assertEquals(Integer.MAX_VALUE, ds.getQueueSize());
    // check illegal stuff
    try {
      dsf.setQueueSize(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
  }
  @Test
  public void testWriteBufferSize() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testWriteBufferSize";
    DiskStore ds = dsf
      .setWriteBufferSize(0)
      .create(name);
    assertEquals(0, ds.getWriteBufferSize());
    name = "testWriteBufferSize2";
    ds = dsf
      .setWriteBufferSize(Integer.MAX_VALUE)
      .create(name);
    assertEquals(Integer.MAX_VALUE, ds.getWriteBufferSize());
    // check illegal stuff
    try {
      dsf.setWriteBufferSize(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testTimeInterval() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testTimeInterval";
    DiskStore ds = dsf
      .setTimeInterval(0)
      .create(name);
    assertEquals(0, ds.getTimeInterval());
    name = "testTimeInterval2";
    ds = dsf
      .setTimeInterval(Long.MAX_VALUE)
      .create(name);
    assertEquals(Long.MAX_VALUE, ds.getTimeInterval());
    // check illegal stuff
    try {
      dsf.setTimeInterval(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testMaxOplogSize() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testMaxOplogSize";
    DiskStore ds = dsf
      .setMaxOplogSize(0)
      .create(name);
    assertEquals(0, ds.getMaxOplogSize());
    name = "testMaxOplogSize2";
    long max = Long.MAX_VALUE / (1024*1024);
    ds = dsf
      .setMaxOplogSize(max)
      .create(name);
    assertEquals(max, ds.getMaxOplogSize());
    // check illegal stuff
    try {
      dsf.setMaxOplogSize(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      dsf.setMaxOplogSize(max+1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testFlush() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testFlush";
    DiskStore ds = dsf.create(name);
    ds.flush();
  }

  @Test
  public void testForceRoll() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testForceRoll";
    DiskStore ds = dsf.create(name);
    ds.forceRoll();
  }
  
  @Test
  public void testDestroyWithPersistentRegion() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testDestroy";
    DiskStore ds = dsf.create(name);
    
    Region region = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT)
    .setDiskStoreName("testDestroy")
    .create("region");
    
    try {
      ds.destroy();
      fail("Should have thrown an exception");
    } catch(IllegalStateException expected) {
      //expected
    }
    
    region.destroyRegion();
    
    //This should now work
    ds.destroy();
  }
  
  @Test
  public void testDestroyWithClosedRegion() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testDestroy";
    DiskStore ds = dsf.create(name);
    
    Region region = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT)
    .setDiskStoreName("testDestroy")
    .create("region");
    
    region.close();
    
    //This should now work
    ds.destroy();
  }
  
  @Test
  public void testDestroyWithOverflowRegion() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testDestroy";
    DiskStore ds = dsf.create(name);
    
    Region region = cache.createRegionFactory(RegionShortcut.LOCAL_OVERFLOW)
    .setDiskStoreName("testDestroy")
    .create("region");
    
    try {
      ds.destroy();
      fail("Should have thrown an exception");
    } catch(IllegalStateException expected) {
      System.err.println("Got expected :" + expected.getMessage());
    }
    
    region.close();
    
    //The destroy should now work.
    ds.destroy();
  }

  @Test
  public void testForceCompaction() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAllowForceCompaction(true);
    String name = "testForceCompaction";
    DiskStore ds = dsf.create(name);
    assertEquals(false, ds.forceCompaction());
  }

  @Test
  public void testMissingInitFile() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testMissingInitFile";
    DiskStore diskStore = dsf.create(name);
    File ifFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + DiskInitFile.IF_FILE_EXT);
    assertTrue(ifFile.exists());
    AttributesFactory af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    cache.createRegion("r", af.create());
    cache.close();
    assertTrue(ifFile.exists());
    assertTrue(ifFile.delete());
    assertFalse(ifFile.exists());
    cache = createCache();
    dsf = cache.createDiskStoreFactory();
    assertEquals(null, ((GemFireCacheImpl)cache).findDiskStore(name));
    try {
      dsf.create(name);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    // if test passed clean up files
    removeFiles(diskStore);
  }

  private void removeFiles(DiskStore diskStore) {
    final String diskStoreName = diskStore.getName();
    File[] dirs = diskStore.getDiskDirs();
    
    for(File dir : dirs) {
      File[] files = dir.listFiles(new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith("BACKUP" + diskStoreName);
        }
        
      });
      for(File file : files) {
        file.delete();
      }
    }
  }

  @Test
  public void testMissingCrfFile() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testMissingCrfFile";
    DiskStore diskStore = dsf.create(name);
    File crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.crf");
    AttributesFactory af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    r.put("key", "value");
    assertTrue(crfFile.exists());
    cache.close();
    assertTrue(crfFile.exists());
    assertTrue(crfFile.delete());
    assertFalse(crfFile.exists());
    cache = createCache();
    dsf = cache.createDiskStoreFactory();
    assertEquals(null, ((GemFireCacheImpl)cache).findDiskStore(name));
    try {
      dsf.create(name);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    // if test passed clean up files
    removeFiles(diskStore);
  }
  
  @Test
  public void testMissingDrfFile() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    String name = "testMissingDrfFile";
    DiskStore diskStore = dsf.create(name);
    File drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.drf");
    AttributesFactory af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    r.put("key", "value");
    assertTrue(drfFile.exists());
    cache.close();
    assertTrue(drfFile.exists());
    assertTrue(drfFile.delete());
    assertFalse(drfFile.exists());
    cache = createCache();
    dsf = cache.createDiskStoreFactory();
    assertEquals(null, ((GemFireCacheImpl)cache).findDiskStore(name));
    try {
      dsf.create(name);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    // if test passed clean up files
    removeFiles(diskStore);
  }

  @Test
  public void testRedefiningDefaultDiskStore() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(!DiskStoreFactory.DEFAULT_AUTO_COMPACT);
    String name = "testMissingDrfFile";
    assertEquals(null, cache.findDiskStore(DiskStoreFactory.DEFAULT_DISK_STORE_NAME));
    DiskStore diskStore = dsf.create(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    r.put("key", "value");
    DiskStore ds = ((LocalRegion)r).getDiskStore();
    assertEquals(ds, cache.findDiskStore(DiskStoreFactory.DEFAULT_DISK_STORE_NAME));
    assertEquals(DiskStoreFactory.DEFAULT_DISK_STORE_NAME, ds.getName());
    assertEquals(!DiskStoreFactory.DEFAULT_AUTO_COMPACT, ds.getAutoCompact());
    cache.close();
    // if test passed clean up files
    removeFiles(diskStore);
  }
  
  // setDiskDirs and setDiskDirsAndSizes are tested in DiskRegionIllegalArguementsJUnitTest
  // also setDiskUsageWarningPercentage and setDiskUsageCriticalPercentage
}
