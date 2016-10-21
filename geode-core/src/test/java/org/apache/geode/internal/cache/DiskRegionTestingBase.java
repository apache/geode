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
 * DiskRegionTestingBase: This class is extended to write more JUnit tests for Disk Regions.
 */
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.internal.cache.LocalRegion.NonTXEntry;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * All disk region unit tests extend this base class , common method to be used in all tests are
 * present here.
 * 
 * @since GemFire 5.1
 */
public abstract class DiskRegionTestingBase {

  protected static final boolean debug = false;

  protected static Cache cache = null;
  protected static DistributedSystem ds = null;
  protected static Properties props = new Properties();
  protected static File[] dirs = null;
  protected static int[] diskDirSize = null;

  protected Region region;
  protected LogWriter logWriter;

  boolean testFailed;
  String failureCause = "";

  @Rule
  public TestName name = new TestName();

  @Before
  public final void setUp() throws Exception {
    preSetUp();

    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, "config"); // to keep diskPerf logs smaller
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(STATISTIC_ARCHIVE_FILE, "stats.gfs");

    File testingDirectory = new File("testingDirectory");
    testingDirectory.mkdir();
    testingDirectory.deleteOnExit();
    failureCause = "";
    testFailed = false;
    cache = createCache();

    File file1 = new File("testingDirectory/" + name.getMethodName() + "1");
    file1.mkdir();
    file1.deleteOnExit();
    File file2 = new File("testingDirectory/" + name.getMethodName() + "2");
    file2.mkdir();
    file2.deleteOnExit();
    File file3 = new File("testingDirectory/" + name.getMethodName() + "3");
    file3.mkdir();
    file3.deleteOnExit();
    File file4 = new File("testingDirectory/" + name.getMethodName() + "4");
    file4.mkdir();
    file4.deleteOnExit();
    dirs = new File[4];
    dirs[0] = file1;
    dirs[1] = file2;
    dirs[2] = file3;
    dirs[3] = file4;
    diskDirSize = new int[4];
    // set default values of disk dir sizes here
    diskDirSize[0] = Integer.MAX_VALUE;
    diskDirSize[1] = Integer.MAX_VALUE;
    diskDirSize[2] = Integer.MAX_VALUE;
    diskDirSize[3] = Integer.MAX_VALUE;
    deleteFiles();

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;

    postSetUp();
  }

  protected void preSetUp() throws Exception {}

  protected void postSetUp() throws Exception {}

  @After
  public final void tearDown() throws Exception {
    preTearDown();

    try {
      if (cache != null && !cache.isClosed()) {
        for (Iterator itr = cache.rootRegions().iterator(); itr.hasNext();) {
          Region root = (Region) itr.next();
          if (root.isDestroyed() || root instanceof HARegion) {
            continue;
          }
          try {
            logWriter
                .info("<ExpectedException action=add>RegionDestroyedException</ExpectedException>");
            root.localDestroyRegion("teardown");
            logWriter.info(
                "<ExpectedException action=remove>RegionDestroyedException</ExpectedException>");
          } catch (RegionDestroyedException e) {
            // ignore
          }
        }
      }

      for (DiskStoreImpl dstore : ((GemFireCacheImpl) cache).listDiskStoresIncludingRegionOwned()) {
        dstore.waitForClose();
      }
    } finally {
      closeCache();
    }
    ds.disconnect();
    // Asif : below is not needed but leave it
    deleteFiles();
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;

    postTearDown();
  }

  protected void preTearDown() throws Exception {}

  protected void postTearDown() throws Exception {}

  protected Cache createCache() {
    cache = new CacheFactory(props).create();
    ds = cache.getDistributedSystem();
    logWriter = cache.getLogger();
    return cache;
  }

  /** Close the cache */
  private static synchronized final void closeCache() {
    if (cache != null) {
      try {
        if (!cache.isClosed()) {
          CacheTransactionManager txMgr = cache.getCacheTransactionManager();
          if (txMgr != null) {
            if (txMgr.exists()) {
              // make sure we cleanup this threads txid stored in a thread local
              txMgr.rollback();
            }
          }
          cache.close();
        }
      } finally {
        cache = null;
      }
    }
  }

  /**
   * cleans all the directory of all the files present in them
   */
  protected static void deleteFiles() {
    closeDiskStores();
    for (int i = 0; i < dirs.length; i++) {
      System.out.println("trying to delete files in " + dirs[i].getAbsolutePath());
      File[] files = dirs[i].listFiles();
      for (int j = 0; j < files.length; j++) {
        System.out.println("deleting " + files[j]);
        int cnt = 0;
        IOException ioe = null;
        while (cnt < 3) {
          try {
            cnt++;
            FileUtil.delete(files[j]);
            break;
          } catch (IOException e) {
            ioe = e;
            try {
              Thread.sleep(1000);
            } catch (Exception igore) {
            }
          }
        }
        if (cnt >= 3) {
          throw new RuntimeException("Error deleting file " + files[j], ioe);
        }
      }
    }
  }

  protected static void closeDiskStores() {
    if (cache != null) {
      ((GemFireCacheImpl) cache).closeDiskStores();
    }
  }

  /**
   * clears and closes the region
   * 
   */
  protected void closeDown() {
    try {
      if (!region.isDestroyed()) {
        region.destroyRegion();
      }
    } catch (Exception e) {
      this.logWriter.error("DiskRegionTestingBase::closeDown:Exception in destroyiong the region",
          e);
    }
  }

  /**
   * puts a 100 integers into the region
   */
  protected void put100Int() {
    for (int i = 0; i < 100; i++) {
      region.put(new Integer(i), new Integer(i));
    }
  }

  protected void verify100Int() {
    verify100Int(true);
  }

  protected void verify100Int(boolean verifySize) {
    if (verifySize) {
      assertEquals(100, region.size());
    }
    for (int i = 0; i < 100; i++) {
      Integer key = new Integer(i);
      assertTrue(region.containsKey(key));
      assertEquals(key, region.get(key));
    }
  }

  /**
   * will keep on putting till region overflows
   */
  protected void putTillOverFlow(Region region) {
    int i = 0;
    for (i = 0; i < 1010; i++) {
      region.put(new Integer(i + 200), new Integer(i + 200));
    }
  }

  /**
   * put an entry
   */
  protected void putForValidation(Region region) {
    final byte[] value = new byte[1024];
    region.put("testKey", value);
  }

  /**
   * get val from disk
   */
  protected void validatePut(Region region) {
    // flush data to disk
    ((LocalRegion) region).getDiskRegion().flushForTesting();
    try {
      ((LocalRegion) region).getValueOnDisk("testKey");
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("Failed to get the value on disk");
    }
  }

  protected HashMap<String, VersionTag> saveVersionTags(LocalRegion region) {
    HashMap<String, VersionTag> tagmap = new HashMap<String, VersionTag>();
    Iterator entryItr = region.entrySet().iterator();
    while (entryItr.hasNext()) {
      RegionEntry entry = ((NonTXEntry) entryItr.next()).getRegionEntry();
      String key = (String) entry.getKey();
      VersionTag tag = entry.getVersionStamp().asVersionTag();
      tagmap.put(key, tag);
    }
    return tagmap;
  }

  protected void compareVersionTags(HashMap<String, VersionTag> map1,
      HashMap<String, VersionTag> map2) {
    assertEquals(map1.size(), map2.size());

    for (String key : map1.keySet()) {
      VersionTag tag1 = map1.get(key);
      VersionTag tag2 = map2.get(key);
      assertEquals(tag1.getEntryVersion(), tag2.getEntryVersion());
      assertEquals(tag1.getRegionVersion(), tag2.getRegionVersion());
      assertEquals(tag1.getMemberID(), tag2.getMemberID());
    }
  }

  /**
   * Since these are not visible to cache.diskPerf we add wrapper methods to make the following
   * parameters/visible
   */
  public static void setCacheObserverCallBack() {
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
  }

  public static void unSetCacheObserverCallBack() {
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }

  protected void verify(LocalRegion lr, DiskRegionProperties drp) {
    DiskStore ds = cache.findDiskStore(lr.getDiskStoreName());
    assertTrue(ds != null);
    assertTrue(lr.getAttributes().isDiskSynchronous() == drp.isSynchronous());
    assertTrue(ds.getAutoCompact() == drp.isRolling());
    assertEquals(drp.getMaxOplogSize() / (1024 * 1024), ds.getMaxOplogSize());
    if (drp.getTimeInterval() != -1) {
      assertEquals(drp.getTimeInterval(), ds.getTimeInterval());
    } else {
      assertEquals(DiskStoreFactory.DEFAULT_TIME_INTERVAL, ds.getTimeInterval());
    }
    assertEquals((int) drp.getBytesThreshold(), ds.getQueueSize());
    int dirnum = drp.getDiskDirs().length;
    int dirnum2 = ds.getDiskDirs().length;
    int[] diskSizes = drp.getDiskDirSizes();
    int[] ds_diskSizes = ds.getDiskDirSizes();
    assertEquals(dirnum, dirnum2);
    if (diskSizes == null) {
      diskSizes = new int[dirnum];
      java.util.Arrays.fill(diskSizes, Integer.MAX_VALUE);
    }
    for (int i = 0; i < dirnum; i++) {
      assertTrue("diskSizes not matching", diskSizes[i] == ds_diskSizes[i]);
    }

    assertEquals(DiskStoreFactory.DEFAULT_DISK_USAGE_WARNING_PERCENTAGE,
        ds.getDiskUsageWarningPercentage(), 0.01);
    assertEquals(DiskStoreFactory.DEFAULT_DISK_USAGE_CRITICAL_PERCENTAGE,
        ds.getDiskUsageCriticalPercentage(), 0.01);
  }

  public String getName() {
    return name.getMethodName();
  }

}
