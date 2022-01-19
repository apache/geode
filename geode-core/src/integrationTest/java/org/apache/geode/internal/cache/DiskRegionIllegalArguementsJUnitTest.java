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
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.distributed.DistributedSystem;

/**
 * This test tests Illegal arguments being passed to create disk regions. The creation of the DWA
 * object should throw a relevant exception if the arguments specified are incorrect.
 */
public class DiskRegionIllegalArguementsJUnitTest {

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
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  /**
   * test Illegal max oplog size
   */
  @Test
  public void testMaxOplogSize() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setMaxOplogSize(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }
    dsf.setMaxOplogSize(1);
    assertEquals(1, dsf.create("test").getMaxOplogSize());
  }

  @Test
  public void testCompactionThreshold() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();

    try {
      dsf.setCompactionThreshold(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }

    try {
      dsf.setCompactionThreshold(101);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }

    dsf.setCompactionThreshold(0);
    dsf.setCompactionThreshold(100);
    assertEquals(100, dsf.create("test").getCompactionThreshold());
  }

  @Test
  public void testAutoCompact() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(true);
    assertEquals(true, dsf.create("test").getAutoCompact());

    dsf.setAutoCompact(false);
    assertEquals(false, dsf.create("test2").getAutoCompact());
  }

  @Test
  public void testAllowForceCompaction() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAllowForceCompaction(true);
    assertEquals(true, dsf.create("test").getAllowForceCompaction());

    dsf.setAllowForceCompaction(false);
    assertEquals(false, dsf.create("test2").getAllowForceCompaction());
  }

  @Test
  public void testDiskDirSize() {

    File file1 = new File("file1");

    File file2 = new File("file2");
    File file3 = new File("file3");
    File file4 = new File("file4");
    file1.mkdir();
    file2.mkdir();
    file3.mkdir();
    file4.mkdir();
    file1.deleteOnExit();
    file2.deleteOnExit();
    file3.deleteOnExit();
    file4.deleteOnExit();

    File[] dirs = {file1, file2, file3, file4};

    int[] ints = {1, 2, 3, -4};

    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setDiskDirsAndSizes(dirs, ints);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }

    int[] ints1 = {1, 2, 3};

    try {
      dsf.setDiskDirsAndSizes(dirs, ints1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }
    ints[3] = 4;
    dsf.setDiskDirsAndSizes(dirs, ints);
  }

  @Test
  public void testDiskDirs() {
    File file1 = new File("file6");

    File file2 = new File("file7");
    File file3 = new File("file8");
    File file4 = new File("file9");

    File[] dirs = {file1, file2, file3, file4};

    int[] ints = {1, 2, 3, 4};

    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setDiskDirsAndSizes(dirs, ints);
      // The disk store would create the disk store directories.
      // fail("expected IllegalArgumentException");

    } catch (IllegalArgumentException ignored) {
    }

    int[] ints1 = {1, 2, 3};
    file1.mkdir();
    file2.mkdir();
    file3.mkdir();
    file4.mkdir();
    file1.deleteOnExit();
    file2.deleteOnExit();
    file3.deleteOnExit();
    file4.deleteOnExit();

    try {
      dsf.setDiskDirsAndSizes(dirs, ints1);
      // The disk store would create the disk store directories.

      // fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }
    dsf.setDiskDirsAndSizes(dirs, ints);
  }

  @Test
  public void testQueueSize() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setQueueSize(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }

    dsf.setQueueSize(1);
    assertEquals(1, dsf.create("test").getQueueSize(), 1);
  }

  @Test
  public void testTimeInterval() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setTimeInterval(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }

    dsf.setTimeInterval(1);
    assertEquals(dsf.create("test").getTimeInterval(), 1);
  }

  @Test
  public void testDiskUsageWarningPercentage() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setDiskUsageWarningPercentage(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }

    try {
      dsf.setDiskUsageWarningPercentage(101);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }

    dsf.setDiskUsageWarningPercentage(50);
    assertEquals(50.0f, dsf.create("test").getDiskUsageWarningPercentage(), 0.01);
  }

  @Test
  public void testDiskUsageCriticalPercentage() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    try {
      dsf.setDiskUsageCriticalPercentage(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }

    try {
      dsf.setDiskUsageCriticalPercentage(101);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException ignored) {
    }

    dsf.setDiskUsageCriticalPercentage(50);
    assertEquals(50.0f, dsf.create("test").getDiskUsageCriticalPercentage(), 0.01);
  }
}
