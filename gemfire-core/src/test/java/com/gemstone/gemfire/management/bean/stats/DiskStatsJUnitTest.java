/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.bean.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.DiskStoreStats;
import com.gemstone.gemfire.management.internal.beans.DiskStoreMBeanBridge;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author rishim
 */
@Category(IntegrationTest.class)
public class DiskStatsJUnitTest extends MBeanStatsTestCase {

  private DiskStoreMBeanBridge bridge;

  private DiskStoreStats diskStoreStats;

  private static long testStartTime = NanoTimer.getTime();

  public void init() {
    diskStoreStats = new DiskStoreStats(system, "test");

    bridge = new DiskStoreMBeanBridge();
    bridge.addDiskStoreStats(diskStoreStats);
  }

  @Test
  @Ignore("Bug 52268")
  public void testDiskCounters() throws InterruptedException {
    diskStoreStats.startRead();
    diskStoreStats.startWrite();
    diskStoreStats.startBackup();
    diskStoreStats.startRecovery();
    diskStoreStats.incWrittenBytes(20, true);
    diskStoreStats.startFlush();
    diskStoreStats.setQueueSize(10);

    sample();
    
    assertEquals(1, getTotalBackupInProgress());
    assertEquals(1, getTotalRecoveriesInProgress());
    assertEquals(20, getTotalBytesOnDisk());
    assertEquals(10, getTotalQueueSize());

    diskStoreStats.endRead(testStartTime, 20);
    diskStoreStats.endWrite(testStartTime);
    diskStoreStats.endBackup();
    diskStoreStats.endFlush(testStartTime);

    sample();
    
    assertEquals(1, getTotalBackupCompleted());
    assertTrue(getFlushTimeAvgLatency()>0);
    assertTrue(getDiskReadsAvgLatency()>0);
    assertTrue(getDiskWritesAvgLatency()>0);
    assertTrue(getDiskReadsRate()>0);
    assertTrue(getDiskWritesRate()>0);
  }

  private long getDiskReadsAvgLatency() {
    return bridge.getDiskReadsAvgLatency();
  }

  private float getDiskReadsRate() {
    return bridge.getDiskReadsRate();
  }

  private long getDiskWritesAvgLatency() {
    return bridge.getDiskWritesAvgLatency();
  }

  private float getDiskWritesRate() {
    return bridge.getDiskWritesRate();
  }

  private long getFlushTimeAvgLatency() {
    return bridge.getFlushTimeAvgLatency();
  }

  private int getTotalBackupInProgress() {
    return bridge.getTotalBackupInProgress();
  }

  private int getTotalBackupCompleted() {
    return bridge.getTotalBackupCompleted();
  }

  private long getTotalBytesOnDisk() {
    return bridge.getTotalBytesOnDisk();
  }

  private int getTotalQueueSize() {
    return bridge.getTotalQueueSize();
  }

  private int getTotalRecoveriesInProgress() {
    return bridge.getTotalRecoveriesInProgress();
  }
}
