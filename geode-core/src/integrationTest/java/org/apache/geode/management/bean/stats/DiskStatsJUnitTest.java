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
package org.apache.geode.management.bean.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.DiskStoreStats;
import org.apache.geode.management.internal.beans.DiskStoreMBeanBridge;
import org.apache.geode.test.junit.categories.JMXTest;

@Category({JMXTest.class})
public class DiskStatsJUnitTest extends MBeanStatsTestCase {

  private DiskStoreMBeanBridge bridge;

  private DiskStoreStats diskStoreStats;

  private static final long testStartTime = NanoTimer.getTime();

  @Override
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
    assertTrue(getFlushTimeAvgLatency() > 0);
    assertTrue(getDiskReadsAvgLatency() > 0);
    assertTrue(getDiskWritesAvgLatency() > 0);
    assertTrue(getDiskReadsRate() > 0);
    assertTrue(getDiskWritesRate() > 0);
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
