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
package org.apache.geode.management.bean.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.DiskRegionStats;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.management.internal.beans.DiskRegionBridge;
import org.apache.geode.management.internal.beans.PartitionedRegionBridge;
import org.apache.geode.management.internal.beans.RegionMBeanBridge;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Implementing RegionMXBean to ensure test coverage
 * 
 */
@Category(IntegrationTest.class)
public class RegionStatsJUnitTest extends MBeanStatsTestCase{
  
  private RegionMBeanBridge bridge;
  
  private PartitionedRegionBridge parBridge;
  
  private DiskRegionBridge diskBridge;
  
  private CachePerfStats cachePerfStats;
  
  private PartitionedRegionStats partitionedRegionStats;
  
  private DiskRegionStats diskRegionStats;

  protected void init() {
    cachePerfStats = new CachePerfStats(system);
    partitionedRegionStats = new PartitionedRegionStats(system, "/tests");
    diskRegionStats = new DiskRegionStats(system, "test-disk");

    bridge = new RegionMBeanBridge(cachePerfStats);
    parBridge = new PartitionedRegionBridge(partitionedRegionStats);
    diskBridge = new DiskRegionBridge(diskRegionStats);
  }
  
  @Test
  public void testPlainCounters() throws InterruptedException {
    cachePerfStats.incEntryCount(400);
    cachePerfStats.incDiskTasksWaiting();
    partitionedRegionStats.incBucketCount(2);
    partitionedRegionStats.incLowRedundancyBucketCount(1);
    partitionedRegionStats.setActualRedundantCopies(2);

    partitionedRegionStats.setConfiguredRedundantCopies(3);
    partitionedRegionStats.incDataStoreEntryCount(1);
    partitionedRegionStats.incPrimaryBucketCount(10);

    sample();

    assertEquals(400, getEntryCount());
    assertEquals(2, getBucketCount());
    assertEquals(1, getNumBucketsWithoutRedundancy());
    assertEquals(2, getActualRedundancy());
    //assertTrue(getAvgBucketSize() > 0);
    //assertIndexDetailsEquals(3, getConfiguredRedundancy());
    assertEquals(1, getDataStoreEntryCount());
    assertEquals(10, getPrimaryBucketCount());
  }

  @Test
  public void testDiskCounters() throws InterruptedException {
    final long startTime = CachePerfStats.getStatTime();

    diskRegionStats.incNumEntriesInVM(10);
    diskRegionStats.incNumOverflowOnDisk(15);
    diskRegionStats.startWrite();
    diskRegionStats.incWrittenBytes(1000);

    sample();

    assertEquals(1, getTotalDiskWritesProgress());
    assertEquals(10, getTotalDiskEntriesInVM());
    assertEquals(15, getTotalEntriesOnlyOnDisk());

    diskRegionStats.endWrite(startTime, CachePerfStats.getStatTime());
    diskRegionStats.endRead(startTime, CachePerfStats.getStatTime(), 1000);

    sample();
    
    assertTrue(getDiskReadsRate() > 0);
    assertTrue(getDiskReadsAverageLatency() > 0);
    
    assertTrue(getDiskWritesRate() > 0);
    assertTrue(getDiskWritesAverageLatency() > 0);
  }
  
  @Test
  public void testTimeBasedCounters() throws InterruptedException {
    final long startTime = CachePerfStats.getStatTime();

    cachePerfStats.startCacheListenerCall();
    cachePerfStats.startCacheWriterCall();
    partitionedRegionStats.startPutRemote();
    
    sample();

    cachePerfStats.endCacheListenerCall(startTime);
    cachePerfStats.endCacheWriterCall(startTime); 
    partitionedRegionStats.endPutRemote(startTime);
  
    sample();

    assertTrue(getCacheListenerCallsAvgLatency() > 0);
    assertTrue(getCacheWriterCallsAvgLatency() > 0);
    assertTrue(getPutRemoteLatency() > 0);
  }

  private long getEntryCount() {
    return bridge.getEntryCount();
  }
 
  private long getCacheListenerCallsAvgLatency() {
    return bridge.getCacheListenerCallsAvgLatency();
  }
 
  private long getCacheWriterCallsAvgLatency() {
    return bridge.getCacheWriterCallsAvgLatency();
  }
 
  private float getCreatesRate() {
    return bridge.getCreatesRate();
  }
 
  private float getPutAllRate() {
    return bridge.getPutAllRate();
  }
 
  private float getPutLocalRate() {
    return parBridge.getPutLocalRate();
  }
 
  private float getPutRemoteRate() {
    return parBridge.getPutRemoteRate();
  }
 
  private long getPutRemoteAvgLatency() {
    return parBridge.getPutRemoteAvgLatency();
  }
 
  private long getPutRemoteLatency() {
    return parBridge.getPutRemoteLatency();
  }
 
  private float getPutsRate() {
    return bridge.getPutsRate();
  }
 
  private float getDestroyRate() {
    return bridge.getDestroyRate();
  }
 
  private float getGetsRate() {
    return bridge.getGetsRate();
  }
 
  private long getHitCount() {
    return bridge.getHitCount();
  }
 
  private float getHitRatio() {
   return bridge.getHitRatio();
  }
 
  private long getLastAccessedTime() {
    return bridge.getLastAccessedTime();
  }
 
  private long getLastModifiedTime() {
    return bridge.getLastModifiedTime();
  }
 
  private float getLruDestroyRate() {
    return bridge.getLruDestroyRate();
  }
 
  private float getLruEvictionRate() {
    return bridge.getLruEvictionRate();
  }
 
  private long getMissCount() {
   return bridge.getMissCount();
  }
 
  private float getDiskReadsRate() {
    return diskBridge.getDiskReadsRate();
  }
 
  private float getDiskWritesRate() {
    return diskBridge.getDiskWritesRate();
  }
 
  private long getDiskReadsAverageLatency() {
    return diskBridge.getDiskReadsAverageLatency();
  }
 
  private long getDiskWritesAverageLatency() {
    return diskBridge.getDiskWritesAverageLatency();
  }
 
  private long getTotalDiskWritesProgress() {
    return diskBridge.getTotalDiskWritesProgress();
  }
 
  private long getTotalDiskEntriesInVM() {
    return diskBridge.getTotalDiskEntriesInVM();
  }
 
  private long getTotalEntriesOnlyOnDisk() {
    return diskBridge.getTotalEntriesOnlyOnDisk();
  }
 
  private int getActualRedundancy() {
    return parBridge.getActualRedundancy();
  }
 
  private int getAvgBucketSize() {
    return parBridge.getAvgBucketSize();
  }
 
  private int getBucketCount() {
    return parBridge.getBucketCount();
  }
 
  private int getConfiguredRedundancy() {
    return parBridge.getConfiguredRedundancy();
  }
 

  private int getNumBucketsWithoutRedundancy() {
    return parBridge.getNumBucketsWithoutRedundancy();
  }
 
  private int getPrimaryBucketCount() {
    return parBridge.getPrimaryBucketCount();
  }
 
  private int getDataStoreEntryCount() {
    return parBridge.getTotalBucketSize();
  }

  private long getDiskTaskWaiting() {
    return 0;
  }
}
