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
package com.gemstone.gemfire.management.bean.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.locks.DLockStats;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.statistics.VMStatsContract;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.DiskStoreStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegionStats;
import com.gemstone.gemfire.internal.cache.execute.FunctionServiceStats;
import com.gemstone.gemfire.internal.stats50.VMStats50;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.beans.MemberMBeanBridge;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 */
@Category(IntegrationTest.class)
public class MemberLevelStatsJUnitTest extends MBeanStatsTestCase {
  @Rule public TestName name = new TestName();

  private MemberMBeanBridge bridge;

  private CachePerfStats cachePerfStats;
  
  private FunctionServiceStats funcServiceStats;
  
  private DistributionStats distributionStats;
  
  private DLockStats dlockStats;

  private List<DiskStoreStats> diskStatsList = new ArrayList<DiskStoreStats>();
  
  private List<PartitionedRegionStats> parRegionStatsList = new ArrayList<PartitionedRegionStats>();
  
  private static TimeUnit nanoSeconds = TimeUnit.NANOSECONDS;
  
  private static long testStartTime = NanoTimer.getTime();

  public void init() {
    cachePerfStats = new CachePerfStats(system);
    funcServiceStats = new FunctionServiceStats(system, "FunctionExecution");
    long statId = OSProcess.getId();
    distributionStats = new DistributionStats(system, statId);
    DistributionStats.enableClockStats = true;
    dlockStats = new DLockStats(system, statId);

    bridge = new MemberMBeanBridge();
    bridge.addCacheStats(cachePerfStats);
    bridge.addFunctionStats(funcServiceStats);
    bridge.addDistributionStats(distributionStats);
    bridge.addLockServiceStats(dlockStats);
    
    
    VMStatsContract vmstats = system.getStatSampler().getVMStats();
    assertTrue(vmstats instanceof VMStats50);
    
    bridge.addSystemStats();
    bridge.addVMStats();
    
    for(int i = 0; i<4 ; i++){
      DiskStoreStats stats = new DiskStoreStats(system, name.getMethodName() + i);
      diskStatsList.add(stats);
      bridge.addDiskStoreStats(stats);
    }
    
    for(int i = 0; i<4 ; i++){
      PartitionedRegionStats stats = new PartitionedRegionStats(system, name.getMethodName() + i);
      parRegionStatsList.add(stats);
      bridge.addPartionRegionStats(stats);
    }
  }
  

  @Test
  public void testPlainCounters() throws InterruptedException {
    cachePerfStats.incRegions(3);
    cachePerfStats.incEntryCount(400);
    cachePerfStats.incGetInitialImageKeysReceived();
    cachePerfStats.incDiskTasksWaiting();

    funcServiceStats.incFunctionExecutionsRunning();
    funcServiceStats.incFunctionExecutionHasResultRunning();
    
    sample();

    assertEquals(3, getTotalRegionCount());
    assertEquals(400, getTotalRegionEntryCount());
    assertEquals(1, getInitialImageKeysReceived());
    assertEquals(1,getNumRunningFunctions());
    assertEquals(1,getNumRunningFunctionsHavingResults());
    assertEquals(1,getTotalDiskTasksWaiting());
  }
  
  @Test
  public void testLockServiceCounters() throws InterruptedException {
    dlockStats.incServices(5);
    dlockStats.incGrantors(2);
    dlockStats.incRequestQueues(10);
    long lockWaitStarts = dlockStats.startLockWait();

    sample();
    
    int lockWaitinProgress = getLockWaitsInProgress();
    int lockRequestQueue = getLockRequestQueues();

    int numLockService = getTotalNumberOfLockService() ;
    int lockGrantors = getTotalNumberOfGrantors() ;
    assertEquals(1, lockWaitinProgress);
    assertEquals(2,lockGrantors);
    assertEquals(5,numLockService);
    assertEquals(10,lockRequestQueue);
    
    dlockStats.endLockWait(lockWaitStarts, true);
    
    sample();
    
    long lockWaitTime =  getTotalLockWaitTime() ;
    assertTrue(lockWaitTime >0);
  }

  @Test
  public void testTimeBasedCounters() throws InterruptedException {
    long startGIITime = cachePerfStats.startGetInitialImage();
    long startCacheListenerTime = cachePerfStats.startCacheListenerCall();
    long startCacheWriterTime = cachePerfStats.startCacheWriterCall();
    long startLoadTime = cachePerfStats.startLoad();
    
    cachePerfStats.endPutAll(testStartTime);
    
    long startGetsTime1 = cachePerfStats.startGet();
    long startGetsTime2 = cachePerfStats.startGet();
    
    long startNetLoadTime = cachePerfStats.startNetload();
    long startNetSearchTime = cachePerfStats.startNetsearch();
    
    sample();

    assertEquals(1, getInitialImagesInProgres());
    cachePerfStats.endGetInitialImage(startGIITime);
    cachePerfStats.endCacheListenerCall(startCacheListenerTime);
    cachePerfStats.endCacheWriterCall(startCacheWriterTime);
    cachePerfStats.endGet(startGetsTime1, true);
    cachePerfStats.endGet(startGetsTime2, false);
    cachePerfStats.endLoad(startLoadTime);
    cachePerfStats.endNetload(startNetLoadTime);
    cachePerfStats.endNetsearch(startNetSearchTime);
    
    long startPutsTime = cachePerfStats.endPut(testStartTime, false);
    
    cachePerfStats.txSuccess(CachePerfStats.getStatTime() - testStartTime, testStartTime,
        1);
    cachePerfStats.txRollback(CachePerfStats.getStatTime() - testStartTime, testStartTime,
        1);
    
    sample();
    
    assertEquals(1, getTotalNetLoadsCompleted());
    assertEquals(1, getTotalNetSearchCompleted());
    
    long netLoadAverageLatency = getNetLoadsAverageLatency();
    print("netLoadAverageLatency is " + new Long(netLoadAverageLatency).toString());
    
    long netSearchAverageLatency = getNetSearchAverageLatency();
    
    print("netSearchAverageLatency is " + new Long(netSearchAverageLatency).toString());
    
    assertTrue(netLoadAverageLatency > 0);
    assertTrue(netSearchAverageLatency > 0);
    assertEquals(1, getTotalHitCount()); // Gets minus Misses
    assertTrue(getInitialImageTime() > 0);

    long cacheListenerLatency = getCacheListenerCallsAvgLatency();
    assertTrue(cacheListenerLatency > 0);
        
    long cacheWriterLatency = getCacheWriterCallsAvgLatency();
    assertTrue(cacheWriterLatency > 0);
        
    float putAllRate = getPutAllRate();
    long putAllAverageLatency = getPutAllAvgLatency();
    assertTrue(putAllAverageLatency > 0);
        
    print("putAllRate is " + new Float(putAllRate).toString());
    assertTrue(putAllRate > 0);
    
    float getsRate = getGetsRate();
    long getsAverageLatency =  getGetsAvgLatency();
    assertTrue(getsAverageLatency > 0);
        
    print("getsRate is " + new Float(getsRate).toString());
    assertTrue(getsRate > 0);
    
    assertEquals(1, getTotalMissCount());
    
    float putsRate = getPutsRate();
    long putsAverageLatency = getPutsAvgLatency();
    
    assertTrue(putsAverageLatency > 0);
    
    print("putsRate is " + new Float(putsRate).toString());
    assertTrue(putsRate > 0);
    
    assertEquals(1,  getTotalLoadsCompleted());
    long loadAverageLatency = getLoadsAverageLatency();
    assertTrue(loadAverageLatency > 0);
        
    assertEquals(1,getTransactionCommittedTotalCount());  
    
    assertEquals(1, getTransactionRolledBackTotalCount());
    assertEquals(2,getTotalTransactionsCount());
    
    float txnCommitRates = getTransactionCommitsRate();
    
    print("txnCommitRates is " + new Float(txnCommitRates).toString());
    assertTrue(txnCommitRates > 0);
    
    long txnCommitAverageLatency = getTransactionCommitsAvgLatency();
    assertTrue(txnCommitAverageLatency > 0);
  }

  @Test
  public void testRates() throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      cachePerfStats.incCreates();
      cachePerfStats.incDestroys();
      
      funcServiceStats.incFunctionExecutionsCompleted();
      distributionStats.incSentBytes(20);
      distributionStats.incReceivedBytes(20);
    }

    sample();

    float createsRate = getCreatesRate();
    float destroyRate = getDestroysRate();
    float functionExecutionRate = getFunctionExecutionRate();
    float byteSentRate = getBytesSentRate();
    float byteReceivedRate = getBytesReceivedRate();

    print("createsRate is " + new Float(createsRate).toString());
    print("destroyRate is " + new Float(destroyRate).toString());
    print("functionExecutionRate is " + new Float(functionExecutionRate).toString());
    print("byteSentRate is " + new Float(byteSentRate).toString());
    print("byteReceivedRate is " + new Float(byteReceivedRate).toString());
    
    assertTrue(createsRate > 0);
    assertTrue(destroyRate > 0);
    assertTrue(functionExecutionRate > 0);
    assertTrue(byteSentRate > 0);
    assertTrue(byteReceivedRate > 0);
  }
  
  @Test
  public void testDistributionStats() throws InterruptedException {
    
    long initTime = System.currentTimeMillis();
    
    long startTime =  distributionStats.startReplyWait();
    
    distributionStats.incNodes(20);

    sample();
    
    assertEquals(1 , bridge.getReplyWaitsInProgress());
    assertEquals(20 , bridge.getVisibleNodes());

    distributionStats.endReplyWait(startTime, initTime);
    sample();
    assertEquals(0 , bridge.getReplyWaitsInProgress());
    assertEquals(1 , bridge.getReplyWaitsCompleted());

  }

  @Test
  public void testDiskCounters() throws InterruptedException {
    for(DiskStoreStats stats : diskStatsList){
      stats.startRead();
      stats.startWrite();
      stats.startBackup();
      stats.incWrittenBytes(200, false);
      stats.startFlush();
    }
    
    sample();
    
    assertEquals(4, getTotalBackupInProgress());

    assertTrue(getDiskWritesRate()>0);
    
    for(DiskStoreStats stats : diskStatsList){
      stats.endRead(testStartTime,20);
      stats.endWrite(testStartTime);
      stats.endBackup();
      stats.endFlush(testStartTime);
    }
    
    sample();
    
    assertEquals(4, getTotalBackupCompleted());
    assertTrue(getDiskFlushAvgLatency()>0);
    assertTrue(getDiskReadsRate()>0);

    
    for(DiskStoreStats stats : diskStatsList){
      bridge.removeDiskStoreStats(stats);
      stats.close();
    }
    
    assertEquals(4, getTotalBackupCompleted());
  }
  
  @Test
  public void testRegionCounters() throws InterruptedException {
    for(PartitionedRegionStats stats : parRegionStatsList){
      stats.incBucketCount(1);
      stats.incPrimaryBucketCount(1);
      stats.incDataStoreEntryCount(1);
    }
    
    sample();
    
    assertEquals(4, getTotalBucketCount());
    assertEquals(4, getTotalBucketSize());
    assertEquals(4, getTotalPrimaryBucketCount());

    
    for(PartitionedRegionStats stats : parRegionStatsList){
      bridge.removePartionRegionStats(stats);
      stats.close();
    }
    
    sample();
    
    assertEquals(0, getTotalBucketCount());
    assertEquals(0, getTotalBucketSize());
    assertEquals(0, getTotalPrimaryBucketCount());
  }
  
  @Test
  public void testCacheBasedStats() throws Exception{
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    ManagementService service = ManagementService.getExistingManagementService(cache);
    long start = cache.getResourceManager().getStats().startRebalance();
    assertEquals(1 , service.getMemberMXBean().getRebalancesInProgress());
    cache.getResourceManager().getStats().endRebalance(start);
    assertEquals(0 , service.getMemberMXBean().getRebalancesInProgress());
  }
  
  public void print(String message) {
    system.getLogWriter().fine(message);
  }
  
  public int getTotalBucketSize() {
    return bridge.getTotalBucketSize();
  }

  private int getInitialImageKeysReceived() {
    return bridge.getInitialImageKeysReceived();
  }

  private long getInitialImageTime() {
    return bridge.getInitialImageTime();
  }

  private int getInitialImagesInProgres() {
    return bridge.getInitialImagesInProgres();
  }

  private float getBytesReceivedRate() {
    return bridge.getBytesReceivedRate();
  }

  private float getBytesSentRate() {
    return bridge.getBytesSentRate();
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

  private float getDestroysRate() {
    return bridge.getDestroysRate();
  }

  private float getDiskReadsRate() {
    return bridge.getDiskReadsRate();
  }

  private float getDiskWritesRate() {
    return bridge.getDiskWritesRate();
  }

  private int getTotalBackupInProgress() {
    return bridge.getTotalBackupInProgress();
  }

  private int getTotalBackupCompleted() {
    return bridge.getTotalBackupCompleted();
  }

  private long getDiskFlushAvgLatency() {
    return bridge.getDiskFlushAvgLatency();
  }

  private float getFunctionExecutionRate() {
    return bridge.getFunctionExecutionRate();
  }

  private long getGetsAvgLatency() {
    return bridge.getGetsAvgLatency();
  }

  private float getGetsRate() {
    return bridge.getGetsRate();
  }

  private int getLockWaitsInProgress() {
    return bridge.getLockWaitsInProgress();
  }

  private int getNumRunningFunctions() {
    return bridge.getNumRunningFunctions();
  }

  private int getNumRunningFunctionsHavingResults() {
    return bridge.getNumRunningFunctionsHavingResults();
  }

  private long getPutAllAvgLatency() {
    return bridge.getPutAllAvgLatency();
  }

  private float getPutAllRate() {
    return bridge.getPutAllRate();
  }

  private long getPutsAvgLatency() {
    return bridge.getPutAllAvgLatency();
  }

  private float getPutsRate() {
    return bridge.getPutsRate();
  }

  
  private int getTotalPrimaryBucketCount() {
    return bridge.getTotalPrimaryBucketCount();
  }

  private int getTotalBucketCount() {
    return bridge.getTotalBucketCount();
  }

  private int getTotalHitCount() {
    return bridge.getTotalHitCount();
  }

  private float getLruDestroyRate() {
    return bridge.getLruDestroyRate();
  }

  private float getLruEvictionRate() {
    return bridge.getLruEvictionRate();
  }

  private int getTotalLoadsCompleted() {
    return bridge.getTotalLoadsCompleted();
  }

  private long getLoadsAverageLatency() {
    return bridge.getLoadsAverageLatency();
  }

  private int getTotalNetLoadsCompleted() {
    return bridge.getTotalNetLoadsCompleted();
  }

  private long getNetLoadsAverageLatency() {
    return bridge.getNetLoadsAverageLatency();
  }

  private int getTotalNetSearchCompleted() {
    return bridge.getTotalNetSearchCompleted();
  }

  private long getNetSearchAverageLatency() {
    return bridge.getNetSearchAverageLatency();
  }
  

  private int getTotalMissCount() {
    return bridge.getTotalMissCount();
  }

  private int getLockRequestQueues() {
    return bridge.getLockRequestQueues();
  }
  
  private long getTotalLockWaitTime() {
    return bridge.getTotalLockWaitTime();
  }

  private int getTotalNumberOfLockService() {
    return bridge.getTotalNumberOfLockService();
  }

  private int getTotalNumberOfGrantors() {
    return bridge.getTotalNumberOfGrantors();
  }

  private int getTotalDiskTasksWaiting() {
    return bridge.getTotalDiskTasksWaiting();
  }

  private int getTotalRegionCount() {
    return bridge.getTotalRegionCount();
  }

  private int getTotalRegionEntryCount() {
    return bridge.getTotalRegionEntryCount();
  }

  private int getTotalTransactionsCount() {
    return bridge.getTotalTransactionsCount();
  }

  private long getTransactionCommitsAvgLatency() {
    return bridge.getTransactionCommitsAvgLatency();
  }

  private float getTransactionCommitsRate() {
    return bridge.getTransactionCommitsRate();
  }

  private int getTransactionCommittedTotalCount() {
    return bridge.getTransactionCommittedTotalCount();
  }

  private int getTransactionRolledBackTotalCount() {
    return bridge.getTransactionRolledBackTotalCount();
  }
}
