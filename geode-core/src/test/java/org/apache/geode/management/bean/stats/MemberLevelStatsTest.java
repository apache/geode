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

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.apache.geode.internal.statistics.SuppliableStatistics.toSuppliableStatistics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.CancelCriterion;
import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockStats;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.DiskStoreStats;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.internal.cache.execute.metrics.FunctionServiceStats;
import org.apache.geode.internal.statistics.GemFireStatSampler;
import org.apache.geode.internal.statistics.StatSamplerStats;
import org.apache.geode.internal.statistics.StatisticsConfig;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.internal.statistics.StatisticsRegistry;
import org.apache.geode.internal.stats50.VMStats50;
import org.apache.geode.logging.internal.spi.LogFile;
import org.apache.geode.management.internal.beans.MemberMBeanBridge;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.categories.StatisticsTest;

@Category({JMXTest.class, StatisticsTest.class})
public class MemberLevelStatsTest {

  @Rule
  public TestName name = new TestName();

  private final AtomicLong clockTime = new AtomicLong(1000);

  private GemFireStatSampler statSampler;
  private MemberMBeanBridge memberMBeanBridge;
  private CachePerfStats cachePerfStats;
  private FunctionServiceStats funcServiceStats;
  private DistributionStats distributionStats;
  private DLockStats dlockStats;
  private StatisticsManager statisticsManager;

  private DiskStoreStats[] diskStoreStatsArray;
  private PartitionedRegionStats[] partitionedRegionStatsArray;

  @Before
  public void setUp() throws Exception {
    DistributionStats.enableClockStats = true;

    statisticsManager = new StatisticsRegistry("TestStatisticsRegistry", 1);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    StatisticsConfig statisticsConfig = mock(StatisticsConfig.class);
    StatSamplerStats statSamplerStats =
        new StatSamplerStats(statisticsManager, statisticsManager.getPid());

    statSampler = new GemFireStatSampler(mock(CancelCriterion.class), statSamplerStats,
        mock(LogFile.class), statisticsConfig, statisticsManager,
        mock(DistributionManager.class), 1);

    when(system.getStatSampler()).thenReturn(statSampler);
    when(statisticsConfig.getStatisticSampleRate()).thenReturn(60000);
    when(statisticsConfig.getStatisticArchiveFile()).thenReturn(new File(""));

    statSampler.start();

    cachePerfStats = new CachePerfStats(statisticsManager, clockTime::get);
    funcServiceStats = new FunctionServiceStats(statisticsManager, "FunctionExecution",
        clockTime::get);
    distributionStats =
        new DistributionStats(statisticsManager, "distributionStats", statisticsManager
            .getPid(), clockTime::get);
    dlockStats = new DLockStats(statisticsManager, "dlockStats", statisticsManager.getPid(),
        clockTime::get);

    memberMBeanBridge = new MemberMBeanBridge(system, statisticsManager);
    memberMBeanBridge.addCacheStats(cachePerfStats);
    memberMBeanBridge.addFunctionStats(funcServiceStats);
    memberMBeanBridge.addDistributionStats(distributionStats);
    memberMBeanBridge.addLockServiceStats(dlockStats);
    memberMBeanBridge.addProcessStats(statSampler.getProcessStats());
    memberMBeanBridge.addStatSamplerStats(statSamplerStats);

    diskStoreStatsArray = new DiskStoreStats[4];
    for (int i = 0; i < diskStoreStatsArray.length; i++) {
      DiskStoreStats stats = new DiskStoreStats(statisticsManager, name.getMethodName() + i,
          clockTime::get);
      diskStoreStatsArray[i] = stats;
      memberMBeanBridge.addDiskStoreStats(stats);
    }

    partitionedRegionStatsArray = new PartitionedRegionStats[4];
    for (int i = 0; i < 4; i++) {
      PartitionedRegionStats stats = new PartitionedRegionStats(
          statisticsManager, name.getMethodName() + i, disabledClock());
      partitionedRegionStatsArray[i] = stats;
      memberMBeanBridge.addPartitionedRegionStats(stats);
    }

    sampleStats();
    tickClock();
  }

  @After
  public void tearDown() throws Exception {
    DistributionStats.enableClockStats = true;
    statSampler.stop();
  }

  @Test
  public void testPlainCounters() {
    cachePerfStats.incDiskTasksWaiting();
    cachePerfStats.incEntryCount(400);
    cachePerfStats.incGetInitialImageKeysReceived();
    cachePerfStats.incRegions(3);

    funcServiceStats.incFunctionExecutionHasResultRunning();
    funcServiceStats.incFunctionExecutionsRunning();

    sampleStats();
    tickClock();

    assertThat(memberMBeanBridge.getInitialImageKeysReceived()).isEqualTo(1);
    assertThat(memberMBeanBridge.getNumRunningFunctions()).isEqualTo(1);
    assertThat(memberMBeanBridge.getNumRunningFunctionsHavingResults()).isEqualTo(1);
    assertThat(memberMBeanBridge.getTotalDiskTasksWaiting()).isEqualTo(1);
    assertThat(memberMBeanBridge.getTotalRegionCount()).isEqualTo(3);
    assertThat(memberMBeanBridge.getTotalRegionEntryCount()).isEqualTo(400);
  }

  @Test
  public void testLockServiceCounters() {
    dlockStats.incServices(5);
    dlockStats.incGrantors(2);
    dlockStats.incRequestQueues(10);
    long startLockWait = dlockStats.startLockWait();

    sampleStats();
    tickClock();

    assertThat(memberMBeanBridge.getLockRequestQueues()).isEqualTo(10);
    assertThat(memberMBeanBridge.getLockWaitsInProgress()).isEqualTo(1);
    assertThat(memberMBeanBridge.getTotalNumberOfGrantors()).isEqualTo(2);
    assertThat(memberMBeanBridge.getTotalNumberOfLockService()).isEqualTo(5);

    dlockStats.endLockWait(startLockWait, true);

    sampleStats();

    assertThat(memberMBeanBridge.getTotalLockWaitTime()).isEqualTo(1000);
  }

  @Test
  public void testTimeBasedCounters() {
    long startCacheListenerCall = cachePerfStats.startCacheListenerCall();
    long startCacheWriterCall = cachePerfStats.startCacheWriterCall();
    long startGetHit = cachePerfStats.startGet();
    long startGetMiss = cachePerfStats.startGet();
    long startGetInitialImage = cachePerfStats.startGetInitialImage();
    long startLoad = cachePerfStats.startLoad();
    long startNetload = cachePerfStats.startNetload();
    long startNetsearch = cachePerfStats.startNetsearch();

    cachePerfStats.endPutAll(clockTime.get());

    sampleStats();
    tickClock();

    assertThat(memberMBeanBridge.getInitialImagesInProgress()).isEqualTo(1);

    cachePerfStats.endCacheListenerCall(startCacheListenerCall);
    cachePerfStats.endCacheWriterCall(startCacheWriterCall);
    cachePerfStats.endGet(startGetHit, false);
    cachePerfStats.endGet(startGetMiss, true);
    cachePerfStats.endGetInitialImage(startGetInitialImage);
    cachePerfStats.endLoad(startLoad);
    cachePerfStats.endNetload(startNetload);
    cachePerfStats.endNetsearch(startNetsearch);
    cachePerfStats.endPut(clockTime.get(), false);
    cachePerfStats.txRollback(clockTime.get(), 0, 1);
    cachePerfStats.txSuccess(clockTime.get(), 0, 1);

    cachePerfStats.endPutAll(clockTime.get());

    sampleStats();
    tickClock();

    assertThat(memberMBeanBridge.getCacheListenerCallsAvgLatency()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getCacheWriterCallsAvgLatency()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getGetsAvgLatency()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getGetsRate()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getInitialImageTime()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getLoadsAverageLatency()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getNetLoadsAverageLatency()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getNetSearchAverageLatency()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getPutAllAvgLatency()).isEqualTo(0); // TODO
    assertThat(memberMBeanBridge.getPutAllRate()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getPutsRate()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getTotalHitCount()).isEqualTo(1);
    assertThat(memberMBeanBridge.getTotalLoadsCompleted()).isEqualTo(1);
    assertThat(memberMBeanBridge.getTotalMissCount()).isEqualTo(1);
    assertThat(memberMBeanBridge.getTotalNetLoadsCompleted()).isEqualTo(1);
    assertThat(memberMBeanBridge.getTotalNetSearchCompleted()).isEqualTo(1);
    assertThat(memberMBeanBridge.getTotalTransactionsCount()).isEqualTo(2);
    assertThat(memberMBeanBridge.getTransactionCommitsAvgLatency()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getTransactionCommitsRate()).isGreaterThan(0);
    assertThat(memberMBeanBridge.getTransactionCommittedTotalCount()).isEqualTo(1);
    assertThat(memberMBeanBridge.getTransactionRolledBackTotalCount()).isEqualTo(1);
  }

  @Test
  public void testRates() {
    for (int i = 0; i < 20; i++) {
      cachePerfStats.incCreates();
      cachePerfStats.incDestroys();

      distributionStats.incSentBytes(20);
      distributionStats.incReceivedBytes(20);

      funcServiceStats.incFunctionExecutionsCompleted();
    }

    sampleStats();
    tickClock();

    assertThat(memberMBeanBridge.getBytesReceivedRate()).isEqualTo(400);
    assertThat(memberMBeanBridge.getBytesSentRate()).isEqualTo(400);
    assertThat(memberMBeanBridge.getCreatesRate()).isEqualTo(20);
    assertThat(memberMBeanBridge.getDestroysRate()).isEqualTo(20);
    assertThat(memberMBeanBridge.getFunctionExecutionRate()).isEqualTo(20);
  }

  @Test
  public void testDistributionStats() {
    distributionStats.incNodes(20);
    long startReplyWait = distributionStats.startReplyWait();

    sampleStats();
    tickClock();

    assertThat(memberMBeanBridge.getReplyWaitsInProgress()).isEqualTo(1);
    assertThat(memberMBeanBridge.getVisibleNodes()).isEqualTo(20);

    distributionStats.endReplyWait(startReplyWait, 1000);

    sampleStats();
    tickClock();

    assertThat(memberMBeanBridge.getReplyWaitsCompleted()).isEqualTo(1);
    assertThat(memberMBeanBridge.getReplyWaitsInProgress()).isZero();
  }

  @Test
  public void testDiskCounters() {
    long[] startReadArray = new long[diskStoreStatsArray.length];
    long[] startWriteArray = new long[diskStoreStatsArray.length];
    long[] startFlushArray = new long[diskStoreStatsArray.length];

    for (int i = 0; i < diskStoreStatsArray.length; i++) {
      DiskStoreStats diskStoreStats = diskStoreStatsArray[i];
      diskStoreStats.startBackup();
      diskStoreStats.incWrittenBytes(200, false);
      startReadArray[i] = diskStoreStats.startRead();
      startWriteArray[i] = diskStoreStats.startWrite();
      startFlushArray[i] = diskStoreStats.startFlush();
    }

    sampleStats();
    tickClock();

    assertThat(memberMBeanBridge.getTotalBackupInProgress()).isEqualTo(4);
    assertThat(memberMBeanBridge.getDiskWritesRate()).isEqualTo(800);

    for (int i = 0; i < diskStoreStatsArray.length; i++) {
      DiskStoreStats diskStoreStats = diskStoreStatsArray[i];
      diskStoreStats.endBackup();
      diskStoreStats.endRead(startReadArray[i], 20);
      diskStoreStats.endWrite(startWriteArray[i]);
      diskStoreStats.endFlush(startFlushArray[i]);
    }

    sampleStats();
    tickClock();

    assertThat(memberMBeanBridge.getDiskFlushAvgLatency()).isEqualTo(1000);
    assertThat(memberMBeanBridge.getDiskReadsRate()).isEqualTo(80);
    assertThat(memberMBeanBridge.getTotalBackupCompleted()).isEqualTo(4);
  }

  @Test
  public void testRegionCounters() {
    for (PartitionedRegionStats stats : partitionedRegionStatsArray) {
      stats.incBucketCount(1);
      stats.incPrimaryBucketCount(1);
      stats.incDataStoreEntryCount(1);
    }

    sampleStats();
    tickClock();

    assertThat(memberMBeanBridge.getTotalBucketCount()).isEqualTo(4);
    assertThat(memberMBeanBridge.getTotalBucketSize()).isEqualTo(4);
    assertThat(memberMBeanBridge.getTotalPrimaryBucketCount()).isEqualTo(4);

    for (PartitionedRegionStats stats : partitionedRegionStatsArray) {
      memberMBeanBridge.removePartitionedRegionStats(stats);
      stats.close();
    }

    sampleStats();
    tickClock();

    assertThat(memberMBeanBridge.getTotalBucketCount()).isZero();
    assertThat(memberMBeanBridge.getTotalBucketSize()).isZero();
    assertThat(memberMBeanBridge.getTotalPrimaryBucketCount()).isZero();
  }

  @Test
  public void testVMStats() {
    Statistics[] realStats = statisticsManager.findStatisticsByType(VMStats50.getGCType());
    long[] totals = modifyStatsAndReturnTotalCountAndTime(10, 2500, realStats);
    memberMBeanBridge.addVMStats(statSampler.getVMStats());
    assertThat(memberMBeanBridge.getGarbageCollectionCount()).isEqualTo(totals[0]);
    assertThat(memberMBeanBridge.getGarbageCollectionTime()).isEqualTo(totals[1]);

    long[] newTotals = modifyStatsAndReturnTotalCountAndTime(20, 3500, realStats);
    sampleStats();
    assertThat(memberMBeanBridge.getGarbageCollectionCount()).isEqualTo(newTotals[0]);
    assertThat(memberMBeanBridge.getGarbageCollectionTime()).isEqualTo(newTotals[1]);
  }

  private long[] modifyStatsAndReturnTotalCountAndTime(
      long baseCount, long baseTime,
      Statistics[] modifiedStats) {
    long[] totalCountAndTime = {0, 0};
    for (Statistics gcStat : modifiedStats) {
      StatisticDescriptor[] statistics = gcStat.getType().getStatistics();
      for (StatisticDescriptor d : statistics) {
        if ("collections".equals(d.getName())) {
          baseCount += 1;
          gcStat.setLong(d, baseCount);
          totalCountAndTime[0] += baseCount;
        } else if ("collectionTime".equals(d.getName())) {
          baseTime += 100;
          gcStat.setLong(d, baseTime);
          totalCountAndTime[1] += baseTime;
        }
      }
    }
    return totalCountAndTime;
  }

  private void sampleStats() {
    toSuppliableStatistics(cachePerfStats.getStats()).updateSuppliedValues();
    statSampler.getSampleCollector().sample(clockTime.get());
  }

  private void tickClock() {
    clockTime.addAndGet(1000);
  }
}
