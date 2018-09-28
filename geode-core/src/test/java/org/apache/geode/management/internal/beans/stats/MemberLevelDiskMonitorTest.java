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
package org.apache.geode.management.internal.beans.stats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.Statistics;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.internal.statistics.SampleCollector;
import org.apache.geode.internal.statistics.StatArchiveHandlerConfig;
import org.apache.geode.internal.statistics.StatisticsSampler;
import org.apache.geode.internal.statistics.TestStatisticsManager;
import org.apache.geode.internal.statistics.TestStatisticsSampler;
import org.apache.geode.internal.statistics.ValueMonitor;
import org.apache.geode.test.junit.categories.StatisticsTest;

@Category(StatisticsTest.class)
public class MemberLevelDiskMonitorTest {
  @Rule
  public TestName testName = new TestName();

  private MemberLevelDiskMonitor memberLevelDiskMonitor;

  @Before
  public void setUp() {
    final long startTime = System.currentTimeMillis();
    TestStatisticsManager manager =
        new TestStatisticsManager(1, getClass().getSimpleName(), startTime);
    StatArchiveHandlerConfig mockStatArchiveHandlerConfig = mock(StatArchiveHandlerConfig.class,
        getClass().getSimpleName() + "$" + StatArchiveHandlerConfig.class.getSimpleName());
    when(mockStatArchiveHandlerConfig.getArchiveFileName()).thenReturn(new File(""));
    when(mockStatArchiveHandlerConfig.getArchiveFileSizeLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getArchiveDiskSpaceLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemId()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemStartTime()).thenReturn(startTime);
    when(mockStatArchiveHandlerConfig.getSystemDirectoryPath()).thenReturn("");
    when(mockStatArchiveHandlerConfig.getProductDescription())
        .thenReturn(getClass().getSimpleName());

    StatisticsSampler sampler = new TestStatisticsSampler(manager);
    SampleCollector sampleCollector = new SampleCollector(sampler);
    sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime(),
        new MainWithChildrenRollingFileHandler());
    memberLevelDiskMonitor = spy(new MemberLevelDiskMonitor(this.testName.getMethodName()));

    assertThat(memberLevelDiskMonitor).isNotNull();
    assertThat(memberLevelDiskMonitor.getMonitors()).isEmpty();
    assertThat(memberLevelDiskMonitor.getListeners()).isEmpty();
    assertThat(memberLevelDiskMonitor.getFlushes()).isEqualTo(0);
    assertThat(memberLevelDiskMonitor.getQueueSize()).isEqualTo(0);
    assertThat(memberLevelDiskMonitor.getFlushTime()).isEqualTo(0);
    assertThat(memberLevelDiskMonitor.getFlushedBytes()).isEqualTo(0);
    assertThat(memberLevelDiskMonitor.getDiskReadBytes()).isEqualTo(0);
    assertThat(memberLevelDiskMonitor.getBackupsCompleted()).isEqualTo(0);
    assertThat(memberLevelDiskMonitor.getDiskWrittenBytes()).isEqualTo(0);
    assertThat(memberLevelDiskMonitor.getBackupsInProgress()).isEqualTo(0);
  }

  @Test
  public void computeDeltaShouldReturnZeroForUnknownStatistics() {
    assertThat(memberLevelDiskMonitor.computeDelta(Collections.emptyMap(), "unknownStatistic", 6))
        .isEqualTo(0);
  }

  @Test
  public void computeDeltaShouldOperateForHandledStatistics() {
    Map<String, Number> statsMap = new HashMap<>();
    statsMap.put(StatsKey.NUM_FLUSHES, 10);
    statsMap.put(StatsKey.DISK_QUEUE_SIZE, 148);
    statsMap.put(StatsKey.TOTAL_FLUSH_TIME, 10000);
    statsMap.put(StatsKey.FLUSHED_BYTES, 2048);
    statsMap.put(StatsKey.DISK_READ_BYTES, 1024);
    statsMap.put(StatsKey.DISK_RECOVERED_BYTES, 512);
    statsMap.put(StatsKey.BACKUPS_COMPLETED, 5);
    statsMap.put(StatsKey.DISK_WRITEN_BYTES, 8192);
    statsMap.put(StatsKey.BACKUPS_IN_PROGRESS, 2);

    assertThat(memberLevelDiskMonitor.computeDelta(statsMap, StatsKey.NUM_FLUSHES, 16))
        .isEqualTo(6L);
    assertThat(memberLevelDiskMonitor.computeDelta(statsMap, StatsKey.DISK_QUEUE_SIZE, 150))
        .isEqualTo(2);
    assertThat(memberLevelDiskMonitor.computeDelta(statsMap, StatsKey.TOTAL_FLUSH_TIME, 10000))
        .isEqualTo(0L);
    assertThat(memberLevelDiskMonitor.computeDelta(statsMap, StatsKey.FLUSHED_BYTES, 3000))
        .isEqualTo(952L);
    assertThat(memberLevelDiskMonitor.computeDelta(statsMap, StatsKey.DISK_READ_BYTES, 2048))
        .isEqualTo(1024L);
    assertThat(memberLevelDiskMonitor.computeDelta(statsMap, StatsKey.DISK_RECOVERED_BYTES, 1024))
        .isEqualTo(512L);
    assertThat(memberLevelDiskMonitor.computeDelta(statsMap, StatsKey.BACKUPS_COMPLETED, 6))
        .isEqualTo(1);
    assertThat(memberLevelDiskMonitor.computeDelta(statsMap, StatsKey.DISK_WRITEN_BYTES, 8193))
        .isEqualTo(1L);
    assertThat(memberLevelDiskMonitor.computeDelta(statsMap, StatsKey.BACKUPS_IN_PROGRESS, 1))
        .isEqualTo(-1);
  }

  @Test
  public void increaseStatsShouldIncrementStatisticsUsingTheSelectedValue() {
    memberLevelDiskMonitor.increaseStats(StatsKey.NUM_FLUSHES, 5);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_QUEUE_SIZE, 13);
    memberLevelDiskMonitor.increaseStats(StatsKey.TOTAL_FLUSH_TIME, 1024);
    memberLevelDiskMonitor.increaseStats(StatsKey.FLUSHED_BYTES, 12);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_READ_BYTES, 5);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_RECOVERED_BYTES, 2048);
    memberLevelDiskMonitor.increaseStats(StatsKey.BACKUPS_COMPLETED, 20);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_WRITEN_BYTES, 51);
    memberLevelDiskMonitor.increaseStats(StatsKey.BACKUPS_IN_PROGRESS, 60);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.NUM_FLUSHES)).isEqualTo(5L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.DISK_QUEUE_SIZE)).isEqualTo(13);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.TOTAL_FLUSH_TIME)).isEqualTo(1024L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.FLUSHED_BYTES)).isEqualTo(12L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.DISK_READ_BYTES)).isEqualTo(2053L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.BACKUPS_COMPLETED)).isEqualTo(20);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.DISK_WRITEN_BYTES)).isEqualTo(51L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.BACKUPS_IN_PROGRESS)).isEqualTo(60);

    memberLevelDiskMonitor.increaseStats(StatsKey.NUM_FLUSHES, 2);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_QUEUE_SIZE, 2);
    memberLevelDiskMonitor.increaseStats(StatsKey.TOTAL_FLUSH_TIME, 1);
    memberLevelDiskMonitor.increaseStats(StatsKey.FLUSHED_BYTES, 8);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_READ_BYTES, 5);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_RECOVERED_BYTES, 2);
    memberLevelDiskMonitor.increaseStats(StatsKey.BACKUPS_COMPLETED, 1);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_WRITEN_BYTES, 11);
    memberLevelDiskMonitor.increaseStats(StatsKey.BACKUPS_IN_PROGRESS, 6);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.NUM_FLUSHES)).isEqualTo(7L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.DISK_QUEUE_SIZE)).isEqualTo(15);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.TOTAL_FLUSH_TIME)).isEqualTo(1025L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.FLUSHED_BYTES)).isEqualTo(20L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.DISK_READ_BYTES)).isEqualTo(2060L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.BACKUPS_COMPLETED)).isEqualTo(21);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.DISK_WRITEN_BYTES)).isEqualTo(62L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.BACKUPS_IN_PROGRESS)).isEqualTo(66);
  }

  @Test
  public void addStatisticsToMonitorShouldAddListenerAndMonitor() {
    Statistics statistics = mock(Statistics.class);
    memberLevelDiskMonitor.addStatisticsToMonitor(statistics);

    assertThat(memberLevelDiskMonitor.getMonitors().size()).isEqualTo(1);
    assertThat(memberLevelDiskMonitor.getListeners().size()).isEqualTo(1);
  }

  @Test
  public void stopListenerShouldRemoveListenerAndMonitor() {
    Statistics statistics = mock(Statistics.class);
    ValueMonitor regionMonitor = mock(ValueMonitor.class);
    MemberLevelDiskMonitor.MemberLevelDiskStatisticsListener listener =
        mock(MemberLevelDiskMonitor.MemberLevelDiskStatisticsListener.class);
    memberLevelDiskMonitor.getListeners().put(statistics, listener);
    memberLevelDiskMonitor.getMonitors().put(statistics, regionMonitor);

    memberLevelDiskMonitor.stopListener();
    verify(regionMonitor, times(1)).removeListener(listener);
    verify(regionMonitor, times(1)).removeStatistics(statistics);
    assertThat(memberLevelDiskMonitor.getMonitors()).isEmpty();
    assertThat(memberLevelDiskMonitor.getListeners()).isEmpty();
  }

  @Test
  public void getStatisticShouldReturnZeroForUnknownStatistics() {
    assertThat(memberLevelDiskMonitor.getStatistic("unhandledStatistic")).isEqualTo(0);
  }

  @Test
  public void getStatisticShouldReturnTheRecordedValueForHandledStatistics() {
    memberLevelDiskMonitor.increaseStats(StatsKey.NUM_FLUSHES, 5);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_QUEUE_SIZE, 13);
    memberLevelDiskMonitor.increaseStats(StatsKey.TOTAL_FLUSH_TIME, 1024);
    memberLevelDiskMonitor.increaseStats(StatsKey.FLUSHED_BYTES, 12);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_READ_BYTES, 5);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_RECOVERED_BYTES, 2048);
    memberLevelDiskMonitor.increaseStats(StatsKey.BACKUPS_COMPLETED, 20);
    memberLevelDiskMonitor.increaseStats(StatsKey.DISK_WRITEN_BYTES, 51);
    memberLevelDiskMonitor.increaseStats(StatsKey.BACKUPS_IN_PROGRESS, 60);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.NUM_FLUSHES)).isEqualTo(5L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.DISK_QUEUE_SIZE)).isEqualTo(13);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.TOTAL_FLUSH_TIME)).isEqualTo(1024L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.FLUSHED_BYTES)).isEqualTo(12L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.DISK_READ_BYTES)).isEqualTo(2053L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.BACKUPS_COMPLETED)).isEqualTo(20);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.DISK_WRITEN_BYTES)).isEqualTo(51L);
    assertThat(memberLevelDiskMonitor.getStatistic(StatsKey.BACKUPS_IN_PROGRESS)).isEqualTo(60);
  }

  @Test
  public void removeStatisticsFromMonitorShouldRemoveListenerAndMonitor() {
    Statistics statistics = mock(Statistics.class);
    ValueMonitor regionMonitor = mock(ValueMonitor.class);
    MemberLevelDiskMonitor.MemberLevelDiskStatisticsListener listener =
        mock(MemberLevelDiskMonitor.MemberLevelDiskStatisticsListener.class);
    memberLevelDiskMonitor.getListeners().put(statistics, listener);
    memberLevelDiskMonitor.getMonitors().put(statistics, regionMonitor);

    memberLevelDiskMonitor.removeStatisticsFromMonitor(statistics);
    assertThat(memberLevelDiskMonitor.getListeners()).isEmpty();
    assertThat(memberLevelDiskMonitor.getMonitors()).isEmpty();
    verify(listener, times(1)).decreaseDiskStoreStats();
    verify(regionMonitor, times(1)).removeListener(listener);
    verify(regionMonitor, times(1)).removeStatistics(statistics);
  }

  @Test
  public void decreaseDiskStoreStatsShouldNotThrowNPE() {
    Statistics statistics = mock(Statistics.class);
    memberLevelDiskMonitor.addStatisticsToMonitor(statistics);
    memberLevelDiskMonitor.getListeners().values().forEach((l) -> l.decreaseDiskStoreStats());

  }
}
