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
public class AggregateRegionStatsMonitorTest {
  @Rule
  public TestName testName = new TestName();

  private AggregateRegionStatsMonitor aggregateRegionStatsMonitor;

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
    aggregateRegionStatsMonitor =
        spy(new AggregateRegionStatsMonitor(this.testName.getMethodName()));

    assertThat(aggregateRegionStatsMonitor).isNotNull();
    assertThat(aggregateRegionStatsMonitor.getMonitors()).isEmpty();
    assertThat(aggregateRegionStatsMonitor.getListeners()).isEmpty();
    assertThat(aggregateRegionStatsMonitor.getDiskSpace()).isEqualTo(0);
    assertThat(aggregateRegionStatsMonitor.getTotalBucketCount()).isEqualTo(0);
    assertThat(aggregateRegionStatsMonitor.getLruDestroys()).isEqualTo(0);
    assertThat(aggregateRegionStatsMonitor.getLruEvictions()).isEqualTo(0);
    assertThat(aggregateRegionStatsMonitor.getTotalBucketSize()).isEqualTo(0);
    assertThat(aggregateRegionStatsMonitor.getTotalPrimaryBucketCount()).isEqualTo(0);
  }

  @Test
  public void computeDeltaShouldReturnZeroForUnknownStatistics() {
    assertThat(
        aggregateRegionStatsMonitor.computeDelta(Collections.emptyMap(), "unknownStatistic", 6))
            .isEqualTo(0);
  }

  @Test
  public void computeDeltaShouldOperateForHandledStatistics() {
    Map<String, Number> statsMap = new HashMap<>();
    statsMap.put(StatsKey.PRIMARY_BUCKET_COUNT, 5);
    statsMap.put(StatsKey.BUCKET_COUNT, 13);
    statsMap.put(StatsKey.TOTAL_BUCKET_SIZE, 1024);
    statsMap.put(StatsKey.LRU_EVICTIONS, 12);
    statsMap.put(StatsKey.LRU_DESTROYS, 5);
    statsMap.put(StatsKey.DISK_SPACE, 2048);

    assertThat(aggregateRegionStatsMonitor.computeDelta(statsMap, StatsKey.PRIMARY_BUCKET_COUNT, 6))
        .isEqualTo(1);
    assertThat(aggregateRegionStatsMonitor.computeDelta(statsMap, StatsKey.BUCKET_COUNT, 15))
        .isEqualTo(2);
    assertThat(aggregateRegionStatsMonitor.computeDelta(statsMap, StatsKey.TOTAL_BUCKET_SIZE, 1030))
        .isEqualTo(6);
    assertThat(aggregateRegionStatsMonitor.computeDelta(statsMap, StatsKey.LRU_EVICTIONS, 20))
        .isEqualTo(8L);
    assertThat(aggregateRegionStatsMonitor.computeDelta(statsMap, StatsKey.LRU_DESTROYS, 6))
        .isEqualTo(1L);
    assertThat(aggregateRegionStatsMonitor.computeDelta(statsMap, StatsKey.DISK_SPACE, 2050))
        .isEqualTo(2L);
  }

  @Test
  public void increaseStatsShouldIncrementStatisticsUsingTheSelectedValue() {
    aggregateRegionStatsMonitor.increaseStats(StatsKey.PRIMARY_BUCKET_COUNT, 5);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.BUCKET_COUNT, 13);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.TOTAL_BUCKET_SIZE, 1024);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.LRU_EVICTIONS, 12);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.LRU_DESTROYS, 5);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.DISK_SPACE, 2048);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.PRIMARY_BUCKET_COUNT))
        .isEqualTo(5);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.BUCKET_COUNT)).isEqualTo(13);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.TOTAL_BUCKET_SIZE))
        .isEqualTo(1024);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.LRU_EVICTIONS)).isEqualTo(12L);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.LRU_DESTROYS)).isEqualTo(5L);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.DISK_SPACE)).isEqualTo(2048L);

    aggregateRegionStatsMonitor.increaseStats(StatsKey.PRIMARY_BUCKET_COUNT, 2);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.BUCKET_COUNT, 2);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.TOTAL_BUCKET_SIZE, 1);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.LRU_EVICTIONS, 8);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.LRU_DESTROYS, 5);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.DISK_SPACE, 2);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.PRIMARY_BUCKET_COUNT))
        .isEqualTo(7);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.BUCKET_COUNT)).isEqualTo(15);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.TOTAL_BUCKET_SIZE))
        .isEqualTo(1025);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.LRU_EVICTIONS)).isEqualTo(20L);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.LRU_DESTROYS)).isEqualTo(10L);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.DISK_SPACE)).isEqualTo(2050L);
  }

  @Test
  public void removeLRUStatisticsShouldRemoveListenerAndMonitor() {
    Statistics statistics = mock(Statistics.class);
    ValueMonitor regionMonitor = mock(ValueMonitor.class);
    AggregateRegionStatsMonitor.MemberLevelRegionStatisticsListener listener =
        mock(AggregateRegionStatsMonitor.MemberLevelRegionStatisticsListener.class);
    aggregateRegionStatsMonitor.getListeners().put(statistics, listener);
    aggregateRegionStatsMonitor.getMonitors().put(statistics, regionMonitor);

    aggregateRegionStatsMonitor.removeLRUStatistics(statistics);
    assertThat(aggregateRegionStatsMonitor.getListeners()).isEmpty();
    assertThat(aggregateRegionStatsMonitor.getMonitors()).isEmpty();
    verify(regionMonitor, times(1)).removeListener(listener);
    verify(regionMonitor, times(1)).removeStatistics(statistics);
  }

  @Test
  public void removeDirectoryStatisticsShouldRemoveListenerAndMonitor() {
    Statistics statistics = mock(Statistics.class);
    ValueMonitor regionMonitor = mock(ValueMonitor.class);
    AggregateRegionStatsMonitor.MemberLevelRegionStatisticsListener listener =
        mock(AggregateRegionStatsMonitor.MemberLevelRegionStatisticsListener.class);
    aggregateRegionStatsMonitor.getListeners().put(statistics, listener);
    aggregateRegionStatsMonitor.getMonitors().put(statistics, regionMonitor);

    aggregateRegionStatsMonitor.removeDirectoryStatistics(statistics);
    assertThat(aggregateRegionStatsMonitor.getListeners()).isEmpty();
    assertThat(aggregateRegionStatsMonitor.getMonitors()).isEmpty();
    verify(regionMonitor, times(1)).removeListener(listener);
    verify(regionMonitor, times(1)).removeStatistics(statistics);
  }

  @Test
  public void removePartitionStatisticsShouldDecreaseStatsAndRemoveBothListenerAndMonitor() {
    Statistics statistics = mock(Statistics.class);
    ValueMonitor regionMonitor = mock(ValueMonitor.class);
    AggregateRegionStatsMonitor.MemberLevelRegionStatisticsListener listener =
        mock(AggregateRegionStatsMonitor.MemberLevelRegionStatisticsListener.class);
    aggregateRegionStatsMonitor.getListeners().put(statistics, listener);
    aggregateRegionStatsMonitor.getMonitors().put(statistics, regionMonitor);

    aggregateRegionStatsMonitor.removePartitionStatistics(statistics);
    assertThat(aggregateRegionStatsMonitor.getListeners()).isEmpty();
    assertThat(aggregateRegionStatsMonitor.getMonitors()).isEmpty();
    verify(listener, times(1)).decreaseParStats();
    verify(regionMonitor, times(1)).removeListener(listener);
    verify(regionMonitor, times(1)).removeStatistics(statistics);
  }

  @Test
  public void getStatisticShouldReturnZeroForUnknownStatistics() {
    assertThat(aggregateRegionStatsMonitor.getStatistic("unhandledStatistic")).isEqualTo(0);
  }

  @Test
  public void getStatisticShouldReturnTheRecordedValueForHandledStatistics() {
    aggregateRegionStatsMonitor.increaseStats(StatsKey.PRIMARY_BUCKET_COUNT, 5);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.BUCKET_COUNT, 13);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.TOTAL_BUCKET_SIZE, 1024);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.LRU_EVICTIONS, 12);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.LRU_DESTROYS, 5);
    aggregateRegionStatsMonitor.increaseStats(StatsKey.DISK_SPACE, 2048);

    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.PRIMARY_BUCKET_COUNT))
        .isEqualTo(5);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.BUCKET_COUNT)).isEqualTo(13);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.TOTAL_BUCKET_SIZE))
        .isEqualTo(1024);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.LRU_EVICTIONS)).isEqualTo(12L);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.LRU_DESTROYS)).isEqualTo(5L);
    assertThat(aggregateRegionStatsMonitor.getStatistic(StatsKey.DISK_SPACE)).isEqualTo(2048L);
  }

  @Test
  public void addStatisticsToMonitorShouldAddListenerAndMonitor() {
    Statistics statistics = mock(Statistics.class);
    aggregateRegionStatsMonitor.addStatisticsToMonitor(statistics);

    assertThat(aggregateRegionStatsMonitor.getMonitors().size()).isEqualTo(1);
    assertThat(aggregateRegionStatsMonitor.getListeners().size()).isEqualTo(1);
  }

  @Test
  public void stopListenerShouldRemoveListenerAndMonitor() {
    Statistics statistics = mock(Statistics.class);
    ValueMonitor regionMonitor = mock(ValueMonitor.class);
    AggregateRegionStatsMonitor.MemberLevelRegionStatisticsListener listener =
        mock(AggregateRegionStatsMonitor.MemberLevelRegionStatisticsListener.class);
    aggregateRegionStatsMonitor.getListeners().put(statistics, listener);
    aggregateRegionStatsMonitor.getMonitors().put(statistics, regionMonitor);

    aggregateRegionStatsMonitor.stopListener();
    verify(regionMonitor, times(1)).removeListener(listener);
    verify(regionMonitor, times(1)).removeStatistics(statistics);
    assertThat(aggregateRegionStatsMonitor.getMonitors()).isEmpty();
    assertThat(aggregateRegionStatsMonitor.getListeners()).isEmpty();
  }

  @Test
  public void decreaseDiskStoreStatsShouldNotThrowNPE() {
    Statistics statistics = mock(Statistics.class);
    aggregateRegionStatsMonitor.addStatisticsToMonitor(statistics);
    aggregateRegionStatsMonitor.getListeners().values().forEach((l) -> l.decreaseParStats());
  }
}
