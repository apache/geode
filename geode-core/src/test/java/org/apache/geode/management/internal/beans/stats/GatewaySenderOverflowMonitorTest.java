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
public class GatewaySenderOverflowMonitorTest {
  @Rule
  public TestName testName = new TestName();

  private GatewaySenderOverflowMonitor gatewaySenderOverflowMonitor;

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
    gatewaySenderOverflowMonitor =
        spy(new GatewaySenderOverflowMonitor(this.testName.getMethodName()));

    assertThat(gatewaySenderOverflowMonitor).isNotNull();
    assertThat(gatewaySenderOverflowMonitor.getMonitors()).isEmpty();
    assertThat(gatewaySenderOverflowMonitor.getListeners()).isEmpty();
    assertThat(gatewaySenderOverflowMonitor.getLruEvictions()).isEqualTo(0);
    assertThat(gatewaySenderOverflowMonitor.getBytesOverflowedToDisk()).isEqualTo(0);
    assertThat(gatewaySenderOverflowMonitor.getEntriesOverflowedToDisk()).isEqualTo(0);
  }

  @Test
  public void computeDeltaShouldReturnZeroForUnknownStatistics() {
    assertThat(
        gatewaySenderOverflowMonitor.computeDelta(Collections.emptyMap(), "unknownStatistic", 6))
            .isEqualTo(0);
  }

  @Test
  public void computeDeltaShouldOperateForHandledStatistics() {
    Map<String, Number> statsMap = new HashMap<>();
    statsMap.put(StatsKey.GATEWAYSENDER_LRU_EVICTIONS, 50);
    statsMap.put(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK, 2048);
    statsMap.put(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK, 100);

    assertThat(gatewaySenderOverflowMonitor.computeDelta(statsMap,
        StatsKey.GATEWAYSENDER_LRU_EVICTIONS, 60)).isEqualTo(10L);
    assertThat(gatewaySenderOverflowMonitor.computeDelta(statsMap,
        StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK, 2100)).isEqualTo(52L);
    assertThat(gatewaySenderOverflowMonitor.computeDelta(statsMap,
        StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK, 150)).isEqualTo(50L);
  }

  @Test
  public void increaseStatsShouldIncrementStatisticsUsingTheSelectedValue() {
    gatewaySenderOverflowMonitor.increaseStats(StatsKey.GATEWAYSENDER_LRU_EVICTIONS, 5L);
    gatewaySenderOverflowMonitor.increaseStats(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK,
        1024L);
    gatewaySenderOverflowMonitor.increaseStats(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK,
        10000L);
    assertThat(gatewaySenderOverflowMonitor.getStatistic(StatsKey.GATEWAYSENDER_LRU_EVICTIONS))
        .isEqualTo(5L);
    assertThat(
        gatewaySenderOverflowMonitor.getStatistic(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK))
            .isEqualTo(1024L);
    assertThat(gatewaySenderOverflowMonitor
        .getStatistic(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK)).isEqualTo(10000L);

    gatewaySenderOverflowMonitor.increaseStats(StatsKey.GATEWAYSENDER_LRU_EVICTIONS, 5L);
    gatewaySenderOverflowMonitor.increaseStats(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK,
        1024L);
    gatewaySenderOverflowMonitor.increaseStats(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK,
        10000L);
    assertThat(gatewaySenderOverflowMonitor.getStatistic(StatsKey.GATEWAYSENDER_LRU_EVICTIONS))
        .isEqualTo(10L);
    assertThat(
        gatewaySenderOverflowMonitor.getStatistic(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK))
            .isEqualTo(2048L);
    assertThat(gatewaySenderOverflowMonitor
        .getStatistic(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK)).isEqualTo(20000L);
  }

  @Test
  public void getStatisticShouldReturnZeroForUnknownStatistics() {
    assertThat(gatewaySenderOverflowMonitor.getStatistic("unhandledStatistic")).isEqualTo(0);
  }

  @Test
  public void getStatisticShouldReturnTheRecordedValueForHandledStatistics() {
    gatewaySenderOverflowMonitor.increaseStats(StatsKey.GATEWAYSENDER_LRU_EVICTIONS, 5);
    gatewaySenderOverflowMonitor.increaseStats(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK,
        2048);
    gatewaySenderOverflowMonitor.increaseStats(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK,
        10000);

    assertThat(gatewaySenderOverflowMonitor.getStatistic(StatsKey.GATEWAYSENDER_LRU_EVICTIONS))
        .isEqualTo(5L);
    assertThat(
        gatewaySenderOverflowMonitor.getStatistic(StatsKey.GATEWAYSENDER_BYTES_OVERFLOWED_TO_DISK))
            .isEqualTo(2048L);
    assertThat(gatewaySenderOverflowMonitor
        .getStatistic(StatsKey.GATEWAYSENDER_ENTRIES_OVERFLOWED_TO_DISK)).isEqualTo(10000L);
  }

  @Test
  public void addStatisticsToMonitorShouldAddListenerAndMonitor() {
    Statistics statistics = mock(Statistics.class);
    gatewaySenderOverflowMonitor.addStatisticsToMonitor(statistics);

    assertThat(gatewaySenderOverflowMonitor.getMonitors().size()).isEqualTo(1);
    assertThat(gatewaySenderOverflowMonitor.getListeners().size()).isEqualTo(1);
  }

  @Test
  public void stopListenerShouldRemoveListenerAndMonitor() {
    Statistics statistics = mock(Statistics.class);
    ValueMonitor regionMonitor = mock(ValueMonitor.class);
    GatewaySenderOverflowMonitor.GatewaySenderOverflowStatisticsListener listener =
        mock(GatewaySenderOverflowMonitor.GatewaySenderOverflowStatisticsListener.class);
    gatewaySenderOverflowMonitor.getListeners().put(statistics, listener);
    gatewaySenderOverflowMonitor.getMonitors().put(statistics, regionMonitor);

    gatewaySenderOverflowMonitor.stopListener();
    verify(regionMonitor, times(1)).removeListener(listener);
    verify(regionMonitor, times(1)).removeStatistics(statistics);
    assertThat(gatewaySenderOverflowMonitor.getMonitors()).isEmpty();
    assertThat(gatewaySenderOverflowMonitor.getListeners()).isEmpty();
  }
}
