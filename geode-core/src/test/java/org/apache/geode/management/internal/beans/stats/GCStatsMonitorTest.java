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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.statistics.StatisticId;
import org.apache.geode.internal.statistics.StatisticsNotification;
import org.apache.geode.internal.statistics.ValueMonitor;
import org.apache.geode.test.junit.categories.StatisticsTest;

@Category(StatisticsTest.class)
public class GCStatsMonitorTest {
  @Rule
  public TestName testName = new TestName();

  private GCStatsMonitor gcStatsMonitor;

  @Before
  public void setUp() {
    ValueMonitor valueMonitor = mock(ValueMonitor.class);
    gcStatsMonitor = new GCStatsMonitor(testName.getMethodName(), valueMonitor);

    assertThat(gcStatsMonitor).isNotNull();
    assertThat(gcStatsMonitor.getCollections()).isEqualTo(0L);
    assertThat(gcStatsMonitor.getCollectionTime()).isEqualTo(0L);
  }

  @Test
  public void getStatisticShouldReturnZeroForUnknownStatistics() {
    assertThat(gcStatsMonitor.getStatistic("unknownStatistic")).isEqualTo(0);
  }

  @Test
  public void addStatsToMonitor() throws Exception {
    Statistics stats = mock(Statistics.class);
    when(stats.getUniqueId()).thenReturn(11L);
    StatisticDescriptor d1 = mock(StatisticDescriptor.class);
    when(d1.getName()).thenReturn(StatsKey.VM_GC_STATS_COLLECTIONS);
    StatisticDescriptor d2 = mock(StatisticDescriptor.class);
    when(d2.getName()).thenReturn(StatsKey.VM_GC_STATS_COLLECTION_TIME);
    StatisticDescriptor[] descriptors = {d1, d2};
    StatisticsType type = mock(StatisticsType.class);
    when(stats.getType()).thenReturn(type);
    when(type.getStatistics()).thenReturn(descriptors);

    when(stats.get(any(StatisticDescriptor.class))).thenReturn(8L, 300L);
    gcStatsMonitor.addStatisticsToMonitor(stats);
    assertThat(gcStatsMonitor.getCollections()).isEqualTo(8L);
    assertThat(gcStatsMonitor.getCollectionTime()).isEqualTo(300L);

    when(stats.getUniqueId()).thenReturn(12L);
    when(stats.get(any(StatisticDescriptor.class))).thenReturn(10L, 500L);
    gcStatsMonitor.addStatisticsToMonitor(stats);
    // make sure the results are the sums of these two sets of numbers
    assertThat(gcStatsMonitor.getCollections()).isEqualTo(18L);
    assertThat(gcStatsMonitor.getCollectionTime()).isEqualTo(800L);

    when(stats.getUniqueId()).thenReturn(11L);
    when(stats.get(any(StatisticDescriptor.class))).thenReturn(9L, 400L);
    gcStatsMonitor.addStatisticsToMonitor(stats);
    // make sure this new set of numbers replaces thee old set with the same unique ID
    assertThat(gcStatsMonitor.getCollections()).isEqualTo(19L);
    assertThat(gcStatsMonitor.getCollectionTime()).isEqualTo(900L);
  }

  @Test
  public void handleNotification() throws Exception {
    StatisticsNotification notification = mock(StatisticsNotification.class);
    Statistics stats = mock(Statistics.class);
    when(stats.getUniqueId()).thenReturn(11L);

    StatisticId s1 = mock(StatisticId.class);
    when(s1.getStatistics()).thenReturn(stats);
    StatisticDescriptor d1 = mock(StatisticDescriptor.class);
    when(s1.getStatisticDescriptor()).thenReturn(d1);
    when(d1.getName()).thenReturn(StatsKey.VM_GC_STATS_COLLECTIONS);
    StatisticId s2 = mock(StatisticId.class);
    when(s2.getStatistics()).thenReturn(stats);
    StatisticDescriptor d2 = mock(StatisticDescriptor.class);
    when(s2.getStatisticDescriptor()).thenReturn(d2);
    when(d2.getName()).thenReturn(StatsKey.VM_GC_STATS_COLLECTION_TIME);
    List<StatisticId> list = Arrays.asList(s1, s2);
    when(notification.iterator()).thenReturn(list.iterator());
    when(notification.getValue(s1)).thenReturn(6L);
    when(notification.getValue(s2)).thenReturn(100L);

    gcStatsMonitor.handleNotification(notification);
    assertThat(gcStatsMonitor.getCollections()).isEqualTo(6L);
    assertThat(gcStatsMonitor.getCollectionTime()).isEqualTo(100L);

    // make sure a new set of number replaces the old one if they have the same
    // uniqueId
    when(notification.getValue(s1)).thenReturn(7L);
    when(notification.getValue(s2)).thenReturn(200L);
    when(notification.iterator()).thenReturn(list.iterator());
    gcStatsMonitor.handleNotification(notification);
    assertThat(gcStatsMonitor.getCollections()).isEqualTo(7L);
    assertThat(gcStatsMonitor.getCollectionTime()).isEqualTo(200L);

    // with a new set of stats with different uniqueId, then the value
    // is the sum of the two
    when(stats.getUniqueId()).thenReturn(12L);
    when(notification.getValue(s1)).thenReturn(10L);
    when(notification.getValue(s2)).thenReturn(300L);
    when(notification.iterator()).thenReturn(list.iterator());
    gcStatsMonitor.handleNotification(notification);
    assertThat(gcStatsMonitor.getCollections()).isEqualTo(17L);
    assertThat(gcStatsMonitor.getCollectionTime()).isEqualTo(500L);
  }
}
