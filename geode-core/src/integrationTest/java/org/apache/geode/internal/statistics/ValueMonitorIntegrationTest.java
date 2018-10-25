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
package org.apache.geode.internal.statistics;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.internal.statistics.StatisticsNotification.Type;
import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * Integration test for the SampleCollector class.
 *
 * @since GemFire 7.0
 */
@Category(StatisticsTest.class)
public class ValueMonitorIntegrationTest {
  private StatArchiveHandlerConfig mockStatArchiveHandlerConfig;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    mockStatArchiveHandlerConfig = mock(StatArchiveHandlerConfig.class,
        testName.getMethodName() + "$StatArchiveHandlerConfig");
    when(mockStatArchiveHandlerConfig.getArchiveFileName()).thenReturn(new File(""));
    when(mockStatArchiveHandlerConfig.getArchiveFileSizeLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getArchiveDiskSpaceLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemId()).thenReturn(1L);
    when(mockStatArchiveHandlerConfig.getSystemDirectoryPath()).thenReturn("");
    when(mockStatArchiveHandlerConfig.getProductDescription()).thenReturn("Geode");
  }

  private StatisticsNotification createStatisticsNotification(final long timeStamp, final Type type,
      final Number value) {
    return new StatisticsNotification() {

      @Override
      public long getTimeStamp() {
        return timeStamp;
      }

      @Override
      public Type getType() {
        return type;
      }

      @Override
      public Iterator<StatisticId> iterator() {
        return null;
      }

      @Override
      public Iterator<StatisticId> iterator(final StatisticDescriptor statDesc) {
        return null;
      }

      @Override
      public Iterator<StatisticId> iterator(final Statistics statistics) {
        return null;
      }

      @Override
      public Iterator<StatisticId> iterator(final StatisticsType statisticsType) {
        return null;
      }

      @Override
      public Number getValue(final StatisticId statId) {
        return value;
      }
    };
  }

  @Test
  public void testAddRemoveListener() throws Exception {
    long startTime = System.currentTimeMillis();
    StatisticsManager mockStatisticsManager =
        mock(StatisticsManager.class, testName.getMethodName() + "$StatisticsManager");
    when(mockStatisticsManager.getName()).thenReturn("mockStatisticsManager");
    when(mockStatisticsManager.getId()).thenReturn(1L);
    when(mockStatisticsManager.getStartTime()).thenReturn(startTime);
    when(mockStatisticsManager.getStatListModCount()).thenReturn(0);
    when(mockStatisticsManager.getStatsList()).thenReturn(new ArrayList<>());

    StatisticsSampler mockStatisticsSampler =
        mock(StatisticsSampler.class, testName.getMethodName() + "$StatisticsSampler");
    when(mockStatisticsSampler.getStatisticsModCount()).thenReturn(0);
    when(mockStatisticsSampler.getStatistics()).thenReturn(new Statistics[] {});

    // need a real SampleCollector for this test or the monitor can't get the handler
    SampleCollector sampleCollector = new SampleCollector(mockStatisticsSampler);
    sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime(),
        new MainWithChildrenRollingFileHandler());

    List<StatisticsNotification> notifications = new ArrayList<>();
    StatisticsListener listener = notifications::add;

    ValueMonitor monitor = new ValueMonitor();
    Number value = 43;
    Type type = Type.VALUE_CHANGED;
    long timeStamp = System.currentTimeMillis();
    StatisticsNotification notification = createStatisticsNotification(timeStamp, type, value);
    monitor.notifyListeners(notification);
    assertThat(notifications.isEmpty()).isTrue();

    monitor.addListener(listener);
    monitor.notifyListeners(notification);
    assertThat(notifications.size()).isEqualTo(1);
    notification = notifications.remove(0);
    assertThat(notification).isNotNull();
    assertThat(notification.getTimeStamp()).isEqualTo(timeStamp);
    assertThat(notification.getType()).isEqualTo(type);
    StatisticId statId = mock(StatisticId.class);
    assertThat(notification.getValue(statId)).isEqualTo(value);

    monitor.removeListener(listener);
    monitor.notifyListeners(notification);
    assertThat(notifications.isEmpty()).isTrue();
  }

  private int assertStatisticsNotification(StatisticsNotification notification,
      Map<String, Number> expectedValues) throws StatisticNotFoundException {
    int statCount = 0;
    for (StatisticId statId : notification) {
      Number value = expectedValues.remove(statId.getStatisticDescriptor().getName());
      assertThat(value).isNotNull();
      assertThat(notification.getValue(statId)).isEqualTo(value);
      statCount++;
    }

    return statCount;
  }

  @Test
  public void testValueMonitorListener() throws Exception {
    long startTime = System.currentTimeMillis();
    TestStatisticsManager manager =
        new TestStatisticsManager(1, "ValueMonitorIntegrationTest", startTime);
    StatisticsSampler sampler = new TestStatisticsSampler(manager);
    SampleCollector sampleCollector = new SampleCollector(sampler);
    sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime(),
        new MainWithChildrenRollingFileHandler());

    StatisticDescriptor[] statsST1 = new StatisticDescriptor[] {
        manager.createDoubleCounter("double_counter_1", "double_counter_1_desc",
            "double_counter_1_units"),
        manager.createIntCounter("int_counter_2", "int_counter_2_desc", "int_counter_2_units"),
        manager.createLongCounter("long_counter_3", "long_counter_3_desc", "long_counter_3_units")};
    StatisticsType ST1 = manager.createType("ST1_name", "ST1_desc", statsST1);

    Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1_text", 1);
    st1_1.incDouble("double_counter_1", 1000.0001);
    st1_1.incInt("int_counter_2", 2);
    st1_1.incLong("long_counter_3", 3333333333L);

    Statistics st1_2 = manager.createAtomicStatistics(ST1, "st1_2_text", 2);
    st1_2.incDouble("double_counter_1", 2000.0002);
    st1_2.incInt("int_counter_2", 3);
    st1_2.incLong("long_counter_3", 4444444444L);

    List<StatisticsNotification> notifications = new ArrayList<>();
    StatisticsListener listener = notifications::add;

    ValueMonitor monitor = new ValueMonitor().addStatistics(st1_1);
    monitor.addListener(listener);
    assertThat(notifications.isEmpty()).isTrue();

    long timeStamp = NanoTimer.getTime();
    sampleCollector.sample(timeStamp);
    await()
        .until(() -> notifications.size() > 0);
    assertThat(notifications.size()).isEqualTo(1);

    StatisticsNotification notification = notifications.remove(0);
    assertThat(notification.getType()).isEqualTo(StatisticsNotification.Type.VALUE_CHANGED);

    // validate 1 notification occurs with all 3 stats of st1_1
    st1_1.incDouble("double_counter_1", 1.1);
    st1_1.incInt("int_counter_2", 2);
    st1_1.incLong("long_counter_3", 3);
    timeStamp += NanoTimer.millisToNanos(1000);
    sampleCollector.sample(timeStamp);
    await()
        .until(() -> notifications.size() > 0);
    assertThat(notifications.size()).isEqualTo(1);
    notification = notifications.remove(0);
    assertThat(notification.getType()).isEqualTo(StatisticsNotification.Type.VALUE_CHANGED);

    Map<String, Number> expectedValues = new HashMap<>();
    expectedValues.put("double_counter_1", 1001.1001);
    expectedValues.put("int_counter_2", 4);
    expectedValues.put("long_counter_3", 3333333336L);
    int statCount = assertStatisticsNotification(notification, expectedValues);
    assertThat(statCount).isEqualTo(3);

    // validate no notification occurs when no stats are updated
    timeStamp += NanoTimer.millisToNanos(1000);
    sampleCollector.sample(timeStamp);
    await()
        .until(() -> notifications.size() == 0);
    assertThat(notifications.isEmpty()).isTrue();

    // validate no notification occurs when only other stats are updated
    st1_2.incDouble("double_counter_1", 3.3);
    st1_2.incInt("int_counter_2", 1);
    st1_2.incLong("long_counter_3", 2);
    timeStamp += NanoTimer.millisToNanos(1000);
    sampleCollector.sample(timeStamp);
    await()
        .until(() -> notifications.size() == 0);
    assertThat(notifications.isEmpty()).isTrue();

    // validate notification only contains stats added to monitor
    st1_1.incInt("int_counter_2", 100);
    st1_2.incInt("int_counter_2", 200);
    assertThat(sampleCollector.currentHandlersForTesting().size()).isEqualTo(2);
    timeStamp += NanoTimer.millisToNanos(1000);
    sampleCollector.sample(timeStamp);
    await()
        .until(() -> notifications.size() > 0);
    assertThat(notifications.size()).isEqualTo(1);
    notification = notifications.remove(0);
    assertThat(notification.getType()).isEqualTo(StatisticsNotification.Type.VALUE_CHANGED);
    expectedValues = new HashMap<>();
    expectedValues.put("int_counter_2", 104);
    statCount = assertStatisticsNotification(notification, expectedValues);
    assertThat(statCount).isEqualTo(1);
  }
}
