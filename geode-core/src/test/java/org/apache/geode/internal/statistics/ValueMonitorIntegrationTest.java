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

import static org.apache.geode.test.dunit.Wait.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.statistics.StatisticsNotification.Type;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Integration test for the SampleCollector class.
 * 
 * @since GemFire 7.0
 */
@Category(IntegrationTest.class)
public class ValueMonitorIntegrationTest {

  private Mockery mockContext;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    this.mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
  }

  @After
  public void tearDown() throws Exception {
    this.mockContext.assertIsSatisfied();
    this.mockContext = null;
  }

  @Test
  public void testAddRemoveListener() throws Exception {
    long startTime = System.currentTimeMillis();
    List<Statistics> statsList = new ArrayList<Statistics>();
    StatisticsManager mockStatisticsManager = this.mockContext.mock(StatisticsManager.class,
        testName.getMethodName() + "$StatisticsManager");
    this.mockContext.checking(new Expectations() {
      {
        allowing(mockStatisticsManager).getName();
        will(returnValue("mockStatisticsManager"));
        allowing(mockStatisticsManager).getId();
        will(returnValue(1));
        allowing(mockStatisticsManager).getStartTime();
        will(returnValue(startTime));
        allowing(mockStatisticsManager).getStatListModCount();
        will(returnValue(0));
        allowing(mockStatisticsManager).getStatsList();
        will(returnValue(statsList));
      }
    });

    StatisticsSampler mockStatisticsSampler = this.mockContext.mock(StatisticsSampler.class,
        testName.getMethodName() + "$StatisticsSampler");
    this.mockContext.checking(new Expectations() {
      {
        allowing(mockStatisticsSampler).getStatisticsModCount();
        will(returnValue(0));
        allowing(mockStatisticsSampler).getStatistics();
        will(returnValue(new Statistics[] {}));
      }
    });

    StatArchiveHandlerConfig mockStatArchiveHandlerConfig = this.mockContext.mock(
        StatArchiveHandlerConfig.class, testName.getMethodName() + "$StatArchiveHandlerConfig");
    this.mockContext.checking(new Expectations() {
      {
        allowing(mockStatArchiveHandlerConfig).getArchiveFileName();
        will(returnValue(new File("")));
        allowing(mockStatArchiveHandlerConfig).getArchiveFileSizeLimit();
        will(returnValue(0));
        allowing(mockStatArchiveHandlerConfig).getArchiveDiskSpaceLimit();
        will(returnValue(0));
        allowing(mockStatArchiveHandlerConfig).getSystemId();
        will(returnValue(1));
        allowing(mockStatArchiveHandlerConfig).getSystemStartTime();
        will(returnValue(startTime));
        allowing(mockStatArchiveHandlerConfig).getSystemDirectoryPath();
        will(returnValue(""));
        allowing(mockStatArchiveHandlerConfig).getProductDescription();
        will(returnValue("testAddRemoveListener"));
      }
    });

    // need a real SampleCollector for this test or the monitor can't get the handler
    SampleCollector sampleCollector = new SampleCollector(mockStatisticsSampler);
    sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime());

    List<StatisticsNotification> notifications = new ArrayList<>();
    StatisticsListener listener = (final StatisticsNotification notification) -> {
      notifications.add(notification);
    };
    ValueMonitor monitor = new ValueMonitor();

    long timeStamp = System.currentTimeMillis();
    Type type = Type.VALUE_CHANGED;
    Number value = 43;

    StatisticsNotification notification = createStatisticsNotification(timeStamp, type, value);
    monitor.notifyListeners(notification);

    assertTrue(notifications.isEmpty());

    monitor.addListener(listener);
    monitor.notifyListeners(notification);

    assertEquals(1, notifications.size());
    notification = notifications.remove(0);
    assertNotNull(notification);

    assertEquals(timeStamp, notification.getTimeStamp());
    assertEquals(type, notification.getType());
    StatisticId statId = createStatisticId(null, null);
    assertEquals(value, notification.getValue(statId));

    monitor.removeListener(listener);
    monitor.notifyListeners(notification);

    assertTrue(notifications.isEmpty());
  }

  @Test
  public void testValueMonitorListener() throws Exception {
    long startTime = System.currentTimeMillis();
    TestStatisticsManager manager =
        new TestStatisticsManager(1, "ValueMonitorIntegrationTest", startTime);
    StatisticsSampler sampler = new TestStatisticsSampler(manager);

    StatArchiveHandlerConfig mockStatArchiveHandlerConfig = this.mockContext.mock(
        StatArchiveHandlerConfig.class, testName.getMethodName() + "$StatArchiveHandlerConfig");
    this.mockContext.checking(new Expectations() {
      {
        allowing(mockStatArchiveHandlerConfig).getArchiveFileName();
        will(returnValue(new File("")));
        allowing(mockStatArchiveHandlerConfig).getArchiveFileSizeLimit();
        will(returnValue(0));
        allowing(mockStatArchiveHandlerConfig).getArchiveDiskSpaceLimit();
        will(returnValue(0));
        allowing(mockStatArchiveHandlerConfig).getSystemId();
        will(returnValue(1));
        allowing(mockStatArchiveHandlerConfig).getSystemStartTime();
        will(returnValue(startTime));
        allowing(mockStatArchiveHandlerConfig).getSystemDirectoryPath();
        will(returnValue(""));
        allowing(mockStatArchiveHandlerConfig).getProductDescription();
        will(returnValue("testFoo"));
      }
    });

    SampleCollector sampleCollector = new SampleCollector(sampler);
    sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime());

    StatisticDescriptor[] statsST1 = new StatisticDescriptor[] {
        manager.createDoubleCounter("double_counter_1", "double_counter_1_desc",
            "double_counter_1_units"),
        manager.createIntCounter("int_counter_2", "int_counter_2_desc", "int_counter_2_units"),
        manager.createLongCounter("long_counter_3", "long_counter_3_desc", "long_counter_3_units")};
    StatisticsType ST1 = manager.createType("ST1_name", "ST1_desc", statsST1);
    Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1_text", 1);
    Statistics st1_2 = manager.createAtomicStatistics(ST1, "st1_2_text", 2);

    st1_1.incDouble("double_counter_1", 1000.0001);
    st1_1.incInt("int_counter_2", 2);
    st1_1.incLong("long_counter_3", 3333333333L);

    st1_2.incDouble("double_counter_1", 2000.0002);
    st1_2.incInt("int_counter_2", 3);
    st1_2.incLong("long_counter_3", 4444444444L);

    List<StatisticsNotification> notifications = new ArrayList<>();
    StatisticsListener listener = (final StatisticsNotification notification) -> {
      notifications.add(notification);
    };
    ValueMonitor monitor = new ValueMonitor().addStatistics(st1_1);
    monitor.addListener(listener);

    assertTrue("Unexpected notifications: " + notifications, notifications.isEmpty());

    long timeStamp = NanoTimer.getTime();
    sampleCollector.sample(timeStamp);

    awaitAtLeastTimeoutOrUntilNotifications(notifications, 2 * 1000);
    assertEquals("Unexpected notifications: " + notifications, 1, notifications.size());

    StatisticsNotification notification = notifications.remove(0);
    assertEquals(StatisticsNotification.Type.VALUE_CHANGED, notification.getType());

    // validate 1 notification occurs with all 3 stats of st1_1

    st1_1.incDouble("double_counter_1", 1.1);
    st1_1.incInt("int_counter_2", 2);
    st1_1.incLong("long_counter_3", 3);

    timeStamp += NanoTimer.millisToNanos(1000);
    sampleCollector.sample(timeStamp);

    awaitAtLeastTimeoutOrUntilNotifications(notifications, 2 * 1000);
    assertEquals("Unexpected notifications: " + notifications, 1, notifications.size());

    notification = notifications.remove(0);
    assertEquals(StatisticsNotification.Type.VALUE_CHANGED, notification.getType());

    int statCount = 0;
    Map<String, Number> expectedValues = new HashMap<>();
    expectedValues.put("double_counter_1", 1001.1001);
    expectedValues.put("int_counter_2", 4);
    expectedValues.put("long_counter_3", 3333333336L);

    for (StatisticId statId : notification) {
      Number value = expectedValues.remove(statId.getStatisticDescriptor().getName());
      assertNotNull(value);
      assertEquals(value, notification.getValue(statId));
      statCount++;
    }
    assertEquals(3, statCount);

    // validate no notification occurs when no stats are updated

    timeStamp += NanoTimer.millisToNanos(1000);
    sampleCollector.sample(timeStamp);

    awaitAtLeastTimeoutOrUntilNotifications(notifications, 2 * 1000);
    assertTrue("Unexpected notifications: " + notifications, notifications.isEmpty());

    // validate no notification occurs when only other stats are updated

    st1_2.incDouble("double_counter_1", 3.3);
    st1_2.incInt("int_counter_2", 1);
    st1_2.incLong("long_counter_3", 2);

    timeStamp += NanoTimer.millisToNanos(1000);
    sampleCollector.sample(timeStamp);

    awaitAtLeastTimeoutOrUntilNotifications(notifications, 2 * 1000);
    assertTrue("Unexpected notifications: " + notifications, notifications.isEmpty());

    // validate notification only contains stats added to monitor

    st1_1.incInt("int_counter_2", 100);
    st1_2.incInt("int_counter_2", 200);

    assertEquals(2, sampleCollector.currentHandlersForTesting().size());

    timeStamp += NanoTimer.millisToNanos(1000);
    sampleCollector.sample(timeStamp);

    awaitAtLeastTimeoutOrUntilNotifications(notifications, 2 * 1000);
    assertEquals("Unexpected notifications: " + notifications, 1, notifications.size());

    notification = notifications.remove(0);
    assertEquals(StatisticsNotification.Type.VALUE_CHANGED, notification.getType());

    statCount = 0;
    expectedValues = new HashMap<>();
    expectedValues.put("int_counter_2", 104);

    for (StatisticId statId : notification) {
      Number value = expectedValues.remove(statId.getStatisticDescriptor().getName());
      assertNotNull(value);
      assertEquals(value, notification.getValue(statId));
      statCount++;
    }
    assertEquals(1, statCount);
  }

  private StatisticId createStatisticId(final StatisticDescriptor descriptor,
      final Statistics stats) {
    return new StatisticId() {

      @Override
      public StatisticDescriptor getStatisticDescriptor() {
        return descriptor;
      }

      @Override
      public Statistics getStatistics() {
        return stats;
      }
    };
  }

  protected StatisticsNotification createStatisticsNotification(final long timeStamp,
      final Type type, final Number value) {
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
      public Number getValue(final StatisticId statId) throws StatisticNotFoundException {
        return value;
      }
    };
  }

  /**
   * Wait for at least the specified time or until notifications is >0.
   */
  private static void awaitAtLeastTimeoutOrUntilNotifications(
      final List<StatisticsNotification> notifications, final long timeoutMillis) {
    long pollingIntervalMillis = 10;
    boolean throwOnTimeout = false;
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return notifications.size() > 0;
      }

      @Override
      public String description() {
        return "waiting for notification";
      }
    };
    waitForCriterion(wc, timeoutMillis, pollingIntervalMillis, throwOnTimeout);
  }
}
