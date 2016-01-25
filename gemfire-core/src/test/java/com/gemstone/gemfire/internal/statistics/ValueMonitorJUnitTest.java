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
package com.gemstone.gemfire.internal.statistics;

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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.StatisticsManager;
import com.gemstone.gemfire.internal.statistics.StatisticsNotification.Type;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import junit.framework.TestCase;

/**
 * Integration test for the SampleCollector class.
 *   
 * @author Kirk Lund
 * @since 7.0
 */
@Category(IntegrationTest.class)
public class ValueMonitorJUnitTest {

  private Mockery mockContext;
  
  
  @Before
  public void setUp() throws Exception {
    this.mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
  }
  
  @After
  public void tearDown() throws Exception {
    this.mockContext.assertIsSatisfied();
    this.mockContext = null;
  }
  
  @Test
  public void testAddRemoveListener() throws Exception {
    final long startTime = System.currentTimeMillis();
    final List<Statistics> statsList = new ArrayList<Statistics>();
    final StatisticsManager mockStatisticsManager = this.mockContext.mock(StatisticsManager.class, "testAddRemoveListener$StatisticsManager");
    this.mockContext.checking(new Expectations() {{
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
    }});
    
    final StatisticsSampler mockStatisticsSampler = this.mockContext.mock(StatisticsSampler.class, "testAddRemoveListener$StatisticsSampler");
    this.mockContext.checking(new Expectations() {{
      allowing(mockStatisticsSampler).getStatisticsModCount();
      will(returnValue(0));
      allowing(mockStatisticsSampler).getStatistics();
      will(returnValue(new Statistics[]{}));
    }});
    
    final StatArchiveHandlerConfig mockStatArchiveHandlerConfig = this.mockContext.mock(StatArchiveHandlerConfig.class, "testAddRemoveListener$StatArchiveHandlerConfig");
    this.mockContext.checking(new Expectations() {{
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
    }});
    
    // need a real SampleCollector for this test or the monitor can't get the handler
    SampleCollector sampleCollector = new SampleCollector(mockStatisticsSampler);
    sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime());
    
    final List<StatisticsNotification> notifications = new ArrayList<StatisticsNotification>();
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {
        notifications.add(notification);
      }
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
    final long startTime = System.currentTimeMillis();
    TestStatisticsManager manager = new TestStatisticsManager(
        1, 
        "ValueMonitorJUnitTest", 
        startTime);
    StatisticsSampler sampler = new TestStatisticsSampler(manager);
    
    final StatArchiveHandlerConfig mockStatArchiveHandlerConfig = 
        this.mockContext.mock(StatArchiveHandlerConfig.class, "testFoo$StatArchiveHandlerConfig");
    this.mockContext.checking(new Expectations() {{
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
    }});
    
    SampleCollector sampleCollector = new SampleCollector(sampler);
    sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime());

    StatisticDescriptor[] statsST1 = new StatisticDescriptor[] {
        manager.createDoubleCounter("double_counter_1", "double_counter_1_desc", "double_counter_1_units"),
        manager.createIntCounter(   "int_counter_2",    "int_counter_2_desc",    "int_counter_2_units"),
        manager.createLongCounter(  "long_counter_3",   "long_counter_3_desc",   "long_counter_3_units")
    };
    StatisticsType ST1 = manager.createType("ST1_name", "ST1_desc", statsST1);
    Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1_text", 1);
    Statistics st1_2 = manager.createAtomicStatistics(ST1, "st1_2_text", 2);
    
    st1_1.incDouble("double_counter_1", 1000.0001);
    st1_1.incInt("int_counter_2", 2);
    st1_1.incLong("long_counter_3", 3333333333L);
    
    st1_2.incDouble("double_counter_1", 2000.0002);
    st1_2.incInt("int_counter_2", 3);
    st1_2.incLong("long_counter_3", 4444444444L);
    
    final List<StatisticsNotification> notifications = new ArrayList<StatisticsNotification>();
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {
        notifications.add(notification);
      }
    };
    ValueMonitor monitor = new ValueMonitor().addStatistics(st1_1);
    monitor.addListener(listener);
    
    assertTrue("Unexpected notifications: " + notifications, notifications.isEmpty());

    long timeStamp = NanoTimer.getTime();
    sampleCollector.sample(timeStamp);
    
    waitForNotification(notifications, 2*1000, 10, false);
    assertEquals("Unexpected notifications: " + notifications, 1, notifications.size());
    
    StatisticsNotification notification = notifications.remove(0);
    assertEquals(StatisticsNotification.Type.VALUE_CHANGED, notification.getType());
    
    // validate 1 notification occurs with all 3 stats of st1_1
    
    st1_1.incDouble("double_counter_1", 1.1);
    st1_1.incInt("int_counter_2", 2);
    st1_1.incLong("long_counter_3", 3);
    
    timeStamp += NanoTimer.millisToNanos(1000);
    sampleCollector.sample(timeStamp);
    
    waitForNotification(notifications, 2*1000, 10, false);
    assertEquals("Unexpected notifications: " + notifications, 1, notifications.size());
    
    notification = notifications.remove(0);
    assertEquals(StatisticsNotification.Type.VALUE_CHANGED, notification.getType());
    
    int statCount = 0;
    Map<String, Number> expectedValues = new HashMap<String, Number>();
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
    
    waitForNotification(notifications, 2*1000, 10, false);
    assertTrue("Unexpected notifications: " + notifications, notifications.isEmpty());
    
    // validate no notification occurs when only other stats are updated
    
    st1_2.incDouble("double_counter_1", 3.3);
    st1_2.incInt("int_counter_2", 1);
    st1_2.incLong("long_counter_3", 2);
    
    timeStamp += NanoTimer.millisToNanos(1000);
    sampleCollector.sample(timeStamp);
    
    waitForNotification(notifications, 2*1000, 10, false);
    assertTrue("Unexpected notifications: " + notifications, notifications.isEmpty());
    
    // validate notification only contains stats added to monitor
    
    st1_1.incInt("int_counter_2", 100);
    st1_2.incInt("int_counter_2", 200);

    assertEquals(2, sampleCollector.currentHandlersForTesting().size());
    
    timeStamp += NanoTimer.millisToNanos(1000);
    sampleCollector.sample(timeStamp);
    
    waitForNotification(notifications, 2*1000, 10, false);
    assertEquals("Unexpected notifications: " + notifications, 1, notifications.size());
    
    notification = notifications.remove(0);
    assertEquals(StatisticsNotification.Type.VALUE_CHANGED, notification.getType());
    
    statCount = 0;
    expectedValues = new HashMap<String, Number>();
    expectedValues.put("int_counter_2", 104);

    for (StatisticId statId : notification) {
      Number value = expectedValues.remove(statId.getStatisticDescriptor().getName());
      assertNotNull(value);
      assertEquals(value, notification.getValue(statId));
      statCount++;
    }
    assertEquals(1, statCount);
  }
  
  protected StatisticId createStatisticId(
      final StatisticDescriptor descriptor, final Statistics stats) {
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
  
  protected StatisticsNotification createStatisticsNotification(
      final long timeStamp, final Type type, final Number value) {
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
        return null; // TODO
      }

      @Override
      public Iterator<StatisticId> iterator(StatisticDescriptor statDesc) {
        return null; // TODO
      }

      @Override
      public Iterator<StatisticId> iterator(Statistics statistics) {
        return null; // TODO
      }

      @Override
      public Iterator<StatisticId> iterator(StatisticsType statisticsType) {
        return null; // TODO
      }

      @Override
      public Number getValue(StatisticId statId) throws StatisticNotFoundException {
        return value;
      }
      
    };
  }
  
  private static void waitForNotification(final List<StatisticsNotification> notifications, long ms, long interval, boolean throwOnTimeout) {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return notifications.size() > 0;
      }
      public String description() {
        return "waiting for notification";
      }
    };
    DistributedTestCase.waitForCriterion(wc, ms, interval, throwOnTimeout);
  }
}
