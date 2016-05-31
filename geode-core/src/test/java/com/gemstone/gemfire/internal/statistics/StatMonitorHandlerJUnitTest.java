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

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.statistics.StatMonitorHandler.StatMonitorNotifier;
import com.gemstone.gemfire.internal.util.StopWatch;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit tests for the StatMonitorHandler and its inner classes.
 *   
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class StatMonitorHandlerJUnitTest {

  @Test
  public void testAddNewMonitor() throws Exception {
    StatMonitorHandler handler = new StatMonitorHandler();
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(handler.addMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().isEmpty());
    assertTrue(handler.getMonitorsSnapshot().contains(monitor));
    handler.sampled(NanoTimer.getTime(), Collections.<ResourceInstance>emptyList());
    waitForNotificationCount(monitor, 1, 2*1000, 10, false);
    assertEquals(1, monitor.getNotificationCount());
  }
  
  @Test
  public void testAddExistingMonitorReturnsFalse() throws Exception {
    StatMonitorHandler handler = new StatMonitorHandler();
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    StatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(handler.addMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().isEmpty());
    assertTrue(handler.getMonitorsSnapshot().contains(monitor));
    assertFalse(handler.addMonitor(monitor));
  }
  
  @Test
  public void testRemoveExistingMonitor() throws Exception {
    StatMonitorHandler handler = new StatMonitorHandler();
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(handler.addMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().isEmpty());
    assertTrue(handler.getMonitorsSnapshot().contains(monitor));
    assertTrue(handler.removeMonitor(monitor));
    assertFalse(handler.getMonitorsSnapshot().contains(monitor));
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    handler.sampled(NanoTimer.getTime(), Collections.<ResourceInstance>emptyList());
    assertEquals(0, monitor.getNotificationCount());
  }
  
  @Test
  public void testRemoveMissingMonitorReturnsFalse() throws Exception {
    StatMonitorHandler handler = new StatMonitorHandler();
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
    StatisticsMonitor monitor = new TestStatisticsMonitor();
    assertFalse(handler.getMonitorsSnapshot().contains(monitor));
    assertFalse(handler.removeMonitor(monitor));
    assertTrue(handler.getMonitorsSnapshot().isEmpty());
  }
  
  @Test
  public void testNotificationSampleFrequencyDefault() throws Exception {
    final int sampleFrequency = 1;
    StatMonitorHandler handler = new StatMonitorHandler();
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    final int sampleCount = 100;
    for (int i = 0; i < sampleCount; i++) {
      handler.sampled(NanoTimer.getTime(), Collections.<ResourceInstance>emptyList());
      waitForNotificationCount(monitor, 1+i, 2*1000, 10, false);
    }
    assertEquals(sampleCount/sampleFrequency, monitor.getNotificationCount());
  }
  
  @Test
  public void testNotificationSampleTimeMillis() throws Exception {
    final long currentTime = System.currentTimeMillis();
    StatMonitorHandler handler = new StatMonitorHandler();
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    long nanoTimeStamp = NanoTimer.getTime();
    handler.sampled(nanoTimeStamp, Collections.<ResourceInstance>emptyList());
    waitForNotificationCount(monitor, 1, 2*1000, 10, false);
    assertTrue(monitor.getTimeStamp() != nanoTimeStamp);
    assertTrue(monitor.getTimeStamp() >= currentTime);
  }
  
  @Test
  public void testNotificationResourceInstances() throws Exception {
    final int resourceInstanceCount = 100;
    final List<ResourceInstance> resourceInstances = new ArrayList<ResourceInstance>();
    for (int i = 0; i < resourceInstanceCount; i++) {
      resourceInstances.add(new ResourceInstance(i, null, null));
    }
    
    StatMonitorHandler handler = new StatMonitorHandler();
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    handler.sampled(NanoTimer.getTime(), Collections.unmodifiableList(resourceInstances));

    waitForNotificationCount(monitor, 1, 2*1000, 10, false);
    
    final List<ResourceInstance> notificationResourceInstances = monitor.getResourceInstances();
    assertNotNull(notificationResourceInstances);
    assertEquals(resourceInstances, notificationResourceInstances);
    assertEquals(resourceInstanceCount, notificationResourceInstances.size());
    
    int i = 0;
    for (ResourceInstance resourceInstance : notificationResourceInstances) {
      assertEquals(i, resourceInstance.getId());
      i++;
    }
  }
  
  @Test
  public void testStatMonitorNotifierAliveButWaiting() throws Exception {
    assumeTrue(StatMonitorHandler.enableMonitorThread);
    StatMonitorHandler handler = new StatMonitorHandler();
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    
    final StatMonitorNotifier notifier = handler.getStatMonitorNotifier();
    assertTrue(notifier.isAlive());
    
    waitUntilWaiting(notifier);
    
    for (int i = 0; i < 20; i++) {
      assert(notifier.isWaiting());
      Thread.sleep(10);
    }
  }
  
  @Test
  public void testStatMonitorNotifierWakesUpForWork() throws Exception {
    assumeTrue(StatMonitorHandler.enableMonitorThread);
    StatMonitorHandler handler = new StatMonitorHandler();
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);
    
    final StatMonitorNotifier notifier = handler.getStatMonitorNotifier();
    assertTrue(notifier.isAlive());
    
    waitUntilWaiting(notifier);
    
    // if notification occurs then notifier woke up...
    assertEquals(0, monitor.getNotificationCount());
    handler.sampled(NanoTimer.getTime(), Collections.<ResourceInstance>emptyList());
    
    waitForNotificationCount(monitor, 1, 2*1000, 10, false);
    assertEquals(1, monitor.getNotificationCount());

    // and goes back to waiting...
    waitUntilWaiting(notifier);
  }

  private static void waitUntilWaiting(StatMonitorNotifier notifier) throws InterruptedException {
    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 2000; done = (notifier.isWaiting())) {
      Thread.sleep(10);
    }
    assertTrue("waiting for notifier to be waiting", done);
  }
  
  private static void waitForNotificationCount(final TestStatisticsMonitor monitor, final int expected, long ms, long interval, boolean throwOnTimeout) throws InterruptedException {
    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < ms; done = (monitor.getNotificationCount() >= expected)) {
      Thread.sleep(interval);
    }
    if (throwOnTimeout) {
      assertTrue("waiting for notification count to be " + expected, done);
    }
  }
  
  /**
   * @since GemFire 7.0
   */
  private static class TestStatisticsMonitor extends StatisticsMonitor {
    private volatile long timeStamp;
    private volatile List<ResourceInstance> resourceInstances;
    private volatile int notificationCount;
    
    public TestStatisticsMonitor() {
      super();
    }
    
    @Override
    protected void monitor(long timeStamp, List<ResourceInstance> resourceInstances) {
      this.timeStamp = timeStamp;
      this.resourceInstances = resourceInstances;
      this.notificationCount++;
    }
    
    long getTimeStamp() {
      return this.timeStamp;
    }
    
    List<ResourceInstance> getResourceInstances() {
      return this.resourceInstances;
    }
    
    int getNotificationCount() {
      return this.notificationCount;
    }
  }
}
