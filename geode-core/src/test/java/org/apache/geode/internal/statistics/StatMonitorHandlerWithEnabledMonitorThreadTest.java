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

import static org.apache.geode.internal.statistics.StatMonitorHandler.ENABLE_MONITOR_THREAD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.statistics.StatMonitorHandler.StatMonitorNotifier;
import org.apache.geode.internal.util.StopWatch;

/**
 * Extracted tests from StatMonitorHandlerTest that require enableMonitorThread
 */
public class StatMonitorHandlerWithEnabledMonitorThreadTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void before() throws Exception {
    System.setProperty(ENABLE_MONITOR_THREAD, "true");
  }

  @Test
  public void testStatMonitorNotifierAliveButWaiting() throws Exception {
    StatMonitorHandler handler = new StatMonitorHandler();
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);

    final StatMonitorNotifier notifier = handler.getStatMonitorNotifier();
    assertTrue(notifier.isAlive());

    waitUntilWaiting(notifier);

    for (int i = 0; i < 20; i++) {
      assertTrue(notifier.isWaiting());
      Thread.sleep(10);
    }
  }

  @Test
  public void testStatMonitorNotifierWakesUpForWork() throws Exception {
    StatMonitorHandler handler = new StatMonitorHandler();
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    handler.addMonitor(monitor);

    final StatMonitorNotifier notifier = handler.getStatMonitorNotifier();
    assertTrue(notifier.isAlive());

    waitUntilWaiting(notifier);

    // if notification occurs then notifier woke up...
    assertEquals(0, monitor.getNotificationCount());
    handler.sampled(NanoTimer.getTime(), Collections.emptyList());

    waitForNotificationCount(monitor, 1, 2 * 1000, 10, false);
    assertEquals(1, monitor.getNotificationCount());

    // and goes back to waiting...
    waitUntilWaiting(notifier);
  }

  private static void waitUntilWaiting(StatMonitorNotifier notifier) throws InterruptedException {
    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 2000; done =
        (notifier.isWaiting())) {
      Thread.sleep(10);
    }
    assertTrue("waiting for notifier to be waiting", done);
  }

  private static void waitForNotificationCount(final TestStatisticsMonitor monitor,
      final int expected, final long ms, final long interval, final boolean throwOnTimeout)
      throws InterruptedException {
    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < ms; done =
        (monitor.getNotificationCount() >= expected)) {
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
      notificationCount++;
    }

    long getTimeStamp() {
      return timeStamp;
    }

    List<ResourceInstance> getResourceInstances() {
      return resourceInstances;
    }

    int getNotificationCount() {
      return notificationCount;
    }
  }
}
