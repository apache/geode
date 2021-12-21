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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;

/**
 * Unit tests for the StatisticsMonitor class. No disk IO.
 *
 * @since GemFire 7.0
 */
public class StatisticsMonitorTest {

  private TestStatisticsManager manager;
  private SampleCollector sampleCollector;

  @Before
  public void setUp() throws Exception {
    final long startTime = System.currentTimeMillis();
    manager = new TestStatisticsManager(1, getClass().getSimpleName(), startTime);

    final StatArchiveHandlerConfig mockStatArchiveHandlerConfig =
        mock(StatArchiveHandlerConfig.class,
            getClass().getSimpleName() + "$" + StatArchiveHandlerConfig.class.getSimpleName());
    when(mockStatArchiveHandlerConfig.getArchiveFileName()).thenReturn(new File(""));
    when(mockStatArchiveHandlerConfig.getArchiveFileSizeLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getArchiveDiskSpaceLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemId()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemStartTime()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemDirectoryPath()).thenReturn("");
    when(mockStatArchiveHandlerConfig.getProductDescription())
        .thenReturn(getClass().getSimpleName());

    StatisticsSampler sampler = new TestStatisticsSampler(manager);
    sampleCollector = new SampleCollector(sampler);
    sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime(),
        new MainWithChildrenRollingFileHandler());
  }

  @After
  public void tearDown() throws Exception {
    if (sampleCollector != null) {
      sampleCollector.close();
      sampleCollector = null;
    }
    manager = null;
  }

  @Test
  public void testAddListener() {
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {}
    };

    assertNull(sampleCollector.getStatMonitorHandlerSnapshot());

    monitor.addListener(listener);

    assertFalse(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertTrue(monitor.getStatisticsListenersSnapshot().contains(listener));
    assertEquals(1, monitor.getStatisticsListenersSnapshot().size());

    assertNotNull(sampleCollector.getStatMonitorHandlerSnapshot());
    assertFalse(
        sampleCollector.getStatMonitorHandlerSnapshot().getMonitorsSnapshot().isEmpty());
  }

  @Test
  public void testAddExistingListener() {
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {}
    };

    monitor.addListener(listener);
    assertFalse(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertTrue(monitor.getStatisticsListenersSnapshot().contains(listener));
    assertEquals(1, monitor.getStatisticsListenersSnapshot().size());

    monitor.addListener(listener);
    assertFalse(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertTrue(monitor.getStatisticsListenersSnapshot().contains(listener));
    assertEquals(1, monitor.getStatisticsListenersSnapshot().size());
  }

  @Test
  public void testRemoveListener() {
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {}
    };

    assertNull(sampleCollector.getStatMonitorHandlerSnapshot());

    monitor.addListener(listener);
    assertFalse(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertTrue(monitor.getStatisticsListenersSnapshot().contains(listener));
    assertEquals(1, monitor.getStatisticsListenersSnapshot().size());

    assertNotNull(sampleCollector.getStatMonitorHandlerSnapshot());
    assertFalse(
        sampleCollector.getStatMonitorHandlerSnapshot().getMonitorsSnapshot().isEmpty());

    monitor.removeListener(listener);
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertFalse(monitor.getStatisticsListenersSnapshot().contains(listener));

    assertNotNull(sampleCollector.getStatMonitorHandlerSnapshot());
    assertTrue(
        sampleCollector.getStatMonitorHandlerSnapshot().getMonitorsSnapshot().isEmpty());
  }

  @Test
  public void testRemoveMissingListener() {
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {}
    };

    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertFalse(monitor.getStatisticsListenersSnapshot().contains(listener));

    monitor.removeListener(listener);

    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertFalse(monitor.getStatisticsListenersSnapshot().contains(listener));
  }

  // TODO: test addStatistic
  // TODO: test removeStatistic
  // TODO: test monitor and/or monitorStatisticIds
  // TODO: test notifyListeners

  /**
   * @since GemFire 7.0
   */
  static class TestStatisticsMonitor extends StatisticsMonitor {
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
