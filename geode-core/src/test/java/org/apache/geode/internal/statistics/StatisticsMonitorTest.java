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
import static org.mockito.Mockito.*;

import java.io.File;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit tests for the StatisticsMonitor class. No disk IO.
 *   
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class StatisticsMonitorTest {
  
  private TestStatisticsManager manager;
  private SampleCollector sampleCollector;

  @Before
  public void setUp() throws Exception {
    final long startTime = System.currentTimeMillis();
    this.manager = new TestStatisticsManager(1, getClass().getSimpleName(), startTime);
    
    final StatArchiveHandlerConfig mockStatArchiveHandlerConfig = mock(StatArchiveHandlerConfig.class, getClass().getSimpleName() + "$" + StatArchiveHandlerConfig.class.getSimpleName());
    when(mockStatArchiveHandlerConfig.getArchiveFileName()).thenReturn(new File(""));
    when(mockStatArchiveHandlerConfig.getArchiveFileSizeLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getArchiveDiskSpaceLimit()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemId()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemStartTime()).thenReturn(0L);
    when(mockStatArchiveHandlerConfig.getSystemDirectoryPath()).thenReturn("");
    when(mockStatArchiveHandlerConfig.getProductDescription()).thenReturn(getClass().getSimpleName());

    StatisticsSampler sampler = new TestStatisticsSampler(manager);
    this.sampleCollector = new SampleCollector(sampler);
    this.sampleCollector.initialize(mockStatArchiveHandlerConfig, NanoTimer.getTime());
  }

  @After
  public void tearDown() throws Exception {
    if (this.sampleCollector != null) {
      this.sampleCollector.close();
      this.sampleCollector = null;
    }
    this.manager = null;
  }
  
  @Test
  public void testAddListener() {
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {
      }
    };
    
    assertNull(this.sampleCollector.getStatMonitorHandlerSnapshot());
        
    monitor.addListener(listener);

    assertFalse(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertTrue(monitor.getStatisticsListenersSnapshot().contains(listener));
    assertEquals(1, monitor.getStatisticsListenersSnapshot().size());

    assertNotNull(this.sampleCollector.getStatMonitorHandlerSnapshot());
    assertFalse(this.sampleCollector.getStatMonitorHandlerSnapshot().getMonitorsSnapshot().isEmpty());
  }
  
  @Test
  public void testAddExistingListener() {
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {
      }
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
      public void handleNotification(StatisticsNotification notification) {
      }
    };
    
    assertNull(this.sampleCollector.getStatMonitorHandlerSnapshot());

    monitor.addListener(listener);
    assertFalse(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertTrue(monitor.getStatisticsListenersSnapshot().contains(listener));
    assertEquals(1, monitor.getStatisticsListenersSnapshot().size());
    
    assertNotNull(this.sampleCollector.getStatMonitorHandlerSnapshot());
    assertFalse(this.sampleCollector.getStatMonitorHandlerSnapshot().getMonitorsSnapshot().isEmpty());
    
    monitor.removeListener(listener);
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    assertFalse(monitor.getStatisticsListenersSnapshot().contains(listener));

    assertNotNull(this.sampleCollector.getStatMonitorHandlerSnapshot());
    assertTrue(this.sampleCollector.getStatMonitorHandlerSnapshot().getMonitorsSnapshot().isEmpty());
  }
  
  @Test
  public void testRemoveMissingListener() {
    TestStatisticsMonitor monitor = new TestStatisticsMonitor();
    assertTrue(monitor.getStatisticsListenersSnapshot().isEmpty());
    StatisticsListener listener = new StatisticsListener() {
      @Override
      public void handleNotification(StatisticsNotification notification) {
      }
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
