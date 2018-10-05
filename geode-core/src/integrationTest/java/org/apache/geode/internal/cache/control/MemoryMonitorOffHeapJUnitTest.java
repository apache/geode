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
package org.apache.geode.internal.cache.control;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.logging.LogService;

public class MemoryMonitorOffHeapJUnitTest {
  private static final Logger logger = LogService.getLogger();
  private static final int SYSTEM_LISTENERS = 1;

  DistributedSystem ds;
  GemFireCacheImpl cache;

  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(OFF_HEAP_MEMORY_SIZE, "1m");
    this.ds = DistributedSystem.connect(p);
    this.cache = (GemFireCacheImpl) CacheFactory.create(this.ds);
    logger.info(addExpectedAbove);
    logger.info(addExpectedBelow);
  }

  @After
  public void tearDown() throws Exception {
    try {
      this.cache.close();
      this.ds.disconnect();
    } finally {
      logger.info(removeExpectedAbove);
      logger.info(removeExpectedBelow);
    }
  }

  static final String expectedEx = "Member: .*? above .*? critical threshold";
  public static final String addExpectedAbove =
      "<ExpectedException action=add>" + expectedEx + "</ExpectedException>";
  public static final String removeExpectedAbove =
      "<ExpectedException action=remove>" + expectedEx + "</ExpectedException>";

  static final String expectedBelow = "Member: .*? below .*? critical threshold";
  public static final String addExpectedBelow =
      "<ExpectedException action=add>" + expectedBelow + "</ExpectedException>";
  public static final String removeExpectedBelow =
      "<ExpectedException action=remove>" + expectedBelow + "</ExpectedException>";

  @Test
  public void testGeneratingEvents() throws Exception {
    InternalResourceManager internalManager = this.cache.getInternalResourceManager();
    OffHeapMemoryMonitor monitor = internalManager.getOffHeapMonitor();

    monitor.setEvictionThreshold(50.0f);
    monitor.setCriticalThreshold(75.0f);
    monitor.stopMonitoring(true);

    assertEquals(524288, internalManager.getStats().getOffHeapEvictionThreshold());
    assertEquals(786432, internalManager.getStats().getOffHeapCriticalThreshold());

    // Register a bunch of listeners
    for (int i = 0; i < 10; i++) {
      ResourceListener listener = new TestMemoryThresholdListener();
      internalManager.addResourceListener(ResourceType.OFFHEAP_MEMORY, listener);
    }
    assertEquals(10 + SYSTEM_LISTENERS,
        internalManager.getResourceListeners(ResourceType.OFFHEAP_MEMORY).size());

    // Start at normal
    setThenTestListenersAndStats(400000, 0, 0, 0, 0, 0, 0, 0);

    // Move to eviction
    setThenTestListenersAndStats(550000, 0, 1, 0, 0, 1, 0, 0);

    // Stay at eviction
    setThenTestListenersAndStats(560000, 0, 1, 0, 0, 1, 0, 0);

    // Move to critical
    setThenTestListenersAndStats(850000, 0, 1, 0, 1, 2, 1, 0);

    // Stay at critical (above critical clear margin)
    setThenTestListenersAndStats(786431, 0, 1, 0, 1, 2, 1, 0);
    setThenTestListenersAndStats(765465, 0, 1, 0, 1, 2, 1, 0);

    // Move to eviction
    setThenTestListenersAndStats(765454, 0, 1, 1, 1, 3, 1, 0);

    // Stay at eviction (above eviction clear margin)
    setThenTestListenersAndStats(524281, 0, 1, 1, 1, 3, 1, 0);
    setThenTestListenersAndStats(503321, 0, 1, 1, 1, 3, 1, 0);

    // Move to normal
    setThenTestListenersAndStats(503310, 1, 1, 1, 1, 3, 1, 1);

    // Disable eviction and verify normal event
    monitor.setEvictionThreshold(0f);
    setThenTestListenersAndStats(503315, 1, 1, 1, 1, 3, 1, 2);

    // Enable eviction verify normal event
    monitor.setEvictionThreshold(50f);
    setThenTestListenersAndStats(503315, 1, 1, 1, 1, 3, 1, 3);

    // Disable critical verify normal event
    monitor.setCriticalThreshold(0f);
    setThenTestListenersAndStats(503315, 1, 1, 1, 1, 3, 1, 4);

    // Enable critical verify normal event
    monitor.setCriticalThreshold(75f);
    setThenTestListenersAndStats(503315, 1, 1, 1, 1, 3, 1, 5);
  }

  private void setThenTestListenersAndStats(final long memUsed, final int evictionStop,
      final int evictionStart, final int safe, final int critical, final int evictionEvents,
      final int criticalEvents, final int normalEvents) {
    this.cache.getInternalResourceManager().getOffHeapMonitor().updateStateAndSendEvent(memUsed);
    ResourceManagerStats stats = this.cache.getInternalResourceManager().getStats();

    assertEquals(evictionStop, stats.getOffHeapEvictionStopEvents());
    assertEquals(evictionStart, stats.getOffHeapEvictionStartEvents());
    assertEquals(critical, stats.getOffHeapCriticalEvents());
    assertEquals(safe, stats.getOffHeapSafeEvents());

    for (ResourceListener listener : this.cache.getInternalResourceManager()
        .getResourceListeners(ResourceType.OFFHEAP_MEMORY)) {
      if (listener instanceof TestMemoryThresholdListener) {
        assertEquals(evictionEvents,
            ((TestMemoryThresholdListener) listener).getEvictionThresholdCalls());
        assertEquals(criticalEvents,
            ((TestMemoryThresholdListener) listener).getCriticalThresholdCalls());
        assertEquals(normalEvents, ((TestMemoryThresholdListener) listener).getNormalCalls());
      }
    }
  }

  @Test
  public void testDisabledThresholds() throws Exception {
    final InternalResourceManager irm = this.cache.getInternalResourceManager();
    final OffHeapMemoryMonitor monitor = irm.getOffHeapMonitor();

    final RegionFactory regionFactory = this.cache.createRegionFactory(RegionShortcut.LOCAL);
    regionFactory.setOffHeap(true);
    final EvictionAttributesImpl evictionAttrs = new EvictionAttributesImpl();
    evictionAttrs.setAlgorithm(EvictionAlgorithm.NONE);
    regionFactory.setEvictionAttributes(evictionAttrs);
    final Region region = regionFactory.create("testDefaultThresholdsRegion");
    TestMemoryThresholdListener listener = new TestMemoryThresholdListener();
    irm.addResourceListener(ResourceType.OFFHEAP_MEMORY, listener);

    region.put("1", new Byte[550000]);
    region.put("2", new Byte[200000]);
    assertEquals(0, irm.getStats().getOffHeapEvictionStartEvents());
    assertEquals(0, irm.getStats().getOffHeapEvictionStopEvents());
    assertEquals(0, irm.getStats().getOffHeapCriticalEvents());
    assertEquals(0, irm.getStats().getOffHeapSafeEvents());
    assertEquals(0, listener.getEvictionThresholdCalls());
    assertEquals(0, listener.getCriticalThresholdCalls());

    // Enable eviction threshold and make sure event is generated
    monitor.setEvictionThreshold(50f);
    assertEquals(1, irm.getStats().getOffHeapEvictionStartEvents());
    assertEquals(0, irm.getStats().getOffHeapCriticalEvents());
    assertEquals(1, listener.getEvictionThresholdCalls());
    assertEquals(0, listener.getCriticalThresholdCalls());

    // Enable critical threshold and make sure event is generated
    region.put("3", new Byte[200000]);
    monitor.setCriticalThreshold(70f);
    assertEquals(1, irm.getStats().getOffHeapEvictionStartEvents());
    assertEquals(1, irm.getStats().getOffHeapCriticalEvents());
    assertEquals(2, listener.getEvictionThresholdCalls());
    assertEquals(1, listener.getCriticalThresholdCalls());

    // Disable thresholds and verify events
    monitor.setEvictionThreshold(0f);
    monitor.setCriticalThreshold(0f);

    assertEquals(1, irm.getStats().getOffHeapEvictionStartEvents());
    assertEquals(1, irm.getStats().getOffHeapEvictionStopEvents());
    assertEquals(1, irm.getStats().getOffHeapCriticalEvents());
    assertEquals(1, irm.getStats().getOffHeapSafeEvents());

    assertEquals(2, listener.getEvictionThresholdCalls());
    assertEquals(2, listener.getCriticalThresholdCalls());
    assertEquals(0, listener.getNormalCalls());
    assertEquals(2, listener.getEvictionDisabledCalls());
    assertEquals(2, listener.getCriticalDisabledCalls());
  }

  @Test
  public void testAllowedThreholds() {
    final OffHeapMemoryMonitor monitor =
        this.cache.getInternalResourceManager().getOffHeapMonitor();

    // Test eviction bounds
    try {
      monitor.setEvictionThreshold(100.1f);
      fail("Too high value allowed for setEvictionThreshold");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    try {
      monitor.setEvictionThreshold(-0.1f);
      fail("Too low value allowed for setEvictionThreshold");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    monitor.setEvictionThreshold(13f);
    monitor.setEvictionThreshold(0f);
    monitor.setEvictionThreshold(92f);
    monitor.setEvictionThreshold(100f);
    monitor.setEvictionThreshold(0f);

    // Test critical bounds
    try {
      monitor.setCriticalThreshold(100.1f);
      fail("Too high value allowed for setCriticalThreshold");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    try {
      monitor.setCriticalThreshold(-0.1f);
      fail("Too low value allowed for setCriticalThreshold");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    monitor.setCriticalThreshold(13f);
    monitor.setCriticalThreshold(0f);
    monitor.setCriticalThreshold(92f);
    monitor.setCriticalThreshold(100f);
    monitor.setCriticalThreshold(0f);

    // Test values relative to each other
    monitor.setEvictionThreshold(1f);
    monitor.setCriticalThreshold(1.1f);
    monitor.setCriticalThreshold(0);
    monitor.setCriticalThreshold(1.1f);
    monitor.setEvictionThreshold(0);
    monitor.setEvictionThreshold(1.0f);
    monitor.setCriticalThreshold(100f);
    monitor.setEvictionThreshold(99.9f);
    monitor.setCriticalThreshold(0f);
    monitor.setEvictionThreshold(0f);
    monitor.setEvictionThreshold(64.1f);
    monitor.setCriticalThreshold(64.2f);

    try {
      monitor.setCriticalThreshold(50f);
      monitor.setEvictionThreshold(50.1f);
      fail("Allowed eviction threshold to be set higher than critical threshold");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
  }

  @Test
  public void testMonitorRunning() {
    final OffHeapMemoryMonitor monitor =
        this.cache.getInternalResourceManager().getOffHeapMonitor();

    assertFalse("Off-heap monitor is not running", monitor.started);

    monitor.setEvictionThreshold(1f);
    assertTrue("Off-heap monitor is running", monitor.started);
    monitor.setEvictionThreshold(0f);
    assertFalse("Off-heap monitor is not running", monitor.started);

    monitor.setCriticalThreshold(1f);
    assertTrue("Off-heap monitor is running", monitor.started);
    monitor.setCriticalThreshold(0f);
    assertFalse("Off-heap monitor is not running", monitor.started);

    monitor.setEvictionThreshold(1f);
    monitor.setCriticalThreshold(1.1f);
    assertTrue("Off-heap monitor is running", monitor.started);

    monitor.setEvictionThreshold(0f);
    monitor.setCriticalThreshold(0f);
    assertFalse("Off-heap monitor is not running", monitor.started);
  }

  @Test
  public void testGettersAndSetters() {
    final OffHeapMemoryMonitor monitor =
        this.cache.getInternalResourceManager().getOffHeapMonitor();

    assertEquals(0f, monitor.getEvictionThreshold(), 0.01);
    assertEquals(0f, monitor.getCriticalThreshold(), 0.01);

    monitor.setEvictionThreshold(35);
    assertEquals(35f, monitor.getEvictionThreshold(), 0.01);
    assertEquals(0f, monitor.getCriticalThreshold(), 0.01);

    monitor.setCriticalThreshold(45);
    assertEquals(35f, monitor.getEvictionThreshold(), 0.01);
    assertEquals(45f, monitor.getCriticalThreshold(), 0.01);

    monitor.setEvictionThreshold(0);
    monitor.setCriticalThreshold(0);
    assertEquals(0f, monitor.getEvictionThreshold(), 0.01);
    assertEquals(0f, monitor.getCriticalThreshold(), 0.01);
  }
}
