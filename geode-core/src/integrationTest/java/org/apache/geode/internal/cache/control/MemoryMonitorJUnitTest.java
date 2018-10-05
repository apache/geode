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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.util.Properties;
import java.util.Set;

import javax.management.ListenerNotFoundException;
import javax.management.NotificationEmitter;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.logging.LogService;

public class MemoryMonitorJUnitTest {
  private static Logger logger = LogService.getLogger();

  public static final int SYSTEM_LISTENERS = 1;

  DistributedSystem ds;
  GemFireCacheImpl cache;

  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    this.ds = DistributedSystem.connect(p);
    this.cache = (GemFireCacheImpl) CacheFactory.create(this.ds);
    HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
    logger.info(addExpectedAbove);
    logger.info(addExpectedBelow);
  }

  @After
  public void tearDown() throws Exception {
    try {
      HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
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

  /**
   * Test: 1. safe events are not delivered before critical 2. listeners are invoked 3. duplicate
   * safe and critical events are not delivered 4. stats are updated
   */
  @Test
  public void testInvokeListeners() throws Exception {
    InternalResourceManager internalManager = this.cache.getInternalResourceManager();
    HeapMemoryMonitor heapMonitor = internalManager.getHeapMonitor();

    heapMonitor.setTestMaxMemoryBytes(1000);
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    internalManager.setCriticalHeapPercentage(90.0f);
    internalManager.setEvictionHeapPercentage(80.0f);

    // test stats
    assertEquals(900, internalManager.getStats().getCriticalThreshold());
    assertEquals(800, internalManager.getStats().getEvictionThreshold());

    // register a bunch of listeners
    for (int i = 0; i < 10; i++) {
      ResourceListener listener = new TestMemoryThresholdListener();
      internalManager.addResourceListener(ResourceType.HEAP_MEMORY, listener);
    }
    Set<ResourceListener> heapListeners =
        internalManager.getResourceListeners(ResourceType.HEAP_MEMORY);
    assertEquals(10 + SYSTEM_LISTENERS, heapListeners.size());

    heapMonitor.updateStateAndSendEvent(700);
    assertEquals(0, internalManager.getStats().getEvictionStartEvents());
    for (ResourceListener listener : heapListeners) {
      if (listener instanceof TestMemoryThresholdListener) {
        assertEquals(0, ((TestMemoryThresholdListener) listener).getEvictionThresholdCalls());
      }
    }

    // make sure listeners are invoked
    heapMonitor.updateStateAndSendEvent(870);
    assertEquals(0, internalManager.getStats().getEvictionStopEvents());
    assertEquals(1, internalManager.getStats().getEvictionStartEvents());
    assertEquals(0, internalManager.getStats().getHeapCriticalEvents());
    for (ResourceListener listener : heapListeners) {
      if (listener instanceof TestMemoryThresholdListener) {
        assertEquals(0, ((TestMemoryThresholdListener) listener).getCriticalThresholdCalls());
        assertEquals(1, ((TestMemoryThresholdListener) listener).getEvictionThresholdCalls());
      }
    }

    // make sure same event is not triggered twice
    heapMonitor.updateStateAndSendEvent(880);
    assertEquals(0, internalManager.getStats().getEvictionStopEvents());
    assertEquals(1, internalManager.getStats().getEvictionStartEvents());
    assertEquals(0, internalManager.getStats().getHeapCriticalEvents());
    for (ResourceListener listener : heapListeners) {
      if (listener instanceof TestMemoryThresholdListener) {
        assertEquals(0, ((TestMemoryThresholdListener) listener).getCriticalThresholdCalls());
        assertEquals(1, ((TestMemoryThresholdListener) listener).getEvictionThresholdCalls());
      }
    }
  }

  /**
   * By default both thresholds are disabled. make sure no event goes through. Enable thresholds and
   * verify that events are delivered
   */
  // TODO: write a converse of this test when default values are enabled
  @Test
  public void testDefaultThresholds() throws Exception {
    final InternalResourceManager irm = this.cache.getInternalResourceManager();
    final HeapMemoryMonitor hmm = irm.getHeapMonitor();
    TestMemoryThresholdListener listener = new TestMemoryThresholdListener();
    irm.addResourceListener(ResourceType.HEAP_MEMORY, listener);

    hmm.setTestMaxMemoryBytes(1000);
    hmm.updateStateAndSendEvent(870);
    assertEquals(0, irm.getStats().getHeapCriticalEvents());
    hmm.updateStateAndSendEvent(950);
    hmm.updateStateAndSendEvent(770);
    assertEquals(0, irm.getStats().getHeapCriticalEvents());
    assertEquals(0, irm.getStats().getEvictionStartEvents());

    // enable thresholds and make sure events go through

    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(770);
    irm.setEvictionHeapPercentage(80f);
    assertEquals(0, irm.getStats().getEvictionStartEvents());
    assertEquals(0, irm.getStats().getHeapCriticalEvents());

    hmm.updateStateAndSendEvent(870);
    assertEquals(1, listener.getEvictionThresholdCalls());
    assertEquals(1, irm.getStats().getEvictionStartEvents());
    assertEquals(0, irm.getStats().getHeapCriticalEvents());
    listener.resetThresholdCalls();

    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(870);
    irm.setEvictionHeapPercentage(0.0f); // resets the old state
    assertEquals(1, irm.getStats().getEvictionStartEvents());
    assertEquals(0, irm.getStats().getHeapCriticalEvents());

    hmm.updateStateAndSendEvent(870);
    assertEquals(1, irm.getStats().getEvictionStartEvents());
    assertEquals(0, irm.getStats().getHeapCriticalEvents());

    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(870);
    irm.setCriticalHeapPercentage(90f);
    assertEquals(1, irm.getStats().getEvictionStartEvents());
    assertEquals(0, irm.getStats().getHeapCriticalEvents());

    hmm.updateStateAndSendEvent(970);
    assertEquals(1, irm.getStats().getHeapCriticalEvents());
    assertEquals(0, listener.getEvictionThresholdCalls());
    assertEquals(1, irm.getStats().getEvictionStartEvents());
  }

  @Test
  public void testSubRegionCloseRemovesListener() {
    // test local sub region
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region parent = cache.createRegion("parent", factory.create());
    parent.createSubregion("sub", factory.create());
    parent.close();
    assertEquals(0 + SYSTEM_LISTENERS, cache.getInternalResourceManager(false)
        .getResourceListeners(ResourceType.HEAP_MEMORY).size());

    // test nested local region
    parent = cache.createRegion("parent2", factory.create());
    parent.createSubregion("sub", factory.create()).createSubregion("subsub", factory.create());
    parent.close();
    assertEquals(0 + SYSTEM_LISTENERS, cache.getInternalResourceManager(false)
        .getResourceListeners(ResourceType.HEAP_MEMORY).size());

    // test distributed sub region
    factory.setScope(Scope.DISTRIBUTED_ACK);
    parent = cache.createRegion("parent3", factory.create());
    parent.createSubregion("sub", factory.create());
    parent.close();
    assertEquals(0 + SYSTEM_LISTENERS, cache.getInternalResourceManager(false)
        .getResourceListeners(ResourceType.HEAP_MEMORY).size());
    // test nested distributed region
    parent = cache.createRegion("parent4", factory.create());
    parent.createSubregion("sub", factory.create()).createSubregion("subsub", factory.create());
    parent.close();
    assertEquals(0 + SYSTEM_LISTENERS, cache.getInternalResourceManager(false)
        .getResourceListeners(ResourceType.HEAP_MEMORY).size());
  }

  /**
   * creates a distributed region and invokes criticalThresholdReached then does a put to verify
   * that the put is rejected
   */
  @Test
  public void testPutsRejectionDistributedRegion() throws Exception {
    AttributesFactory attr = new AttributesFactory();
    attr.setScope(Scope.DISTRIBUTED_ACK);
    Region region = cache.createRegion("DistributedRegion", attr.create());
    checkOpRejection(region, false, true);
    region.close();
    assertEquals(0 + SYSTEM_LISTENERS, cache.getInternalResourceManager(false)
        .getResourceListeners(ResourceType.HEAP_MEMORY).size());
  }

  @Test
  public void testTxDistributedRegion() throws Exception {
    AttributesFactory attr = new AttributesFactory();
    attr.setScope(Scope.DISTRIBUTED_ACK);
    Region region = cache.createRegion("DistributedRegion", attr.create());
    checkOpRejection(region, true, true);
    region.close();
    assertEquals(0 + SYSTEM_LISTENERS, cache.getInternalResourceManager(false)
        .getResourceListeners(ResourceType.HEAP_MEMORY).size());
  }

  @Test
  public void testPutsLocalRegion() throws Exception {
    AttributesFactory attr = new AttributesFactory();
    attr.setScope(Scope.LOCAL);
    Region region = cache.createRegion("localRegion", attr.create());
    checkOpRejection(region, false, true);
    region.close();
    assertEquals(0 + SYSTEM_LISTENERS, cache.getInternalResourceManager(false)
        .getResourceListeners(ResourceType.HEAP_MEMORY).size());
  }

  @Test
  public void testTxLocalRegion() throws Exception {
    AttributesFactory attr = new AttributesFactory();
    attr.setScope(Scope.LOCAL);
    Region region = cache.createRegion("localRegion", attr.create());
    checkOpRejection(region, true, true);
    region.close();
    assertEquals(0 + SYSTEM_LISTENERS, cache.getInternalResourceManager(false)
        .getResourceListeners(ResourceType.HEAP_MEMORY).size());
  }

  @Test
  public void testPutsRejectedSubRegion() throws Exception {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region subRegion =
        cache.createRegion("local1", factory.create()).createSubregion("sub1", factory.create());
    checkOpRejection(subRegion, false, true);
    subRegion.close();

    // root region is still present
    assertEquals(1 + SYSTEM_LISTENERS, cache.getInternalResourceManager(false)
        .getResourceListeners(ResourceType.HEAP_MEMORY).size());
  }

  @Test
  public void testTxSubRegion() throws Exception {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region subRegion =
        cache.createRegion("local1", factory.create()).createSubregion("sub1", factory.create());
    checkOpRejection(subRegion, true, true);
    subRegion.close();

    // root region is still present
    assertEquals(1 + SYSTEM_LISTENERS, cache.getInternalResourceManager(false)
        .getResourceListeners(ResourceType.HEAP_MEMORY).size());
  }

  @Test
  public void testPutsPartitionedRegion() throws Exception {
    PartitionAttributes pa =
        new PartitionAttributesFactory().setRedundantCopies(0).setTotalNumBuckets(3).create();
    Region region = new RegionFactory().setPartitionAttributes(pa).create("parReg");
    checkOpRejection(region, false, true);
    region.close();
    assertEquals(0 + SYSTEM_LISTENERS, cache.getInternalResourceManager(false)
        .getResourceListeners(ResourceType.HEAP_MEMORY).size());
  }

  private void checkOpRejection(Region region, boolean useTransaction, boolean expectLowMemEx)
      throws Exception {
    if (useTransaction) {
      cache.getCacheTransactionManager().begin();
    }
    region.put("key-1", "value-1");
    int addSubregion = 0;
    if (region.getName().equals("sub1")) {
      addSubregion++;
    }

    InternalResourceManager internalManager = cache.getInternalResourceManager();
    HeapMemoryMonitor heapMonitor = internalManager.getHeapMonitor();
    TestMemoryThresholdListener listener = new TestMemoryThresholdListener();
    internalManager.addResourceListener(ResourceType.HEAP_MEMORY, listener);
    heapMonitor.setTestMaxMemoryBytes(100);
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    internalManager.setCriticalHeapPercentage(95.0f);


    // make sure that the region is added as a memory event listener
    assertEquals(2 + addSubregion + SYSTEM_LISTENERS,
        internalManager.getResourceListeners(ResourceType.HEAP_MEMORY).size());
    assertEquals(2 + addSubregion + SYSTEM_LISTENERS,
        internalManager.getResourceListeners(ResourceType.HEAP_MEMORY).size());

    heapMonitor.updateStateAndSendEvent(97);
    assertEquals(1, listener.getCriticalThresholdCalls());
    boolean caughtException = false;
    try {
      region.put("key-1", "value-2");
    } catch (LowMemoryException low) {
      cache.getLogger().info("caught expected exception", low);
      caughtException = true;
    }
    if (expectLowMemEx && !caughtException) {
      throw new AssertionError("An expected exception was not thrown");
    }
    // make region healthy
    heapMonitor.updateStateAndSendEvent(91);
    // try the puts again
    try {
      region.put("key-1", "value-2");
    } catch (LowMemoryException low) {
      throw new AssertionError("Unexpected exception:", low);
    }
    if (useTransaction) {
      cache.getCacheTransactionManager().commit();
    }
    internalManager.removeResourceListener(listener);
  }

  @Test
  public void testCriticalHeapThreshold() throws Exception {
    final int toohigh = 101;
    final int toolow = -1;
    final float disabled = 0.0f;
    final float justright = 92.5f;
    final ResourceManager rm = this.cache.getResourceManager();

    long usageThreshold = -1;
    int once = 0;
    for (MemoryPoolMXBean p : ManagementFactory.getMemoryPoolMXBeans()) {
      if (p.isUsageThresholdSupported() && HeapMemoryMonitor.isTenured(p)) {
        usageThreshold = p.getUsageThreshold();
        once++;
      }
    }
    assertEquals("Expected only one pool to be assigned", 1, once);

    // Default test, current default is disabled
    assertEquals(rm.getCriticalHeapPercentage(), MemoryThresholds.DEFAULT_CRITICAL_PERCENTAGE,
        0.01);
    NotificationEmitter emitter = (NotificationEmitter) ManagementFactory.getMemoryMXBean();
    try {
      emitter.removeNotificationListener(
          InternalResourceManager.getInternalResourceManager(cache).getHeapMonitor());
      assertTrue("Expected that the resource manager was not registered", false);
    } catch (ListenerNotFoundException expected) {
    }

    try {
      rm.setCriticalHeapPercentage(toohigh);
      assertTrue("Expected illegal argument exception for value " + toohigh, false);
    } catch (IllegalArgumentException toohi) {
    }

    try {
      rm.setCriticalHeapPercentage(toolow);
      assertTrue("Expected illegal argument exception for value " + toolow, false);
    } catch (IllegalArgumentException toohi) {
    }

    rm.setCriticalHeapPercentage(justright);
    emitter = (NotificationEmitter) ManagementFactory.getMemoryMXBean();
    {
      InternalResourceManager irm = InternalResourceManager.getInternalResourceManager(cache);
      HeapMemoryMonitor hmm = irm.getHeapMonitor();
      // Expect no exception for removal (it was installed during set)
      hmm.stopMonitoring();
    }
    assertEquals(rm.getCriticalHeapPercentage(), justright, 0.01);

    rm.setCriticalHeapPercentage(disabled);
    assertEquals(rm.getCriticalHeapPercentage(), disabled, 0.01);
    emitter = (NotificationEmitter) ManagementFactory.getMemoryMXBean();
    try {
      emitter.removeNotificationListener(
          InternalResourceManager.getInternalResourceManager(cache).getHeapMonitor());
      assertTrue("Expected that the resource manager was not registered", false);
    } catch (ListenerNotFoundException expected) {
    }
    // Assert the threshold was reset
    for (MemoryPoolMXBean p : ManagementFactory.getMemoryPoolMXBeans()) {
      if (HeapMemoryMonitor.isTenured(p)) {
        assertEquals(usageThreshold, p.getUsageThreshold());
      }
    }
  }

  @Test
  public void testHandleNotification() throws Exception {
    final InternalResourceManager irm = this.cache.getInternalResourceManager();
    final HeapMemoryMonitor hmm = irm.getHeapMonitor();

    hmm.setTestMaxMemoryBytes(100);
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    irm.setEvictionHeapPercentage(80);
    irm.setCriticalHeapPercentage(90);


    TestMemoryThresholdListener listener = new TestMemoryThresholdListener();
    listener.resetThresholdCalls();
    irm.addResourceListener(ResourceType.HEAP_MEMORY, listener);

    assertEquals(0, listener.getAllCalls());
    // test EVICTION, CRITICAL, EVICTION, NORMAL cycle
    for (int i = 0; i < 3; i++) {
      hmm.updateStateAndSendEvent(82); // EVICTION
      assertEquals(i * 4 + 1, listener.getAllCalls());
      assertEquals((i * 3) + 1, listener.getEvictionThresholdCalls());
      assertEquals(i + 1, irm.getStats().getEvictionStartEvents());
      assertEquals(82, listener.getCurrentHeapPercentage());
      assertEquals(2, listener.getBytesFromThreshold());

      hmm.updateStateAndSendEvent(92); // CRITICAL
      assertEquals(i * 4 + 2, listener.getAllCalls());
      assertEquals(i + 1, listener.getCriticalThresholdCalls());
      assertEquals(i + 1, irm.getStats().getHeapCriticalEvents());
      assertEquals(92, listener.getCurrentHeapPercentage());
      assertEquals(2, listener.getBytesFromThreshold());

      hmm.updateStateAndSendEvent(85); // EVICTION
      assertEquals(i * 4 + 3, listener.getAllCalls());
      assertEquals((i * 3) + 3, listener.getEvictionThresholdCalls());
      assertEquals(i + 1, irm.getStats().getHeapSafeEvents());
      assertEquals(85, listener.getCurrentHeapPercentage());
      assertEquals(5, listener.getBytesFromThreshold());

      hmm.updateStateAndSendEvent(76); // NORMAL
      assertEquals(i * 4 + 4, listener.getAllCalls());
      assertEquals(i + 1, listener.getNormalCalls());
      assertEquals(i + 1, irm.getStats().getEvictionStopEvents());
      assertEquals(76, listener.getCurrentHeapPercentage());
      assertEquals(4, listener.getBytesFromThreshold());
    }
    listener.resetThresholdCalls();

    // test EVICTION to CRITICAL back to EVICTION
    hmm.updateStateAndSendEvent(95); // CRITICAL
    assertEquals(1, listener.getEvictionThresholdCalls());
    assertEquals(1, listener.getCriticalThresholdCalls());
    assertEquals(4, irm.getStats().getHeapCriticalEvents());
    assertEquals(4, irm.getStats().getEvictionStartEvents());
    assertEquals(1, listener.getAllCalls());
    assertEquals(95, listener.getCurrentHeapPercentage());
    assertEquals(5, listener.getBytesFromThreshold());
    hmm.updateStateAndSendEvent(75); // EVICTION
    assertEquals(1, listener.getNormalCalls());
    assertEquals(1, listener.getEvictionThresholdCalls());
    assertEquals(1, listener.getCriticalThresholdCalls());
    assertEquals(75, listener.getCurrentHeapPercentage());
    assertEquals(5, listener.getBytesFromThreshold());
    assertEquals(2, listener.getAllCalls());
    listener.resetThresholdCalls();

    // generate many events in threshold thickness for eviction threshold
    for (int i = 0; i < 5; i++) {
      hmm.updateStateAndSendEvent(82); // EVICTION
      assertEquals(1, listener.getEvictionThresholdCalls());
      assertEquals((i * 2) + 1, listener.getAllCalls());
      assertEquals(82, listener.getCurrentHeapPercentage());
      assertEquals(2, listener.getBytesFromThreshold());
      hmm.updateStateAndSendEvent(79); // EVICTION THICKNESS
    }
    listener.resetThresholdCalls();
    // generate many events in threshold thickness for critical threshold
    for (int i = 0; i < 5; i++) {
      hmm.updateStateAndSendEvent(92); // CRITICAL
      assertEquals(1, listener.getCriticalThresholdCalls());
      assertEquals((i * 2) + 1, listener.getAllCalls());
      assertEquals(92, listener.getCurrentHeapPercentage());
      assertEquals(2, listener.getBytesFromThreshold());
      hmm.updateStateAndSendEvent(89); // CRITICAL THICKNESS
    }
    hmm.updateStateAndSendEvent(75);
    listener.resetThresholdCalls();

    // generate many events around threshold thickness for eviction threshold
    for (int i = 1; i < 6; i++) {
      hmm.updateStateAndSendEvent(82); // EVICTION
      assertEquals(i, listener.getEvictionThresholdCalls());
      assertEquals(82, listener.getCurrentHeapPercentage());
      assertEquals(2, listener.getBytesFromThreshold());
      hmm.updateStateAndSendEvent(77); // NORMAL
      assertEquals(i, listener.getNormalCalls());
      assertEquals(77, listener.getCurrentHeapPercentage());
      assertEquals(3, listener.getBytesFromThreshold());
      assertEquals(i * 2, listener.getAllCalls());
    }
    hmm.updateStateAndSendEvent(87); // EVICTION
    listener.resetThresholdCalls();
    // generate many events around threshold thickness for critical threshold
    for (int i = 1; i < 6; i++) {
      hmm.updateStateAndSendEvent(92); // CRITICAL
      assertEquals(i, listener.getCriticalThresholdCalls());
      assertEquals(92, listener.getCurrentHeapPercentage());
      assertEquals(2, listener.getBytesFromThreshold());
      hmm.updateStateAndSendEvent(87); // EVICTION
      assertEquals(i * 2, listener.getEvictionThresholdCalls());
      assertEquals(87, listener.getCurrentHeapPercentage());
      assertEquals(3, listener.getBytesFromThreshold());
      assertEquals(i * 2, listener.getAllCalls());
    }
    listener.resetThresholdCalls();

    // from CRITICAL drop to EVICTION THICKNESS, and then to NORMAL
    hmm.updateStateAndSendEvent(96); // CRITICAL
    assertEquals(1, listener.getCriticalThresholdCalls());
    assertEquals(1, listener.getAllCalls());
    assertEquals(6, listener.getBytesFromThreshold());
    assertEquals(96, listener.getCurrentHeapPercentage());
    listener.resetThresholdCalls();
    hmm.updateStateAndSendEvent(79); // EVICTION THICKNESS
    assertEquals(1, listener.getEvictionThresholdCalls());
    assertEquals(1, listener.getAllCalls());
    assertEquals(11, listener.getBytesFromThreshold());
    assertEquals(79, listener.getCurrentHeapPercentage());
    listener.resetThresholdCalls();
    hmm.updateStateAndSendEvent(77); // NORMAL
    assertEquals(1, listener.getNormalCalls());
    assertEquals(1, listener.getAllCalls());
    assertEquals(3, listener.getBytesFromThreshold());
    assertEquals(77, listener.getCurrentHeapPercentage());
    listener.resetThresholdCalls();
  }

  @Test
  public void testDisabledThresholds() {
    final InternalResourceManager irm = this.cache.getInternalResourceManager();
    final HeapMemoryMonitor hmm = irm.getHeapMonitor();
    hmm.setTestMaxMemoryBytes(100);
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    irm.setEvictionHeapPercentage(80);
    irm.setCriticalHeapPercentage(90);

    TestMemoryThresholdListener listener = new TestMemoryThresholdListener();
    irm.addResourceListener(ResourceType.HEAP_MEMORY, listener);
    listener.resetThresholdCalls();
    assertEquals(0, listener.getAllCalls());

    // make sure that both thresholds are enabled, disable one threshold, make sure
    // events for the other are delivered, enable the threshold
    // eviction threshold
    hmm.updateStateAndSendEvent(82); // EVICTION
    assertEquals(1, listener.getEvictionThresholdCalls());
    assertEquals(1, listener.getAllCalls());
    assertEquals(1, irm.getStats().getEvictionStartEvents());
    hmm.updateStateAndSendEvent(92); // CRITICAL
    assertEquals(1, listener.getCriticalThresholdCalls());
    assertEquals(2, listener.getAllCalls());
    assertEquals(1, irm.getStats().getHeapCriticalEvents());
    listener.resetThresholdCalls();
    // disable threshold
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(92);
    irm.setEvictionHeapPercentage(0f);
    assertEquals(1, listener.getEvictionDisabledCalls());
    assertEquals(1, irm.getStats().getEvictionStopEvents());
    irm.setEvictionHeapPercentage(0f);
    assertEquals(1, listener.getEvictionDisabledCalls());
    assertEquals(0, irm.getStats().getEvictionThreshold());
    assertEquals(1, irm.getStats().getEvictionStopEvents());

    hmm.updateStateAndSendEvent(75); // EVICTION_DISABLED
    assertEquals(1, listener.getNormalCalls());
    assertEquals(0, listener.getEvictionThresholdCalls());
    assertEquals(1, irm.getStats().getEvictionStopEvents());
    assertEquals(1, irm.getStats().getHeapSafeEvents());
    hmm.updateStateAndSendEvent(85); // EVICTION_DISABLED
    assertEquals(0, listener.getEvictionThresholdCalls());
    assertEquals(1, irm.getStats().getEvictionStartEvents());
    assertEquals(2, listener.getAllCalls());
    hmm.updateStateAndSendEvent(92); // CRITICAL
    assertEquals(3, listener.getAllCalls());
    assertEquals(2, listener.getCriticalThresholdCalls());
    listener.resetThresholdCalls();
    // enable threshold again
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(92);
    irm.setEvictionHeapPercentage(80f);
    assertEquals(1, listener.getEvictionThresholdCalls());
    hmm.updateStateAndSendEvent(84); // EVICTION
    assertEquals(2, listener.getAllCalls()); // EVICTION_CRITICAL+EVICTION
    hmm.updateStateAndSendEvent(77); // NORMAL
    assertEquals(1, listener.getNormalCalls());
    assertEquals(3, listener.getAllCalls());
    hmm.updateStateAndSendEvent(82); // EVICTION
    assertEquals(3, listener.getEvictionThresholdCalls());
    assertEquals(4, listener.getAllCalls());
    hmm.updateStateAndSendEvent(91); // CRITICAL
    assertEquals(2, listener.getCriticalThresholdCalls());
    assertEquals(5, listener.getAllCalls());
    hmm.updateStateAndSendEvent(87); // EVICTION
    assertEquals(5, listener.getEvictionThresholdCalls());
    assertEquals(3, irm.getStats().getHeapSafeEvents());
    assertEquals(6, listener.getAllCalls());
    listener.resetThresholdCalls();

    // now disable critical
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(87);
    irm.setCriticalHeapPercentage(0f); // forced event
    assertEquals(1, listener.getCriticalDisabledCalls());
    irm.setCriticalHeapPercentage(0f);
    assertEquals(1, listener.getCriticalDisabledCalls());
    assertEquals(0, irm.getStats().getCriticalThreshold());

    hmm.updateStateAndSendEvent(92); // NO EVENT
    assertEquals(0, listener.getCriticalThresholdCalls());
    assertEquals(3, irm.getStats().getHeapCriticalEvents());
    assertEquals(2, listener.getAllCalls());
    hmm.updateStateAndSendEvent(89); // NO EVENT
    assertEquals(3, irm.getStats().getHeapSafeEvents());
    assertEquals(3, listener.getAllCalls());
    hmm.updateStateAndSendEvent(77); // NORMAL
    assertEquals(1, listener.getNormalCalls());
    assertEquals(4, listener.getAllCalls());
    hmm.updateStateAndSendEvent(93); // EVICTION
    assertEquals(2, listener.getEvictionThresholdCalls());
    assertEquals(5, listener.getAllCalls());
    listener.resetThresholdCalls();
    // enable critical threshold again
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(93);
    irm.setCriticalHeapPercentage(90f); // forced event
    assertEquals(1, listener.getAllCalls());
    hmm.updateStateAndSendEvent(92); // NO EVENT
    assertEquals(1, listener.getCriticalThresholdCalls());
    assertEquals(2, listener.getAllCalls());
    hmm.updateStateAndSendEvent(89); // CRITICAL THICKNESS
    assertEquals(1, listener.getEvictionThresholdCalls());
    hmm.updateStateAndSendEvent(87); // EVICTION
    assertEquals(2, listener.getEvictionThresholdCalls());
    assertEquals(4, listener.getAllCalls());
    hmm.updateStateAndSendEvent(77); // NORMAL
    assertEquals(1, listener.getNormalCalls());
    assertEquals(5, listener.getAllCalls());
    hmm.updateStateAndSendEvent(83); // EVICTION
    assertEquals(3, listener.getEvictionThresholdCalls());
    assertEquals(6, listener.getAllCalls());
    listener.resetThresholdCalls();

    // disable both thresholds (to make sure previous event is reset)
    // enable one at a time, and confirm event delivery
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(33);
    irm.setCriticalHeapPercentage(0f);
    assertEquals(1, listener.getCriticalDisabledCalls());
    irm.setEvictionHeapPercentage(0f);
    assertEquals(1, listener.getEvictionDisabledCalls());

    hmm.updateStateAndSendEvent(95); // NO EVENT
    hmm.updateStateAndSendEvent(87); // NO EVENT
    hmm.updateStateAndSendEvent(77); // NO EVENT
    assertEquals(5, listener.getAllCalls()); // the two DISABLE calls
    listener.resetThresholdCalls();

    // enable eviction, verify that forced event is not generated
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(77);
    irm.setEvictionHeapPercentage(80f);
    assertEquals(0, listener.getEvictionThresholdCalls());
    hmm.updateStateAndSendEvent(88); // EVICTION
    assertEquals(1, listener.getEvictionThresholdCalls());
    assertEquals(2, listener.getAllCalls());
    hmm.updateStateAndSendEvent(92); // NO EVENT
    assertEquals(3, listener.getAllCalls());
    hmm.updateStateAndSendEvent(77); // NORMAL
    assertEquals(2, listener.getNormalCalls());
    hmm.updateStateAndSendEvent(98); // EVICTION
    assertEquals(2, listener.getEvictionThresholdCalls());
    assertEquals(5, listener.getAllCalls());
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(98);
    irm.setEvictionHeapPercentage(0f); // resets old state
    listener.resetThresholdCalls();

    hmm.updateStateAndSendEvent(87); // NO EVENT
    hmm.updateStateAndSendEvent(77); // NO EVENT
    hmm.updateStateAndSendEvent(85); // NO EVENT
    assertEquals(3, listener.getAllCalls());

    // enable critical, verify that forced event is not generated
    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(85);
    irm.setCriticalHeapPercentage(90f);
    assertEquals(0, listener.getCriticalThresholdCalls());
    assertEquals(4, listener.getAllCalls());
    hmm.updateStateAndSendEvent(94); // CRITICAL
    assertEquals(5, listener.getAllCalls());
    hmm.updateStateAndSendEvent(77); // NORMAL
    assertEquals(2, listener.getNormalCalls());
    assertEquals(6, listener.getAllCalls());
    hmm.updateStateAndSendEvent(87); // NO EVENT
    assertEquals(6, listener.getAllCalls());
    assertEquals(1, listener.getCriticalThresholdCalls());
    hmm.updateStateAndSendEvent(85); // NORMAL
    assertEquals(2, listener.getNormalCalls());
  }

  @Test
  public void testAddListeners() {
    final InternalResourceManager internalManager = this.cache.getInternalResourceManager();
    ResourceListener<MemoryEvent> memoryListener = new ResourceListener<MemoryEvent>() {
      public void onEvent(MemoryEvent event) {
        cache.getLogger().info("Received MemoryEvent");
      }
    };
    internalManager.addResourceListener(ResourceType.HEAP_MEMORY, memoryListener);

    assertEquals(1 + SYSTEM_LISTENERS,
        internalManager.getResourceListeners(ResourceType.HEAP_MEMORY).size());
  }
}
