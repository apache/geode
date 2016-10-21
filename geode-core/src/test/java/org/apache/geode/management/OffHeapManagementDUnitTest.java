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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.OutOfOffHeapMemoryException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.offheap.OffHeapMemoryStats;
import org.apache.geode.internal.offheap.OffHeapStorage;
import org.apache.geode.internal.offheap.OffHeapStoredObject;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.beans.MemberMBean;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests the off-heap additions to the RegionMXBean and MemberMXBean JMX interfaces.
 */
@SuppressWarnings("serial")
@Category(DistributedTest.class)
public class OffHeapManagementDUnitTest extends JUnit4CacheTestCase {
  /**
   * Specified assertion operations.
   */
  private static enum ASSERT_OP {
    EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN
  }

  /**
   * Name of off-heap test region.
   */
  private static final String OFF_HEAP_REGION_NAME = "offHeapRegion";

  /**
   * Path of off-heap test region.
   */
  private static final String OFF_HEAP_REGION_PATH = "/" + OFF_HEAP_REGION_NAME;

  /**
   * Expected total off-heap reserved memory (1 megabyte).
   */
  private static final long TOTAL_MEMORY = 1048576;

  /**
   * Half of expected memory total.
   */
  private static final int HALF_TOTAL_MEMORY = (int) (TOTAL_MEMORY / 2);

  /**
   * An arbitrary array size.
   */
  private static final int ALLOCATION_SIZE = 100000;

  /**
   * A non-arbitrary array size.
   */
  private static final int NEW_ALLOCATION_SIZE = 400000;

  /**
   * Java object serialization overhead.
   */
  private static final int OBJECT_OVERHEAD = 8;

  /**
   * A region entry key.
   */
  private static final String KEY = "key";

  /**
   * Another region entry key.
   */
  private static final String KEY2 = "key2";

  /**
   * Yet another region entry key.
   */
  private static final String KEY3 = "key3";

  /**
   * A region entry value.
   */
  private static final byte[] VALUE =
      "Proin lobortis enim vel sem congue ut condimentum leo rhoncus. In turpis lorem, rhoncus nec rutrum vel, sodales vitae lacus. Etiam nunc ligula, scelerisque id egestas vitae, gravida non enim. Donec ac ligula purus. Mauris gravida ligula sit amet mi ornare blandit. Aliquam at velit ac enim varius malesuada ut eu tortor. Quisque diam nisi, fermentum vel accumsan at, commodo et velit."
          .getBytes();

  /**
   * The expected size of the region entry value in off-heap memory.
   */
  private static final int OBJECT_SIZE = VALUE.length + OBJECT_OVERHEAD;

  /**
   * Listens for off-heap JMX notifications.
   */
  private static final OffHeapNotificationListener notificationListener =
      new OffHeapNotificationListener();

  /**
   * Local MBeanServer.
   */
  private static MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;

  @Override
  public final void postSetUp() throws Exception {
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(OffHeapStorage.STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY, "true");
      }
    });
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        System.clearProperty(OffHeapStorage.STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY);
      }
    });
  }

  /**
   * Tests off-heap additions to the RegionMXBean and MemberMXBean interfaces.
   * 
   * @throws Exception
   */
  @Test
  public void testOffHeapMBeanAttributesAndStats() throws Exception {
    final VM vm = Host.getHost(0).getVM(0);

    try {
      // Setup off-heap memory for cache
      setSystemPropertiesOnVm(vm, true, getSystemProperties());

      // Create our off-heap region
      assertNotNull(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE));

      // Make sure our off-heap region has off-heap enabled.
      assertOffHeapRegionAttributesOnVm(vm);

      // Make sure our starting off heap stats are correct
      assertOffHeapMetricsOnVm(vm, TOTAL_MEMORY, 0, 0, 0);

      /*
       * Perform ops on the off-heap region and assert that the off-heap metrics correctly reflect
       * the ops
       */
      {
        doPutOnVm(vm, KEY, VALUE, OFF_HEAP_REGION_NAME, false);
        assertOffHeapMetricsOnVm(vm, (TOTAL_MEMORY - OBJECT_SIZE), OBJECT_SIZE, 1, 0);

        doPutOnVm(vm, KEY2, VALUE, OFF_HEAP_REGION_NAME, false);
        assertOffHeapMetricsOnVm(vm, (TOTAL_MEMORY - (2 * OBJECT_SIZE)), (2 * OBJECT_SIZE), 2, 0);

        doPutOnVm(vm, KEY3, VALUE, OFF_HEAP_REGION_NAME, false);
        assertOffHeapMetricsOnVm(vm, (TOTAL_MEMORY - (3 * OBJECT_SIZE)), (3 * OBJECT_SIZE), 3, 0);

        doDestroyOnVm(vm, KEY3, OFF_HEAP_REGION_NAME);
        assertOffHeapMetricsOnVm(vm, (TOTAL_MEMORY - (2 * OBJECT_SIZE)), (2 * OBJECT_SIZE), 2, 0);

        doDestroyOnVm(vm, KEY2, OFF_HEAP_REGION_NAME);
        assertOffHeapMetricsOnVm(vm, (TOTAL_MEMORY - OBJECT_SIZE), OBJECT_SIZE, 1, 0);

        doDestroyOnVm(vm, KEY, OFF_HEAP_REGION_NAME);
        assertOffHeapMetricsOnVm(vm, TOTAL_MEMORY, 0, 0, 0);
      }
    } finally {
      doCleanupOnVm(vm);
    }
  }

  /**
   * Tests the fragmentation statistic for off-heap memory.
   * 
   * @throws Exception
   */
  @Test
  public void testFragmentationStat() throws Exception {
    final VM vm = Host.getHost(0).getVM(0);

    try {
      // Setup off-heap memory for cache
      setSystemPropertiesOnVm(vm, true, getSystemProperties());

      // Create our off-heap region
      assertNotNull(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE));
      vm.invoke(new SerializableRunnable() {
        @Override
        public void run() {
          Region region = getCache().getRegion(OFF_HEAP_REGION_NAME);
          assertNotNull(region);
        }
      });

      // Make sure our off-heap region has off-heap enabled.
      assertOffHeapRegionAttributesOnVm(vm);

      // Make sure our starting off heap stats are correct
      assertOffHeapMetricsOnVm(vm, TOTAL_MEMORY, 0, 0, 0);

      // After allocating large chunk (equal to total memory)
      // we should still have no fragmentation
      int largeChunk = (int) TOTAL_MEMORY - OffHeapStoredObject.HEADER_SIZE;
      doPutOnVm(vm, KEY, new byte[largeChunk], OFF_HEAP_REGION_NAME, false);
      // No compaction has run, so fragmentation should be zero
      assertFragmentationStatOnVm(vm, 0, ASSERT_OP.EQUAL);

      // Allocate more memory to trigger compaction
      doPutOnVm(vm, KEY, new byte[ALLOCATION_SIZE], OFF_HEAP_REGION_NAME, true);
      // When total memory is used no fragmentation
      assertFragmentationStatOnVm(vm, 0, ASSERT_OP.EQUAL);

      // After freeing all memory we should have no fragmentation
      doDestroyOnVm(vm, KEY, OFF_HEAP_REGION_NAME);
      assertFragmentationStatOnVm(vm, 0, ASSERT_OP.EQUAL);

      // Allocate HALF_TOTAL_MEMORY twice and release one to create one fragment
      int halfChunk = HALF_TOTAL_MEMORY - OffHeapStoredObject.HEADER_SIZE;
      doPutOnVm(vm, KEY + "0", new byte[halfChunk], OFF_HEAP_REGION_NAME, false);
      doPutOnVm(vm, KEY + "1", new byte[halfChunk], OFF_HEAP_REGION_NAME, false);
      doDestroyOnVm(vm, KEY + "0", OFF_HEAP_REGION_NAME);

      // Allocate largeChunk to trigger compaction and fragmentation should be zero
      // as all free memory is available as one fragment
      doPutOnVm(vm, KEY + "1", new byte[largeChunk], OFF_HEAP_REGION_NAME, true);
      assertFragmentationStatOnVm(vm, 0, ASSERT_OP.EQUAL);

      // Consume the available fragment as below
      // [16][262120][16][262120][16] = [524288] (HALF_TOTAL_MEMORY)
      int smallChunk = OffHeapStoredObject.MIN_CHUNK_SIZE - OffHeapStoredObject.HEADER_SIZE;
      int mediumChunk = 262112; // (262120 - ObjectChunk.OFF_HEAP_HEADER_SIZE)
      doPutOnVm(vm, KEY + "S1", new byte[smallChunk], OFF_HEAP_REGION_NAME, false);
      doPutOnVm(vm, KEY + "M1", new byte[mediumChunk], OFF_HEAP_REGION_NAME, false);
      doPutOnVm(vm, KEY + "S2", new byte[smallChunk], OFF_HEAP_REGION_NAME, false);
      doPutOnVm(vm, KEY + "M2", new byte[mediumChunk], OFF_HEAP_REGION_NAME, false);
      doPutOnVm(vm, KEY + "S3", new byte[smallChunk], OFF_HEAP_REGION_NAME, false);

      // free small chunks to create gaps
      doDestroyOnVm(vm, KEY + "S1", OFF_HEAP_REGION_NAME);
      doDestroyOnVm(vm, KEY + "S2", OFF_HEAP_REGION_NAME);
      doDestroyOnVm(vm, KEY + "S3", OFF_HEAP_REGION_NAME);

      // Now free memory should be 48 so allocate a 40 byte object
      doPutOnVm(vm, KEY + "newKey", new byte[40], OFF_HEAP_REGION_NAME, true);

      /*
       * Setup a fragmentation attribute monitor
       */
      {
        setupOffHeapMonitorOnVm(vm, "OffHeapFragmentation", 0, 0);
        clearNotificationListenerOnVm(vm);
      }

      // Make sure we have some fragmentation
      assertFragmentationStatOnVm(vm, 100, ASSERT_OP.EQUAL);

      // Make sure our fragmentation monitor was triggered
      waitForNotificationListenerOnVm(vm, 5000, 500, true);
    } finally {
      doCleanupOnVm(vm);
    }
  }

  /**
   * Tests the compation time statistic for off-heap memory.
   * 
   * @throws Exception
   */
  @Test
  public void testCompactionTimeStat() throws Exception {
    final VM vm = Host.getHost(0).getVM(0);

    try {
      // Setup off-heap memory for cache
      setSystemPropertiesOnVm(vm, true, getSystemProperties());

      // Create our off-heap region
      assertNotNull(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE));

      // Make sure our off-heap region has off-heap enabled.
      assertOffHeapRegionAttributesOnVm(vm);

      // Make sure our starting off heap stats are correct
      assertOffHeapMetricsOnVm(vm, TOTAL_MEMORY, 0, 0, 0);

      // After allocating large chunck we should still have no compaction time
      doPutOnVm(vm, KEY, new byte[HALF_TOTAL_MEMORY], OFF_HEAP_REGION_NAME, false);
      assertCompactionTimeStatOnVm(vm, 0, ASSERT_OP.EQUAL);

      // After freeing all memory we should have no compaction time
      doDestroyOnVm(vm, KEY, OFF_HEAP_REGION_NAME);
      assertCompactionTimeStatOnVm(vm, 0, ASSERT_OP.EQUAL);

      // Consume all off-heap memory using an allocation size
      int numAllocations = doConsumeOffHeapMemoryOnVm(vm, ALLOCATION_SIZE);
      assertTrue(numAllocations > 0);

      // Randomly free 3 allocations to produce off-heap gaps
      doFreeOffHeapMemoryOnVm(vm, numAllocations, 3);

      /*
       * Setup a compaction time attribute monitor
       */
      {
        setupOffHeapMonitorOnVm(vm, "OffHeapCompactionTime", 0, 0);
        clearNotificationListenerOnVm(vm);
      }

      // Allocate enough memory to force compaction which will update compaction time stat
      doPutOnVm(vm, KEY, new byte[NEW_ALLOCATION_SIZE], OFF_HEAP_REGION_NAME, true);

      // Make sure our compaction time monitor was triggered
      waitForNotificationListenerOnVm(vm, 5000, 500, true);

      /*
       * Make sure we have some compaction time. In some environments the compaction time is
       * reported as 0 due to time sample granularity and compaction speed.
       */
      assertCompactionTimeStatOnVm(vm, 0, ASSERT_OP.GREATER_THAN_OR_EQUAL);
    } finally {
      doCleanupOnVm(vm);
    }
  }

  /**
   * Asserts that a monitor assigned to the OffHeapObjects attribute is triggered.
   */
  @Test
  public void testOffHeapObjectsMonitoring() throws Exception {
    final VM vm = Host.getHost(0).getVM(0);

    try {
      // Setup off-heap memory for cache
      setSystemPropertiesOnVm(vm, true, getSystemProperties());

      // Create our off-heap region
      assertNotNull(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE));

      // Make sure our off-heap region has off-heap enabled.
      assertOffHeapRegionAttributesOnVm(vm);

      // Make sure our starting off heap stats are correct
      assertOffHeapMetricsOnVm(vm, TOTAL_MEMORY, 0, 0, 0);

      /*
       * Tests off-heap objects notification
       */
      {
        setupOffHeapMonitorOnVm(vm, "OffHeapObjects", 0, -1);

        clearNotificationListenerOnVm(vm);

        doPutOnVm(vm, KEY, VALUE, OFF_HEAP_REGION_NAME, false);

        waitForNotificationListenerOnVm(vm, 5000, 500, true);
      }

    } finally {
      doCleanupOnVm(vm);
    }
  }

  /**
   * Asserts that a monitor assigned to the OffHeapFreeSize attribute is triggered.
   */
  @Test
  public void testOffHeapFreeSizeMonitoring() throws Exception {
    final VM vm = Host.getHost(0).getVM(0);

    try {
      // Setup off-heap memory for cache
      setSystemPropertiesOnVm(vm, true, getSystemProperties());

      // Create our off-heap region
      assertNotNull(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE));

      // Make sure our off-heap region has off-heap enabled.
      assertOffHeapRegionAttributesOnVm(vm);

      // Make sure our starting off heap stats are correct
      assertOffHeapMetricsOnVm(vm, TOTAL_MEMORY, 0, 0, 0);

      /*
       * Tests off-heap objects notification
       */
      {
        setupOffHeapMonitorOnVm(vm, "OffHeapFreeSize", TOTAL_MEMORY, TOTAL_MEMORY);

        clearNotificationListenerOnVm(vm);

        doPutOnVm(vm, KEY, VALUE, OFF_HEAP_REGION_NAME, false);

        waitForNotificationListenerOnVm(vm, 5000, 500, true);
      }

    } finally {
      doCleanupOnVm(vm);
    }
  }

  /**
   * Asserts that a monitor assigned to the OffHeapAllocatedSize attribute is triggered.
   */
  @Test
  public void testOffHeapAllocatedSizeMonitoring() throws Exception {
    final VM vm = Host.getHost(0).getVM(0);

    try {
      // Setup off-heap memory for cache
      setSystemPropertiesOnVm(vm, true, getSystemProperties());

      // Create our off-heap region
      assertNotNull(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE));

      // Make sure our off-heap region has off-heap enabled.
      assertOffHeapRegionAttributesOnVm(vm);

      // Make sure our starting off heap stats are correct
      assertOffHeapMetricsOnVm(vm, TOTAL_MEMORY, 0, 0, 0);

      /*
       * Tests off-heap objects notification
       */
      {
        setupOffHeapMonitorOnVm(vm, "OffHeapAllocatedSize", 0, OBJECT_SIZE);

        clearNotificationListenerOnVm(vm);

        doPutOnVm(vm, KEY, VALUE, OFF_HEAP_REGION_NAME, false);

        waitForNotificationListenerOnVm(vm, 5000, 500, true);
      }

    } finally {
      doCleanupOnVm(vm);
    }
  }

  /**
   * Destroys a number of entries previously allocated.
   * 
   * @param vm a virtual machine
   * @param numAllocations the number of previous off-heap allocations
   * @param numDestroys the number of destroys to perform
   */
  protected void doFreeOffHeapMemoryOnVm(VM vm, final int numAllocations, final int numDestroys) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        doFreeOffHeapMemory(numAllocations, numDestroys);
      }
    });
  }

  /**
   * Performs some destroys to free off-heap allocations.
   * 
   * @param numAllocations the number of previous off-heap allocations
   * @param numDestroys the number of destroys to perform
   */
  protected void doFreeOffHeapMemory(int numAllocations, int numDestroys) {
    assertTrue(numDestroys <= numAllocations);

    Region region = getCache().getRegion(OFF_HEAP_REGION_NAME);
    assertNotNull(region);
    assertTrue(numDestroys <= region.size());

    String key = "KEY0";
    Object value = key;
    int destroyed = 0;

    while (destroyed < numDestroys) {
      key = "KEY" + ((int) (Math.random() * numAllocations));
      value = region.get(key);

      if (null != value) {
        region.destroy(key);
        ++destroyed;
      }
    }
  }

  /**
   * Consumes off off-heap memory until the allocation size cannot be satisfied.
   * 
   * @param vm a virtual machine
   * @param allocationSize the number of bytes for each allocation
   * @return the number of successful puts
   */
  protected int doConsumeOffHeapMemoryOnVm(VM vm, final int allocationSize) {
    return (Integer) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        return doConsumeOffHeapMemory(allocationSize);
      }
    });
  }

  /**
   * Consumes off off-heap memory until the allocation size cannot be satisfied.
   * 
   * @param allocationSize the number of bytes for each allocation
   * @return the number of successful puts
   */
  protected int doConsumeOffHeapMemory(int allocationSize) { // TODO:KIRK: change this to handle new
                                                             // OutOfOffHeapMemoryException
    OffHeapMemoryStats stats = ((GemFireCacheImpl) getCache()).getOffHeapStore().getStats();
    int i = 0;

    // Loop until we fail
    try {
      while (true) {
        doPut("KEY" + (i++), new byte[allocationSize], OFF_HEAP_REGION_NAME, false);
      }
    } catch (OutOfOffHeapMemoryException e) {
    }

    return i;
  }

  /**
   * Asserts that the compactionTime stat is available and satisfies an assert operation.
   * 
   * @param vm a virtual machine.
   * @param compactionTime total off heap compaction time.
   * @param op an assert operation.
   */
  protected void assertCompactionTimeStatOnVm(VM vm, final long compactionTime,
      final ASSERT_OP op) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        assertCompactionTimeStat(compactionTime, op);
      }
    });
  }

  /**
   * Asserts that the compactionTime stat is available and satisfies an assert operation.
   * 
   * @param compactionTime total off heap compaction time.
   * @param op an assert operation.
   */
  protected void assertCompactionTimeStat(long compactionTime, ASSERT_OP op) {
    ManagementService service = ManagementService.getExistingManagementService(getCache());
    assertNotNull(service);

    assertTrue(service.isManager());

    MemberMXBean memberBean = service.getMemberMXBean();
    assertNotNull(memberBean);

    switch (op) {
      case EQUAL:
        assertEquals(compactionTime, memberBean.getOffHeapCompactionTime());
        break;
      case GREATER_THAN:
        assertTrue(compactionTime < memberBean.getOffHeapCompactionTime());
        break;
      case GREATER_THAN_OR_EQUAL:
        assertTrue(compactionTime <= memberBean.getOffHeapCompactionTime());
        break;
      case LESS_THAN:
        assertTrue(compactionTime > memberBean.getOffHeapCompactionTime());
        break;
    }
  }

  /**
   * Asserts that the fragmentation stat is available and satisfies an assert operation.
   * 
   * @param vm a virtual machine
   * @param fragmentation a fragmentation percentage
   * @param op an assertion operation
   */
  protected void assertFragmentationStatOnVm(VM vm, final int fragmentation, final ASSERT_OP op) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        assertFragmentationStat(fragmentation, op);
      }
    });
  }

  /**
   * Asserts that the fragmentation stat is available and satisfies an assert operation.
   * 
   * @param fragmentation a fragmentation percentage
   * @param op an assertion operation
   */
  protected void assertFragmentationStat(int fragmentation, ASSERT_OP op) {
    ManagementService service = ManagementService.getExistingManagementService(getCache());
    assertNotNull(service);

    assertTrue(service.isManager());

    MemberMXBean memberBean = service.getMemberMXBean();
    assertNotNull(memberBean);

    switch (op) {
      case EQUAL:
        assertEquals(fragmentation, memberBean.getOffHeapFragmentation());
        break;
      case GREATER_THAN:
        assertTrue(fragmentation < memberBean.getOffHeapFragmentation());
        break;
      case LESS_THAN:
        assertTrue(fragmentation > memberBean.getOffHeapFragmentation());
        break;
    }
  }

  /**
   * Returns off-heap system properties for enabling off-heap and the JMX system.
   */
  protected Properties getSystemProperties() {
    Properties props = getDistributedSystemProperties();

    props.setProperty(OFF_HEAP_MEMORY_SIZE, "1m");
    props.setProperty(JMX_MANAGER, "true");
    props.setProperty(JMX_MANAGER_START, "true");
    props.setProperty(JMX_MANAGER_PORT, "0");

    return props;
  }

  /**
   * Removes off heap region on vm and disconnects.
   * 
   * @param vm a virutal machine.
   */
  protected void doCleanupOnVm(VM vm) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        cleanup();
      }
    });
  }

  /**
   * Removes off-heap region and disconnects.
   */
  protected void cleanup() {
    Cache existingCache = basicGetCache();

    if (null != existingCache && !existingCache.isClosed()) {
      Region region = getCache().getRegion(OFF_HEAP_REGION_NAME);

      if (null != region) {
        region.destroyRegion();
      }
    }

    disconnectFromDS();
  }

  /**
   * Asserts that the off heap region data is available and enabled for a VM.
   */
  @SuppressWarnings("serial")
  protected void assertOffHeapRegionAttributesOnVm(VM vm) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        assertOffHeapRegionAttributes();
      }
    });
  }

  /**
   * Asserts that the off heap region data is available and enabled.
   */
  protected void assertOffHeapRegionAttributes() {
    ManagementService service = ManagementService.getExistingManagementService(getCache());
    assertNotNull(service);

    assertTrue(service.isManager());

    RegionMXBean regionBean = service.getLocalRegionMBean(OFF_HEAP_REGION_PATH);
    assertNotNull(regionBean);

    RegionAttributesData regionData = regionBean.listRegionAttributes();
    assertNotNull(regionData);

    assertTrue(regionData.getOffHeap());
  }

  /**
   * Asserts that OffHeapMetrics match input parameters for a VM.
   * 
   * @param vm a virtual machine.
   * @param freeMemory total off-heap free memory in bytes.
   * @param allocatedMemory allocated (or used) off-heap memory in bytes.
   * @param objects number of objects stored in off-heap memory.
   * @param fragmentation the fragmentation percentage.
   */
  protected void assertOffHeapMetricsOnVm(VM vm, final long freeMemory, final long allocatedMemory,
      final long objects, final int fragmentation) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        assertOffHeapMetrics(freeMemory, allocatedMemory, objects, fragmentation);
      }
    });
  }

  /**
   * Asserts that OffHeapMetrics match input parameters.
   * 
   * @param freeMemory total off-heap free memory in bytes.
   * @param allocatedMemory allocated (or used) off-heap memory in bytes.
   * @param objects number of objects stored in off-heap memory.
   * @param fragmentation the fragmentation percentage.
   */
  protected void assertOffHeapMetrics(long freeMemory, long allocatedMemory, long objects,
      int fragmentation) {
    ManagementService service = ManagementService.getExistingManagementService(getCache());
    assertNotNull(service);

    assertTrue(service.isManager());

    MemberMXBean memberBean = service.getMemberMXBean();
    assertNotNull(memberBean);

    assertEquals(freeMemory, memberBean.getOffHeapFreeMemory());
    assertEquals(allocatedMemory, memberBean.getOffHeapUsedMemory());
    assertEquals(objects, memberBean.getOffHeapObjects());
    assertEquals(fragmentation, memberBean.getOffHeapFragmentation());
  }

  /**
   * Creates an off-heap region on a vm.
   * 
   * @param vm a virtual machine.
   * @param name a region name.
   * @param dataPolicy a data policy.
   * @return true if successful.
   */
  protected boolean createOffHeapRegionOnVm(final VM vm, final String name,
      final DataPolicy dataPolicy) {
    return (Boolean) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        return (null != createOffHeapRegion(name, dataPolicy));
      }
    });
  }

  /**
   * Creates an off-heap region.
   * 
   * @param name a region name.
   * @param dataPolicy a data policy.
   * @return the newly created region.
   */
  protected Region createOffHeapRegion(String name, DataPolicy dataPolicy) {
    return getCache().createRegionFactory().setOffHeap(true).setDataPolicy(dataPolicy).create(name);
  }

  /**
   * Sets the distributed system properties for a vm.
   * 
   * @param vm a virtual machine.
   * @param management starts the ManagementService when true.
   * @param props distributed system properties.
   */
  @SuppressWarnings("serial")
  protected void setSystemPropertiesOnVm(VM vm, final boolean management, final Properties props) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        setSystemProperties(management, props);
      }
    });
  }

  /**
   * Sets the distributed system properties.
   * 
   * @param management starts the ManagementService when true.
   * @param props distributed system properties.
   */
  protected void setSystemProperties(boolean management, Properties props) {
    getSystem(props);

    if (management) {
      ManagementService service = ManagementService.getManagementService(getCache());
      if (!service.isManager()) {
        service.startManager();
      }
    }
  }

  /**
   * Performs a destroy operation on a vm.
   * 
   * @param vm a virtual machine.
   * @param key the region entry to destroy.
   * @param regionName a region name.
   */
  protected void doDestroyOnVm(final VM vm, final Object key, final String regionName) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        doDestroy(key, regionName);
      }
    });
  }

  /**
   * Performs a destroy operation.
   * 
   * @param key the region entry to destroy.
   * @param regionName a region name.
   */
  protected void doDestroy(Object key, String regionName) {
    Region region = getCache().getRegion(regionName);
    assertNotNull(region);

    region.destroy(key);
  }

  /**
   * Performs a put operation on a vm.
   * 
   * @param vm a virtual machine.
   * @param key region entry key.
   * @param value region entry value.
   * @param regionName a region name.
   */
  protected void doPutOnVm(final VM vm, final Object key, final Object value,
      final String regionName, final boolean expectException) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        doPut(key, value, regionName, expectException);
      }
    });
  }

  /**
   * Performs a put operation.
   * 
   * @param key region entry key.
   * @param value region entry value.
   * @param regionName a region name.
   */
  protected void doPut(Object key, Object value, String regionName, boolean expectException) {
    Region region = getCache().getRegion(regionName);
    assertNotNull(region);

    try {
      region.put(key, value);
    } catch (OutOfOffHeapMemoryException e) {
      if (!expectException)
        throw e;
    }
  }

  /**
   * Adds an off-heap notification listener to the MemberMXBean for a vm.
   * 
   * @param vm a virtual machine.
   */
  protected void addOffHeapNotificationListenerOnVm(VM vm) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        addOffHeapNotificationListener();
      }
    });
  }

  /**
   * Adds an off-heap notification listener to the MemberMXBean.
   */
  protected void addOffHeapNotificationListener() {
    ManagementService service = ManagementService.getExistingManagementService(getCache());
    assertNotNull(service);

    assertTrue(service.isManager());

    MemberMXBean memberBean = service.getMemberMXBean();
    assertNotNull(memberBean);

    assertTrue(memberBean instanceof MemberMBean);

    ((MemberMBean) memberBean).addNotificationListener(notificationListener, null, null);
  }

  /**
   * Creates and adds a generic GaugeMonitor for an attribute of the MemberMXBean on a VM.
   * 
   * @param vm a virtual machine.
   * @param attribute the attribute to monitor.
   * @param highThreshold the high threshold trigger.
   * @param lowThreshold the low threshold trigger.
   */
  protected void setupOffHeapMonitorOnVm(VM vm, final String attribute, final long highThreshold,
      final long lowThreshold) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        setupOffHeapMonitor(attribute, highThreshold, lowThreshold);
      }
    });
  }

  /**
   * Creates and adds a generic GaugeMonitor for an attribute of the MemberMXBean.
   * 
   * @param attribute the attribute to monitor.
   * @param highThreshold the high threshold trigger.
   * @param lowThreshold the low threshold trigger.
   */
  protected void setupOffHeapMonitor(String attribute, long highThreshold, long lowThreshold) {
    ObjectName memberMBeanObjectName = MBeanJMXAdapter.getMemberMBeanName(
        InternalDistributedSystem.getConnectedInstance().getDistributedMember());
    assertNotNull(memberMBeanObjectName);

    try {
      ObjectName offHeapMonitorName = new ObjectName("monitors:type=Gauge,attr=" + attribute);
      mbeanServer.createMBean("javax.management.monitor.GaugeMonitor", offHeapMonitorName);

      AttributeList al = new AttributeList();
      al.add(new Attribute("ObservedObject", memberMBeanObjectName));
      al.add(new Attribute("GranularityPeriod", 500));
      al.add(new Attribute("ObservedAttribute", attribute));
      al.add(new Attribute("Notify", true));
      al.add(new Attribute("NotifyHigh", true));
      al.add(new Attribute("NotifyLow", true));
      al.add(new Attribute("HighTheshold", highThreshold));
      al.add(new Attribute("LowThreshold", lowThreshold));

      mbeanServer.setAttributes(offHeapMonitorName, al);
      mbeanServer.addNotificationListener(offHeapMonitorName, notificationListener, null, null);
      mbeanServer.invoke(offHeapMonitorName, "start", new Object[] {}, new String[] {});
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  /**
   * Waits to receive MBean notifications.
   * 
   * @param vm a virtual machine.
   * @param wait how long to wait for in millis.
   * @param interval the polling interval to check for notifications.
   * @param throwOnTimeout throws an exception on timeout if true.
   */
  protected void waitForNotificationListenerOnVm(VM vm, final long wait, final long interval,
      final boolean throwOnTimeout) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Wait.waitForCriterion(new WaitCriterion() {
          @Override
          public boolean done() {
            return (notificationListener.getNotificationSize() > 0);
          }

          @Override
          public String description() {
            return "Awaiting Notification Listener";
          }
        }, wait, interval, throwOnTimeout);
      }
    });
  }

  /**
   * Clears received notifications.
   * 
   * @param vm a virtual machine.
   */
  protected void clearNotificationListenerOnVm(VM vm) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        notificationListener.clear();
      }
    });
  }
}


/**
 * Collects MBean Notifications.
 */
class OffHeapNotificationListener implements NotificationListener {
  List<Notification> notificationList = Collections.synchronizedList(new ArrayList<Notification>());

  @Override
  public void handleNotification(Notification notification, Object handback) {
    this.notificationList.add(notification);
  }

  public void clear() {
    this.notificationList.clear();
  }

  public int getNotificationSize() {
    return this.notificationList.size();
  }
}
