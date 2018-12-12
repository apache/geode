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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import com.google.common.base.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.OutOfOffHeapMemoryException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.offheap.OffHeapStorage;
import org.apache.geode.internal.offheap.OffHeapStoredObject;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.JMXTest;

/**
 * Tests the off-heap additions to the RegionMXBean and MemberMXBean JMX interfaces.
 */
@Category({JMXTest.class})
@SuppressWarnings("serial")
public class OffHeapManagementDUnitTest extends CacheTestCase {

  /**
   * Specified assertion operations.
   */
  private enum ASSERT_OP {
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
  private static final int TOTAL_MEMORY = 1048576;

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
  private static MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

  private VM vm;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    vm = Host.getHost(0).getVM(0);

    vm.invoke(() -> {
      System.setProperty(OffHeapStorage.STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY, "true");
    });
  }

  @After
  public void tearDown() throws Exception {
    doCleanupOnVm(vm);
  }

  /**
   * Returns off-heap system properties for enabling off-heap and the JMX system.
   */
  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();

    config.setProperty(OFF_HEAP_MEMORY_SIZE, "1m");
    config.setProperty(JMX_MANAGER, "true");
    config.setProperty(JMX_MANAGER_START, "true");
    config.setProperty(JMX_MANAGER_PORT, "0");

    return config;
  }

  /**
   * Tests off-heap additions to the RegionMXBean and MemberMXBean interfaces.
   */
  @Test
  public void testOffHeapMBeanAttributesAndStats() throws Exception {
    // Setup off-heap memory for cache
    setSystemPropertiesOnVm(vm, true, getDistributedSystemProperties());

    // Create our off-heap region
    assertThat(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE)).isNotNull();

    // Make sure our off-heap region has off-heap enabled.
    assertOffHeapRegionAttributesOnVm(vm);

    // Make sure our starting off heap stats are correct
    assertOffHeapMetricsOnVm(vm, TOTAL_MEMORY, 0, 0, 0);

    /*
     * Perform ops on the off-heap region and assert that the off-heap metrics correctly reflect the
     * ops
     */
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

  /**
   * Tests the fragmentation statistic for off-heap memory.
   */
  @Test
  public void testFragmentationStat() throws Exception {
    // Setup off-heap memory for cache
    setSystemPropertiesOnVm(vm, true, getDistributedSystemProperties());

    // Create our off-heap region
    assertThat(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE)).isNotNull();
    vm.invoke(() -> {
      Region region = getCache().getRegion(OFF_HEAP_REGION_NAME);
      assertThat(region).isNotNull();
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
    setupOffHeapMonitorOnVm(vm, "OffHeapFragmentation", 0, 0);
    clearNotificationListenerOnVm(vm);

    // Make sure we have some fragmentation
    assertFragmentationStatOnVm(vm, 100, ASSERT_OP.EQUAL);

    // Make sure our fragmentation monitor was triggered
    waitForNotificationListenerOnVm(vm, 5000);
  }

  /**
   * Tests the compaction time statistic for off-heap memory.
   */
  @Test
  public void testCompactionTimeStat() throws Exception {
    // Setup off-heap memory for cache
    setSystemPropertiesOnVm(vm, true, getDistributedSystemProperties());

    // Create our off-heap region
    assertThat(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE)).isNotNull();

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
    assertThat(numAllocations > 0).isTrue();

    // Randomly free 3 allocations to produce off-heap gaps
    doFreeOffHeapMemoryOnVm(vm, numAllocations, 3);

    /*
     * Setup a compaction time attribute monitor
     */
    setupOffHeapMonitorOnVm(vm, "OffHeapCompactionTime", 0, 0);
    clearNotificationListenerOnVm(vm);

    // Allocate enough memory to force compaction which will update compaction time stat
    doPutOnVm(vm, KEY, new byte[NEW_ALLOCATION_SIZE], OFF_HEAP_REGION_NAME, true);

    // Make sure our compaction time monitor was triggered
    waitForNotificationListenerOnVm(vm, 5000);

    /*
     * Make sure we have some compaction time. In some environments the compaction time is reported
     * as 0 due to time sample granularity and compaction speed.
     */
    assertCompactionTimeStatOnVm(vm, 0, ASSERT_OP.GREATER_THAN_OR_EQUAL);
  }

  /**
   * Asserts that a monitor assigned to the OffHeapObjects attribute is triggered.
   */
  @Test
  public void testOffHeapObjectsMonitoring() throws Exception {
    // Setup off-heap memory for cache
    setSystemPropertiesOnVm(vm, true, getDistributedSystemProperties());

    // Create our off-heap region
    assertThat(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE)).isNotNull();

    // Make sure our off-heap region has off-heap enabled.
    assertOffHeapRegionAttributesOnVm(vm);

    // Make sure our starting off heap stats are correct
    assertOffHeapMetricsOnVm(vm, TOTAL_MEMORY, 0, 0, 0);

    /*
     * Tests off-heap objects notification
     */
    setupOffHeapMonitorOnVm(vm, "OffHeapObjects", 0, -1);

    clearNotificationListenerOnVm(vm);

    doPutOnVm(vm, KEY, VALUE, OFF_HEAP_REGION_NAME, false);

    waitForNotificationListenerOnVm(vm, 5000);
  }

  /**
   * Asserts that a monitor assigned to the OffHeapFreeSize attribute is triggered.
   */
  @Test
  public void testOffHeapFreeSizeMonitoring() throws Exception {
    // Setup off-heap memory for cache
    setSystemPropertiesOnVm(vm, true, getDistributedSystemProperties());

    // Create our off-heap region
    assertThat(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE)).isNotNull();

    // Make sure our off-heap region has off-heap enabled.
    assertOffHeapRegionAttributesOnVm(vm);

    // Make sure our starting off heap stats are correct
    assertOffHeapMetricsOnVm(vm, TOTAL_MEMORY, 0, 0, 0);

    /*
     * Tests off-heap objects notification
     */
    setupOffHeapMonitorOnVm(vm, "OffHeapFreeSize", TOTAL_MEMORY, TOTAL_MEMORY);

    clearNotificationListenerOnVm(vm);

    doPutOnVm(vm, KEY, VALUE, OFF_HEAP_REGION_NAME, false);

    waitForNotificationListenerOnVm(vm, 5000);
  }

  /**
   * Asserts that a monitor assigned to the OffHeapAllocatedSize attribute is triggered.
   */
  @Test
  public void testOffHeapAllocatedSizeMonitoring() throws Exception {
    // Setup off-heap memory for cache
    setSystemPropertiesOnVm(vm, true, getDistributedSystemProperties());

    // Create our off-heap region
    assertThat(createOffHeapRegionOnVm(vm, OFF_HEAP_REGION_NAME, DataPolicy.REPLICATE)).isNotNull();

    // Make sure our off-heap region has off-heap enabled.
    assertOffHeapRegionAttributesOnVm(vm);

    // Make sure our starting off heap stats are correct
    assertOffHeapMetricsOnVm(vm, TOTAL_MEMORY, 0, 0, 0);

    /*
     * Tests off-heap objects notification
     */
    setupOffHeapMonitorOnVm(vm, "OffHeapAllocatedSize", 0, OBJECT_SIZE);

    clearNotificationListenerOnVm(vm);

    doPutOnVm(vm, KEY, VALUE, OFF_HEAP_REGION_NAME, false);

    waitForNotificationListenerOnVm(vm, 5000);
  }

  /**
   * Destroys a number of entries previously allocated.
   *
   * @param vm a virtual machine
   * @param numAllocations the number of previous off-heap allocations
   * @param numDestroys the number of destroys to perform
   */
  private void doFreeOffHeapMemoryOnVm(final VM vm, final int numAllocations,
      final int numDestroys) {
    vm.invoke(() -> {
      doFreeOffHeapMemory(numAllocations, numDestroys);
    });
  }

  /**
   * Performs some destroys to free off-heap allocations.
   *
   * @param numAllocations the number of previous off-heap allocations
   * @param numDestroys the number of destroys to perform
   */
  private void doFreeOffHeapMemory(final int numAllocations, final int numDestroys) {
    assertThat(numDestroys <= numAllocations).isTrue();

    Region region = getCache().getRegion(OFF_HEAP_REGION_NAME);
    assertThat(region).isNotNull();
    assertThat(numDestroys <= region.size()).isTrue();

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
   *
   * @return the number of successful puts
   */
  private int doConsumeOffHeapMemoryOnVm(final VM vm, final int allocationSize) {
    return vm.invoke(() -> doConsumeOffHeapMemory(allocationSize));
  }

  /**
   * Consumes off off-heap memory until the allocation size cannot be satisfied.
   *
   * @param allocationSize the number of bytes for each allocation
   *
   * @return the number of successful puts
   */
  private int doConsumeOffHeapMemory(final int allocationSize) {
    int i = 0;

    // Loop until we fail
    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      while (stopwatch.elapsed(MINUTES) < 2) {
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
  private void assertCompactionTimeStatOnVm(final VM vm, final long compactionTime,
      final ASSERT_OP op) {
    vm.invoke(() -> assertCompactionTimeStat(compactionTime, op));
  }

  /**
   * Asserts that the compactionTime stat is available and satisfies an assert operation.
   *
   * @param compactionTime total off heap compaction time.
   * @param op an assert operation.
   */
  private void assertCompactionTimeStat(final long compactionTime, final ASSERT_OP op) {
    ManagementService service = ManagementService.getExistingManagementService(getCache());
    assertThat(service).isNotNull();

    assertThat(service.isManager()).isTrue();

    MemberMXBean memberBean = service.getMemberMXBean();
    assertThat(memberBean).isNotNull();

    switch (op) {
      case EQUAL:
        assertThat(memberBean.getOffHeapCompactionTime()).isEqualTo(compactionTime);
        break;
      case GREATER_THAN:
        assertThat(compactionTime < memberBean.getOffHeapCompactionTime()).isTrue();
        break;
      case GREATER_THAN_OR_EQUAL:
        assertThat(compactionTime <= memberBean.getOffHeapCompactionTime()).isTrue();
        break;
      case LESS_THAN:
        assertThat(compactionTime > memberBean.getOffHeapCompactionTime()).isTrue();
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
  private void assertFragmentationStatOnVm(final VM vm, final int fragmentation,
      final ASSERT_OP op) {
    vm.invoke(() -> assertFragmentationStat(fragmentation, op));
  }

  /**
   * Asserts that the fragmentation stat is available and satisfies an assert operation.
   *
   * @param fragmentation a fragmentation percentage
   * @param op an assertion operation
   */
  private void assertFragmentationStat(final int fragmentation, final ASSERT_OP op) {
    ManagementService service = ManagementService.getExistingManagementService(getCache());
    assertThat(service).isNotNull();

    assertThat(service.isManager()).isTrue();

    MemberMXBean memberBean = service.getMemberMXBean();
    assertThat(memberBean).isNotNull();

    switch (op) {
      case EQUAL:
        assertThat(memberBean.getOffHeapFragmentation()).isEqualTo(fragmentation);
        break;
      case GREATER_THAN:
        assertThat(fragmentation < memberBean.getOffHeapFragmentation()).isTrue();
        break;
      case LESS_THAN:
        assertThat(fragmentation > memberBean.getOffHeapFragmentation()).isTrue();
        break;
    }
  }

  /**
   * Removes off heap region on vm and disconnects.
   *
   * @param vm a virtual machine.
   */
  private void doCleanupOnVm(final VM vm) {
    vm.invoke(() -> cleanup());
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
  private void assertOffHeapRegionAttributesOnVm(final VM vm) {
    vm.invoke(() -> assertOffHeapRegionAttributes());
  }

  /**
   * Asserts that the off heap region data is available and enabled.
   */
  private void assertOffHeapRegionAttributes() {
    ManagementService service = ManagementService.getExistingManagementService(getCache());
    assertThat(service).isNotNull();

    assertThat(service.isManager()).isTrue();

    RegionMXBean regionBean = service.getLocalRegionMBean(OFF_HEAP_REGION_PATH);
    assertThat(regionBean).isNotNull();

    RegionAttributesData regionData = regionBean.listRegionAttributes();
    assertThat(regionData).isNotNull();

    assertThat(regionData.getOffHeap()).isTrue();
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
  private void assertOffHeapMetricsOnVm(final VM vm, final int freeMemory,
      final int allocatedMemory, final int objects, final int fragmentation) {
    vm.invoke(() -> assertOffHeapMetrics(freeMemory, allocatedMemory, objects, fragmentation));
  }

  /**
   * Asserts that OffHeapMetrics match input parameters.
   *
   * @param freeMemory total off-heap free memory in bytes.
   * @param allocatedMemory allocated (or used) off-heap memory in bytes.
   * @param objects number of objects stored in off-heap memory.
   * @param fragmentation the fragmentation percentage.
   */
  private void assertOffHeapMetrics(final int freeMemory, final int allocatedMemory,
      final int objects, final int fragmentation) {
    ManagementService service = ManagementService.getExistingManagementService(getCache());
    assertThat(service).isNotNull();

    assertThat(service.isManager()).isTrue();

    MemberMXBean memberBean = service.getMemberMXBean();
    assertThat(memberBean).isNotNull();

    assertThat(memberBean.getOffHeapFreeMemory()).isEqualTo(freeMemory);
    assertThat(memberBean.getOffHeapUsedMemory()).isEqualTo(allocatedMemory);
    assertThat(memberBean.getOffHeapObjects()).isEqualTo(objects);
    assertThat(memberBean.getOffHeapFragmentation()).isEqualTo(fragmentation);
  }

  /**
   * Creates an off-heap region on a vm.
   *
   * @param vm a virtual machine.
   * @param name a region name.
   * @param dataPolicy a data policy.
   *
   * @return true if successful.
   */
  private boolean createOffHeapRegionOnVm(final VM vm, final String name,
      final DataPolicy dataPolicy) {
    return vm.invoke(() -> null != createOffHeapRegion(name, dataPolicy));
  }

  /**
   * Creates an off-heap region.
   *
   * @param name a region name.
   * @param dataPolicy a data policy.
   *
   * @return the newly created region.
   */
  private Region createOffHeapRegion(final String name, final DataPolicy dataPolicy) {
    return getCache().createRegionFactory().setOffHeap(true).setDataPolicy(dataPolicy).create(name);
  }

  /**
   * Sets the distributed system properties for a vm.
   *
   * @param vm a virtual machine.
   * @param management starts the ManagementService when true.
   * @param props distributed system properties.
   */
  private void setSystemPropertiesOnVm(final VM vm, final boolean management,
      final Properties props) {
    vm.invoke(() -> setSystemProperties(management, props));
  }

  /**
   * Sets the distributed system properties.
   *
   * @param management starts the ManagementService when true.
   * @param props distributed system properties.
   */
  private void setSystemProperties(final boolean management, final Properties props) {
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
  private void doDestroyOnVm(final VM vm, final Object key, final String regionName) {
    vm.invoke(() -> doDestroy(key, regionName));
  }

  /**
   * Performs a destroy operation.
   *
   * @param key the region entry to destroy.
   * @param regionName a region name.
   */
  private void doDestroy(final Object key, final String regionName) {
    Region region = getCache().getRegion(regionName);
    assertThat(region).isNotNull();

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
  private void doPutOnVm(final VM vm, final Object key, final Object value, final String regionName,
      final boolean expectException) {
    vm.invoke(() -> doPut(key, value, regionName, expectException));
  }

  /**
   * Performs a put operation.
   *
   * @param key region entry key.
   * @param value region entry value.
   * @param regionName a region name.
   */
  private void doPut(final Object key, final Object value, final String regionName,
      final boolean expectException) {
    Region region = getCache().getRegion(regionName);
    assertThat(region).isNotNull();

    try {
      region.put(key, value);
      if (expectException) {
        fail("Expected OutOfOffHeapMemoryException");
      }
    } catch (OutOfOffHeapMemoryException e) {
      if (!expectException) {
        throw e;
      }
    }
  }

  /**
   * Creates and adds a generic GaugeMonitor for an attribute of the MemberMXBean on a VM.
   *
   * @param vm a virtual machine.
   * @param attribute the attribute to monitor.
   * @param highThreshold the high threshold trigger.
   * @param lowThreshold the low threshold trigger.
   */
  private void setupOffHeapMonitorOnVm(final VM vm, final String attribute, final int highThreshold,
      final int lowThreshold) {
    vm.invoke(() -> setupOffHeapMonitor(attribute, highThreshold, lowThreshold));
  }

  /**
   * Creates and adds a generic GaugeMonitor for an attribute of the MemberMXBean.
   *
   * @param attribute the attribute to monitor.
   * @param highThreshold the high threshold trigger.
   * @param lowThreshold the low threshold trigger.
   */
  private void setupOffHeapMonitor(final String attribute, final int highThreshold,
      final int lowThreshold) throws JMException {
    ObjectName memberMBeanObjectName = MBeanJMXAdapter.getMemberMBeanName(
        InternalDistributedSystem.getConnectedInstance().getDistributedMember());
    assertThat(memberMBeanObjectName).isNotNull();

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
  }

  /**
   * Waits to receive MBean notifications.
   *
   * @param vm a virtual machine.
   * @param wait how long to wait for in millis.
   */
  private void waitForNotificationListenerOnVm(final VM vm, final long wait) {
    vm.invoke(() -> await("Awaiting Notification Listener")
        .untilAsserted(() -> assertThat(notificationListener.getNotificationSize() > 0).isTrue()));
  }

  /**
   * Clears received notifications.
   *
   * @param vm a virtual machine.
   */
  private void clearNotificationListenerOnVm(final VM vm) {
    vm.invoke(() -> notificationListener.clear());
  }

  /**
   * Collects MBean Notifications.
   */
  private static class OffHeapNotificationListener implements NotificationListener {

    private List<Notification> notificationList =
        Collections.synchronizedList(new ArrayList<Notification>());

    @Override
    public void handleNotification(final Notification notification, final Object handback) {
      notificationList.add(notification);
    }

    void clear() {
      notificationList.clear();
    }

    int getNotificationSize() {
      return notificationList.size();
    }
  }
}
