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
package org.apache.geode.internal.cache.wan.parallel;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Properties;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.cache30.MyGatewayEventFilter1;
import org.apache.geode.cache30.MyGatewayTransportFilter1;
import org.apache.geode.cache30.MyGatewayTransportFilter2;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.WanTest;

/**
 * DUnit for ParallelSenderQueue overflow operations.
 */
@Category({WanTest.class})
public class ParallelGatewaySenderQueueOverflowDUnitTest extends WANTestBase {

  @Test
  public void testParallelSenderQueueEventsOverflow_NoDiskStoreSpecified() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSenderWithoutDiskStore("ln", 2, 10, 10, false, true));
    vm5.invoke(() -> WANTestBase.createSenderWithoutDiskStore("ln", 2, 10, 10, false, true));
    vm6.invoke(() -> WANTestBase.createSenderWithoutDiskStore("ln", 2, 10, 10, false, true));
    vm7.invoke(() -> WANTestBase.createSenderWithoutDiskStore("ln", 2, 10, 10, false, true));

    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    // give some time for the senders to pause
    Wait.pause(1000);

    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));

    int numEventPuts = 50;
    vm4.invoke(() -> WANTestBase.doHeavyPuts(getTestMethodName(), numEventPuts));


    // considering a memory limit of 40 MB, maximum of 40 events can be in memory. Rest should be on
    // disk.
    await().untilAsserted(() -> {
      long numOvVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
      long numOvVm5 = (Long) vm5.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
      long numOvVm6 = (Long) vm6.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
      long numOvVm7 = (Long) vm7.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));

      long numMemVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
      long numMemVm5 = (Long) vm5.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
      long numMemVm6 = (Long) vm6.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
      long numMemVm7 = (Long) vm7.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));

      LogWriterUtils.getLogWriter().info("Entries overflown to disk: " + numOvVm4 + "," + numOvVm5
          + "," + numOvVm6 + "," + numOvVm7);
      LogWriterUtils.getLogWriter().info(
          "Entries in VM: " + numMemVm4 + "," + numMemVm5 + "," + numMemVm6 + "," + numMemVm7);
      long totalOverflown = numOvVm4 + numOvVm5 + numOvVm6 + numOvVm7;
      assertTrue("Total number of entries overflown to disk should be at least greater than 55",
          (totalOverflown > 55));

      long totalInMemory = numMemVm4 + numMemVm5 + numMemVm6 + numMemVm7;
      // expected is twice the number of events put due to redundancy level of 1
      assertEquals("Total number of entries on disk and in VM is incorrect", (numEventPuts * 2),
          (totalOverflown + totalInMemory));

    });

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 50, 240000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 50, 240000));
  }

  /**
   * Keep same max memory limit for all the VMs
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testParallelSenderQueueEventsOverflow() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 10, false, false, null, true));

    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    // give some time for the senders to pause
    Wait.pause(1000);

    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));

    int numEventPuts = 50;
    vm4.invoke(() -> WANTestBase.doHeavyPuts(getTestMethodName(), numEventPuts));

    long numOvVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
    long numOvVm5 = (Long) vm5.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
    long numOvVm6 = (Long) vm6.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
    long numOvVm7 = (Long) vm7.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));

    long numMemVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
    long numMemVm5 = (Long) vm5.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
    long numMemVm6 = (Long) vm6.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
    long numMemVm7 = (Long) vm7.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));

    LogWriterUtils.getLogWriter().info("Entries overflown to disk: " + numOvVm4 + "," + numOvVm5
        + "," + numOvVm6 + "," + numOvVm7);
    LogWriterUtils.getLogWriter()
        .info("Entries in VM: " + numMemVm4 + "," + numMemVm5 + "," + numMemVm6 + "," + numMemVm7);

    long totalOverflown = numOvVm4 + numOvVm5 + numOvVm6 + numOvVm7;
    // considering a memory limit of 40 MB, maximum of 40 events can be in memory. Rest should be on
    // disk.
    assertTrue("Total number of entries overflown to disk should be at least greater than 55",
        (totalOverflown > 55));

    long totalInMemory = numMemVm4 + numMemVm5 + numMemVm6 + numMemVm7;
    // expected is twice the number of events put due to redundancy level of 1
    assertEquals("Total number of entries on disk and in VM is incorrect", (numEventPuts * 2),
        (totalOverflown + totalInMemory));

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 50));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 50));
  }

  /**
   * Set a different memory limit for each VM and make sure that all the VMs are utilized to full
   * extent of available memory.
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testParallelSenderQueueEventsOverflow_2() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 5, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 5, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 20, 10, false, false, null, true));

    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    // give some time for the senders to pause
    Wait.pause(1000);

    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));

    int numEventPuts = 50;
    vm4.invoke(() -> WANTestBase.doHeavyPuts(getTestMethodName(), numEventPuts));

    long numOvVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
    long numOvVm5 = (Long) vm5.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
    long numOvVm6 = (Long) vm6.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
    long numOvVm7 = (Long) vm7.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));

    long numMemVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
    long numMemVm5 = (Long) vm5.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
    long numMemVm6 = (Long) vm6.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
    long numMemVm7 = (Long) vm7.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));

    LogWriterUtils.getLogWriter().info("Entries overflown to disk: " + numOvVm4 + "," + numOvVm5
        + "," + numOvVm6 + "," + numOvVm7);
    LogWriterUtils.getLogWriter()
        .info("Entries in VM: " + numMemVm4 + "," + numMemVm5 + "," + numMemVm6 + "," + numMemVm7);

    long totalOverflown = numOvVm4 + numOvVm5 + numOvVm6 + numOvVm7;
    // considering a memory limit of 40 MB, maximum of 40 events can be in memory. Rest should be on
    // disk.
    assertTrue("Total number of entries overflown to disk should be at least greater than 55",
        (totalOverflown > 55));

    long totalInMemory = numMemVm4 + numMemVm5 + numMemVm6 + numMemVm7;
    // expected is twice the number of events put due to redundancy level of 1
    assertEquals("Total number of entries on disk and in VM is incorrect", (numEventPuts * 2),
        (totalOverflown + totalInMemory));

    // assert the numbers for each VM
    assertTrue("Number of entries in memory VM4 is incorrect. Should be less than 10",
        (numMemVm4 < 10));
    assertTrue("Number of entries in memory VM5 is incorrect. Should be less than 5",
        (numMemVm5 < 5));
    assertTrue("Number of entries in memory VM6 is incorrect. Should be less than 5",
        (numMemVm6 < 5));
    assertTrue("Number of entries in memory VM7 is incorrect. Should be less than 20",
        (numMemVm7 < 20));

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 50));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 50));
  }

  @Ignore("TODO: test is disabled")
  @Test
  public void testParallelSenderQueueNoEventsOverflow() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 10, false, false, null, true));

    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    // give some time for the senders to pause
    Wait.pause(1000);

    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));

    int numEventPuts = 15;
    vm4.invoke(() -> WANTestBase.doHeavyPuts(getTestMethodName(), numEventPuts));

    long numOvVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
    long numOvVm5 = (Long) vm5.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
    long numOvVm6 = (Long) vm6.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));
    long numOvVm7 = (Long) vm7.invoke(() -> WANTestBase.getNumberOfEntriesOverflownToDisk("ln"));

    long numMemVm4 = (Long) vm4.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
    long numMemVm5 = (Long) vm5.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
    long numMemVm6 = (Long) vm6.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));
    long numMemVm7 = (Long) vm7.invoke(() -> WANTestBase.getNumberOfEntriesInVM("ln"));

    LogWriterUtils.getLogWriter().info("Entries overflown to disk: " + numOvVm4 + "," + numOvVm5
        + "," + numOvVm6 + "," + numOvVm7);
    LogWriterUtils.getLogWriter()
        .info("Entries in VM: " + numMemVm4 + "," + numMemVm5 + "," + numMemVm6 + "," + numMemVm7);

    long totalOverflown = numOvVm4 + numOvVm5 + numOvVm6 + numOvVm7;
    // all 30 (considering redundant copies) events should accommodate in 40 MB space given to 4
    // senders
    assertEquals("Total number of entries overflown to disk is incorrect", 0, totalOverflown);

    long totalInMemory = numMemVm4 + numMemVm5 + numMemVm6 + numMemVm7;
    // expected is twice the number of events put due to redundancy level of 1
    assertEquals("Total number of entries on disk and in VM is incorrect", (numEventPuts * 2),
        (totalOverflown + totalInMemory));

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 15));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 15));
  }

  /**
   * Test to validate that ParallelGatewaySenderQueue diskSynchronous attribute when persistence of
   * sender is enabled.
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void test_ValidateParallelGatewaySenderQueueAttributes_1() {
    Integer localLocPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer remoteLocPort =
        (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, localLocPort));

    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + localLocPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    File directory =
        new File("TKSender" + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    directory.mkdir();
    File[] dirs1 = new File[] {directory};
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setDiskDirs(dirs1);
    DiskStore diskStore = dsf.create("FORNY");

    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setParallel(true);// set parallel to true
    fact.setBatchConflationEnabled(true);
    fact.setBatchSize(200);
    fact.setBatchTimeInterval(300);
    fact.setPersistenceEnabled(true);// enable the persistence
    fact.setDiskSynchronous(true);
    fact.setDiskStoreName("FORNY");
    fact.setMaximumQueueMemory(200);
    fact.setAlertThreshold(1200);
    GatewayEventFilter myEventFilter1 = new MyGatewayEventFilter1();
    fact.addGatewayEventFilter(myEventFilter1);
    GatewayTransportFilter myStreamFilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamFilter1);
    GatewayTransportFilter myStreamFilter2 = new MyGatewayTransportFilter2();
    fact.addGatewayTransportFilter(myStreamFilter2);
    final IgnoredException exTKSender = IgnoredException.addIgnoredException("Could not connect");
    try {
      GatewaySender sender1 = fact.create("TKSender", 2);

      AttributesFactory factory = new AttributesFactory();
      factory.addGatewaySenderId(sender1.getId());
      factory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
      Region region = cache.createRegionFactory(factory.create())
          .create("test_ValidateGatewaySenderAttributes");
      Set<GatewaySender> senders = cache.getGatewaySenders();
      assertEquals(senders.size(), 1);
      GatewaySender gatewaySender = senders.iterator().next();
      Set<RegionQueue> regionQueues = ((AbstractGatewaySender) gatewaySender).getQueues();
      assertEquals(regionQueues.size(), 1);
      RegionQueue regionQueue = regionQueues.iterator().next();
      assertEquals(true, regionQueue.getRegion().getAttributes().isDiskSynchronous());
    } finally {
      exTKSender.remove();
    }
  }

  /**
   * Test to validate that ParallelGatewaySenderQueue diskSynchronous attribute when persistence of
   * sender is not enabled.
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void test_ValidateParallelGatewaySenderQueueAttributes_2() {
    Integer localLocPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer remoteLocPort =
        (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, localLocPort));

    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + localLocPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setParallel(true);// set parallel to true
    fact.setBatchConflationEnabled(true);
    fact.setBatchSize(200);
    fact.setBatchTimeInterval(300);
    fact.setPersistenceEnabled(false);// set persistence to false
    fact.setDiskSynchronous(true);
    fact.setMaximumQueueMemory(200);
    fact.setAlertThreshold(1200);
    GatewayEventFilter myEventFilter1 = new MyGatewayEventFilter1();
    fact.addGatewayEventFilter(myEventFilter1);
    GatewayTransportFilter myStreamFilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamFilter1);
    GatewayTransportFilter myStreamFilter2 = new MyGatewayTransportFilter2();
    fact.addGatewayTransportFilter(myStreamFilter2);
    final IgnoredException ex = IgnoredException.addIgnoredException("Could not connect");
    try {
      GatewaySender sender1 = fact.create("TKSender", 2);
      AttributesFactory factory = new AttributesFactory();
      factory.addGatewaySenderId(sender1.getId());
      factory.setDataPolicy(DataPolicy.PARTITION);
      Region region = cache.createRegionFactory(factory.create())
          .create("test_ValidateGatewaySenderAttributes");
      Set<GatewaySender> senders = cache.getGatewaySenders();
      assertEquals(senders.size(), 1);
      GatewaySender gatewaySender = senders.iterator().next();
      Set<RegionQueue> regionQueues = ((AbstractGatewaySender) gatewaySender).getQueues();
      assertEquals(regionQueues.size(), 1);
      RegionQueue regionQueue = regionQueues.iterator().next();
      assertEquals(false, regionQueue.getRegion().getAttributes().isDiskSynchronous());
    } finally {
      ex.remove();
    }
  }
}
