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
package org.apache.geode.internal.cache.wan.serial;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.Wait;

/**
 * 
 */
@Category(DistributedTest.class)
public class SerialWANPersistenceEnabledGatewaySenderDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public SerialWANPersistenceEnabledGatewaySenderDUnitTest() {
    super();
  }

  /**
   * Just enable the persistence for GatewaySender and see if it remote site receives all the
   * events.
   */
  @Test
  public void testReplicatedRegionWithGatewaySenderPersistenceEnabled() {

    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, true, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, true, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

  }

  /**
   * Enable persistence for the Region and see if the remote site gets all the events.
   */
  @Test
  public void testPersistentReplicatedRegionWithGatewaySender() {

    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", null,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", null,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

  }

  /**
   * Enable persistence for region as well as GatewaySender and see if remote site receives all the
   * events.
   * 
   */
  @Test
  public void testPersistentReplicatedRegionWithGatewaySenderPersistenceEnabled() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, true, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, true, null, true));

    vm2.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", null,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", null,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

  }

  /**
   * Enable persistence for GatewaySender, kill the sender and restart it. Check if the remote site
   * receives all the event.
   */
  @Test
  public void testReplicatedRegionWithGatewaySenderPersistenceEnabled_Restart() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    String firstDStore = (String) vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        false, 100, 10, false, true, null, null, true));
    String secondDStore = (String) vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        false, 100, 10, false, true, null, null, true));

    LogWriterUtils.getLogWriter().info("The first ds is " + firstDStore);
    LogWriterUtils.getLogWriter().info("The first ds is " + secondDStore);

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    LogWriterUtils.getLogWriter().info("Completed puts in the region");

    // verify if the queue has all the events
    // vm4.invoke(() -> WANTestBase.checkQueueSize( "ln", 1000
    // ));
    // vm5.invoke(() -> WANTestBase.checkQueueSize( "ln", 1000
    // ));
    //
    // vm2.invoke(() -> WANTestBase.validateRegionSize(
    // testName + "_RR", 0 ));
    // vm3.invoke(() -> WANTestBase.validateRegionSize(
    // testName + "_RR", 0 ));

    // kill the vm
    vm4.invoke(() -> WANTestBase.killSender());
    vm5.invoke(() -> WANTestBase.killSender());
    vm6.invoke(() -> WANTestBase.killSender());
    vm7.invoke(() -> WANTestBase.killSender());

    LogWriterUtils.getLogWriter().info("Killed all the sender. ");
    // restart the vm
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, false, 100, 10, false, true,
        null, firstDStore, true));
    LogWriterUtils.getLogWriter().info("Created the sender.... in vm4 ");
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, false, 100, 10, false, true,
        null, secondDStore, true));
    LogWriterUtils.getLogWriter().info("Created the sender.... in vm5 ");
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.startSender("ln"));
    LogWriterUtils.getLogWriter().info("Started the sender in vm 4");

    vm5.invoke(() -> WANTestBase.startSender("ln"));
    LogWriterUtils.getLogWriter().info("Started the sender in vm 5");
    try {
      inv1.join();
    } catch (InterruptedException e) {
      fail("Got interrupted exception while waiting for startSender to finish.");
    }

    Wait.pause(5000);

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

  }

  /**
   * Enable persistence for Region and persistence for GatewaySender. Kill the vm with regions and
   * bring that up again. Check if the remote site receives all the event. again?
   * 
   */
  @Test
  public void testPersistentReplicatedRegionWithGatewaySenderPersistenceEnabled_Restart() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    String firstDStore = (String) vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        false, 100, 10, false, true, null, null, true));
    String secondDStore = (String) vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        false, 100, 10, false, true, null, null, true));

    LogWriterUtils.getLogWriter().info("The first ds is " + firstDStore);
    LogWriterUtils.getLogWriter().info("The first ds is " + secondDStore);

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    LogWriterUtils.getLogWriter().info("Completed puts in the region");

    // kill the vm
    vm4.invoke(() -> WANTestBase.killSender());
    vm5.invoke(() -> WANTestBase.killSender());

    LogWriterUtils.getLogWriter().info("Killed the sender. ");
    // restart the vm
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));

    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, false, 100, 10, false, true,
        null, firstDStore, true));
    LogWriterUtils.getLogWriter().info("Created the sender.... in vm4 ");
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, false, 100, 10, false, true,
        null, secondDStore, true));
    LogWriterUtils.getLogWriter().info("Created the sender.... in vm5 ");

    vm4.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));

    vm5.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));

    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.startSender("ln"));
    LogWriterUtils.getLogWriter().info("Started the sender in vm 4");

    vm5.invoke(() -> WANTestBase.startSender("ln"));
    LogWriterUtils.getLogWriter().info("Started the sender in vm 5");
    try {
      inv1.join();
    } catch (InterruptedException e) {
      fail("Got interrupted exception while waiting for startSender to finish.");
    }

    Wait.pause(5000);

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

  }

  /**
   * Enable persistence for Region. No persistence for GatewaySender. Kill the vm with regions and
   * bring that up again. Check if the remote site receives all the event. again?
   * 
   */
  @Test
  public void testPersistentReplicatedRegionWithGatewaySender_Restart() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    LogWriterUtils.getLogWriter().info("Completed puts in the region");

    // verify if the queue has all the events
    // vm4.invoke(() -> WANTestBase.checkQueueSize( "ln", 1000
    // ));
    // vm5.invoke(() -> WANTestBase.checkQueueSize( "ln", 1000
    // ));
    //
    // vm2.invoke(() -> WANTestBase.validateRegionSize(
    // testName + "_RR", 0 ));
    // vm3.invoke(() -> WANTestBase.validateRegionSize(
    // testName + "_RR", 0 ));

    // kill the vm
    vm4.invoke(() -> WANTestBase.killSender());
    vm5.invoke(() -> WANTestBase.killSender());

    LogWriterUtils.getLogWriter().info("Killed the sender. ");
    // restart the vm
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    LogWriterUtils.getLogWriter().info("Created the sender.... in vm4 ");
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    LogWriterUtils.getLogWriter().info("Created the sender.... in vm5 ");

    vm4.invoke(() -> WANTestBase.startSender("ln"));
    LogWriterUtils.getLogWriter().info("Started the sender in vm 4");

    vm5.invoke(() -> WANTestBase.startSender("ln"));
    LogWriterUtils.getLogWriter().info("Started the sender in vm 5");

    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase
        .createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));

    try {
      inv1.join();
    } catch (InterruptedException e) {
      fail("Got interrupted exception while waiting for startSender to finish.");
    }

    Wait.pause(5000);
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

  }


  /**
   * Enable persistence for Region and persistence for GatewaySender. Kill the vm with regions and
   * bring that up again. Check if the remote site receives all the event. again? In this case put
   * is continuously happening while the vm is down.
   */
  @Test
  public void testPersistentReplicatedRegionWithGatewaySenderPersistenceEnabled_Restart2() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    String firstDStore = (String) vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        false, 100, 10, false, true, null, null, true));
    String secondDStore = (String) vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        false, 100, 10, false, true, null, null, true));

    LogWriterUtils.getLogWriter().info("The first ds is " + firstDStore);
    LogWriterUtils.getLogWriter().info("The first ds is " + secondDStore);

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    LogWriterUtils.getLogWriter().info("Completed puts in the region");

    // kill the vm
    vm4.invoke(() -> WANTestBase.killSender());
    vm5.invoke(() -> WANTestBase.killSender());

    LogWriterUtils.getLogWriter().info("Killed the sender. ");
    // restart the vm
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));

    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, false, 100, 10, false, true,
        null, firstDStore, true));
    LogWriterUtils.getLogWriter().info("Created the sender.... in vm4 ");
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, false, 100, 10, false, true,
        null, secondDStore, true));
    LogWriterUtils.getLogWriter().info("Created the sender.... in vm5 ");

    vm4.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));

    vm5.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getTestMethodName() + "_RR", "ln",
        isOffHeap()));

    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.startSender("ln"));
    LogWriterUtils.getLogWriter().info("Started the sender in vm 4");

    vm5.invoke(() -> WANTestBase.startSender("ln"));
    LogWriterUtils.getLogWriter().info("Started the sender in vm 5");
    try {
      inv1.join();
    } catch (InterruptedException e) {
      fail("Got interrupted exception while waiting for startSender to finish.");
    }

    Wait.pause(5000);

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

  }
}
