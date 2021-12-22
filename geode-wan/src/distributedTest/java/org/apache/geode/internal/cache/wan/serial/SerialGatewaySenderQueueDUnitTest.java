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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.cache30.MyGatewayEventFilter1;
import org.apache.geode.cache30.MyGatewayTransportFilter1;
import org.apache.geode.cache30.MyGatewayTransportFilter2;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class SerialGatewaySenderQueueDUnitTest extends WANTestBase {

  @Test
  public void unprocessedTokensMapShouldDrainCompletely() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm3.invoke(() -> WANTestBase.createCache(nyPort));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm2.invoke(WANTestBase::createReceiver);
    vm3.invoke(WANTestBase::createReceiver);

    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm6.invoke(() -> WANTestBase.createCache(lnPort));
    vm7.invoke(() -> WANTestBase.createCache(lnPort));

    vm4.invoke(() -> WANTestBase.createSenderWithMultipleDispatchers("ln", 2, false, 100, 10, false,
        false, null, true, 1, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createSenderWithMultipleDispatchers("ln", 2, false, 100, 10, false,
        false, null, true, 1, OrderPolicy.KEY));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedProxyRegion(getTestMethodName() + "_RR", "ln",
            isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedProxyRegion(getTestMethodName() + "_RR", "ln",
            isOffHeap()));

    AsyncInvocation a1 =
        vm6.invokeAsync(() -> WANTestBase.doPutsSameKey(getTestMethodName() + "_RR", 2000, "DA"));
    AsyncInvocation a2 =
        vm7.invokeAsync(() -> WANTestBase.doPutsSameKey(getTestMethodName() + "_RR", 2000, "DA"));

    a1.await();
    a2.await();

    vm4.invoke(() -> await().untilAsserted(this::queueIsDrained));
    vm5.invoke(() -> await().untilAsserted(this::queueIsDrained));
  }

  private int unprocessedTokensSize(String senderId) {
    AbstractGatewaySender sender = (AbstractGatewaySender) findGatewaySender(senderId);
    SerialGatewaySenderEventProcessor processor =
        (SerialGatewaySenderEventProcessor) sender.getEventProcessor();
    return processor.numUnprocessedEventTokens();
  }

  private GatewaySender findGatewaySender(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    return sender;
  }


  @Test
  public void testPrimarySecondaryQueueDrainInOrder_RR() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm3.invoke(() -> WANTestBase.createCache(nyPort));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm2.invoke(WANTestBase::createReceiver);
    vm3.invoke(WANTestBase::createReceiver);

    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm6.invoke(() -> WANTestBase.createCache(lnPort));
    vm7.invoke(() -> WANTestBase.createCache(lnPort));

    vm4.invoke(() -> WANTestBase.createSenderWithMultipleDispatchers("ln", 2, false, 100, 10, false,
        false, null, true, 1, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createSenderWithMultipleDispatchers("ln", 2, false, 100, 10, false,
        false, null, true, 1, OrderPolicy.KEY));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.addQueueListener("ln", false));
    vm5.invoke(() -> WANTestBase.addQueueListener("ln", false));

    vm2.invoke(() -> WANTestBase.addListenerOnRegion(getTestMethodName() + "_RR"));
    vm3.invoke(() -> WANTestBase.addListenerOnRegion(getTestMethodName() + "_RR"));

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));

    vm6.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 1000));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 1000));
    // secondary queue size stats in serial queue should be 0
    assertEquals(0, v4List.get(10) + v5List.get(10));

    HashMap primarySenderUpdates = vm4.invoke(WANTestBase::checkQueue);
    HashMap secondarySenderUpdates = vm5.invoke(WANTestBase::checkQueue);
    assertEquals(primarySenderUpdates, secondarySenderUpdates);

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    Wait.pause(2000);
    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    Wait.pause(2000);
    // We should wait till primarySenderUpdates and secondarySenderUpdates become same
    // If in 300000ms they don't then throw error.
    primarySenderUpdates = vm4.invoke(WANTestBase::checkQueue);
    secondarySenderUpdates = vm5.invoke(WANTestBase::checkQueue);

    checkPrimarySenderUpdatesOnVM5(primarySenderUpdates);
    // assertIndexDetailsEquals(primarySenderUpdates, secondarySenderUpdates);

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    Wait.pause(5000);
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    primarySenderUpdates = vm4.invoke(WANTestBase::checkQueue);
    HashMap receiverUpdates = vm2.invoke(WANTestBase::checkQueue);

    List destroyList = (List) primarySenderUpdates.get("Destroy");
    List createList = (List) receiverUpdates.get("Create");
    for (int i = 0; i < 1000; i++) {
      assertEquals(destroyList.get(i), createList.get(i));
    }
    assertEquals(primarySenderUpdates.get("Destroy"), receiverUpdates.get("Create"));

    Wait.pause(5000);
    // We expect that after this much time secondary would have got batch removal message
    // removing all the keys.
    secondarySenderUpdates = vm5.invoke(WANTestBase::checkQueue);
    assertEquals(secondarySenderUpdates.get("Destroy"), receiverUpdates.get("Create"));

    vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
  }


  protected void checkPrimarySenderUpdatesOnVM5(HashMap primarySenderUpdates) {
    vm5.invoke(() -> WANTestBase.checkQueueOnSecondary(primarySenderUpdates));
  }

  @Test
  public void testPrimarySecondaryQueueDrainInOrder_PR() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);


    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    vm2.invoke(() -> WANTestBase.addListenerOnRegion(getTestMethodName() + "_PR"));
    vm3.invoke(() -> WANTestBase.addListenerOnRegion(getTestMethodName() + "_PR"));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSenderWithMultipleDispatchers("ln", 2, false, 100, 10, false,
        false, null, true, 1, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createSenderWithMultipleDispatchers("ln", 2, false, 100, 10, false,
        false, null, true, 1, OrderPolicy.KEY));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.addQueueListener("ln", false));
    vm5.invoke(() -> WANTestBase.addQueueListener("ln", false));

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));

    vm6.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    Wait.pause(5000);
    HashMap primarySenderUpdates = vm4.invoke(WANTestBase::checkQueue);
    HashMap secondarySenderUpdates = vm5.invoke(WANTestBase::checkQueue);
    checkPrimarySenderUpdatesOnVM5(primarySenderUpdates);

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    Wait.pause(4000);
    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    Wait.pause(15000);
    primarySenderUpdates = vm4.invoke(WANTestBase::checkQueue);
    secondarySenderUpdates = vm5.invoke(WANTestBase::checkQueue);
    assertEquals(primarySenderUpdates, secondarySenderUpdates);

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    Wait.pause(5000);
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  /**
   * Test to validate that serial gateway sender queue diskSynchronous attribute when persistence of
   * sender is enabled.
   */
  @Test
  public void test_ValidateSerialGatewaySenderQueueAttributes_1() {
    Integer localLocPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer remoteLocPort =
        vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, localLocPort));

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

      RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
      regionFactory.addGatewaySenderId(sender1.getId());
      Region region = regionFactory.create("test_ValidateGatewaySenderAttributes");
      Set<GatewaySender> senders = cache.getGatewaySenders();
      assertEquals(senders.size(), 1);
      GatewaySender gatewaySender = senders.iterator().next();
      Set<RegionQueue> regionQueues = ((AbstractGatewaySender) gatewaySender).getQueues();
      assertEquals(regionQueues.size(), GatewaySender.DEFAULT_DISPATCHER_THREADS);
      RegionQueue regionQueue = regionQueues.iterator().next();
      assertEquals(true, regionQueue.getRegion().getAttributes().isDiskSynchronous());
    } finally {
      exTKSender.remove();
    }
  }

  /**
   * Test to validate that serial gateway sender queue diskSynchronous attribute when persistence of
   * sender is not enabled.
   */
  @Test
  public void test_ValidateSerialGatewaySenderQueueAttributes_2() {
    Integer localLocPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer remoteLocPort =
        vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, localLocPort));

    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + localLocPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
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
    final IgnoredException exp = IgnoredException.addIgnoredException("Could not connect");
    try {
      GatewaySender sender1 = fact.create("TKSender", 2);
      RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
      regionFactory.addGatewaySenderId(sender1.getId());
      Region region = regionFactory.create("test_ValidateGatewaySenderAttributes");
      Set<GatewaySender> senders = cache.getGatewaySenders();
      assertEquals(senders.size(), 1);
      GatewaySender gatewaySender = senders.iterator().next();
      Set<RegionQueue> regionQueues = ((AbstractGatewaySender) gatewaySender).getQueues();
      assertEquals(regionQueues.size(), GatewaySender.DEFAULT_DISPATCHER_THREADS);
      RegionQueue regionQueue = regionQueues.iterator().next();

      assertEquals(false, regionQueue.getRegion().getAttributes().isDiskSynchronous());
    } finally {
      exp.remove();
    }
  }

  /**
   * Test to validate that the maximum number of senders can be created and used successfully.
   */
  @Test
  public void testCreateMaximumSenders() {
    // Create locators
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // Create receiver and region
    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm2.invoke(WANTestBase::createReceiver);
    vm2.invoke(() -> WANTestBase.addListenerOnRegion(getTestMethodName() + "_RR"));

    // Create maximum number of senders
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    StringBuilder builder = new StringBuilder();
    long maxSenders = ThreadIdentifier.Bits.GATEWAY_ID.mask() + 1;
    for (int i = 0; i < maxSenders; i++) {
      String senderId = "ln-" + i;
      builder.append(senderId);
      if (i + 1 != maxSenders) {
        builder.append(',');
      }
      vm4.invoke(() -> WANTestBase.createSenderWithMultipleDispatchers(senderId, 2, false, 100, 10,
          false, false, null, false, 1, OrderPolicy.KEY, 32768));
    }

    // Create region with the sender ids
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR",
        builder.toString(), isOffHeap()));

    // Do puts
    int numPuts = 100;
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", numPuts));

    // Verify receiver listener events
    vm2.invoke(() -> WANTestBase.verifyListenerEvents(maxSenders * numPuts));
  }


  @Test
  public void whenBatchBasedOnTimeOnlyThenQueueShouldNotDispatchUntilIntervalIsHit() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    int batchIntervalTime = 5000;

    // Create receiver and region
    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm2.invoke(WANTestBase::createReceiver);
    vm2.invoke(() -> WANTestBase.addListenerOnRegion(getTestMethodName() + "_RR"));

    // Create sender with batchSize disabled
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    StringBuilder builder = new StringBuilder();
    String senderId = "ln";
    builder.append(senderId);
    vm4.invoke(() -> {
      InternalGatewaySenderFactory gateway =
          (InternalGatewaySenderFactory) cache.createGatewaySenderFactory();
      gateway.setParallel(false);
      gateway.setMaximumQueueMemory(100);
      gateway.setBatchSize(RegionQueue.BATCH_BASED_ON_TIME_ONLY);
      gateway.setBatchConflationEnabled(true);
      gateway.setDispatcherThreads(1);
      gateway.setSocketBufferSize(32768);
      gateway.setBatchTimeInterval(batchIntervalTime);
      gateway.create(senderId, 2);
    });

    // Create region with the sender ids
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR",
        builder.toString(), isOffHeap()));

    // Do puts
    int numPuts = 100;
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", numPuts));

    // attempt to prove the absence of a dispatch/ prove a dispatch has not occurred
    // will verify that no events have occurred over a period of time less than batch interval but
    // more than enough
    // for a regular dispatch to have occurred
    vm2.invoke(() -> {
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < batchIntervalTime - 1000) {
        assertEquals(0, listener1.getNumEvents());
      }
    });

    // Verify receiver listener events
    vm2.invoke(() -> WANTestBase.verifyListenerEvents(numPuts));
  }


  /**
   * Test to validate that the maximum number of senders plus one fails to be created.
   */
  @Test
  public void testCreateMaximumPlusOneSenders() {
    // Create locators
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // Create receiver
    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm2.invoke(WANTestBase::createReceiver);

    // Create maximum number of senders
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    for (int i = 0; i < ThreadIdentifier.Bits.GATEWAY_ID.mask() + 1; i++) {
      String senderId = "ln-" + i;
      vm4.invoke(() -> WANTestBase.createSenderWithMultipleDispatchers(senderId, 2, false, 100, 10,
          false, false, null, false, 1, OrderPolicy.KEY, 32768));
    }

    // Attempt to create one more sender
    vm4.invoke(SerialGatewaySenderQueueDUnitTest::attemptToCreateGatewaySenderOverLimit);
  }

  private static void attemptToCreateGatewaySenderOverLimit() {
    IgnoredException exp =
        IgnoredException.addIgnoredException(IllegalStateException.class.getName());
    try {
      createSenderWithMultipleDispatchers("ln-one-too-many", 2, false, 100, 10, false, false, null,
          false, 1, OrderPolicy.KEY, 32768);
      fail("Should not have been able to create gateway sender");
    } catch (IllegalStateException e) {
      /* ignore expected exception */
    } finally {
      exp.remove();
    }
  }

  private void queueIsDrained() {
    assertThat(unprocessedTokensSize("ln"))
        .as("number of unprocessed tokens in queue")
        .isEqualTo(0);
  }
}
