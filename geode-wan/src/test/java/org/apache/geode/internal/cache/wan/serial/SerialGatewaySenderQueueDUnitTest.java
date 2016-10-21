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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

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
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
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
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class SerialGatewaySenderQueueDUnitTest extends WANTestBase {

  @Test
  public void testPrimarySecondaryQueueDrainInOrder_RR() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm3.invoke(() -> WANTestBase.createCache(nyPort));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm2.invoke(() -> WANTestBase.createReceiver());
    vm3.invoke(() -> WANTestBase.createReceiver());

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
    Wait.pause(5000);
    HashMap primarySenderUpdates = (HashMap) vm4.invoke(() -> WANTestBase.checkQueue());
    HashMap secondarySenderUpdates = (HashMap) vm5.invoke(() -> WANTestBase.checkQueue());
    assertEquals(primarySenderUpdates, secondarySenderUpdates);

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    Wait.pause(2000);
    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    Wait.pause(2000);
    // We should wait till primarySenderUpdates and secondarySenderUpdates become same
    // If in 300000ms they don't then throw error.
    primarySenderUpdates = (HashMap) vm4.invoke(() -> WANTestBase.checkQueue());
    secondarySenderUpdates = (HashMap) vm5.invoke(() -> WANTestBase.checkQueue());

    checkPrimarySenderUpdatesOnVM5(primarySenderUpdates);
    // assertIndexDetailsEquals(primarySenderUpdates, secondarySenderUpdates);

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    Wait.pause(5000);
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    primarySenderUpdates = (HashMap) vm4.invoke(() -> WANTestBase.checkQueue());
    HashMap receiverUpdates = (HashMap) vm2.invoke(() -> WANTestBase.checkQueue());

    List destroyList = (List) primarySenderUpdates.get("Destroy");
    List createList = (List) receiverUpdates.get("Create");
    for (int i = 0; i < 1000; i++) {
      assertEquals(destroyList.get(i), createList.get(i));
    }
    assertEquals(primarySenderUpdates.get("Destroy"), receiverUpdates.get("Create"));

    Wait.pause(5000);
    // We expect that after this much time secondary would have got batch removal message
    // removing all the keys.
    secondarySenderUpdates = (HashMap) vm5.invoke(() -> WANTestBase.checkQueue());
    assertEquals(secondarySenderUpdates.get("Destroy"), receiverUpdates.get("Create"));
  }

  protected void checkPrimarySenderUpdatesOnVM5(HashMap primarySenderUpdates) {
    vm5.invoke(() -> WANTestBase.checkQueueOnSecondary(primarySenderUpdates));
  }

  @Test
  public void testPrimarySecondaryQueueDrainInOrder_PR() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

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
    HashMap primarySenderUpdates = (HashMap) vm4.invoke(() -> WANTestBase.checkQueue());
    HashMap secondarySenderUpdates = (HashMap) vm5.invoke(() -> WANTestBase.checkQueue());
    checkPrimarySenderUpdatesOnVM5(primarySenderUpdates);

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    Wait.pause(4000);
    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    Wait.pause(15000);
    primarySenderUpdates = (HashMap) vm4.invoke(() -> WANTestBase.checkQueue());
    secondarySenderUpdates = (HashMap) vm5.invoke(() -> WANTestBase.checkQueue());
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
    Integer localLocPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer remoteLocPort =
        (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, localLocPort));

    WANTestBase test = new WANTestBase(getTestMethodName());
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

      AttributesFactory factory = new AttributesFactory();
      factory.addGatewaySenderId(sender1.getId());
      factory.setDataPolicy(DataPolicy.PARTITION);
      Region region = cache.createRegionFactory(factory.create())
          .create("test_ValidateGatewaySenderAttributes");
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
    Integer localLocPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer remoteLocPort =
        (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, localLocPort));

    WANTestBase test = new WANTestBase(getTestMethodName());
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

      AttributesFactory factory = new AttributesFactory();
      factory.addGatewaySenderId(sender1.getId());
      factory.setDataPolicy(DataPolicy.PARTITION);
      Region region = cache.createRegionFactory(factory.create())
          .create("test_ValidateGatewaySenderAttributes");
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

}
