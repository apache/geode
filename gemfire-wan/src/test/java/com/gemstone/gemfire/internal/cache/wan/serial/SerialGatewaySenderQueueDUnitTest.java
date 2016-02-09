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
package com.gemstone.gemfire.internal.cache.wan.serial;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.cache30.MyGatewayEventFilter1;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter1;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter2;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;


public class SerialGatewaySenderQueueDUnitTest extends WANTestBase{

  private static final long serialVersionUID = 1L;

  public SerialGatewaySenderQueueDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }
  
  
  public void testPrimarySecondaryQueueDrainInOrder_RR() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class, "createFirstLocatorWithDSId",
        new Object[] { 1 });

    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createCache", new Object[] {nyPort });
    vm3.invoke(WANTestBase.class, "createCache", new Object[] {nyPort });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap() });

    vm2.invoke(WANTestBase.class, "createReceiver2", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver2", new Object[] { nyPort });
    
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSenderWithMultipleDispatchers", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, 1, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createSenderWithMultipleDispatchers", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, 1, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln", false});
    vm5.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln", false});
    
    vm2.invoke(WANTestBase.class, "addListenerOnRegion", new Object[] {getTestMethodName() + "_RR"});
    vm3.invoke(WANTestBase.class, "addListenerOnRegion", new Object[] {getTestMethodName() + "_RR"});

    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln"});
    
    vm6.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
      1000 });
    Wait.pause(5000);
    HashMap primarySenderUpdates = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue");
    HashMap secondarySenderUpdates = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue");
    assertEquals(primarySenderUpdates, secondarySenderUpdates);
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln"});
    Wait.pause(2000);
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln"});
    Wait.pause(2000);
    // We should wait till primarySenderUpdates and secondarySenderUpdates become same
    // If in 300000ms they don't then throw error.
    primarySenderUpdates = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue");
    secondarySenderUpdates = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue");
    
    vm5.invoke(WANTestBase.class,
        "checkQueueOnSecondary", new Object[] { primarySenderUpdates });
//    assertEquals(primarySenderUpdates, secondarySenderUpdates);
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln"});
    Wait.pause(5000);
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName() + "_RR", 1000 });
    primarySenderUpdates = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue");
    HashMap receiverUpdates = (HashMap)vm2.invoke(WANTestBase.class, "checkQueue");
    
    List destroyList = (List)primarySenderUpdates.get("Destroy");
    List createList = (List)receiverUpdates.get("Create");
    for(int i = 0; i< 1000; i++){
      assertEquals(destroyList.get(i), createList.get(i));
    }
    assertEquals(primarySenderUpdates.get("Destroy"), receiverUpdates.get("Create"));
    
    Wait.pause(5000);
    // We expect that after this much time secondary would have got batchremoval message
    // removing all the keys.
    secondarySenderUpdates = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue");
    assertEquals(secondarySenderUpdates.get("Destroy"), receiverUpdates.get("Create"));
  }
  
  public void testPrimarySecondaryQueueDrainInOrder_PR() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
  
    vm2.invoke(WANTestBase.class, "addListenerOnRegion", new Object[] {getTestMethodName() + "_PR"});
    vm3.invoke(WANTestBase.class, "addListenerOnRegion", new Object[] {getTestMethodName() + "_PR"});
  
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSenderWithMultipleDispatchers", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, 1, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createSenderWithMultipleDispatchers", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true,1, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln", false});
    vm5.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln", false});

    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln"});
    
    vm6.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR",
      1000 });
    Wait.pause(5000);
    HashMap primarySenderUpdates = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue");
    HashMap secondarySenderUpdates = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue");
    vm5.invoke(WANTestBase.class,
        "checkQueueOnSecondary", new Object[] { primarySenderUpdates });
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln"});
    Wait.pause(4000);
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln"});
    Wait.pause(15000);
    primarySenderUpdates = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue");
    secondarySenderUpdates = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue");
    assertEquals(primarySenderUpdates, secondarySenderUpdates);
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln"});
    Wait.pause(5000);
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName() + "_PR", 1000 });
  }
  
  /**
   * Test to validate that serial gateway sender queue diskSynchronous attribute
   * when persistence of sender is enabled. 
   */
  public void test_ValidateSerialGatewaySenderQueueAttributes_1() {
    Integer localLocPort = (Integer) vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Integer remoteLocPort = (Integer) vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, localLocPort });

    WANTestBase test = new WANTestBase(getTestMethodName());
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost["
        + localLocPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    File directory = new File("TKSender" + "_disk_"
        + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    directory.mkdir();
    File[] dirs1 = new File[] { directory };
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
    GatewayEventFilter myeventfilter1 = new MyGatewayEventFilter1();
    fact.addGatewayEventFilter(myeventfilter1);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    fact.addGatewayTransportFilter(myStreamfilter2);
    final IgnoredException exTKSender = IgnoredException.addIgnoredException("Could not connect");
    try {
      GatewaySender sender1 = fact.create("TKSender", 2);

      AttributesFactory factory = new AttributesFactory();
      factory.addGatewaySenderId(sender1.getId());
      factory.setDataPolicy(DataPolicy.PARTITION);
      Region region = cache.createRegionFactory(factory.create()).create(
          "test_ValidateGatewaySenderAttributes");
      Set<GatewaySender> senders = cache.getGatewaySenders();
      assertEquals(senders.size(), 1);
      GatewaySender gatewaySender = senders.iterator().next();
      Set<RegionQueue> regionQueues = ((AbstractGatewaySender) gatewaySender)
          .getQueues();
      assertEquals(regionQueues.size(), GatewaySender.DEFAULT_DISPATCHER_THREADS);
      RegionQueue regionQueue = regionQueues.iterator().next();
      assertEquals(true, regionQueue.getRegion().getAttributes()
          .isDiskSynchronous());
    } finally {
      exTKSender.remove();
    }

  }
  
  /**
   * Test to validate that serial gateway sender queue diskSynchronous attribute
   * when persistence of sender is not enabled. 
   */
  public void test_ValidateSerialGatewaySenderQueueAttributes_2() {
    Integer localLocPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    
    Integer remoteLocPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, localLocPort });
    
    WANTestBase test = new WANTestBase(getTestMethodName());
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + localLocPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);  

    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setBatchConflationEnabled(true);
    fact.setBatchSize(200);
    fact.setBatchTimeInterval(300);
    fact.setPersistenceEnabled(false);//set persistence to false
    fact.setDiskSynchronous(true);
    fact.setMaximumQueueMemory(200);
    fact.setAlertThreshold(1200);
    GatewayEventFilter myeventfilter1 = new MyGatewayEventFilter1();
    fact.addGatewayEventFilter(myeventfilter1);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    fact.addGatewayTransportFilter(myStreamfilter2);
    final IgnoredException exp = IgnoredException.addIgnoredException("Could not connect");
    try {
      GatewaySender sender1 = fact.create("TKSender", 2);

      AttributesFactory factory = new AttributesFactory();
      factory.addGatewaySenderId(sender1.getId());
      factory.setDataPolicy(DataPolicy.PARTITION);
      Region region = cache.createRegionFactory(factory.create()).create(
          "test_ValidateGatewaySenderAttributes");
      Set<GatewaySender> senders = cache.getGatewaySenders();
      assertEquals(senders.size(), 1);
      GatewaySender gatewaySender = senders.iterator().next();
      Set<RegionQueue> regionQueues = ((AbstractGatewaySender) gatewaySender)
          .getQueues();
      assertEquals(regionQueues.size(), GatewaySender.DEFAULT_DISPATCHER_THREADS);
      RegionQueue regionQueue = regionQueues.iterator().next();

      assertEquals(false, regionQueue.getRegion().getAttributes()
          .isDiskSynchronous());
    } finally {
      exp.remove();
    }
  }
  
}
