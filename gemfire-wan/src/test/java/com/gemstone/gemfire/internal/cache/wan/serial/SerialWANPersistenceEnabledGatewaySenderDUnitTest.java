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


import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;

/**
 * @author skumar
 * 
 */
public class SerialWANPersistenceEnabledGatewaySenderDUnitTest extends
    WANTestBase {

  private static final long serialVersionUID = 1L;

  public SerialWANPersistenceEnabledGatewaySenderDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Just enable the persistence for GatewaySender and see if it remote site
   * receives all the events.
   */
  public void testReplicatedRegionWithGatewaySenderPersistenceEnabled() {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });

  }

  /**
   * Enable persistence for the Region and see if the remote site gets all the
   * events.
   */
  public void testPersistentReplicatedRegionWithGatewaySender() {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });

  }

  /**
   * Enable persistence for region as well as GatewaySender and see if remote
   * site receives all the events.
   * 
   */
  public void testPersistentReplicatedRegionWithGatewaySenderPersistenceEnabled() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true });

    vm2.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });

  }

  /**
   * Enable persistence for GatewaySender, kill the sender and restart it. Check
   * if the remote site receives all the event.
   */
  public void testReplicatedRegionWithGatewaySenderPersistenceEnabled_Restart() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    String firstDStore = (String)vm4.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, false,
            100, 10, false, true, null, null, true });
    String secondDStore = (String)vm5.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, false,
            100, 10, false, true, null, null, true });

    getLogWriter().info("The first ds is " + firstDStore);
    getLogWriter().info("The first ds is " + secondDStore);

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    getLogWriter().info("Completed puts in the region");

    // verify if the queue has all the events
    // vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", 1000
    // });
    // vm5.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", 1000
    // });
    //
    // vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
    // testName + "_RR", 0 });
    // vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
    // testName + "_RR", 0 });

    // kill the vm
    vm4.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm5.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm6.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm7.invoke(WANTestBase.class, "killSender", new Object[] {});
    
    getLogWriter().info("Killed all the sender. ");
    // restart the vm
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createSenderWithDiskStore", new Object[] {
        "ln", 2, false, 100, 10, false, true, null,
        firstDStore, true });
    getLogWriter().info("Creted the sender.... in vm4 ");
    vm5.invoke(WANTestBase.class, "createSenderWithDiskStore", new Object[] {
        "ln", 2, false, 100, 10, false, true, null,
        secondDStore, true });
    getLogWriter().info("Creted the sender.... in vm5 ");
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "startSender",
        new Object[] { "ln" });
    getLogWriter().info("Started the sender in vm 4");

    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    getLogWriter().info("Started the sender in vm 5");
    try {
      inv1.join();
    } catch (InterruptedException e) {
      fail("Got interrupted exception while waiting for startSender to finish.");
    }

    pause(5000);

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });

  }

  /**
   * Enable persistence for Region and persistence for GatewaySender. Kill the
   * vm with regions and bring that up again. Check if the remote site receives
   * all the event. again?
   * 
   */
  public void testPersistentReplicatedRegionWithGatewaySenderPersistenceEnabled_Restart() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    String firstDStore = (String)vm4.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, false,
            100, 10, false, true, null, null, true });
    String secondDStore = (String)vm5.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, false,
            100, 10, false, true, null, null, true  });

    getLogWriter().info("The first ds is " + firstDStore);
    getLogWriter().info("The first ds is " + secondDStore);

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    getLogWriter().info("Completed puts in the region");

    // kill the vm
    vm4.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm5.invoke(WANTestBase.class, "killSender", new Object[] {});

    getLogWriter().info("Killed the sender. ");
    // restart the vm
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    
    vm4.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, false,
      100, 10, false, true, null, firstDStore, true  });
    getLogWriter().info("Created the sender.... in vm4 ");
    vm5.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, false,
            100, 10, false, true, null, secondDStore, true  });
    getLogWriter().info("Created the sender.... in vm5 ");
    
    vm4.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    
    vm5.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
  
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "startSender",
        new Object[] { "ln" });
    getLogWriter().info("Started the sender in vm 4");

    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    getLogWriter().info("Started the sender in vm 5");
    try {
      inv1.join();
    } catch (InterruptedException e) {
      fail("Got interrupted exception while waiting for startSedner to finish.");
    }

    pause(5000);

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });

  }
  
  /**
   * Enable persistence for Region. No persistence for GatewaySender. Kill the
   * vm with regions and bring that up again. Check if the remote site receives
   * all the event. again?
   * 
   */
  public void testPersistentReplicatedRegionWithGatewaySender_Restart() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class,
        "createSender", new Object[] { "ln", 2, false,
            100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class,
        "createSender", new Object[] { "ln", 2, false,
            100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    getLogWriter().info("Completed puts in the region");

    // verify if the queue has all the events
    // vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", 1000
    // });
    // vm5.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", 1000
    // });
    //
    // vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
    // testName + "_RR", 0 });
    // vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
    // testName + "_RR", 0 });

    // kill the vm
    vm4.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm5.invoke(WANTestBase.class, "killSender", new Object[] {});

    getLogWriter().info("Killed the sender. ");
    // restart the vm
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createSender", new Object[] {
        "ln", 2, false, 100, 10, false, false, null, true});
    getLogWriter().info("Creted the sender.... in vm4 ");
    vm5.invoke(WANTestBase.class, "createSender", new Object[] {
        "ln", 2, false, 100, 10, false, false, null, true});
    getLogWriter().info("Creted the sender.... in vm5 ");
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    getLogWriter().info("Started the sender in vm 4");

    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    getLogWriter().info("Started the sender in vm 5");
    
    AsyncInvocation inv1 =  vm4.invokeAsync(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    
    try {
      inv1.join();
    } catch (InterruptedException e) {
      fail("Got interrupted exception while waiting for startSedner to finish.");
    }

    pause(5000);
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
      1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });

  }
  
  
  /**
   * Enable persistence for Region and persistence for GatewaySender. Kill the
   * vm with regions and bring that up again. Check if the remote site receives
   * all the event. again?
   * In this case put is continuously happening while the vm is down.
   */
  public void testPersistentReplicatedRegionWithGatewaySenderPersistenceEnabled_Restart2() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    String firstDStore = (String)vm4.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, false,
            100, 10, false, true, null, null, true  });
    String secondDStore = (String)vm5.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, false,
            100, 10, false, true, null, null, true  });

    getLogWriter().info("The first ds is " + firstDStore);
    getLogWriter().info("The first ds is " + secondDStore);

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    getLogWriter().info("Completed puts in the region");

    // kill the vm
    vm4.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm5.invoke(WANTestBase.class, "killSender", new Object[] {});

    getLogWriter().info("Killed the sender. ");
    // restart the vm
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    
    vm4.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, false,
      100, 10, false, true, null, firstDStore, true  });
    getLogWriter().info("Created the sender.... in vm4 ");
    vm5.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, false,
            100, 10, false, true, null, secondDStore, true  });
    getLogWriter().info("Created the sender.... in vm5 ");
    
    vm4.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    
    vm5.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
  
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "startSender",
        new Object[] { "ln" });
    getLogWriter().info("Started the sender in vm 4");

    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    getLogWriter().info("Started the sender in vm 5");
    try {
      inv1.join();
    } catch (InterruptedException e) {
      fail("Got interrupted exception while waiting for startSedner to finish.");
    }

    pause(5000);

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });

  }
}
