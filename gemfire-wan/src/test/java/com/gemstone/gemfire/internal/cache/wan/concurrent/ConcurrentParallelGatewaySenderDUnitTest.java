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
package com.gemstone.gemfire.internal.cache.wan.concurrent;

import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.BatchException70;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderEventProcessor;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;

import java.net.SocketException;
import java.util.Set;

/**
 * Test the functionality of ParallelGatewaySender with multiple dispatchers.
 * @author skumar
 *
 */
public class ConcurrentParallelGatewaySenderDUnitTest extends WANTestBase {
  
  public ConcurrentParallelGatewaySenderDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Normal happy scenario test case.
   * checks that all the dispatchers have successfully 
   * dispatched something individually.
   * 
   * @throws Exception
   */
  public void testParallelPropagationConcurrentArtifacts() throws Exception {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    try {
      // set the test hook to find out dispacthed events by each of the
      // concurrent dispatcher
      vm4.invoke(ConcurrentParallelGatewaySenderDUnitTest.class, "setTestHook",
          new Object[] {"ln", Boolean.TRUE });
      vm5.invoke(ConcurrentParallelGatewaySenderDUnitTest.class, "setTestHook",
          new Object[] {"ln", Boolean.TRUE });
      vm6.invoke(ConcurrentParallelGatewaySenderDUnitTest.class, "setTestHook",
          new Object[] {"ln", Boolean.TRUE });
      vm7.invoke(ConcurrentParallelGatewaySenderDUnitTest.class, "setTestHook",
          new Object[] {"ln", Boolean.TRUE });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          1000 });

      // verify all buckets drained on all sender nodes.
      vm4.invoke(WANTestBase.class,
          "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class,
          "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });
      vm6.invoke(WANTestBase.class,
          "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });
      vm7.invoke(WANTestBase.class,
          "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 1000 });

      int dispatched1 = (Integer)vm4.invoke(WANTestBase.class,
          "verifyAndGetEventsDispatchedByConcurrentDispatchers",
          new Object[] { "ln" });
      int dispatched2 = (Integer)vm5.invoke(WANTestBase.class,
          "verifyAndGetEventsDispatchedByConcurrentDispatchers",
          new Object[] { "ln" });
      int dispatched3 = (Integer)vm6.invoke(WANTestBase.class,
          "verifyAndGetEventsDispatchedByConcurrentDispatchers",
          new Object[] { "ln" });
      int dispatched4 = (Integer)vm7.invoke(WANTestBase.class,
          "verifyAndGetEventsDispatchedByConcurrentDispatchers",
          new Object[] { "ln" });

      assertEquals(1000, dispatched1 + dispatched2 + dispatched3 + dispatched4);
    }
    finally {
      vm4.invoke(ConcurrentParallelGatewaySenderDUnitTest.class, "setTestHook", new Object[] {"ln", Boolean.FALSE });
      vm5.invoke(ConcurrentParallelGatewaySenderDUnitTest.class, "setTestHook", new Object[] {"ln", Boolean.FALSE });
      vm6.invoke(ConcurrentParallelGatewaySenderDUnitTest.class, "setTestHook", new Object[] {"ln", Boolean.FALSE });
      vm7.invoke(ConcurrentParallelGatewaySenderDUnitTest.class, "setTestHook", new Object[] {"ln", Boolean.FALSE });
    }
  }
  
  /**
   * Normal happy scenario test case.
   * @throws Exception
   */
  public void testParallelPropagation() throws Exception {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }
  
  
  /**
   * Normal happy scenario test case when bucket division tests boundary cases.
   * @throws Exception
   */
  public void testParallelPropagationWithUnEqualBucketDivison() throws Exception {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.PARTITION });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.PARTITION });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.PARTITION });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }
  
  
  /**
   * Initially the GatewayReceiver is not up but later it comes up.
   * We verify that 
   * @throws Exception
   */
  public void testParallelPropagation_withoutRemoteSite() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //keep a larger batch to minimize number of exception occurrences in the log
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 300, false, false, null, true, 6, OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 300, false, false, null, true, 6, OrderPolicy.PARTITION });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 300, false, false, null, true, 6, OrderPolicy.PARTITION });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 300, false, false, null, true, 6, OrderPolicy.PARTITION });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", false });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", false });
    vm6.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", false });
    vm7.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", false });
    
    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
      1000 });

    
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 1, 100, isOffHeap() });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    // Just making sure that though the remote site is started later,
    // remote site is still able to get the data. Since the receivers are
    // started before creating partition region it is quite possible that the
    // region may loose some of the events. This needs to be handled by the code
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }
  
  /**
   * Testing for colocated region with orderpolicy Partition
   */
  public void testParallelPropogationColocatedPartitionedRegions() {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION });


    vm4.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
            "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
            "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
            "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
            "ln", 1, 100, isOffHeap() });
    

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
            "ln", 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
            "ln", 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "putcolocatedPartitionedRegion", new Object[] { 1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      customerRegionName, 1000 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      orderRegionName, 1000 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      shipmentRegionName, 1000 });
  }
  
  /**
   * Local and remote sites are up and running.
   * Local site cache is closed and the site is built again.
   * Puts are done to local site.
   * Expected: Remote site should receive all the events put after the local
   * site was built back.
   * 
   * @throws Exception
   */
  public void testParallelPropagationWithLocalCacheClosedAndRebuilt() throws Exception {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.PARTITION });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.PARTITION});
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.PARTITION });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
      1000 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
              testName + "_PR", 1000 });
    //-------------------Close and rebuild local site ---------------------------------

    vm4.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm5.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm6.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm7.invoke(WANTestBase.class, "killSender", new Object[] {});
    
    Integer regionSize = 
      (Integer) vm2.invoke(WANTestBase.class, "getRegionSize", new Object[] {testName + "_PR" });
    getLogWriter().info("Region size on remote is: " + regionSize);
    
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.PARTITION });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.PARTITION });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.PARTITION });

    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm6.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm7.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    //------------------------------------------------------------------------------------
    
    addExpectedException(EntryExistsException.class.getName());
    addExpectedException(BatchException70.class.getName());
    addExpectedException(ServerOperationException.class.getName());
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 10000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 10000 });
  }
  
  /**
   * Colocated regions using ConcurrentParallelGatewaySender.
   * Normal scenario 
   * @throws Exception
   */
  public void testParallelColocatedPropagation() throws Exception {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, null, 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName, 1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 1000 });
  }
  
  
  /**
   * Colocated regions using ConcurrentParallelGatewaySender.
   * Normal scenario 
   * @throws Exception
   */
  public void testParallelColocatedPropagationOrderPolicyPartition() throws Exception {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.PARTITION });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.PARTITION });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.PARTITION });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.PARTITION });

    vm4.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createColocatedPartitionedRegions",
        new Object[] { testName, null, 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName, 1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 1000 });
  }
  
  public void testPartitionedParallelPropagationHA() throws Exception {
    addExpectedException(SocketException.class.getName()); // for Connection reset
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm6.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm7.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 2, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 2, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 2, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 2, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    AsyncInvocation inv1 = vm7.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { testName + "_PR", 5000 });
    pause(500);
    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "killSender");
    AsyncInvocation inv3 = vm6.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { testName + "_PR", 10000 });
    pause(1500);
    AsyncInvocation inv4 = vm5.invokeAsync(WANTestBase.class, "killSender");
    inv1.join();
    inv2.join();
    inv3.join();
    inv4.join();
    
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 10000 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 10000 });
    
    //verify all buckets drained on the sender nodes that up and running.
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 10000 });
  }
  
  public void testWANPDX_PR_MultipleVM_ConcurrentParallelSender() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm3.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm3.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 0, 2, isOffHeap()});

    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 2, isOffHeap()});
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 2, isOffHeap()});
    
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm3.invoke(WANTestBase.class, "doPutsPDXSerializable", new Object[] { testName + "_PR",
        10 });

    vm2.invoke(WANTestBase.class, "validateRegionSize_PDX", new Object[] {
        testName + "_PR", 10 });
  }
  
  public void testWANPDX_PR_MultipleVM_ConcurrentParallelSender_StartedLater() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver_PDX", new Object[] { nyPort });

    vm3.invoke(WANTestBase.class, "createCache_PDX", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createCache_PDX", new Object[] { lnPort });

    vm3.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 0, 2, isOffHeap()});

    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 2, isOffHeap()});
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 2, isOffHeap()});
    
    vm3.invoke(WANTestBase.class, "doPutsPDXSerializable", new Object[] { testName + "_PR",
        10 });

    AsyncInvocation inv1 = vm3.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    try{
      inv1.join();
      inv2.join();
    }
    catch(InterruptedException ie) {
      fail("Caught interrupted exception");
    }
    
    vm4.invoke(WANTestBase.class, "doPutsPDXSerializable", new Object[] { testName + "_PR",
      40 });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize_PDX", new Object[] {
        testName + "_PR", 40 });
  }

  public static void setTestHook(String senderId, boolean hook) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    ConcurrentParallelGatewaySenderEventProcessor cProc = (ConcurrentParallelGatewaySenderEventProcessor)((AbstractGatewaySender)sender)
        .getEventProcessor();
    if (cProc == null) return;
    cProc.TEST_HOOK = hook;
  }
  
}
