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
package org.apache.geode.internal.cache.wan.concurrent;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;

import java.net.SocketException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.wan.BatchException70;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.junit.categories.WanTest;

/**
 * Test the functionality of ParallelGatewaySender with multiple dispatchers.
 *
 */
@Category({WanTest.class})
public class ConcurrentParallelGatewaySenderDUnitTest extends WANTestBase {

  public ConcurrentParallelGatewaySenderDUnitTest() {
    super();
  }

  /**
   * Normal happy scenario test case. checks that all the dispatchers have successfully dispatched
   * something individually.
   *
   */
  @Test
  public void testParallelPropagationConcurrentArtifacts() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));

    int dispatched1 = (Integer) vm4
        .invoke(() -> WANTestBase.verifyAndGetEventsDispatchedByConcurrentDispatchers("ln"));
    int dispatched2 = (Integer) vm5
        .invoke(() -> WANTestBase.verifyAndGetEventsDispatchedByConcurrentDispatchers("ln"));
    int dispatched3 = (Integer) vm6
        .invoke(() -> WANTestBase.verifyAndGetEventsDispatchedByConcurrentDispatchers("ln"));
    int dispatched4 = (Integer) vm7
        .invoke(() -> WANTestBase.verifyAndGetEventsDispatchedByConcurrentDispatchers("ln"));

    assertEquals(1000, dispatched1 + dispatched2 + dispatched3 + dispatched4);
  }

  /**
   * Normal happy scenario test case.
   *
   */
  @Test
  public void testParallelPropagation() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }


  /**
   * Normal happy scenario test case when bucket division tests boundary cases.
   *
   */
  @Test
  public void testParallelPropagationWithUnEqualBucketDivision() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.PARTITION));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.PARTITION));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.PARTITION));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.PARTITION));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }


  /**
   * Initially the GatewayReceiver is not up but later it comes up. We verify that
   *
   */
  @Test
  public void testParallelPropagation_withoutRemoteSite() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // keep a larger batch to minimize number of exception occurrences in the log
    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 300, false, false, null,
        true, 6, OrderPolicy.PARTITION));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 300, false, false, null,
        true, 6, OrderPolicy.PARTITION));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 300, false, false, null,
        true, 6, OrderPolicy.PARTITION));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 300, false, false, null,
        true, 6, OrderPolicy.PARTITION));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    // make sure all the senders are running before doing any puts
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    createCacheInVMs(nyPort, vm2, vm3);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    createReceiverInVMs(vm2, vm3);

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    // Just making sure that though the remote site is started later,
    // remote site is still able to get the data. Since the receivers are
    // started before creating partition region it is quite possible that the
    // region may loose some of the events. This needs to be handled by the code
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  /**
   * Testing for colocated region with orderPolicy Partition
   */
  @Test
  public void testParallelPropagationColocatedPartitionedRegions() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.PARTITION));


    vm4.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 1, 100, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 1, 100, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 1, 100, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 1, 100, isOffHeap()));


    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 1, 100, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 1, 100, isOffHeap()));

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.putcolocatedPartitionedRegion(1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(customerRegionName, 1000));
    vm2.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, 1000));
    vm2.invoke(() -> WANTestBase.validateRegionSize(shipmentRegionName, 1000));
  }

  /**
   * Local and remote sites are up and running. Local site cache is closed and the site is built
   * again. Puts are done to local site. Expected: Remote site should receive all the events put
   * after the local site was built back.
   *
   */
  @Test
  public void testParallelPropagationWithLocalCacheClosedAndRebuilt() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.PARTITION));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.PARTITION));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.PARTITION));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.PARTITION));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    // -------------------Close and rebuild local site ---------------------------------

    vm4.invoke(() -> WANTestBase.killSender());
    vm5.invoke(() -> WANTestBase.killSender());
    vm6.invoke(() -> WANTestBase.killSender());
    vm7.invoke(() -> WANTestBase.killSender());

    Integer regionSize =
        (Integer) vm2.invoke(() -> WANTestBase.getRegionSize(getTestMethodName() + "_PR"));
    LogWriterUtils.getLogWriter().info("Region size on remote is: " + regionSize);

    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm6.invoke(() -> WANTestBase.createCache(lnPort));
    vm7.invoke(() -> WANTestBase.createCache(lnPort));

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.PARTITION));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.PARTITION));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.PARTITION));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.PARTITION));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    // ------------------------------------------------------------------------------------

    IgnoredException.addIgnoredException(EntryExistsException.class.getName());
    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 10000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));
  }

  /**
   * Colocated regions using ConcurrentParallelGatewaySender. Normal scenario
   *
   */
  @Test
  public void testParallelColocatedPropagation() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.KEY));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.KEY));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.KEY));

    vm4.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), "ln", 1,
        100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), null, 1,
        100, isOffHeap()));
    vm3.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), null, 1,
        100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName(), 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));
  }


  /**
   * Colocated regions using ConcurrentParallelGatewaySender. Normal scenario
   *
   */
  @Test
  public void testParallelColocatedPropagationOrderPolicyPartition() throws Exception {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.PARTITION));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.PARTITION));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.PARTITION));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 7, OrderPolicy.PARTITION));

    vm4.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), "ln", 1,
        100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), null, 1,
        100, isOffHeap()));
    vm3.invoke(() -> WANTestBase.createColocatedPartitionedRegions(getTestMethodName(), null, 1,
        100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName(), 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));
  }


  /**
   * Data is sent to all members containing the Partitioned Region when senders are killed.
   *
   * Setup:
   * vm0 - locator with DSID=1 - on lnPort
   * vm1 - remote locator with DSID=2 - on nyPort - connected to lnPort
   * vm2-3 - server with cache connected to nyPort - has partitionedRegion w/o sender - has receiver
   * vm4-7 - server with cache connected to lnPort - has partitionedRegion w/ ln sender - has sender
   *
   * Test:
   * 1. Put 5000 entries to VM7 (async)
   * 2. Assert that VM2 received at least 10 entries from VM7 through the sender (sync)
   * 3. Kill sender on VM4 (async)
   * 4. Get number of entries from VM2 (sync)
   * 5. Put 10000 entries to VM6, with the first 5000 entries having the same keys as in the first
   * round of puts (async)
   * 6. Assert that VM2 has twenty more entries than it did before puts on VM6 were started (sync)
   * 7. Kill sender on VM5 (async)
   * 8. Wait for previously started async invocations to complete
   * 9. Assert that VM2, VM3, VM6, and VM7 all have 10000 entries
   * 10. Assert that the sender queues on VM6 and VM7 are empty
   *
   **/
  @Test
  public void testPartitionedParallelPropagationHA() throws Exception {
    IgnoredException.addIgnoredException(SocketException.class.getName()); // for Connection reset

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    String regionName = getTestMethodName() + "_PR";

    createCacheInVMs(nyPort, vm2, vm3);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(regionName, null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(regionName, null, 1, 100,
        isOffHeap()));

    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.KEY));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.KEY));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 6, OrderPolicy.KEY));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(regionName, "ln", 2, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(regionName, "ln", 2, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(regionName, "ln", 2, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(regionName, "ln", 2, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    AsyncInvocation putsToVM7 =
        vm7.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 5000));

    vm2.invoke(() -> await()
        .untilAsserted(() -> assertEquals(
            "Failure in waiting for at least 10 events to be received by the receiver", true,
            (getRegionSize(getTestMethodName() + "_PR") > 10))));

    AsyncInvocation killsSenderFromVM4 = vm4.invokeAsync(() -> WANTestBase.killSender());

    int prevRegionSize = vm2.invoke(() -> WANTestBase.getRegionSize(getTestMethodName() + "_PR"));

    AsyncInvocation putsToVM6 =
        vm6.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 10000));

    vm2.invoke(() -> await().untilAsserted(() -> assertEquals(
        "Failure in waiting for additional 20 events to be received by the receiver ", true,
        getRegionSize(getTestMethodName() + "_PR") > 20 + prevRegionSize)));

    AsyncInvocation killsSenderFromVM5 = vm5.invokeAsync(() -> WANTestBase.killSender());

    putsToVM7.get();
    killsSenderFromVM4.get();
    putsToVM6.get();
    killsSenderFromVM5.get();

    vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));
    vm7.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));

    // verify all buckets drained on the sender nodes that up and running.
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
  }

  @Test
  public void testWANPDX_PR_MultipleVM_ConcurrentParallelSender() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3, vm4);

    vm3.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));
    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 2,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 2,
        isOffHeap()));

    startSenderInVMs("ln", vm3, vm4);

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 10));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 10));
  }

  @Test
  public void testWANPDX_PR_MultipleVM_ConcurrentParallelSender_StartedLater() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX(nyPort));

    vm3.invoke(() -> WANTestBase.createCache_PDX(lnPort));
    vm4.invoke(() -> WANTestBase.createCache_PDX(lnPort));

    vm3.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));
    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 2,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 10));

    startSenderInVMsAsync("ln", vm3, vm4);

    vm4.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 40));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 40));
  }
}
