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

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.junit.categories.WanTest;


@Category({WanTest.class})
public class SerialWANPropagationsFeatureDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public SerialWANPropagationsFeatureDUnitTest() {
    super();
  }

  @Test
  public void testSerialReplicatedWanWithOverflow() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 10, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 10, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    createReceiverInVMs(vm2, vm3);

    startSenderInVMs("ln", vm4, vm5);
    vm2.invoke(() -> addListenerToSleepAfterCreateEvent(1000, getUniqueName() + "_RR"));
    vm3.invoke(() -> addListenerToSleepAfterCreateEvent(1000, getUniqueName() + "_RR"));

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doHeavyPuts(getUniqueName() + "_RR", 15));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 15));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 15));
    vm4.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
    vm5.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
  }

  @Test
  public void testSerialReplicatedWanWithPersistence() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, true, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, true, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm4.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
    vm5.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));

  }

  @Test
  public void testReplicatedSerialPropagationWithConflation() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 1000, true, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 1000, true, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm4.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
    vm5.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
  }

  @Test
  public void testReplicatedSerialPropagationWithParallelThreads() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doMultiThreadedPuts(getUniqueName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm4.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
    vm5.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
  }

  @Test
  public void testSerialPropagationWithFilter() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false,
        new MyGatewayEventFilter(), true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false,
        new MyGatewayEventFilter(), true));

    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(getUniqueName(), "ln", 1, 100, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createPartitionedRegion(getUniqueName(), "ln", 1, 100, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createPartitionedRegion(getUniqueName(), "ln", 1, 100, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createPartitionedRegion(getUniqueName(), "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getUniqueName(), null, 1, 100, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createPartitionedRegion(getUniqueName(), null, 1, 100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName(), 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName(), 800));
    vm4.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
    vm5.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
  }

  @Test
  public void testReplicatedSerialPropagationWithFilter() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false,
        new MyGatewayEventFilter(), true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false,
        new MyGatewayEventFilter(), true));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), null, isOffHeap()));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName(), 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName(), 800));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName(), 800));
    vm4.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
    vm5.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
  }

  @Test
  public void testReplicatedSerialPropagationWithFilter_AfterAck() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm6, vm7);
    createReceiverInVMs(vm6, vm7);

    createCacheInVMs(lnPort, vm2, vm3, vm4, vm5);
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false,
        new MyGatewayEventFilter_AfterAck(), true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false,
        new MyGatewayEventFilter_AfterAck(), true));

    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), null, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName(), 1000));

    vm4.invoke(() -> WANTestBase.validateQueueContents("ln", 0));
    vm5.invoke(() -> WANTestBase.validateQueueContents("ln", 0));

    Integer vm4Acks = vm4.invoke(() -> WANTestBase.validateAfterAck("ln"));
    Integer vm5Acks = vm5.invoke(() -> WANTestBase.validateAfterAck("ln"));

    assertEquals(2000, (vm4Acks + vm5Acks));

    vm6.invoke(() -> WANTestBase.validateRegionSize(getUniqueName(), 1000));
    vm7.invoke(() -> WANTestBase.validateRegionSize(getUniqueName(), 1000));
    vm4.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
    vm5.invoke(() -> WANTestBase.waitForConcurrentSerialSenderQueueToDrain("ln"));
  }

  /**
   * Test unstarted sender
   * Site-LN: dsid=2: senderId="ny": vm2, vm4
   * Site-NY: dsid=1: senderId="ln": vm3, vm6
   * NY site's sender's manual-start=true
   *
   * Make sure the events are sent from LN to NY and will not be added into tmpDroppedEvents
   * while normal events put from NY site can still be added to tmpDroppedEvents
   * Start the sender, make sure the events in tmpDroppedEvents are sent to LN finally
   */
  @Test
  public void unstartedSenderShouldNotAddReceivedEventsIntoTmpDropped() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(2));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(1, lnPort));

    // create receiver on site-ln and site-ny
    createCacheInVMs(lnPort, vm2, vm4);
    createReceiverInVMs(vm2, vm4);
    createCacheInVMs(nyPort, vm3, vm5);
    createReceiverInVMs(vm3, vm5);

    // create senders on site-ny, Note: sender-id is its destination, i.e. ny
    vm2.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, true));

    // create senders on site-ln, Note: sender-id is its destination, i.e. ln
    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    // create PR on site-ny
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 1, 100,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 1, 100,
        isOffHeap()));

    // create PR on site-ln
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    // start sender on site-ny
    startSenderInVMs("ny", vm2, vm4);

    // do 100 puts on site-ln
    vm3.invoke(() -> WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 0, 100));

    // verify site-ny have 100 entries
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));

    // verify site-ln has not received the events from site-ny yet
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0));
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0));

    // start sender on site-ln
    startSenderInVMsAsync("ln", vm3, vm5);

    // verify tmpDroppedEvents should be 0 now at site-ny
    vm3.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ln", 0));
    vm5.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ln", 0));

    vm3.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
  }

  /**
   * Test that gateway sender's secondary queues do not keep dropped events
   * by the primary gateway sender received while it was starting but was not
   * started yet, after the primary finishes starting.
   * Site-LN: dsid=2: senderId="ny": vm2, vm4
   * Site-NY: dsid=1: senderId="ln": vm3, vm6
   * NY site's sender's manual-start=true
   * LN site's sender's manual-start=true
   *
   * put some events from LN and start the sender in NY simultaneously
   * Make sure there are no events in tmpDroppedEvents and the queues are drained.
   */
  @Test
  public void startedSenderReceivingEventsWhileStartingShouldDrainQueues()
      throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(2));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(1, lnPort));

    createCacheInVMs(lnPort, vm2, vm4);
    createReceiverInVMs(vm2, vm4);
    createCacheInVMs(nyPort, vm3, vm5);
    createReceiverInVMs(vm3, vm5);

    vm2.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, true));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 1, 100,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 1, 100,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    AsyncInvocation<Void> inv =
        vm2.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    startSenderInVMsAsync("ny", vm2, vm4);
    inv.await();

    vm2.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));
    vm4.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));

    vm2.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ny"));
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ny"));
  }

  /**
   * Test that gateway sender's secondary queues do not keep dropped events
   * by the primary gateway sender received while it was stopping after it is started again.
   * Site-LN: dsid=2: senderId="ny": vm2, vm4
   * Site-NY: dsid=1: senderId="ln": vm3, vm6
   * NY site's sender's manual-start=false
   * LN site's sender's manual-start=false
   *
   * put some events from LN and stop the sender in NY simultaneously
   * Start the sender in NY.
   * Make sure there are no events in tmpDroppedEvents and the queues are drained.
   */
  @Test
  public void startedSenderReceivingEventsWhileStoppingShouldDrainQueues()
      throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(2));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(1, lnPort));

    createCacheInVMs(lnPort, vm2, vm4);
    createReceiverInVMs(vm2, vm4);
    createCacheInVMs(nyPort, vm3, vm5);
    createReceiverInVMs(vm3, vm5);

    vm2.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, false));
    vm4.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, false));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, false));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, false));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 1, 100,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 1, 100,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    AsyncInvocation<Void> inv =
        vm2.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    stopSenderInVMsAsync("ny", vm2, vm4);
    inv.await();

    startSenderInVMsAsync("ny", vm2, vm4);

    vm2.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));
    vm4.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));

    vm2.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ny"));
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ny"));
  }

  /**
   * Test that a stopped gateway sender receiving events
   * does not store them in tmpDroppedEvents but after started
   * does not leave any event in the
   * gateway sender's secondary queues.
   * Site-LN: dsid=2: senderId="ny": vm2, vm4
   * Site-NY: dsid=1: senderId="ln": vm3, vm6
   * NY site's sender's manual-start=false
   * LN site's sender's manual-start=false
   *
   * put some events from LN and stop the sender in NY simultaneously
   * Start the sender in NY.
   * Make sure there are no events in tmpDroppedEvents and the queues are drained.
   */
  @Test
  public void stoppedSenderShouldNotAddEventsToTmpDroppedEventsButStillDrainQueuesWhenStarted() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(2));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(1, lnPort));

    createCacheInVMs(lnPort, vm2, vm4);
    createReceiverInVMs(vm2, vm4);
    createCacheInVMs(nyPort, vm3, vm5);
    createReceiverInVMs(vm3, vm5);

    vm2.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, false));
    vm4.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, false));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, false));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, false));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 1, 100,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 1, 100,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    stopSenderInVMsAsync("ny", vm2, vm4);

    vm2.invoke(() -> WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 0, 100));

    // verify tmpDroppedEvents is 0 at site-ny
    vm2.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));
    vm4.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));

    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0));


    startSenderInVMsAsync("ny", vm2, vm4);

    vm2.invoke(() -> WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 100, 1000));

    vm2.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));
    vm4.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));

    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 900));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 900));

    // verify the secondary's queues are drained at site-ny
    vm2.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ny"));
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ny"));
  }

  /**
   * Test that a stopped primary gateway sender receiving events
   * does not store them in tmpDroppedEvents but after started
   * does not leave any event in the
   * gateway sender's secondary queues.
   * Site-LN: dsid=2: senderId="ny": vm2, vm4
   * Site-NY: dsid=1: senderId="ln": vm3, vm6
   * NY site's sender's manual-start=false
   * LN site's sender's manual-start=false
   *
   * put some events from LN and stop one instance of the sender in NY simultaneously
   * Start the stopped instance of the sender in NY.
   * Make sure there are no events in tmpDroppedEvents and the queues are drained.
   */
  @Test
  public void stoppedPrimarySenderShouldNotAddEventsToTmpDroppedEventsButStillDrainQueuesWhenStarted() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(2));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(1, lnPort));

    createCacheInVMs(lnPort, vm2, vm4);
    createReceiverInVMs(vm2, vm4);
    createCacheInVMs(nyPort, vm3, vm5);
    createReceiverInVMs(vm3, vm5);

    vm2.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, false));
    vm4.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, false));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, false));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, false));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 1, 100,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 1, 100,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    stopSenderInVMsAsync("ny", vm2);

    vm2.invoke(() -> WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 0, 100));

    // verify tmpDroppedEvents is 0 at site-ny
    vm2.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));
    vm4.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));

    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));

    startSenderInVMsAsync("ny", vm2);

    vm2.invoke(() -> WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 100, 1000));

    vm2.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));
    vm4.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));

    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));

    // verify the secondary's queues are drained at site-ny
    vm2.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ny"));
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ny"));
  }

}
