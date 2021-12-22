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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ParallelWANPropagationLoopBackDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public ParallelWANPropagationLoopBackDUnitTest() {
    super();
  }

  /**
   * Test loop back issue between 2 WAN sites (LN & NY). LN -> NY -> LN. Site1 (LN): vm2, vm4, vm5
   * Site2 (NY): vm3, vm6, vm7
   */
  @Test
  public void testParallelPropagationLoopBack() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // create receiver on site1 and site2
    createCacheInVMs(lnPort, vm2, vm4, vm5);
    vm2.invoke(WANTestBase::createReceiver);
    createCacheInVMs(nyPort, vm3, vm6, vm7);
    vm3.invoke(WANTestBase::createReceiver);

    // create senders on site1
    vm2.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    // create senders on site2
    vm3.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, true));

    // create PR on site1
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 100,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 100,
        isOffHeap()));

    // create PR on site2
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 0, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 0, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 0, 100,
        isOffHeap()));

    // start sender on site1
    startSenderInVMs("ln", vm2, vm4, vm5);


    // start sender on site2
    startSenderInVMs("ny", vm3, vm6, vm7);


    // pause senders on site1
    vm2.invoke(() -> WANTestBase.pauseSender("ln"));
    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));

    // pause senders on site2
    vm3.invoke(() -> WANTestBase.pauseSender("ny"));
    vm6.invoke(() -> WANTestBase.pauseSender("ny"));
    vm7.invoke(() -> WANTestBase.pauseSender("ny"));

    // this is required since sender pause doesn't take effect immediately
    Wait.pause(1000);

    // Do 100 puts on site1
    vm2.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 100));
    // do next 100 puts on site2
    vm3.invoke(() -> WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 100, 200));
    // verify queue size on both sites
    vm2.invoke(() -> WANTestBase.verifyQueueSize("ln", 100));
    vm4.invoke(() -> WANTestBase.verifyQueueSize("ln", 100));
    vm5.invoke(() -> WANTestBase.verifyQueueSize("ln", 100));

    vm3.invoke(() -> WANTestBase.verifyQueueSize("ny", 100));
    vm6.invoke(() -> WANTestBase.verifyQueueSize("ny", 100));
    vm7.invoke(() -> WANTestBase.verifyQueueSize("ny", 100));

    // resume sender on site1
    vm2.invoke(() -> WANTestBase.resumeSender("ln"));
    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));

    // validate events reached site2 from site1
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200));

    // on site2, verify queue size again
    // this ensures that loopback is not happening since the queue size is same as before
    // the event coming from site1 are not enqueued again
    vm3.invoke(() -> WANTestBase.verifyQueueSize("ny", 100));
    vm6.invoke(() -> WANTestBase.verifyQueueSize("ny", 100));
    vm7.invoke(() -> WANTestBase.verifyQueueSize("ny", 100));

    // resume sender on site2
    vm3.invoke(() -> WANTestBase.resumeSender("ny"));
    vm6.invoke(() -> WANTestBase.resumeSender("ny"));
    vm7.invoke(() -> WANTestBase.resumeSender("ny"));

    // validate region size on both the sites
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200));
  }

  /**
   * Test loop back issue among 3 WAN sites with Ring topology i.e. LN -> NY -> TK -> LN Site1 (LN):
   * vm3, vm6 Site2 (NY): vm4, vm7 Site3 (TK): vm5
   */
  @Test
  public void testParallelPropagationLoopBack3Sites() {
    // Create locators
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer tkPort = vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    // create cache and receivers on all the 3 sites
    createCacheInVMs(lnPort, vm3, vm6);
    createReceiverInVMs(vm3, vm6);
    createCacheInVMs(nyPort, vm4, vm7);
    createReceiverInVMs(vm4, vm7);
    createCacheInVMs(tkPort, vm5);
    createReceiverInVMs(vm5);


    // create senders on all the 3 sites
    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createSender("ny", 3, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ny", 3, true, 100, 10, false, false, null, true));

    vm5.invoke(() -> WANTestBase.createSender("tk", 1, true, 100, 10, false, false, null, true));

    // create PR on the 3 sites
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 0, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny", 0, 100,
        isOffHeap()));

    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "tk", 0, 100,
        isOffHeap()));

    // start senders on all the sites
    startSenderInVMs("ln", vm3, vm6);

    startSenderInVMs("ny", vm4, vm7);

    vm5.invoke(() -> WANTestBase.startSender("tk"));

    // pause senders on site1 and site3. Site2 has the sender running to pass along events
    vm3.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));

    vm5.invoke(() -> WANTestBase.pauseSender("tk"));

    // do puts on site1
    vm3.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 100));

    // do more puts on site3
    vm5.invoke(() -> WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 100, 200));

    // verify queue size on site1 and site3
    vm3.invoke(() -> WANTestBase.verifyQueueSize("ln", 100));
    vm5.invoke(() -> WANTestBase.verifyQueueSize("tk", 100));

    // resume sender on site1 so that events reach site2 and from there to site3
    vm3.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm3.invoke(() -> waitForSenderRunningState("ln"));

    // validate region size on site2 (should have 100) and site3 (should have 200)
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200));

    // verify queue size remains same on site3 which means event loopback did not happen
    // this means events coming from site1 are not enqueued back into the sender
    vm5.invoke(() -> WANTestBase.verifyQueueSize("tk", 100));

    // resume sender on site3
    vm5.invoke(() -> WANTestBase.resumeSender("tk"));
    vm5.invoke(() -> waitForSenderRunningState("tk"));

    // validate region size
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200));
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200));
  }

  /**
   * Test loop back issue among 3 WAN sites with N to N topology i.e. each site connected to all
   * other sites. Puts are done to only one DS. LN site: vm3, vm6 NY site: vm4, vm7 TK site: vm5
   */
  @Test
  public void testParallelPropagationLoopBack3SitesNtoNTopologyPutFromOneDS() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer tkPort = vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    createCacheInVMs(lnPort, vm3, vm6);
    createCacheInVMs(nyPort, vm4, vm7);
    createCacheInVMs(tkPort, vm5);
    vm3.invoke(WANTestBase::createReceiver);
    vm4.invoke(WANTestBase::createReceiver);
    vm5.invoke(WANTestBase::createReceiver);

    // site1
    vm3.invoke(() -> WANTestBase.createSender("ln1", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln1", 2, true, 100, 10, false, false, null, true));

    vm3.invoke(() -> WANTestBase.createSender("ln2", 3, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln2", 3, true, 100, 10, false, false, null, true));

    // site2
    vm4.invoke(() -> WANTestBase.createSender("ny1", 1, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ny1", 1, true, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createSender("ny2", 3, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ny2", 3, true, 100, 10, false, false, null, true));

    // site3
    vm5.invoke(() -> WANTestBase.createSender("tk1", 1, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("tk2", 2, true, 100, 10, false, false, null, true));

    // create PR
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln1,ln2", 0,
        1, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln1,ln2", 0,
        1, isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny1,ny2", 0,
        1, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ny1,ny2", 0,
        1, isOffHeap()));

    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "tk1,tk2", 0,
        1, isOffHeap()));

    // start all the senders
    vm3.invoke(() -> WANTestBase.startSender("ln1"));
    vm3.invoke(() -> WANTestBase.startSender("ln2"));
    vm6.invoke(() -> WANTestBase.startSender("ln1"));
    vm6.invoke(() -> WANTestBase.startSender("ln2"));

    vm4.invoke(() -> WANTestBase.startSender("ny1"));
    vm4.invoke(() -> WANTestBase.startSender("ny2"));
    vm7.invoke(() -> WANTestBase.startSender("ny1"));
    vm7.invoke(() -> WANTestBase.startSender("ny2"));

    vm5.invoke(() -> WANTestBase.startSender("tk1"));
    vm5.invoke(() -> WANTestBase.startSender("tk2"));

    // pause senders on all the sites
    vm3.invoke(() -> WANTestBase.pauseSender("ln1"));
    vm3.invoke(() -> WANTestBase.pauseSender("ln2"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln1"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln2"));

    vm4.invoke(() -> WANTestBase.pauseSender("ny1"));
    vm4.invoke(() -> WANTestBase.pauseSender("ny2"));
    vm7.invoke(() -> WANTestBase.pauseSender("ny1"));
    vm7.invoke(() -> WANTestBase.pauseSender("ny2"));

    vm5.invoke(() -> WANTestBase.pauseSender("tk1"));
    vm5.invoke(() -> WANTestBase.pauseSender("tk2"));

    // this is required since sender pause doesn't take effect immediately
    Wait.pause(1000);

    // do puts on site1
    vm3.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 100));

    // verify queue size on site1 and site3
    vm3.invoke(() -> WANTestBase.verifyQueueSize("ln1", 100));
    vm3.invoke(() -> WANTestBase.verifyQueueSize("ln2", 100));

    // resume sender (from site1 to site2) on site1
    vm3.invoke(() -> WANTestBase.resumeSender("ln1"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln1"));

    // validate region size on site2
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));

    // verify queue size on site2 (sender 2 to 1)
    // should remain at 0 as the events from site1 should not go back to site1
    vm4.invoke(() -> WANTestBase.verifyQueueSize("ny1", 0));

    // verify queue size on site2 (sender 2 to 3)
    // should remain at 0 as events from site1 will reach site3 directly..site2 need not send to
    // site3 again
    vm4.invoke(() -> WANTestBase.verifyQueueSize("ny2", 0));

    // do more puts on site3
    vm5.invoke(() -> WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 100, 200));

    // resume sender (from site3 to site2) on site3
    vm5.invoke(() -> WANTestBase.resumeSender("tk2"));

    // validate region size on site2
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200));

    // verify queue size on site2 (sender 2 to 3)
    // should remain at 0 as the events from site3 should not go back to site3
    vm4.invoke(() -> WANTestBase.verifyQueueSize("ny2", 0));

    // verify queue size on site2 (sender 2 to 1)
    // should remain at 0 as events from site3 will reach site1 directly..site2 need not send to
    // site1 again
    vm4.invoke(() -> WANTestBase.verifyQueueSize("ny1", 0));

    // resume all senders
    vm3.invoke(() -> WANTestBase.resumeSender("ln2"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln2"));

    vm4.invoke(() -> WANTestBase.resumeSender("ny1"));
    vm4.invoke(() -> WANTestBase.resumeSender("ny2"));
    vm7.invoke(() -> WANTestBase.resumeSender("ny1"));
    vm7.invoke(() -> WANTestBase.resumeSender("ny2"));

    vm5.invoke(() -> WANTestBase.resumeSender("tk1"));

    // validate region size on all sites
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200));
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200));
  }

  /**
   * Test loop back issue between 2 WAN sites (LN & NY). LN -> NY -> LN.
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
    vm2.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, true));

    // create senders on site-ln, Note: sender-id is its destination, i.e. ln
    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

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

    // verify tmpDroppedEvents should be 100 at site-ln, because the sender is not started yet
    vm3.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ln", 100));
    vm5.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ln", 100));

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

    vm2.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, true));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

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

    vm2.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, false));
    vm4.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, false));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));

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

    vm2.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, false));
    vm4.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, false));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));

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

    vm2.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, false));
    vm4.invoke(() -> WANTestBase.createSender("ny", 1, true, 100, 10, false, false, null, false));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));

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

    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 50));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 50));

    startSenderInVMsAsync("ny", vm2);

    vm2.invoke(() -> WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 100, 1000));

    vm2.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));
    vm4.invoke(() -> WANTestBase.verifyTmpDroppedEventSize("ny", 0));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));

    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 950));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 950));

    // verify the secondary's queues are drained at site-ny
    vm2.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ny"));
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ny"));
  }
}
