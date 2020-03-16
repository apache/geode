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

import static org.junit.Assert.fail;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ConcurrentParallelGatewaySenderOperation_1_DUnitTest extends WANTestBase {
  private static final long serialVersionUID = 1L;

  public ConcurrentParallelGatewaySenderOperation_1_DUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
  }

  @Test
  public void testParallelGatewaySenderWithoutStarting() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
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

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm4.invoke(() -> WANTestBase.verifySenderStoppedState("ln"));
    vm5.invoke(() -> WANTestBase.verifySenderStoppedState("ln"));
    vm6.invoke(() -> WANTestBase.verifySenderStoppedState("ln"));
    vm7.invoke(() -> WANTestBase.verifySenderStoppedState("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0));
  }

  /**
   * Defect 44323 (ParallelGatewaySender should not be started on Accessor node)
   */
  @Test
  public void testParallelGatewaySenderStartOnAccessorNode() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

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

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegionAsAccessor(getTestMethodName() + "_PR",
        "ln", 1, 100));
    vm7.invoke(() -> WANTestBase.createPartitionedRegionAsAccessor(getTestMethodName() + "_PR",
        "ln", 1, 100));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    // start the senders
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }


  /**
   * Normal scenario in which the sender is paused in between.
   *
   */
  @Test
  public void testParallelPropagationSenderPause() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));

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

    // make sure all the senders are running before doing any puts
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    // FIRST RUN: now, the senders are started. So, start the puts
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 100));

    // now, deprecatedPause all of the senders
    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));
    deprecatedPause(2000);
    // SECOND RUN: keep one thread doing puts to the region
    vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // verify region size remains on remote vm and is restricted below a specified limit (i.e.
    // number of puts in the first run)
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 100));
  }

  /**
   * Normal scenario in which a paused sender is resumed.
   *
   */
  @Test
  public void testParallelPropagationSenderResume() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 8, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 8, OrderPolicy.KEY));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 8, OrderPolicy.KEY));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 8, OrderPolicy.KEY));

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

    // make sure all the senders are running before doing any puts
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    // now, the senders are started. So, start the puts
    vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // now, deprecatedPause all of the senders
    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    // sleep for a second or two
    deprecatedPause(2000);

    // resume the senders
    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    deprecatedPause(2000);

    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    // find the region size on remote vm
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));

  }

  /**
   * Negative scenario in which a sender that is stopped (and not paused) is resumed. Expected:
   * resume is only valid for deprecatedPause. If a sender which is stopped is resumed, it will not
   * be started
   * again.
   *
   */
  @Test
  public void testParallelPropagationSenderResumeNegativeScenario() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(nyPort, vm4, vm5);
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 4, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 4, OrderPolicy.KEY));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    // wait till the senders are running
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    // start the puts
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 100));

    // let the queue drain completely
    vm4.invoke(() -> WANTestBase.validateQueueContents("ln", 0));

    // stop the senders
    vm4.invoke(() -> WANTestBase.stopSender("ln"));
    vm5.invoke(() -> WANTestBase.stopSender("ln"));

    // now, try to resume a stopped sender
    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));

    // do more puts
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // validate region size on remote vm to contain only the events put in local site
    // before the senders are stopped.
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));
  }

  /**
   * Normal scenario in which a sender is stopped.
   *
   */
  @Test
  public void testParallelPropagationSenderStop() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 3, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 3, OrderPolicy.KEY));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 3, OrderPolicy.KEY));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 3, OrderPolicy.KEY));

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

    // FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 100));

    // now, stop all of the senders
    vm4.invoke(() -> WANTestBase.stopSender("ln"));
    vm5.invoke(() -> WANTestBase.stopSender("ln"));
    vm6.invoke(() -> WANTestBase.stopSender("ln"));
    vm7.invoke(() -> WANTestBase.stopSender("ln"));

    // SECOND RUN: keep one thread doing puts
    vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // verify region size remains on remote vm and is restricted below a specified limit (number of
    // puts in the first run)
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 100));
  }

  /**
   * Normal scenario in which a sender is stopped and then started again.
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testParallelPropagationSenderStartAfterStop() throws Throwable {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 4, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 4, OrderPolicy.KEY));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 4, OrderPolicy.KEY));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 4, OrderPolicy.KEY));

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

    // FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 200));

    // now, stop all of the senders
    vm4.invoke(() -> WANTestBase.stopSender("ln"));
    vm5.invoke(() -> WANTestBase.stopSender("ln"));
    vm6.invoke(() -> WANTestBase.stopSender("ln"));
    vm7.invoke(() -> WANTestBase.stopSender("ln"));

    deprecatedPause(2000);

    // SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // Region size on remote site should remain same and below the number of puts done in the FIRST
    // RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200));

    // start the senders again
    AsyncInvocation<?> vm4start = vm4.invokeAsync(() -> WANTestBase.startSender("ln"));
    AsyncInvocation<?> vm5start = vm5.invokeAsync(() -> WANTestBase.startSender("ln"));
    AsyncInvocation<?> vm6start = vm6.invokeAsync(() -> WANTestBase.startSender("ln"));
    AsyncInvocation<?> vm7start = vm7.invokeAsync(() -> WANTestBase.startSender("ln"));
    int START_TIMEOUT = 30000;
    vm4start.getResult(START_TIMEOUT);
    vm5start.getResult(START_TIMEOUT);
    vm6start.getResult(START_TIMEOUT);
    vm7start.getResult(START_TIMEOUT);

    // make sure all the senders are running before doing any puts
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    // Region size on remote site should remain same and below the number of puts done in the FIRST
    // RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200));

    // SECOND RUN: do some more puts
    AsyncInvocation<?> async =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    async.await();

    // verify all the buckets on all the sender nodes are drained
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    // verify the events propagate to remote site
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));

    vm4.invoke(() -> WANTestBase.validateQueueSizeStat("ln", 0));
    vm5.invoke(() -> WANTestBase.validateQueueSizeStat("ln", 0));
    vm6.invoke(() -> WANTestBase.validateQueueSizeStat("ln", 0));
    vm7.invoke(() -> WANTestBase.validateQueueSizeStat("ln", 0));
  }

  /**
   * Normal scenario in which a sender is stopped and then started again. Differs from above test
   * case in the way that when the sender is starting from stopped state, puts are simultaneously
   * happening on the region by another thread.
   *
   */
  @SuppressWarnings("deprecation")
  @Ignore("Bug47553")
  @Test
  public void testParallelPropagationSenderStartAfterStop_Scenario2() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
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

    getLogWriter().info("All the senders are now started");

    // FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 200));

    getLogWriter().info("Done few puts");

    // now, stop all of the senders
    vm4.invoke(() -> WANTestBase.stopSender("ln"));
    vm5.invoke(() -> WANTestBase.stopSender("ln"));
    vm6.invoke(() -> WANTestBase.stopSender("ln"));
    vm7.invoke(() -> WANTestBase.stopSender("ln"));

    getLogWriter().info("All the senders are stopped");
    deprecatedPause(2000);

    // SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    getLogWriter().info("Done some more puts in second run");

    // Region size on remote site should remain same and below the number of puts done in the FIRST
    // RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200));

    // SECOND RUN: start async puts on region
    AsyncInvocation<?> async =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 5000));
    getLogWriter().info("Started high number of puts by async thread");

    getLogWriter().info("Starting the senders at the same time");
    // when puts are happening by another thread, start the senders
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    getLogWriter().info("All the senders are started");

    async.await();

    deprecatedPause(2000);

    // verify all the buckets on all the sender nodes are drained
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    // verify that the queue size ultimately becomes zero. That means all the events propagate to
    // remote site.
    vm4.invoke(() -> WANTestBase.validateQueueContents("ln", 0));
  }

  /**
   * Normal scenario in which a sender is stopped and then started again on accessor node.
   *
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testParallelPropagationSenderStartAfterStopOnAccessorNode() throws Throwable {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegionAsAccessor(getTestMethodName() + "_PR",
        "ln", 1, 100));
    vm7.invoke(() -> WANTestBase.createPartitionedRegionAsAccessor(getTestMethodName() + "_PR",
        "ln", 1, 100));

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 4, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 4, OrderPolicy.KEY));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 4, OrderPolicy.KEY));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 4, OrderPolicy.KEY));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));


    createReceiverInVMs(vm2, vm3);

    // make sure all the senders are not running on accessor nodes and running on non-accessor nodes
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    // FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 200));

    // now, stop all of the senders
    vm4.invoke(() -> WANTestBase.stopSender("ln"));
    vm5.invoke(() -> WANTestBase.stopSender("ln"));
    vm6.invoke(() -> WANTestBase.stopSender("ln"));
    vm7.invoke(() -> WANTestBase.stopSender("ln"));

    // SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // Region size on remote site should remain same and below the number of puts done in the FIRST
    // RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200));

    // start the senders again
    AsyncInvocation<?> vm4start = vm4.invokeAsync(() -> WANTestBase.startSender("ln"));
    AsyncInvocation<?> vm5start = vm5.invokeAsync(() -> WANTestBase.startSender("ln"));
    AsyncInvocation<?> vm6start = vm6.invokeAsync(() -> WANTestBase.startSender("ln"));
    AsyncInvocation<?> vm7start = vm7.invokeAsync(() -> WANTestBase.startSender("ln"));
    vm4start.await();
    vm5start.await();
    vm6start.await();
    vm7start.await();

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));


    // Region size on remote site should remain same and below the number of puts done in the FIRST
    // RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200));

    // SECOND RUN: do some more puts
    AsyncInvocation<?> async =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    async.await();

    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));

    // verify all buckets drained only on non-accessor nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    // verify the events propagate to remote site
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }


  /**
   * Normal scenario in which a combinations of start, deprecatedPause, resume operations is tested
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testStartPauseResumeParallelGatewaySender() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    getLogWriter().info("Created cache on local site");

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));
    vm6.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));
    vm7.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
        true, 5, OrderPolicy.KEY));

    getLogWriter().info("Created senders on local site");

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    getLogWriter().info("Created PRs on local site");

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    getLogWriter().info("Created PRs on remote site");

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    getLogWriter().info("Done 1000 puts on local site");

    // Since puts are already done on userPR, it will have the buckets created.
    // During sender start, it will wait until those buckets are created for shadowPR as well.
    // Start the senders in async threads, so colocation of shadowPR will be complete and
    // missing buckets will be created in PRHARedundancyProvider.createMissingBuckets().
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    getLogWriter().info("Started senders on local site");

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 5000));
    getLogWriter().info("Done 5000 puts on local site");

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));
    getLogWriter().info("Paused senders on local site");

    vm4.invoke(() -> WANTestBase.verifySenderPausedState("ln"));
    vm5.invoke(() -> WANTestBase.verifySenderPausedState("ln"));
    vm6.invoke(() -> WANTestBase.verifySenderPausedState("ln"));
    vm7.invoke(() -> WANTestBase.verifySenderPausedState("ln"));

    AsyncInvocation<?> inv1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    getLogWriter().info("Started 1000 async puts on local site");

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));
    getLogWriter().info("Resumed senders on local site");

    vm4.invoke(() -> WANTestBase.verifySenderResumedState("ln"));
    vm5.invoke(() -> WANTestBase.verifySenderResumedState("ln"));
    vm6.invoke(() -> WANTestBase.verifySenderResumedState("ln"));
    vm7.invoke(() -> WANTestBase.verifySenderResumedState("ln"));

    try {
      inv1.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail("Interrupted the async invocation.");
    }

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 5000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 5000));
  }
}
