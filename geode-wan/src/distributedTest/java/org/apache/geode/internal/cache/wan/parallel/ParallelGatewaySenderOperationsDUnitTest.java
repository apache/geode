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

import static org.apache.geode.distributed.internal.DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortsForDUnitSite;
import static org.apache.geode.internal.cache.tier.sockets.Message.MAX_MESSAGE_SIZE_PROPERTY;
import static org.apache.geode.internal.util.ArrayUtils.asList;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import util.TestException;

import org.apache.geode.GemFireIOException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.tier.sockets.MessageTooLargeException;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.internal.offheap.MemoryAllocatorImpl;
import org.apache.geode.internal.offheap.OffHeapRegionEntryHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * DUnit test for operations on ParallelGatewaySender
 */
@Category(WanTest.class)
@SuppressWarnings("serial")
public class ParallelGatewaySenderOperationsDUnitTest extends WANTestBase {

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    addIgnoredException("Broken pipe||Unexpected IOException");
  }

  @Test(timeout = 300_000)
  public void testStopOneConcurrentGatewaySenderWithSSL() throws Exception {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    vm2.invoke(() -> createReceiverWithSSL(nyPort));
    vm3.invoke(() -> createReceiverWithSSL(nyPort));

    vm4.invoke(() -> createCacheWithSSL(lnPort));
    vm5.invoke(() -> createCacheWithSSL(lnPort));

    vm4.invoke(() -> createConcurrentSender("ln", 2, true, 100, 10, false, true, null, true, 5,
        GatewaySender.OrderPolicy.KEY));
    vm5.invoke(() -> createConcurrentSender("ln", 2, true, 100, 10, false, true, null, true, 5,
        GatewaySender.OrderPolicy.KEY));

    String regionName = getUniqueName() + "_PR";
    vm4.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));

    vm2.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    vm3.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> doPuts(regionName, 10));

    vm4.invoke(() -> stopSender("ln"));
    vm4.invoke(() -> startSender("ln"));

    vm4.invoke(() -> doPuts(regionName, 10));

    vm5.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> startSender("ln"));
  }

  @Test
  public void testParallelGatewaySenderWithoutStarting() {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, false);

    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 1000));

    vm4.invoke(() -> verifySenderStoppedState("ln"));
    vm5.invoke(() -> verifySenderStoppedState("ln"));
    vm6.invoke(() -> verifySenderStoppedState("ln"));
    vm7.invoke(() -> verifySenderStoppedState("ln"));

    validateRegionSizes(getUniqueName() + "_PR", 0, vm2, vm3);
  }

  /**
   * ParallelGatewaySender should not be started on Accessor node
   *
   * <p>
   * TRAC #44323: NewWan: ParallelGatewaySender should not be started on Accessor Node
   */
  @Test
  public void testParallelGatewaySenderStartOnAccessorNode() {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, true, true);

    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));

    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 10));

    vm4.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));

    validateRegionSizes(getUniqueName() + "_PR", 10, vm2, vm3);
  }

  /**
   * Normal scenario in which the sender is paused in between.
   */
  @Test
  public void testParallelPropagationSenderPause() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    // make sure all the senders are running before doing any puts
    waitForSendersRunning();

    // FIRST RUN: now, the senders are started. So, start the puts
    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 100));

    // now, pause all of the senders
    vm4.invoke(() -> pauseSender("ln"));
    vm5.invoke(() -> pauseSender("ln"));
    vm6.invoke(() -> pauseSender("ln"));
    vm7.invoke(() -> pauseSender("ln"));

    // SECOND RUN: keep one thread doing puts to the region
    vm4.invokeAsync(() -> doPuts(getUniqueName() + "_PR", 1000));

    // verify region size remains on remote vm and is restricted below a specified limit (i.e.
    // number of puts in the first run)
    vm2.invoke(() -> validateRegionSizeRemainsSame(getUniqueName() + "_PR", 100));
  }

  /**
   * Normal scenario in which a paused sender is resumed.
   */
  @Test
  public void testParallelPropagationSenderResume() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    // make sure all the senders are running before doing any puts
    waitForSendersRunning();

    int numPuts = 1000;
    // now, the senders are started. So, start the puts
    AsyncInvocation async = vm4.invokeAsync(() -> doPuts(getUniqueName() + "_PR", numPuts));

    // now, pause all of the senders
    vm4.invoke(() -> pauseSender("ln"));
    vm5.invoke(() -> pauseSender("ln"));
    vm6.invoke(() -> pauseSender("ln"));
    vm7.invoke(() -> pauseSender("ln"));

    // resume the senders
    vm4.invoke(() -> resumeSender("ln"));
    vm5.invoke(() -> resumeSender("ln"));
    vm6.invoke(() -> resumeSender("ln"));
    vm7.invoke(() -> resumeSender("ln"));

    async.await(2, TimeUnit.MINUTES);
    validateParallelSenderQueueAllBucketsDrained();

    // find the region size on remote vm
    vm2.invoke(() -> validateRegionSize(getUniqueName() + "_PR", numPuts));
  }

  /**
   * Negative scenario in which a sender that is stopped (and not paused) is resumed. Expected:
   * resume is only valid for pause. If a sender which is stopped is resumed, it will not be started
   * again.
   */
  @Test
  public void testParallelPropagationSenderResumeNegativeScenario() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(
        () -> createPartitionedRegion(getUniqueName() + "_PR", "ln", 1, 100, isOffHeap()));
    vm5.invoke(
        () -> createPartitionedRegion(getUniqueName() + "_PR", "ln", 1, 100, isOffHeap()));

    vm2.invoke(
        () -> createPartitionedRegion(getUniqueName() + "_PR", null, 1, 100, isOffHeap()));
    vm3.invoke(
        () -> createPartitionedRegion(getUniqueName() + "_PR", null, 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    // wait till the senders are running
    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));

    // start the puts
    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 100));

    // let the queue drain completely
    vm4.invoke(() -> validateQueueContents("ln", 0));

    // stop the senders
    vm4.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> stopSender("ln"));

    // now, try to resume a stopped sender
    vm4.invoke(() -> resumeSender("ln"));
    vm5.invoke(() -> resumeSender("ln"));

    // do more puts
    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 1000));

    // validate region size on remote vm to contain only the events put in local site
    // before the senders are stopped.
    vm2.invoke(() -> validateRegionSize(getUniqueName() + "_PR", 100));
  }

  /**
   * Normal scenario in which a sender is stopped.
   */
  @Test
  public void testParallelPropagationSenderStop() throws Exception {
    addIgnoredException("Broken pipe");
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    // make sure all the senders are running before doing any puts
    waitForSendersRunning();

    // FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 100));

    // now, stop all of the senders
    stopSenders();

    // SECOND RUN: keep one thread doing puts
    vm4.invokeAsync(() -> doPuts(getUniqueName() + "_PR", 1000));

    // verify region size remains on remote vm and is restricted below a specified limit (number of
    // puts in the first run)
    vm2.invoke(() -> validateRegionSizeRemainsSame(getUniqueName() + "_PR", 100));
  }

  /**
   * Normal scenario in which a sender is stopped and then started again.
   */
  @Test
  public void testParallelPropagationSenderStartAfterStop() throws Exception {
    addIgnoredException("Broken pipe");
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];
    String regionName = getUniqueName() + "_PR";

    createCacheInVMs(nyPort, vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm2.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    vm3.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));

    createReceiverInVMs(vm2, vm3);

    vm4.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    vm6.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    vm7.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));

    vm4.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    // make sure all the senders are running before doing any puts
    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));

    // FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> doPuts(regionName, 200));

    // now, stop all of the senders
    vm4.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> stopSender("ln"));
    vm6.invoke(() -> stopSender("ln"));
    vm7.invoke(() -> stopSender("ln"));

    // Region size on remote site should remain same and below the number of puts done in the FIRST
    // RUN
    vm2.invoke(() -> validateRegionSizeRemainsSame(regionName, 200));

    // SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(() -> doPuts(regionName, 1000));

    // Region size on remote site should remain same and below the number of puts done in the FIRST
    // RUN
    vm2.invoke(() -> validateRegionSizeRemainsSame(regionName, 200));

    // start the senders again
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));

    // Region size on remote site should remain same and below the number of puts done in the FIRST
    // RUN
    vm2.invoke(() -> validateRegionSizeRemainsSame(regionName, 200));

    // SECOND RUN: do some more puts
    vm4.invoke(() -> doPuts(regionName, 1000));

    // verify all the buckets on all the sender nodes are drained
    validateParallelSenderQueueAllBucketsDrained();

    // verify the events propagate to remote site
    vm2.invoke(() -> validateRegionSize(regionName, 1000));

    vm4.invoke(() -> validateQueueSizeStat("ln", 0));
    vm5.invoke(() -> validateQueueSizeStat("ln", 0));
    vm6.invoke(() -> validateQueueSizeStat("ln", 0));
    vm7.invoke(() -> validateQueueSizeStat("ln", 0));
  }

  /**
   * Normal scenario in which a sender is stopped and then started again. Differs from above test
   * case in the way that when the sender is starting from stopped state, puts are simultaneously
   * happening on the region by another thread.
   */
  @Test
  public void testParallelPropagationSenderStartAfterStop_Scenario2() throws Exception {
    addIgnoredException("Broken pipe");
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    // make sure all the senders are running before doing any puts
    waitForSendersRunning();

    // FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 200));

    // Make sure the puts make it to the remote side
    vm2.invoke(() -> validateRegionSize(getUniqueName() + "_PR", 200));
    vm3.invoke(() -> validateRegionSize(getUniqueName() + "_PR", 200));

    // now, stop all of the senders
    stopSenders();

    vm2.invoke(() -> validateRegionSize(getUniqueName() + "_PR", 200));
    vm3.invoke(() -> validateRegionSize(getUniqueName() + "_PR", 200));

    // SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 1000));

    // Region size on remote site should remain same and below the number of puts done in the FIRST
    // RUN
    vm2.invoke(() -> validateRegionSizeRemainsSame(getUniqueName() + "_PR", 200));

    // SECOND RUN: start async puts on region
    ArrayList<Integer> vm4List = null;
    ArrayList<Integer> vm5List = null;
    ArrayList<Integer> vm6List = null;
    ArrayList<Integer> vm7List = null;
    boolean foundEventsDroppedDueToPrimarySenderNotRunning = false;
    int count = 0;

    do {
      stopSenders();
      AsyncInvocation async = vm4.invokeAsync(() -> doPuts(getUniqueName() + "_PR", 5000));

      // when puts are happening by another thread, start the senders
      startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

      async.join();
      vm4List =
          (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStatsForDroppedEvents("ln"));
      vm5List =
          (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStatsForDroppedEvents("ln"));
      vm6List =
          (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStatsForDroppedEvents("ln"));
      vm7List =
          (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStatsForDroppedEvents("ln"));
      if (vm4List.get(0) + vm5List.get(0) + vm6List.get(0) + vm7List.get(0) > 0) {
        foundEventsDroppedDueToPrimarySenderNotRunning = true;
      }
      count++;
    } while (foundEventsDroppedDueToPrimarySenderNotRunning == false && count < 5);
    assertThat(foundEventsDroppedDueToPrimarySenderNotRunning);

    // verify all the buckets on all the sender nodes are drained
    validateParallelSenderQueueAllBucketsDrained();

    // verify that the queue size ultimately becomes zero. That means all the events propagate to
    // remote site.

    vm4.invoke(() -> validateQueueContents("ln", 0));
  }

  /**
   * Normal scenario in which a sender is stopped and then started again on accessor node.
   */
  @Test
  public void testParallelPropagationSenderStartAfterStopOnAccessorNode() throws Exception {
    addIgnoredException("Broken pipe");
    addIgnoredException("Connection reset");
    addIgnoredException("Unexpected IOException");
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, true, true);

    // make sure all the senders are not running on accessor nodes and running on non-accessor nodes
    waitForSendersRunning();

    // FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 200));

    // now, stop all of the senders
    stopSenders();

    // SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 1000));

    // Region size on remote site should remain same and below the number of puts done in the FIRST
    // RUN
    vm2.invoke(() -> validateRegionSizeRemainsSame(getUniqueName() + "_PR", 200));

    // start the senders again
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    // Region size on remote site should remain same and below the number of puts done in the FIRST
    // RUN
    vm2.invoke(() -> validateRegionSizeRemainsSame(getUniqueName() + "_PR", 200));

    // SECOND RUN: do some more puts
    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 1000));

    // verify all buckets drained only on non-accessor nodes.
    vm4.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));

    // verify the events propagate to remote site
    vm2.invoke(() -> validateRegionSize(getUniqueName() + "_PR", 1000));
  }

  /**
   * Normal scenario in which a combinations of start, pause, resume operations is tested
   */
  @Test
  public void testStartPauseResumeParallelGatewaySender() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 1000));

    // Since puts are already done on userPR, it will have the buckets created.
    // During sender start, it will wait until those buckets are created for shadowPR as well.
    // Start the senders in async threads, so colocation of shadowPR will be complete and
    // missing buckets will be created in PRHARedundancyProvider.createMissingBuckets().
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    waitForSendersRunning();

    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 5000));

    vm4.invoke(() -> pauseSender("ln"));
    vm5.invoke(() -> pauseSender("ln"));
    vm6.invoke(() -> pauseSender("ln"));
    vm7.invoke(() -> pauseSender("ln"));

    vm4.invoke(() -> verifySenderPausedState("ln"));
    vm5.invoke(() -> verifySenderPausedState("ln"));
    vm6.invoke(() -> verifySenderPausedState("ln"));
    vm7.invoke(() -> verifySenderPausedState("ln"));

    AsyncInvocation async = vm4.invokeAsync(() -> doPuts(getUniqueName() + "_PR", 1000));

    vm4.invoke(() -> resumeSender("ln"));
    vm5.invoke(() -> resumeSender("ln"));
    vm6.invoke(() -> resumeSender("ln"));
    vm7.invoke(() -> resumeSender("ln"));

    vm4.invoke(() -> verifySenderResumedState("ln"));
    vm5.invoke(() -> verifySenderResumedState("ln"));
    vm6.invoke(() -> verifySenderResumedState("ln"));
    vm7.invoke(() -> verifySenderResumedState("ln"));

    async.await();

    // verify all buckets drained on all sender nodes.
    validateParallelSenderQueueAllBucketsDrained();

    validateRegionSizes(getUniqueName() + "_PR", 5000, vm2, vm3);
  }

  /**
   * Since the sender is attached to a region and in use, it can not be destroyed. Hence, exception
   * is thrown by the sender API.
   */
  @Test
  public void testDestroyParallelGatewaySenderExceptionScenario() {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    // make sure all the senders are running before doing any puts
    waitForSendersRunning();

    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 1000));

    // try destroying on couple of nodes
    Throwable caughtException = catchThrowable(() -> vm4.invoke(() -> destroySender("ln")));

    assertThat(caughtException).isInstanceOf(RMIException.class);
    assertThat(caughtException.getCause()).isInstanceOf(GatewaySenderException.class);

    caughtException = catchThrowable(() -> vm5.invoke(() -> destroySender("ln")));

    assertThat(caughtException).isInstanceOf(RMIException.class);
    assertThat(caughtException.getCause()).isInstanceOf(GatewaySenderException.class);

    vm2.invoke(() -> validateRegionSize(getUniqueName() + "_PR", 1000));
  }

  @Test
  public void testDestroyParallelGatewaySender() {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    // make sure all the senders are running
    waitForSendersRunning();

    vm4.invoke(() -> doPuts(getUniqueName() + "_PR", 1000));

    // stop the sender and remove from region before calling destroy on it
    stopSenders();

    vm4.invoke(() -> removeSenderFromTheRegion("ln", getUniqueName() + "_PR"));
    vm5.invoke(() -> removeSenderFromTheRegion("ln", getUniqueName() + "_PR"));
    vm6.invoke(() -> removeSenderFromTheRegion("ln", getUniqueName() + "_PR"));
    vm7.invoke(() -> removeSenderFromTheRegion("ln", getUniqueName() + "_PR"));

    vm4.invoke(() -> destroySender("ln"));
    vm5.invoke(() -> destroySender("ln"));
    vm6.invoke(() -> destroySender("ln"));
    vm7.invoke(() -> destroySender("ln"));

    vm4.invoke(() -> verifySenderDestroyed("ln", true));
    vm5.invoke(() -> verifySenderDestroyed("ln", true));
    vm6.invoke(() -> verifySenderDestroyed("ln", true));
    vm7.invoke(() -> verifySenderDestroyed("ln", true));
  }

  @Test
  public void destroyParallelGatewaySenderShouldNotStopDispatchingFromOtherSendersAttachedToTheRegion() {
    String site2SenderId = "site2-sender";
    String site3SenderId = "site3-sender";
    String regionName = testName.getMethodName();
    int[] ports = getRandomAvailableTCPPortsForDUnitSite(3);
    int site1Port = ports[0];
    int site2Port = ports[1];
    int site3Port = ports[2];
    Set<String> site1RemoteLocators =
        Stream.of("localhost[" + site2Port + "]", "localhost[" + site3Port + "]")
            .collect(Collectors.toSet());
    Set<String> site2RemoteLocators =
        Stream.of("localhost[" + site1Port + "]", "localhost[" + site3Port + "]")
            .collect(Collectors.toSet());
    Set<String> site3RemoteLocators =
        Stream.of("localhost[" + site1Port + "]", "localhost[" + site2Port + "]")
            .collect(Collectors.toSet());

    // Start 3 sites.
    vm0.invoke(() -> createLocator(1, site1Port,
        Collections.singleton("localhost[" + site1Port + "]"), site1RemoteLocators));
    vm1.invoke(() -> createLocator(2, site2Port,
        Collections.singleton("localhost[" + site2Port + "]"), site2RemoteLocators));
    vm2.invoke(() -> createLocator(3, site3Port,
        Collections.singleton("localhost[" + site3Port + "]"), site3RemoteLocators));

    // Create the cache on the 3 sites.
    createCacheInVMs(site1Port, vm3);
    createCacheInVMs(site2Port, vm4);
    createCacheInVMs(site3Port, vm5);

    // Create receiver and region on sites 2 and 3.
    asList(vm4, vm5).forEach(vm -> vm.invoke(() -> {
      createReceiver();
      createPartitionedRegion(regionName, null, 1, 113, isOffHeap());
    }));

    // Create senders and partitioned region on site 1.
    vm3.invoke(() -> {
      createSender(site2SenderId, 2, true, 100, 20, false, false, null, false);
      createSender(site3SenderId, 3, true, 100, 20, false, false, null, false);
      waitForSenderRunningState(site2SenderId);
      waitForSenderRunningState(site3SenderId);

      createPartitionedRegion(regionName, String.join(",", site2SenderId, site3SenderId), 1, 113,
          isOffHeap());
    });

    // #################################################################################### //

    final int FIRST_BATCH = 100;
    final int SECOND_BATCH = 200;
    final Map<String, String> firstBatch = new HashMap<>();
    IntStream.range(0, FIRST_BATCH).forEach(i -> firstBatch.put("Key" + i, "Value" + i));
    final Map<String, String> secondBatch = new HashMap<>();
    IntStream.range(FIRST_BATCH, SECOND_BATCH)
        .forEach(i -> secondBatch.put("Key" + i, "Value" + i));

    // Insert first batch and wait until the queues are empty.
    vm3.invoke(() -> {
      cache.getRegion(regionName).putAll(firstBatch);
      checkQueueSize(site2SenderId, 0);
      checkQueueSize(site3SenderId, 0);
    });

    // Wait until sites 2 and 3 have received all updates.
    asList(vm4, vm5).forEach(vm -> vm.invoke(() -> {
      Region<String, String> region = cache.getRegion(regionName);
      await().untilAsserted(() -> assertThat(region.size()).isEqualTo(FIRST_BATCH));
      firstBatch.forEach((key, value) -> assertThat(region.get(key)).isEqualTo(value));
    }));

    // Stop sender to site3, remove it from the region and destroy it.
    vm3.invoke(() -> {
      stopSender(site3SenderId);
      removeSenderFromTheRegion(site3SenderId, regionName);
      destroySender(site3SenderId);
      verifySenderDestroyed(site3SenderId, true);
    });

    // Insert second batch and wait until the queue is empty.
    vm3.invoke(() -> {
      cache.getRegion(regionName).putAll(secondBatch);
      checkQueueSize(site2SenderId, 0);
    });

    // Site 3 should only have the first batch.
    vm5.invoke(() -> {
      Region<String, String> region = cache.getRegion(regionName);
      await().untilAsserted(() -> assertThat(region.size()).isEqualTo(FIRST_BATCH));
      firstBatch.forEach((key, value) -> assertThat(region.get(key)).isEqualTo(value));
    });

    // Site 2 should have both batches.
    vm4.invoke(() -> {
      Region<String, String> region = cache.getRegion(regionName);
      await().untilAsserted(() -> assertThat(region.size()).isEqualTo(SECOND_BATCH));
      firstBatch.forEach((key, value) -> assertThat(region.get(key)).isEqualTo(value));
      secondBatch.forEach((key, value) -> assertThat(region.get(key)).isEqualTo(value));
    });
  }

  @Test
  public void testParallelGatewaySenderMessageTooLargeException() {
    vm4.invoke(() -> System.setProperty(MAX_MESSAGE_SIZE_PROPERTY, String.valueOf(1024 * 1024)));

    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    // Create and start sender with reduced maximum message size and 1 dispatcher thread
    String regionName = getUniqueName() + "_PR";
    vm4.invoke(() -> createCache(lnPort));
    vm4.invoke(() -> setNumDispatcherThreadsForTheRun(1));
    vm4.invoke(() -> createSender("ln", 2, true, 100, 100, false, false, null, false));
    vm4.invoke(() -> createPartitionedRegion(regionName, "ln", 0, 100, isOffHeap()));

    // Do puts
    int numPuts = 200;
    vm4.invoke(() -> doPuts(regionName, numPuts, new byte[11000]));
    validateRegionSizes(regionName, numPuts, vm4);

    // Start receiver
    addIgnoredException(MessageTooLargeException.class.getName(), vm4);
    addIgnoredException(GemFireIOException.class.getName(), vm4);

    vm2.invoke(() -> createCache(nyPort));
    vm2.invoke(() -> createPartitionedRegion(regionName, null, 0, 100, isOffHeap()));
    vm2.invoke(() -> createReceiver());

    validateRegionSizes(regionName, numPuts, vm2);

    vm4.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      GeodeAwaitility.await()
          .untilAsserted(() -> assertThat(System.getProperty(MAX_MESSAGE_SIZE_PROPERTY))
              .isEqualTo(String.valueOf(1024 * 1024)));
      GeodeAwaitility.await().untilAsserted(() -> {
        assertThat(sender.getStatistics().getBatchesResized()).isGreaterThan(0);
      });
    });
  }

  @Test
  public void testParallelGatewaySenderConcurrentPutClearNoOffheapOrphans()
      throws Exception {
    MemberVM locator = clusterStartupRule.startLocatorVM(1, new Properties());
    Properties properties = new Properties();
    properties.put(OFF_HEAP_MEMORY_SIZE_NAME, "100");
    MemberVM server = clusterStartupRule.startServerVM(2, properties, locator.getPort());
    final String regionName = "portfolios";
    final String gatewaySenderId = "ln";

    server.invoke(() -> {
      IgnoredException ie = addIgnoredException("could not get remote locator");
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender =
          cache.createGatewaySenderFactory().setParallel(true).create(gatewaySenderId, 1);
      Region userRegion = cache.createRegionFactory(RegionShortcut.PARTITION).setOffHeap(true)
          .addGatewaySenderId("ln").create(regionName);
      PartitionedRegion shadowRegion = (PartitionedRegion) ((AbstractGatewaySender) sender)
          .getEventProcessor().getQueue().getRegion();
      CacheWriter mockCacheWriter = mock(CacheWriter.class);
      CountDownLatch cacheWriterLatch = new CountDownLatch(1);
      CountDownLatch shadowRegionClearLatch = new CountDownLatch(1);

      doAnswer(invocation -> {
        // The cache writer is invoked between when the region entry is created with value of
        // Token.REMOVED_PHASE_1 and when it is replaced with the actual GatewaySenderEvent.
        // We use this hook to trigger the clear logic when the token is still in the
        // region entry.
        cacheWriterLatch.countDown();
        // Wait until the clear is complete before putting the actual GatewaySenderEvent in the
        // region entry.
        shadowRegionClearLatch.await();
        return null;
      }).when(mockCacheWriter).beforeCreate(any());

      shadowRegion.setCacheWriter(mockCacheWriter);

      ExecutorService service = Executors.newFixedThreadPool(2);

      List<Callable<Object>> callables = new ArrayList<>();

      // In one thread, we are going to put some test data in the user region,
      // which will eventually put the GatewaySenderEvent into the shadow region
      callables.add(Executors.callable(() -> {
        try {
          userRegion.put("testKey", "testValue");
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }));

      // In another thread, we are clear the shadow region's buckets. If the region entry is a
      // Token.REMOVED_PHASE_1 at this time, a leak is possible. We can guarantee that the
      // Token.REMOVED_PHASE_1 is present by using the mocked cache writer defined
      // above.
      callables.add(Executors.callable(() -> {
        try {
          OffHeapRegionEntryHelper.doWithOffHeapClear(new Runnable() {
            @Override
            public void run() {
              // Wait for the cache writer to be invoked to release this countdown latch.
              // This guarantees that the region entry will contain a Token.REMOVED_PHASE_1.
              try {
                cacheWriterLatch.await();
              } catch (InterruptedException e) {
                throw new TestException(
                    "Thread was interrupted while waiting for mocked cache writer to be invoked");
              }

              clearShadowBucketRegions(shadowRegion);

              // Signal to the cache writer that the clear is complete and the put can continue.
              shadowRegionClearLatch.countDown();
            }
          });
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }));

      List<Future<Object>> futures = service.invokeAll(callables, 10, TimeUnit.SECONDS);

      for (Future<Object> future : futures) {
        try {
          future.get();
        } catch (Exception ex) {
          throw new TestException(
              "Exception thrown while executing put and clear concurrently",
              ex);
        }
      }

      userRegion.close();

      await("Waiting for off-heap to be freed").until(
          () -> 0 == ((MemoryAllocatorImpl) cache.getOffHeapStore()).getOrphans(cache).size());
    });
  }

  private void clearShadowBucketRegions(PartitionedRegion shadowRegion) {
    PartitionedRegionDataStore.BucketVisitor bucketVisitor =
        new PartitionedRegionDataStore.BucketVisitor() {
          @Override
          public void visit(Integer bucketId, Region r) {
            ((BucketRegion) r).clearEntries(null);
          }
        };

    shadowRegion.getDataStore().visitBuckets(bucketVisitor);
  }

  private void createSendersReceiversAndPartitionedRegion(Integer lnPort, Integer nyPort,
      boolean createAccessors, boolean startSenders) {
    createSendersAndReceivers(lnPort, nyPort);

    createPartitionedRegions(createAccessors);

    if (startSenders) {
      startSenderInVMs("ln", vm4, vm5, vm6, vm7);
    }
  }

  private void createSendersAndReceivers(Integer lnPort, Integer nyPort) {
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
  }

  private void createPartitionedRegions(boolean createAccessors) {
    String regionName = getUniqueName() + "_PR";
    vm4.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));

    if (createAccessors) {
      vm6.invoke(() -> createPartitionedRegionAsAccessor(regionName, "ln", 1, 100));
      vm7.invoke(() -> createPartitionedRegionAsAccessor(regionName, "ln", 1, 100));
    } else {
      vm6.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
      vm7.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    }

    vm2.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    vm3.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
  }

  private void stopSenders() {
    vm4.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> stopSender("ln"));
    vm6.invoke(() -> stopSender("ln"));
    vm7.invoke(() -> stopSender("ln"));
  }

  private void waitForSendersRunning() {
    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));
  }

  private void validateParallelSenderQueueAllBucketsDrained() {
    vm4.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));
  }
}
