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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.execute.data.CustId;
import org.apache.geode.internal.cache.execute.data.Order;
import org.apache.geode.internal.cache.execute.data.OrderId;
import org.apache.geode.internal.cache.execute.data.Shipment;
import org.apache.geode.internal.cache.execute.data.ShipmentId;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.BatchException70;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
@RunWith(JUnitParamsRunner.class)
public class ParallelWANPropagationDUnitTest extends WANTestBase {
  private static final long serialVersionUID = 1L;

  public ParallelWANPropagationDUnitTest() {
    super();
  }

  @Test
  public void test_ParallelGatewaySenderMetaRegionNotExposedToUser_Bug44216() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCache(lnPort);
    createSender("ln", 2, true, 100, 300, false, false, null, true);
    createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap());

    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals("ln")) {
        sender = s;
        break;
      }
    }
    try {
      sender.start();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Failed with IOException");
    }

    GemFireCacheImpl gemCache = (GemFireCacheImpl) cache;
    Set<Region<?, ?>> regionSet = gemCache.rootRegions();

    for (Region<?, ?> r : regionSet) {
      if (r.getName()
          .equals(((AbstractGatewaySender) sender).getQueues().toArray(new RegionQueue[1])[0]
              .getRegion().getName())) {
        fail("The shadowPR is exposed to the user");
      }
    }
  }

  @Test
  public void testParallelPropagation_withoutRemoteSite() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // keep a larger batch to minimize number of exception occurrences in the log
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 300, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 300, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 300, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 300, false, false, null, true));

    vm4.invoke(createPartitionedRegionRunnable("ln", 1));
    vm5.invoke(createPartitionedRegionRunnable("ln", 1));
    vm6.invoke(createPartitionedRegionRunnable("ln", 1));
    vm7.invoke(createPartitionedRegionRunnable("ln", 1));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    // make sure all the senders are running before doing any puts
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    createCacheInVMs(nyPort, vm2, vm3);
    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());
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
   * Normal happy scenario test case.
   *
   */
  @Test
  public void testParallelPropagation() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(createPartitionedRegionRunnable("ln", 1));
    vm5.invoke(createPartitionedRegionRunnable("ln", 1));
    vm6.invoke(createPartitionedRegionRunnable("ln", 1));
    vm7.invoke(createPartitionedRegionRunnable("ln", 1));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  protected SerializableRunnableIF createReceiverPartitionedRegionRedundancy1() {
    return () -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap());
  }

  protected SerializableRunnableIF createPartitionedRegionRunnable(String senderIds,
      int redundantCopies) {
    return () -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", senderIds,
        redundantCopies, 100,
        isOffHeap());
  }

  protected SerializableRunnableIF waitForSenderRunnable() {
    return () -> WANTestBase.waitForSenderRunningState("ln");
  }

  @Test
  public void testParallelPropagation_ManualStart() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));

    vm4.invoke(createPartitionedRegionRunnable("ln", 1));
    vm5.invoke(createPartitionedRegionRunnable("ln", 1));
    vm6.invoke(createPartitionedRegionRunnable("ln", 1));
    vm7.invoke(createPartitionedRegionRunnable("ln", 1));

    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  /**
   * Normal happy scenario test case2.
   *
   */
  @Test
  public void testParallelPropagationPutBeforeSenderStart() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(createPartitionedRegionRunnable("ln", 1));
    vm5.invoke(createPartitionedRegionRunnable("ln", 1));
    vm6.invoke(createPartitionedRegionRunnable("ln", 1));
    vm7.invoke(createPartitionedRegionRunnable("ln", 1));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  /**
   * Local and remote sites are up and running. Local site cache is closed and the site is built
   * again. Puts are done to local site. Expected: Remote site should receive all the events put
   * after the local site was built back.
   *
   */
  @Category({WanTest.class})
  @Test
  public void testParallelPropagationWithLocalCacheClosedAndRebuilt() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(createPartitionedRegionRunnable("ln", 1));
    vm5.invoke(createPartitionedRegionRunnable("ln", 1));
    vm6.invoke(createPartitionedRegionRunnable("ln", 1));
    vm7.invoke(createPartitionedRegionRunnable("ln", 1));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    // -------------------Close and rebuild local site ---------------------------------

    vm4.invoke((SerializableRunnableIF) WANTestBase::killSender);
    vm5.invoke((SerializableRunnableIF) WANTestBase::killSender);
    vm6.invoke((SerializableRunnableIF) WANTestBase::killSender);
    vm7.invoke((SerializableRunnableIF) WANTestBase::killSender);

    Integer regionSize =
        vm2.invoke(() -> WANTestBase.getRegionSize(getTestMethodName() + "_PR"));
    getLogWriter().info("Region size on remote is: " + regionSize);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(createPartitionedRegionRunnable("ln", 1));
    vm5.invoke(createPartitionedRegionRunnable("ln", 1));
    vm6.invoke(createPartitionedRegionRunnable("ln", 1));
    vm7.invoke(createPartitionedRegionRunnable("ln", 1));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    // ------------------------------------------------------------------------------------

    IgnoredException.addIgnoredException(EntryExistsException.class.getName());
    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testParallelColocatedPropagation() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

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
   * Create colocated partitioned regions. Parent region has PGS attached and child region doesn't.
   *
   * Validate that events for parent region reaches remote site.
   *
   */

  @Test
  public void testParallelColocatedPropagation2() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createColocatedPartitionedRegions2(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createColocatedPartitionedRegions2(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createColocatedPartitionedRegions2(getTestMethodName(), "ln", 1,
        100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createColocatedPartitionedRegions2(getTestMethodName(), "ln", 1,
        100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> WANTestBase.createColocatedPartitionedRegions2(getTestMethodName(), null, 1,
        100, isOffHeap()));
    vm3.invoke(() -> WANTestBase.createColocatedPartitionedRegions2(getTestMethodName(), null, 1,
        100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName(), 1000));
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_child1", 1000));
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_child2", 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_child1", 0));
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_child2", 0));
  }

  @Category({WanTest.class})
  @Test
  public void testParallelPropagationWithOverflow() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    // let all the senders start before doing any puts to ensure that none of the events is lost
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.doHeavyPuts(getTestMethodName(), 150));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 150));
  }

  @Test
  public void testSerialReplicatedAndParallelPartitionedPropagation() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(
        () -> WANTestBase.createSender("lnSerial", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnSerial", 2, false, 100, 10, false, false, null, true));

    vm4.invoke(
        () -> WANTestBase.createSender("lnParallel", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnParallel", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(
        () -> WANTestBase.createSender("lnParallel", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(
        () -> WANTestBase.createSender("lnParallel", 2, true, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnParallel",
        1, 100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnParallel",
        1, 100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnParallel",
        1, 100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnParallel",
        1, 100, isOffHeap()));

    startSenderInVMs("lnSerial", vm4, vm5);

    startSenderInVMs("lnParallel", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    vm5.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testPartitionedParallelPropagationToTwoWanSites() {
    Integer lnPort = createFirstLocatorWithDSId(1);
    Integer nyPort = vm0.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer tkPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    createReceiverInVMs(vm2);

    createCacheInVMs(tkPort, vm3);
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());
    createReceiverInVMs(vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(
        () -> WANTestBase.createSender("lnParallel1", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnParallel1", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(
        () -> WANTestBase.createSender("lnParallel1", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(
        () -> WANTestBase.createSender("lnParallel1", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(
        () -> WANTestBase.createSender("lnParallel2", 3, true, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnParallel2", 3, true, 100, 10, false, false, null, true));
    vm6.invoke(
        () -> WANTestBase.createSender("lnParallel2", 3, true, 100, 10, false, false, null, true));
    vm7.invoke(
        () -> WANTestBase.createSender("lnParallel2", 3, true, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnParallel1,lnParallel2", 1, 100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnParallel1,lnParallel2", 1, 100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnParallel1,lnParallel2", 1, 100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnParallel1,lnParallel2", 1, 100, isOffHeap()));

    startSenderInVMs("lnParallel1", vm4, vm5, vm6, vm7);

    startSenderInVMs("lnParallel2", vm4, vm5, vm6, vm7);


    // before doing puts, make sure that the senders are started.
    // this will ensure that not a single events is lost
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("lnParallel1"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("lnParallel1"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("lnParallel1"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("lnParallel1"));

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("lnParallel2"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("lnParallel2"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("lnParallel2"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("lnParallel2"));


    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel1"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel1"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel1"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel1"));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel2"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel2"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel2"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("lnParallel2"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testPartitionedParallelPropagationHA() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));


    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 2, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 2, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 2, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 2, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());

    AsyncInvocation<Void> inv1 =
        vm7.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 5000));
    deprecatedPause(500);
    AsyncInvocation<Void> inv2 = vm4.invokeAsync((SerializableRunnableIF) WANTestBase::killSender);
    AsyncInvocation<Void> inv3 =
        vm6.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 10000));
    deprecatedPause(1500);
    AsyncInvocation<Void> inv4 = vm5.invokeAsync((SerializableRunnableIF) WANTestBase::killSender);
    inv1.await();
    inv2.await();
    inv3.await();
    inv4.await();

    vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));
    vm7.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));

    // verify all buckets drained on the sender nodes that up and running.
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));
    vm4.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));
    vm5.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));
  }

  @Test
  public void testParallelPropagationWithFilter() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));

    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));

    // wait for senders to be running before doing any puts. This will ensure that
    // not a single events is lost
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName(), 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 800));
  }


  @Test
  public void testParallelPropagationWithPutAll() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(createPartitionedRegionRunnable("ln", 1));
    vm5.invoke(createPartitionedRegionRunnable("ln", 1));
    vm6.invoke(createPartitionedRegionRunnable("ln", 1));
    vm7.invoke(createPartitionedRegionRunnable("ln", 1));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.doPutAll(getTestMethodName() + "_PR", 100, 50));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 5000));

  }

  /**
   * There was a bug that all destroy events were being put into different buckets of sender queue
   * against the key 0. Bug# 44304
   *
   */
  @Test
  public void testParallelPropagationWithDestroy() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 100, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 100, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 100, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 100, false, false, null, true));

    vm4.invoke(createPartitionedRegionRunnable("ln", 1));
    vm5.invoke(createPartitionedRegionRunnable("ln", 1));
    vm6.invoke(createPartitionedRegionRunnable("ln", 1));
    vm7.invoke(createPartitionedRegionRunnable("ln", 1));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    deprecatedPause(2000);

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    vm4.invoke(() -> WANTestBase.doDestroys(getTestMethodName() + "_PR", 500));


    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueBucketSize("ln", 15));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueBucketSize("ln", 15));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueBucketSize("ln", 15));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueBucketSize("ln", 15));

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    // give some time for the queue to drain
    deprecatedPause(5000);

    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 500));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 500));

  }

  /**
   * Normal happy scenario test case. But with Tx operations
   *
   */
  @Test
  public void testParallelPropagationTxOperations() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5);
    // vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    // vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    // vm6.invoke(() -> WANTestBase.createSender( "ln", 2,
    // true, 100, 10, false, false, null, true ));
    // vm7.invoke(() -> WANTestBase.createSender( "ln", 2,
    // true, 100, 10, false, false, null, true ));

    vm4.invoke(createPartitionedRegionRunnable("ln", 1));
    vm5.invoke(createPartitionedRegionRunnable("ln", 1));
    // vm6.invoke(() -> WANTestBase.createPartitionedRegion(
    // testName + "_PR", "ln", true, 1, 100, isOffHeap() ));
    // vm7.invoke(() -> WANTestBase.createPartitionedRegion(
    // testName + "_PR", "ln", true, 1, 100, isOffHeap() ));

    startSenderInVMs("ln", vm4, vm5);
    // vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    // vm7.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    // vm6.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    // vm7.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm4.invoke(() -> WANTestBase.doTxPuts(getTestMethodName() + "_PR"));

    // verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    // vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    // vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 3));
  }

  @Ignore
  @Test
  public void testParallelGatewaySenderQueueLocalSize() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);
    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(createPartitionedRegionRunnable("ln", 1));
    vm5.invoke(createPartitionedRegionRunnable("ln", 1));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));

    /*
     * Remember pausing sender does not guarantee that peek will be paused immediately as its quite
     * possible event processor is already in peeking events and send them after peeking without a
     * check for deprecatedPause. hence below deprecatedPause of 1 sec to allow dispatching to be
     * paused
     */
    // vm4.invoke(() -> WANTestBase.waitForSenderPausedState( "ln" ));
    // vm5.invoke(() -> WANTestBase.waitForSenderPausedState( "ln" ));
    deprecatedPause(1000);

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 10));

    vm4.invoke(() -> WANTestBase.validateQueueContents("ln", 10));
    vm5.invoke(() -> WANTestBase.validateQueueContents("ln", 10));

    // instead of checking size as 5 and 5. check that combined size is 10
    Integer localSize1 = vm4.invoke(() -> WANTestBase.getPRQLocalSize("ln"));
    Integer localSize2 = vm5.invoke(() -> WANTestBase.getPRQLocalSize("ln"));
    assertEquals(10, localSize1 + localSize2);
  }

  /**
   * Added for defect #50364 Can't colocate region that has AEQ with a region that does not have
   * that same AEQ
   */
  @Test
  @Parameters(method = "getRegionShortcuts")
  public void testParallelSenderAttachedToChildRegionButNotToParentRegion(RegionShortcut shortcut) {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // create cache and receiver on site2
    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);
    // create cache on site1
    createCacheInVMs(lnPort, vm3);

    // create sender on site1
    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    // start sender on site1
    startSenderInVMs("ln", vm3);

    // create leader (parent) PR on site1
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PARENT_PR", null, 0,
        100, isOffHeap(), shortcut));
    String parentRegionFullPath =
        vm3.invoke(() -> WANTestBase.getRegionFullPath(getTestMethodName() + "PARENT_PR"));

    // create colocated (child) PR on site1
    vm3.invoke(() -> WANTestBase.createColocatedPartitionedRegion(getTestMethodName() + "CHILD_PR",
        "ln", 0, 100, parentRegionFullPath));

    // create leader and colocated PR on site2
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "PARENT_PR", null, 0,
        100, isOffHeap()));
    vm2.invoke(() -> WANTestBase.createColocatedPartitionedRegion(getTestMethodName() + "CHILD_PR",
        null, 0, 100, parentRegionFullPath));

    // do puts in colocated (child) PR on site1
    vm3.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "CHILD_PR", 1000));

    // verify the puts reach site2
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "CHILD_PR", 1000));
  }

  @Test
  public void testParallelPropagationWithFilter_AfterAck() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm6, vm7);
    createReceiverInVMs(vm6, vm7);

    createCacheInVMs(lnPort, vm2, vm3, vm4, vm5);

    vm2.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter_AfterAck(), true));
    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter_AfterAck(), true));
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter_AfterAck(), true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter_AfterAck(), true));

    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm2, vm3, vm4, vm5);

    vm6.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));

    // wait for senders to be running before doing any puts. This will ensure
    // that
    // not a single events is lost
    vm2.invoke(waitForSenderRunnable());
    vm3.invoke(waitForSenderRunnable());
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName(), 1000));

    vm2.invoke(() -> WANTestBase.validateQueueContents("ln", 0));
    vm3.invoke(() -> WANTestBase.validateQueueContents("ln", 0));
    vm4.invoke(() -> WANTestBase.validateQueueContents("ln", 0));
    vm5.invoke(() -> WANTestBase.validateQueueContents("ln", 0));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));
    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));

    vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));
    vm7.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 1000));

    Integer vm2Acks = vm2.invoke(() -> WANTestBase.validateAfterAck("ln"));
    Integer vm3Acks = vm3.invoke(() -> WANTestBase.validateAfterAck("ln"));
    Integer vm4Acks = vm4.invoke(() -> WANTestBase.validateAfterAck("ln"));
    Integer vm5Acks = vm5.invoke(() -> WANTestBase.validateAfterAck("ln"));

    assertEquals(2000, (vm2Acks + vm3Acks + vm4Acks + vm5Acks));

  }

  /**
   * Test that, when a parallel gateway sender is added to a partitioned region through attributes
   * mutator, transaction events are not sent to all region members but only to those who are
   * hosting the bucket for the event and thus, events are not stored in the bucketToTempQueueMap
   * member of the ParallelGatewaySenderQueue.
   * Redundancy = 1 in the partitioned region.
   *
   */
  @Test
  public void testParallelPropagationTxNotificationsNotSentToAllRegionMembersWhenAddingParallelGatewaySenderThroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(createPartitionedRegionRunnable(null, 1));
    vm5.invoke(createPartitionedRegionRunnable(null, 1));
    vm6.invoke(createPartitionedRegionRunnable(null, 1));
    vm7.invoke(createPartitionedRegionRunnable(null, 1));

    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());

    vm4.invoke(() -> addSenderThroughAttributesMutator(getTestMethodName() + "_PR", "ln"));
    vm5.invoke(() -> addSenderThroughAttributesMutator(getTestMethodName() + "_PR", "ln"));
    vm6.invoke(() -> addSenderThroughAttributesMutator(getTestMethodName() + "_PR", "ln"));
    vm7.invoke(() -> addSenderThroughAttributesMutator(getTestMethodName() + "_PR", "ln"));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.doTxPuts(getTestMethodName() + "_PR"));

    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 3));
    vm4.invoke(() -> WANTestBase.verifyQueueSize("ln", 3));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0));
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0));

    vm4.invoke(() -> WANTestBase.validateEmptyBucketToTempQueueMap("ln"));
    vm5.invoke(() -> WANTestBase.validateEmptyBucketToTempQueueMap("ln"));
    vm6.invoke(() -> WANTestBase.validateEmptyBucketToTempQueueMap("ln"));
    vm7.invoke(() -> WANTestBase.validateEmptyBucketToTempQueueMap("ln"));
  }

  /**
   * Test that, when a parallel gateway sender is added to a partitioned region through attributes
   * mutator, transaction events are not sent to all region members but only to those who are
   * hosting the bucket for the event and thus, events are not stored in the bucketToTempQueueMap
   * member of the ParallelGatewaySenderQueue.
   * No redundancy in the partitioned region.
   *
   */
  @Test
  public void testParallelPropagationTxNotificationsNotSentToAllRegionMembersWhenAddingParallelGatewaySenderThroughAttributesMutatorNoRedundancy() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(createPartitionedRegionRunnable(null, 0));
    vm5.invoke(createPartitionedRegionRunnable(null, 0));

    vm2.invoke(createReceiverPartitionedRegionRedundancy1());
    vm3.invoke(createReceiverPartitionedRegionRedundancy1());

    vm4.invoke(() -> addSenderThroughAttributesMutator(getTestMethodName() + "_PR", "ln"));
    vm5.invoke(() -> addSenderThroughAttributesMutator(getTestMethodName() + "_PR", "ln"));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());

    vm4.invoke(() -> WANTestBase.doTxPuts(getTestMethodName() + "_PR"));

    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 3));
    vm4.invoke(() -> WANTestBase.verifyQueueSize("ln", 3));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0));
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0));

    vm4.invoke(() -> WANTestBase.validateEmptyBucketToTempQueueMap("ln"));
    vm5.invoke(() -> WANTestBase.validateEmptyBucketToTempQueueMap("ln"));
  }

  @Test
  public void testPartitionedParallelPropagationWithGroupTransactionEventsAndMixOfEventsInAndNotInTransactions()
      throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> setNumDispatcherThreadsForTheRun(2));
    vm5.invoke(() -> setNumDispatcherThreadsForTheRun(2));
    vm6.invoke(() -> setNumDispatcherThreadsForTheRun(2));
    vm7.invoke(() -> setNumDispatcherThreadsForTheRun(2));

    vm4.invoke(
        () -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true, true));
    vm5.invoke(
        () -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true, true));
    vm6.invoke(
        () -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true, true));
    vm7.invoke(
        () -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true, true));

    vm4.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 2, 10,
            isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 2, 10,
            isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 2, 10,
            isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createCustomerOrderShipmentPartitionedRegion("ln", 2, 10,
            isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> createCustomerOrderShipmentPartitionedRegion(null, 1, 8, isOffHeap()));
    vm3.invoke(() -> createCustomerOrderShipmentPartitionedRegion(null, 1, 8, isOffHeap()));

    int customers = 4;

    int transactionsPerCustomer = 1000;
    final Map<Object, Object> keyValuesInTransactions = new HashMap<>();
    for (int custId = 0; custId < customers; custId++) {
      for (int i = 0; i < transactionsPerCustomer; i++) {
        CustId custIdObject = new CustId(custId);
        OrderId orderId = new OrderId(i, custIdObject);
        ShipmentId shipmentId1 = new ShipmentId(i, orderId);
        ShipmentId shipmentId2 = new ShipmentId(i + 1, orderId);
        ShipmentId shipmentId3 = new ShipmentId(i + 2, orderId);
        keyValuesInTransactions.put(orderId, new Order());
        keyValuesInTransactions.put(shipmentId1, new Shipment());
        keyValuesInTransactions.put(shipmentId2, new Shipment());
        keyValuesInTransactions.put(shipmentId3, new Shipment());
      }
    }

    int ordersPerCustomerNotInTransactions = 1000;

    final Map<Object, Object> keyValuesNotInTransactions = new HashMap<>();
    for (int custId = 0; custId < customers; custId++) {
      for (int i = 0; i < ordersPerCustomerNotInTransactions; i++) {
        CustId custIdObject = new CustId(custId);
        OrderId orderId = new OrderId(i + transactionsPerCustomer * customers, custIdObject);
        keyValuesNotInTransactions.put(orderId, new Order());
      }
    }

    // eventsPerTransaction is 1 (orders) + 3 (shipments)
    int eventsPerTransaction = 4;
    AsyncInvocation<Void> inv1 =
        vm7.invokeAsync(
            () -> WANTestBase.doOrderAndShipmentPutsInsideTransactions(keyValuesInTransactions,
                eventsPerTransaction));

    AsyncInvocation<Void> inv2 =
        vm6.invokeAsync(
            () -> WANTestBase.putGivenKeyValue(orderRegionName, keyValuesNotInTransactions));

    inv1.await();
    inv2.await();

    int entries =
        ordersPerCustomerNotInTransactions * customers + transactionsPerCustomer * customers;

    vm4.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));
    vm5.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));
    vm6.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));
    vm7.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));

    vm2.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));
    vm3.invoke(() -> WANTestBase.validateRegionSize(orderRegionName, entries));

    vm4.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));
    vm5.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));
    vm6.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));
    vm7.invoke(() -> WANTestBase.checkConflatedStats("ln", 0));

    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
  }

  private static RegionShortcut[] getRegionShortcuts() {
    return new RegionShortcut[] {RegionShortcut.PARTITION, RegionShortcut.PARTITION_PERSISTENT};
  }
}
