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

import static org.apache.geode.internal.Assert.fail;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ParallelWANStatsDistributedTest extends WANTestBase {

  private static final int NUM_PUTS = 100;
  private static final long serialVersionUID = 1L;

  private String testName;

  public ParallelWANStatsDistributedTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() {
    testName = getTestMethodName();
  }

  @Test
  public void testConnectionStatsAreCreated() {
    // 1. Create locators
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // 2. Create cache & receiver in vm2
    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    // 3. Create cache & sender in vm4
    createSenderInVm(lnPort, vm4);

    // 4. Create Region in vm4 (sender)
    createSenderPRInVM(1, vm4);

    // 5. Create region in vm2 (receiver)
    createReceiverPR(vm2, 1);

    // 6. Start sender in vm4
    startSenderInVMs("ln", vm4);

    vm4.invoke(() -> WANTestBase.checkConnectionStats("ln"));
  }

  @Test
  public void testQueueSizeInSecondaryBucketRegionQueuesWithMemberRestart() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    createSendersWithConflation(lnPort);

    createSenderPRs(1);

    startPausedSenders();

    createReceiverPR(vm2, 1);
    putKeyValues();

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));

      // queue size
      assertThat(v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0)).isEqualTo(NUM_PUTS);
      // eventsReceived
      assertThat(v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)).isEqualTo(
          NUM_PUTS * 2);
      // events queued
      assertThat(v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)).isEqualTo(
          NUM_PUTS * 2);
      // events distributed
      assertThat(v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)).isEqualTo(0);
      // secondary queue size
      assertThat(v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10)).isEqualTo(
          NUM_PUTS);

      System.out.println("Current secondary queue sizes:" + v4List.get(10) + ":" + v5List.get(10)
          + ":" + v6List.get(10) + ":" + v7List.get(10));
    });

    // stop vm7 to trigger rebalance and move some primary buckets
    vm7.invoke(WANTestBase::closeCache);
    await().untilAsserted(() -> {
      int v4secondarySize = vm4.invoke(() -> WANTestBase.getSecondaryQueueSizeInStats("ln"));
      int v5secondarySize = vm5.invoke(() -> WANTestBase.getSecondaryQueueSizeInStats("ln"));
      int v6secondarySize = vm6.invoke(() -> WANTestBase.getSecondaryQueueSizeInStats("ln"));
      assertThat(v4secondarySize + v5secondarySize + v6secondarySize).isEqualTo(NUM_PUTS);
      System.out
          .println("New secondary queue sizes:" + v4secondarySize + ":" + v5secondarySize + ":"
              + v6secondarySize);
    });

    vm7.invoke(() -> WANTestBase.createCache(lnPort));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(testName, "ln", 1, 10, isOffHeap()));
    startSenderInVMs("ln", vm7);
    vm7.invoke(() -> pauseSender("ln"));

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      // secondary queue size
      assertThat(v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10)).isEqualTo(
          NUM_PUTS);
      System.out.println("After restart vm7, secondary queue sizes:" + v4List.get(10) + ":"
          + v5List.get(10) + ":" + v6List.get(10) + ":" + v7List.get(10));
    });

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));
    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(0, NUM_PUTS, NUM_PUTS));

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln", 0));

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

      // events distributed:
      assertThat(v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)).isEqualTo(NUM_PUTS);
      // secondary queue size:
      assertThat(v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10)).isEqualTo(0);
    });
  }

  // TODO: add a test without redundancy for primary switch
  @Test
  public void testQueueSizeInSecondaryWithPrimarySwitch() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    createSendersWithConflation(lnPort);

    createSenderPRs(1);

    startPausedSenders();

    createReceiverPR(vm2, 1);

    putKeyValues();

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));

      // queue size:
      assertThat(v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0)).isEqualTo(NUM_PUTS);
      // events received:
      assertThat(v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)).isEqualTo(
          NUM_PUTS * 2);
      // events queued:
      assertThat(v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)).isEqualTo(
          NUM_PUTS * 2);
      // events distributed:
      assertThat(v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)).isEqualTo(0);
      // secondary queue size:
      assertThat(v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10)).isEqualTo(
          NUM_PUTS);
    });

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));
    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));
    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(0, NUM_PUTS, NUM_PUTS));

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln", 0));

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

      // events distributed:
      assertThat(v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)).isEqualTo(NUM_PUTS);
      // secondary queue size:
      assertThat(v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10)).isEqualTo(0);
    });
  }

  @Test
  public void testPartitionedRegionParallelPropagation_BeforeDispatch() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createSendersWithConflation(lnPort);

    createSenderPRs(0);

    startPausedSenders();

    createReceiverPR(vm2, 1);
    createReceiverPR(vm3, 1);

    putKeyValues();

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", NUM_PUTS));

      // queue size:
      assertThat(v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0)).isEqualTo(NUM_PUTS);
      // events received:
      assertThat(v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)).isEqualTo(NUM_PUTS);
      // events queued:
      assertThat(v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)).isEqualTo(NUM_PUTS);
      // events distributed
      assertThat(v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)).isEqualTo(0);
      // batches distributed:
      assertThat(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4)).isEqualTo(0);
      // batches redistributed
      assertThat(v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)).isEqualTo(0);
    });
  }

  @Test
  public void testPartitionedRegionParallelPropagation_AfterDispatch_NoRedundancy() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    createSenders(lnPort);

    createReceiverPR(vm2, 0);

    createSenderPRs(0);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.doPuts(testName, NUM_PUTS));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

      // queue size:
      assertThat(v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0)).isEqualTo(0);
      // eventsReceived:
      assertThat(v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)).isEqualTo(NUM_PUTS);
      // events queued:
      assertThat(v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)).isEqualTo(NUM_PUTS);
      // events distributed:
      assertThat(v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)).isEqualTo(NUM_PUTS);
      // batches distributed:
      assertThat(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4))
          .isGreaterThanOrEqualTo(10);
      // batches redistributed:
      assertThat(v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)).isEqualTo(0);
    });

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(10, NUM_PUTS, NUM_PUTS));
  }

  @Test
  public void testPartitionedRegionParallelPropagation_AfterDispatch_Redundancy_3() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    createSenders(lnPort);

    createReceiverPR(vm2, 0);

    createSenderPRs(3);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.doPuts(testName, NUM_PUTS));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

      // queue size:
      assertThat(v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0)).isEqualTo(0);
      // events received:
      assertThat(v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)).isEqualTo(400);
      // events queued:
      assertThat(v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)).isEqualTo(400);
      // events distributed
      assertThat(v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)).isEqualTo(NUM_PUTS);
      // batches distributed:
      assertThat(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4))
          .isGreaterThanOrEqualTo(10);
      // batches redistributed:
      assertThat(v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)).isEqualTo(0);
    });

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(10, NUM_PUTS, NUM_PUTS));
  }

  @Test
  public void testWANStatsTwoWanSites_Bug44331() {
    Integer lnPort = createFirstLocatorWithDSId(1);
    Integer nyPort = vm0.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer tkPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    createCacheInVMs(nyPort, vm2);
    createCacheInVMs(tkPort, vm3);
    createReceiverInVMs(vm2);
    createReceiverInVMs(vm3);

    vm4.invoke(() -> WANTestBase.createCache(lnPort));

    vm4.invoke(() -> WANTestBase.createSender("ln1", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createSender("ln2", 3, true, 100, 10, false, false, null, true));

    createReceiverPR(vm2, 0);
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(testName, null, 0, 10, isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(testName, "ln1,ln2", 0, 10, isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender("ln1"));

    vm4.invoke(() -> WANTestBase.startSender("ln2"));

    vm4.invoke(() -> WANTestBase.doPuts(testName, NUM_PUTS));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));
    vm3.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));

    await().untilAsserted(() -> {
      List<Integer> v4Sender1List = vm4.invoke(() -> WANTestBase.getSenderStats("ln1", 0));
      assertThat(v4Sender1List.get(0).intValue()).isEqualTo(0); // queue size
      assertThat(v4Sender1List.get(1).intValue()).isEqualTo(NUM_PUTS); // eventsReceived
      assertThat(v4Sender1List.get(2).intValue()).isEqualTo(NUM_PUTS); // events queued
      assertThat(v4Sender1List.get(3).intValue()).isEqualTo(NUM_PUTS); // events distributed
      assertThat(v4Sender1List.get(4)).isGreaterThanOrEqualTo(10); // batches distributed
      assertThat(v4Sender1List.get(5).intValue()).isEqualTo(0); // batches redistributed
    });

    await().untilAsserted(() -> {
      List<Integer> v4Sender2List = vm4.invoke(() -> WANTestBase.getSenderStats("ln2", 0));
      assertThat(v4Sender2List.get(0).intValue()).isEqualTo(0); // queue size
      assertThat(v4Sender2List.get(1).intValue()).isEqualTo(NUM_PUTS); // eventsReceived
      assertThat(v4Sender2List.get(2).intValue()).isEqualTo(NUM_PUTS); // events queued
      assertThat(v4Sender2List.get(3).intValue()).isEqualTo(NUM_PUTS); // events distributed
      assertThat(v4Sender2List.get(4)).isGreaterThanOrEqualTo(10); // batches distributed
      assertThat(v4Sender2List.get(5).intValue()).isEqualTo(0); // batches redistributed
    });

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(10, NUM_PUTS, NUM_PUTS));
    vm3.invoke(() -> WANTestBase.checkGatewayReceiverStats(10, NUM_PUTS, NUM_PUTS));
  }

  @Category({WanTest.class})
  @Test
  public void testParallelPropagationHA() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);

    createReceiverPR(vm2, 0);

    createReceiverInVMs(vm2);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    createSenderPRs(3);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    AsyncInvocation<Void> inv1 = vm5.invokeAsync(() -> WANTestBase.doPuts(testName, 1000));
    vm2.invoke(() -> await()
        .untilAsserted(() -> assertThat(getRegionSize(testName)).as(
            "Waiting for first batch to be received").isGreaterThan(10)));
    AsyncInvocation<Void> inv2 = vm4.invokeAsync(() -> WANTestBase.killSender());
    inv1.await();
    inv2.await();

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, 1000));

    await().untilAsserted(() -> {
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

      // queue size
      assertThat(v5List.get(0)).isZero();
      assertThat(v6List.get(0)).isZero();
      assertThat(v7List.get(0)).isZero();
      int receivedEvents = v5List.get(1) + v6List.get(1) + v7List.get(1);
      // We may see a single retried event on all members due to the kill
      assertThat(receivedEvents).isBetween(3000, 3003);
      int queuedEvents = v5List.get(2) + v6List.get(2) + v7List.get(2);
      assertThat(queuedEvents).isBetween(3000, 3003);
      // quite possible that vm4 has distributed some of the batches.
      // batches redistributed
      assertThat(v5List.get(5) + v6List.get(5) + v7List.get(5)).isEqualTo(0);
    });

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStatsHA(NUM_PUTS, 1000, 1000));
  }

  /**
   * 1 region and sender configured on local site and 1 region and a receiver configured on remote
   * site. Puts to the local region are in progress. Remote region is destroyed in the middle.
   */
  @Test
  public void testParallelPropagationWithRemoteRegionDestroy() {
    addIgnoredException("RegionDestroyedException");
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverPR(vm2, 0);
    createReceiverInVMs(vm2);

    createSenders(lnPort);

    vm2.invoke(() -> WANTestBase.addCacheListenerAndDestroyRegion(testName));

    createSenderPRs(0);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    // start puts in RR_1 in another thread
    vm4.invoke(() -> WANTestBase.doPuts(testName, 2000));

    // verify that all is well in local site. All the events should be present in local region
    vm4.invoke(() -> WANTestBase.validateRegionSize(testName, 2000));

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", -1));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", -1));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", -1));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", -1));

      // batches distributed: it's quite possible that vm4 has distributed some of the batches.
      assertThat(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4))
          .isGreaterThanOrEqualTo(1);
      // batches redistributed:
      assertThat(v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5))
          .isGreaterThanOrEqualTo(1);
    });
  }

  @Test
  public void testParallelPropagationWithFilter() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);

    createReceiverPR(vm2, 1);

    createReceiverInVMs(vm2);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    createSenderPRs(0);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new MyGatewayEventFilter(), true));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.doPuts(testName, 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, 800));

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

      // queue size:
      assertThat(v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0)).isEqualTo(0);
      // events received:
      assertThat(v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)).isEqualTo(1000);
      // events queued:
      assertThat(v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)).isEqualTo(900);
      // events distributed:
      assertThat(v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)).isEqualTo(800);
      // batches distributed:
      assertThat(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4))
          .isGreaterThanOrEqualTo(80);
      // batches redistributed:
      assertThat(v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)).isEqualTo(0);
      // events filtered:
      assertThat(v4List.get(6) + v5List.get(6) + v6List.get(6) + v7List.get(6)).isEqualTo(200);
    });

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(80, 800, 800));
  }

  @Test
  public void testParallelPropagationConflation() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    createSendersWithConflation(lnPort);

    createSenderPRs(0);

    startPausedSenders();

    createReceiverPR(vm2, 1);

    Map<Object, Object> keyValues = putKeyValues();

    // Verify the conflation indexes map is empty
    verifyConflationIndexesSize("ln", 0, vm4, vm5, vm6, vm7);

    final Map<Object, Object> updateKeyValues = new HashMap<>();
    for (int i = 0; i < 50; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm4.invoke(() -> WANTestBase.putGivenKeyValue(testName, updateKeyValues));

    // Verify the conflation indexes map equals the number of updates
    verifyConflationIndexesSize("ln", 50, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln",
        keyValues.size() + updateKeyValues.size() /* creates aren't conflated */));

    // Do the puts again. Since these are updates, the previous updates will be conflated.
    vm4.invoke(() -> WANTestBase.putGivenKeyValue(testName, updateKeyValues));

    // Verify the conflation indexes map still equals the number of updates
    verifyConflationIndexesSize("ln", 50, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln",
        keyValues.size() + updateKeyValues.size() /* creates aren't conflated */));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, 0));

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(0, 0, 0));

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));

    keyValues.putAll(updateKeyValues);
    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, keyValues.size()));

    vm2.invoke(() -> WANTestBase.validateRegionContents(testName, keyValues));

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(0, 150, NUM_PUTS));

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln", 0));

    await().untilAsserted(() -> {
      List<Integer> v4List = vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v5List = vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v6List = vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
      List<Integer> v7List = vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

      // Verify final stats
      // 0 -> eventQueueSize
      // 1 -> eventsReceived
      // 2 -> eventsQueued
      // 3 -> eventsDistributed
      // 4 -> batchesDistributed
      // 5 -> batchesRedistributed
      // 7 -> eventsNotQueuedConflated
      // 9 -> conflationIndexesMapSize
      assertThat(v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0)).isEqualTo(0);
      assertThat(v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)).isEqualTo(200);
      assertThat(v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)).isEqualTo(200);
      assertThat(v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)).isEqualTo(150);
      assertThat(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4)).isGreaterThan(10);
      assertThat(v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)).isEqualTo(0);
      assertThat(v4List.get(7) + v5List.get(7) + v6List.get(7) + v7List.get(7)).isEqualTo(50);
      assertThat(v4List.get(9) + v5List.get(9) + v6List.get(9) + v7List.get(9)).isEqualTo(0);
    });
  }

  @Test
  public void testConflationWithSameEntryPuts() {
    // Start locators
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm2.invoke(() -> createFirstRemoteLocator(2, lnPort));

    // Configure sending site member
    String senderId = "ny";
    String regionName = testName + "_PR";
    vm1.invoke(() -> createCache(lnPort));
    vm1.invoke(() -> createSender(senderId, 2, true, 100, 10, true, true, null, false));
    vm1.invoke(() -> createPartitionedRegion(regionName, senderId, 0, 10, isOffHeap()));

    // Do puts of the same key
    int numIterations = 100;
    vm1.invoke(() -> putSameEntry(regionName, numIterations));

    // Wait for appropriate queue size
    vm1.invoke(() -> checkQueueSize(senderId, 2));

    // Verify the conflation indexes size stat
    verifyConflationIndexesSize(senderId, 1, vm1);

    // Configure receiving site member
    vm3.invoke(() -> createCache(nyPort));
    vm3.invoke(WANTestBase::createReceiver);
    vm3.invoke(() -> createPartitionedRegion(regionName, null, 0, 10, isOffHeap()));

    // Wait for queue to drain
    vm1.invoke(() -> checkQueueSize(senderId, 0));

    // Verify the conflation indexes size stat
    verifyConflationIndexesSize(senderId, 0, vm1);
  }

  @Test
  public void testPartitionedRegionParallelPropagation_RestartSenders_NoRedundancy() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    createReceiverInVMs(vm2);

    createSenders(lnPort);

    createReceiverPR(vm2, 0);

    createSenderPRs(0);

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    // pause the senders
    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    vm4.invoke(() -> WANTestBase.doPuts(testName, NUM_PUTS));

    vm4.invoke(() -> WANTestBase.stopSender("ln"));
    vm5.invoke(() -> WANTestBase.stopSender("ln"));
    vm6.invoke(() -> WANTestBase.stopSender("ln"));
    vm7.invoke(() -> WANTestBase.stopSender("ln"));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.checkQueueSizeInStats("ln", 0));
    vm5.invoke(() -> WANTestBase.checkQueueSizeInStats("ln", 0));
    vm6.invoke(() -> WANTestBase.checkQueueSizeInStats("ln", 0));
    vm7.invoke(() -> WANTestBase.checkQueueSizeInStats("ln", 0));

    vm2.invoke(() -> WANTestBase.validateRegionSize(testName, NUM_PUTS));

  }

  @Test
  public void testMaximumTimeBetweenPingsInGatewayReceiverIsHonored() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> createServer(nyPort));

    // Set a maximum time between pings lower than the time between pings sent at the sender (5000)
    // so that the receiver will not receive the ping on time and will close the connection.
    int maximumTimeBetweenPingsInGatewayReceiver = 4000;
    createReceiverInVMs(maximumTimeBetweenPingsInGatewayReceiver, vm2);
    createReceiverPR(vm2, 0);

    int maximumTimeBetweenPingsInServer = 60000;
    vm4.invoke(() -> createServer(lnPort, maximumTimeBetweenPingsInServer));
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    createSenderPRInVM(0, vm4);
    String senderId = "ln";
    startSenderInVMs(senderId, vm4);

    // Send some puts to start the connections from the sender to the receiver
    vm4.invoke(() -> WANTestBase.doPuts(testName, 2));

    // Create client to check if connections are later closed.
    ClientCache clientCache = new ClientCacheFactory()
        .addPoolLocator("localhost", lnPort)
        .setPoolLoadConditioningInterval(-1)
        .setPoolIdleTimeout(-1)
        .setPoolPingInterval(5000)
        .create();
    Region<Long, String> clientRegion =
        clientCache.<Long, String>createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(testName);
    for (long i = 0; i < 2; i++) {
      clientRegion.put(i, "Value_" + i);
    }

    // Wait more than maximum-time-between-pings in the gateway receiver so that connections are
    // closed.
    try {
      Thread.sleep(maximumTimeBetweenPingsInGatewayReceiver + 2000);
    } catch (InterruptedException e) {
      fail("Interrupted");
    }

    assertThat((int) vm4.invoke(() -> getGatewaySenderPoolDisconnects(senderId))).isNotEqualTo(0);
    assertThat(((PoolImpl) clientCache.getDefaultPool()).getStats().getDisConnects()).isEqualTo(0);
  }

  protected Map<Object, Object> putKeyValues() {
    final Map<Object, Object> keyValues = new HashMap<>();
    for (int i = 0; i < NUM_PUTS; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(() -> WANTestBase.putGivenKeyValue(testName, keyValues));

    vm4.invoke(() -> WANTestBase.checkQueueSize("ln", keyValues.size()));

    return keyValues;
  }

  protected void createReceiverPR(VM vm, int redundancy) {
    vm.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, null, redundancy, 10, isOffHeap()));
  }

  protected void createSenderPRs(int redundancy) {
    vm4.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, "ln", redundancy, 10, isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, "ln", redundancy, 10, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, "ln", redundancy, 10, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, "ln", redundancy, 10, isOffHeap()));
  }

  protected void createSenderPRInVM(int redundancy, VM vm) {
    vm.invoke(
        () -> WANTestBase.createPartitionedRegion(testName, "ln", redundancy, 10, isOffHeap()));
  }

  protected void startPausedSenders() {
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> pauseSender("ln"));
    vm5.invoke(() -> pauseSender("ln"));
    vm6.invoke(() -> pauseSender("ln"));
    vm7.invoke(() -> pauseSender("ln"));
  }

  protected void createSenders(Integer lnPort) {
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
  }

  protected void createSendersWithConflation(Integer lnPort) {
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, true, false, null, true));
  }

  protected void createSenderInVm(Integer lnPort, VM vm) {
    createCacheInVMs(lnPort, vm);
    vm.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
  }

  private void verifyConflationIndexesSize(String senderId, int expectedSize, VM... vms) {
    await().untilAsserted(() -> {
      int actualSize = 0;
      for (VM vm : vms) {
        List<Integer> stats = vm.invoke(() -> WANTestBase.getSenderStats(senderId, -1));
        actualSize += stats.get(9);
      }
      assertThat(actualSize).isEqualTo(expectedSize);
    });
  }

  private void putSameEntry(String regionName, int numIterations) {
    // This does one create and numInterations-1 updates
    Region<Object, Object> region = cache.getRegion(regionName);
    for (int i = 0; i < numIterations; i++) {
      region.put(0, i);
    }
  }
}
