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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.WanTest;

/**
 * All the test cases are similar to SerialWANPropagationDUnitTest except that the we create
 * concurrent serial GatewaySender with concurrency of 4
 */
@Category({WanTest.class})
public class ConcurrentWANPropagation_2_DUnitTest extends WANTestBase {

  public ConcurrentWANPropagation_2_DUnitTest() {
    super();
  }

  private static final long serialVersionUID = 1L;

  @Test
  public void testSerialReplicatedWanWithOverflow() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // keep the maxQueueMemory low enough to trigger eviction
    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 10, 5, false, false, null,
        true, 5, OrderPolicy.KEY));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 10, 5, false, false, null,
        true, 5, OrderPolicy.KEY));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

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
  }

  @Ignore("Bug46921")
  @Test
  public void testSerialReplicatedWanWithPersistence() {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, true, null,
        true, 5, OrderPolicy.THREAD));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, true, null,
        true, 5, OrderPolicy.THREAD));

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

  }

  @Test
  public void testReplicatedSerialPropagationToTwoWanSites() throws Exception {

    Integer lnPort = createFirstLocatorWithDSId(1);
    Integer nyPort = vm0.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer tkPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    createCacheInVMs(nyPort, vm2);
    createCacheInVMs(tkPort, vm3);
    vm2.invoke(() -> WANTestBase.createReceiver());
    vm3.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("lnSerial1", 2, false, 100, 10, false,
        false, null, true, 5, OrderPolicy.THREAD));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("lnSerial1", 2, false, 100, 10, false,
        false, null, true, 5, OrderPolicy.THREAD));

    vm4.invoke(() -> WANTestBase.createConcurrentSender("lnSerial2", 3, false, 100, 10, false,
        false, null, true, 5, OrderPolicy.THREAD));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("lnSerial2", 3, false, 100, 10, false,
        false, null, true, 5, OrderPolicy.THREAD));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    startSenderInVMs("lnSerial1", vm4, vm5);

    startSenderInVMs("lnSerial2", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR",
        "lnSerial1,lnSerial2", isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR",
        "lnSerial1,lnSerial2", isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR",
        "lnSerial1,lnSerial2", isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR",
        "lnSerial1,lnSerial2", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
  }

  @Test
  public void testReplicatedSerialPropagationHA() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, false, null,
        true, 5, OrderPolicy.THREAD));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, false, null,
        true, 5, OrderPolicy.THREAD));

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

    AsyncInvocation inv1 =
        vm5.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 10000));
    Wait.pause(2000);
    AsyncInvocation inv2 = vm4.invokeAsync(() -> WANTestBase.killSender());

    inv1.join();
    inv2.join();

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 10000));
  }

  @Test
  public void testReplicatedSerialPropagationWithConflation() throws Exception {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 1000, true, false,
        null, true, 5, OrderPolicy.THREAD));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 1000, true, false,
        null, true, 5, OrderPolicy.THREAD));

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
  }

  @Test
  public void testReplicatedSerialPropagationWithParallelThreads() throws Exception {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, false, null,
        true, 4, OrderPolicy.THREAD));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, false, null,
        true, 4, OrderPolicy.THREAD));

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
  }

  @Test
  public void testSerialPropagationWithFilter() throws Exception {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, false,
        new MyGatewayEventFilter(), true, 5, OrderPolicy.THREAD));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, false,
        new MyGatewayEventFilter(), true, 5, OrderPolicy.THREAD));

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
  }

  @Test
  public void testReplicatedSerialPropagationWithFilter() throws Exception {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), null, isOffHeap()));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), null, isOffHeap()));
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, false,
        new MyGatewayEventFilter(), true, 5, OrderPolicy.THREAD));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, false,
        new MyGatewayEventFilter(), true, 5, OrderPolicy.THREAD));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getUniqueName(), "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName(), 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName(), 800));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName(), 800));
  }

  @Test
  public void testNormalRegionSerialPropagation() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm2.invoke(() -> WANTestBase.createReceiver());

    WANTestBase.createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, false, null,
        true, 5, OrderPolicy.THREAD));
    vm5.invoke(() -> WANTestBase.createConcurrentSender("ln", 2, false, 100, 10, false, false, null,
        true, 5, OrderPolicy.THREAD));


    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createNormalRegion(getUniqueName() + "_RR", "ln"));
    vm5.invoke(() -> WANTestBase.createNormalRegion(getUniqueName() + "_RR", "ln"));

    vm5.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 1000));

    vm4.invoke(() -> WANTestBase.checkQueueStats("ln", 0, 0, 0, 0));

    vm5.invoke(() -> WANTestBase.checkQueueStats("ln", 0, 1000, 0, 0));

    vm5.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));

    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 0));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 0));

    vm2.invoke(() -> WANTestBase.checkGatewayReceiverStats(0, 0, 0));

  }

}
