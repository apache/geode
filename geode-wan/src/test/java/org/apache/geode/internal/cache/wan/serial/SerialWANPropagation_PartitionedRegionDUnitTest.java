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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.junit.experimental.categories.Category;

import org.apache.geode.CancelException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class SerialWANPropagation_PartitionedRegionDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public SerialWANPropagation_PartitionedRegionDUnitTest() {
    super();
  }

  @Test
  public void testPartitionedSerialPropagation() throws Exception {

    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender("ln"));
    // vm5.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testBothReplicatedAndPartitionedSerialPropagation() throws Exception {

    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    vm5.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testSerialReplicatedAndPartitionedPropagation() throws Exception {

    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(
        () -> WANTestBase.createSender("lnSerial", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnSerial", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial",
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial", 1,
        100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial", 1,
        100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial", 1,
        100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial", 1,
        100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender("lnSerial"));
    vm5.invoke(() -> WANTestBase.startSender("lnSerial"));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    vm5.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testSerialReplicatedAndSerialPartitionedPropagation() throws Exception {

    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(
        () -> WANTestBase.createSender("lnSerial1", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnSerial1", 2, false, 100, 10, false, false, null, true));

    vm5.invoke(
        () -> WANTestBase.createSender("lnSerial2", 2, false, 100, 10, false, false, null, true));
    vm6.invoke(
        () -> WANTestBase.createSender("lnSerial2", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial1",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial1",
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial1",
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "lnSerial1",
        isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial2",
        1, 100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial2",
        1, 100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial2",
        1, 100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "lnSerial2",
        1, 100, isOffHeap()));

    startSenderInVMs("lnSerial1", vm4, vm5);

    startSenderInVMs("lnSerial2", vm5, vm6);

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
    vm5.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testPartitionedSerialPropagationToTwoWanSites() throws Exception {

    Integer lnPort = createFirstLocatorWithDSId(1);
    Integer nyPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer tkPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());
    createCacheInVMs(tkPort, vm3);
    vm3.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(
        () -> WANTestBase.createSender("lnSerial1", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnSerial1", 2, false, 100, 10, false, false, null, true));

    vm4.invoke(
        () -> WANTestBase.createSender("lnSerial2", 3, false, 100, 10, false, false, null, true));
    vm5.invoke(
        () -> WANTestBase.createSender("lnSerial2", 3, false, 100, 10, false, false, null, true));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    startSenderInVMs("lnSerial1", vm4, vm5);
    startSenderInVMs("lnSerial2", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnSerial1,lnSerial2", 1, 100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnSerial1,lnSerial2", 1, 100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnSerial1,lnSerial2", 1, 100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR",
        "lnSerial1,lnSerial2", 1, 100, isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testPartitionedSerialPropagationHA() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");

    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    // do initial 100 puts to create all the buckets
    vm5.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 100));

    IgnoredException.addIgnoredException(CancelException.class.getName());
    IgnoredException.addIgnoredException(CacheClosedException.class.getName());
    IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    // start async puts
    AsyncInvocation inv =
        vm5.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    // close the cache on vm4 in between the puts
    vm4.invoke(() -> WANTestBase.killSender());

    inv.join();
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testPartitionedSerialPropagationWithParallelThreads() throws Exception {

    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

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

    createReceiverInVMs(vm2, vm3);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    startSenderInVMs("ln", vm4, vm5);



    vm4.invoke(() -> WANTestBase.doMultiThreadedPuts( // TODO: eats exceptions
        getTestMethodName() + "_PR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }
}
