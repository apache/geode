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
package com.gemstone.gemfire.internal.cache.wan.serial;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;


@Category(DistributedTest.class)
public class SerialWANPropogationsFeatureDUnitTest extends WANTestBase{

  private static final long serialVersionUID = 1L;

  public SerialWANPropogationsFeatureDUnitTest() {
    super();
  }

  @Test
  public void testSerialReplicatedWanWithOverflow() {

    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 10, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 10, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap()  ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap()  ));

    startSenderInVMs("ln", vm4, vm5);
    vm2.invoke(() -> addListenerToSleepAfterCreateEvent(1000, getTestMethodName() + "_RR"));
    vm3.invoke(() -> addListenerToSleepAfterCreateEvent(1000, getTestMethodName() + "_RR"));

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));

    vm4.invoke(() -> WANTestBase.doHeavyPuts(
        getTestMethodName() + "_RR", 15 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 15, 240000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 15, 240000 ));
  }

  @Test
  public void testSerialReplicatedWanWithPersistence() {

    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, true, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, true, null, true ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap()  ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap()  ));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));

  }

  @Test
  public void testReplicatedSerialPropagationWithConflation() throws Exception {

    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 1000, true, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 1000, true, false, null, true ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap()  ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap()  ));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
  }
  
  @Test
  public void testReplicatedSerialPropagationWithParallelThreads()
      throws Exception {

    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap()  ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap()  ));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap()  ));

    vm4.invoke(() -> WANTestBase.doMultiThreadedPuts(
        getTestMethodName() + "_RR", 1000 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
  }
  
  @Test
  public void testSerialPropogationWithFilter() throws Exception {

    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2,lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false,
        new MyGatewayEventFilter(), true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false,
        new MyGatewayEventFilter(), true ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap()  ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap()  ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap()  ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap()  ));

    startSenderInVMs("ln", vm4, vm5);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap()  ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap()  ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 1000 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 800 ));
  }

  @Test
  public void testReplicatedSerialPropagationWithFilter() throws Exception {

    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false,
        new MyGatewayEventFilter(), true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false,
        new MyGatewayEventFilter(), true ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), null, isOffHeap()  ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), null, isOffHeap()  ));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), "ln", isOffHeap()  ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), "ln", isOffHeap()  ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), "ln", isOffHeap()  ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), "ln", isOffHeap()  ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 1000 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 800 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 800 ));
  }
  
  @Test
  public void testReplicatedSerialPropagationWithFilter_AfterAck()
      throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm6, vm7);
    createReceiverInVMs(vm6, vm7);

    createCacheInVMs(lnPort, vm2, vm3, vm4, vm5);
    vm4.invoke(() -> WANTestBase.createSender( "ln", 2, false, 100, 10, false, false,
            new MyGatewayEventFilter_AfterAck(), true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2, false, 100, 10, false, false,
            new MyGatewayEventFilter_AfterAck(), true ));

    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), null, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), null, isOffHeap() ));

    startSenderInVMs("ln", vm4, vm5);

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), "ln", isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), "ln", isOffHeap() ));
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName(), "ln", isOffHeap() ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 1000 ));

    vm4.invoke(() -> WANTestBase.validateQueueContents( "ln",
        0 ));
    vm5.invoke(() -> WANTestBase.validateQueueContents( "ln",
        0 ));

    Integer vm4Acks = (Integer)vm4.invoke(() -> WANTestBase.validateAfterAck( "ln"));
    Integer vm5Acks = (Integer)vm5.invoke(() -> WANTestBase.validateAfterAck( "ln"));

    assertEquals(2000, (vm4Acks + vm5Acks));

    vm6.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
    vm7.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
  }
}
