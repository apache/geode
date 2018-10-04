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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.UpdateOperation;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class PDXNewWanDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;
  public static final String KEY_0 = "Key_0";

  public PDXNewWanDUnitTest() {
    super();
  }

  /**
   * Test 1> Site 1 : 1 locator, 1 member 2> Site 2 : 1 locator, 1 member 3> DR is defined on member
   * 1 on site1 4> Serial GatewaySender is defined on member 1 on site1 5> Same DR is defined on
   * site2 member 1 6> Put is done with value which is PDXSerializable 7> Validate whether other
   * sites member receive this put operation.
   */
  @Test
  public void testWANPDX_RR_SerialSender() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_RR", 1));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 1));
  }

  /**
   * Test 1> Site 1 : 1 locator, 1 member 2> Site 2 : 1 locator, 1 member 3> DR is defined on member
   * 1 on site1 4> Serial GatewaySender is defined on member 1 on site1 5> Same DR is defined on
   * site2 member 1 6> Put is done with value which is PDXSerializable 7> Validate whether other
   * sites member receive this put operation. 8> Bounce site 1 and delete all of it's data 9> Make
   * sure that site 1 get the the PDX types along with entries and can deserialize entries.
   */
  @Test
  public void testWANPDX_RemoveRemoteData() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX(nyPort));

    vm3.invoke(() -> WANTestBase.createCache_PDX(lnPort));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_RR", 1));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 1));


    // bounce vm2
    vm2.invoke(() -> WANTestBase.closeCache());

    vm2.invoke(() -> WANTestBase.deletePDXDir());

    vm2.invoke(() -> WANTestBase.createReceiver_PDX(nyPort));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_RR", 2));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 2));
  }


  @Test
  public void testWANPDX_CacheWriterCheck() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> setSystemProperty("gemfire.disk.recoverValues", "false"));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX(nyPort));

    vm3.invoke(() -> WANTestBase.createCache_PDX(lnPort));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_RR", 1));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 1));

    // Close VM2 cache
    vm2.invoke(() -> WANTestBase.closeCache());

    // do some puts on VM3 and create extra pdx id
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable2(getTestMethodName() + "_RR", 2));

    // start cache in vm2 again, now it should receive pdx id from vm3
    vm2.invoke(() -> WANTestBase.createReceiver_PDX(nyPort));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));


    try {
      Wait.pause(10000);
      // Define a different type from vm3
      vm3.invoke(() -> WANTestBase.doPutsPDXSerializable2(getTestMethodName() + "_RR", 2));

      // Give the updates some time to make it over the WAN
      Wait.pause(10000);

      vm2.invoke(() -> WANTestBase.validateRegionSizeOnly_PDX(getTestMethodName() + "_RR", 2));

      vm3.invoke(() -> WANTestBase.closeCache());

      vm2.invoke(() -> WANTestBase.closeCache());
    } finally {
      vm2.invoke(() -> setSystemProperty("gemfire.disk.recoverValues", "true"));
    }
  }

  private void setSystemProperty(String key, String value) {
    System.setProperty(key, value);
  }

  /**
   * Test 1> Site 1 : 1 locator, 1 member 2> Site 2 : 1 locator, 1 member 3> DR is defined on member
   * 1 on site1 4> Serial GatewaySender is defined on member 1 on site1 5> Same DR is defined on
   * site2 member 1 6> Put is done with value which is PDXSerializable 7> Validate whether other
   * sites member receive this put operation. 8> Bounce site 1 and delete all of it's data 9> Make
   * some conflicting PDX registries in site 1 before the reconnect 10> Make sure we flag a warning
   * about the conflicting updates.
   */
  @Test
  public void testWANPDX_ConflictingData() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX(nyPort));

    vm3.invoke(() -> WANTestBase.createCache_PDX(lnPort));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_RR", 1));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 1));

    // bounce vm3
    vm3.invoke(() -> WANTestBase.closeCache());

    IgnoredException ex1 =
        IgnoredException.addIgnoredException("Trying to add a PDXType with the same id");
    IgnoredException ex2 = IgnoredException.addIgnoredException("CacheWriterException");
    IgnoredException ex3 = IgnoredException.addIgnoredException("does match the existing PDX type");
    IgnoredException ex4 = IgnoredException.addIgnoredException("ServerOperationException");
    IgnoredException ex5 = IgnoredException.addIgnoredException("Stopping the processor");

    try {
      // blow away vm3's PDX data
      vm3.invoke(() -> WANTestBase.deletePDXDir());

      vm3.invoke(() -> WANTestBase.createCache_PDX(lnPort));

      vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

      vm3.invoke(() -> WANTestBase.startSender("ln"));

      vm3.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

      // Define a different type from vm3
      vm3.invoke(() -> WANTestBase.doPutsPDXSerializable2(getTestMethodName() + "_RR", 2));

      // Give the updates some time to make it over the WAN
      Wait.pause(10000);

      vm2.invoke(() -> WANTestBase.validateRegionSizeOnly_PDX(getTestMethodName() + "_RR", 2));

      vm3.invoke(() -> WANTestBase.closeCache());
    } finally {
      ex1.remove();
      ex2.remove();
      ex3.remove();
      ex4.remove();
      ex5.remove();
    }
  }

  /**
   * Test 1> Site 1 : 1 locator, 1 member 2> Site 2 : 1 locator, 1 member 3> Site 3 : 1 locator, 1
   * member 3> DR is defined on member 1 on site1 4> Serial GatewaySender is defined on member 1 on
   * site1 5> Same DR is defined on site2 member 1 6> Put is done with value which is
   * PDXSerializable 7> Validate whether other sites member receive this put operation.
   */
  @Test
  public void testWANPDX_RR_SerialSender3Sites() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    Integer tkPort = (Integer) vm2.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

    createCacheInVMs(lnPort, vm3);
    createCacheInVMs(nyPort, vm4);
    createCacheInVMs(tkPort, vm5);
    vm3.invoke(() -> WANTestBase.createReceiver());
    vm4.invoke(() -> WANTestBase.createReceiver());
    vm5.invoke(() -> WANTestBase.createReceiver());


    // Create all of our gateway senders
    vm3.invoke(() -> WANTestBase.createSender("ny", 2, false, 100, 10, false, false, null, true));
    vm3.invoke(() -> WANTestBase.createSender("tk", 3, false, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.createSender("ln", 1, false, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.createSender("tk", 3, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 1, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ny", 2, false, 100, 10, false, false, null, true));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ny,tk",
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln,tk",
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln,ny",
        isOffHeap()));

    // Start all of the senders
    vm3.invoke(() -> WANTestBase.startSender("ny"));
    vm3.invoke(() -> WANTestBase.startSender("tk"));
    vm4.invoke(() -> WANTestBase.startSender("ln"));
    vm4.invoke(() -> WANTestBase.startSender("tk"));
    vm5.invoke(() -> WANTestBase.startSender("ln"));
    vm5.invoke(() -> WANTestBase.startSender("ny"));

    // Pause ln to ny. This means the PDX type will not be dispatched
    // to ny from ln
    vm3.invoke(() -> WANTestBase.pauseSender("ny"));

    Wait.pause(5000);

    // Do some puts that define a PDX type in ln
    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_RR", 1));

    // Make sure that tk received the update
    vm5.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 1));

    // Make ny didn't receive the update because the sender is paused
    vm4.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 0));

    // Now, do a put from tk. This serialized object will be distributed
    // to ny from tk, using the type defined by ln.
    vm5.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_RR", 2));

    // Verify the ny can read the object
    vm4.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 2));

    // Wait for vm3 to receive the update (prevents a broken pipe suspect string)
    vm3.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 2));
  }

  @Test
  public void testWANPDX_RR_SerialSender_StartedLater() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_RR", 10));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_RR", 40));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 40));
  }

  /**
   * Test 1> Site 1 : 1 locator, 1 member 2> Site 2 : 1 locator, 1 member 3> PR is defined on member
   * 1 on site1 4> Serial GatewaySender is defined on member 1 on site1 5> Same PR is defined on
   * site2 member 1 6> Put is done with value which is PDXSerializable 7> Validate whether other
   * sites member receive this put operation.
   */

  @Test
  public void testWANPDX_PR_SerialSender() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 2,
        isOffHeap()));
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));


    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 1));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 1));
  }

  @Test
  public void testWANPDX_PR_SerialSender_StartedLater() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX(nyPort));

    vm3.invoke(() -> WANTestBase.createCache_PDX(lnPort));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 20));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 40));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 40));
  }

  /**
   * Test 1> Site 1 : 1 locator, 2 member 2> Site 2 : 1 locator, 2 member 3> PR is defined on member
   * 1, 2 on site1 4> Serial GatewaySender is defined on member 1 on site1 5> Same PR is defined on
   * site2 member 1, 2 6> Put is done with value which is PDXSerializable 7> Validate whether other
   * sites member receive this put operation.
   */

  @Test
  public void testWANPDX_PR_MultipleVM_SerialSender() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 5,
        isOffHeap()));
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3, vm4);

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 5,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 5,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 10));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 10));
  }

  @Test
  public void testWANPDX_PR_MultipleVM_SerialSender_StartedLater() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX(nyPort));

    vm3.invoke(() -> WANTestBase.createCache_PDX(lnPort));
    vm4.invoke(() -> WANTestBase.createCache_PDX(lnPort));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 5,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 5,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 5,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 10));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm4.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 40));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 40));
  }

  /**
   * Test 1> Site 1 : 1 locator, 1 member 2> Site 2 : 1 locator, 1 member 3> PR is defined on member
   * 1 on site1 4> Parallel GatewaySender is defined on member 1 on site1 5> Same PR is defined on
   * site2 member 1 6> Put is done with value which is PDXSerializable 7> Validate whether other
   * sites member receive this put operation.
   */

  @Test
  public void testWANPDX_PR_ParallelSender() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 1,
        isOffHeap()));
    vm2.invoke(() -> WANTestBase.createReceiver());

    vm3.invoke(() -> WANTestBase.createCache(lnPort));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 1,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 1));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 1));
  }

  @Test
  public void testWANPDX_PR_ParallelSender_WithDelayedTypeRegistry()
      throws InterruptedException, ExecutionException {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // Create the receiver side of the WAN gateway. Only vm2 will be a receiver, vm3 is
    // just a peer
    createCacheInVMs(nyPort, vm2, vm3);
    vm2.invoke(() -> WANTestBase.createReceiver());
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 4,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 4,
        isOffHeap()));

    AsyncInvocation deserializationFuture;
    try {
      // Delay processing of sending type registry update from vm2
      vm2.invoke(() -> {
        DistributionMessageObserver.setInstance(new BlockingPdxTypeUpdateObserver());
      });

      // Create the sender side of the WAN connection. 2 VMs, with paused senders
      vm4.invoke(() -> WANTestBase.createCache(lnPort));
      vm5.invoke(() -> WANTestBase.createCache(lnPort));

      vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));
      vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, false));

      // Create the partitioned region in vm4
      vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 4,
          isOffHeap()));

      vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 4,
          isOffHeap()));

      vm5.invoke(() -> {
        Region region = cache.getRegion(getTestMethodName() + "_PR");
        PartitionRegionHelper.assignBucketsToPartitions(region);
      });

      vm4.invoke(() -> WANTestBase.pauseSender("ln"));
      vm5.invoke(() -> WANTestBase.pauseSender("ln"));

      // Do some puts to fill up our queues
      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 20));

      vm4.invoke(() -> {
        final Region r = cache.getRegion(Region.SEPARATOR + getTestMethodName() + "_PR");
        PdxValue result = (PdxValue) r.put(KEY_0, new PdxValue(0));
      });

      // Force VM4 to be the primary
      vm4.invoke(() -> {
        final Region region = cache.getRegion(Region.SEPARATOR + getTestMethodName() + "_PR");
        DistributedMember primary = PartitionRegionHelper.getPrimaryMemberForKey(region, KEY_0);
        // If we are not the primary
        DistributedMember localMember = cache.getDistributedSystem().getDistributedMember();
        if (!primary.equals(localMember)) {
          PartitionRegionHelper.moveBucketByKey(region, primary, localMember, KEY_0);

        }
      });

      vm5.invoke(() -> WANTestBase.resumeSender("ln"));

      boolean blocking = vm2.invoke(() -> {
        BlockingPdxTypeUpdateObserver observer =
            (BlockingPdxTypeUpdateObserver) DistributionMessageObserver.getInstance();
        return observer.startedBlocking.await(1, TimeUnit.MINUTES);
      });

      assertTrue(blocking);

      vm4.invoke(() -> WANTestBase.resumeSender("ln"));

      vm2.invoke(() -> {
        final Region region = cache.getRegion(Region.SEPARATOR + getTestMethodName() + "_PR");
        await().until(() -> region.containsKey(KEY_0));

      });

      // Make sure vm3 can deserialize the value
      deserializationFuture = vm3.invokeAsync(() -> {
        final Region r = cache.getRegion(Region.SEPARATOR + getTestMethodName() + "_PR");
        PdxValue result = (PdxValue) r.get(KEY_0);
        assertEquals(result, new PdxValue(0));
      });

      try {
        deserializationFuture.await(10, TimeUnit.SECONDS);
        fail("Get should have been blocked waiting for PDX type to be distributed");
      } catch (TimeoutException e) {
        // This is what we hope will happen. The get will be blocked by some sort of lock, rather
        // than failing due to a missing type.
      }

    } finally {

      vm2.invoke(() -> {
        BlockingPdxTypeUpdateObserver observer =
            (BlockingPdxTypeUpdateObserver) DistributionMessageObserver.getInstance();
        observer.latch.countDown();
      });
    }

    deserializationFuture.get();
  }

  @Test
  public void testWANPDX_PR_ParallelSender_47826() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 1,
        isOffHeap()));
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 1,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 1));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 1));
  }

  @Test
  public void testWANPDX_PR_ParallelSender_StartedLater() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX(nyPort));

    vm3.invoke(() -> WANTestBase.createCache_PDX(lnPort));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 10));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 40));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 40));
  }


  @Test
  public void testWANPDX_PR_MultipleVM_ParallelSender() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3, vm4);

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

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
  public void testWANPDX_PR_MultipleVM_ParallelSender_StartedLater() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createReceiver_PDX(nyPort));

    vm3.invoke(() -> WANTestBase.createCache_PDX(lnPort));
    vm4.invoke(() -> WANTestBase.createCache_PDX(lnPort));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

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


  @Test
  public void testWANPDX_RR_SerialSenderWithFilter() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false,
        new PDXGatewayEventFilter(), true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_RR", 1));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_RR", 1));

    vm3.invoke(() -> PDXNewWanDUnitTest.verifyFilterInvocation(1));
  }


  @Test
  public void testWANPDX_PR_MultipleVM_ParallelSenderWithFilter() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3, vm4);

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new PDXGatewayEventFilter(), true));
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false,
        new PDXGatewayEventFilter(), true));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 2,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 2,
        isOffHeap()));

    startSenderInVMs("ln", vm3, vm4);

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 10));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 10));

    vm3.invoke(() -> PDXNewWanDUnitTest.verifyFilterInvocation(5));
    vm4.invoke(() -> PDXNewWanDUnitTest.verifyFilterInvocation(5));
  }


  /**
   * When remote site bounces then we should send pdx event again.
   */
  @Ignore
  @Test
  public void testWANPDX_PR_SerialSender_RemoteSite_Bounce() {
    Integer lnPort = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm3);

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 1));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 1));

    vm2.invoke(() -> WANTestBase.killSender());

    createReceiverInVMs(vm2, vm4);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 2,
        isOffHeap()));
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 2,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.doPutsPDXSerializable(getTestMethodName() + "_PR", 1));

    vm2.invoke(() -> WANTestBase.validateRegionSize_PDX(getTestMethodName() + "_PR", 1));
  }



  public static void verifyFilterInvocation(int invocation) {
    await().untilAsserted(
        () -> assertEquals(((PDXGatewayEventFilter) eventFilter).beforeEnqueueInvoked, invocation));
    await()
        .untilAsserted(
            () -> assertEquals(((PDXGatewayEventFilter) eventFilter).beforeTransmitInvoked,
                invocation));
    await().untilAsserted(
        () -> assertEquals(((PDXGatewayEventFilter) eventFilter).afterAckInvoked, invocation));
  }


  private static class BlockingPdxTypeUpdateObserver extends DistributionMessageObserver {
    private CountDownLatch latch = new CountDownLatch(1);
    private CountDownLatch startedBlocking = new CountDownLatch(1);

    @Override
    public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof UpdateOperation.UpdateMessage
          && ((UpdateOperation.UpdateMessage) message).getRegionPath()
              .contains(PeerTypeRegistration.REGION_FULL_PATH)) {
        startedBlocking.countDown();
        try {
          latch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted", e);
        }

      }
    }
  }

  public static class PdxValue implements PdxSerializable {
    public int value;

    public PdxValue() {

    }

    public PdxValue(int value) {
      this.value = value;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("value", value);

    }

    @Override
    public void fromData(PdxReader reader) {
      value = reader.readInt("value");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      PdxValue pdxValue = (PdxValue) o;

      return value == pdxValue.value;
    }

    @Override
    public int hashCode() {
      return value;
    }
  }


}
