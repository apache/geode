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

import static org.junit.Assert.fail;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ReplicatedRegion_ParallelWANPersistenceDUnitTest extends WANTestBase {

  public ReplicatedRegion_ParallelWANPersistenceDUnitTest() {
    super();
    // TODO Auto-generated constructor stub
  }

  final String expectedExceptions = null;

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_PGSPERSISTENCE_VALIDATEQUEUE_Restart_Validate_Receiver() {
    // create locator on local site
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    // create locator on remote site
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    // create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // create senders with disk store
    String diskStore1 = vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore2 = vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore3 = vm6.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore4 = vm7.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));

    LogWriterUtils.getLogWriter()
        .info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    // start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 3000));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");

    // --------------------close and rebuild local site
    // -------------------------------------------------
    // kill the senders
    /*
     * ExpectedException exp1 = addExpectedException(CacheClosedException.class .getName()); try {
     */ vm4.invoke(() -> WANTestBase.killSender());
    vm5.invoke(() -> WANTestBase.killSender());
    vm6.invoke(() -> WANTestBase.killSender());
    vm7.invoke(() -> WANTestBase.killSender());
    /*
     * } finally { exp1.remove(); }
     */
    LogWriterUtils.getLogWriter().info("Killed all the senders.");

    // restart the vm
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm6.invoke(() -> WANTestBase.createCache(lnPort));
    vm7.invoke(() -> WANTestBase.createCache(lnPort));

    LogWriterUtils.getLogWriter().info("Created back the cache");

    // create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore1, true));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore2, true));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore3, true));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore4, true));

    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");

    // create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    AsyncInvocation inv2 = vm5.invokeAsync(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    AsyncInvocation inv3 = vm6.invokeAsync(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    AsyncInvocation inv4 = vm7.invokeAsync(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    // start the senders in async mode. This will ensure that the
    // node of shadow PR that went down last will come up first
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    // wait for senders running
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    LogWriterUtils.getLogWriter().info("All the senders are now running...");

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));

    // ----------------------------------------------------------------------------------------------------

    vm4.invoke(() -> WANTestBase.doNextPuts(getTestMethodName() + "_RR", 3000, 10000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 10000));

  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DRPERSISTENCE_PGSPERSISTENCE_VALIDATEQUEUE_Restart_Validate_Receiver() {
    // create locator on local site
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    // create locator on remote site
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    createReceiverInVMs(vm2, vm3);

    // create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // create senders with disk store
    String diskStore1 = vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore2 = vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore3 = vm6.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore4 = vm7.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));

    LogWriterUtils.getLogWriter()
        .info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    // start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 3000));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");

    // --------------------close and rebuild local site
    // -------------------------------------------------
    // kill the senders
    /*
     * ExpectedException exp1 = addExpectedException(CacheClosedException.class .getName()); try {
     */
    vm4.invoke(() -> WANTestBase.killSender());
    vm5.invoke(() -> WANTestBase.killSender());
    vm6.invoke(() -> WANTestBase.killSender());
    vm7.invoke(() -> WANTestBase.killSender());
    /*
     * } finally { exp1.remove(); }
     */

    // restart the vm
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm6.invoke(() -> WANTestBase.createCache(lnPort));
    vm7.invoke(() -> WANTestBase.createCache(lnPort));

    LogWriterUtils.getLogWriter().info("Created back the cache");

    // create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore1, true));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore2, true));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore3, true));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore4, true));

    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");

    // create PR on local site
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    AsyncInvocation inv2 =
        vm5.invokeAsync(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    AsyncInvocation inv3 =
        vm6.invokeAsync(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    AsyncInvocation inv4 =
        vm7.invokeAsync(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));

    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    // start the senders in async mode. This will ensure that the
    // node of shadow PR that went down last will come up first
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    // wait for senders running
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    LogWriterUtils.getLogWriter().info("All the senders are now running...");

    // ----------------------------------------------------------------------------------------------------

    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));
    vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));
    vm7.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));

    /*
     * exp1 = addExpectedException(CacheClosedException.class.getName()); try {
     */ vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));

    vm4.invoke(() -> WANTestBase.doNextPuts(getTestMethodName() + "_RR", 3000, 10000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 10000));
    /*
     * } finally { exp1.remove(); }
     */ }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DRPERSISTENCE_PRPERSISTENCE_PGSPERSISTENCE_VALIDATEQUEUE_Restart_Validate_Receiver() {
    // create locator on local site
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    // create locator on remote site
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // create receiver on remote site
    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm3.invoke(() -> WANTestBase.createCache(nyPort));
    vm2.invoke(() -> WANTestBase.createReceiver());
    vm3.invoke(() -> WANTestBase.createReceiver());

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        isOffHeap()));

    // create cache in local site
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm6.invoke(() -> WANTestBase.createCache(lnPort));
    vm7.invoke(() -> WANTestBase.createCache(lnPort));

    // create senders with disk store
    String diskStore1 = vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore2 = vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore3 = vm6.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore4 = vm7.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));

    LogWriterUtils.getLogWriter()
        .info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));

    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        "ln", 1, 100, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        "ln", 1, 100, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        "ln", 1, 100, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion(getTestMethodName() + "_PR",
        "ln", 1, 100, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    // start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 3000));
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 3000));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");

    // --------------------close and rebuild local site
    // -------------------------------------------------
    // kill the senders
    /*
     * ExpectedException exp1 = addExpectedException(CacheClosedException.class .getName()); try {
     */ vm4.invoke(() -> WANTestBase.killSender());
    vm5.invoke(() -> WANTestBase.killSender());
    vm6.invoke(() -> WANTestBase.killSender());
    vm7.invoke(() -> WANTestBase.killSender());
    /*
     * } finally { exp1.remove(); }
     */
    LogWriterUtils.getLogWriter().info("Killed all the senders.");

    // restart the vm
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));
    vm6.invoke(() -> WANTestBase.createCache(lnPort));
    vm7.invoke(() -> WANTestBase.createCache(lnPort));

    LogWriterUtils.getLogWriter().info("Created back the cache");

    // create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore1, true));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore2, true));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore3, true));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore4, true));

    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");

    // create PR on local site
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    AsyncInvocation inv2 =
        vm5.invokeAsync(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    AsyncInvocation inv3 =
        vm6.invokeAsync(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    AsyncInvocation inv4 =
        vm7.invokeAsync(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));

    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    inv1 = vm4.invokeAsync(() -> WANTestBase
        .createPersistentPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap()));
    inv2 = vm5.invokeAsync(() -> WANTestBase
        .createPersistentPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap()));
    inv3 = vm6.invokeAsync(() -> WANTestBase
        .createPersistentPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap()));
    inv4 = vm7.invokeAsync(() -> WANTestBase
        .createPersistentPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap()));

    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    // start the senders in async mode. This will ensure that the
    // node of shadow PR that went down last will come up first
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    // wait for senders running
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    LogWriterUtils.getLogWriter().info("All the senders are now running...");

    // ----------------------------------------------------------------------------------------------------

    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));
    vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));
    vm7.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));

    vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 3000));
    vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 3000));
    vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 3000));
    vm7.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 3000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 3000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 3000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 3000));

    vm4.invoke(() -> WANTestBase.doNextPuts(getTestMethodName() + "_RR", 3000, 10000));
    vm4.invoke(() -> WANTestBase.doNextPuts(getTestMethodName() + "_PR", 3000, 10000));

    /*
     * exp1 = addExpectedException(CacheClosedException.class.getName()); try {
     */
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 10000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 10000));
    /*
     * } finally { exp1.remove(); }
     */
  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DRPERSISTENCE_PGSPERSISTENCE_4NODES_2NODESDOWN_Validate_Receiver()
      throws Exception {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // create senders with disk store
    String diskStore1 = vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore2 = vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore3 = vm6.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));
    String diskStore4 = vm7.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2,
        true, 100, 10, false, true, null, null, true));

    LogWriterUtils.getLogWriter()
        .info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
        Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    Thread.sleep(60000);
    {
      AsyncInvocation inv1 = vm7.invokeAsync(() -> ReplicatedRegion_ParallelWANPropagationDUnitTest
          .doPuts0(getTestMethodName() + "_RR", 10000));
      Thread.sleep(1000);
      AsyncInvocation inv2 = vm4.invokeAsync(() -> WANTestBase.killSender());
      Thread.sleep(2000);
      AsyncInvocation inv3 = vm6.invokeAsync(() -> ReplicatedRegion_ParallelWANPropagationDUnitTest
          .doPuts1(getTestMethodName() + "_RR", 10000));
      Thread.sleep(1500);
      AsyncInvocation inv4 = vm5.invokeAsync(() -> WANTestBase.killSender());
      try {
        inv1.join();
        inv2.join();
        inv3.join();
        inv4.join();
      } catch (Exception e) {
        Assert.fail("UnExpected Exception", e);
      }
    }

    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm5.invoke(() -> WANTestBase.createCache(lnPort));

    LogWriterUtils.getLogWriter().info("Created back the cache");

    // create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore1, true));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
        null, diskStore2, true));

    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");

    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    AsyncInvocation inv2 =
        vm5.invokeAsync(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    AsyncInvocation inv3 = vm6.invokeAsync(() -> ReplicatedRegion_ParallelWANPropagationDUnitTest
        .doPuts2(getTestMethodName() + "_RR", 15000));
    try {
      inv1.join();
      inv2.join();

    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    startSenderInVMsAsync("ln", vm4, vm5);

    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    // wait for senders running
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 15000));
  }
}
