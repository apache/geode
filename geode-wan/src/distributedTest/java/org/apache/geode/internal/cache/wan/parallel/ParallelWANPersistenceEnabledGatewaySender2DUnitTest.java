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



import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ParallelWANPersistenceEnabledGatewaySender2DUnitTest extends WANTestBase {


  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogService.getLogger();

  public ParallelWANPersistenceEnabledGatewaySender2DUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() throws Exception {
    // The restart tests log this string
    IgnoredException.addIgnoredException("failed accepting client connection");
  }

  protected SerializableRunnableIF killSenderRunnable() {
    return WANTestBase::killSender;
  }

  protected SerializableRunnableIF createPartitionedRegionRunnable() {
    return () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1, 100,
        isOffHeap());
  }

  protected SerializableRunnableIF pauseSenderRunnable() {
    return () -> WANTestBase.pauseSender("ln");
  }

  protected SerializableRunnableIF stopSenderRunnable() {
    return () -> WANTestBase.stopSender("ln");
  }

  protected SerializableRunnableIF startSenderRunnable() {
    return () -> WANTestBase.startSender("ln");
  }


  protected SerializableRunnableIF waitForSenderRunnable() {
    return () -> WANTestBase.waitForSenderRunningState("ln");
  }

  private SerializableRunnableIF waitForSenderNonRunnable() {
    return () -> WANTestBase.waitForSenderNonRunningState("ln");
  }

  @Test
  public void testPersistentPR_Restart_one_server_while_clean_queue() throws InterruptedException {
    // create locator on local site
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    // create locator on remote site
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // create cache in remote site
    createCacheInVMs(nyPort, vm2, vm3);

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

    logger
        .info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);

    // create PR on remote site
    vm2.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 100, isOffHeap()));

    // create PR on local site
    vm4.invoke(createPartitionedRegionRunnable());
    vm5.invoke(createPartitionedRegionRunnable());
    vm6.invoke(createPartitionedRegionRunnable());
    vm7.invoke(createPartitionedRegionRunnable());


    // start the senders on local site
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    // wait for senders to become running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());

    logger.info("All senders are running.");

    // start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName(), 3000));
    logger.info("Completed puts in the region");


    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 0));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 0));
    logger.info("Check that no events are propagated to remote site");

    vm7.invoke(killSenderRunnable());

    logger.info("Killed vm7 sender.");
    // --------------------close and rebuild local site
    // -------------------------------------------------
    // stop the senders

    vm4.invoke(() -> WANTestBase.stopSender("ln"));
    vm5.invoke(() -> WANTestBase.stopSender("ln"));
    vm6.invoke(() -> WANTestBase.stopSender("ln"));

    logger.info("Stopped all the senders.");

    // wait for senders to stop
    vm4.invoke(waitForSenderNonRunnable());
    vm5.invoke(waitForSenderNonRunnable());
    vm6.invoke(waitForSenderNonRunnable());

    // create receiver on remote site
    createReceiverInVMs(vm2, vm3);

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 0));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 0));

    logger.info("Start all the senders.");

    AsyncInvocation<Void> startSenderwithCleanQueuesInVM4 =
        vm4.invokeAsync(() -> startSenderwithCleanQueues("ln"));

    AsyncInvocation<Void> startSenderwithCleanQueuesInVM5 =
        vm5.invokeAsync(() -> startSenderwithCleanQueues("ln"));
    AsyncInvocation<Void> startSenderwithCleanQueuesInVM6 =
        vm6.invokeAsync(() -> startSenderwithCleanQueues("ln"));


    startSenderwithCleanQueuesInVM4.await();
    startSenderwithCleanQueuesInVM5.await();
    startSenderwithCleanQueuesInVM6.await();

    logger.info("Waiting for senders running.");
    // wait for senders running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());

    logger.info("All the senders are now running...");

    // restart the vm
    vm7.invoke("Create back the cache", () -> createCache(lnPort));

    // create senders with disk store
    vm7.invoke("Create sender back from the disk store.",
        () -> WANTestBase.createSenderWithDiskStore("ln", 2, true, 100, 10, false, true,
            null, diskStore4, false));

    // create PR on local site
    vm7.invoke("Create back the partitioned region",
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), "ln", 1,
            100, isOffHeap()));

    // wait for senders running
    // ----------------------------------------------------------------------------------------------------

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 0));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName(), 0));

  }


}
