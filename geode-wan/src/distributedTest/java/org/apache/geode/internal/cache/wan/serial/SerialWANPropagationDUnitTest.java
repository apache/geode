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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.wan.BatchException70;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.util.internal.GeodeGlossary;

@Category({WanTest.class})
public class SerialWANPropagationDUnitTest extends WANTestBase {

  @Override
  public final void postSetUpWANTestBase() throws Exception {
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection refused");
    IgnoredException.addIgnoredException("could not get remote locator information");
    IgnoredException.addIgnoredException("Unexpected IOException");
  }

  /**
   * this test is disabled due to a high rate of failure in unit test runs see ticket #52190
   */
  @Ignore("TODO: test is disabled because of #52190")
  @Test
  public void testReplicatedSerialPropagation_withoutRemoteLocator() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // keep the batch size high enough to reduce the number of exceptions in the log
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 400, false, false, null, true));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(createReplicatedRegionRunnable());
    vm5.invoke(createReplicatedRegionRunnable());
    vm6.invoke(createReplicatedRegionRunnable());
    vm7.invoke(createReplicatedRegionRunnable());

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 1000));

    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);

    vm2.invoke(() -> WANTestBase.createReceiver());
    vm3.invoke(() -> WANTestBase.createReceiver());

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
  }

  protected SerializableRunnableIF createReplicatedRegionRunnable() {
    return () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ln", isOffHeap());
  }

  @Test
  public void testReplicatedSerialPropagation_withoutRemoteSite() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // keep the batch size high enough to reduce the number of exceptions in the log
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 400, false, false, null, true));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(createReplicatedRegionRunnable());
    vm5.invoke(createReplicatedRegionRunnable());
    vm6.invoke(createReplicatedRegionRunnable());
    vm7.invoke(createReplicatedRegionRunnable());

    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 1000));

    createCacheInVMs(nyPort, vm2, vm3);

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    vm2.invoke(() -> WANTestBase.createReceiver());
    vm3.invoke(() -> WANTestBase.createReceiver());

    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
  }

  /**
   * Added to reproduce the bug #46595
   */
  @Test
  public void testReplicatedSerialPropagationWithoutRemoteSite_defect46595() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // reduce the batch-size so maximum number of batches will be sent
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 5, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 5, false, false, null, true));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(createReplicatedRegionRunnable());
    vm5.invoke(createReplicatedRegionRunnable());
    vm6.invoke(createReplicatedRegionRunnable());
    vm7.invoke(createReplicatedRegionRunnable());

    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());
    IgnoredException.addIgnoredException(IOException.class.getName());
    IgnoredException.addIgnoredException(java.net.SocketException.class.getName());

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 10000));

    // pause for some time before starting up the remote site
    Wait.pause(10000);

    createCacheInVMs(nyPort, vm2, vm3);

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    vm2.invoke(() -> WANTestBase.createReceiver());
    vm3.invoke(() -> WANTestBase.createReceiver());

    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 10000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 10000));
  }

  @Test
  public void testReplicatedSerialPropagation() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(createReplicatedRegionRunnable());
    vm5.invoke(createReplicatedRegionRunnable());
    vm6.invoke(createReplicatedRegionRunnable());
    vm7.invoke(createReplicatedRegionRunnable());

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 1000));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
  }

  @Test
  public void testReplicatedSerialPropagationWithLocalSiteClosedAndRebuilt() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(createReplicatedRegionRunnable());
    vm5.invoke(createReplicatedRegionRunnable());
    vm6.invoke(createReplicatedRegionRunnable());
    vm7.invoke(createReplicatedRegionRunnable());

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 1000));

    // ---------close local site and build again-----------------------------------------
    vm4.invoke(() -> WANTestBase.killSender());
    vm5.invoke(() -> WANTestBase.killSender());
    vm6.invoke(() -> WANTestBase.killSender());
    vm7.invoke(() -> WANTestBase.killSender());

    Integer regionSize =
        vm2.invoke(() -> WANTestBase.getRegionSize(getUniqueName() + "_RR"));
    LogWriterUtils.getLogWriter().info("Region size on remote is: " + regionSize);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm4.invoke(createReplicatedRegionRunnable());
    vm5.invoke(createReplicatedRegionRunnable());
    vm6.invoke(createReplicatedRegionRunnable());
    vm7.invoke(createReplicatedRegionRunnable());

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    IgnoredException.addIgnoredException(EntryExistsException.class.getName());
    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 1000));
    // ----------------------------------------------------------------------------------

    // verify remote site receives all the events
    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
  }

  /**
   * Two regions configured with the same sender and put is in progress on both the regions. One of
   * the two regions is destroyed in the middle.
   */
  @Test
  public void testReplicatedSerialPropagationWithLocalRegionDestroy() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // these are part of remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    // these are part of local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // senders are created on local site
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 20, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 20, false, false, null, true));

    // create one RR (RR_1) on remote site
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));

    // create another RR (RR_2) on remote site
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", null, isOffHeap()));

    // start the senders on local site
    startSenderInVMs("ln", vm4, vm5);

    // create one RR (RR_1) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));

    // create another RR (RR_2) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 1000));
    // do puts in RR_2 in main thread
    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR_2", 500));
    // destroy RR_2 after above puts are complete
    vm4.invoke(() -> WANTestBase.destroyRegion(getUniqueName() + "_RR_2"));

    inv1.join();

    // vm4.invoke(() -> WANTestBase.verifyQueueSize( "ln", 0 ));
    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_2", 500));
  }

  /**
   * 1 region and sender configured on local site and 1 region and a receiver configured on remote
   * site. Puts to the local region are in progress. Remote region is destroyed in the middle.
   */
  @Test
  public void testReplicatedSerialPropagationWithRemoteRegionDestroy() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // these are part of remote site
    createCacheInVMs(nyPort, vm2, vm3);

    // create one RR (RR_1) on remote site
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));

    createReceiverInVMs(vm2, vm3);

    vm2.invoke(() -> addListenerToSleepAfterCreateEvent(1000, getUniqueName() + "_RR_1"));
    vm3.invoke(() -> addListenerToSleepAfterCreateEvent(1000, getUniqueName() + "_RR_1"));

    // these are part of local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // create one RR (RR_1) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));

    // senders are created on local site
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 500, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 500, false, false, null, true));


    // start the senders on local site
    startSenderInVMs("ln", vm4, vm5);

    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 1000));
    // destroy RR_1 in remote site
    vm2.invoke(() -> WANTestBase.destroyRegion(getUniqueName() + "_RR_1"));

    inv1.join();

    // verify that all is well in local site. All the events should be present in local region
    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 1000));
    // assuming some events might have been dispatched before the remote region was destroyed,
    // sender's region queue will have events less than 1000 but the queue will not be empty.
    // NOTE: this much verification might be sufficient in DUnit. Hydra will take care of
    // more in depth validations.
    vm4.invoke(() -> WANTestBase.verifyRegionQueueNotEmpty("ln"));
  }

  /**
   * Two regions configured in local with the same sender and put is in progress on both the
   * regions. Same two regions are configured on remote site as well. One of the two regions is
   * destroyed in the middle on remote site.
   */
  @Test
  public void testReplicatedSerialPropagationWithRemoteRegionDestroy2() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // these are part of remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    // these are part of local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // senders are created on local site
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 200, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 200, false, false, null, true));

    // create one RR (RR_1) on remote site
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));

    // create another RR (RR_2) on remote site
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", null, isOffHeap()));

    // start the senders on local site
    startSenderInVMs("ln", vm4, vm5);

    // create one RR (RR_1) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));

    // create another RR (RR_2) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));
    // destroy RR_2 on remote site in the middle
    vm2.invoke(() -> WANTestBase.destroyRegion(getUniqueName() + "_RR_2"));

    // expected exceptions in the logs
    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    // start puts in RR_2 in another thread
    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR_2", 1000));

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 1000));

    inv1.join();

    // though region RR_2 is destroyed, RR_1 should still get all the events put in it
    // in local site
    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 1000));
  }

  @Test
  public void testReplicatedSerialPropagationWithRemoteRegionDestroy3() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    // these are part of remote site
    createCacheInVMs(nyPort, vm2, vm3);

    // create one RR (RR_1) on remote site
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));

    // create another RR (RR_2) on remote site
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", null, isOffHeap()));

    createReceiverInVMs(vm2, vm3);

    // these are part of local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // create one RR (RR_1) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));

    // create another RR (RR_2) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_2", "ln", isOffHeap()));

    // senders are created on local site
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 200, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 200, false, false, null, true));

    // start the senders on local site
    startSenderInVMs("ln", vm4, vm5);

    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 1000));
    // start puts in RR_2 in another thread
    AsyncInvocation inv2 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR_2", 1000));
    // destroy RR_2 on remote site in the middle
    vm2.invoke(() -> WANTestBase.destroyRegion(getUniqueName() + "_RR_2"));

    inv1.join();
    inv2.join();


    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 1000));

    // though region RR_2 is destroyed, RR_1 should still get all the events put
    // in it
    // in local site
    try {
      vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 1000));
    } finally {
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX + "GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION",
          "False");
      vm4.invoke(new CacheSerializableRunnable("UnSetting system property ") {
        @Override
        public void run2() throws CacheException {
          System.setProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION",
              "False");
        }
      });

      vm5.invoke(new CacheSerializableRunnable("UnSetting system property ") {
        @Override
        public void run2() throws CacheException {
          System.setProperty(
              GeodeGlossary.GEMFIRE_PREFIX + "GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION",
              "False");
        }
      });
    }
  }

  /**
   * one region and sender configured on local site and the same region and a receiver configured on
   * remote site. Puts to the local region are in progress. Receiver on remote site is stopped in
   * the middle by closing remote site cache.
   */
  @Category({WanTest.class})
  @Test
  public void testReplicatedSerialPropagationWithRemoteReceiverStopped() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // these are part of remote site
    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    // these are part of local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // senders are created on local site. Batch size is kept to a high (170) so
    // there will be less number of exceptions (occur during dispatchBatch) in the log
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 350, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 350, false, false, null, true));

    // create one RR (RR_1) on remote site
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));

    // start the senders on local site
    startSenderInVMs("ln", vm4, vm5);

    // create one RR (RR_1) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 500));
    // close cache in remote site. This will automatically kill the remote receivers.
    vm2.invoke(() -> WANTestBase.closeCache());

    inv1.join();

    // verify that all is well in local site
    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 500));
    vm4.invoke(() -> WANTestBase.verifyRegionQueueNotEmpty("ln"));
  }

  @Test
  public void testReplicatedSerialPropagationWithRemoteReceiverRestarted() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // these are part of remote site
    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm2.invoke(() -> WANTestBase.createReceiver());

    // these are part of local site
    createCacheInVMs(lnPort, vm4, vm5);

    // senders are created on local site. Batch size is kept to a high (170) so
    // there will be less number of exceptions (occur during dispatchBatch) in
    // the log
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 350, false, false, null, true));

    // create one RR (RR_1) on remote site
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));

    // start the senders on local site
    vm4.invoke(() -> WANTestBase.startSender("ln"));

    // create one RR (RR_1) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));


    // start puts in RR_1 in another thread
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 8000));
    // close cache in remote site. This will automatically kill the remote
    // receivers.
    Wait.pause(2000);
    vm2.invoke(() -> WANTestBase.closeCache());

    inv1.join();

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 1000));

    // verify that all is well in local site
    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 8000));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));
    vm2.invoke(() -> WANTestBase.createReceiver());

    vm4.invoke(() -> WANTestBase.validateQueueContents("ln", 0));

    vm2.invoke(() -> WANTestBase.checkMinimumGatewayReceiverStats(1, 1));
  }

  @Test
  public void testReplicatedSerialPropagationWithRemoteReceiverRestarted_SenderReceiverPersistent()
      throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // these are part of remote site
    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    // these are part of local site
    createCacheInVMs(lnPort, vm4, vm5);

    // senders are created on local site. Batch size is kept to a high (170) so
    // there will be less number of exceptions (occur during dispatchBatch) in
    // the log
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 350, false, true, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 350, false, true, null, true));

    // create one RR (RR_1) on remote site
    vm2.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getUniqueName() + "_RR_1",
        null, isOffHeap()));
    vm2.invoke(
        () -> WANTestBase.addListenerToSleepAfterCreateEvent(2000, getUniqueName() + "_RR_1"));
    // start the senders on local site
    startSenderInVMs("ln", vm4, vm5);

    // create one RR (RR_1) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 8000));
    // close cache in remote site. This will automatically kill the remote
    // receivers.
    vm2.invoke(() -> WANTestBase.closeCache());

    inv1.join();

    // verify that all is well in local site
    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 8000));

    vm4.invoke(() -> WANTestBase.verifyRegionQueueNotEmpty("ln"));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getUniqueName() + "_RR_1",
        null, isOffHeap()));
    vm2.invoke(() -> WANTestBase.createReceiver());

    vm4.invoke(() -> WANTestBase.validateQueueContents("ln", 0));

    vm2.invoke(() -> WANTestBase.checkMinimumGatewayReceiverStats(1, 1));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 8000));
  }

  @Test
  public void testReplicatedSerialPropagationWithRemoteSiteBouncedBack_ReceiverPersistent()
      throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // these are part of remote site
    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    // these are part of local site
    createCacheInVMs(lnPort, vm4, vm5);

    // senders are created on local site. Batch size is kept to a high (170) so
    // there will be less number of exceptions (occur during dispatchBatch) in
    // the log
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 350, false, false, null, true));

    // create one RR (RR_1) on remote site
    vm2.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getUniqueName() + "_RR_1",
        null, isOffHeap()));

    // start the senders on local site
    vm4.invoke(() -> WANTestBase.startSender("ln"));

    // create one RR (RR_1) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 8000));
    // close cache in remote site. This will automatically kill the remote
    // receivers.
    Wait.pause(2000);
    vm1.invoke(() -> WANTestBase.shutdownLocator());
    vm2.invoke(() -> WANTestBase.closeCache());

    inv1.join();

    // Do some extra puts after cache close so that some events are in the queue.
    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 1000));

    // verify that all is well in local site
    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 8000));

    vm4.invoke(() -> WANTestBase.verifyRegionQueueNotEmpty("ln"));

    vm1.invoke(() -> WANTestBase.bringBackLocatorOnOldPort(2, lnPort, nyPort));

    createCacheInVMs(nyPort, vm2);

    vm2.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getUniqueName() + "_RR_1",
        null, isOffHeap()));

    vm2.invoke(() -> WANTestBase.createReceiver());

    vm4.invoke(() -> WANTestBase.validateQueueContents("ln", 0));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 8000));

    vm2.invoke(() -> WANTestBase.checkMinimumGatewayReceiverStats(1, 1));
  }

  @Test
  public void testReplicatedSerialPropagationWithRemoteSiteBouncedBackWithMultipleRemoteLocators()
      throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort1 = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer nyPort2 =
        vm3.invoke(() -> WANTestBase.createSecondRemoteLocator(2, nyPort1, lnPort));

    // these are part of remote site
    createCacheInVMs(nyPort1, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    // these are part of local site
    createCacheInVMs(lnPort, vm4, vm5);

    // senders are created on local site. Batch size is kept to a high (170) so
    // there will be less number of exceptions (occur during dispatchBatch) in
    // the log
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 350, false, false, null, true));

    // create one RR (RR_1) on remote site
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));

    // start the senders on local site
    vm4.invoke(() -> WANTestBase.startSender("ln"));

    // create one RR (RR_1) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 8000));
    // close cache in remote site. This will automatically kill the remote
    // receivers.
    Wait.pause(2000);
    vm1.invoke(() -> WANTestBase.shutdownLocator());
    vm2.invoke(() -> WANTestBase.closeCache());

    inv1.join();

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 1000));
    // verify that all is well in local site
    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 8000));

    vm4.invoke(() -> WANTestBase.verifyRegionQueueNotEmpty("ln"));

    createCacheInVMs(nyPort2, vm6);
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", null, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createReceiver());

    vm4.invoke(() -> WANTestBase.validateQueueContents("ln", 0));

    vm6.invoke(() -> WANTestBase.checkMinimumGatewayReceiverStats(1, 1));
  }

  @Test
  public void testReplicatedSerialPropagationWithRemoteReceiverRestartedOnOtherNode()
      throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // these are part of remote site
    createCacheInVMs(nyPort, vm2, vm3);

    // these are part of local site
    createCacheInVMs(lnPort, vm4);

    // senders are created on local site. Batch size is kept to a high (170) so
    // there will be less number of exceptions (occur during dispatchBatch) in
    // the log
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 350, false, false, null, true));

    // create one RR (RR_1) on remote site
    vm2.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getUniqueName() + "_RR_1",
        null, isOffHeap()));
    vm2.invoke(() -> WANTestBase.createReceiver());

    vm3.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getUniqueName() + "_RR_1",
        null, isOffHeap()));

    vm2.invoke(() -> addListenerToSleepAfterCreateEvent(2000, getUniqueName() + "_RR_1"));
    vm3.invoke(() -> addListenerToSleepAfterCreateEvent(2000, getUniqueName() + "_RR_1"));

    // create one RR (RR_1) on local site
    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR_1", "ln", isOffHeap()));
    // start the senders on local site
    vm4.invoke(() -> WANTestBase.startSender("ln"));

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR_1", 8000));
    // close cache in remote site. This will automatically kill the remote
    // receivers.
    vm2.invoke(() -> WANTestBase.closeCache());
    vm3.invoke(() -> WANTestBase.closeCache());

    inv1.join();

    // verify that all is well in local site
    vm4.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 8000));

    vm4.invoke(() -> WANTestBase.verifyRegionQueueNotEmpty("ln"));

    createCacheInVMs(nyPort, vm3);
    vm3.invoke(() -> WANTestBase.createPersistentReplicatedRegion(getUniqueName() + "_RR_1",
        null, isOffHeap()));
    vm3.invoke(() -> WANTestBase.createReceiver());

    vm4.invoke(() -> WANTestBase.validateQueueContents("ln", 0));

    vm3.invoke(() -> WANTestBase.checkMinimumGatewayReceiverStats(1, 1));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR_1", 8000));
  }

  @Test
  public void testReplicatedSerialPropagationToTwoWanSites() throws Exception {
    Integer lnPort = createFirstLocatorWithDSId(1);
    Integer nyPort = vm0.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
    Integer tkPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(3, lnPort));

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
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(createReplicatedRegionRunnable());
    vm5.invoke(createReplicatedRegionRunnable());
    vm6.invoke(createReplicatedRegionRunnable());
    vm7.invoke(createReplicatedRegionRunnable());

    AsyncInvocation inv1 =
        vm5.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 10000));
    Wait.pause(2000);
    AsyncInvocation inv2 = vm4.invokeAsync(() -> WANTestBase.killSender());

    inv1.join();
    inv2.join();

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 10000));
  }

  /**
   * Local site:: vm4: Primary vm5: Secondary
   *
   * Remote site:: vm2, vm3, vm6, vm7: All hosting receivers
   *
   * vm4 is killed, so vm5 takes primary charge
   *
   * SUR: disabling due to connection information not available in open source enable this once in
   * closed source
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testReplicatedSerialPropagationHA_ReceiverAffinity() throws Exception {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3, vm6, vm7);
    createReceiverInVMs(vm2, vm3, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Started receivers on remote site");

    WANTestBase.createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    LogWriterUtils.getLogWriter().info("Started senders on local site");

    vm4.invoke(createReplicatedRegionRunnable());
    vm5.invoke(createReplicatedRegionRunnable());

    AsyncInvocation inv1 =
        vm5.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 10000));
    LogWriterUtils.getLogWriter().info("Started async puts on local site");
    Wait.pause(1000);

    Map oldConnectionInfo =
        vm4.invoke(() -> WANTestBase.getSenderToReceiverConnectionInfo("ln"));
    assertNotNull(oldConnectionInfo);
    String oldServerHost = (String) oldConnectionInfo.get("serverHost");
    int oldServerPort = (Integer) oldConnectionInfo.get("serverPort");
    LogWriterUtils.getLogWriter().info("Got sender to receiver connection information");

    AsyncInvocation inv2 = vm4.invokeAsync(() -> WANTestBase.killSender());
    inv2.join();
    LogWriterUtils.getLogWriter().info("Killed primary sender on local site");
    Wait.pause(5000);// give some time for vm5 to take primary charge

    Map newConnectionInfo =
        vm5.invoke(() -> WANTestBase.getSenderToReceiverConnectionInfo("ln"));
    assertNotNull(newConnectionInfo);
    String newServerHost = (String) newConnectionInfo.get("serverHost");
    int newServerPort = (Integer) newConnectionInfo.get("serverPort");
    LogWriterUtils.getLogWriter().info("Got new sender to receiver connection information");
    assertEquals(oldServerHost, newServerHost);
    assertEquals(oldServerPort, newServerPort);

    LogWriterUtils.getLogWriter().info(
        "Matched the new connection info with old connection info. Receiver affinity verified.");

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 10000));
  }

  /**
   * Local site:: vm4: Primary vm5: Secondary
   *
   * Remote site:: vm2, vm3, vm6, vm7: All hosting receivers
   *
   * vm4 is killed, so vm5 takes primary charge. vm4 brought up. vm5 is killed, so vm4 takes primary
   * charge again.
   *
   * SUR: commenting due to connection information not available in open source enable this once in
   * closed source
   */
  @Ignore("TODO: test is disabled")
  @Test
  public void testReplicatedSerialPropagationHA_ReceiverAffinityScenario2() throws Exception {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3, vm6, vm7);
    createReceiverInVMs(vm2, vm3, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Started receivers on remote site");

    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    LogWriterUtils.getLogWriter().info("Started senders on local site");

    vm4.invoke(createReplicatedRegionRunnable());
    vm5.invoke(createReplicatedRegionRunnable());

    AsyncInvocation inv1 =
        vm5.invokeAsync(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 10000));
    LogWriterUtils.getLogWriter().info("Started async puts on local site");
    Wait.pause(1000);

    Map oldConnectionInfo =
        vm4.invoke(() -> WANTestBase.getSenderToReceiverConnectionInfo("ln"));
    assertNotNull(oldConnectionInfo);
    String oldServerHost = (String) oldConnectionInfo.get("serverHost");
    int oldServerPort = (Integer) oldConnectionInfo.get("serverPort");
    LogWriterUtils.getLogWriter().info("Got sender to receiver connection information");

    // ---------------------------- KILL vm4
    // --------------------------------------
    AsyncInvocation inv2 = vm4.invokeAsync(() -> WANTestBase.killSender());
    inv2.join();
    LogWriterUtils.getLogWriter().info("Killed vm4 (primary sender) on local site");
    // -----------------------------------------------------------------------------

    vm5.invoke(() -> WANTestBase.waitForSenderToBecomePrimary("ln"));
    LogWriterUtils.getLogWriter().info("vm5 sender has now acquired primary status");
    Wait.pause(5000);// give time to process unprocessedEventsMap

    // ---------------------------REBUILD vm4
    // --------------------------------------
    LogWriterUtils.getLogWriter().info("Rebuilding vm4....");
    createCacheInVMs(lnPort, vm4);
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm4.invoke(() -> WANTestBase.startSender("ln"));
    vm4.invoke(createReplicatedRegionRunnable());
    LogWriterUtils.getLogWriter().info("Rebuilt vm4");
    // -----------------------------------------------------------------------------

    // --------------------------- KILL vm5
    // ----------------------------------------
    inv1.join();// once the puts are done, kill vm5
    LogWriterUtils.getLogWriter().info("puts in vm5 are done");

    inv2 = vm5.invokeAsync(() -> WANTestBase.killSender());
    inv2.join();
    vm4.invoke(() -> WANTestBase.waitForSenderToBecomePrimary("ln"));
    // -----------------------------------------------------------------------------

    Map newConnectionInfo =
        vm4.invoke(() -> WANTestBase.getSenderToReceiverConnectionInfo("ln"));
    assertNotNull(newConnectionInfo);
    String newServerHost = (String) newConnectionInfo.get("serverHost");
    int newServerPort = (Integer) newConnectionInfo.get("serverPort");
    LogWriterUtils.getLogWriter().info("Got new sender to receiver connection information");
    assertEquals(oldServerHost, newServerHost);
    assertEquals(oldServerPort, newServerPort);
    LogWriterUtils.getLogWriter().info(
        "Matched the new connection info with old connection info. Receiver affinity verified.");

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 10000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 10000));
  }

  @Test
  public void testNormalRegionSerialPropagation() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", null, isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender("ln"));
    vm5.invoke(() -> WANTestBase.startSender("ln"));

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

  /**
   * Added for defect #48582 NPE when WAN sender configured but not started. All to all topology
   * with 2 WAN sites: Site 1 (LN site): vm4, vm5, vm6, vm7 Site 2 (NY site): vm2, vm3
   */
  @Test
  public void testReplicatedSerialPropagationWithRemoteSenderConfiguredButNotStarted() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    createReceiverInVMs(vm4, vm5);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, true));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(createReplicatedRegionRunnable());
    vm5.invoke(createReplicatedRegionRunnable());
    vm6.invoke(createReplicatedRegionRunnable());
    vm7.invoke(createReplicatedRegionRunnable());

    vm2.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, true));
    vm3.invoke(() -> WANTestBase.createSender("ny", 1, false, 100, 10, false, false, null, true));

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ny", isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getUniqueName() + "_RR", "ny", isOffHeap()));

    vm4.invoke(() -> WANTestBase.doPuts(getUniqueName() + "_RR", 1000));

    startSenderInVMs("ny", vm2, vm3);

    vm2.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getUniqueName() + "_RR", 1000));
  }

  @Test
  public void testPreloadedSerialPropagation() throws Exception {
    // Start locators
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    // Create receiver and preloaded region
    String regionName = getUniqueName() + "_preloaded";
    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(regionName, null, Scope.DISTRIBUTED_ACK,
        DataPolicy.PRELOADED, isOffHeap()));
    vm2.invoke(() -> WANTestBase.createReceiver());
    vm2.invoke(() -> WANTestBase.addListenerOnRegion(regionName));

    // Create sender and preloaded region
    vm4.invoke(() -> WANTestBase.createCache(lnPort));
    vm4.invoke(() -> WANTestBase.createSender("ln", 2, false, 100, 10, false, false, null, false));
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(regionName, "ln", Scope.DISTRIBUTED_ACK,
        DataPolicy.PRELOADED, isOffHeap()));

    // Do puts
    int numEvents = 10;
    vm4.invoke(() -> WANTestBase.doPuts(regionName, numEvents));

    // Verify receiver listener events
    vm2.invoke(() -> WANTestBase.verifyListenerEvents(numEvents));

    // Do destroys
    vm4.invoke(() -> WANTestBase.doDestroys(regionName, numEvents));

    // Verify receiver listener events
    vm2.invoke(() -> WANTestBase.verifyListenerEvents(numEvents * 2));
  }
}
