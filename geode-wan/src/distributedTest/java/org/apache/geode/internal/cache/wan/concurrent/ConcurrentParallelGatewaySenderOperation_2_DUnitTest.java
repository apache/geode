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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ConcurrentParallelGatewaySenderOperation_2_DUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public ConcurrentParallelGatewaySenderOperation_2_DUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() throws Exception {
    IgnoredException.addIgnoredException("RegionDestroyedException");
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
  }

  @Test
  public void shuttingOneSenderInAVMShouldNotAffectOthersBatchRemovalThread() {
    Integer lnport = (Integer) vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = (Integer) vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnport));

    createCacheInVMs(lnport, vm2, vm3);
    vm2.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, true));
    vm2.invoke(() -> WANTestBase.createSender("ln2", 2, true, 100, 10, false, true, null, true));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln,ln2", 1,
        100, false));

    createCacheInVMs(nyPort, vm4, vm5);
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        false));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100,
        false));
    vm4.invoke(() -> WANTestBase.createReceiver());

    vm2.invoke(() -> WANTestBase.startSender("ln"));
    vm2.invoke(() -> WANTestBase.startSender("ln2"));

    vm3.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, true, null, true));
    vm3.invoke(() -> WANTestBase.createSender("ln2", 2, true, 100, 10, false, true, null, true));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln,ln2", 1,
        100, false));

    vm3.invoke(() -> WANTestBase.startSender("ln"));
    vm3.invoke(() -> WANTestBase.startSender("ln2"));

    vm2.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm2.invoke(() -> WANTestBase.waitForSenderRunningState("ln2"));
    vm3.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm3.invoke(() -> WANTestBase.waitForSenderRunningState("ln2"));

    AsyncInvocation asyncPuts = vm2.invokeAsync(() -> {
      WANTestBase.doPuts(getTestMethodName() + "_PR", 1000);
    });
    // Guarantee some entries are in the queue even if the asyncPuts thread is slow
    vm2.invoke(() -> {
      WANTestBase.doPuts(getTestMethodName() + "_PR", 100);
    });
    vm2.invoke(() -> await()
        .until(() -> WANTestBase.getSenderStats("ln", -1).get(3) > 0));
    vm2.invoke(() -> WANTestBase.stopSender("ln")); // Things have dispatched
    // Dispatch additional values
    vm2.invoke(() -> {
      WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 1000, 1100);
    });

    await().until(() -> asyncPuts.isDone());

    vm2.invoke(() -> await()
        .untilAsserted(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1100)));
    vm4.invoke(() -> await()
        .untilAsserted(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1100)));

    vm3.invoke(() -> {
      await()
          .untilAsserted(
              () -> assertTrue(WANTestBase.getQueueContentSize("ln2", true) + " was the size",
                  WANTestBase.getQueueContentSize("ln2", true) == 0));
    });
  }

  // to test that when userPR is locally destroyed, shadow Pr is also locally
  // destroyed and on recreation userPr , shadow Pr is also recreated.
  @Test
  public void testParallelGatewaySender_SingleNode_UserPR_localDestroy_RecreateRegion()
      throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    try {
      String regionName = getTestMethodName() + "_PR";

      createCacheInVMs(lnPort, vm4);
      vm4.invoke(() -> AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = -1);
      vm4.invoke(
          () -> createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 10, isOffHeap()));
      vm4.invoke(() -> createConcurrentSender("ln", 2, true, 100, 10, false, false, null, false, 5,
          OrderPolicy.KEY));
      vm4.invoke(() -> startSender("ln"));
      vm4.invoke(() -> pauseSender("ln"));

      createCacheInVMs(nyPort, vm2);
      vm2.invoke(() -> createPartitionedRegion(regionName, null, 1, 10, isOffHeap()));
      vm2.invoke(() -> createReceiver());

      vm4.invoke(() -> doPuts(regionName, 10));
      vm4.invoke(() -> validateRegionSize(regionName, 10));

      vm2.invoke(() -> validateRegionSize(regionName, 0));

      vm4.invoke(() -> localDestroyRegion(getTestMethodName() + "_PR"));

      vm2.invoke(() -> validateRegionSize(regionName, 0));

      vm4.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 10, isOffHeap()));
      vm4.invoke(() -> doPutsFrom(regionName, 10, 20));

      vm2.invoke(() -> await()
          .untilAsserted(() -> validateRegionSize(regionName, 0)));

      vm4.invoke(() -> validateRegionSize(regionName, 10));
    } finally {
      vm4.invoke(() -> AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = 0);
    }
  }

  @Test
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_RecreateRegion()
      throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    try {
      String regionName = getTestMethodName() + "_PR";

      createCacheInVMs(lnPort, vm4);
      vm4.invoke(() -> AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = -1);
      vm4.invoke(
          () -> createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 10, isOffHeap()));
      vm4.invoke(() -> createConcurrentSender("ln", 2, true, 100, 10, false, false, null, false, 4,
          OrderPolicy.KEY));
      vm4.invoke(() -> startSender("ln"));
      vm4.invoke(() -> pauseSender("ln"));

      createCacheInVMs(nyPort, vm2);
      vm2.invoke(() -> createPartitionedRegion(regionName, null, 1, 10, isOffHeap()));
      vm2.invoke(() -> createReceiver());

      vm4.invoke(() -> doPuts(regionName, 10));
      vm4.invoke(() -> validateRegionSize(regionName, 10));

      vm2.invoke(() -> validateRegionSize(regionName, 0));

      vm4.invoke(() -> resumeSender("ln"));
      vm4.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));
      vm4.invoke(() -> localDestroyRegion(getTestMethodName() + "_PR"));

      vm2.invoke(() -> validateRegionSize(regionName, 10));

      vm4.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 10, isOffHeap()));
      vm4.invoke(() -> doPutsFrom(regionName, 10, 20));

      vm2.invoke(() -> await()
          .untilAsserted(() -> validateRegionSize(regionName, 20)));

      vm4.invoke(() -> validateRegionSize(regionName, 10));

    } finally {
      vm4.invoke(() -> AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = 0);
    }
  }

  @Test
  public void testParallelGatewaySender_SingleNode_UserPR_Close_RecreateRegion() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];
    try {
      String regionName = getTestMethodName() + "_PR";
      createCacheInVMs(lnPort, vm4);
      vm4.invoke(() -> AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = -1);
      vm4.invoke(
          () -> createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 10, isOffHeap()));
      vm4.invoke(() -> createConcurrentSender("ln", 2, true, 100, 10, false, false, null, false, 7,
          OrderPolicy.KEY));
      vm4.invoke(() -> startSender("ln"));
      vm4.invoke(() -> pauseSender("ln"));

      createCacheInVMs(nyPort, vm2);
      vm2.invoke(() -> createPartitionedRegion(regionName, null, 1, 10, isOffHeap()));
      vm2.invoke(() -> createReceiver());

      vm4.invoke(() -> doPuts(regionName, 10));
      vm4.invoke(() -> validateRegionSize(regionName, 10));
      vm4.invoke(() -> closeRegion(getTestMethodName() + "_PR"));
      vm4.invoke(() -> resumeSender("ln"));

      Thread.sleep(500);

      vm2.invoke(() -> validateRegionSize(regionName, 0));

      vm4.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 10, isOffHeap()));
      vm4.invoke(() -> doPutsFrom(regionName, 10, 20));

      vm2.invoke(() -> await()
          .untilAsserted(() -> validateRegionSize(regionName, 10)));

      vm4.invoke(() -> validateRegionSize(regionName, 10));
    } finally {
      vm4.invoke(() -> AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = 0);
    }
  }

  @Test
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_SimultaneousPut_RecreateRegion()
      throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    try {
      createAndStartSender(vm4, lnPort, 6, false, true);

      vm4.invoke(() -> addCacheListenerAndDestroyRegion(getTestMethodName() + "_PR"));

      createReceiverAndDoPutsInPausedSender(nyPort);

      vm4.invoke(() -> resumeSender("ln"));

      AsyncInvocation putAsync =
          vm4.invokeAsync(() -> WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 10, 101));
      try {
        putAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      if (putAsync.getException() != null
          && !(putAsync.getException() instanceof RegionDestroyedException)) {
        Assert.fail("Expected RegionDestroyedException but got", putAsync.getException());
      }

      // before destroy, there is wait for queue to drain, so data will be
      // dispatched
      vm2.invoke(() -> validateRegionSizeWithinRange(getTestMethodName() + "_PR", 10, 101)); // possible
                                                                                             // size
                                                                                             // is
                                                                                             // more
                                                                                             // than
                                                                                             // 10

      vm4.invoke(
          () -> createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 10, isOffHeap()));

      vm4.invoke(() -> doPutsFrom(getTestMethodName() + "_PR", 10, 20));

      vm4.invoke(() -> validateRegionSize(getTestMethodName() + "_PR", 10));

      vm2.invoke(() -> validateRegionSizeWithinRange(getTestMethodName() + "_PR", 20, 101)); // possible
                                                                                             // size
                                                                                             // is
                                                                                             // more
                                                                                             // than
                                                                                             // 20
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }

  @Test
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_NodeDown() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    try {
      createAndStartSender(vm4, lnPort, 5, false, true);
      createAndStartSender(vm5, lnPort, 5, false, true);
      createAndStartSender(vm6, lnPort, 5, false, true);

      createReceiverAndDoPutsInPausedSender(nyPort);

      vm4.invoke(() -> WANTestBase.resumeSender("ln"));
      vm5.invoke(() -> WANTestBase.resumeSender("ln"));
      vm6.invoke(() -> WANTestBase.resumeSender("ln"));

      Wait.pause(200);
      AsyncInvocation localDestroyAsync =
          vm4.invokeAsync(() -> WANTestBase.destroyRegion(getTestMethodName() + "_PR"));

      AsyncInvocation closeAsync = vm4.invokeAsync(() -> WANTestBase.closeCache());
      try {
        localDestroyAsync.join();
        closeAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      vm2.invoke(() -> validateRegionSize(getTestMethodName() + "_PR", 10));
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
      vm5.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
      vm6.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }

  }

  @Test
  public void testParallelGatewaySender_SingleNode_UserPR_Close_SimultaneousPut_RecreateRegion()
      throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    try {
      String regionName = getTestMethodName() + "_PR";

      createCacheInVMs(lnPort, vm4);
      vm4.invoke(() -> AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = -1);
      vm4.invoke(
          () -> createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 10, isOffHeap()));
      vm4.invoke(() -> createConcurrentSender("ln", 2, true, 100, 10, false, false, null, false, 5,
          OrderPolicy.KEY));
      vm4.invoke(() -> startSender("ln"));
      vm4.invoke(() -> pauseSender("ln"));

      createCacheInVMs(nyPort, vm2);
      vm2.invoke(() -> createPartitionedRegion(regionName, null, 1, 10, isOffHeap()));
      vm2.invoke(() -> createReceiver());

      vm4.invoke(() -> doPuts(regionName, 10));
      vm4.invoke(() -> validateRegionSize(regionName, 10));

      vm2.invoke(() -> validateRegionSize(regionName, 0));

      AsyncInvocation putAsync =
          vm4.invokeAsync(() -> WANTestBase.doPutsFrom(getTestMethodName() + "_PR", 10, 2000));
      AsyncInvocation localDestroyAsync =
          vm4.invokeAsync(() -> ConcurrentParallelGatewaySenderOperation_2_DUnitTest
              .closeRegion(getTestMethodName() + "_PR"));
      try {
        putAsync.join();
        localDestroyAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }
      vm2.invoke(() -> validateRegionSize(regionName, 0));

      vm4.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 10, isOffHeap()));
      vm4.invoke(() -> doPutsFrom(regionName, 10, 20));

      vm2.invoke(() -> await()
          .untilAsserted(() -> validateRegionSize(regionName, 0)));

      vm4.invoke(() -> validateRegionSize(regionName, 10));
    } finally {
      vm4.invoke(() -> AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = 0);
    }
  }

  @Test
  public void testParallelGatewaySenders_SingleNode_UserPR_localDestroy_RecreateRegion()
      throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];
    Integer tkPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(3, lnPort));
    Integer pnPort = (Integer) vm3.invoke(() -> createFirstRemoteLocator(4, lnPort));

    createCacheInVMs(nyPort, vm4);
    vm4.invoke(() -> createReceiver());
    createCacheInVMs(tkPort, vm5);
    vm5.invoke(() -> createReceiver());
    createCacheInVMs(pnPort, vm6);
    vm6.invoke(() -> createReceiver());

    try {
      vm7.invoke(() -> createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME(lnPort));

      LogWriterUtils.getLogWriter().info("Created cache on local site");

      vm7.invoke(() -> createConcurrentSender("ln1", 2, true, 100, 10, false, false, null, true, 5,
          OrderPolicy.KEY));
      vm7.invoke(() -> createConcurrentSender("ln2", 3, true, 100, 10, false, false, null, true, 5,
          OrderPolicy.KEY));
      vm7.invoke(() -> createConcurrentSender("ln3", 4, true, 100, 10, false, false, null, true, 5,
          OrderPolicy.KEY));

      vm7.invoke(() -> startSender("ln1"));
      vm7.invoke(() -> startSender("ln2"));
      vm7.invoke(() -> startSender("ln3"));

      String regionName = getTestMethodName() + "_PR";
      vm7.invoke(() -> createPartitionedRegion(regionName, "ln1,ln2,ln3", 1, 10, isOffHeap()));

      LogWriterUtils.getLogWriter().info("Created PRs on local site");

      vm4.invoke(() -> createPartitionedRegion(regionName, null, 1, 10, isOffHeap()));
      vm5.invoke(() -> createPartitionedRegion(regionName, null, 1, 10, isOffHeap()));
      vm6.invoke(() -> createPartitionedRegion(regionName, null, 1, 10, isOffHeap()));

      vm7.invoke(() -> doPuts(regionName, 10));

      vm7.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln1"));
      vm7.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln2"));
      vm7.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln3"));

      vm7.invoke(() -> localDestroyRegion(regionName));

      vm7.invoke(() -> createPartitionedRegion(regionName, "ln1,ln2,ln3", 1, 10, isOffHeap()));

      vm7.invoke(() -> doPutsFrom(regionName, 10, 20));

      vm7.invoke(() -> validateRegionSize(regionName, 10));

      validateRegionSizes(regionName, 20, vm4, vm5, vm6);
    } finally {
      vm7.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }

  @Test
  public void testParallelGatewaySender_MultipleNode_UserPR_localDestroy_Recreate()
      throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> createReceiver());

    try {
      createAndStartSender(vm4, lnPort, 5, true, false);
      createAndStartSender(vm5, lnPort, 5, true, false);

      String regionName = getTestMethodName() + "_PR";
      vm2.invoke(() -> createPartitionedRegion(regionName, null, 1, 10, isOffHeap()));

      AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.doPuts(regionName, 10));
      Wait.pause(1000);
      vm5.invoke(() -> localDestroyRegion(regionName));

      try {
        inv1.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
        fail("Interrupted the async invocation.");
      }


      validateRegionSizes(regionName, 10, vm4, vm2);

      vm5.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 10, isOffHeap()));

      vm4.invoke(() -> doPutsFrom(regionName, 10, 20));

      validateRegionSizes(regionName, 20, vm4, vm2);
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
      vm5.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }

  @Test
  public void testParallelGatewaySenders_MultipleNode_UserPR_localDestroy_Recreate()
      throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];
    Integer tkPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(3, lnPort));

    createCacheInVMs(nyPort, vm6);
    vm6.invoke(() -> createReceiver());
    createCacheInVMs(tkPort, vm7);
    vm7.invoke(() -> createReceiver());

    try {
      createAndStartTwoSenders(vm4, lnPort, 4);
      createAndStartTwoSenders(vm5, lnPort, 4);

      String regionName = getTestMethodName() + "_PR";
      vm6.invoke(() -> WANTestBase.createPartitionedRegion(regionName, null, 1, 100, isOffHeap()));
      vm7.invoke(() -> WANTestBase.createPartitionedRegion(regionName, null, 1, 100, isOffHeap()));

      AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.doPuts(regionName, 10));

      Wait.pause(1000);
      vm5.invoke(() -> WANTestBase.localDestroyRegion(regionName));

      try {
        inv1.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      validateRegionSizes(regionName, 10, vm4, vm6, vm7);

      vm5.invoke(
          () -> WANTestBase.createPartitionedRegion(regionName, "ln1,ln2", 1, 100, isOffHeap()));

      vm4.invoke(() -> WANTestBase.doPutsFrom(regionName, 10, 20));

      validateRegionSizes(regionName, 20, vm4, vm6, vm7);
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
      vm5.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }

  @Test
  public void testParallelGatewaySender_ColocatedPartitionedRegions_localDestroy()
      throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> createReceiver());

    try {
      createAndStartSenderWithCustomerOrderShipmentRegion(vm4, lnPort, 5, true);
      createAndStartSenderWithCustomerOrderShipmentRegion(vm5, lnPort, 5, true);

      LogWriterUtils.getLogWriter().info("Created PRs on local site");

      vm2.invoke(() -> createCustomerOrderShipmentPartitionedRegion(null, 1, 100, isOffHeap()));

      AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.putcolocatedPartitionedRegion(10));
      Wait.pause(1000);

      try {
        vm5.invoke(() -> localDestroyRegion(customerRegionName));
      } catch (Exception ex) {
        assertTrue(ex.getCause() instanceof UnsupportedOperationException);
      }

      try {
        inv1.join();
      } catch (Exception e) {
        Assert.fail("Unexpected exception", e);
      }

      validateRegionSizes(customerRegionName, 10, vm4, vm5, vm2);
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
      vm5.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }

  @Test
  public void testParallelGatewaySender_ColocatedPartitionedRegions_destroy() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    try {
      createAndStartSenderWithCustomerOrderShipmentRegion(vm4, lnPort, 6, true);
      createAndStartSenderWithCustomerOrderShipmentRegion(vm5, lnPort, 6, true);

      LogWriterUtils.getLogWriter().info("Created PRs on local site");

      vm2.invoke(() -> WANTestBase.createCustomerOrderShipmentPartitionedRegion(null, 1, 100,
          isOffHeap()));

      AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.putcolocatedPartitionedRegion(2000));
      Wait.pause(1000);

      try {
        vm5.invoke(() -> WANTestBase.destroyRegion(customerRegionName));
      } catch (Exception ex) {
        assertTrue(ex.getCause() instanceof IllegalStateException);
        return;
      }
      fail("Expected UnsupportedOperationException");
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
      vm5.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }

  public static void clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME() {
    AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = 0;
  }

  public static void closeRegion(String regionName) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    r.close();
  }

  public static void validateRegionSizeWithinRange(String regionName, final int min,
      final int max) {
    final Region r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (r.keySet().size() > min && r.keySet().size() <= max) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected region entries to be within range : " + min + " " + max
            + " but actual entries: " + r.keySet().size();
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  protected static void createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME(Integer locPort) {
    createCache(false, locPort);
    AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = -1;
  }

  protected void createAndStartSender(VM vm, int port, int concurrencyLevel, boolean manualStart,
      boolean pause) {
    vm.invoke(() -> createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME(port));
    vm.invoke(() -> createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 10, isOffHeap()));
    createSender(vm, concurrencyLevel, manualStart);
    vm.invoke(() -> startSender("ln"));
    if (pause) {
      vm.invoke(() -> pauseSender("ln"));
    }
    LogWriterUtils.getLogWriter().info("Created PRs on local site");
  }

  protected void createReceiverAndDoPutsInPausedSender(int port) {
    // Note: This is a test-specific method used by several tests to do puts from vm4 to vm2.
    String regionName = getTestMethodName() + "_PR";
    createCacheInVMs(port, vm2);
    vm2.invoke(() -> createReceiver());
    vm2.invoke(() -> createPartitionedRegion(regionName, null, 1, 10, isOffHeap()));
    vm4.invoke(() -> doPuts(regionName, 10));
    vm4.invoke(() -> validateRegionSize(regionName, 10));
    // since sender is paused, no dispatching
    vm2.invoke(() -> validateRegionSize(regionName, 0));
  }

  protected void createAndStartTwoSenders(VM vm, int port, int concurrencyLevel) {
    // Note: This is a test-specific method used to create and start 2 senders.
    vm.invoke(() -> createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME(port));
    vm.invoke(
        () -> createPartitionedRegion(getTestMethodName() + "_PR", "ln1,ln2", 1, 100, isOffHeap()));
    createSenders(vm, concurrencyLevel);
    vm.invoke(() -> startSender("ln1"));
    vm.invoke(() -> startSender("ln2"));
  }

  protected void createAndStartSenderWithCustomerOrderShipmentRegion(VM vm, int port,
      int concurrencyLevel, boolean manualStart) {
    vm.invoke(() -> createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME(port));
    vm.invoke(() -> createCustomerOrderShipmentPartitionedRegion("ln", 1, 100, isOffHeap()));
    createSender(vm, concurrencyLevel, manualStart);
    vm.invoke(() -> startSender("ln"));
  }

  protected void createSender(VM vm, int concurrencyLevel, boolean manualStart) {
    vm.invoke(() -> createConcurrentSender("ln", 2, true, 100, 10, false, false, null, manualStart,
        concurrencyLevel, OrderPolicy.KEY));
  }

  protected void createSenders(VM vm, int concurrencyLevel) {
    vm.invoke(() -> createConcurrentSender("ln1", 2, true, 100, 10, false, false, null, true,
        concurrencyLevel, OrderPolicy.KEY));
    vm.invoke(() -> createConcurrentSender("ln2", 3, true, 100, 10, false, false, null, true,
        concurrencyLevel, OrderPolicy.KEY));
  }
}
