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
package com.gemstone.gemfire.internal.cache.wan.concurrent;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author skumar
 *
 */
public class ConcurrentParallelGatewaySenderOperation_2_DUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;
  
  public ConcurrentParallelGatewaySenderOperation_2_DUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    addExpectedException("RegionDestroyedException");
    addExpectedException("Broken pipe");
    addExpectedException("Connection reset");
    addExpectedException("Unexpected IOException");
  }
  
  // to test that when userPR is locally destroyed, shadow Pr is also locally
  // destroyed and on recreation usrePr , shadow Pr is also recreated.
  public void testParallelGatewaySender_SingleNode_UserPR_localDestroy_RecreateRegion() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    try {
      createAndStartSender(vm4, lnPort, 5, false, true);

      createReceiverAndDoPutsInPausedSender(nyPort);

      vm4.invoke(() -> localDestroyRegion(testName + "_PR"));

      recreatePRDoPutsAndValidateRegionSizes(0, true);
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_RecreateRegion() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    try {
      createAndStartSender(vm4, lnPort, 4, false, true);

      createReceiverAndDoPutsInPausedSender(nyPort);

      vm4.invoke(() -> resumeSender("ln"));

      vm4.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));

      vm4.invoke(() -> localDestroyRegion(testName + "_PR"));

      recreatePRDoPutsAndValidateRegionSizes(10, false);
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }

  public void testParallelGatewaySender_SingleNode_UserPR_Close_RecreateRegion() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createAndStartSender(vm4, lnPort, 7, false, true);

    createReceiverAndDoPutsInPausedSender(nyPort);

    vm4.invoke(() -> closeRegion(testName + "_PR"));

    vm4.invoke(() -> resumeSender("ln"));

    pause(500); //paused if there is any element which is received on remote site

    recreatePRDoPutsAndValidateRegionSizes(0, false);

    vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
  }
  
  //to test that while localDestroy is in progress, put operation does not successed
  public void testParallelGatewaySender_SingleNode_UserPR_localDestroy_SimultenuousPut_RecreateRegion() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    try {
      createAndStartSender(vm4, lnPort, 5, false, true);

      createReceiverAndDoPutsInPausedSender(nyPort);

      AsyncInvocation putAsync = vm4.invokeAsync(WANTestBase.class,
          "doPutsFrom", new Object[] { testName + "_PR", 100, 2000 });
      AsyncInvocation localDestroyAsync = vm4.invokeAsync(WANTestBase.class,
          "localDestroyRegion", new Object[] { testName + "_PR" });
      try {
        putAsync.join();
        localDestroyAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      if (localDestroyAsync.getException() != null) {
        fail("Not Expected Exception got", putAsync.getException());
      }

      if (putAsync.getException() != null
          && !(putAsync.getException() instanceof RegionDestroyedException)) {
        fail("Expected RegionDestroyedException but got",
            putAsync.getException());
      }

      vm4.invoke(() -> resumeSender("ln"));

      pause(500); // paused if there is any element which is received on remote
                  // site

      recreatePRDoPutsAndValidateRegionSizes(0, false);
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_SimultenuousPut_RecreateRegion() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    try {
      createAndStartSender(vm4, lnPort, 6, false, true);

      vm4.invoke(() -> addCacheListenerAndDestroyRegion(testName + "_PR"));

      createReceiverAndDoPutsInPausedSender(nyPort);

      vm4.invoke(() -> resumeSender("ln"));

      AsyncInvocation putAsync = vm4.invokeAsync(WANTestBase.class,
          "doPutsFrom", new Object[] { testName + "_PR", 10, 101 });
      try {
        putAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      if (putAsync.getException() != null
          && !(putAsync.getException() instanceof RegionDestroyedException)) {
        fail("Expected RegionDestroyedException but got",
            putAsync.getException());
      }

      // before destroy, there is wait for queue to drain, so data will be
      // dispatched
      vm2.invoke(() -> validateRegionSizeWithinRange(testName + "_PR", 10, 101)); // possible size is more than 10

      vm4.invoke(() -> createPartitionedRegion(testName + "_PR", "ln", 1, 10, isOffHeap()));

      vm4.invoke(() -> doPutsFrom(testName + "_PR", 10, 20));

      vm4.invoke(() -> validateRegionSize(testName + "_PR", 10));

      vm2.invoke(() -> validateRegionSizeWithinRange(testName + "_PR", 20, 101)); // possible size is more than 20
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_NodeDown()
      throws Exception {
    addExpectedException("Broken pipe");
    addExpectedException("Connection reset");
    addExpectedException("Unexpected IOException");
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    try {
      createAndStartSender(vm4, lnPort, 5, false, true);
      createAndStartSender(vm5, lnPort, 5, false, true);
      createAndStartSender(vm6, lnPort, 5, false, true);

      createReceiverAndDoPutsInPausedSender(nyPort);

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
      vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

      pause(200);
      AsyncInvocation localDestroyAsync = vm4.invokeAsync(WANTestBase.class,
          "destroyRegion", new Object[] { testName + "_PR" });

      AsyncInvocation closeAsync = vm4.invokeAsync(WANTestBase.class,
          "closeCache");
      try {
        localDestroyAsync.join();
        closeAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      vm2.invoke(() -> validateRegionSize(testName + "_PR", 10));
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
      vm5.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
      vm6.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }

  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Close_SimultenuousPut_RecreateRegion() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    try {
      createAndStartSender(vm4, lnPort, 5, false, true);

      createReceiverAndDoPutsInPausedSender(nyPort);

      AsyncInvocation putAsync = vm4.invokeAsync(WANTestBase.class,
          "doPutsFrom", new Object[] { testName + "_PR", 10, 2000 });
      AsyncInvocation localDestroyAsync = vm4.invokeAsync(
          ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "closeRegion", new Object[] { testName + "_PR" });
      try {
        putAsync.join();
        localDestroyAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      recreatePRDoPutsAndValidateRegionSizes(0, true);
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }

  private void createReceiverAndDoPutsInPausedSender(int port) {
    // Note: This is a test-specific method used by several tests to do puts from vm4 to vm2.
    String regionName = testName + "_PR";
    vm2.invoke(() -> createReceiver(port));
    vm2.invoke(() -> createPartitionedRegion(regionName, null, 1, 10, isOffHeap()));
    vm4.invoke(() -> doPuts(regionName, 10));
    vm4.invoke(() -> validateRegionSize(regionName, 10));
    // since sender is paused, no dispatching
    vm2.invoke(() -> validateRegionSize(regionName, 0));
  }

  private void recreatePRDoPutsAndValidateRegionSizes(int expectedRegionSize, boolean resumeSender) {
    // Note: This is a test-specific method used by several test to recreate a partitioned region,
    // do puts and validate region sizes in vm2 and vm4.
    // since shadowPR is locally destroyed, so no data to dispatch
    String regionName = testName + "_PR";
    vm2.invoke(() -> validateRegionSize(regionName, expectedRegionSize));
    if (resumeSender) {
      vm4.invoke(() -> resumeSender("ln"));
    }
    vm4.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 10, isOffHeap()));
    vm4.invoke(() -> doPutsFrom(regionName, 10, 20));
    validateRegionSizes(regionName, 10, vm4, vm2);
  }

  public void testParallelGatewaySenders_SingleNode_UserPR_localDestroy_RecreateRegion() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];
    Integer tkPort = (Integer)vm2.invoke(() -> createFirstRemoteLocator(3, lnPort));
    Integer pnPort = (Integer)vm3.invoke(() -> createFirstRemoteLocator(4, lnPort));

    vm4.invoke(() -> createReceiver(nyPort));
    vm5.invoke(() -> createReceiver(tkPort));
    vm6.invoke(() -> createReceiver(pnPort));

    try {
      vm7.invoke(() -> createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME(lnPort));

      getLogWriter().info("Created cache on local site");

      vm7.invoke(() -> createConcurrentSender("ln1", 2, true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY));
      vm7.invoke(() -> createConcurrentSender("ln2", 3, true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY));
      vm7.invoke(() -> createConcurrentSender("ln3", 4, true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY));

      vm7.invoke(() -> startSender("ln1"));
      vm7.invoke(() -> startSender("ln2"));
      vm7.invoke(() -> startSender("ln3"));

      String regionName = testName + "_PR";
      vm7.invoke(() -> createPartitionedRegion(regionName, "ln1,ln2,ln3", 1, 10, isOffHeap()));

      getLogWriter().info("Created PRs on local site");

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
  
  public void testParallelGatewaySender_MultipleNode_UserPR_localDestroy_Recreate() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    vm2.invoke(() -> createReceiver(nyPort));

    try {
      createAndStartSender(vm4, lnPort, 5, true, false);
      createAndStartSender(vm5, lnPort, 5, true, false);

      String regionName = testName + "_PR";
      vm2.invoke(() -> createPartitionedRegion(regionName, null, 1, 10, isOffHeap()));

      AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
          new Object[] { regionName, 10 });
      pause(1000);
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
  
  public void testParallelGatewaySenders_MultiplNode_UserPR_localDestroy_Recreate() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];
    Integer tkPort = (Integer)vm2.invoke(() -> createFirstRemoteLocator(3, lnPort));

    vm6.invoke(() -> createReceiver(nyPort));
    vm7.invoke(() -> createReceiver(tkPort));

    try {
      createAndStartTwoSenders(vm4, lnPort);
      createAndStartTwoSenders(vm5, lnPort);

      String regionName = testName + "_PR";
      vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          regionName, null, 1, 100, isOffHeap() });
      vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          regionName, null, 1, 100, isOffHeap() });

      AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
          new Object[] { regionName, 10 });

      pause(1000);
      vm5.invoke(WANTestBase.class, "localDestroyRegion",
          new Object[] { regionName });

      try {
        inv1.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      validateRegionSizes(regionName, 10, vm4, vm6, vm7);

      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          regionName, "ln1,ln2", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          regionName, 10, 20 });

      validateRegionSizes(regionName, 20, vm4, vm6, vm7);
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
      vm5.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }

  private void createAndStartTwoSenders(VM vm, int port) {
    // Note: This is a test-specific method used to create and start 2 senders.
    vm.invoke(() -> createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME(port));
    vm.invoke(() -> createPartitionedRegion(testName + "_PR", "ln1,ln2", 1, 100, isOffHeap()));
    vm.invoke(() -> createConcurrentSender("ln1", 2, true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY));
    vm.invoke(() -> createConcurrentSender("ln2", 3, true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY));
    vm.invoke(() -> startSender("ln1"));
    vm.invoke(() -> startSender("ln2"));
  }

  public void testParallelGatewaySender_ColocatedPartitionedRegions_localDestroy() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    vm2.invoke(() -> createReceiver(nyPort));

    try {
      createAndStartSenderWithCustomerOrderShipmentRegion(vm4, lnPort, 5, true);
      createAndStartSenderWithCustomerOrderShipmentRegion(vm5, lnPort, 5, true);

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(() -> createCustomerOrderShipmentPartitionedRegion(null, null, 1, 100, isOffHeap()));

      AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class,
          "putcolocatedPartitionedRegion", new Object[] { 10 });
      pause(1000);

      try {
        vm5.invoke(() -> localDestroyRegion(customerRegionName));
      } catch (Exception ex) {
        assertTrue(ex.getCause() instanceof UnsupportedOperationException);
      }

      try {
        inv1.join();
      } catch (Exception e) {
        fail("Unexpected exception", e);
      }

      validateRegionSizes(customerRegionName, 10, vm4, vm5, vm2);
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
      vm5.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }

  private void validateRegionSizes(String regionName, int expectedRegionSize, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> validateRegionSize(regionName, expectedRegionSize));
    }
  }

  public void testParallelGatewaySender_ColocatedPartitionedRegions_destroy() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    try {
      createAndStartSenderWithCustomerOrderShipmentRegion(vm4, lnPort, 6, true);
      createAndStartSenderWithCustomerOrderShipmentRegion(vm5, lnPort, 6, true);

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              null, 1, 100, isOffHeap() });

      AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class,
          "putcolocatedPartitionedRegion", new Object[] { 2000 });
      pause(1000);

      try {
        vm5.invoke(WANTestBase.class, "destroyRegion",
            new Object[] { customerRegionName });
      } catch (Exception ex) {
        assertTrue(ex.getCause() instanceof IllegalStateException);
        return;
      }
      fail("Excpeted UnsupportedOperationException");
    } finally {
      vm4.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
      vm5.invoke(() -> clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME());
    }
  }

  private void createAndStartSenderWithCustomerOrderShipmentRegion(VM vm, int port, int concurrencyLevel, boolean manualStart) {
    vm.invoke(() -> createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME(port));
    vm.invoke(() -> createCustomerOrderShipmentPartitionedRegion(null, "ln", 1, 100, isOffHeap()));
    vm.invoke(() -> createConcurrentSender("ln", 2, true, 100, 10, false, false, null, manualStart, concurrencyLevel, OrderPolicy.KEY));
    vm.invoke(() -> startSender("ln"));
  }

  private void createAndStartSender(VM vm, int port, int concurrencyLevel, boolean manualStart, boolean pause) {
    vm.invoke(() -> createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME(port));
    vm.invoke(() -> createConcurrentSender("ln", 2, true, 100, 10, false, false, null, manualStart, concurrencyLevel, OrderPolicy.KEY));
    vm.invoke(() -> startSender("ln"));
    if (pause) {
      vm.invoke(() -> pauseSender("ln"));
    }
    vm.invoke(() -> createPartitionedRegion(testName + "_PR", "ln", 1, 10, isOffHeap()));
    getLogWriter().info("Created PRs on local site");
  }

  public static void createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME(
      Integer locPort) {
    createCache(false, locPort);
    AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = -1;
  }

  public static void clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME() {
    AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = 0;
  }
  
  public static void closeRegion(String regionName) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    r.close();
  }

  public static void validateRegionSizeWithinRange(String regionName,
      final int min, final int max) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (r.keySet().size() > min && r.keySet().size() <= max) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected region entries to be within range : " + min + " "
            + max + " but actual entries: " + r.keySet().size();
      }
    };
    DistributedTestCase.waitForCriterion(wc, 120000, 500, true);
  }
}
