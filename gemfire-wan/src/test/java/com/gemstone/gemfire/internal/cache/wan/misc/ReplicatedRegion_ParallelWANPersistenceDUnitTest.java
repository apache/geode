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
package com.gemstone.gemfire.internal.cache.wan.misc;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.ExpectedException;

public class ReplicatedRegion_ParallelWANPersistenceDUnitTest extends WANTestBase {
  
  public ReplicatedRegion_ParallelWANPersistenceDUnitTest(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }

  final String expectedExceptions = null;

  public void testNothing() {
    
  }
  
  /**Below test is disabled intentionally
  1> In this release 8.0, for rolling upgrade support queue name is changed to old style
  2>Comman parallel sender for different non colocated regions is not supported in 8.0 so no need to bother about 
      ParallelGatewaySenderQueue#convertPathToName
  3> We have to enabled it in next release
  4> Version based rolling upgrade support should be provided. based on the version of the gemfire QSTRING should be used between 8.0 
     and version prior to 8.0*/
  public void DISABLED_test_DR_PGSPERSISTENCE_VALIDATEQUEUE_Restart_Validate_Receiver() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    //create receiver on remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", null, isOffHeap() });
    
    //create cache in local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //create senders with disk store
    String diskStore1 = (String) vm4.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });
    String diskStore2 = (String) vm5.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });
    String diskStore3 = (String) vm6.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });
    String diskStore4 = (String) vm7.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });

    getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    //start puts in region on local site
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR", 3000 });
    getLogWriter().info("Completed puts in the region");
    
    //--------------------close and rebuild local site -------------------------------------------------
    //kill the senders
/*    ExpectedException exp1 = addExpectedException(CacheClosedException.class
        .getName());
    try {
*/      vm4.invoke(WANTestBase.class, "killSender", new Object[] {});
      vm5.invoke(WANTestBase.class, "killSender", new Object[] {});
      vm6.invoke(WANTestBase.class, "killSender", new Object[] {});
      vm7.invoke(WANTestBase.class, "killSender", new Object[] {});
/*    }
    finally {
      exp1.remove();
    }
*/    
    getLogWriter().info("Killed all the senders.");
    
    //restart the vm
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    
    getLogWriter().info("Created back the cache");
    
   //create senders with disk store
    vm4.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore1, true });
    vm5.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore2, true });
    vm6.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore3, true });
    vm7.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore4, true });
    
    getLogWriter().info("Created the senders back from the disk store.");
    
    //create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    AsyncInvocation inv2 = vm5.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    AsyncInvocation inv3 = vm6.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    AsyncInvocation inv4 = vm7.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", isOffHeap() });
    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    
    //start the senders in async mode. This will ensure that the 
    //node of shadow PR that went down last will come up first
    vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    getLogWriter().info("All the senders are now running...");
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    
    //----------------------------------------------------------------------------------------------------
    
    vm4.invoke(WANTestBase.class, "doNextPuts", new Object[] { testName + "_RR", 3000, 10000 });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 10000 });
    
  }
  
  /**Below test is disabled intentionally
  1> In this release 8.0, for rolling upgrade support queue name is changed to old style
  2>Comman parallel sender for different non colocated regions is not supported in 8.0 so no need to bother about 
      ParallelGatewaySenderQueue#convertPathToName
  3> We have to enabled it in next release
  4> Version based rolling upgrade support should be provided. based on the version of the gemfire QSTRING should be used between 8.0 
     and version prior to 8.0*/
  public void DISABLED_test_DRPERSISTENCE_PGSPERSISTENCE_VALIDATEQUEUE_Restart_Validate_Receiver() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    //create receiver on remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", null, isOffHeap() });
    
    //create cache in local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //create senders with disk store
    String diskStore1 = (String) vm4.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });
    String diskStore2 = (String) vm5.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });
    String diskStore3 = (String) vm6.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });
    String diskStore4 = (String) vm7.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });

    getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    vm4.invoke(WANTestBase.class,
        "createReplicatedRegion", new Object[] { testName + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    vm5.invoke(WANTestBase.class,
        "createReplicatedRegion", new Object[] { testName + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    vm6.invoke(WANTestBase.class,
        "createReplicatedRegion", new Object[] { testName + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    vm7.invoke(WANTestBase.class,
        "createReplicatedRegion", new Object[] { testName + "_RR", "ln",
            Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    //start puts in region on local site
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR", 3000 });
    getLogWriter().info("Completed puts in the region");
    
    //--------------------close and rebuild local site -------------------------------------------------
    //kill the senders
/*    ExpectedException exp1 = addExpectedException(CacheClosedException.class
        .getName());
    try {*/
      vm4.invoke(WANTestBase.class, "killSender", new Object[] {});
      vm5.invoke(WANTestBase.class, "killSender", new Object[] {});
      vm6.invoke(WANTestBase.class, "killSender", new Object[] {});
      vm7.invoke(WANTestBase.class, "killSender", new Object[] {});
/*    }
    finally {
      exp1.remove();
    }*/
    
    //restart the vm
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    
    getLogWriter().info("Created back the cache");
    
   //create senders with disk store
    vm4.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore1, true });
    vm5.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore2, true });
    vm6.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore3, true });
    vm7.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore4, true });
    
    getLogWriter().info("Created the senders back from the disk store.");
    
    //create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    AsyncInvocation inv2 = vm5.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    AsyncInvocation inv3 = vm6.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    AsyncInvocation inv4 = vm7.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    
    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    
    //start the senders in async mode. This will ensure that the 
    //node of shadow PR that went down last will come up first
    vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    getLogWriter().info("All the senders are now running...");
    
    //----------------------------------------------------------------------------------------------------
    
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    
/*    exp1 = addExpectedException(CacheClosedException.class.getName());
    try {
*/      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_RR", 3000 });
      vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_RR", 3000 });

      vm4.invoke(WANTestBase.class, "doNextPuts", new Object[] {
          testName + "_RR", 3000, 10000 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_RR", 10000 });
      vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_RR", 10000 });
/*    }
    finally {
      exp1.remove();
    }
*/  }
  
  /**Below test is disabled intentionally
  1> In this release 8.0, for rolling upgrade support queue name is changed to old style
  2>Comman parallel sender for different non colocated regions is not supported in 8.0 so no need to bother about 
      ParallelGatewaySenderQueue#convertPathToName
  3> We have to enabled it in next release
  4> Version based rolling upgrade support should be provided. based on the version of the gemfire QSTRING should be used between 8.0 
     and version prior to 8.0*/
  public void DISABLED_test_DRPERSISTENCE_PRPERSISTENCE_PGSPERSISTENCE_VALIDATEQUEUE_Restart_Validate_Receiver() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    //create receiver on remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", null, isOffHeap() });
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 1, 100, isOffHeap() });
    
    //create cache in local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //create senders with disk store
    String diskStore1 = (String) vm4.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });
    String diskStore2 = (String) vm5.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });
    String diskStore3 = (String) vm6.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });
    String diskStore4 = (String) vm7.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, null, true });

    getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", Scope.DISTRIBUTED_ACK,
        DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", Scope.DISTRIBUTED_ACK,
        DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", Scope.DISTRIBUTED_ACK,
        DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", Scope.DISTRIBUTED_ACK,
        DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
        new Object[] { testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
        new Object[] { testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
        new Object[] { testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
        new Object[] { testName + "_PR", "ln", 1, 100, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    //start puts in region on local site
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR", 3000 });
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 3000 });
    getLogWriter().info("Completed puts in the region");
    
    //--------------------close and rebuild local site -------------------------------------------------
    //kill the senders
/*    ExpectedException exp1 = addExpectedException(CacheClosedException.class
        .getName());
    try {
*/      vm4.invoke(WANTestBase.class, "killSender", new Object[] {});
      vm5.invoke(WANTestBase.class, "killSender", new Object[] {});
      vm6.invoke(WANTestBase.class, "killSender", new Object[] {});
      vm7.invoke(WANTestBase.class, "killSender", new Object[] {});
/*    }
    finally {
      exp1.remove();
    }
*/    
    getLogWriter().info("Killed all the senders.");
    
    //restart the vm
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    
    getLogWriter().info("Created back the cache");
    
   //create senders with disk store
    vm4.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore1, true });
    vm5.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore2, true });
    vm6.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore3, true });
    vm7.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore4, true });
    
    getLogWriter().info("Created the senders back from the disk store.");
    
    //create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    AsyncInvocation inv2 = vm5.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    AsyncInvocation inv3 = vm6.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    AsyncInvocation inv4 = vm7.invokeAsync(WANTestBase.class, "createReplicatedRegion", new Object[] {
      testName + "_RR", "ln", Scope.DISTRIBUTED_ACK, DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    
    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    
    inv1 = vm4.invokeAsync(WANTestBase.class,
        "createPersistentPartitionedRegion", new Object[] { testName + "_PR", "ln", 1,
            100, isOffHeap() });
    inv2 = vm5.invokeAsync(WANTestBase.class,
        "createPersistentPartitionedRegion", new Object[] { testName + "_PR", "ln", 1,
            100, isOffHeap() });
    inv3 = vm6.invokeAsync(WANTestBase.class,
        "createPersistentPartitionedRegion", new Object[] { testName + "_PR", "ln", 1,
            100, isOffHeap() });
    inv4 = vm7.invokeAsync(WANTestBase.class,
        "createPersistentPartitionedRegion", new Object[] { testName + "_PR", "ln", 1,
            100, isOffHeap() });

    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    }
    catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    
    //start the senders in async mode. This will ensure that the 
    //node of shadow PR that went down last will come up first
    vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    getLogWriter().info("All the senders are now running...");
    
    //----------------------------------------------------------------------------------------------------
    
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3000 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3000 });
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3000 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3000 });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 3000 });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 3000 });
    
    vm4.invoke(WANTestBase.class, "doNextPuts", new Object[] { testName + "_RR", 3000, 10000 });
    vm4.invoke(WANTestBase.class, "doNextPuts", new Object[] { testName + "_PR", 3000, 10000 });
    
/*    exp1 = addExpectedException(CacheClosedException.class.getName());
    try {*/
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_RR", 10000 });
      vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_RR", 10000 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10000 });
      vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10000 });
/*    }
    finally {
      exp1.remove();
    }
*/    
  }
  
  /**Below test is disabled intentionally
  1> In this release 8.0, for rolling upgrade support queue name is changed to old style
  2>Comman parallel sender for different non colocated regions is not supported in 8.0 so no need to bother about 
      ParallelGatewaySenderQueue#convertPathToName
  3> We have to enabled it in next release
  4> Version based rolling upgrade support should be provided. based on the version of the gemfire QSTRING should be used between 8.0 
     and version prior to 8.0*/
  public void DISABLED_test_DRPERSISTENCE_PGSPERSISTENCE_4NODES_2NODESDOWN_Validate_Receiver()
      throws Exception {

    Integer lnPort = (Integer) vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer) vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    // create senders with disk store
    String diskStore1 = (String) vm4.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, true, 100, 10,
            false, true, null, null, true });
    String diskStore2 = (String) vm5.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, true, 100, 10,
            false, true, null, null, true });
    String diskStore3 = (String) vm6.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, true, 100, 10,
            false, true, null, null, true });
    String diskStore4 = (String) vm7.invoke(WANTestBase.class,
        "createSenderWithDiskStore", new Object[] { "ln", 2, true, 100, 10,
            false, true, null, null, true });

    getLogWriter().info(
        "The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + ","
            + diskStore4);

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", Scope.DISTRIBUTED_ACK,
        DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", Scope.DISTRIBUTED_ACK,
        DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", Scope.DISTRIBUTED_ACK,
        DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", Scope.DISTRIBUTED_ACK,
        DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });

    pauseWaitCriteria(60000);
    {
      AsyncInvocation inv1 = vm7.invokeAsync(
          ReplicatedRegion_ParallelWANPropogationDUnitTest.class, "doPuts0",
          new Object[] { testName + "_RR", 10000 });
      pauseWaitCriteria(1000);
      AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "killSender");
      pauseWaitCriteria(2000);
      AsyncInvocation inv3 = vm6.invokeAsync(
          ReplicatedRegion_ParallelWANPropogationDUnitTest.class, "doPuts1",
          new Object[] { testName + "_RR", 10000 });
      pauseWaitCriteria(1500);
      AsyncInvocation inv4 = vm5.invokeAsync(WANTestBase.class, "killSender");
      try {
        inv1.join();
        inv2.join();
        inv3.join();
        inv4.join();
      } catch (Exception e) {
        fail("UnExpected Exception", e);
      }
    }

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    getLogWriter().info("Created back the cache");

    // create senders with disk store
    vm4.invoke(WANTestBase.class, "createSenderWithDiskStore", new Object[] {
        "ln", 2, true, 100, 10, false, true, null, diskStore1, true });
    vm5.invoke(WANTestBase.class, "createSenderWithDiskStore", new Object[] {
        "ln", 2, true, 100, 10, false, true, null, diskStore2, true });

    getLogWriter().info("Created the senders back from the disk store.");

    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class,
        "createReplicatedRegion",
        new Object[] { testName + "_RR", "ln", Scope.DISTRIBUTED_ACK,
            DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    AsyncInvocation inv2 = vm5.invokeAsync(WANTestBase.class,
        "createReplicatedRegion",
        new Object[] { testName + "_RR", "ln", Scope.DISTRIBUTED_ACK,
            DataPolicy.PERSISTENT_REPLICATE, isOffHeap() });
    AsyncInvocation inv3 = vm6.invokeAsync(
        ReplicatedRegion_ParallelWANPropogationDUnitTest.class, "doPuts2",
        new Object[] { testName + "_RR", 15000 });
    try {
      inv1.join();
      inv2.join();

    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });

    getLogWriter().info("Waiting for senders running.");
    // wait for senders running
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });

    vm6.invoke(WANTestBase.class,
        "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class,
        "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 15000 });
  }
}
