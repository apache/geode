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

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

import java.util.Set;

/**
 * @author skumar
 *
 */
public class CommonParallelGatewaySenderDUnitTest extends WANTestBase {

  public CommonParallelGatewaySenderDUnitTest(String name ){
    super(name);
  }
  
  public void testSameSenderWithNonColocatedRegions() throws Exception {
    IgnoredException.addIgnoredException("cannot have the same parallel");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", "ln", 1, 100, isOffHeap() });
    try {
      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", "ln", 1, 100, isOffHeap() });
      fail("Expected IllegateStateException : cannot have the same parallel gateway sender");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("cannot have the same parallel gateway sender id"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  /**
   * Simple scenario. Two regions attach the same PGS
   * @throws Exception
   * Below test is disabled intentionally
    1> In this release 8.0, for rolling upgrade support queue name is changed to old style
    2>Comman parallel sender for different non colocated regions is not supported in 8.0 so no need to bother about 
      ParallelGatewaySenderQueue#convertPathToName
    3> We have to enabled it in next release
    4> Version based rolling upgrade support should be provided. based on the version of the gemfire QSTRING should be used between 8.0 
     and version prior to 8.0
   */
  public void DISABLED_testParallelPropagation() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", null, 1, 100, isOffHeap() });
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", null, 1, 100, isOffHeap() });
    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR1",
        1000 });
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR2",
      1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(CommonParallelGatewaySenderDUnitTest.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(CommonParallelGatewaySenderDUnitTest.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(CommonParallelGatewaySenderDUnitTest.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(CommonParallelGatewaySenderDUnitTest.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_PR1", 1000 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_PR2", 1000 });
  }
  
  /**
   * The PGS is persistence enabled but not the Regions
   * Below test is disabled intentionally
    1> In this release 8.0, for rolling upgrade support queue name is changed to old style
    2>Comman parallel sender for different non colocated regions is not supported in 8.0 so no need to bother about 
      ParallelGatewaySenderQueue#convertPathToName
    3> We have to enabled it in next release
    4> Version based rolling upgrade support should be provided. based on the version of the gemfire QSTRING should be used between 8.0 
     and version prior to 8.0
   */
  public void DISABLED_testParallelPropagationPersistenceEnabled() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, true, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, true, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, true, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, true, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR1", null, 1, 100, isOffHeap() });
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR2", null, 1, 100, isOffHeap() });
    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR1",
        1000 });
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR2",
      1000 });
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(CommonParallelGatewaySenderDUnitTest.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(CommonParallelGatewaySenderDUnitTest.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(CommonParallelGatewaySenderDUnitTest.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(CommonParallelGatewaySenderDUnitTest.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_PR1", 1000 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_PR2", 1000 });
  }
  
  
  /**
   * Enable persistence for GatewaySender.
   * Pause the sender and do some puts in local region.  
   * Close the local site and rebuild the region and sender from disk store.
   * Dispatcher should not start dispatching events recovered from persistent sender.
   * Check if the remote site receives all the events.
   * Below test is disabled intentionally
    1> In this release 8.0, for rolling upgrade support queue name is changed to old style
    2>Comman parallel sender for different non colocated regions is not supported in 8.0 so no need to bother about 
      ParallelGatewaySenderQueue#convertPathToName
    3> We have to enabled it in next release
    4> Version based rolling upgrade support should be provided. based on the version of the gemfire QSTRING should be used between 8.0 
     and version prior to 8.0
   */
  public void DISABLED_testPRWithGatewaySenderPersistenceEnabled_Restart() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    //create receiver on remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

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

    LogWriterUtils.getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    //create PR on remote site
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR1", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR1", null, 1, 100, isOffHeap() });
    
    //create PR on remote site
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR2", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR2", null, 1, 100, isOffHeap() });
    
    //create PR on local site
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR1", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR1", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR1", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR1", "ln", 1, 100, isOffHeap() });

    //create PR on local site
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR2", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR2", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR2", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR2", "ln", 1, 100, isOffHeap() });

    
    //start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    //wait for senders to become running
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //pause the senders
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    //start puts in region on local site
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName()+"PR1", 3000 });
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName()+"PR2", 5000 });
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    //--------------------close and rebuild local site -------------------------------------------------
    //kill the senders
    vm4.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm5.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm6.invoke(WANTestBase.class, "killSender", new Object[] {});
    vm7.invoke(WANTestBase.class, "killSender", new Object[] {});
    
    LogWriterUtils.getLogWriter().info("Killed all the senders.");
    
    //restart the vm
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    
    LogWriterUtils.getLogWriter().info("Created back the cache");
    
   //create senders with disk store
    vm4.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore1, true });
    vm5.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore2, true });
    vm6.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore3, true });
    vm7.invoke(WANTestBase.class, "createSenderWithDiskStore", 
        new Object[] { "ln", 2, true, 100, 10, false, true, null, diskStore4, true });
    
    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");
    //create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR1", "ln", 1, 100, isOffHeap() });
    AsyncInvocation inv2 = vm5.invokeAsync(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR1", "ln", 1, 100, isOffHeap() });
    AsyncInvocation inv3 = vm6.invokeAsync(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR1", "ln", 1, 100, isOffHeap() });
    AsyncInvocation inv4 = vm7.invokeAsync(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR1", "ln", 1, 100, isOffHeap() });
    
    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    inv1 = vm4.invokeAsync(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR2", "ln", 1, 100, isOffHeap() });
    inv2 = vm5.invokeAsync(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR2", "ln", 1, 100, isOffHeap() });
    inv3 = vm6.invokeAsync(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR2", "ln", 1, 100, isOffHeap() });
    inv4 = vm7.invokeAsync(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName()+"PR2", "ln", 1, 100, isOffHeap() });
    
    try {
      inv1.join();
      inv2.join();
      inv3.join();
      inv4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    
    LogWriterUtils.getLogWriter().info("Created back the partitioned regions");
    
    //start the senders in async mode. This will ensure that the 
    //node of shadow PR that went down last will come up first
    vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    LogWriterUtils.getLogWriter().info("All the senders are now running...");
    
    //----------------------------------------------------------------------------------------------------
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName()+"PR1", 3000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName()+"PR1", 3000 });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName()+"PR2", 5000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName()+"PR2", 5000 });
  }
  
  public static void validateParallelSenderQueueAllBucketsDrained(final String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    ConcurrentParallelGatewaySenderQueue regionQueue = (ConcurrentParallelGatewaySenderQueue)((AbstractGatewaySender)sender).getQueues().toArray(new RegionQueue[1])[0];
    
    Set<PartitionedRegion> shadowPRs = (Set<PartitionedRegion>)regionQueue.getRegions();
    
    for(PartitionedRegion shadowPR: shadowPRs) {
      Set<BucketRegion> buckets = shadowPR.getDataStore().getAllLocalBucketRegions();
      
      for (final BucketRegion bucket : buckets) {
        WaitCriterion wc = new WaitCriterion() {
          public boolean done() {
            if (bucket.keySet().size() == 0) {
              LogWriterUtils.getLogWriter().info("Bucket " + bucket.getId() + " is empty");
              return true;
            }
            return false;
          }
     
          public String description() {
            return "Expected bucket entries for bucket: " + bucket.getId() + " is: 0 but actual entries: " 
              + bucket.keySet().size() + " This bucket isPrimary: " + bucket.getBucketAdvisor().isPrimary() + " KEYSET: " + bucket.keySet();
          }
        };
        Wait.waitForCriterion(wc, 180000, 50, true);
      
      }//for loop ends
    }
    

  }
  
}
