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

import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.Wait;

/**
 * @author skumar
 *
 */
public class ConcurrentParallelGatewaySenderOperation_1_DUnitTest extends WANTestBase {
  private static final long serialVersionUID = 1L;
  
  public ConcurrentParallelGatewaySenderOperation_1_DUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
  }
  
  public void testParallelGatewaySenderWithoutStarting() {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });
    
    vm4.invoke(WANTestBase.class, "verifySenderStoppedState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "verifySenderStoppedState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "verifySenderStoppedState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "verifySenderStoppedState", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {getTestMethodName() + "_PR", 0 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {getTestMethodName() + "_PR", 0 });
  }
  
  /**
   * Defect 44323 (ParallelGatewaySender should not be started on Accessor node)
   */
  public void testParallelGatewaySenderStartOnAccessorNode() {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionAsAccessor", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100 });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionAsAccessor", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100 });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    
    //start the senders
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    Wait.pause(2000);
    
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });

    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {getTestMethodName() + "_PR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {getTestMethodName() + "_PR", 1000 });
  }

  
  /**
   * Normal scenario in which the sender is paused in between.
   * @throws Exception
   */
  public void testParallelPropagationSenderPause() throws Exception {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //FIRST RUN: now, the senders are started. So, start the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 100 });
    
    //now, pause all of the senders
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    Wait.pause(2000);
    //SECOND RUN: keep one thread doing puts to the region
    vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });
    
    //verify region size remains on remote vm and is restricted below a specified limit (i.e. number of puts in the first run)
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {getTestMethodName() + "_PR", 100 });
  }

  /**
   * Normal scenario in which a paused sender is resumed.
   * @throws Exception
   */
  public void testParallelPropagationSenderResume() throws Exception {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 8, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 8, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 8, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 8, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //now, the senders are started. So, start the puts
    vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });
    
    //now, pause all of the senders
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    //sleep for a second or two
    Wait.pause(2000);
    
    //resume the senders
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    Wait.pause(2000);
    
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    //find the region size on remote vm
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {getTestMethodName() + "_PR", 1000 });
 
  }
  
  /**
   * Negative scenario in which a sender that is stopped (and not paused) is resumed.
   * Expected: resume is only valid for pause. If a sender which is stopped is resumed,
   * it will not be started again.
   * 
   * @throws Exception
   */
  public void testParallelPropagationSenderResumeNegativeScenario() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
  
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //wait till the senders are running
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });

    //start the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 100 });

    //let the queue drain completely
    vm4.invoke(WANTestBase.class, "validateQueueContents", new Object[] { "ln", 0 });
    
    //stop the senders
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    
    //now, try to resume a stopped sender
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    //do more puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });
    
    //validate region size on remote vm to contain only the events put in local site 
    //before the senders are stopped.
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {getTestMethodName() + "_PR", 100 });
  }

  /**
   * Normal scenario in which a sender is stopped.
   * @throws Exception
   */
  public void testParallelPropagationSenderStop() throws Exception {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 3, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 3, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 3, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 3, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 100 });
    
    //now, stop all of the senders
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    
    //SECOND RUN: keep one thread doing puts
    vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });
    
    //verify region size remains on remote vm and is restricted below a specified limit (number of puts in the first run)
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {getTestMethodName() + "_PR", 100 });
  }

  /**
   * Normal scenario in which a sender is stopped and then started again.
   */
  public void testParallelPropagationSenderStartAfterStop() throws Throwable {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 200 });
    
    //now, stop all of the senders
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    
    Wait.pause(2000);

    //SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });
    
    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {getTestMethodName() + "_PR", 200 });
    
    //start the senders again
    AsyncInvocation vm4start = vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation vm5start = vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation vm6start = vm6.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation vm7start = vm7.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    int START_TIMEOUT = 30000;
    vm4start.getResult(START_TIMEOUT);
    vm5start.getResult(START_TIMEOUT);
    vm6start.getResult(START_TIMEOUT);
    vm7start.getResult(START_TIMEOUT);

    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {getTestMethodName() + "_PR", 200 });

    //SECOND RUN: do some more puts
    AsyncInvocation async = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });
    async.join();
    
    //verify all the buckets on all the sender nodes are drained
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    //verify the events propagate to remote site
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {getTestMethodName() + "_PR", 1000 });
    
    vm4.invoke(WANTestBase.class, "validateQueueSizeStat", new Object[] { "ln", 0 });
    vm5.invoke(WANTestBase.class, "validateQueueSizeStat", new Object[] { "ln", 0 });
    vm6.invoke(WANTestBase.class, "validateQueueSizeStat", new Object[] { "ln", 0 });
    vm7.invoke(WANTestBase.class, "validateQueueSizeStat", new Object[] { "ln", 0 });
  }

  /**
   * Normal scenario in which a sender is stopped and then started again.
   * Differs from above test case in the way that when the sender is starting from
   * stopped state, puts are simultaneously happening on the region by another thread.
   * @throws Exception
   */
  public void Bug47553_testParallelPropagationSenderStartAfterStop_Scenario2() throws Exception {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    LogWriterUtils.getLogWriter().info("All the senders are now started");
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 200 });
    
    LogWriterUtils.getLogWriter().info("Done few puts");
    
    //now, stop all of the senders
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    
    LogWriterUtils.getLogWriter().info("All the senders are stopped");
    Wait.pause(2000);
    
    //SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });
    LogWriterUtils.getLogWriter().info("Done some more puts in second run");
    
    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {getTestMethodName() + "_PR", 200 });
    
    //SECOND RUN: start async puts on region
    AsyncInvocation async = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 5000 });
    LogWriterUtils.getLogWriter().info("Started high number of puts by async thread");

    LogWriterUtils.getLogWriter().info("Starting the senders at the same time");
    //when puts are happening by another thread, start the senders
    vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });

    LogWriterUtils.getLogWriter().info("All the senders are started");
    
    async.join();
        
    Wait.pause(2000);
    
    //verify all the buckets on all the sender nodes are drained
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    //verify that the queue size ultimately becomes zero. That means all the events propagate to remote site.
    vm4.invoke(WANTestBase.class, "validateQueueContents", new Object[] { "ln", 0 });
  }
  
  /**
   * Normal scenario in which a sender is stopped and then started again on accessor node.
   * @throws Exception
   */
  public void testParallelPropagationSenderStartAfterStopOnAccessorNode() throws Throwable {
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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionAsAccessor", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100});
    vm7.invoke(WANTestBase.class, "createPartitionedRegionAsAccessor", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100});

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are not running on accessor nodes and running on non-accessor nodes
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 200 });
    
    //now, stop all of the senders
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    
    Wait.pause(2000);
    
    //SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });
    
    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {getTestMethodName() + "_PR", 200 });
    
    //start the senders again
    AsyncInvocation vm4start = vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation vm5start = vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation vm6start = vm6.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation vm7start = vm7.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    int START_TIMEOUT = 30000;
    vm4start.getResult(START_TIMEOUT);
    vm5start.getResult(START_TIMEOUT);
    vm6start.getResult(START_TIMEOUT);
    vm7start.getResult(START_TIMEOUT);

    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {getTestMethodName() + "_PR", 200 });

    //SECOND RUN: do some more puts
    AsyncInvocation async = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });
    async.join();
    Wait.pause(5000);
    
    //verify all buckets drained only on non-accessor nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    //verify the events propagate to remote site
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {getTestMethodName() + "_PR", 1000 });
  }

  
  /**
   * Normal scenario in which a combinations of start, pause, resume operations
   * is tested
   */
  public void testStartPauseResumeParallelGatewaySender() throws Exception {
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

    LogWriterUtils.getLogWriter().info("Created cache on local site");
    
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
    vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
    vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
    
    LogWriterUtils.getLogWriter().info("Created senders on local site");
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() });

    LogWriterUtils.getLogWriter().info("Created PRs on local site");
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() });
    LogWriterUtils.getLogWriter().info("Created PRs on remote site");
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 1000 });
    LogWriterUtils.getLogWriter().info("Done 1000 puts on local site");
    
    //Since puts are already done on userPR, it will have the buckets created. 
    //During sender start, it will wait until those buckets are created for shadowPR as well.
    //Start the senders in async threads, so colocation of shadowPR will be complete and 
    //missing buckets will be created in PRHARedundancyProvider.createMissingBuckets().
    vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    LogWriterUtils.getLogWriter().info("Started senders on local site");
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR", 5000 });
    LogWriterUtils.getLogWriter().info("Done 5000 puts on local site");
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    LogWriterUtils.getLogWriter().info("Paused senders on local site");
    
    vm4.invoke(WANTestBase.class, "verifySenderPausedState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "verifySenderPausedState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "verifySenderPausedState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "verifySenderPausedState", new Object[] { "ln" });
    
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_PR", 1000 });
    LogWriterUtils.getLogWriter().info("Started 1000 async puts on local site");

    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    LogWriterUtils.getLogWriter().info("Resumed senders on local site");

    vm4.invoke(WANTestBase.class, "verifySenderResumedState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "verifySenderResumedState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "verifySenderResumedState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "verifySenderResumedState", new Object[] { "ln" });

    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail("Interrupted the async invocation.");
    }
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName() + "_PR", 5000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName() + "_PR", 5000 });
  }
}
