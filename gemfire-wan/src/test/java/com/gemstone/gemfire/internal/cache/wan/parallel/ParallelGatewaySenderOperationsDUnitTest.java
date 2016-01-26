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
package com.gemstone.gemfire.internal.cache.wan.parallel;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderException;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.ExpectedException;

/**
 * DUnit test for operations on ParallelGatewaySender
 * 
 * @author pdeole
 *
 */
public class ParallelGatewaySenderOperationsDUnitTest extends WANTestBase {
  private static final long serialVersionUID = 1L;
  
  public ParallelGatewaySenderOperationsDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    addExpectedException("Broken pipe||Unexpected IOException");
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

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    
    vm4.invoke(WANTestBase.class, "verifySenderStoppedState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "verifySenderStoppedState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "verifySenderStoppedState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "verifySenderStoppedState", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {testName + "_PR", 0 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {testName + "_PR", 0 });
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

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionAsAccessor", new Object[] {
        testName + "_PR", "ln", 1, 100 });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionAsAccessor", new Object[] {
        testName + "_PR", "ln", 1, 100 });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    
    //start the senders
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    pause(2000);
    
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });

    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {testName + "_PR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {testName + "_PR", 1000 });
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

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //FIRST RUN: now, the senders are started. So, start the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 100 });
    
    //now, pause all of the senders
    vm4.invoke(WANTestBase.class, "pauseSenderAndWaitForDispatcherToPause", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSenderAndWaitForDispatcherToPause", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSenderAndWaitForDispatcherToPause", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSenderAndWaitForDispatcherToPause", new Object[] { "ln" });
    
    //SECOND RUN: keep one thread doing puts to the region
    vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    
    //verify region size remains on remote vm and is restricted below a specified limit (i.e. number of puts in the first run)
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {testName + "_PR", 100 });
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

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //now, the senders are started. So, start the puts
    vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    
    //now, pause all of the senders
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    //sleep for a second or two
    pause(2000);
    
    //resume the senders
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    pause(2000);
    
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    //find the region size on remote vm
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {testName + "_PR", 1000 });
 
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

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 1, 100, isOffHeap() });
  
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //wait till the senders are running
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });

    //start the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 100 });

    //let the queue drain completely
    vm4.invoke(WANTestBase.class, "validateQueueContents", new Object[] { "ln", 0 });
    
    //stop the senders
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    
    //now, try to resume a stopped sender
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    //do more puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    
    //validate region size on remote vm to contain only the events put in local site 
    //before the senders are stopped.
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {testName + "_PR", 100 });
  }

  /**
   * Normal scenario in which a sender is stopped.
   * @throws Exception
   */
  public void testParallelPropagationSenderStop() throws Exception {
    addExpectedException("Broken pipe");
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
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 100 });
    
    //now, stop all of the senders
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    
    //SECOND RUN: keep one thread doing puts
    vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    
    //verify region size remains on remote vm and is restricted below a specified limit (number of puts in the first run)
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {testName + "_PR", 100 });
  }

  /**
   * Normal scenario in which a sender is stopped and then started again.
   * @throws Exception
   */
  public void testParallelPropagationSenderStartAfterStop() throws Exception {
    addExpectedException("Broken pipe");
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
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 200 });
    
    //now, stop all of the senders
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    
    pause(2000);
    
    //SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    
    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {testName + "_PR", 200 });
    
    //start the senders again
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {testName + "_PR", 200 });

    //SECOND RUN: do some more puts
    AsyncInvocation async = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    async.join();
    
    pause(2000);
    
    //verify all the buckets on all the sender nodes are drained
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm6.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm7.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    //verify the events propagate to remote site
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {testName + "_PR", 1000 });
    
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
  public void testParallelPropagationSenderStartAfterStop_Scenario2() throws Exception {
    addExpectedException("Broken pipe");
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
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    getLogWriter().info("All the senders are now started");
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 200 });
    
    getLogWriter().info("Done few puts");
    
    //now, stop all of the senders
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    
    getLogWriter().info("All the senders are stopped");
    pause(2000);
    
    //SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    getLogWriter().info("Done some more puts in second run");
    
    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {testName + "_PR", 200 });
    
    //SECOND RUN: start async puts on region
    AsyncInvocation async = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 5000 });
    getLogWriter().info("Started high number of puts by async thread");

    getLogWriter().info("Starting the senders at the same time");
    //when puts are happening by another thread, start the senders
    vm4.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });

    getLogWriter().info("All the senders are started");
    
    async.join();
        
    pause(2000);
    
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
  public void testParallelPropagationSenderStartAfterStopOnAccessorNode() throws Exception {
    addExpectedException("Broken pipe");
    addExpectedException("Connection reset");
    addExpectedException("Unexpected IOException");
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
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegionAsAccessor", new Object[] {
        testName + "_PR", "ln", 1, 100 });
    vm7.invoke(WANTestBase.class, "createPartitionedRegionAsAccessor", new Object[] {
        testName + "_PR", "ln", 1, 100 });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    //make sure all the senders are not running on accessor nodes and running on non-accessor nodes
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 200 });
    
    //now, stop all of the senders
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    
    pause(2000);
    
    //SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    
    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {testName + "_PR", 200 });
    
    //start the senders again
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(WANTestBase.class, "validateRegionSizeRemainsSame", new Object[] {testName + "_PR", 200 });

    //SECOND RUN: do some more puts
    AsyncInvocation async = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    async.join();
    pause(5000);
    
    //verify all buckets drained only on non-accessor nodes.
    vm4.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    vm5.invoke(WANTestBase.class, "validateParallelSenderQueueAllBucketsDrained", new Object[] {"ln"});
    
    //verify the events propagate to remote site
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {testName + "_PR", 1000 });
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

    getLogWriter().info("Created cache on local site");
    
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    
    getLogWriter().info("Created senders on local site");
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    getLogWriter().info("Created PRs on local site");
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    getLogWriter().info("Created PRs on remote site");
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    getLogWriter().info("Done 1000 puts on local site");
    
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
    
    getLogWriter().info("Started senders on local site");
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 5000 });
    getLogWriter().info("Done 5000 puts on local site");
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    getLogWriter().info("Paused senders on local site");
    
    vm4.invoke(WANTestBase.class, "verifySenderPausedState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "verifySenderPausedState", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "verifySenderPausedState", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "verifySenderPausedState", new Object[] { "ln" });
    
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { testName + "_PR", 1000 });
    getLogWriter().info("Started 1000 async puts on local site");

    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    getLogWriter().info("Resumed senders on local site");

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
      testName + "_PR", 5000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 5000 });
  }
  
  // to test that when userPR is locally destroyed, shadow Pr is also locally
  // destroyed and on recreation usrePr , shadow Pr is also recreated.
  public void testParallelGatewaySender_SingleNode_UserPR_localDestroy_RecreateRegion() throws Exception {
    addExpectedException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    try {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, false });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      // since sender is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "localDestroyRegion",
          new Object[] { testName + "_PR" });

      // since shodowPR is locally destroyed, so no data to dispatch
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });
    } finally {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
        "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_RecreateRegion() throws Exception {
    addExpectedException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    try {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, false });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      // since resume is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "destroyRegion", new Object[] { testName
          + "_PR" });

      // before destoy, there is wait for queue to drain, so data will be
      // dispatched
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 20 });

    } finally {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  
  public void testParallelGatewaySender_SingleNode_UserPR_Close_RecreateRegion() throws Exception {
    ExpectedException exp = addExpectedException(RegionDestroyedException.class
        .getName());
    try {
      Integer lnPort = (Integer) vm0.invoke(WANTestBase.class,
          "createFirstLocatorWithDSId", new Object[] { 1 });
      Integer nyPort = (Integer) vm1.invoke(WANTestBase.class,
          "createFirstRemoteLocator", new Object[] { 2, lnPort });

      vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
      try {
        vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
            "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
            new Object[] { lnPort });

        getLogWriter().info("Created cache on local site");

        getLogWriter().info("Created senders on local site");

        vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
            true, 100, 10, false, false, null, false });

        vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

        vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

        vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
            testName + "_PR", "ln", 1, 10, isOffHeap() });

        getLogWriter().info("Created PRs on local site");

        vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
            testName + "_PR", null, 1, 10, isOffHeap() });

        vm4.invoke(WANTestBase.class, "doPuts", new Object[] {
            testName + "_PR", 10 });

        vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
            testName + "_PR", 10 });

        // since resume is paused, no dispatching
        vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
            testName + "_PR", 0 });

        vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
            "closeRegion", new Object[] { testName + "_PR" });

        vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

        pause(500); // paused if there is any element which is received on
                    // remote
                    // site

        // before close, there is wait for queue to drain, so data will be
        // dispatched
        vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
            testName + "_PR", 0 });

        vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
            testName + "_PR", "ln", 1, 10, isOffHeap() });

        vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
            testName + "_PR", 10, 20 });

        vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
            testName + "_PR", 10 });

        vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
            testName + "_PR", 10 });
      } finally {
        vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
            "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      }
    } finally {
      exp.remove();
    }
  }
  
  //to test that while localDestroy is in progress, put operation does not successed
  public void testParallelGatewaySender_SingleNode_UserPR_localDestroy_SimultenuousPut_RecreateRegion() throws Exception {
    addExpectedException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    try {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      getLogWriter().info("Created cache on local site");

      getLogWriter().info("Created senders on local site");

      vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, false });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      // since resume is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

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

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

      pause(500); // paused if there is any element which is received on remote
                  // site

      // since shodowPR is locally destroyed, so no data to dispatch
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

    } finally {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_SimultenuousPut_RecreateRegion() throws Exception {
    addExpectedException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    try {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, false });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "addCacheListenerAndDestroyRegion", new Object[] {
        testName + "_PR"});
      
      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      // since resume is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

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
      vm2.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "validateRegionSizeWithinRange", new Object[] { testName + "_PR", 10,
              101 }); // possible size is more than 10

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm2.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "validateRegionSizeWithinRange", new Object[] { testName + "_PR", 20,
              101 });// possible size is more than 20

    } finally {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_NodeDown() throws Exception {
    addExpectedException("RegionDestroyedException");
    addExpectedException("Connection reset");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    try {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm5.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm6.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, false });
      vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, false });
      vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, false });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
      vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
      vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });
      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });
      vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10000 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10000 });

      // since resume is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

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

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10000 });// possible size is more than 20

    } finally {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm5.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm6.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Close_SimultenuousPut_RecreateRegion() throws Exception {
    addExpectedException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    try {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      getLogWriter().info("Created cache on local site");

      getLogWriter().info("Created senders on local site");

      vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, false });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      // since resume is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      AsyncInvocation putAsync = vm4.invokeAsync(WANTestBase.class,
          "doPutsFrom", new Object[] { testName + "_PR", 10, 2000 });
      AsyncInvocation localDestroyAsync = vm4.invokeAsync(
          ParallelGatewaySenderOperationsDUnitTest.class, "closeRegion",
          new Object[] { testName + "_PR" });
      try {
        putAsync.join();
        localDestroyAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });
    } finally {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySenders_SingleNode_UserPR_localDestroy_RecreateRegion()
      throws Exception {
    addExpectedException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });
    Integer pnPort = (Integer)vm3.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 4, lnPort });

    vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    vm6.invoke(WANTestBase.class, "createReceiver", new Object[] { pnPort });

    try {
      vm7.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      getLogWriter().info("Created cache on local site");

      vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
          true, 100, 10, false, false, null, true });

      vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
          true, 100, 10, false, false, null, true });

      vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln3", 4,
          true, 100, 10, false, false, null, true });

      vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
      vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
      vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln3" });

      vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln1,ln2,ln3", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });
      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });
      vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm7.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });
      
      vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 10 });
      vm7.invoke(WANTestBase.class, "validateQueueContents",
          new Object[] { "ln1", 0 });
      vm7.invoke(WANTestBase.class, "validateQueueContents",
          new Object[] { "ln2", 0 });
      vm7.invoke(WANTestBase.class, "validateQueueContents",
          new Object[] { "ln3", 0 });
      
      vm7.invoke(WANTestBase.class, "localDestroyRegion",
          new Object[] { testName + "_PR" });

      vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln1,ln2,ln3", 1, 10, isOffHeap() });

      vm7.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm7.invoke(WANTestBase.class, "validateQueueContents",
          new Object[] { "ln1", 0 });
      vm7.invoke(WANTestBase.class, "validateQueueContents",
          new Object[] { "ln2", 0 });
      vm7.invoke(WANTestBase.class, "validateQueueContents",
          new Object[] { "ln3", 0 });
      
      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 20 });
      vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 20 });
      vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 20 });
    } finally {
      vm7.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySender_MultipleNode_UserPR_localDestroy_Recreate() throws Exception {
    addExpectedException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    try {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm5.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 100, isOffHeap() });
      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, true });
      vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, true });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 100, isOffHeap() });

      AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
          new Object[] { testName + "_PR", 1000 });
      pause(1000);
      vm5.invoke(WANTestBase.class, "localDestroyRegion",
          new Object[] { testName + "_PR" });

      try {
        inv1.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 1000 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 1000 });

      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 1000, 2000 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 2000 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 2000 });
    } finally {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm5.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySenders_MultiplNode_UserPR_localDestroy_Recreate()
      throws Exception {
    addExpectedException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    vm6.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm7.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    try {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm5.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln1,ln2", 1, 100, isOffHeap() });
      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln1,ln2", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
          true, 100, 10, false, false, null, true });
      vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
          true, 100, 10, false, false, null, true });
      vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
          true, 100, 10, false, false, null, true });
      vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
          true, 100, 10, false, false, null, true });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });

      vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 100, isOffHeap() });
      vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 100, isOffHeap() });

      AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
          new Object[] { testName + "_PR", 1000 });
      pause(1000);
      vm5.invoke(WANTestBase.class, "localDestroyRegion",
          new Object[] { testName + "_PR" });

      try {
        inv1.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 1000 });

      vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 1000 });
      vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 1000 });

      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln1,ln2", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 1000, 2000 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 2000 });

      vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 2000 });
      vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 2000 });
    } finally {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm5.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySender_ColocatedPartitionedRegions_localDestroy() throws Exception {
    addExpectedException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    try {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm5.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              "ln", 1, 100, isOffHeap() });
      vm5.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              "ln", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, true });
      vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, true });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              null, 1, 100, isOffHeap() });

      AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class,
          "putcolocatedPartitionedRegion", new Object[] { 2000 });
      pause(1000);

      try {
        vm5.invoke(WANTestBase.class, "localDestroyRegion",
            new Object[] { customerRegionName });
      } catch (Exception ex) {
        assertTrue(ex.getCause() instanceof UnsupportedOperationException);
      }

      try {
        inv1.join();
      } catch (Exception e) {
        fail("Unexpected exception", e);
      }

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          customerRegionName, 2000 });
      vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          customerRegionName, 2000 });
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          customerRegionName, 2000 });
    } finally {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm5.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
    
  }
  
  public void testParallelGatewaySender_ColocatedPartitionedRegions_destroy() throws Exception {
    addExpectedException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    try {
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm5.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              "ln", 1, 100, isOffHeap() });
      vm5.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              "ln", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, true });
      vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
          true, 100, 10, false, false, null, true });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

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
      vm4.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm5.invoke(ParallelGatewaySenderOperationsDUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  /**
   * Since the sender is attached to a region and in use, it can not be
   * destroyed. Hence, exception is thrown by the sender API.
   */
  public void testDestroyParallelGatewaySenderExceptionScenario() {
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

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2, true,
        100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2, true,
        100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2, true,
        100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2, true,
        100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    // make sure all the senders are running before doing any puts
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
    // try destroying on couple of nodes
    try {
      vm4.invoke(WANTestBase.class, "destroySender", new Object[] { "ln" });
    }
    catch (RMIException e) {
      assertTrue("Cause of the exception should be GatewaySenderException", e
          .getCause() instanceof GatewaySenderException);
    }
    try {
      vm5.invoke(WANTestBase.class, "destroySender", new Object[] { "ln" });
    }
    catch (RMIException e) {
      assertTrue("Cause of the exception should be GatewaySenderException", e
          .getCause() instanceof GatewaySenderException);
    }

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }

  public void testDestroyParallelGatewaySender() {
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

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2, true,
        100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2, true,
        100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2, true,
        100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2, true,
        100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    // make sure all the senders are running
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
    pause(2000);
    
    //stop the sender and remove from region before calling destroy on it
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "removeSenderFromTheRegion", new Object[] {
        "ln", testName + "_PR" });
    vm5.invoke(WANTestBase.class, "removeSenderFromTheRegion", new Object[] {
        "ln", testName + "_PR" });
    vm6.invoke(WANTestBase.class, "removeSenderFromTheRegion", new Object[] {
        "ln", testName + "_PR" });
    vm7.invoke(WANTestBase.class, "removeSenderFromTheRegion", new Object[] {
        "ln", testName + "_PR" });

    vm4.invoke(WANTestBase.class, "destroySender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "destroySender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "destroySender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "destroySender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "verifySenderDestroyed",
        new Object[] { "ln", true });
    vm5.invoke(WANTestBase.class, "verifySenderDestroyed",
        new Object[] { "ln", true });
    vm6.invoke(WANTestBase.class, "verifySenderDestroyed",
        new Object[] { "ln", true });
    vm7.invoke(WANTestBase.class, "verifySenderDestroyed",
        new Object[] { "ln", true });
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
