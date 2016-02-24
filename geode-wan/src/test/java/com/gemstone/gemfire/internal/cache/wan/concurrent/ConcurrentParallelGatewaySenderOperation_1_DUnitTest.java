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
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY ));
    vm6.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY ));
    vm7.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    
    vm4.invoke(() -> WANTestBase.verifySenderStoppedState( "ln" ));
    vm5.invoke(() -> WANTestBase.verifySenderStoppedState( "ln" ));
    vm6.invoke(() -> WANTestBase.verifySenderStoppedState( "ln" ));
    vm7.invoke(() -> WANTestBase.verifySenderStoppedState( "ln" ));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 0 ));
  }
  
  /**
   * Defect 44323 (ParallelGatewaySender should not be started on Accessor node)
   */
  public void testParallelGatewaySenderStartOnAccessorNode() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY ));
    vm6.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY ));
    vm7.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegionAsAccessor(
        getTestMethodName() + "_PR", "ln", 1, 100 ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegionAsAccessor(
        getTestMethodName() + "_PR", "ln", 1, 100 ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    
    //start the senders
    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    vm7.invoke(() -> WANTestBase.startSender( "ln" ));
    
    Wait.pause(2000);
    
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));

    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000 ));
  }

  
  /**
   * Normal scenario in which the sender is paused in between.
   * @throws Exception
   */
  public void testParallelPropagationSenderPause() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY ));
    vm6.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY ));
    vm7.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    vm7.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    //make sure all the senders are running before doing any puts
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    //FIRST RUN: now, the senders are started. So, start the puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 100 ));
    
    //now, pause all of the senders
    vm4.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm5.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm6.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm7.invoke(() -> WANTestBase.pauseSender( "ln" ));
    Wait.pause(2000);
    //SECOND RUN: keep one thread doing puts to the region
    vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    
    //verify region size remains on remote vm and is restricted below a specified limit (i.e. number of puts in the first run)
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 100 ));
  }

  /**
   * Normal scenario in which a paused sender is resumed.
   * @throws Exception
   */
  public void testParallelPropagationSenderResume() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 8, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 8, OrderPolicy.KEY ));
    vm6.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 8, OrderPolicy.KEY ));
    vm7.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 8, OrderPolicy.KEY ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    vm7.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    //make sure all the senders are running before doing any puts
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    //now, the senders are started. So, start the puts
    vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    
    //now, pause all of the senders
    vm4.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm5.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm6.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm7.invoke(() -> WANTestBase.pauseSender( "ln" ));
    
    //sleep for a second or two
    Wait.pause(2000);
    
    //resume the senders
    vm4.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm5.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm6.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm7.invoke(() -> WANTestBase.resumeSender( "ln" ));
    
    Wait.pause(2000);
    
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    
    //find the region size on remote vm
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000 ));
 
  }
  
  /**
   * Negative scenario in which a sender that is stopped (and not paused) is resumed.
   * Expected: resume is only valid for pause. If a sender which is stopped is resumed,
   * it will not be started again.
   * 
   * @throws Exception
   */
  public void testParallelPropagationSenderResumeNegativeScenario() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
  
    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));

    //wait till the senders are running
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));

    //start the puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 100 ));

    //let the queue drain completely
    vm4.invoke(() -> WANTestBase.validateQueueContents( "ln", 0 ));
    
    //stop the senders
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm5.invoke(() -> WANTestBase.stopSender( "ln" ));
    
    //now, try to resume a stopped sender
    vm4.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm5.invoke(() -> WANTestBase.resumeSender( "ln" ));
    
    //do more puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    
    //validate region size on remote vm to contain only the events put in local site 
    //before the senders are stopped.
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100 ));
  }

  /**
   * Normal scenario in which a sender is stopped.
   * @throws Exception
   */
  public void testParallelPropagationSenderStop() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 3, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 3, OrderPolicy.KEY ));
    vm6.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 3, OrderPolicy.KEY ));
    vm7.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 3, OrderPolicy.KEY ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    vm7.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    //make sure all the senders are running before doing any puts
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 100 ));
    
    //now, stop all of the senders
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm5.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm6.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm7.invoke(() -> WANTestBase.stopSender( "ln" ));
    
    //SECOND RUN: keep one thread doing puts
    vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    
    //verify region size remains on remote vm and is restricted below a specified limit (number of puts in the first run)
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 100 ));
  }

  /**
   * Normal scenario in which a sender is stopped and then started again.
   */
  public void testParallelPropagationSenderStartAfterStop() throws Throwable {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY ));
    vm6.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY ));
    vm7.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    vm7.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    //make sure all the senders are running before doing any puts
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 200 ));
    
    //now, stop all of the senders
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm5.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm6.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm7.invoke(() -> WANTestBase.stopSender( "ln" ));
    
    Wait.pause(2000);

    //SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    
    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200 ));
    
    //start the senders again
    AsyncInvocation vm4start = vm4.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    AsyncInvocation vm5start = vm5.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    AsyncInvocation vm6start = vm6.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    AsyncInvocation vm7start = vm7.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    int START_TIMEOUT = 30000;
    vm4start.getResult(START_TIMEOUT);
    vm5start.getResult(START_TIMEOUT);
    vm6start.getResult(START_TIMEOUT);
    vm7start.getResult(START_TIMEOUT);

    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200 ));

    //SECOND RUN: do some more puts
    AsyncInvocation async = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    async.join();
    
    //verify all the buckets on all the sender nodes are drained
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    
    //verify the events propagate to remote site
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000 ));
    
    vm4.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 0 ));
    vm5.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 0 ));
    vm6.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 0 ));
    vm7.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 0 ));
  }

  /**
   * Normal scenario in which a sender is stopped and then started again.
   * Differs from above test case in the way that when the sender is starting from
   * stopped state, puts are simultaneously happening on the region by another thread.
   * @throws Exception
   */
  public void Bug47553_testParallelPropagationSenderStartAfterStop_Scenario2() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY ));
    vm6.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY ));
    vm7.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 7, OrderPolicy.KEY ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    vm7.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    //make sure all the senders are running before doing any puts
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    LogWriterUtils.getLogWriter().info("All the senders are now started");
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 200 ));
    
    LogWriterUtils.getLogWriter().info("Done few puts");
    
    //now, stop all of the senders
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm5.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm6.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm7.invoke(() -> WANTestBase.stopSender( "ln" ));
    
    LogWriterUtils.getLogWriter().info("All the senders are stopped");
    Wait.pause(2000);
    
    //SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    LogWriterUtils.getLogWriter().info("Done some more puts in second run");
    
    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200 ));
    
    //SECOND RUN: start async puts on region
    AsyncInvocation async = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 5000 ));
    LogWriterUtils.getLogWriter().info("Started high number of puts by async thread");

    LogWriterUtils.getLogWriter().info("Starting the senders at the same time");
    //when puts are happening by another thread, start the senders
    vm4.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    vm5.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    vm6.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    vm7.invokeAsync(() -> WANTestBase.startSender( "ln" ));

    LogWriterUtils.getLogWriter().info("All the senders are started");
    
    async.join();
        
    Wait.pause(2000);
    
    //verify all the buckets on all the sender nodes are drained
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    
    //verify that the queue size ultimately becomes zero. That means all the events propagate to remote site.
    vm4.invoke(() -> WANTestBase.validateQueueContents( "ln", 0 ));
  }
  
  /**
   * Normal scenario in which a sender is stopped and then started again on accessor node.
   * @throws Exception
   */
  public void testParallelPropagationSenderStartAfterStopOnAccessorNode() throws Throwable {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY ));
    vm6.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY ));
    vm7.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegionAsAccessor(
        getTestMethodName() + "_PR", "ln", 1, 100));
    vm7.invoke(() -> WANTestBase.createPartitionedRegionAsAccessor(
        getTestMethodName() + "_PR", "ln", 1, 100));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    vm7.invoke(() -> WANTestBase.startSender( "ln" ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    //make sure all the senders are not running on accessor nodes and running on non-accessor nodes
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 200 ));
    
    //now, stop all of the senders
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm5.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm6.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm7.invoke(() -> WANTestBase.stopSender( "ln" ));
    
    Wait.pause(2000);
    
    //SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    
    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200 ));
    
    //start the senders again
    AsyncInvocation vm4start = vm4.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    AsyncInvocation vm5start = vm5.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    AsyncInvocation vm6start = vm6.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    AsyncInvocation vm7start = vm7.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    int START_TIMEOUT = 30000;
    vm4start.getResult(START_TIMEOUT);
    vm5start.getResult(START_TIMEOUT);
    vm6start.getResult(START_TIMEOUT);
    vm7start.getResult(START_TIMEOUT);

    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200 ));

    //SECOND RUN: do some more puts
    AsyncInvocation async = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    async.join();
    Wait.pause(5000);
    
    //verify all buckets drained only on non-accessor nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    
    //verify the events propagate to remote site
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000 ));
  }

  
  /**
   * Normal scenario in which a combinations of start, pause, resume operations
   * is tested
   */
  public void testStartPauseResumeParallelGatewaySender() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));

    LogWriterUtils.getLogWriter().info("Created cache on local site");
    
    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY ));
    vm6.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY ));
    vm7.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY ));
    
    LogWriterUtils.getLogWriter().info("Created senders on local site");
    
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));

    LogWriterUtils.getLogWriter().info("Created PRs on local site");
    
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    LogWriterUtils.getLogWriter().info("Created PRs on remote site");
    
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    LogWriterUtils.getLogWriter().info("Done 1000 puts on local site");
    
    //Since puts are already done on userPR, it will have the buckets created. 
    //During sender start, it will wait until those buckets are created for shadowPR as well.
    //Start the senders in async threads, so colocation of shadowPR will be complete and 
    //missing buckets will be created in PRHARedundancyProvider.createMissingBuckets().
    vm4.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    vm5.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    vm6.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    vm7.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    LogWriterUtils.getLogWriter().info("Started senders on local site");
    
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 5000 ));
    LogWriterUtils.getLogWriter().info("Done 5000 puts on local site");
    
    vm4.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm5.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm6.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm7.invoke(() -> WANTestBase.pauseSender( "ln" ));
    LogWriterUtils.getLogWriter().info("Paused senders on local site");
    
    vm4.invoke(() -> WANTestBase.verifySenderPausedState( "ln" ));
    vm5.invoke(() -> WANTestBase.verifySenderPausedState( "ln" ));
    vm6.invoke(() -> WANTestBase.verifySenderPausedState( "ln" ));
    vm7.invoke(() -> WANTestBase.verifySenderPausedState( "ln" ));
    
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    LogWriterUtils.getLogWriter().info("Started 1000 async puts on local site");

    vm4.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm5.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm6.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm7.invoke(() -> WANTestBase.resumeSender( "ln" ));
    LogWriterUtils.getLogWriter().info("Resumed senders on local site");

    vm4.invoke(() -> WANTestBase.verifySenderResumedState( "ln" ));
    vm5.invoke(() -> WANTestBase.verifySenderResumedState( "ln" ));
    vm6.invoke(() -> WANTestBase.verifySenderResumedState( "ln" ));
    vm7.invoke(() -> WANTestBase.verifySenderResumedState( "ln" ));

    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail("Interrupted the async invocation.");
    }
    
    //verify all buckets drained on all sender nodes.
    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName() + "_PR", 5000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName() + "_PR", 5000 ));
  }
}
