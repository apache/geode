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

import java.util.concurrent.TimeUnit;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.MessageTooLargeException;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderException;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.junit.categories.FlakyTest;
import com.jayway.awaitility.Awaitility;

/**
 * DUnit test for operations on ParallelGatewaySender
 */
public class ParallelGatewaySenderOperationsDUnitTest extends WANTestBase {
  private static final long serialVersionUID = 1L;
  
  public ParallelGatewaySenderOperationsDUnitTest(String name) {
    super(name);
  }

  @Override
  protected final void postSetUpWANTestBase() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe||Unexpected IOException");
  }
  
  public void testParallelGatewaySenderWithoutStarting() {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, false);

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    
    vm4.invoke(() -> WANTestBase.verifySenderStoppedState( "ln" ));
    vm5.invoke(() -> WANTestBase.verifySenderStoppedState( "ln" ));
    vm6.invoke(() -> WANTestBase.verifySenderStoppedState( "ln" ));
    vm7.invoke(() -> WANTestBase.verifySenderStoppedState( "ln" ));
    
    validateRegionSizes(getTestMethodName() + "_PR", 0, vm2, vm3);
  }
  
  /**
   * Defect 44323 (ParallelGatewaySender should not be started on Accessor node)
   */
  public void testParallelGatewaySenderStartOnAccessorNode() {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, true, true);

    Wait.pause(2000);
    
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 10 ));

    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    validateRegionSizes(getTestMethodName() + "_PR", 10, vm2, vm3);
  }

  
  /**
   * Normal scenario in which the sender is paused in between.
   * @throws Exception
   */
  public void testParallelPropagationSenderPause() throws Exception {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    //make sure all the senders are running before doing any puts
    waitForSendersRunning();
    
    //FIRST RUN: now, the senders are started. So, start the puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 100 ));
    
    //now, pause all of the senders
    vm4.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm5.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm6.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm7.invoke(() -> WANTestBase.pauseSender( "ln" ));
    
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
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    //make sure all the senders are running before doing any puts
    waitForSendersRunning();
    
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

    validateParallelSenderQueueAllBucketsDrained();
    
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
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 1, 100, isOffHeap() ));

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    startSenderInVMs("ln", vm4, vm5);

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
    IgnoredException.addIgnoredException("Broken pipe");
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    //make sure all the senders are running before doing any puts
    waitForSendersRunning();
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 100 ));
    
    //now, stop all of the senders
    stopSenders();
    
    //SECOND RUN: keep one thread doing puts
    vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    
    //verify region size remains on remote vm and is restricted below a specified limit (number of puts in the first run)
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 100 ));
  }

  /**
   * Normal scenario in which a sender is stopped and then started again.
   * @throws Exception
   */
  @Category(FlakyTest.class) // GEODE-933: thread sleeps, random ports, async actions, time sensitive
  public void testParallelPropagationSenderStartAfterStop() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe");
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    //make sure all the senders are running before doing any puts
    waitForSendersRunning();
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 200 ));
    
    //now, stop all of the senders
    stopSenders();
    
    Wait.pause(2000);
    
    //SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    
    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200 ));
    
    //start the senders again
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200 ));

    //SECOND RUN: do some more puts
    AsyncInvocation async = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    async.join();
    
    Wait.pause(2000);
    
    //verify all the buckets on all the sender nodes are drained
    validateParallelSenderQueueAllBucketsDrained();
    
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
  public void testParallelPropagationSenderStartAfterStop_Scenario2() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe");
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    //make sure all the senders are running before doing any puts
    waitForSendersRunning();
    
    LogWriterUtils.getLogWriter().info("All the senders are now started");
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 200 ));
    
    LogWriterUtils.getLogWriter().info("Done few puts");
    
    //now, stop all of the senders
    stopSenders();
    
    LogWriterUtils.getLogWriter().info("All the senders are stopped");
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200, 120000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 200, 120000));
    
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
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("All the senders are started");
    
    async.join();

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 5000, 120000));
    vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 5000, 120000));
    
    //verify all the buckets on all the sender nodes are drained
    validateParallelSenderQueueAllBucketsDrained();
    
    //verify that the queue size ultimately becomes zero. That means all the events propagate to remote site.
    vm4.invoke(() -> WANTestBase.validateQueueContents( "ln", 0 ));
  }
  
  /**
   * Normal scenario in which a sender is stopped and then started again on accessor node.
   * @throws Exception
   */
  public void testParallelPropagationSenderStartAfterStopOnAccessorNode() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, true, true);

    //make sure all the senders are not running on accessor nodes and running on non-accessor nodes
    waitForSendersRunning();
    
    //FIRST RUN: now, the senders are started. So, do some of the puts
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 200 ));
    
    //now, stop all of the senders
    stopSenders();
    
    Wait.pause(2000);
    
    //SECOND RUN: do some of the puts after the senders are stopped
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    
    //Region size on remote site should remain same and below the number of puts done in the FIRST RUN
    vm2.invoke(() -> WANTestBase.validateRegionSizeRemainsSame(getTestMethodName() + "_PR", 200 ));
    
    //start the senders again
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

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
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    LogWriterUtils.getLogWriter().info("Done 1000 puts on local site");
    
    //Since puts are already done on userPR, it will have the buckets created. 
    //During sender start, it will wait until those buckets are created for shadowPR as well.
    //Start the senders in async threads, so colocation of shadowPR will be complete and 
    //missing buckets will be created in PRHARedundancyProvider.createMissingBuckets().
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    waitForSendersRunning();
    
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
    validateParallelSenderQueueAllBucketsDrained();

    validateRegionSizes(getTestMethodName() + "_PR", 5000, vm2, vm3);
  }

  /**
   * Since the sender is attached to a region and in use, it can not be
   * destroyed. Hence, exception is thrown by the sender API.
   */
  public void testDestroyParallelGatewaySenderExceptionScenario() {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    // make sure all the senders are running before doing any puts
    waitForSendersRunning();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR",
        1000 ));
    
    // try destroying on couple of nodes
    try {
      vm4.invoke(() -> WANTestBase.destroySender( "ln" ));
    }
    catch (RMIException e) {
      assertTrue("Cause of the exception should be GatewaySenderException", e
          .getCause() instanceof GatewaySenderException);
    }
    try {
      vm5.invoke(() -> WANTestBase.destroySender( "ln" ));
    }
    catch (RMIException e) {
      assertTrue("Cause of the exception should be GatewaySenderException", e
          .getCause() instanceof GatewaySenderException);
    }

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_PR", 1000 ));
  }

  public void testDestroyParallelGatewaySender() {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    createSendersReceiversAndPartitionedRegion(lnPort, nyPort, false, true);

    // make sure all the senders are running
    waitForSendersRunning();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR",
        1000 ));
    
    Wait.pause(2000);
    
    //stop the sender and remove from region before calling destroy on it
    stopSenders();

    vm4.invoke(() -> WANTestBase.removeSenderFromTheRegion(
        "ln", getTestMethodName() + "_PR" ));
    vm5.invoke(() -> WANTestBase.removeSenderFromTheRegion(
        "ln", getTestMethodName() + "_PR" ));
    vm6.invoke(() -> WANTestBase.removeSenderFromTheRegion(
        "ln", getTestMethodName() + "_PR" ));
    vm7.invoke(() -> WANTestBase.removeSenderFromTheRegion(
        "ln", getTestMethodName() + "_PR" ));

    vm4.invoke(() -> WANTestBase.destroySender( "ln" ));
    vm5.invoke(() -> WANTestBase.destroySender( "ln" ));
    vm6.invoke(() -> WANTestBase.destroySender( "ln" ));
    vm7.invoke(() -> WANTestBase.destroySender( "ln" ));

    vm4.invoke(() -> WANTestBase.verifySenderDestroyed( "ln", true ));
    vm5.invoke(() -> WANTestBase.verifySenderDestroyed( "ln", true ));
    vm6.invoke(() -> WANTestBase.verifySenderDestroyed( "ln", true ));
    vm7.invoke(() -> WANTestBase.verifySenderDestroyed( "ln", true ));
  }

  public void testParallelGatewaySenderMessageTooLargeException() {
    Integer[] locatorPorts = createLNAndNYLocators();
    Integer lnPort = locatorPorts[0];
    Integer nyPort = locatorPorts[1];

    // Create and start sender with reduced maximum message size and 1 dispatcher thread
    String regionName = getTestMethodName() + "_PR";
    vm4.invoke(() -> setMaximumMessageSize( 1024*1024 ));
    vm4.invoke(() -> createCache( lnPort ));
    vm4.invoke(() -> setNumDispatcherThreadsForTheRun( 1 ));
    vm4.invoke(() -> createSender( "ln", 2, true, 100, 100, false, false, null, false ));
    vm4.invoke(() -> createPartitionedRegion( regionName, "ln", 0, 100, isOffHeap() ));

    // Do puts
    int numPuts = 200;
    vm4.invoke(() -> doPuts( regionName, numPuts, new byte[11000] ));
    validateRegionSizes(regionName, numPuts, vm4);

    // Start receiver
    IgnoredException ignoredMTLE = IgnoredException.addIgnoredException(MessageTooLargeException.class.getName(), vm4);
    IgnoredException ignoredGIOE = IgnoredException.addIgnoredException(GemFireIOException.class.getName(), vm4);
    vm2.invoke(() -> createCache( nyPort ));
    vm2.invoke(() -> createPartitionedRegion( regionName, null, 0, 100, isOffHeap() ));
    vm2.invoke(() -> createReceiver());
    validateRegionSizes( regionName, numPuts, vm2 );

    vm4.invoke(() -> {
      final AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      assertTrue(sender.getStatistics().getBatchesResized() > 0);
    });
    ignoredMTLE.remove();
    ignoredGIOE.remove();
  }

  private void setMaximumMessageSize(int maximumMessageSizeBytes) {
    Message.MAX_MESSAGE_SIZE = maximumMessageSizeBytes;
    LogWriterUtils.getLogWriter().info("Set gemfire.client.max-message-size: " + System.getProperty("gemfire.client.max-message-size"));
  }

  private void createSendersReceiversAndPartitionedRegion(Integer lnPort, Integer nyPort, boolean createAccessors,
      boolean startSenders) {
    // Note: This is a test-specific method used by several test to create
    // receivers, senders and partitioned regions.
    createSendersAndReceivers(lnPort, nyPort);

    createPartitionedRegions(createAccessors);

    if (startSenders) {
      startSenderInVMs("ln", vm4, vm5, vm6, vm7);
    }
  }

  private void createSendersAndReceivers(Integer lnPort, Integer nyPort) {
    // Note: This is a test-specific method used by several test to create
    // receivers and senders.
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, true));
  }

  private void createPartitionedRegions(boolean createAccessors) {
    // Note: This is a test-specific method used by several test to create
    // partitioned regions.
    String regionName = getTestMethodName() + "_PR";
    vm4.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));

    if (createAccessors) {
      vm6.invoke(() -> createPartitionedRegionAsAccessor(regionName, "ln", 1, 100));
      vm7.invoke(() -> createPartitionedRegionAsAccessor(regionName, "ln", 1, 100));
    } else {
      vm6.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
      vm7.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    }

    vm2.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
    vm3.invoke(() -> createPartitionedRegion(regionName, "ln", 1, 100, isOffHeap()));
  }

  private void stopSenders() {
    vm4.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> stopSender("ln"));
    vm6.invoke(() -> stopSender("ln"));
    vm7.invoke(() -> stopSender("ln"));
  }

  private void waitForSendersRunning() {
    vm4.invoke(() -> waitForSenderRunningState("ln"));
    vm5.invoke(() -> waitForSenderRunningState("ln"));
    vm6.invoke(() -> waitForSenderRunningState("ln"));
    vm7.invoke(() -> waitForSenderRunningState("ln"));
  }

  private void validateParallelSenderQueueAllBucketsDrained() {
    vm4.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));
    vm5.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));
    vm6.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> validateParallelSenderQueueAllBucketsDrained("ln"));
  }
}
