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
package com.gemstone.gemfire.internal.cache.wan.serial;

import java.util.HashSet;
import java.util.Set;

import org.junit.Ignore;

import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionResponse;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.ServerLocator;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderException;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author skumar
 * 
 */
public class SerialGatewaySenderOperationsDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public SerialGatewaySenderOperationsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
    IgnoredException.addIgnoredException("Connection refused");
    IgnoredException.addIgnoredException("could not get remote locator information");
    
    //Stopping the gateway closed the region,
    //which causes this exception to get logged
    IgnoredException.addIgnoredException(RegionDestroyedException.class.getSimpleName());
  }

  public void testSerialGatewaySenderOperationsWithoutStarting() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createReceivers(nyPort);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        10 ));
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        100 ));

    vm4.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifyGatewaySenderOperations( "ln" ));
    vm5.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifyGatewaySenderOperations( "ln" ));

  }

  protected void createSenderRegions() {
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
  }

  protected void createReceiverRegions() {
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));
  }

  protected void createSenderCaches(Integer lnPort) {
    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    vm6.invoke(() -> WANTestBase.createCache( lnPort ));
    vm7.invoke(() -> WANTestBase.createCache( lnPort ));
  }

  protected void createReceivers(Integer nyPort) {
    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));
  }

  protected void createSenderVM5() {
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, true, null, true ));
  }

  protected void createSenderVM4() {
    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, true, null, true ));
  }

  
  public void testStartPauseResumeSerialGatewaySender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createReceivers(nyPort);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        10 ));

    startSenders();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        100 ));

    vm4.invoke(() -> WANTestBase.pauseSender( "ln" ));
    vm5.invoke(() -> WANTestBase.pauseSender( "ln" ));

    vm4.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderPausedState( "ln" ));
    vm5.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderPausedState( "ln" ));

    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_RR", 10 ));

    vm4.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm5.invoke(() -> WANTestBase.resumeSender( "ln" ));

    vm4.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderResumedState( "ln" ));
    vm5.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderResumedState( "ln" ));

    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail("Interrupted the async invocation.");
    }

    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    validateQueueContents(vm4, "ln", 0);
    validateQueueContents(vm5, "ln", 0);
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 100 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 100 ));

  }

  public void testStopSerialGatewaySender() throws Throwable {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createReceivers(nyPort);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        20 ));

    startSenders();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        20 ));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 20 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 20 ));
    
    vm2.invoke(() -> WANTestBase.stopReceivers());
    vm3.invoke(() -> WANTestBase.stopReceivers());
    
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        20 ));
    
    vm4.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 20 ));
    vm5.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 20 ));
    
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm5.invoke(() -> WANTestBase.stopSender( "ln" ));

    vm4.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderStoppedState( "ln" ));
    vm5.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderStoppedState( "ln" ));

    vm4.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 0 ));
    vm5.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 0 ));
    /**
     * Should have no effect on GatewaySenderState
     */
    vm4.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm5.invoke(() -> WANTestBase.resumeSender( "ln" ));

    vm4.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderStoppedState( "ln" ));
    vm5.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderStoppedState( "ln" ));

    AsyncInvocation vm4async = vm4.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    AsyncInvocation vm5async = vm5.invokeAsync(() -> WANTestBase.startSender( "ln" ));
    int START_WAIT_TIME = 30000;
    vm4async.getResult(START_WAIT_TIME);
    vm5async.getResult(START_WAIT_TIME);

    vm4.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 20 ));
    vm5.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 20 ));

    vm5.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
      110 ));
    
    vm4.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 130 ));
    vm5.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 130 ));
    
    vm2.invoke(() -> WANTestBase.startReceivers());
    vm3.invoke(() -> WANTestBase.startReceivers());

    vm4.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderResumedState( "ln" ));
    vm5.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderResumedState( "ln" ));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName() + "_RR", 110 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName() + "_RR", 110 ));
    
    vm4.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 0 ));
    vm5.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 0 ));
  }

  protected void startSenders() {
    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
  }

  public void testStopOneSerialGatewaySenderBothPrimary() throws Throwable {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createReceivers(nyPort);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    startSenders();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        100 ));

    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));

    vm4.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderStoppedState( "ln" ));
    
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        200 ));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 200 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 200 ));
    
    //Do some puts while restarting a sender
    AsyncInvocation asyncPuts = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        300 ));
    
    Thread.sleep(10);
    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    
    asyncPuts.getResult();
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 300 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 300 ));
    
    vm4.invoke(() -> WANTestBase.validateQueueSizeStat( "ln", 0 ));
    
    
  }

  public void testStopOneSerialGatewaySender_PrimarySecondary() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createReceivers(nyPort);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    startSenders();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        10 ));

    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));

    vm4.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderStoppedState( "ln" ));
    
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        100 ));
    
    LogWriterUtils.getLogWriter().info("Completed puts in the region");

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 100 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 100 ));
  }
  
  public void testStopOneSender_StartAnotherSender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    createSenderVM4();
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm4.invoke(() -> WANTestBase.startSender( "ln" ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        10 ));
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm4.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderStoppedState( "ln" ));

    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    createSenderVM5();
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));

    vm5.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        100 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 100 ));
  }

  public void test_Bug44153_StopOneSender_StartAnotherSender_CheckQueueSize() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm4.invoke(() -> WANTestBase.createCache( lnPort ));
    createSenderVM4();
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm4.invoke(() -> WANTestBase.startSender( "ln" ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        10 ));
    validateQueueContents(vm4, "ln", 10);
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));

    vm4.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderStoppedState( "ln" ));

    vm5.invoke(() -> WANTestBase.createCache( lnPort ));
    createSenderVM5();
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));

    vm5.invoke(() -> WANTestBase.doPutsFrom( getTestMethodName() + "_RR", 10, 110 ));

    validateQueueContents(vm5, "ln", 100);
    validateQueueClosedVM4();
    vm5.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm5.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderStoppedState( "ln" ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    validateQueueContents(vm4, "ln", 10);
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));

    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 100 ));
    vm5.invoke(() -> WANTestBase.stopSender( "ln" ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 110 ));
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));
  }
  
  private void validateQueueClosedVM4() {
    // TODO Auto-generated method stub
    
  }

  private void validateQueueContents(VM vm, String site, int size) {
    vm.invoke(() -> WANTestBase.validateQueueContents( site,
        size ));
  }

  /**
   * Destroy SerialGatewaySender on all the nodes.
   */
  public void testDestroySerialGatewaySenderOnAllNodes() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createReceivers(nyPort);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    startSenders();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        10 ));
    
    //before destroying, stop the sender
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm5.invoke(() -> WANTestBase.stopSender( "ln" ));
    
    vm4.invoke(() -> WANTestBase.removeSenderFromTheRegion( "ln", getTestMethodName() + "_RR" ));
    vm5.invoke(() -> WANTestBase.removeSenderFromTheRegion( "ln", getTestMethodName() + "_RR" ));

    vm4.invoke(() -> WANTestBase.destroySender( "ln" ));
    vm5.invoke(() -> WANTestBase.destroySender( "ln" ));
    
    vm4.invoke(() -> WANTestBase.verifySenderDestroyed( "ln", false ));
    vm5.invoke(() -> WANTestBase.verifySenderDestroyed( "ln", false ));
  }

  /**
   * Destroy SerialGatewaySender on a single node.
   */
  public void testDestroySerialGatewaySenderOnSingleNode() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createReceivers(nyPort);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    startSenders();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        10 ));
    
    //before destroying, stop the sender
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));
        
    vm4.invoke(() -> WANTestBase.removeSenderFromTheRegion( "ln", getTestMethodName() + "_RR" ));
    
    vm4.invoke(() -> WANTestBase.destroySender( "ln" ));
    
    vm4.invoke(() -> WANTestBase.verifySenderDestroyed( "ln", false ));
    vm5.invoke(() -> WANTestBase.verifySenderRunningState( "ln" ));
  }
  
  /**
   * Since the sender is attached to a region and in use, it can not be destroyed.
   * Hence, exception is thrown by the sender API.
   */
  public void testDestroySerialGatewaySenderExceptionScenario() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createReceivers(nyPort);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    startSenders();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        10 ));
    
    try {
      vm4.invoke(() -> WANTestBase.destroySender( "ln" ));
    } catch (RMIException e) {
      assertTrue("Cause of the exception should be GatewaySenderException", e.getCause() instanceof GatewaySenderException);
    }
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 10 ));
  }

  
  public void testGatewaySenderNotRegisteredAsCacheServer() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    SerializableRunnable check = new SerializableRunnable("assert no cache servers") {
      public void run() {
        InternalLocator inl = (InternalLocator)Locator.getLocator();
        ServerLocator servel = inl.getServerLocatorAdvisee();
        LogWriterUtils.getLogWriter().info("Server load map is " + servel.getLoadMap());
        assertTrue("expected an empty map but found " + servel.getLoadMap(),
            servel.getLoadMap().isEmpty());
        QueueConnectionRequest request = new QueueConnectionRequest(
            ClientProxyMembershipID.getNewProxyMembership(InternalDistributedSystem.getConnectedInstance()),
            1, new HashSet<>(), "", false);
        QueueConnectionResponse response = (QueueConnectionResponse)servel.processRequest(request);
        assertTrue("expected no servers but found " + response.getServers(),
            response.getServers().isEmpty());
      }
    };
    vm0.invoke(check);
    vm1.invoke(check);
    
  }
  

  
  public static void verifySenderPausedState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender)s;
        break;
      }
    }
    assertTrue(sender.isPaused());
  }

  public static void verifySenderResumedState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender)s;
        break;
      }
    }
    assertFalse(sender.isPaused());
    assertTrue(sender.isRunning());
  }

  public static void verifySenderStoppedState(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender)s;
        break;
      }
    }
    assertFalse(sender.isRunning());
    assertFalse(sender.isPaused());
  }

  public static void verifyGatewaySenderOperations(String senderId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    assertFalse(sender.isPaused());
    assertFalse(((AbstractGatewaySender)sender).isRunning());
    sender.pause();
    sender.resume();
    sender.stop();
  }
}
