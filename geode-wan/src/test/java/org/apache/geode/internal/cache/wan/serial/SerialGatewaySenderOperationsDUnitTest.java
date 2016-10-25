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
package org.apache.geode.internal.cache.wan.serial;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.internal.locator.QueueConnectionRequest;
import org.apache.geode.cache.client.internal.locator.QueueConnectionResponse;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocator;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

/**
 * 
 */
@Category(DistributedTest.class)
public class SerialGatewaySenderOperationsDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public SerialGatewaySenderOperationsDUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
    IgnoredException.addIgnoredException("Connection refused");
    IgnoredException.addIgnoredException("could not get remote locator information");
    
    //Stopping the gateway closed the region,
    //which causes this exception to get logged
    IgnoredException.addIgnoredException(RegionDestroyedException.class.getSimpleName());
  }

  @Test
  public void testSerialGatewaySenderOperationsWithoutStarting() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

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

  protected void createSenderVM5() {
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, true, null, true ));
  }

  protected void createSenderVM4() {
    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, true, null, true ));
  }

  
  @Test
  public void testStartPauseResumeSerialGatewaySender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        10 ));

    startSenderInVMs("ln", vm4, vm5);

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

  @Test
  public void testStopSerialGatewaySender() throws Throwable {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        20 ));

    startSenderInVMs("ln", vm4, vm5);

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

  @Test
  public void testStopOneSerialGatewaySenderBothPrimary() throws Throwable {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    startSenderInVMs("ln", vm4, vm5);

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

  @Test
  public void testStopOneSerialGatewaySender_PrimarySecondary() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    startSenderInVMs("ln", vm4, vm5);

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
  
  @Test
  public void testStopOneSender_StartAnotherSender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", null, isOffHeap() ));
    vm2.invoke(() -> WANTestBase.createReceiver());

    createCacheInVMs(lnPort, vm4);
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

  @Test
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
    vm5.invoke(() -> WANTestBase.stopSender( "ln" ));
    vm5.invoke(() -> SerialGatewaySenderOperationsDUnitTest.verifySenderStoppedState( "ln" ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    validateQueueContents(vm4, "ln", 10);
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));

    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", null, isOffHeap() ));
    vm2.invoke(() -> WANTestBase.createReceiver());

    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 100 ));
    vm5.invoke(() -> WANTestBase.stopSender( "ln" ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 110 ));
    vm4.invoke(() -> WANTestBase.stopSender( "ln" ));
  }

  private void validateQueueContents(VM vm, String site, int size) {
    vm.invoke(() -> WANTestBase.validateQueueContents( site,
        size ));
  }

  /**
   * Destroy SerialGatewaySender on all the nodes.
   */
  @Test
  public void testDestroySerialGatewaySenderOnAllNodes() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    startSenderInVMs("ln", vm4, vm5);

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
  @Test
  public void testDestroySerialGatewaySenderOnSingleNode() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    startSenderInVMs("ln", vm4, vm5);

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
  @Test
  public void testDestroySerialGatewaySenderExceptionScenario() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createSenderCaches(lnPort);

    createSenderVM4();
    createSenderVM5();

    createReceiverRegions();

    createSenderRegions();

    startSenderInVMs("ln", vm4, vm5);

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

  
  @Test
  public void testGatewaySenderNotRegisteredAsCacheServer() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, true, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, true, null, true ));

    startSenderInVMs("ln", vm4, vm5);

    SerializableRunnable check = new SerializableRunnable("assert no cache servers") {
      public void run() {
        InternalLocator inl = (InternalLocator)Locator.getLocator();
        ServerLocator server = inl.getServerLocatorAdvisee();
        LogWriterUtils.getLogWriter().info("Server load map is " + server.getLoadMap());
        assertTrue("expected an empty map but found " + server.getLoadMap(),
            server.getLoadMap().isEmpty());
        QueueConnectionRequest request = new QueueConnectionRequest(
            ClientProxyMembershipID.getNewProxyMembership(InternalDistributedSystem.getConnectedInstance()),
            1, new HashSet<>(), "", false);
        QueueConnectionResponse response = (QueueConnectionResponse)server.processRequest(request);
        assertTrue("expected no servers but found " + response.getServers(),
            response.getServers().isEmpty());
      }
    };
    vm0.invoke(check);
    vm1.invoke(check);
    
  }

  @Test
  public void registeringInstantiatorsInGatewayShouldNotCauseDeadlock() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(() -> createReceiverAndServer( nyPort ));
    vm3.invoke(() -> createReceiverAndServer( lnPort ));

    vm2.invoke(() -> createSender( "ln", 1, false, 100, 10, false, false, null, false));
    vm3.invoke(() -> createSender( "ny", 2, false, 100, 10, false, false, null, false ));

    vm4.invoke(() -> createClientWithLocator(nyPort, "localhost"));
    
    // Register instantiator
    vm4.invoke(() -> Instantiator.register(new TestObjectInstantiator()));
  }

  @Test
  public void registeringDataSerializableInGatewayShouldNotCauseDeadlock() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
      "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
      "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(() -> createReceiverAndServer( nyPort ));
    vm3.invoke(() -> createReceiverAndServer( lnPort ));

    vm2.invoke(() -> createSender( "ln", 1, false, 100, 10, false, false, null, false));
    vm3.invoke(() -> createSender( "ny", 2, false, 100, 10, false, false, null, false ));

    vm4.invoke(() -> createClientWithLocator(nyPort, "localhost"));

    // Register instantiator
    vm4.invoke(() -> DataSerializer.register(TestObjectDataSerializer.class));
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
  
  static class TestObjectInstantiator extends Instantiator {

    TestObjectInstantiator(Class<TestObject> c, byte id) {
      super(c, id);
    }

    TestObjectInstantiator() {
      this(TestObject.class, (byte) 99);
    }

    public DataSerializable newInstance() {
      return new TestObject();
    }
  }

  static class TestObjectDataSerializer extends DataSerializer implements Serializable {

    @Override
    public Class<?>[] getSupportedClasses() {
      return new Class<?>[] {TestObject.class};
    }

    @Override
    public boolean toData(final Object o, final DataOutput out) throws IOException {
      return o instanceof TestObject;
    }

    @Override
    public Object fromData(final DataInput in) throws IOException, ClassNotFoundException {
      return new TestObject();
    }

    @Override
    public int getId() {
      return 99;
    }
  }

  static class TestObject implements DataSerializable {
    
    private static final long serialVersionUID = 1L;
    
    protected String id;

    public TestObject() {}

    public TestObject(String id) {
      this.id = id;
    }

    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(this.id, out);
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.id = DataSerializer.readString(in);
    }
  }
}
