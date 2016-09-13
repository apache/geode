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
package org.apache.geode.internal.cache.wan.concurrent;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.wan.BatchException70;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;

/**
 * All the test cases are similar to SerialWANPropagationDUnitTest except that
 * the we create concurrent serial GatewaySender with concurrency of 4
 *
 */
@Category(DistributedTest.class)
public class ConcurrentWANPropagation_1_DUnitTest extends WANTestBase {

  /**
   * @param name
   */
  public ConcurrentWANPropagation_1_DUnitTest() {
    super();
  }

  private static final long serialVersionUID = 1L;
  
  /**
   * All the test cases are similar to SerialWANPropagationDUnitTest
   * @throws Exception
   */
  @Test
  public void testReplicatedSerialPropagation_withoutRemoteSite() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //keep the batch size high enough to reduce the number of exceptions in the log
    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 400, false, false, null, true, 4, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 400, false, false, null, true, 4, OrderPolicy.KEY ));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    
    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
      1000 ));

    createCacheInVMs(nyPort, vm2, vm3);

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", null, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", null, isOffHeap() ));
  
    vm2.invoke(() -> WANTestBase.createReceiver());
    vm3.invoke(() -> WANTestBase.createReceiver());
    
    vm4.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
  }
  
  @Test
  public void testReplicatedSerialPropagation() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
  }
  
  
  @Test
  public void testReplicatedSerialPropagationWithLocalSiteClosedAndRebuilt() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));
    
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));
    
    //---------close local site and build again-----------------------------------------
    vm4.invoke(() -> WANTestBase.killSender( ));
    vm5.invoke(() -> WANTestBase.killSender( ));
    vm6.invoke(() -> WANTestBase.killSender( ));
    vm7.invoke(() -> WANTestBase.killSender( ));
    
    Integer regionSize = 
      (Integer) vm2.invoke(() -> WANTestBase.getRegionSize(getTestMethodName() + "_RR" ));
    LogWriterUtils.getLogWriter().info("Region size on remote is: " + regionSize);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
      false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
      false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD ));
    
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", "ln", isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
      getTestMethodName() + "_RR", "ln", isOffHeap() ));

    startSenderInVMs("ln", vm4, vm5);

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    IgnoredException.addIgnoredException(EntryExistsException.class.getName());
    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());
    
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
      1000 ));
    //----------------------------------------------------------------------------------

    //verify remote site receives all the events
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
  }
  
  /**
   * Two regions configured with the same sender and put is in progress 
   * on both the regions.
   * One of the two regions is destroyed in the middle.
   * 
   * @throws Exception
   */
  @Test
  public void testReplicatedSerialPropagationWithLocalRegionDestroy() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //these are part of remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    //these are part of local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //senders are created on local site
    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 20, false, false, null, true, 3, OrderPolicy.THREAD ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 20, false, false, null, true ,3, OrderPolicy.THREAD));

    //create one RR (RR_1) on remote site
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", null, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", null, isOffHeap() ));

    //create another RR (RR_2) on remote site
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", null, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", null, isOffHeap() ));
    
    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5);

    //create one RR (RR_1) on local site
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));

    //create another RR (RR_2) on local site
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", "ln", isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", "ln", isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", "ln", isOffHeap() ));
    
    //start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_RR_1", 1000 ));
    //do puts in RR_2 in main thread
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR_2", 500 ));
    //destroy RR_2 after above puts are complete
    vm4.invoke(() -> WANTestBase.destroyRegion( getTestMethodName() + "_RR_2"));
    
    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    //sleep for some time to let all the events propagate to remote site
    Thread.sleep(20);
    //vm4.invoke(() -> WANTestBase.verifyQueueSize( "ln", 0 ));
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR_1", 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR_2", 500 ));
  }

  /**
   * 1 region and sender configured on local site and 1 region and a 
   * receiver configured on remote site. Puts to the local region are in progress.
   * Remote region is destroyed in the middle.
   * 
   * @throws Exception
   */
  @Test
  public void testReplicatedSerialPropagationWithRemoteRegionDestroy() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //these are part of remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);


    //these are part of local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //senders are created on local site
    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 500, false, false, null, true, 5, OrderPolicy.KEY ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 500, false, false, null, true, 5, OrderPolicy.KEY ));

    //create one RR (RR_1) on remote site
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", null, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", null, isOffHeap() ));

    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5);

    //create one RR (RR_1) on local site
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));

    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());
    
    //start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_RR_1", 10000 ));
    //destroy RR_1 in remote site
    vm2.invoke(() -> WANTestBase.destroyRegion( getTestMethodName() + "_RR_1"));
    
    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    //verify that all is well in local site. All the events should be present in local region
    vm4.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR_1", 10000 ));
    //assuming some events might have been dispatched before the remote region was destroyed,
    //sender's region queue will have events less than 1000 but the queue will not be empty.
    //NOTE: this much verification might be sufficient in DUnit. Hydra will take care of 
    //more in depth validations.
    vm4.invoke(() -> WANTestBase.verifyRegionQueueNotEmptyForConcurrentSender("ln" ));
  }
  
  /**
   * Two regions configured in local with the same sender and put is in progress 
   * on both the regions. Same two regions are configured on remote site as well.
   * One of the two regions is destroyed in the middle on remote site.
   * 
   * @throws Exception
   */
  @Test
  public void testReplicatedSerialPropagationWithRemoteRegionDestroy2() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //these are part of remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    //these are part of local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //senders are created on local site
    vm4.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 200, false, false, null, true, 5, OrderPolicy.THREAD ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        false, 100, 200, false, false, null, true, 5, OrderPolicy.THREAD ));

    //create one RR (RR_1) on remote site
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", null, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", null, isOffHeap() ));

    //create another RR (RR_2) on remote site
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", null, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", null, isOffHeap() ));
    
    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5);

    //create one RR (RR_1) on local site
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", "ln", isOffHeap() ));

    //create another RR (RR_2) on local site
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", "ln", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", "ln", isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", "ln", isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", "ln", isOffHeap() ));
    //destroy RR_2 on remote site in the middle
    vm2.invoke(() -> WANTestBase.destroyRegion( getTestMethodName() + "_RR_2"));
    
    //expected exceptions in the logs
    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());
    
    //start puts in RR_2 in another thread
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR_2", 1000 ));
    
    //start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_RR_1", 1000 ));
   
    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    //though region RR_2 is destroyed, RR_1 should still get all the events put in it 
    //in local site
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR_1", 1000 ));

  }

  @Test
  public void testReplicatedSerialPropagationWithRemoteRegionDestroy3()
      throws Exception {
    final String senderId = "ln";
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));
    // these are part of remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    // these are part of local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // senders are created on local site
    vm4.invoke(() -> WANTestBase.createConcurrentSender(  senderId, 2,
        false, 100, 200, false, false, null, true, 5, OrderPolicy.THREAD ));
    vm5.invoke(() -> WANTestBase.createConcurrentSender( senderId, 2,
        false, 100, 200, false, false, null, true, 5, OrderPolicy.THREAD ));

    // create one RR (RR_1) on remote site
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", null, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", null, isOffHeap() ));

    // create another RR (RR_2) on remote site
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", null, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", null, isOffHeap() ));


    // start the senders on local site
    startSenderInVMs("ln", vm4, vm5);

    // create one RR (RR_1) on local site
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", senderId, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", senderId, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", senderId, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_1", senderId, isOffHeap() ));

    // create another RR (RR_2) on local site
    vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", senderId, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", senderId, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", senderId, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR_2", senderId, isOffHeap() ));

    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_RR_1", 1000 ));
    // start puts in RR_2 in another thread
    AsyncInvocation inv2 = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName() + "_RR_2", 1000 ));
    // destroy RR_2 on remote site in the middle
    vm2.invoke(() -> WANTestBase.destroyRegion( getTestMethodName()
        + "_RR_2" ));

    try {
      inv1.join();
      inv2.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    // though region RR_2 is destroyed, RR_1 should still get all the events put
    // in it
    // in local site
    try {
      vm2.invoke(() -> WANTestBase.validateRegionSize(
          getTestMethodName() + "_RR_1", 1000 ));
    } finally {
      System.setProperty(
          DistributionConfig.GEMFIRE_PREFIX + "GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION", "False");
      vm4.invoke(new CacheSerializableRunnable("UnSetting system property ") {
        public void run2() throws CacheException {
          System.setProperty(
              DistributionConfig.GEMFIRE_PREFIX + "GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION", "False");
        }
      });

      vm5.invoke(new CacheSerializableRunnable("UnSetting system property ") {
        public void run2() throws CacheException {
          System.setProperty(
              DistributionConfig.GEMFIRE_PREFIX + "GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION", "False");
        }
      });
    }
  }

}
