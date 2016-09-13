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

import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import java.io.IOException;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.internal.cache.ColocationHelper;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;

@Category(DistributedTest.class)
public class ParallelWANPersistenceEnabledGatewaySenderDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;
  
  public ParallelWANPersistenceEnabledGatewaySenderDUnitTest() {
    super();
  }

  @Override
  protected final void postSetUpWANTestBase() throws Exception {
    //The restart tests log this string
    IgnoredException.addIgnoredException("failed accepting client connection");
  }
  
  @Test
  public void testPartitionedRegionWithGatewaySenderPersistenceEnabled() throws IOException {
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
      Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));
      createCache(lnPort);
      GatewaySenderFactory fact = cache.createGatewaySenderFactory();
      fact.setPersistenceEnabled(true);
      fact.setParallel(true);
      final IgnoredException ex = IgnoredException.addIgnoredException("Could not connect");
      try {
        GatewaySender sender1 = fact.create("NYSender", 2);

        AttributesFactory rFact = new AttributesFactory();
        rFact.addGatewaySenderId(sender1.getId());

        PartitionAttributesFactory pFact = new PartitionAttributesFactory();
        pFact.setTotalNumBuckets(100);
        pFact.setRedundantCopies(1);
        rFact.setPartitionAttributes(pFact.create());
        Region r = cache.createRegionFactory(rFact.create()).create("MyRegion");
        sender1.start();
      } finally {
        ex.remove();
      }
      
    }
    catch (Exception e) {
      fail("Unexpected Exception :" + e);
    }
  }
  
  /**
   * Enable persistence for region as well as GatewaySender and see if remote
   * site receives all the events.
   */
  @Test
  public void testPersistentPartitionedRegionWithGatewaySenderPersistenceEnabled() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, true, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, true, null, true ));
    vm6.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, true, null, true ));
    vm7.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, true, null, true ));

    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap() ));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 1000 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 1000 ));

  }

  /**
   * Enable persistence for the GatewaySender but not the region
   */
  @Test
  public void testPartitionedRegionWithPersistentGatewaySender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);
    
    LogWriterUtils.getLogWriter().info("Created remote receivers");
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    
    LogWriterUtils.getLogWriter().info("Created local site cache");

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, true, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, true, null, true ));
    vm6.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, true, null, true ));
    vm7.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, true, null, true ));
  
    LogWriterUtils.getLogWriter().info("Created local site senders");

    vm4.invoke(createPartitionedRegionRunnable());
    vm5.invoke(createPartitionedRegionRunnable());
    vm6.invoke(createPartitionedRegionRunnable());
    vm7.invoke(createPartitionedRegionRunnable());

    LogWriterUtils.getLogWriter().info("Created local site persistent PR");

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Started the senders");

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 1000 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 1000 ));
  }

  protected SerializableRunnableIF createPartitionedRegionRunnable() {
    return () -> WANTestBase.createPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap() );
  }



  
  /**
   * Enable persistence for GatewaySender.
   * Pause the sender and do some puts in local region.  
   * Close the local site and rebuild the region and sender from disk store.
   * Dispatcher should not start dispatching events recovered from persistent sender.
   * Check if the remote site receives all the events.
   */
  @Test
  public void testPRWithGatewaySenderPersistenceEnabled_Restart() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    //create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //create senders with disk store
    String diskStore1 = (String) vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore2 = (String) vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore3 = (String) vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore4 = (String) vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));

    LogWriterUtils.getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    //create PR on remote site
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    
    //create PR on local site
    vm4.invoke(createPartitionedRegionRunnable());
    vm5.invoke(createPartitionedRegionRunnable());
    vm6.invoke(createPartitionedRegionRunnable());
    vm7.invoke(createPartitionedRegionRunnable());

    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);
    
    //wait for senders to become running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    //pause the senders
    vm4.invoke(pauseSenderRunnable());
    vm5.invoke(pauseSenderRunnable());
    vm6.invoke(pauseSenderRunnable());
    vm7.invoke(pauseSenderRunnable());
    
    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 3000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    //--------------------close and rebuild local site -------------------------------------------------
    //kill the senders
    vm4.invoke(killSenderRunnable());
    vm5.invoke(killSenderRunnable());
    vm6.invoke(killSenderRunnable());
    vm7.invoke(killSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("Killed all the senders.");
    
    //restart the vm
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Created back the cache");
    
   //create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore1, true ));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore2, true ));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore3, true ));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore4, true ));
    
    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");
    //create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(createPartitionedRegionRunnable());
    AsyncInvocation inv2 = vm5.invokeAsync(createPartitionedRegionRunnable());
    AsyncInvocation inv3 = vm6.invokeAsync(createPartitionedRegionRunnable());
    AsyncInvocation inv4 = vm7.invokeAsync(createPartitionedRegionRunnable());
    
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
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);
    
    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("All the senders are now running...");
    
    //----------------------------------------------------------------------------------------------------
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 3000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 3000 ));
  }

  protected SerializableRunnableIF killSenderRunnable() {
    return () -> WANTestBase.killSender();
  }

  protected SerializableRunnableIF pauseSenderRunnable() {
    return () -> WANTestBase.pauseSender( "ln" );
  }

  protected SerializableRunnableIF waitForSenderRunnable() {
    return () -> WANTestBase.waitForSenderRunningState( "ln" );
  }
  
  /**
   * Enable persistence for PR and GatewaySender.
   * Pause the sender and do some puts in local region.  
   * Close the local site and rebuild the region and sender from disk store.
   * Dispatcher should not start dispatching events recovered from persistent sender.
   * Check if the remote site receives all the events.
   */
  @Test
  public void testPersistentPRWithGatewaySenderPersistenceEnabled_Restart() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    //create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //create senders with disk store
    String diskStore1 = (String) vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore2 = (String) vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore3 = (String) vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore4 = (String) vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));

    LogWriterUtils.getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    //create PR on remote site
    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    
    //create PR on local site
    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));

    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    //wait for senders to become running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    //pause the senders
    vm4.invoke(pauseSenderRunnable());
    vm5.invoke(pauseSenderRunnable());
    vm6.invoke(pauseSenderRunnable());
    vm7.invoke(pauseSenderRunnable());
    
    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 3000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    //--------------------close and rebuild local site -------------------------------------------------
    //kill the senders
    vm4.invoke(killSenderRunnable());
    vm5.invoke(killSenderRunnable());
    vm6.invoke(killSenderRunnable());
    vm7.invoke(killSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("Killed all the senders.");
    
    //restart the vm
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Created back the cache");
    
   //create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore1, true ));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore2, true ));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore3, true ));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore4, true ));
    
    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");
    //create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv2 = vm5.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv3 = vm6.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv4 = vm7.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    
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
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("All the senders are now running...");
    
    //----------------------------------------------------------------------------------------------------
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 3000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 3000 ));
  }
  
  /**
   * Enable persistence for PR and GatewaySender.
   * Pause the sender and do some puts in local region.  
   * Close the local site and rebuild the region and sender from disk store.
   * Dispatcher should not start dispatching events recovered from persistent sender.
   * Check if the remote site receives all the events.
   */
  @Test
  public void testPersistentPRWithGatewaySenderPersistenceEnabled_Restart2() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //create senders with disk store
    String diskStore1 = (String) vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, false ));
    String diskStore2 = (String) vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, false ));
    String diskStore3 = (String) vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, false ));
    String diskStore4 = (String) vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, false ));

    LogWriterUtils.getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    //create PR on local site
    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));

    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);
    
    //wait for senders to become running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    //pause the senders
    vm4.invoke(pauseSenderRunnable());
    vm5.invoke(pauseSenderRunnable());
    vm6.invoke(pauseSenderRunnable());
    vm7.invoke(pauseSenderRunnable());
    
    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 300 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    //--------------------close and rebuild local site -------------------------------------------------
    //kill the senders
    vm4.invoke(killSenderRunnable());
    vm5.invoke(killSenderRunnable());
    vm6.invoke(killSenderRunnable());
    vm7.invoke(killSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("Killed all the senders.");
    
    //restart the vm
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Created back the cache");
    
  //create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore1, true ));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore2, true ));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore3, true ));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore4, true ));
    
    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");
    
    //create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv2 = vm5.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv3 = vm6.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv4 = vm7.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    
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
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);
    
    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());


    LogWriterUtils.getLogWriter().info("Creating the receiver.");
    //create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    //create PR on remote site
    
    LogWriterUtils.getLogWriter().info("Creating the partitioned region at receiver. ");
    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    createReceiverInVMs(vm2, vm3);

    vm4.invoke(pauseSenderRunnable());
    vm5.invoke(pauseSenderRunnable());
    vm6.invoke(pauseSenderRunnable());
    vm7.invoke(pauseSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("Doing some extra puts. ");
    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPutsAfter300( getTestMethodName(), 1000 ));
    //----------------------------------------------------------------------------------------------------
    vm4.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm5.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm6.invoke(() -> WANTestBase.resumeSender( "ln" ));
    vm7.invoke(() -> WANTestBase.resumeSender( "ln" ));
    
    LogWriterUtils.getLogWriter().info("Validating the region size at the receiver end. ");
    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 1000 ));
  }
  
  
  /**
   * Enable persistence for PR and GatewaySender.
   * Pause the sender and do some puts in local region.  
   * Close the local site and rebuild the region and sender from disk store.
   * Dispatcher should not start dispatching events recovered from persistent sender.
   * --> At the same time, do some more puts on the local region.  
   * Check if the remote site receives all the events.
   */
  @Test
  public void testPersistentPRWithGatewaySenderPersistenceEnabled_Restart_Scenario2() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //create senders with disk store
    String diskStore1 = (String) vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore2 = (String) vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore3 = (String) vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore4 = (String) vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));

    LogWriterUtils.getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    //create PR on remote site
    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    
    //create PR on local site
    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));

    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    //wait for senders to become running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    //pause the senders
    vm4.invoke(pauseSenderRunnable());
    vm5.invoke(pauseSenderRunnable());
    vm6.invoke(pauseSenderRunnable());
    vm7.invoke(pauseSenderRunnable());
    
    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 3000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    //--------------------close and rebuild local site -------------------------------------------------
    //kill the senders
    vm4.invoke(killSenderRunnable());
    vm5.invoke(killSenderRunnable());
    vm6.invoke(killSenderRunnable());
    vm7.invoke(killSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("Killed all the senders.");
    
    //restart the vm
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Created back the cache");
    
   //create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore1, true ));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore2, true ));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore3, true ));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore4, true ));
    
    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");
    
    //create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv2 = vm5.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv3 = vm6.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv4 = vm7.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    
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
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);
    
    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("All the senders are now running...");
    
    //----------------------------------------------------------------------------------------------------
    
    //Dispatcher should be dispatching now. Do some more puts through async thread
    AsyncInvocation async1 = vm4.invokeAsync(() -> WANTestBase.doPuts( getTestMethodName(), 1000 ));
    try {
      async1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 3000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 3000 ));
  }

  /**
   * Test case for bug# 44275.
   * Enable persistence for PR and GatewaySender. 
   * Do puts into region with key as a String.
   * Close the local site and rebuild the region and sender from disk store. 
   * Check if the remote site receives all the events.
   */
  @Test
  public void testPersistentPRWithPersistentGatewaySender_Restart_Bug44275() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    //create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //create senders with disk store
    String diskStore1 = (String) vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore2 = (String) vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore3 = (String) vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore4 = (String) vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));

    LogWriterUtils.getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    //create PR on remote site
    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    
    //create PR on local site
    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));

    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);
    
    //wait for senders to become running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    //pause the senders
    vm4.invoke(pauseSenderRunnable());
    vm5.invoke(pauseSenderRunnable());
    vm6.invoke(pauseSenderRunnable());
    vm7.invoke(pauseSenderRunnable());
    
    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPutsWithKeyAsString( getTestMethodName(), 1000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    //--------------------close and rebuild local site -------------------------------------------------
    //kill the senders
    vm4.invoke(killSenderRunnable());
    vm5.invoke(killSenderRunnable());
    vm6.invoke(killSenderRunnable());
    vm7.invoke(killSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("Killed all the senders.");
    
    //restart the vm
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Created back the cache");
    
  //create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore1, true ));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore2, true ));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore3, true ));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore4, true ));
    
    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");
    
    //create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv2 = vm5.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv3 = vm6.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv4 = vm7.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    
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
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("All the senders are now running...");
    
    //----------------------------------------------------------------------------------------------------
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
    getTestMethodName(), 1000 ));
  }
  
  /**
   * Test case for bug# 44275.
   * Enable persistence for PR and GatewaySender. 
   * Do puts into region with key as a String.
   * Close the local site and rebuild the region and sender from disk store. 
   * Check if the remote site receives all the events.
   */
  @Test
  public void testPersistentPRWithPersistentGatewaySender_Restart_DoOps() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //create senders with disk store
    String diskStore1 = (String) vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore2 = (String) vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore3 = (String) vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore4 = (String) vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));

    LogWriterUtils.getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    //create PR on remote site
    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    
    //create PR on local site
    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));

    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    //wait for senders to become running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    //pause the senders
    vm4.invoke(pauseSenderRunnable());
    vm5.invoke(pauseSenderRunnable());
    vm6.invoke(pauseSenderRunnable());
    vm7.invoke(pauseSenderRunnable());
    
    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPutsWithKeyAsString( getTestMethodName(), 1000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    //--------------------close and rebuild local site -------------------------------------------------
    //kill the senders
    vm4.invoke(killSenderRunnable());
    vm5.invoke(killSenderRunnable());
    vm6.invoke(killSenderRunnable());
    vm7.invoke(killSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("Killed all the senders.");
    
    //restart the vm
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Created back the cache");
    
  //create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore1, true ));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore2, true ));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore3, true ));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore4, true ));
    
    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");
    
    // create PR on local site
    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    
    LogWriterUtils.getLogWriter().info("Created back the partitioned regions");
    
    //start the senders in async mode. This will ensure that the 
    //node of shadow PR that went down last will come up first
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("All the senders are now running...");
    
    //----------------------------------------------------------------------------------------------------
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
    getTestMethodName(), 1000 ));
    
    vm4.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
    vm5.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
    vm6.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
    vm7.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
    
   //do some extra puts in region on local site
    vm4.invoke(() -> WANTestBase.doPutsWithKeyAsString( getTestMethodName(), 10000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 10000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
    getTestMethodName(), 10000 ));
  }
  
  @Test
  public void testPersistentPR_Restart() {
    // create locator on local site
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    // create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    // create PR on local site
    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1, 100, isOffHeap() ));

    // start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPutsWithKeyAsString(
        getTestMethodName(), 1000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");

    // --------------------close and rebuild local site
    // -------------------------------------------------
    // kill the senders
    vm4.invoke(killSenderRunnable());
    vm5.invoke(killSenderRunnable());
    vm6.invoke(killSenderRunnable());
    vm7.invoke(killSenderRunnable());

    LogWriterUtils.getLogWriter().info("Killed all the senders.");

    // restart the vm
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Created back the cache");

//    // create PR on local site
//    vm4.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
//        new Object[] { testName, "ln", 1, 100, isOffHeap() });
//    vm5.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
//        new Object[] { testName, "ln", 1, 100, isOffHeap() });
//    vm6.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
//        new Object[] { testName, "ln", 1, 100, isOffHeap() });
//    vm7.invoke(WANTestBase.class, "createPersistentPartitionedRegion",
//        new Object[] { testName, "ln", 1, 100, isOffHeap() });

    // create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1,
            100, isOffHeap() ));
    AsyncInvocation inv2 = vm5.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1,
            100, isOffHeap() ));
    AsyncInvocation inv3 = vm6.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1,
            100, isOffHeap() ));
    AsyncInvocation inv4 = vm7.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion( getTestMethodName(), "ln", 1,
            100, isOffHeap() ));

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

    LogWriterUtils.getLogWriter().info("Created back the partitioned regions");

    vm4.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
    vm5.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
    vm6.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
    vm7.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 1000 ));
  }

  /**
   * Enable persistence for PR and GatewaySender. 
   * Close the local site. 
   * Create the local cache.
   * Don't create back the partitioned region but create just the sender.  
   * Check if the remote site receives all the events.
   * NOTE: This use case is not supported yet. 
   * For ParallelGatewaySender to start, it must be associated with a partitioned region. 
   */
  @Ignore("NotSupported")
  @Test
  public void testPersistentPartitionedRegionWithGatewaySenderPersistenceEnabled_Restart2() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    //create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //create senders with disk store
    String diskStore1 = (String) vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore2 = (String) vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore3 = (String) vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore4 = (String) vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));

    LogWriterUtils.getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    //create PR on remote site
    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    
    //create PR on local site
    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));

    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    //wait for senders to become running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    //pause the senders
    vm4.invoke(pauseSenderRunnable());
    vm5.invoke(pauseSenderRunnable());
    vm6.invoke(pauseSenderRunnable());
    vm7.invoke(pauseSenderRunnable());
    
    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 1000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    //--------------------close and rebuild local site -------------------------------------------------
    //kill the senders
    vm4.invoke(killSenderRunnable());
    vm5.invoke(killSenderRunnable());
    vm6.invoke(killSenderRunnable());
    vm7.invoke(killSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("Killed all the senders.");
    
    //restart the vm
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Created back the cache");
    
    
    //create senders from disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore1, true ));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore2, true ));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore3, true ));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore4, true ));
    
    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");
    
    
    //start the senders. NOTE that the senders are not associated with partitioned region
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Started the senders.");
    
    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("All the senders are now running...");
    
    //----------------------------------------------------------------------------------------------------
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 1000 ));
  }
  
  /**
   * Create a non persistent PR and enable persistence for GatewaySender attached to the PR.
   * Close the local site and rebuild it. Check if the remote site receives all the events.
   * NOTE: This use case is not supported for now. For persistent parallel gateway sender,
   * the PR to which it is attached should also be persistent.
   */
  @Ignore("NotSupported")
  @Test
  public void testNonPersistentPartitionedRegionWithGatewaySenderPersistenceEnabled_Restart() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    //create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    //create senders with disk store
    String diskStore1 = (String) vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore2 = (String) vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore3 = (String) vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));
    String diskStore4 = (String) vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, null, true ));

    LogWriterUtils.getLogWriter().info("The DS are: " + diskStore1 + "," + diskStore2 + "," + diskStore3 + "," + diskStore4);
    
    //create PR on remote site
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    
    //create non persistent PR on local site
    vm4.invoke(createPartitionedRegionRunnable());
    vm5.invoke(createPartitionedRegionRunnable());
    vm6.invoke(createPartitionedRegionRunnable());
    vm7.invoke(createPartitionedRegionRunnable());

    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    //wait for senders to become running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    //pause the senders
    vm4.invoke(pauseSenderRunnable());
    vm5.invoke(pauseSenderRunnable());
    vm6.invoke(pauseSenderRunnable());
    vm7.invoke(pauseSenderRunnable());
    
    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 1000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    //kill the senders
    vm4.invoke(killSenderRunnable());
    vm5.invoke(killSenderRunnable());
    vm6.invoke(killSenderRunnable());
    vm7.invoke(killSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("Killed all the senders. The local site has been brought down.");
    
    //restart the vm
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Created back the cache");
    
  //create senders with disk store
    vm4.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore1, true ));
    vm5.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore2, true ));
    vm6.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore3, true ));
    vm7.invoke(() -> WANTestBase.createSenderWithDiskStore( "ln", 2, true, 100, 10, false, true, null, diskStore4, true ));
    
    LogWriterUtils.getLogWriter().info("Created the senders back from the disk store.");
    
    //create PR on local site
    vm4.invoke(createPartitionedRegionRunnable());
    vm5.invoke(createPartitionedRegionRunnable());
    vm6.invoke(createPartitionedRegionRunnable());
    vm7.invoke(createPartitionedRegionRunnable());
    
    LogWriterUtils.getLogWriter().info("Created back the partitioned regions");
    
    //start the senders
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Started the senders.");
    
    LogWriterUtils.getLogWriter().info("Waiting for senders running.");
    //wait for senders running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("All the senders are now running...");
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
    getTestMethodName(), 1000 ));
  }


  /**
   * Create persistent PR and non-persistent GatewaySender.
   * After doing puts, close the local site. 
   * Rebuild the PR from persistent disk store and create the sender again. 
   * Do more puts. Check if the remote site receives newly added events.
   * 
   * This test can be used as DUnit test for defect #50247 reported by Indian Railways.
   * At present, customer is using this configuration and which is not recommended 
   * since it can lead to event loss of GatewaySender events.
   */
  @Ignore("Bug50247")
  @Test
  public void testPersistentPartitionedRegionWithGatewaySender_Restart() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    //create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    vm6.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    vm7.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));

    //create PR on remote site
    vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), null, 1, 100, isOffHeap() ));
    
    //create PR on local site
    vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    
    //start the senders on local site
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    //wait for senders to become running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    //pause the senders
    vm4.invoke(pauseSenderRunnable());
    vm5.invoke(pauseSenderRunnable());
    vm6.invoke(pauseSenderRunnable());
    vm7.invoke(pauseSenderRunnable());
    
    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 3000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    //----------------- Close and rebuild local site -------------------------------------
    //kill the senders
    vm4.invoke(killSenderRunnable());
    vm5.invoke(killSenderRunnable());
    vm6.invoke(killSenderRunnable());
    vm7.invoke(killSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("Killed all the senders.");
    
    //restart the vm
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Created back the cache");
    
   //create back the senders
    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    vm6.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    vm7.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    
    LogWriterUtils.getLogWriter().info("Created the senders again");
    
    //start the senders
    startSenderInVMsAsync("ln", vm4, vm5, vm6, vm7);

    LogWriterUtils.getLogWriter().info("Started the senders.");
    
    LogWriterUtils.getLogWriter().info("Waiting for senders running.");

    //wait for senders running
    vm4.invoke(waitForSenderRunnable());
    vm5.invoke(waitForSenderRunnable());
    vm6.invoke(waitForSenderRunnable());
    vm7.invoke(waitForSenderRunnable());
    
    LogWriterUtils.getLogWriter().info("All the senders are now running...");
    
    //create PR on local site
    AsyncInvocation inv1 = vm4.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv2 = vm5.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv3 = vm6.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    AsyncInvocation inv4 = vm7.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
      getTestMethodName(), "ln", 1, 100, isOffHeap() ));
    
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

    //-------------------------------------------------------------------------------------------
    
    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 3000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    vm2.invoke(() -> WANTestBase.validateRegionSize( getTestMethodName(), 3000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize( getTestMethodName(), 3000 ));
  }
  
  /**
   * LocalMaxMemory of user PR is 0 (accessor region).
   * Parallel sender persistence is enabled. 
   * In this scenario, the PR for Parallel sender should be created with persistence = false.
   * This is because since the region is accessor region, it won't host any buckets and 
   * hence Parallel sender PR is not required to be persistent.
   * This test is added for defect # 45747. 
   */
  @Test
  public void testParallelPropagationWithSenderPersistenceEnabledForAccessor() {
    //create locator on local site
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    //create locator on remote site
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    //create receiver on remote site
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    //create cache in local site
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    
    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, true, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, true, null, true ));
    vm6.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, true, null, true ));
    vm7.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, true, null, true ));
    
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

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    //start puts in region on local site
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 1000 ));
    LogWriterUtils.getLogWriter().info("Completed puts in the region");
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName() + "_PR", 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName() + "_PR", 1000 ));
  }
  
  /**
   * Test for bug 50120 see if we can recover after deleting the parallel gateway
   * sender files and not recoverying the sender when we have a persistent PR.
   * @throws Throwable 
   */
  @Test
  public void testRemoveGatewayFromPersistentRegionOnRestart() throws Throwable {
    

    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
      Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

      createCacheInVMs(nyPort, vm2, vm3);
      createReceiverInVMs(vm2, vm3);

      createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

      vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, true, null, true ));
      vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, true, null, true ));
      vm6.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, true, null, true ));
      vm7.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, true, null, true ));

      vm4.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap() ));
      vm5.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap() ));
      vm6.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap() ));
      vm7.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), "ln", 1, 100, isOffHeap() ));

      startSenderInVMs("ln", vm4, vm5, vm6, vm7);

      vm2.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap() ));
      vm3.invoke(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap() ));

      vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName(), 113 ));

      vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 113 ));
      vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName(), 113 ));

      //Bounce vm4, vm5, vm6, vm7 without the parallel queue
      vm4.invoke(WANTestBase.class, "closeCache", new Object [] {});
      vm5.invoke(WANTestBase.class, "closeCache", new Object [] {});
      vm6.invoke(WANTestBase.class, "closeCache", new Object [] {});
      vm7.invoke(WANTestBase.class, "closeCache", new Object [] {});

      vm4.invoke(ParallelWANPersistenceEnabledGatewaySenderDUnitTest.class, "setIgnoreQueue" , new Object[] { true});
      vm5.invoke(ParallelWANPersistenceEnabledGatewaySenderDUnitTest.class, "setIgnoreQueue" , new Object[] { true});
      vm6.invoke(ParallelWANPersistenceEnabledGatewaySenderDUnitTest.class, "setIgnoreQueue" , new Object[] { true});
      vm7.invoke(ParallelWANPersistenceEnabledGatewaySenderDUnitTest.class, "setIgnoreQueue" , new Object[] { true});

      createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

      AsyncInvocation async4 = vm4.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap() ));
      AsyncInvocation async5 = vm5.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap() ));
      AsyncInvocation async6 = vm6.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap() ));
      AsyncInvocation async7 = vm7.invokeAsync(() -> WANTestBase.createPersistentPartitionedRegion(
        getTestMethodName(), null, 1, 100, isOffHeap() ));

      async7.getResult(30 * 1000);
      async5.getResult(30 * 1000);
      async6.getResult(30 * 1000);
      async4.getResult(30 * 1000);

      //This should succeed, because the region recovered even though
      //the queue was removed.
      vm4.invoke(() -> WANTestBase.validateRegionSize(
      getTestMethodName(), 113 ));
    } finally {
      vm4.invoke(ParallelWANPersistenceEnabledGatewaySenderDUnitTest.class, "setIgnoreQueue" , new Object[] { false});
      vm5.invoke(ParallelWANPersistenceEnabledGatewaySenderDUnitTest.class, "setIgnoreQueue" , new Object[] { false});
      vm6.invoke(ParallelWANPersistenceEnabledGatewaySenderDUnitTest.class, "setIgnoreQueue" , new Object[] { false});
      vm7.invoke(ParallelWANPersistenceEnabledGatewaySenderDUnitTest.class, "setIgnoreQueue" , new Object[] { false});
    }
  }
  
  public static void setIgnoreQueue(boolean shouldIgnore) {
    ColocationHelper.IGNORE_UNRECOVERED_QUEUE = shouldIgnore;
  }
}
