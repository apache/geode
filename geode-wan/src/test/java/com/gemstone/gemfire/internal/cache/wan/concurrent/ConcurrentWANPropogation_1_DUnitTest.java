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

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.wan.BatchException70;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;

/**
 * All the test cases are similar to SerialWANPropogationDUnitTest except that
 * the we create concurrent serial GatewaySender with concurrency of 4
 * @author skumar
 *
 */
public class ConcurrentWANPropogation_1_DUnitTest extends WANTestBase {

  /**
   * @param name
   */
  public ConcurrentWANPropogation_1_DUnitTest(String name) {
    super(name);
  }

  private static final long serialVersionUID = 1L;
  
  /**
   * All the test cases are similar to SerialWANPropogationDUnitTest
   * @throws Exception
   */
  public void testReplicatedSerialPropagation_withoutRemoteSite() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    
    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    //keep the batch size high enough to reduce the number of exceptions in the log
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 400, false, false, null, true, 4, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 400, false, false, null, true, 4, OrderPolicy.KEY });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    
    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
      1000 });
    
    vm2.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", null, isOffHeap() });
  
    vm2.invoke(WANTestBase.class, "createReceiver2",
        new Object[] {nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver2",
        new Object[] {nyPort });
    
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
  }
  
  public void testReplicatedSerialPropagation() throws Exception {
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
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
  }
  
  
  public void testReplicatedSerialPropagationWithLocalSiteClosedAndRebuilt() throws Exception {
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Unexpected IOException");
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
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });
    
    //---------close local site and build again-----------------------------------------
    vm4.invoke(WANTestBase.class, "killSender", new Object[] { });
    vm5.invoke(WANTestBase.class, "killSender", new Object[] { });
    vm6.invoke(WANTestBase.class, "killSender", new Object[] { });
    vm7.invoke(WANTestBase.class, "killSender", new Object[] { });
    
    Integer regionSize = 
      (Integer) vm2.invoke(WANTestBase.class, "getRegionSize", new Object[] {getTestMethodName() + "_RR" });
    LogWriterUtils.getLogWriter().info("Region size on remote is: " + regionSize);
    
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
      false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
      false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });
  
    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", "ln", isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    IgnoredException.addIgnoredException(EntryExistsException.class.getName());
    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
      1000 });
    //----------------------------------------------------------------------------------

    //verify remote site receives all the events
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
  }
  
  /**
   * Two regions configured with the same sender and put is in progress 
   * on both the regions.
   * One of the two regions is destroyed in the middle.
   * 
   * @throws Exception
   */
  public void testReplicatedSerialPropagationWithLocalRegionDestroy() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    //these are part of remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    //these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //senders are created on local site
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 20, false, false, null, true, 3, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 20, false, false, null, true ,3, OrderPolicy.THREAD});

    //create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap() });

    //create another RR (RR_2) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap() });
    
    //start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });

    //create another RR (RR_2) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });
    
    //start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR_1", 1000 });
    //do puts in RR_2 in main thread
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR_2", 500 });
    //destroy RR_2 after above puts are complete
    vm4.invoke(WANTestBase.class, "destroyRegion", new Object[] { getTestMethodName() + "_RR_2"});
    
    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    //sleep for some time to let all the events propagate to remote site
    Thread.sleep(20);
    //vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln", 0 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR_1", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR_2", 500 });
  }

  /**
   * 1 region and sender configured on local site and 1 region and a 
   * receiver configured on remote site. Puts to the local region are in progress.
   * Remote region is destroyed in the middle.
   * 
   * @throws Exception
   */
  public void testReplicatedSerialPropagationWithRemoteRegionDestroy() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    //these are part of remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    //these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //senders are created on local site
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 500, false, false, null, true, 5, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 500, false, false, null, true, 5, OrderPolicy.KEY });

    //create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap() });

    //start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });

    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());
    
    //start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR_1", 10000 });
    //destroy RR_1 in remote site
    vm2.invoke(WANTestBase.class, "destroyRegion", new Object[] { getTestMethodName() + "_RR_1"});
    
    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    //verify that all is well in local site. All the events should be present in local region
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR_1", 10000 });
    //assuming some events might have been dispatched before the remote region was destroyed,
    //sender's region queue will have events less than 1000 but the queue will not be empty.
    //NOTE: this much verification might be sufficient in DUnit. Hydra will take care of 
    //more in depth validations.
    vm4.invoke(WANTestBase.class, "verifyRegionQueueNotEmptyForConcurrentSender", new Object[] {"ln" });
  }
  
  /**
   * Two regions configured in local with the same sender and put is in progress 
   * on both the regions. Same two regions are configured on remote site as well.
   * One of the two regions is destroyed in the middle on remote site.
   * 
   * @throws Exception
   */
  public void testReplicatedSerialPropagationWithRemoteRegionDestroy2() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    //these are part of remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    //these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //senders are created on local site
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 200, false, false, null, true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 200, false, false, null, true, 5, OrderPolicy.THREAD });

    //create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap() });

    //create another RR (RR_2) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap() });
    
    //start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });

    //create another RR (RR_2) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });
    //destroy RR_2 on remote site in the middle
    vm2.invoke(WANTestBase.class, "destroyRegion", new Object[] { getTestMethodName() + "_RR_2"});
    
    //expected exceptions in the logs
    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());
    
    //start puts in RR_2 in another thread
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR_2", 1000 });
    
    //start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR_1", 1000 });
   
    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    //though region RR_2 is destroyed, RR_1 should still get all the events put in it 
    //in local site
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR_1", 1000 });

  }

  public void testReplicatedSerialPropagationWithRemoteRegionDestroy3()
      throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    // these are part of remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    // these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    // senders are created on local site
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 200, false, false, null, true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 200, false, false, null, true, 5, OrderPolicy.THREAD });

    // create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap() });

    // create another RR (RR_2) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap() });

    // start the senders on local site
    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException",
        new Object[] { "ln", true });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException",
        new Object[] { "ln", true });

    // start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    // create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap() });

    // create another RR (RR_2) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap() });

    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR_1", 1000 });
    // start puts in RR_2 in another thread
    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR_2", 1000 });
    // destroy RR_2 on remote site in the middle
    vm2.invoke(WANTestBase.class, "destroyRegion", new Object[] { getTestMethodName()
        + "_RR_2" });

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
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          getTestMethodName() + "_RR_1", 1000 });
    } finally {
      System.setProperty(
          "gemfire.GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION", "False");
      vm4.invoke(new CacheSerializableRunnable("UnSetting system property ") {
        public void run2() throws CacheException {
          System.setProperty(
              "gemfire.GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION", "False");
        }
      });

      vm5.invoke(new CacheSerializableRunnable("UnSetting system property ") {
        public void run2() throws CacheException {
          System.setProperty(
              "gemfire.GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION", "False");
        }
      });
    }
  }

}
