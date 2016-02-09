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

import java.io.IOException;
import java.util.Map;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.wan.BatchException70;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.Wait;

public class SerialWANPropogationDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public SerialWANPropogationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection refused");
    IgnoredException.addIgnoredException("could not get remote locator information");
    IgnoredException.addIgnoredException("Unexpected IOException");
  }
  
  // this test is disabled due to a high rate of failure in unit test runs
  // see ticket #52190
  public void disabledtestReplicatedSerialPropagation_withoutRemoteLocator() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    //keep the batch size high enough to reduce the number of exceptions in the log
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 400, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 400, false, false, null, true });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()   });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()    });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
      1000 });

    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
  
    vm2.invoke(WANTestBase.class, "createReceiver2",
        new Object[] {nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver2",
        new Object[] {nyPort });
    
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
  } 

  
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
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 400, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 400, false, false, null, true });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    
    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());

    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] {
      "ln", false });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] {
      "ln", false });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
      1000 });
    
    vm2.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
  
    vm2.invoke(WANTestBase.class, "createReceiver2",
        new Object[] {nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver2",
        new Object[] {nyPort });
    
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
  }
  
  /**
   * Added to reproduce the bug #46595
   * 
   * @throws Exception
   */
  public void testReplicatedSerialPropagationWithoutRemoteSite_defect46595()
      throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    // reduce the batch-size so maximum number of batches will be sent
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 5, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 5, false, false, null, true });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });

    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());
    IgnoredException.addIgnoredException(IOException.class.getName());
    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] {
      "ln", false });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] {
      "ln", false });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        10000 });

    // pause for some time before starting up the remote site
    Wait.pause(10000);

    vm2.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", null, isOffHeap()  });
  
    vm2.invoke(WANTestBase.class, "createReceiver2", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver2", new Object[] { nyPort });



    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 10000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 10000 });

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

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()   });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()   });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
  }
  
  public void testReplicatedSerialPropagationWithLocalSiteClosedAndRebuilt() throws Exception {
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
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()   });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()   });
    
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

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      false, 100, 10, false, false, null, true });
  
    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR", "ln", isOffHeap()  });
    
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
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 20, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 20, false, false, null, true });

    //create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });

    //create another RR (RR_2) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap()  });
    
    //start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });

    //create another RR (RR_2) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });
    
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
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 500, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 500, false, false, null, true });

    //create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });

    //start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });

    IgnoredException.addIgnoredException(BatchException70.class.getName());
    IgnoredException.addIgnoredException(ServerOperationException.class.getName());
    
    //start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR_1", 1000 });
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
        getTestMethodName() + "_RR_1", 1000 });
    //assuming some events might have been dispatched before the remote region was destroyed,
    //sender's region queue will have events less than 1000 but the queue will not be empty.
    //NOTE: this much verification might be sufficient in DUnit. Hydra will take care of 
    //more in depth validations.
    vm4.invoke(WANTestBase.class, "verifyRegionQueueNotEmpty", new Object[] {"ln" });
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
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 200, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 200, false, false, null, true });

    //create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });

    //create another RR (RR_2) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap()  });
    
    //start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });

    //create another RR (RR_2) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });
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
  
  /**
   * In this test the system property gemfire.GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION is set to true.
   * This will ensure that other events which did not cause BatchException can reach remote site.
   */
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
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 200, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 200, false, false, null, true });

    // create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });

    // create another RR (RR_2) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", null, isOffHeap()  });
    
    // start the senders on local site
    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    vm5.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] { "ln", true });
    
    // start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    // create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });

    // create another RR (RR_2) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_2", "ln", isOffHeap()  });

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
  
  /**
   * one region and sender configured on local site and the same region and a 
   * receiver configured on remote site. Puts to the local region are in progress.
   * Receiver on remote site is stopped in the middle by closing remote site cache.
   * 
   * @throws Exception
   */
  public void testReplicatedSerialPropagationWithRemoteReceiverStopped() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    //these are part of remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    //vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    //these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //senders are created on local site. Batch size is kept to a high (170) so 
    //there will be less number of exceptions (occur during dispatchBatch) in the log 
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 350, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 350, false, false, null, true });

    //create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });
    //vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
    //    testName + "_RR_1", null, isOffHeap()  });

    //start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });

    //start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR_1", 500 });
    //close cache in remote site. This will automatically kill the remote receivers.
    vm2.invoke(WANTestBase.class, "closeCache");
    //vm3.invoke(WANTestBase.class, "closeCache");
    
    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    //verify that all is well in local site
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR_1", 500 });
    vm4.invoke(WANTestBase.class, "verifyRegionQueueNotEmpty", new Object[] {"ln" });
  }
  
  public void testReplicatedSerialPropagationWithRemoteReceiverRestarted()
      throws Exception {
    Integer lnPort = (Integer) vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer) vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    // these are part of remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    // these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    // senders are created on local site. Batch size is kept to a high (170) so
    // there will be less number of exceptions (occur during dispatchBatch) in
    // the log
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 350, false, false, null, true });

    // create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });

    // start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    // create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });

    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] {
      "ln", false });
    
    // start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR_1", 8000 });
    // close cache in remote site. This will automatically kill the remote
    // receivers.
    Wait.pause(2000);
    vm2.invoke(WANTestBase.class, "closeCache");
    // vm3.invoke(WANTestBase.class, "closeCache");

    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    
    vm4.invoke(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR_1", 1000 });

    // verify that all is well in local site
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR_1", 8000 });

//    vm4.invoke(WANTestBase.class, "verifyRegionQueueNotEmpty",
//        new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "validateQueueContents", new Object[] { "ln",
        0 });

    vm2.invoke(WANTestBase.class, "checkMinimumGatewayReceiverStats",
        new Object[] { 1, 1 });
  }

  public void testReplicatedSerialPropagationWithRemoteReceiverRestarted_SenderReceiverPersistent()
      throws Exception {
    Integer lnPort = (Integer) vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer) vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    // these are part of remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    // these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    // senders are created on local site. Batch size is kept to a high (170) so
    // there will be less number of exceptions (occur during dispatchBatch) in
    // the log
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 350, false, true, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 350, false, true, null, true });

    // create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { getTestMethodName() + "_RR_1", null, isOffHeap()  });

    // start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    // create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });

    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] {
      "ln", false });
    
    // start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR_1", 8000 });
    // close cache in remote site. This will automatically kill the remote
    // receivers.
    Wait.pause(2000);
    vm2.invoke(WANTestBase.class, "closeCache");
    // vm3.invoke(WANTestBase.class, "closeCache");

    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    // verify that all is well in local site
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR_1", 8000 });

    vm4.invoke(WANTestBase.class, "verifyRegionQueueNotEmpty",
        new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createPersistentReplicatedRegion",
        new Object[] { getTestMethodName() + "_RR_1", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "validateQueueContents", new Object[] { "ln",
        0 });

    vm2.invoke(WANTestBase.class, "checkMinimumGatewayReceiverStats",
        new Object[] { 1, 1 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR_1", 8000 });
  }

  public void testReplicatedSerialPropagationWithRemoteSiteBouncedBack_ReceiverPersistent()
      throws Exception {
    Integer lnPort = (Integer) vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer) vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    // these are part of remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    // these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    // senders are created on local site. Batch size is kept to a high (170) so
    // there will be less number of exceptions (occur during dispatchBatch) in
    // the log
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 350, false, false, null, true });

    // create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });

    // start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    // create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });
    
    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] {
      "ln", false });
    // start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR_1", 8000 });
    // close cache in remote site. This will automatically kill the remote
    // receivers.
    Wait.pause(2000);
    vm1.invoke(WANTestBase.class, "shutdownLocator");
    vm2.invoke(WANTestBase.class, "closeCache");
    // vm3.invoke(WANTestBase.class, "closeCache");

    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    // Do some extra puts after cache close so that some events are in the queue.
    vm4.invoke(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR_1", 1000 });

    // verify that all is well in local site
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR_1", 8000 });

    vm4.invoke(WANTestBase.class, "verifyRegionQueueNotEmpty",
        new Object[] { "ln" });

    vm1.invoke(WANTestBase.class, "bringBackLocatorOnOldPort", new Object[] {
        2, lnPort, nyPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "validateQueueContents", new Object[] { "ln",
        0 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName() + "_RR_1", 8000 });
    
    vm2.invoke(WANTestBase.class, "checkMinimumGatewayReceiverStats",
        new Object[] { 1, 1 });
  }

  public void testReplicatedSerialPropagationWithRemoteSiteBouncedBackWithMultipleRemoteLocators()
      throws Exception {
    Integer lnPort = (Integer) vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort1 = (Integer) vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer nyPort2 = (Integer) vm3.invoke(WANTestBase.class,
        "createSecondRemoteLocator", new Object[] { 2, nyPort1, lnPort });

    // these are part of remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort1 });

    // these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    // senders are created on local site. Batch size is kept to a high (170) so
    // there will be less number of exceptions (occur during dispatchBatch) in
    // the log
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 350, false, false, null, true });

    // create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });

    // start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    // create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });

    vm4.invoke(WANTestBase.class, "setRemoveFromQueueOnException", new Object[] {
      "ln", false });
    // start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR_1", 8000 });
    // close cache in remote site. This will automatically kill the remote
    // receivers.
    Wait.pause(2000);
    vm1.invoke(WANTestBase.class, "shutdownLocator");
    vm2.invoke(WANTestBase.class, "closeCache");
    // vm3.invoke(WANTestBase.class, "closeCache");

    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    vm4.invoke(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR_1", 1000 });
    // verify that all is well in local site
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR_1", 8000 });

    vm4.invoke(WANTestBase.class, "verifyRegionQueueNotEmpty",
        new Object[] { "ln" });

    vm6.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort2 });

    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "validateQueueContents", new Object[] { "ln",
        0 });

    vm6.invoke(WANTestBase.class, "checkMinimumGatewayReceiverStats",
        new Object[] { 1, 1 });
  }

  public void testReplicatedSerialPropagationWithRemoteReceiverRestartedOnOtherNode()
      throws Exception {
    Integer lnPort = (Integer) vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer) vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    // these are part of remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });

    // these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    // senders are created on local site. Batch size is kept to a high (170) so
    // there will be less number of exceptions (occur during dispatchBatch) in
    // the log
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 350, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 350, false, false, null, true });

    // create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", null, isOffHeap()  });

    vm3.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR_1", null, isOffHeap()  });
    
    // start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    // create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR_1", "ln", isOffHeap()  });

    // start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR_1", 8000 });
    // close cache in remote site. This will automatically kill the remote
    // receivers.
    vm2.invoke(WANTestBase.class, "closeCache");
    vm3.invoke(WANTestBase.class, "closeCache");
    
    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    // verify that all is well in local site
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR_1", 8000 });

    vm4.invoke(WANTestBase.class, "verifyRegionQueueNotEmpty",
        new Object[] { "ln" });

    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createPersistentReplicatedRegion", new Object[] {
      getTestMethodName() + "_RR_1", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "validateQueueContents", new Object[] { "ln",
        0 });

    vm3.invoke(WANTestBase.class, "checkMinimumGatewayReceiverStats",
        new Object[] { 1, 1 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName() + "_RR_1", 8000 });
  }
  
  public void testReplicatedSerialPropagationToTwoWanSites() throws Exception {

    Integer lnPort = createFirstLocatorWithDSId(1);
    Integer nyPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "lnSerial1",
        2, false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "lnSerial1",
        2, false, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "lnSerial2",
        3, false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "lnSerial2",
        3, false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial1" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial1" });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial2" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial2" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "lnSerial1,lnSerial2", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "lnSerial1,lnSerial2", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "lnSerial1,lnSerial2", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "lnSerial1,lnSerial2", isOffHeap()  });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
  }

  public void testReplicatedSerialPropagationHA() throws Exception {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });

    AsyncInvocation inv1 = vm5.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR", 10000 });
    Wait.pause(2000);
    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "killSender");
    
    inv1.join();
    inv2.join();
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 10000 });
  }
  
  /**
   * Local site:: vm4: Primary vm5: Secondary
   * 
   * Remote site:: vm2, vm3, vm6, vm7: All hosting receivers
   * 
   * vm4 is killed, so vm5 takes primary charge
   * 
   * @throws Exception
   */
  // commenting due to connection information not available in open source
  // enable this once in closed source
  public void SURtestReplicatedSerialPropagationHA_ReceiverAffinity()
      throws Exception {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm6.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm7.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    LogWriterUtils.getLogWriter().info("Started receivers on remote site");

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    LogWriterUtils.getLogWriter().info("Started senders on local site");

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });

    AsyncInvocation inv1 = vm5.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR", 10000 });
    LogWriterUtils.getLogWriter().info("Started async puts on local site");
    Wait.pause(1000);

    Map oldConnectionInfo = (Map)vm4.invoke(WANTestBase.class,
        "getSenderToReceiverConnectionInfo", new Object[] { "ln" });
    assertNotNull(oldConnectionInfo);
    String oldServerHost = (String)oldConnectionInfo.get("serverHost");
    int oldServerPort = (Integer)oldConnectionInfo.get("serverPort");
    LogWriterUtils.getLogWriter().info("Got sender to receiver connection information");

    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "killSender");
    inv2.join();
    LogWriterUtils.getLogWriter().info("Killed primary sender on local site");
    Wait.pause(5000);// give some time for vm5 to take primary charge

    Map newConnectionInfo = (Map)vm5.invoke(WANTestBase.class,
        "getSenderToReceiverConnectionInfo", new Object[] { "ln" });
    assertNotNull(newConnectionInfo);
    String newServerHost = (String)newConnectionInfo.get("serverHost");
    int newServerPort = (Integer)newConnectionInfo.get("serverPort");
    LogWriterUtils.getLogWriter().info("Got new sender to receiver connection information");
    assertEquals(oldServerHost, newServerHost);
    assertEquals(oldServerPort, newServerPort);

    LogWriterUtils.getLogWriter()
        .info(
            "Matched the new connection info with old connection info. Receiver affinity verified.");

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 10000 });
  }

  /**
   * Local site:: vm4: Primary vm5: Secondary
   * 
   * Remote site:: vm2, vm3, vm6, vm7: All hosting receivers
   * 
   * vm4 is killed, so vm5 takes primary charge. vm4 brought up. vm5 is killed,
   * so vm4 takes primary charge again.
   * 
   * @throws Exception
   */
  // commenting due to connection information not available in open source
  // enable this once in closed source
  public void SURtestReplicatedSerialPropagationHA_ReceiverAffinityScenario2()
      throws Exception {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm6.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm7.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    LogWriterUtils.getLogWriter().info("Started receivers on remote site");

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    LogWriterUtils.getLogWriter().info("Started senders on local site");

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });

    AsyncInvocation inv1 = vm5.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR", 10000 });
    LogWriterUtils.getLogWriter().info("Started async puts on local site");
    Wait.pause(1000);

    Map oldConnectionInfo = (Map)vm4.invoke(WANTestBase.class,
        "getSenderToReceiverConnectionInfo", new Object[] { "ln" });
    assertNotNull(oldConnectionInfo);
    String oldServerHost = (String)oldConnectionInfo.get("serverHost");
    int oldServerPort = (Integer)oldConnectionInfo.get("serverPort");
    LogWriterUtils.getLogWriter().info("Got sender to receiver connection information");

    // ---------------------------- KILL vm4
    // --------------------------------------
    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "killSender");
    inv2.join();
    LogWriterUtils.getLogWriter().info("Killed vm4 (primary sender) on local site");
    // -----------------------------------------------------------------------------

    vm5.invoke(WANTestBase.class, "waitForSenderToBecomePrimary",
        new Object[] { "ln" });
    LogWriterUtils.getLogWriter().info("vm5 sender has now acquired primary status");
    Wait.pause(5000);// give time to process unprocessedEventsMap

    // ---------------------------REBUILD vm4
    // --------------------------------------
    LogWriterUtils.getLogWriter().info("Rebuilding vm4....");
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap()  });
    LogWriterUtils.getLogWriter().info("Rebuilt vm4");
    // -----------------------------------------------------------------------------

    // --------------------------- KILL vm5
    // ----------------------------------------
    inv1.join();// once the puts are done, kill vm5
    LogWriterUtils.getLogWriter().info("puts in vm5 are done");

    inv2 = vm5.invokeAsync(WANTestBase.class, "killSender");
    inv2.join();
    vm4.invoke(WANTestBase.class, "waitForSenderToBecomePrimary",
        new Object[] { "ln" });
    // -----------------------------------------------------------------------------

    Map newConnectionInfo = (Map)vm4.invoke(WANTestBase.class,
        "getSenderToReceiverConnectionInfo", new Object[] { "ln" });
    assertNotNull(newConnectionInfo);
    String newServerHost = (String)newConnectionInfo.get("serverHost");
    int newServerPort = (Integer)newConnectionInfo.get("serverPort");
    LogWriterUtils.getLogWriter().info("Got new sender to receiver connection information");
    assertEquals(oldServerHost, newServerHost);
    assertEquals(oldServerPort, newServerPort);
    LogWriterUtils.getLogWriter()
        .info(
            "Matched the new connection info with old connection info. Receiver affinity verified.");

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 10000 });
  }

  public void testNormalRegionSerialPropagation() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    Wait.pause(500);
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createNormalRegion", new Object[] {
        getTestMethodName() + "_RR", "ln" });
    vm5.invoke(WANTestBase.class, "createNormalRegion", new Object[] {
        getTestMethodName() + "_RR", "ln" });

    vm5.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });

    vm4.invoke(WANTestBase.class, "checkQueueStats", new Object[] { "ln", 0,
        0, 0, 0});

    vm5.invoke(WANTestBase.class, "checkQueueStats", new Object[] { "ln", 0,
        1000, 0, 0 });
    
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName() + "_RR", 1000});

    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      getTestMethodName() + "_RR", 0});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 0 });
    
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {0, 0, 0});
    
  }
  
  /**
   * Added for defect #48582 NPE when WAN sender configured but not started.
   * All to all topology with 2 WAN sites:
   * Site 1 (LN site): vm4, vm5, vm6, vm7
   * Site 2 (NY site): vm2, vm3
   */
  public void testReplicatedSerialPropagationWithRemoteSenderConfiguredButNotStarted() {
	Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
		"createFirstLocatorWithDSId", new Object[] { 1 });
	Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
		"createFirstRemoteLocator", new Object[] { 2, lnPort });
	
	vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
	vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
	
	vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
	vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    
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
    
    vm2.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 1,
        false, 100, 10, false, false, null, true });
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 1,
        false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ny", isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ny", isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });
    
    vm2.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 1000 });
  }
}
