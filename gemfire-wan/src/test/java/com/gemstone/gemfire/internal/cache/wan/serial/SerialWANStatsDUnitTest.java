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

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase.MyGatewayEventFilter;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;

public class SerialWANStatsDUnitTest extends WANTestBase {
  
  private static final long serialVersionUID = 1L;

  public SerialWANStatsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    addExpectedException("java.net.ConnectException");
    addExpectedException("java.net.SocketException");
    addExpectedException("Unexpected IOException");
  }
  
  public void testReplicatedSerialPropagation() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap()  });

    vm5.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    
    pause(2000);
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {100, 1000, 1000 });
    
    vm4.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"ln",
      0, 1000, 1000, 1000});
    vm4.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"ln",
      100});
    
    vm5.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"ln",
      0, 1000, 0, 0});
    vm5.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"ln",
      0});
    
  }
  
  public void testReplicatedSerialPropagationWithMultipleDispatchers() throws Exception {
	Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
		"createFirstLocatorWithDSId", new Object[] { 1 });
	Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
		"createFirstRemoteLocator", new Object[] { 2, lnPort });

	vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

	vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
	vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
	vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
	vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

	vm4.invoke(WANTestBase.class, "createSenderWithMultipleDispatchers", new Object[] { "ln", 2,
		false, 100, 10, false, false, null, true, 2, OrderPolicy.KEY });
	vm5.invoke(WANTestBase.class, "createSenderWithMultipleDispatchers", new Object[] { "ln", 2,
		false, 100, 10, false, false, null, true, 2, OrderPolicy.KEY });

	vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
		testName + "_RR", null, isOffHeap()  });

	vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
	vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

	vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
		testName + "_RR", "ln", isOffHeap()  });
	vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
		testName + "_RR", "ln", isOffHeap()  });
	vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
		testName + "_RR", "ln", isOffHeap()  });
	vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
		testName + "_RR", "ln", isOffHeap()  });

	vm5.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
		1000 });

	vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
		testName + "_RR", 1000 });
	    
	pause(2000);
	vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {100, 1000, 1000 });
	    
	vm4.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"ln",
		0, 1000, 1000, 1000});
	vm4.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"ln",
		100});
	    
	vm5.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"ln",
		0, 1000, 0, 0});
	vm5.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"ln",
		0});
	    
	  }
  
  public void testWANStatsTwoWanSites() throws Exception {

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
        testName + "_RR", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial1" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial1" });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial2" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial2" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial1,lnSerial2", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial1,lnSerial2", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial1,lnSerial2", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial1,lnSerial2", isOffHeap()  });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    
    pause(2000);
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {100, 1000, 1000 });
    vm3.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {100, 1000, 1000 });
    
    vm4.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"lnSerial1",
      0, 1000, 1000, 1000});
    vm4.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"lnSerial1",
      100});
    vm4.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"lnSerial2",
      0, 1000, 1000, 1000});
    vm4.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"lnSerial2",
      100});
    vm5.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"lnSerial1",
      0, 1000, 0, 0});
    vm5.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"lnSerial1",
      0});
    vm5.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"lnSerial2",
      0, 1000, 0, 0});
    vm5.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"lnSerial2",
      0});
    
  }

  public void testReplicatedSerialPropagationHA() throws Exception {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap()  });
    
    AsyncInvocation inv1 = vm5.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { testName + "_RR", 10000 });
    pause(2000);
    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "killSender", new Object[] { "ln" });
    Boolean isKilled = Boolean.FALSE;
    try {
      isKilled = (Boolean)inv2.getResult();
    }
    catch (Throwable e) {
      fail("Unexpected exception while killing a sender");
    }
    AsyncInvocation inv3 = null; 
    if(!isKilled){
      inv3 = vm5.invokeAsync(WANTestBase.class, "killSender", new Object[] { "ln" });
      inv3.join();
    }
    inv1.join();
    inv2.join();
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 10000 });
  
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStatsHA", new Object[] {1000, 10000, 10000 });
    
    vm5.invoke(WANTestBase.class, "checkStats_Failover", new Object[] {"ln", 10000});
  }
  
  public void testReplicatedSerialPropagationUNPorcessedEvents() throws Exception {
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
        testName + "_RR_1", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_1", null, isOffHeap()  });

    //create another RR (RR_2) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_2", null, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_2", null, isOffHeap()  });
    
    //start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_1", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_1", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_1", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_1", "ln", isOffHeap()  });

    //create another RR (RR_2) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_2", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_2", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_2", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_2", "ln", isOffHeap()  });
    
    //start puts in RR_1 in another thread
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR_1", 1000 });
    //do puts in RR_2 in main thread
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR_2", 500 });
    //sleep for some time to let all the events propagate to remote site
    Thread.sleep(20);
    //vm4.invoke(WANTestBase.class, "verifyQueueSize", new Object[] { "ln", 0 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR_1", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR_2", 500 });
    
    pause(2000);
    vm4.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"ln",
      0, 1500, 1500, 1500});
    vm4.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"ln",
      75});
    vm4.invoke(WANTestBase.class, "checkUnProcessedStats", new Object[] {"ln", 0});
    
    
    vm5.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"ln",
      0, 1500, 0, 0});
    vm5.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"ln",
      0});
    vm5.invoke(WANTestBase.class, "checkUnProcessedStats", new Object[] {"ln", 1500});
  }
  
  /**
   * 
   * Disabled - see ticket #52118
   * 
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

    //these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //senders are created on local site
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 100, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 100, false, false, null, true });

    //create one RR (RR_1) on remote site
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_1", null, isOffHeap()  });

    //start the senders on local site
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //create one RR (RR_1) on local site
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_1", "ln", isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_1", "ln", isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_1", "ln", isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR_1", "ln", isOffHeap()  });

    //start puts in RR_1 in another thread
    AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_RR_1", 20000 });
    //destroy RR_1 in remote site
    vm2.invoke(WANTestBase.class, "destroyRegion", new Object[] { testName + "_RR_1", 500});
    
    try {
      inv1.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }

    //verify that all is well in local site. All the events should be present in local region
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR_1", 20000 });
    //assuming some events might have been dispatched before the remote region was destroyed,
    //sender's region queue will have events less than 1000 but the queue will not be empty.
    //NOTE: this much verification might be sufficient in DUnit. Hydra will take care of 
    //more in depth validations.
    vm4.invoke(WANTestBase.class, "verifyRegionQueueNotEmpty", new Object[] {"ln" });
    
    vm4.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"ln", true, true});
    
    vm5.invoke(WANTestBase.class, "checkUnProcessedStats", new Object[] {"ln", 20000});
    
    vm2.invoke(WANTestBase.class, "checkExcepitonStats", new Object[] {1});
    
  }
  
  public void testSerialPropogationWithFilter() throws Exception {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class, "createFirstLocatorWithDSId",
        new Object[] {1});
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] {2,lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false,
        new MyGatewayEventFilter(), true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false,
        new MyGatewayEventFilter(), true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName, 1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 800 });
    
    pause(2000);
    vm4.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"ln",
      0, 1000, 900, 800});
    vm4.invoke(WANTestBase.class, "checkEventFilteredStats", new Object[] {"ln",
      200});
    vm4.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"ln",
      80});
    vm4.invoke(WANTestBase.class, "checkUnProcessedStats", new Object[] {"ln", 0});
    
    
    vm5.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"ln",
      0, 1000, 0, 0});
    vm5.invoke(WANTestBase.class, "checkBatchStats", new Object[] {"ln",
      0});
    vm5.invoke(WANTestBase.class, "checkUnProcessedStats", new Object[] {"ln",900});
  }
  
  public void testSerialPropagationConflation() throws Exception {
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
        false, 100, 10, true, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap()  });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap()  });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap()  });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap()  });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null,1, 100, isOffHeap()  });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null,1, 100, isOffHeap()  });

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for(int i=0; i< 1000; i++) {
      keyValues.put(i, i);
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, keyValues });

    pause(5000);
    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size() });
    for(int i=0;i<500;i++) {
      updateKeyValues.put(i, i+"_updated");
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, updateKeyValues });

    pause(5000);
    
    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size()  + updateKeyValues.size() });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 0 });
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, updateKeyValues });

    pause(5000);
    
    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size()  + updateKeyValues.size() });

    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

    keyValues.putAll(updateKeyValues);
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, keyValues.size() });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName, keyValues.size() });
    
    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        testName, keyValues });
    vm3.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        testName, keyValues });
    
    pause(2000);
    vm4.invoke(WANTestBase.class, "checkQueueStats", new Object[] {"ln",
      0, 2000, 2000, 1500});
    vm4.invoke(WANTestBase.class, "checkConflatedStats", new Object[] {"ln",
      500});
  }
    
}
