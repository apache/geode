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
package com.gemstone.gemfire.internal.cache.wan.asyncqueue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Ignore;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.wan.AsyncEventQueueTestBase;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.Wait;

public class AsyncEventListenerDUnitTest extends AsyncEventQueueTestBase {

  private static final long serialVersionUID = 1L;

  public AsyncEventListenerDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test to verify that AsyncEventQueue can not be created when null listener
   * is passed.
   */
  public void testCreateAsyncEventQueueWithNullListener() {
    AsyncEventQueueTestBase test = new AsyncEventQueueTestBase(getTestMethodName());
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);

    AsyncEventQueueFactory asyncQueueFactory = cache
        .createAsyncEventQueueFactory();
    try {
      asyncQueueFactory.create("testId", null);
      fail("AsyncQueueFactory should not allow to create AsyncEventQueue with null listener");
    }
    catch (IllegalArgumentException e) {
      // expected
    }

  }

  public void testSerialAsyncEventQueueAttributes() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 150, true, true, "testDS", true });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventQueueAttributes",
        new Object[] { "ln", 100, 150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true });
  }
  
  public void testSerialAsyncEventQueueSize() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    Wait.pause(1000);// pause at least for the batchTimeInterval

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });

    int vm4size = (Integer)vm4.invoke(AsyncEventQueueTestBase.class,
        "getAsyncEventQueueSize", new Object[] { "ln" });
    int vm5size = (Integer)vm5.invoke(AsyncEventQueueTestBase.class,
        "getAsyncEventQueueSize", new Object[] { "ln" });
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm4size);
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm5size);
  }
  
  /**
   * Added to reproduce defect #50366: 
   * NullPointerException with AsyncEventQueue#size() when number of dispatchers is more than 1
   */
  public void testConcurrentSerialAsyncEventQueueSize() {
	Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
		"createFirstLocatorWithDSId", new Object[] { 1 });

	vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
	vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
	vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
	vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 150, true, false, null, false, 2, OrderPolicy.KEY });
    vm5.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 150, true, false, null, false, 2, OrderPolicy.KEY });

	vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
		new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
	vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
		new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
	vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
	    new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
	vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
		new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

	vm4
	  .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
		new Object[] { "ln" });
	vm5
	  .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
		new Object[] { "ln" });

	Wait.pause(1000);// pause at least for the batchTimeInterval

	vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
		1000 });

	int vm4size = (Integer)vm4.invoke(AsyncEventQueueTestBase.class,
		"getAsyncEventQueueSize", new Object[] { "ln" });
	int vm5size = (Integer)vm5.invoke(AsyncEventQueueTestBase.class,
		"getAsyncEventQueueSize", new Object[] { "ln" });
	assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm4size);
	assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm5size);
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false
   */

  public void testReplicatedSerialAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }
  
  /**
   * Verify that the events loaded by CacheLoader reach the AsyncEventListener
   * with correct operation detail (added for defect #50237).
   */
  public void testReplicatedSerialAsyncEventQueueWithCacheLoader() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithCacheLoaderAndAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithCacheLoaderAndAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithCacheLoaderAndAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithCacheLoaderAndAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class, "doGets", new Object[] { getTestMethodName() + "_RR",
        10 });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventForOperationDetail",
        new Object[] { "ln", 10, true, false });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventForOperationDetail",
        new Object[] { "ln", 0, true, false });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventForOperationDetail",
        new Object[] { "ln", 0, true, false });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventForOperationDetail",
        new Object[] { "ln", 0, true, false });// secondary
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated 
   * WAN: Serial 
   * Region persistence enabled: false 
   * Async queue persistence enabled: false
   * 
   * Error is thrown from AsyncEventListener implementation while processing the batch.
   * Added to test the fix done for defect #45152.
   */

  public void testReplicatedSerialAsyncEventQueue_ExceptionScenario() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithCustomListener", new Object[] { "ln",
        false, 100, 100, false, false, null, false, 1 });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithCustomListener", new Object[] { "ln",
        false, 100, 100, false, false, null, false, 1 });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithCustomListener", new Object[] { "ln",
        false, 100, 100, false, false, null, false, 1 });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithCustomListener", new Object[] { "ln",
        false, 100, 100, false, false, null, false, 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    
    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    Wait.pause(2000);// pause at least for the batchTimeInterval

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        100 });
    
    vm4.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateCustomAsyncEventListener",
        new Object[] { "ln", 100 });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateCustomAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateCustomAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateCustomAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false AsyncEventQueue conflation enabled: true
   */
  public void testReplicatedSerialAsyncEventQueueWithConflationEnabled() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    Wait.pause(1000);// pause at least for the batchTimeInterval

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
        getTestMethodName() + "_RR", keyValues });

    Wait.pause(1000);
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() });

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    // Put the update events and check the queue size.
    // There should be no conflation with the previous create events.
    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
        getTestMethodName() + "_RR", updateKeyValues });

    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() + updateKeyValues.size() });

    // Put the update events again and check the queue size.
    // There should be conflation with the previous update events.
    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
        getTestMethodName() + "_RR", updateKeyValues });

    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() + updateKeyValues.size() });

    vm4.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * event queue persistence enabled: false
   * 
   * Note: The test doesn't create a locator but uses MCAST port instead.
   */
  @Ignore("Disabled until I can sort out the hydra dependencies - see bug 52214")
  public void DISABLED_testReplicatedSerialAsyncEventQueueWithoutLocator() {
    int mPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    vm4.invoke(AsyncEventQueueTestBase.class, "createCacheWithoutLocator",
        new Object[] { mPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCacheWithoutLocator",
        new Object[] { mPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCacheWithoutLocator",
        new Object[] { mPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCacheWithoutLocator",
        new Object[] { mPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * No VM is restarted.
   */

  public void testReplicatedSerialAsyncEventQueueWithPeristenceEnabled() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * There is only one vm in the site and that vm is restarted
   */

  @Ignore("Disabled for 52351")
  public void DISABLED_testReplicatedSerialAsyncEventQueueWithPeristenceEnabled_Restart() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    String firstDStore = (String)vm4.invoke(AsyncEventQueueTestBase.class,
        "createAsyncEventQueueWithDiskStore", new Object[] { "ln", false, 100,
            100, true, null });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    // pause async channel and then do the puts
    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });

    // ------------------ KILL VM4 AND REBUILD
    // ------------------------------------------
    vm4.invoke(AsyncEventQueueTestBase.class, "killSender", new Object[] {});

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithDiskStore",
        new Object[] { "ln", false, 100, 100, true, firstDStore });
    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    // -----------------------------------------------------------------------------------

    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
  }

  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * There are 3 VMs in the site and the VM with primary sender is shut down.
   */
  @Ignore("Disabled for 52351")
  public void DISABLED_testReplicatedSerialAsyncEventQueueWithPeristenceEnabled_Restart2() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithDiskStore",
        new Object[] { "ln", false, 100, 100, true, null });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithDiskStore",
        new Object[] { "ln", false, 100, 100, true, null });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithDiskStore",
        new Object[] { "ln", false, 100, 100, true, null });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm4.invoke(AsyncEventQueueTestBase.class, "addCacheListenerAndCloseCache",
        new Object[] { getTestMethodName() + "_RR" });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm5.invoke(AsyncEventQueueTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR", 2000 });

    // -----------------------------------------------------------------------------------
    vm5.invoke(AsyncEventQueueTestBase.class, "waitForSenderToBecomePrimary",
        new Object[] { AsyncEventQueueImpl
            .getSenderIdFromAsyncEventQueueId("ln") });
    
    vm5.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    int vm4size = (Integer)vm4.invoke(AsyncEventQueueTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });
    int vm5size = (Integer)vm5.invoke(AsyncEventQueueTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });

    LogWriterUtils.getLogWriter().info("vm4 size is: " + vm4size);
    LogWriterUtils.getLogWriter().info("vm5 size is: " + vm5size);
    // verify that there is no event loss
    assertTrue(
        "Total number of entries in events map on vm4 and vm5 should be at least 2000",
        (vm4size + vm5size) >= 2000);
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated 
   * WAN: Serial 
   * Dispatcher threads: more than 1
   * Order policy: key based ordering
   */
  public void testConcurrentSerialAsyncEventQueueWithReplicatedRegion() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });
    vm5.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });
    vm6.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });
    vm7.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.KEY });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 1000 });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated 
   * WAN: Serial 
   * Region persistence enabled: false 
   * Async queue persistence enabled: false
   */
  public void testConcurrentSerialAsyncEventQueueWithReplicatedRegion_2() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD });
    vm5.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD });
    vm6.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD });
    vm7.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, OrderPolicy.THREAD });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm4.invokeAsync(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        500 });
    vm4.invokeAsync(AsyncEventQueueTestBase.class, "doNextPuts", new Object[] { getTestMethodName() + "_RR",
      500, 1000 });
    vm4.invokeAsync(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
      1000, 1500 });
    
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 1000 });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
  }
  
  /**
   * Dispatcher threads set to more than 1 but no order policy set.
   * Added for defect #50514.
   */
  public void testConcurrentSerialAsyncEventQueueWithoutOrderPolicy() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, null });
    vm5.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, null });
    vm6.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, null });
    vm7.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false, 3, null });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 1000 });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] {"ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false
   */
  public void testPartitionedSerialAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR",
        500 });
    vm5.invoke(AsyncEventQueueTestBase.class, "doPutsFrom", new Object[] {
        getTestMethodName() + "_PR", 500, 1000 });
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: false AsyncEventQueue conflation enabled: true
   */
  public void testPartitionedSerialAsyncEventQueueWithConflationEnabled() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    
    Wait.pause(2000);

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
        getTestMethodName() + "_PR", keyValues });

    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() });

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    // Put the update events and check the queue size.
    // There should be no conflation with the previous create events.
    vm5.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
        getTestMethodName() + "_PR", updateKeyValues });

    vm5.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() + updateKeyValues.size() });

    // Put the update events again and check the queue size.
    // There should be conflation with the previous update events.
    vm5.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
      getTestMethodName() + "_PR", updateKeyValues });

    vm5.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] {
      "ln", keyValues.size() + updateKeyValues.size() });

    vm4.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * No VM is restarted.
   */
  public void testPartitionedSerialAsyncEventQueueWithPeristenceEnabled() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, true, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, true, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, true, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, true, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR",
        500 });
    vm5.invoke(AsyncEventQueueTestBase.class, "doPutsFrom", new Object[] {
        getTestMethodName() + "_PR", 500, 1000 });
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });// secondary
  }

  /**
   * Test configuration::
   * 
   * Region: Partitioned WAN: Serial Region persistence enabled: false Async
   * channel persistence enabled: true
   * 
   * There is only one vm in the site and that vm is restarted
   */
  public void testPartitionedSerialAsyncEventQueueWithPeristenceEnabled_Restart() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    String firstDStore = (String)vm4.invoke(AsyncEventQueueTestBase.class,
        "createAsyncEventQueueWithDiskStore", new Object[] { "ln", false, 100,
            100, true, null });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    // pause async channel and then do the puts
    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueueAndWaitForDispatcherToPause",
            new Object[] { "ln" });
  
    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR",
        1000 });

    // ------------------ KILL VM4 AND REBUILD
    // ------------------------------------------
    vm4.invoke(AsyncEventQueueTestBase.class, "killSender", new Object[] {});

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithDiskStore",
        new Object[] { "ln", false, 100, 100, true, firstDStore });
    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    // -----------------------------------------------------------------------------------

    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
  }

  public void testParallelAsyncEventQueueWithReplicatedRegion() {
    try {
      Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
          "createFirstLocatorWithDSId", new Object[] { 1 });

      vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
      vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
      vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
      vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

      vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] {
          "ln", true, 100, 100, true, false, null, false });
      vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] {
          "ln", true, 100, 100, true, false, null, false });
      vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] {
          "ln", true, 100, 100, true, false, null, false });
      vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] {
          "ln", true, 100, 100, true, false, null, false });

      vm4.invoke(AsyncEventQueueTestBase.class,
          "createReplicatedRegionWithAsyncEventQueue", new Object[] {
              getTestMethodName() + "_RR", "ln", isOffHeap() });
      fail("Expected GatewaySenderConfigException where parallel async event queue can not be used with replicated region");
    }
    catch (Exception e) {
      if (!e.getCause().getMessage()
          .contains("can not be used with replicated region")) {
        fail("Expected GatewaySenderConfigException where parallel async event queue can not be used with replicated region");
      }
    }
  }
  
  public void testParallelAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR",
        256 });
    
    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    int vm4size = (Integer)vm4.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm5size = (Integer)vm5.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm6size = (Integer)vm6.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm7size = (Integer)vm7.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, 256);
  }
  
  /**
   * Verify that the events reaching the AsyncEventListener have correct operation detail.
   * (added for defect #50237).
   */
  public void testParallelAsyncEventQueueWithCacheLoader() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
	  "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
    	true, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
    	true, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
    	true, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
    	true, 100, 100, false, false, null, false, "MyAsyncEventListener_CacheLoader" });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithCacheLoaderAndAsyncQueue",
    	new Object[] { getTestMethodName() + "_PR", "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithCacheLoaderAndAsyncQueue",
    	new Object[] { getTestMethodName() + "_PR", "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithCacheLoaderAndAsyncQueue",
    	new Object[] { getTestMethodName() + "_PR", "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithCacheLoaderAndAsyncQueue",
    	new Object[] { getTestMethodName() + "_PR", "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPutAll", new Object[] { getTestMethodName() + "_PR",
    	100, 10 });
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventForOperationDetail",
    	new Object[] { "ln", 250, false, true });
    vm5.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventForOperationDetail",
    	new Object[] { "ln", 250, false, true });
    vm6.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventForOperationDetail",
    	new Object[] { "ln", 250, false, true });
    vm7.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventForOperationDetail",
    	new Object[] { "ln", 250, false, true });
  }
  
  public void testParallelAsyncEventQueueSize() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    Wait.pause(1000);// pause at least for the batchTimeInterval

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR",
        1000 });

    int vm4size = (Integer)vm4.invoke(AsyncEventQueueTestBase.class,
        "getAsyncEventQueueSize", new Object[] { "ln" });
    int vm5size = (Integer)vm5.invoke(AsyncEventQueueTestBase.class,
        "getAsyncEventQueueSize", new Object[] { "ln" });
    
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm4size);
    assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm5size);
  }
  
  /**
   * Added to reproduce defect #50366: 
   * NullPointerException with AsyncEventQueue#size() when number of dispatchers is more than 1
   */
  public void testConcurrentParallelAsyncEventQueueSize() {
	Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
	  "createFirstLocatorWithDSId", new Object[] { 1 });

	vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
	vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
	vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
	vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

	vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
	  true, 100, 100, false, false, null, false, 2, OrderPolicy.KEY });
	vm5.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
	  true, 100, 100, false, false, null, false, 2, OrderPolicy.KEY });
	vm6.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
	  true, 100, 100, false, false, null, false, 2, OrderPolicy.KEY });
	vm7.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
	  true, 100, 100, false, false, null, false, 2, OrderPolicy.KEY });

	vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
	  new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
	vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
	  new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
	vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
	  new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
	vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
	  new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

	vm4
	  .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
		new Object[] { "ln" });
	vm5
	  .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
		new Object[] { "ln" });
	vm6
	  .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
		new Object[] { "ln" });
	vm7
	  .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
		new Object[] { "ln" });
	Wait.pause(1000);// pause at least for the batchTimeInterval

	vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR",
	  1000 });

	int vm4size = (Integer)vm4.invoke(AsyncEventQueueTestBase.class,
	  "getAsyncEventQueueSize", new Object[] { "ln" });
	int vm5size = (Integer)vm5.invoke(AsyncEventQueueTestBase.class,
	  "getAsyncEventQueueSize", new Object[] { "ln" });
	    
	assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm4size);
	assertEquals("Size of AsyncEventQueue is incorrect", 1000, vm5size);
  }
  
  public void testParallelAsyncEventQueueWithConflationEnabled() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });

    Wait.pause(2000);// pause for the batchTimeInterval to ensure that all the
    // senders are paused

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
        getTestMethodName() + "_PR", keyValues });

    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() });

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
        getTestMethodName() + "_PR", updateKeyValues });

 
    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() + updateKeyValues.size() }); // no conflation of creates

    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
        getTestMethodName() + "_PR", updateKeyValues });

    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() + updateKeyValues.size() }); // conflation of updates

    vm4.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    int vm4size = (Integer)vm4.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm5size = (Integer)vm5.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm6size = (Integer)vm6.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm7size = (Integer)vm7.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, keyValues.size());
  }

  /**
   * Added to reproduce defect #47213
   */
  public void testParallelAsyncEventQueueWithConflationEnabled_bug47213() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, true, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPRWithRedundantCopyWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPRWithRedundantCopyWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPRWithRedundantCopyWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPRWithRedundantCopyWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm6
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm7
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });

    Wait.pause(2000);// pause for the batchTimeInterval to ensure that all the
    // senders are paused

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
        getTestMethodName() + "_PR", keyValues });

    Wait.pause(2000);
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() });

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
        getTestMethodName() + "_PR", updateKeyValues });

    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] {
        getTestMethodName() + "_PR", updateKeyValues });

    // pause to ensure that events have been conflated.
    Wait.pause(2000);
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] {
        "ln", keyValues.size() + updateKeyValues.size() });

    vm4.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    int vm4size = (Integer)vm4.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm5size = (Integer)vm5.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm6size = (Integer)vm6.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm7size = (Integer)vm7.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, keyValues.size());
    
  }

  public void testParallelAsyncEventQueueWithOneAccessor() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm3.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm3.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });

    vm3.invoke(AsyncEventQueueTestBase.class,
        "createPartitionedRegionAccessorWithAsyncEventQueue", new Object[] {
            getTestMethodName() + "_PR", "ln" });
    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    vm3.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR",
        256 });
    
    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    vm3.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });
    
    int vm4size = (Integer)vm4.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm5size = (Integer)vm5.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm6size = (Integer)vm6.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm7size = (Integer)vm7.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, 256);

  }

  public void testParallelAsyncEventQueueWithPersistence() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, true, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, true, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, true, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, true, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR",
        256 });
    
    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    int vm4size = (Integer)vm4.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm5size = (Integer)vm5.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm6size = (Integer)vm6.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    int vm7size = (Integer)vm7.invoke(AsyncEventQueueTestBase.class, "getAsyncEventListenerMapSize",
        new Object[] { "ln"});
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, 256);
  }
  
  /**
   * Below test is disabled intentionally Replicated region with Parallel Async
   * Event queue is not supported. Test is added for the same
   * testParallelAsyncEventQueueWithReplicatedRegion
   * 
   * We are gone support this configuration in upcoming releases
   */
  
  public void DISABLED_DUETO_BUG51491_testReplicatedParallelAsyncEventQueue() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        true, 100, 100, false, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        1000 });

    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    int vm4size = (Integer)vm4.invoke(AsyncEventQueueTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });
    int vm5size = (Integer)vm5.invoke(AsyncEventQueueTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });
    int vm6size = (Integer)vm6.invoke(AsyncEventQueueTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });
    int vm7size = (Integer)vm7.invoke(AsyncEventQueueTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });

    assertEquals(vm4size + vm5size + vm6size + vm7size, 1000);
  }
  
/**
 * Test case to test possibleDuplicates. vm4 & vm5 are hosting the PR. vm5 is
 * killed so the buckets hosted by it are shifted to vm4.
 */
  @Ignore("Disabled for 52349")
  public void DISABLED_testParallelAsyncEventQueueHA_Scenario1() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
      "createFirstLocatorWithDSId", new Object[] { 1 });
    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    LogWriterUtils.getLogWriter().info("Created the cache");

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });

    LogWriterUtils.getLogWriter().info("Created the AsyncEventQueue");

    vm4.invoke(AsyncEventQueueTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            getTestMethodName() + "_PR", "ln", isOffHeap() });

    LogWriterUtils.getLogWriter().info("Created PR with AsyncEventQueue");

    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    Wait.pause(1000);// pause for the batchTimeInterval to make sure the AsyncQueue
                // is paused

    LogWriterUtils.getLogWriter().info("Paused the AsyncEventQueue");

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_PR", 80 });

    LogWriterUtils.getLogWriter().info("Done puts");

    Set<Integer> primaryBucketsVm5 = (Set<Integer>)vm5.invoke(
        AsyncEventQueueTestBase.class, "getAllPrimaryBucketsOnTheNode",
        new Object[] { getTestMethodName() + "_PR" });

    LogWriterUtils.getLogWriter().info("Primary buckets on vm5: " + primaryBucketsVm5);
    // ---------------------------- Kill vm5 --------------------------
    vm5.invoke(AsyncEventQueueTestBase.class, "killSender", new Object[] {});

    Wait.pause(1000);// give some time for rebalancing to happen
    vm4.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class,
        "verifyAsyncEventListenerForPossibleDuplicates", new Object[] { "ln",
            primaryBucketsVm5, 5 });
  }

  /**
   * Test case to test possibleDuplicates. vm4 & vm5 are hosting the PR. vm5 is
   * killed and subsequently vm6 is brought up. Buckets are now rebalanced
   * between vm4 & vm6.
   */
  public void testParallelAsyncEventQueueHA_Scenario2() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    LogWriterUtils.getLogWriter().info("Created the cache");

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });

    LogWriterUtils.getLogWriter().info("Created the AsyncEventQueue");

    vm4.invoke(AsyncEventQueueTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            getTestMethodName() + "_PR", "ln", isOffHeap() });

    LogWriterUtils.getLogWriter().info("Created PR with AsyncEventQueue");

    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    Wait.pause(1000);// pause for the batchTimeInterval to make sure the AsyncQueue
                // is paused

    LogWriterUtils.getLogWriter().info("Paused the AsyncEventQueue");

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_PR", 80 });

    LogWriterUtils.getLogWriter().info("Done puts");

    Set<Integer> primaryBucketsVm5 = (Set<Integer>)vm5.invoke(
        AsyncEventQueueTestBase.class, "getAllPrimaryBucketsOnTheNode",
        new Object[] { getTestMethodName() + "_PR" });

    LogWriterUtils.getLogWriter().info("Primary buckets on vm5: " + primaryBucketsVm5);
    // ---------------------------- Kill vm5 --------------------------
    vm5.invoke(AsyncEventQueueTestBase.class, "killSender", new Object[] {});
    // ----------------------------------------------------------------

    // ---------------------------- start vm6 --------------------------
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });
    vm6.invoke(AsyncEventQueueTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            getTestMethodName() + "_PR", "ln", isOffHeap() });

    // ------------------------------------------------------------------

    Wait.pause(1000);// give some time for rebalancing to happen
    Set<Integer> primaryBucketsVm6 = (Set<Integer>)vm6.invoke(
        AsyncEventQueueTestBase.class, "getAllPrimaryBucketsOnTheNode",
        new Object[] { getTestMethodName() + "_PR" });

    vm4.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    vm6.invoke(AsyncEventQueueTestBase.class,
        "verifyAsyncEventListenerForPossibleDuplicates", new Object[] { "ln",
            primaryBucketsVm6, 5 });
  }

  /**
   * Test case to test possibleDuplicates. vm4 & vm5 are hosting the PR. vm6 is
   * brought up and rebalancing is triggered so the buckets get balanced among
   * vm4, vm5 & vm6.
   */
  @Ignore("Depends on hydra code. See bug ")
  public void testParallelAsyncEventQueueHA_Scenario3() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    LogWriterUtils.getLogWriter().info("Created the cache");

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });

    LogWriterUtils.getLogWriter().info("Created the AsyncEventQueue");

    vm4.invoke(AsyncEventQueueTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            getTestMethodName() + "_PR", "ln", isOffHeap() });

    LogWriterUtils.getLogWriter().info("Created PR with AsyncEventQueue");

    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    vm5
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    Wait.pause(1000);// pause for the batchTimeInterval to make sure the AsyncQueue
                // is paused

    LogWriterUtils.getLogWriter().info("Paused the AsyncEventQueue");

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_PR", 80 });

    LogWriterUtils.getLogWriter().info("Done puts");

    // ---------------------------- start vm6 --------------------------
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueueWithListener2",
        new Object[] { "ln", true, 100, 5, false, null });
    vm6.invoke(AsyncEventQueueTestBase.class,
        "createPRWithRedundantCopyWithAsyncEventQueue", new Object[] {
            getTestMethodName() + "_PR", "ln", isOffHeap() });

    // ------------------------------------------------------------------
    vm4.invoke(AsyncEventQueueTestBase.class, "doRebalance", new Object[] {});

    Set<Integer> primaryBucketsVm6 = (Set<Integer>)vm6.invoke(
        AsyncEventQueueTestBase.class, "getAllPrimaryBucketsOnTheNode",
        new Object[] { getTestMethodName() + "_PR" });
    LogWriterUtils.getLogWriter().info("Primary buckets on vm6: " + primaryBucketsVm6);
    vm4.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue",
        new Object[] { "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    vm6.invoke(AsyncEventQueueTestBase.class,
        "verifyAsyncEventListenerForPossibleDuplicates", new Object[] { "ln",
            primaryBucketsVm6, 5 });
  }
  
  /**
   * Added for defect #50364 Can't colocate region that has AEQ with a region that does not have that same AEQ
   */
  public void testParallelAsyncEventQueueAttachedToChildRegionButNotToParentRegion() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    // create cache on node
    vm3.invoke(AsyncEventQueueTestBase.class, "createCache",
        new Object[] { lnPort });

    // create AsyncEventQueue on node
    vm3.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue",
        new Object[] { "ln", true, 100, 10, false, false, null, true });

    // create leader (parent) PR on node
    vm3.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegion",
        new Object[] { getTestMethodName() + "PARENT_PR", null, 0, 100 });
    String parentRegionFullPath = (String)vm3.invoke(
        AsyncEventQueueTestBase.class, "getRegionFullPath",
        new Object[] { getTestMethodName() + "PARENT_PR" });

    // create colocated (child) PR on node
    vm3.invoke(AsyncEventQueueTestBase.class,
        "createColocatedPartitionedRegionWithAsyncEventQueue", new Object[] {
            getTestMethodName() + "CHILD_PR", "ln", 100, parentRegionFullPath });

    // do puts in colocated (child) PR on node
    vm3.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] {
        getTestMethodName() + "CHILD_PR", 1000 });

    // wait for AsyncEventQueue to get empty on node
    vm3.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    // verify the events in listener
    int vm3size = (Integer)vm3.invoke(AsyncEventQueueTestBase.class,
        "getAsyncEventListenerMapSize", new Object[] { "ln" });
    assertEquals(vm3size, 1000);
  }
  
}
