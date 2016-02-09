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

import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.AsyncEventQueueTestBase;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;

/**
 * @author skumar
 *
 */
public class ConcurrentAsyncEventQueueDUnitTest extends AsyncEventQueueTestBase {

  private static final long serialVersionUID = 1L;

  public ConcurrentAsyncEventQueueDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }
  
  public void testConcurrentSerialAsyncEventQueueAttributes() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 150, true, true, "testDS", true, 5, OrderPolicy.THREAD });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateConcurrentAsyncEventQueueAttributes",
        new Object[] { "ln", 100, 150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true, 5, OrderPolicy.THREAD });
  }
  
 
  public void testConcurrentParallelAsyncEventQueueAttributesOrderPolicyKey() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 150, true, true, "testDS", true, 5, OrderPolicy.KEY });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateConcurrentAsyncEventQueueAttributes",
        new Object[] { "ln", 100, 150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true, 5, OrderPolicy.KEY });
  }

  public void testConcurrentParallelAsyncEventQueueAttributesOrderPolicyPartition() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 150, true, true, "testDS", true, 5, OrderPolicy.PARTITION });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateConcurrentAsyncEventQueueAttributes",
        new Object[] { "ln", 100, 150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true, 5, OrderPolicy.PARTITION });
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated 
   * WAN: Serial 
   * Dispatcher threads: more than 1
   * Order policy: key based ordering
   */

  public void testReplicatedSerialAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyKey() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 10, true, false, null, false, 3, OrderPolicy.KEY });
    vm5.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 10, true, false, null, false, 3, OrderPolicy.KEY });
    vm6.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 10, true, false, null, false, 3, OrderPolicy.KEY });
    vm7.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 10, true, false, null, false, 3, OrderPolicy.KEY });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        100 });
    
    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 100 });// primary sender
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
   * Region: Replicated 
   * WAN: Serial 
   * Dispatcher threads: more than 1
   * Order policy: Thread ordering
   */

  public void testReplicatedSerialAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyThread() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 10, true, false, null, false, 3, OrderPolicy.THREAD });
    vm5.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 10, true, false, null, false, 3, OrderPolicy.THREAD });
    vm6.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 10, true, false, null, false, 3, OrderPolicy.THREAD });
    vm7.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        false, 100, 10, true, false, null, false, 3, OrderPolicy.THREAD });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_RR", "ln", isOffHeap() });

    AsyncInvocation inv1 = vm4.invokeAsync(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_RR",
        50 });
    AsyncInvocation inv2 = vm4.invokeAsync(AsyncEventQueueTestBase.class, "doNextPuts", new Object[] { getTestMethodName() + "_RR",
      50, 100 });
    AsyncInvocation inv3 = vm4.invokeAsync(AsyncEventQueueTestBase.class, "doNextPuts", new Object[] { getTestMethodName() + "_RR",
      100, 150 });
    
    try {
      inv1.join();
      inv2.join();
      inv3.join();
    } catch (InterruptedException ie) {
      Assert.fail(
          "Cought interrupted exception while waiting for the task tgo complete.",
          ie);
    }
    
    vm4.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm5.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm6.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });
    vm7.invoke(AsyncEventQueueTestBase.class, "waitForAsyncQueueToGetEmpty",
        new Object[] { "ln" });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 150 });// primary sender
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
   * Region: PartitionedRegion 
   * WAN: Parallel
   * Dispatcher threads: more than 1
   * Order policy: key based ordering
   */
  // Disabling test for bug #48323
  public void testPartitionedParallelAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyKey() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 10, true, false, null, false, 3, OrderPolicy.KEY });
    vm5.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 10, true, false, null, false, 3, OrderPolicy.KEY });
    vm6.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 10, true, false, null, false, 3, OrderPolicy.KEY });
    vm7.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue", new Object[] { "ln",
        true, 100, 10, true, false, null, false, 3, OrderPolicy.KEY });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR",
        100 });
    
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
  
    assertEquals(vm4size + vm5size + vm6size + vm7size, 100);
  
  }
  
  
  /**
   * Test configuration::
   * 
   * Region: PartitionedRegion 
   * WAN: Parallel
   * Dispatcher threads: more than 1
   * Order policy: PARTITION based ordering
   */
  // Disabled test for bug #48323
  public void testPartitionedParallelAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyPartition() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue",
        new Object[] { "ln", true, 100, 10, true, false, null, false, 3,
            OrderPolicy.PARTITION });
    vm5.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue",
        new Object[] { "ln", true, 100, 10, true, false, null, false, 3,
            OrderPolicy.PARTITION });
    vm6.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue",
        new Object[] { "ln", true, 100, 10, true, false, null, false, 3,
            OrderPolicy.PARTITION });
    vm7.invoke(AsyncEventQueueTestBase.class, "createConcurrentAsyncEventQueue",
        new Object[] { "ln", true, 100, 10, true, false, null, false, 3,
            OrderPolicy.PARTITION });

    vm4.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createPartitionedRegionWithAsyncEventQueue",
        new Object[] { getTestMethodName() + "_PR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { getTestMethodName() + "_PR",
        100 });

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

    assertEquals(100, vm4size + vm5size + vm6size + vm7size);
  }
}
