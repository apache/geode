/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.wan.concurrent;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.wan.AsyncEventQueueTestBase;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.junit.categories.AEQTest;

@Category({AEQTest.class})
public class ConcurrentAsyncEventQueueDUnitTest extends AsyncEventQueueTestBase {

  private static final long serialVersionUID = 1L;

  public ConcurrentAsyncEventQueueDUnitTest() {
    super();
  }

  @Test
  public void testConcurrentSerialAsyncEventQueueAttributes() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", false, 100, 150,
        true, true, "testDS", true, 5, OrderPolicy.THREAD));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateConcurrentAsyncEventQueueAttributes("ln", 100,
        150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true, 5,
        OrderPolicy.THREAD));
  }


  @Test
  public void testConcurrentParallelAsyncEventQueueAttributesOrderPolicyKey() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", true, 100, 150,
        true, true, "testDS", true, 5, OrderPolicy.KEY));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateConcurrentAsyncEventQueueAttributes("ln", 100,
        150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true, 5,
        OrderPolicy.KEY));
  }

  @Test
  public void testConcurrentParallelAsyncEventQueueAttributesOrderPolicyPartition() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", true, 100, 150,
        true, true, "testDS", true, 5, OrderPolicy.PARTITION));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateConcurrentAsyncEventQueueAttributes("ln", 100,
        150, AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL, true, "testDS", true, true, 5,
        OrderPolicy.PARTITION));
  }

  /**
   * Test configuration::
   *
   * Region: Replicated WAN: Serial Dispatcher threads: more than 1 Order policy: key based ordering
   */

  @Test
  public void testReplicatedSerialAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyKey() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm3.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm4.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", false, 100, 10,
        true, false, null, false, 3, OrderPolicy.KEY));
    vm2.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", false, 100, 10,
        true, false, null, false, 3, OrderPolicy.KEY));
    vm3.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", false, 100, 10,
        true, false, null, false, 3, OrderPolicy.KEY));
    vm4.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", false, 100, 10,
        true, false, null, false, 3, OrderPolicy.KEY));

    vm1.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm3.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm4.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts(getTestMethodName() + "_RR", 100));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm4.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 100));// primary
                                                                                    // sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0));// secondary
  }

  /**
   * Test configuration::
   *
   * Region: Replicated WAN: Serial Dispatcher threads: more than 1 Order policy: Thread ordering
   */

  @Test
  public void testReplicatedSerialAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyThread() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm3.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm4.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", false, 100, 10,
        true, false, null, false, 3, OrderPolicy.THREAD));
    vm2.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", false, 100, 10,
        true, false, null, false, 3, OrderPolicy.THREAD));
    vm3.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", false, 100, 10,
        true, false, null, false, 3, OrderPolicy.THREAD));
    vm4.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", false, 100, 10,
        true, false, null, false, 3, OrderPolicy.THREAD));

    vm1.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm3.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm4.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));

    AsyncInvocation inv1 =
        vm1.invokeAsync(() -> AsyncEventQueueTestBase.doPuts(getTestMethodName() + "_RR", 50));
    AsyncInvocation inv2 = vm1.invokeAsync(
        () -> AsyncEventQueueTestBase.doNextPuts(getTestMethodName() + "_RR", 50, 100));
    AsyncInvocation inv3 = vm1.invokeAsync(
        () -> AsyncEventQueueTestBase.doNextPuts(getTestMethodName() + "_RR", 100, 150));

    try {
      inv1.join();
      inv2.join();
      inv3.join();
    } catch (InterruptedException ie) {
      Assert.fail("Cought interrupted exception while waiting for the task tgo complete.", ie);
    }

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm4.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 150));// primary
                                                                                    // sender
    vm2.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0));// secondary
    vm3.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0));// secondary
    vm4.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0));// secondary
  }

  /**
   * Test configuration::
   *
   * Region: PartitionedRegion WAN: Parallel Dispatcher threads: more than 1 Order policy: key based
   * ordering
   */
  // Disabling test for bug #48323
  @Test
  public void testPartitionedParallelAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyKey() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm3.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm4.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", true, 100, 10,
        true, false, null, false, 3, OrderPolicy.KEY));
    vm2.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", true, 100, 10,
        true, false, null, false, 3, OrderPolicy.KEY));
    vm3.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", true, 100, 10,
        true, false, null, false, 3, OrderPolicy.KEY));
    vm4.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", true, 100, 10,
        true, false, null, false, 3, OrderPolicy.KEY));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        getTestMethodName() + "_PR", "ln", isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        getTestMethodName() + "_PR", "ln", isOffHeap()));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        getTestMethodName() + "_PR", "ln", isOffHeap()));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        getTestMethodName() + "_PR", "ln", isOffHeap()));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts(getTestMethodName() + "_PR", 100));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm4.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));

    int vm1size =
        vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize("ln"));
    int vm2size =
        vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize("ln"));
    int vm3size =
        vm3.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize("ln"));
    int vm4size =
        vm4.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize("ln"));

    assertEquals(vm1size + vm2size + vm3size + vm4size, 100);

  }


  /**
   * Test configuration::
   *
   * Region: PartitionedRegion WAN: Parallel Dispatcher threads: more than 1 Order policy: PARTITION
   * based ordering
   */
  // Disabled test for bug #48323
  @Test
  public void testPartitionedParallelAsyncEventQueueWithMultipleDispatcherThreadsOrderPolicyPartition() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm3.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm4.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", true, 100, 10,
        true, false, null, false, 3, OrderPolicy.PARTITION));
    vm2.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", true, 100, 10,
        true, false, null, false, 3, OrderPolicy.PARTITION));
    vm3.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", true, 100, 10,
        true, false, null, false, 3, OrderPolicy.PARTITION));
    vm4.invoke(() -> AsyncEventQueueTestBase.createConcurrentAsyncEventQueue("ln", true, 100, 10,
        true, false, null, false, 3, OrderPolicy.PARTITION));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        getTestMethodName() + "_PR", "ln", isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        getTestMethodName() + "_PR", "ln", isOffHeap()));
    vm3.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        getTestMethodName() + "_PR", "ln", isOffHeap()));
    vm4.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        getTestMethodName() + "_PR", "ln", isOffHeap()));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts(getTestMethodName() + "_PR", 100));

    vm1.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm2.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm3.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));
    vm4.invoke(() -> AsyncEventQueueTestBase.waitForAsyncQueueToGetEmpty("ln"));

    int vm1size =
        vm1.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize("ln"));
    int vm2size =
        vm2.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize("ln"));
    int vm3size =
        vm3.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize("ln"));
    int vm4size =
        vm4.invoke(() -> AsyncEventQueueTestBase.getAsyncEventListenerMapSize("ln"));

    assertEquals(100, vm1size + vm2size + vm3size + vm4size);
  }
}
