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
package org.apache.geode.internal.cache.wan.parallel;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ParallelWANPropagationConcurrentOpsDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public ParallelWANPropagationConcurrentOpsDUnitTest() {
    super();
  }

  /**
   * Normal propagation scenario test case for a PR with only one bucket. This has been added for
   * bug# 44284.
   *
   */
  @Test
  public void testParallelPropagationWithSingleBucketPR() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 1,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 1,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 1,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 1,
        isOffHeap()));

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    // pause the senders
    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));

    AsyncInvocation async1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 700));
    AsyncInvocation async2 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    AsyncInvocation async3 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 800));
    AsyncInvocation async4 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    async1.join();
    async2.join();
    async3.join();
    async4.join();

    int queueSize = vm4.invoke(() -> WANTestBase.getQueueContentSize("ln", true));
    assertEquals("Actual queue size is not matching with the expected", 3500, queueSize);

    // resume the senders now
    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  /**
   * Normal propagation scenario test case for a PR with less number of buckets. Buckets have been
   * kept to 10 for this test. This has been added for bug# 44287.
   *
   */
  @Test
  public void testParallelPropagationWithLowNumberOfBuckets() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 10,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1, 10,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 10,
        isOffHeap()));
    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 10,
        isOffHeap()));

    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    AsyncInvocation async1 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 700));
    AsyncInvocation async2 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));
    AsyncInvocation async3 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 800));
    AsyncInvocation async4 =
        vm4.invokeAsync(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    async1.join();
    async2.join();
    async3.join();
    async4.join();

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
  }

  @Test
  public void testParallelQueueDrainInOrder_PR() throws Exception {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> WANTestBase.createReceiver());

    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 3, 4,
        isOffHeap()));

    vm2.invoke(() -> WANTestBase.addListenerOnRegion(getTestMethodName() + "_PR"));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 3, 4,
        isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 3, 4,
        isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 3, 4,
        isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 3, 4,
        isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.addQueueListener("ln", true));
    vm5.invoke(() -> WANTestBase.addQueueListener("ln", true));
    vm6.invoke(() -> WANTestBase.addQueueListener("ln", true));
    vm7.invoke(() -> WANTestBase.addQueueListener("ln", true));

    vm4.invoke(() -> WANTestBase.pauseSender("ln"));
    vm5.invoke(() -> WANTestBase.pauseSender("ln"));
    vm6.invoke(() -> WANTestBase.pauseSender("ln"));
    vm7.invoke(() -> WANTestBase.pauseSender("ln"));

    vm6.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 4));
    vm4.invoke(() -> WANTestBase.addListenerOnBucketRegion(getTestMethodName() + "_PR", 4));
    vm5.invoke(() -> WANTestBase.addListenerOnBucketRegion(getTestMethodName() + "_PR", 4));
    vm6.invoke(() -> WANTestBase.addListenerOnBucketRegion(getTestMethodName() + "_PR", 4));
    vm7.invoke(() -> WANTestBase.addListenerOnBucketRegion(getTestMethodName() + "_PR", 4));

    vm4.invoke(() -> WANTestBase.addListenerOnQueueBucketRegion("ln", 4));
    vm5.invoke(() -> WANTestBase.addListenerOnQueueBucketRegion("ln", 4));
    vm6.invoke(() -> WANTestBase.addListenerOnQueueBucketRegion("ln", 4));
    vm7.invoke(() -> WANTestBase.addListenerOnQueueBucketRegion("ln", 4));

    vm6.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 1000));

    vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));

    HashMap vm4BRUpdates =
        vm4.invoke(() -> WANTestBase.checkBR(getTestMethodName() + "_PR", 4));
    HashMap vm5BRUpdates =
        vm5.invoke(() -> WANTestBase.checkBR(getTestMethodName() + "_PR", 4));
    HashMap vm6BRUpdates =
        vm6.invoke(() -> WANTestBase.checkBR(getTestMethodName() + "_PR", 4));
    HashMap vm7BRUpdates =
        vm7.invoke(() -> WANTestBase.checkBR(getTestMethodName() + "_PR", 4));

    List b0SenderUpdates = (List) vm4BRUpdates.get("Create0");
    List b1SenderUpdates = (List) vm4BRUpdates.get("Create1");
    List b2SenderUpdates = (List) vm4BRUpdates.get("Create2");
    List b3SenderUpdates = (List) vm4BRUpdates.get("Create3");

    HashMap vm4QueueBRUpdates = vm4.invoke(() -> WANTestBase.checkQueue_BR("ln", 4));
    HashMap vm5QueueBRUpdates = vm5.invoke(() -> WANTestBase.checkQueue_BR("ln", 4));
    HashMap vm6QueueBRUpdates = vm6.invoke(() -> WANTestBase.checkQueue_BR("ln", 4));
    HashMap vm7QueueBRUpdates = vm7.invoke(() -> WANTestBase.checkQueue_BR("ln", 4));

    assertEquals(vm4QueueBRUpdates, vm5QueueBRUpdates);
    assertEquals(vm4QueueBRUpdates, vm6QueueBRUpdates);
    assertEquals(vm4QueueBRUpdates, vm7QueueBRUpdates);

    vm4.invoke(() -> WANTestBase.resumeSender("ln"));
    vm5.invoke(() -> WANTestBase.resumeSender("ln"));
    vm6.invoke(() -> WANTestBase.resumeSender("ln"));
    vm7.invoke(() -> WANTestBase.resumeSender("ln"));
    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
    HashMap receiverUpdates =
        vm2.invoke(() -> WANTestBase.checkPR(getTestMethodName() + "_PR"));
    List<Long> createList = (List) receiverUpdates.get("Create");
    ArrayList<Long> b0ReceiverUpdates = new ArrayList<>();
    ArrayList<Long> b1ReceiverUpdates = new ArrayList<>();
    ArrayList<Long> b2ReceiverUpdates = new ArrayList<>();
    ArrayList<Long> b3ReceiverUpdates = new ArrayList<>();
    for (Long key : createList) {
      long mod = key % 4;
      if (mod == 0) {
        b0ReceiverUpdates.add(key);
      } else if (mod == 1) {
        b1ReceiverUpdates.add(key);
      } else if (mod == 2) {
        b2ReceiverUpdates.add(key);
      } else if (mod == 3) {
        b3ReceiverUpdates.add(key);
      }
    }
    b0ReceiverUpdates.remove(0);
    b1ReceiverUpdates.remove(0);
    b2ReceiverUpdates.remove(0);
    b3ReceiverUpdates.remove(0);

    assertEquals(b0SenderUpdates, b0ReceiverUpdates);
    assertEquals(b1SenderUpdates, b1ReceiverUpdates);
    assertEquals(b2SenderUpdates, b2ReceiverUpdates);
    assertEquals(b3SenderUpdates, b3ReceiverUpdates);
  }

}
