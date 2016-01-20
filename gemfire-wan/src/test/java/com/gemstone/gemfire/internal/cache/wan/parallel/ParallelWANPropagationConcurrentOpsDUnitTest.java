/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan.parallel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;

import dunit.AsyncInvocation;

public class ParallelWANPropagationConcurrentOpsDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;
  
  public ParallelWANPropagationConcurrentOpsDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
  }
  
  /**
   * Normal propagation scenario test case for a PR with only one bucket.
   * This has been added for bug# 44284.
   * @throws Exception
   */
  public void testParallelPropagationWithSingleBucketPR() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
 
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
 
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 1, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 1, isOffHeap() });
 
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
 
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 1, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 1, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    //pause the senders
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    pause(5000);
    
    AsyncInvocation async1 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 700 });
    AsyncInvocation async2 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    AsyncInvocation async3 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 800 });
    AsyncInvocation async4 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    
    async1.join();
    async2.join();
    async3.join();
    async4.join();
    
    int queueSize = (Integer) vm4.invoke(WANTestBase.class, "getQueueContentSize", new Object[] { "ln" });
    assertEquals("Actual queue size is not matching with the expected", 3500, queueSize);
    
    //resume the senders now
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }

  /**
   * Normal propagation scenario test case for a PR with less number of buckets.
   * Buckets have been kept to 10 for this test.
   * This has been added for bug# 44287.
   * @throws Exception
   */
  public void testParallelPropagationWithLowNumberofBuckets() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
 
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
 
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 10, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 10, isOffHeap() });
 
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
 
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 10, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 10, isOffHeap() });

    //before doing any puts, let the senders be running in order to ensure that
    //not a single event will be lost
    vm4.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState", new Object[] { "ln" });
    
    AsyncInvocation async1 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 700 });
    AsyncInvocation async2 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    AsyncInvocation async3 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 800 });
    AsyncInvocation async4 = vm4.invokeAsync(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 1000 });
    
    async1.join();
    async2.join();
    async3.join();
    async4.join();
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
  }

  public void testParalleQueueDrainInOrder_PR() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 3, 4, isOffHeap() });
  
    vm2.invoke(WANTestBase.class, "addListenerOnRegion", new Object[] {testName + "_PR"});
  
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 3, 4, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 3, 4, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 3, 4, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 3, 4, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln", true});
    vm5.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln", true});
    vm6.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln", true});
    vm7.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln", true});

    pause(2000);
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln"});
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln"});
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln"});
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln"});
    
    pause(2000);
    
    vm6.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
      4 });
    vm4.invoke(WANTestBase.class, "addListenerOnBucketRegion", new Object[] {testName + "_PR", 4});
    vm5.invoke(WANTestBase.class, "addListenerOnBucketRegion", new Object[] {testName + "_PR", 4});
    vm6.invoke(WANTestBase.class, "addListenerOnBucketRegion", new Object[] {testName + "_PR", 4 });
    vm7.invoke(WANTestBase.class, "addListenerOnBucketRegion", new Object[] {testName + "_PR", 4});
    
    vm4.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln" , 4});
    vm5.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln" , 4});
    vm6.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln" , 4 });
    vm7.invoke(WANTestBase.class, "addListenerOnQueueBucketRegion", new Object[] { "ln" , 4});
    
    vm6.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
      1000 });
    
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 1000 });
    
    HashMap vm4BRUpdates = (HashMap)vm4.invoke(WANTestBase.class, "checkBR", new Object[] {testName + "_PR", 4});
    HashMap vm5BRUpdates = (HashMap)vm5.invoke(WANTestBase.class, "checkBR", new Object[] {testName + "_PR", 4});
    HashMap vm6BRUpdates = (HashMap)vm6.invoke(WANTestBase.class, "checkBR", new Object[] {testName + "_PR", 4});
    HashMap vm7BRUpdates = (HashMap)vm7.invoke(WANTestBase.class, "checkBR", new Object[] {testName + "_PR", 4});
    
    List b0SenderUpdates = (List)vm4BRUpdates.get("Create0");
    List b1SenderUpdates = (List)vm4BRUpdates.get("Create1");
    List b2SenderUpdates = (List)vm4BRUpdates.get("Create2");
    List b3SenderUpdates = (List)vm4BRUpdates.get("Create3");
    
    HashMap vm4QueueBRUpdates = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln", 4});
    HashMap vm5QueueBRUpdates = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln", 4});
    HashMap vm6QueueBRUpdates = (HashMap)vm6.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln", 4});
    HashMap vm7QueueBRUpdates = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue_BR", new Object[] {"ln", 4});
    
    assertEquals(vm4QueueBRUpdates, vm5QueueBRUpdates);
    assertEquals(vm4QueueBRUpdates, vm6QueueBRUpdates);
    assertEquals(vm4QueueBRUpdates, vm7QueueBRUpdates);
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln"});
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln"});
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln"});
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln"});
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 1000 });
    HashMap receiverUpdates = (HashMap)vm2.invoke(WANTestBase.class, "checkPR", new Object[] {
      testName + "_PR"});
    List<Long> createList = (List)receiverUpdates.get("Create");
    ArrayList<Long> b0ReceiverUpdates = new ArrayList<Long>();
    ArrayList<Long> b1ReceiverUpdates = new ArrayList<Long>();
    ArrayList<Long> b2ReceiverUpdates = new ArrayList<Long>();
    ArrayList<Long> b3ReceiverUpdates = new ArrayList<Long>();
    for (Long key : createList) {
      long mod = key % 4;
      if (mod == 0) {
        b0ReceiverUpdates.add(key);
      }
      else if (mod == 1) {
        b1ReceiverUpdates.add(key);
      }
      else if (mod == 2) {
        b2ReceiverUpdates.add(key);
      }
      else if (mod == 3) {
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
