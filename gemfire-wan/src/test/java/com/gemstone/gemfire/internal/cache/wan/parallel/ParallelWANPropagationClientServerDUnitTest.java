/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.wan.parallel;

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;

import dunit.AsyncInvocation;

/**
 * @author skumar
 * 
 */
public class ParallelWANPropagationClientServerDUnitTest extends WANTestBase {
  private static final long serialVersionUID = 1L;

  public ParallelWANPropagationClientServerDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Normal happy scenario test case.
   * 
   * @throws Exception
   */
  public void testParallelPropagationWithClientServer() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiverAndServer", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiverAndServer", new Object[] { nyPort });
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createClientWithLocator", new Object[] {
        nyPort, "localhost", testName + "_PR" });
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
      100 });

    vm5.invoke(WANTestBase.class, "createServer", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createServer", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2, true,
         100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2, true,
         100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", "ln", 1, 100, isOffHeap() });

    vm7.invoke(WANTestBase.class, "createClientWithLocator", new Object[] {
      lnPort, "localhost", testName + "_PR" });
    
    AsyncInvocation inv1 = vm5.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });
    AsyncInvocation inv2 = vm6.invokeAsync(WANTestBase.class, "startSender", new Object[] { "ln" });

    inv1.join();
    inv2.join();
    // before doing any puts, let the senders be running in order to ensure that
    // not a single event will be lost
    
    vm5.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "waitForSenderRunningState",
        new Object[] { "ln" });
    
    vm7.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        10000 });

    
    // verify all buckets drained on all sender nodes.
    vm5.invoke(WANTestBase.class,
        "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class,
        "validateParallelSenderQueueAllBucketsDrained", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 10000 });

    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 10000 });
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 10000 });

    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 10000 });
    
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 10000 });

  }
}
