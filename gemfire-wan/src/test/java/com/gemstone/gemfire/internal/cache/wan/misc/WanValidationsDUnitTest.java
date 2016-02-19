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
package com.gemstone.gemfire.internal.cache.wan.misc;

import java.util.ArrayList;
import java.util.Map;

import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter1;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter2;
import com.gemstone.gemfire.internal.cache.wan.Filter70;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderConfigurationException;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderException;
import com.gemstone.gemfire.internal.cache.wan.MyGatewayTransportFilter3;
import com.gemstone.gemfire.internal.cache.wan.MyGatewayTransportFilter4;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;

public class WanValidationsDUnitTest extends WANTestBase {

  public WanValidationsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test to make sure that serial sender Ids configured in Distributed Region
   * is same across all DR nodes TODO : Should this validation hold tru now.
   * Discuss. If I have 2 members on Which DR is defined. But sender is defined
   * on only one member. How can I add the instance on the sender in Region
   * which does not have a sender. I can bypass the existing validation for the
   * DR with SerialGatewaySender. But for PR with SerialGatewaySender, we need
   * to send the adjunct message. Find out the way to send the adjunct message
   * to the member on which serialGatewaySender is available.
   */
  
  public void testSameSerialGatewaySenderIdAcrossSameDistributedRegion()
      throws Exception {
    IgnoredException.addIgnoredException("another cache has the same region defined");
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));
      
      vm4.invoke(createCacheRunnable(lnPort));
      vm5.invoke(createCacheRunnable(lnPort));

      vm4.invoke(() -> WANTestBase.createSender("ln1", 2, false, 10, 100, false, false, null, true));
      vm4.invoke(() -> WANTestBase.createSender("ln2", 2, false, 10, 100, false, false, null, true));
      
      vm5.invoke(() -> WANTestBase.createSender("ln2", 2, false, 10, 100, false, false, null, true));
      vm5.invoke(() -> WANTestBase.createSender("ln3", 2, false, 10, 100, false, false, null, true));
      
     vm4.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln1,ln2", isOffHeap() ));
      
      vm5.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln2,ln3", isOffHeap() ));
      fail("Expected IllegalStateException with incompatible gateway sender ids message");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage().contains("Cannot create Region"))) {
        Assert.fail(
            "Expected IllegalStateException with incompatible gateway sender ids message",
            e);
      }
    }
  }

  protected SerializableRunnableIF createCacheRunnable(Integer lnPort) {
    return () -> WANTestBase.createCache(lnPort );
  }

  /**
   * Validate that ParallelGatewaySender can be added to Distributed region
   * 
   * @throws Exception
   * 
   * 
   * Below test is disabled intentionally Replicated region with Parallel Async
   * Event queue is not supported. Test is added for the same
   * ReplicatedRegion_ParallelWANPropogationDUnitTest#test_DR_PGS_1Nodes_Put_Receiver
   * 
   * We are gone support this configuration in upcoming releases
   */

  public void DISABLED_DUETO_BUG51491_testParallelGatewaySenderForDistributedRegion() throws Exception {
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
      Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

      vm4.invoke(createCacheRunnable(lnPort));
      vm5.invoke(createCacheRunnable(lnPort));

      vm4.invoke(() -> WANTestBase.createSender( "ln1", 2,
          true, 10, 100, false, false, null, false ));

      vm5.invoke(() -> WANTestBase.createSender( "ln2", 2,
          true, 10, 100, false, false, null, false ));

      vm4.invoke(() -> WANTestBase.createReplicatedRegion(
          getTestMethodName() + "_RR", "ln1", isOffHeap()  ));

      vm5.invoke(() -> WANTestBase.createReplicatedRegion(
          getTestMethodName() + "_RR", "ln1", isOffHeap()  ));

    }
    catch (Exception e) {
      Assert.fail("Caught Exception", e);
    }
  }
  
  /**
   * Test to make sure that serial sender Ids configured in partitioned regions
   * should be same across all PR members
   */
  public void testSameSerialGatewaySenderIdAcrossSamePartitionedRegion()
      throws Exception {
    IgnoredException.addIgnoredException("another cache has the same region defined");
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

      vm4.invoke(createCacheRunnable(lnPort));
      vm5.invoke(createCacheRunnable(lnPort));
      
      vm4.invoke(() -> WANTestBase.createSender("ln1", 2, false, 10, 100, false, false, null, true));
      vm4.invoke(() -> WANTestBase.createSender("ln2", 2, false, 10, 100, false, false, null, true));
      
      vm5.invoke(() -> WANTestBase.createSender("ln2", 2, false, 10, 100, false, false, null, true));
      vm5.invoke(() -> WANTestBase.createSender("ln3", 2, false, 10, 100, false, false, null, true));
      
      vm4.invoke(() -> WANTestBase.createPartitionedRegion(
          getTestMethodName() + "_PR", "ln1,ln2", 1, 100, isOffHeap()  ));
      vm5.invoke(() -> WANTestBase.createPartitionedRegion(
          getTestMethodName() + "_PR", "ln2,ln3", 1, 100, isOffHeap()  ));
      fail("Expected IllegalStateException with incompatible gateway sender ids message");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage().contains("Cannot create Region"))) {
        Assert.fail(
            "Expected IllegalStateException with incompatible gateway sender ids message",
            e);
      }
    }
  }
  
  
  public void testReplicatedSerialAsyncEventQueueWithPeristenceEnabled() {
    IgnoredException.addIgnoredException("another cache has the same region defined");
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

      vm4.invoke(createCacheRunnable(lnPort));
      vm5.invoke(createCacheRunnable(lnPort));

      vm4.invoke(() -> WANTestBase.createReplicatedRegionWithAsyncEventQueue(
              getTestMethodName() + "_RR", "ln1", isOffHeap()  ));
      vm5.invoke(() -> WANTestBase.createReplicatedRegionWithAsyncEventQueue(
              getTestMethodName() + "_RR", "ln2", isOffHeap()  ));
      fail("Expected IllegalStateException with incompatible gateway sender ids message");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage().contains("Cannot create Region"))) {
        Assert.fail(
            "Expected IllegalStateException with incompatible gateway sender ids message",
            e);
      }
    }
  }
  
  /**
   * Test to make sure that parallel sender Ids configured in partitioned
   * regions should be same across all PR members
   */
  public void testSameParallelGatewaySenderIdAcrossSamePartitionedRegion()
      throws Exception {
    IgnoredException.addIgnoredException("another cache has the same region defined");
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

      vm4.invoke(createCacheRunnable(lnPort));
      vm5.invoke(createCacheRunnable(lnPort));
      
      vm4.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));
      vm4.invoke(() -> WANTestBase.createSender("ln2", 2, true, 10, 100, false, false, null, true));
      
      vm5.invoke(() -> WANTestBase.createSender("ln2", 2, true, 10, 100, false, false, null, true));
      vm5.invoke(() -> WANTestBase.createSender("ln3", 2, true, 10, 100, false, false, null, true));

      vm4.invoke(() -> WANTestBase.createPartitionedRegion(
          getTestMethodName() + "_PR", "ln1,ln2", 1, 100, isOffHeap()  ));
      vm5.invoke(() -> WANTestBase.createPartitionedRegion(
          getTestMethodName() + "_PR", "ln2,ln3", 1, 100, isOffHeap()  ));
      
      fail("Expected IllegalStateException with incompatible gateway sender ids message");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage().contains("Cannot create Region"))) {
        Assert.fail(
            "Expected IllegalStateException with incompatible gateway sender ids message",
            e);
      }
    }
  }

  /**
   * Test to make sure that same parallel gateway sender id can be used by 2
   * different PRs
   * 
   * @throws Exception
   */
  public void ___testSameParallelGatewaySenderIdAcrossDifferentPartitionedRegion()
      throws Exception {
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

      vm1.invoke(createCacheRunnable(lnPort));
      
      vm1.invoke(() -> WANTestBase.createSender("ln1_Parallel", 2, true, 10, 100, false, false, null, true));
      vm1.invoke(() -> WANTestBase.createSender("ln2_Parallel", 2, true, 10, 100, false, false, null, true));
      
      vm1.invoke(() -> WANTestBase.createPartitionedRegionWithSerialParallelSenderIds(
              getTestMethodName() + "_PR1", null, "ln1_Parallel,ln2_Parallel", null, isOffHeap()  ));
      vm1.invoke(() -> WANTestBase.createPartitionedRegionWithSerialParallelSenderIds(
              getTestMethodName() + "_PR2", null, "ln1_Parallel,ln2_Parallel", null, isOffHeap()  ));

    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("cannot have the same parallel gateway sender id"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }

  public void testSameParallelGatewaySenderIdAcrossColocatedPartitionedRegion()
      throws Exception {
    IgnoredException.addIgnoredException("another cache has the same region defined");
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

      vm1.invoke(createCacheRunnable(lnPort));
      
      vm1.invoke(() -> WANTestBase.createSender("ln1_Parallel", 2, true, 10, 100, false, false, null, true));
      vm1.invoke(() -> WANTestBase.createSender("ln2_Parallel", 2, true, 10, 100, false, false, null, true));
      
      vm1.invoke(() -> WANTestBase.createPartitionedRegionWithSerialParallelSenderIds(
              getTestMethodName() + "_PR1", null, "ln1_Parallel", null, isOffHeap()  ));
      vm1.invoke(() -> WANTestBase.createPartitionedRegionWithSerialParallelSenderIds(
              getTestMethodName() + "_PR2", null, "ln1_Parallel,ln2_Parallel", getTestMethodName() + "_PR1", isOffHeap()  ));
      //now we support this
      //fail("Expected IllegalStateException with incompatible gateway sender ids in colocated regions");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("should have same parallel gateway sender ids"))) {
        Assert.fail(
            "Expected IllegalStateException with incompatible gateway sender ids in colocated regions",
            e);
      }
    }
  }
  
  /**
   * Validate that if Colocated partitioned region doesn't want to add a PGS even if its 
   * parent has one then it is fine
   * @throws Exception
   */
  
  public void testSameParallelGatewaySenderIdAcrossColocatedPartitionedRegion2()
      throws Exception {
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

      vm1.invoke(createCacheRunnable(lnPort));
      
      vm1.invoke(() -> WANTestBase.createSender("ln1_Parallel", 2, true, 10, 100, false, false, null, true));
      vm1.invoke(() -> WANTestBase.createSender("ln2_Parallel", 2, true, 10, 100, false, false, null, true));
      
      vm1.invoke(() -> WANTestBase.createPartitionedRegionWithSerialParallelSenderIds(
              getTestMethodName() + "_PR1", null, "ln1_Parallel", null, isOffHeap()  ));
      vm1.invoke(() -> WANTestBase.createPartitionedRegionWithSerialParallelSenderIds(
              getTestMethodName() + "_PR2", null, null, getTestMethodName() + "_PR1", isOffHeap()  ));
      
    }
    catch (Exception e) {
      Assert.fail("The tests caught Exception.", e);
    }
  }
  
  /**
   * Validate that if Colocated partitioned region has a subset of PGS
   * then it is fine. 
   * @throws Exception
   */
  
  public void testSameParallelGatewaySenderIdAcrossColocatedPartitionedRegion3()
      throws Exception {
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

      vm1.invoke(createCacheRunnable(lnPort));

      vm1.invoke(() -> WANTestBase.createSender(
          "ln1_Parallel", 2, true, 10, 100, false, false, null, true ));
      vm1.invoke(() -> WANTestBase.createSender(
          "ln2_Parallel", 2, true, 10, 100, false, false, null, true ));

      vm1.invoke(() -> WANTestBase.createPartitionedRegionWithSerialParallelSenderIds(
              getTestMethodName() + "_PR1", null, "ln1_Parallel,ln2_Parallel", null, isOffHeap()  ));
      vm1.invoke(() -> WANTestBase.createPartitionedRegionWithSerialParallelSenderIds(
              getTestMethodName() + "_PR2", null, "ln1_Parallel", getTestMethodName() + "_PR1", isOffHeap()  ));

    } catch (Exception e) {
      Assert.fail("The tests caught Exception.", e);
    }
  }
  
  /**
   * Validate that if Colocated partitioned region has a superset of PGS
   * then Exception is thrown. 
   * @throws Exception
   */
  
  public void testSameParallelGatewaySenderIdAcrossColocatedPartitionedRegion4()
      throws Exception {
    try {
      Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

      vm1.invoke(createCacheRunnable(lnPort));

      vm1.invoke(() -> WANTestBase.createSender("ln1_Parallel", 2, true, 10, 100, false, false, null, true));
      vm1.invoke(() -> WANTestBase.createSender("ln2_Parallel", 2, true, 10, 100, false, false, null, true));
      vm1.invoke(() -> WANTestBase.createSender("ln3_Parallel", 2, true, 10, 100, false, false, null, true));

      vm1.invoke(() -> WANTestBase.createPartitionedRegionWithSerialParallelSenderIds(
              getTestMethodName() + "_PR1", null, "ln1_Parallel,ln2_Parallel", null, isOffHeap()  ));
      vm1.invoke(() -> WANTestBase.createPartitionedRegionWithSerialParallelSenderIds(
              getTestMethodName() + "_PR2", null, "ln1_Parallel,ln2_Parallel,ln3_Parallel", getTestMethodName() + "_PR1", isOffHeap()  ));
      //now we support this
      //fail("Expected IllegalStateException with incompatible gateway sender ids in colocated regions");
    } catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("should have same parallel gateway sender ids"))) {
        Assert.fail(
            "Expected IllegalStateException with incompatible gateway sender ids in colocated regions",
            e);
      }
    }
  }
  
  /**
   * SerialGatewaySender and ParallelGatewaySender with same name is allowed
   */
  public void testSerialGatewaySenderAndParallelGatewaySenderWithSameName() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> WANTestBase.createSenderForValidations(
        "ln", 2, false, 100, false, false, null, null, true, false ));
    try {
      vm1.invoke(() -> WANTestBase.createSenderForValidations(
          "ln", 2, true, 100, false, false, null, null, true, false ));
      fail("Expected IllegateStateException : Sender names should be different.");
    } catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("is already defined in this cache"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  //remote ds ids should be same
  public void testSameRemoteDSAcrossSameSender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> WANTestBase.createSenderForValidations(
        "ln", 2, false, 100, false, false, null, null,
        true, false  ));

    try {
      vm2.invoke(() -> WANTestBase.createSenderForValidations(
          "ln", 3, false, 100, false, false, null, null,
          true, false  ));
      fail("Expected IllegateStateException : Remote Ds Ids should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("because another cache has the same Gateway Sender defined with remote ds id"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  // sender with same name should be either serial or parallel but not both.
  public void testSerialSenderOnBothCache() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> WANTestBase.createSenderForValidations(
        "ln", 2, false, 100, false, false, null, null,
        true, false  ));

    try {
      vm2.invoke(() -> WANTestBase.createSenderForValidations(
          "ln", 2, true, 100, false, false, null, null, true, false  ));
      fail("Expected IllegateStateException : is not serial Gateway Sender");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("because another cache has the same sender as serial gateway sender"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  // sender with same name should be either serial or parallel but not both.
  public void testParallelSenderOnBothCache(){
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> WANTestBase.createSenderForValidations( "ln", 2,
        true, 100,false, false,
        null, null, true, false  ));
    
    try {
      vm2
          .invoke(() -> WANTestBase.createSenderForValidations( "ln", 2,
              false, 100, false, false,null, null,
              true, false  ));
      fail("Expected IllegateStateException : is not parallel Gateway Sender");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage().contains("because another cache has the same sender as parallel gateway sender"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  // isBatchConflation should be same across the same sender
  public void testBatchConflation() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> WANTestBase.createSenderForValidations( "ln", 2,
        false, 100, false, false,
        null, null, true, false  ));

    // isBatchConflation
    try {
      vm2.invoke(() -> WANTestBase.createSenderForValidations( "ln", 2,
          false, 100,true, false,
          null, null, true, false  ));
      fail("Expected IllegateStateException : isBatchConflation Should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage().contains("another cache has the same Gateway Sender defined with isBatchConfaltionEnabled"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  //isPersistentEnabled should be same across the same sender
  public void testisPersistentEnabled() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> WANTestBase.createSenderForValidations( "ln", 2,
        false, 100, false, false,
        null, null, true, false  ));
    try {
      vm2.invoke(() -> WANTestBase.createSenderForValidations( "ln", 2,
        false, 100,false, true, null, null, true, false  ));
      fail("Expected IllegateStateException : isPersistentEnabled Should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage().contains("because another cache has the same Gateway Sender defined with isPersistentEnabled"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  public void testAlertThreshold() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> WANTestBase.createSenderForValidations( "ln", 2,
        false, 100, false, false,
        null, null, true, false ));
    try {
      vm2.invoke(() -> WANTestBase.createSenderForValidations( "ln", 2,
        false, 50, false, false, null, null, true, false  ));
      fail("Expected IllegateStateException : alertThreshold Should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage().contains("because another cache has the same Gateway Sender defined with alertThreshold"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  public void testManualStart() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> WANTestBase.createSenderForValidations( "ln", 2,
        false, 100, false, false,
        null, null, true, false  ));
    try {
      vm2.invoke(() -> WANTestBase.createSenderForValidations( "ln", 2,
        false, 100, false, false, null, null, false, false  ));
      fail("Expected IllegateStateException : manualStart Should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage().contains("because another cache has the same Gateway Sender defined with manual start"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  public void testGatewayEventFilters() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    ArrayList<GatewayEventFilter> eventFiletrs = new ArrayList<GatewayEventFilter>();
    eventFiletrs.add(new MyGatewayEventFilter());
    vm1.invoke(() -> WANTestBase.createSenderForValidations(
        "ln", 2, false, 100, false, false, eventFiletrs,
        null, true, false  ));
    try {
      eventFiletrs.clear();
      eventFiletrs.add(new Filter70());
      vm2.invoke(() -> WANTestBase.createSenderForValidations(
          "ln", 2, false, 100, false, false,
          eventFiletrs, null, true, false  ));
      fail("Expected IllegateStateException : GatewayEventFileters Should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("because another cache has the same Gateway Sender defined with GatewayEventFilters"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  public void testGatewayEventFilters2() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    ArrayList<GatewayEventFilter> eventFiletrs = new ArrayList<GatewayEventFilter>();
    eventFiletrs.add(new MyGatewayEventFilter());
    vm1.invoke(() -> WANTestBase.createSenderForValidations(
        "ln", 2, false, 100, false, false, eventFiletrs,
        null, true, false  ));
    try {
      eventFiletrs.clear();
      eventFiletrs.add(new MyGatewayEventFilter());
      eventFiletrs.add(new Filter70());
      vm2.invoke(() -> WANTestBase.createSenderForValidations(
          "ln", 2, false, 100, false, false,
          eventFiletrs, null, true, false  ));
      fail("Expected IllegateStateException : GatewayEventFileters Should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("because another cache has the same Gateway Sender defined with GatewayEventFilters"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  public void testGatewayTransportFilters() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    ArrayList<GatewayTransportFilter> transportFiletrs = new ArrayList<GatewayTransportFilter>();
    transportFiletrs.add(new MyGatewayTransportFilter1());
    transportFiletrs.add(new MyGatewayTransportFilter2());
    vm1.invoke(() -> WANTestBase.createSenderForValidations(
        "ln", 2, false, 100, false, false, null,
        transportFiletrs, true, false  ));
   try {
      transportFiletrs.clear();
      transportFiletrs.add(new MyGatewayTransportFilter3());
      transportFiletrs.add(new MyGatewayTransportFilter4());
      vm2.invoke(() -> WANTestBase.createSenderForValidations(
          "ln", 2, false, 100, false, false,
          null, transportFiletrs, true, false  ));
      fail("Expected IllegateStateException : GatewayEventFileters Should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("because another cache has the same Gateway Sender defined with GatewayTransportFilters"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }

  public void testGatewayTransportFiltersOrder() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    ArrayList<GatewayTransportFilter> transportFiletrs = new ArrayList<GatewayTransportFilter>();
    transportFiletrs.add(new MyGatewayTransportFilter1());
    transportFiletrs.add(new MyGatewayTransportFilter2());
    vm1.invoke(() -> WANTestBase.createSenderForValidations(
        "ln", 2, false, 100, false, false, null,
        transportFiletrs, true, false  ));
   try {
      transportFiletrs.clear();
      transportFiletrs.add(new MyGatewayTransportFilter2());
      transportFiletrs.add(new MyGatewayTransportFilter1());
      vm2.invoke(() -> WANTestBase.createSenderForValidations(
          "ln", 2, false, 100, false, false,
          null, transportFiletrs, true, false ));
      fail("Expected IllegateStateException : GatewayEventFileters Should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("because another cache has the same Gateway Sender defined with GatewayTransportFilters"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
//  public void ___testGatewaySenderListener() {
//    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
//        "createFirstLocatorWithDSId", new Object[] { 1 });
//
//    vm1.invoke(() -> WANTestBase.createCache( lnPort ));
//    vm2.invoke(() -> WANTestBase.createCache( lnPort ));
//
//    vm1.invoke(() -> WANTestBase.createSenderWithListener(
//        "ln", 2, false, 100, 10, false, false, null,
//        true, true ));
//    
//   try {
//      vm2.invoke(() -> WANTestBase.createSenderWithListener(
//        "ln", 2, false, 100, 10, false, false, null,
//        false, true ));
//      fail("Expected IllegateStateException : GatewayEventFileters Should match");
//    }
//    catch (Exception e) {
//      if (!(e.getCause() instanceof IllegalStateException)
//          || !(e.getCause().getMessage()
//              .contains("because another cache has the same Gateway Sender defined with GatewaySenderEventListener"))) {
//        fail("Expected IllegalStateException", e);
//      }
//    }
//  }
  
  public void testIsDiskSynchronous() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> WANTestBase.createSenderForValidations(
        "ln", 2, false, 100, false, false, null,
        null, true, false ));
    
   try {
      vm2.invoke(() -> WANTestBase.createSenderForValidations(
        "ln", 2, false, 100, false, false, null,
        null, true, true ));
      fail("Expected IllegateStateException : isDiskSynchronous Should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("because another cache has the same Gateway Sender defined with isDiskSynchronous"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  /**
   * This test has been added for the defect # 44372.
   * A single VM hosts a bridge server as well as a Receiver.
   * Expected: Cache.getCacheServer should return only the Bridge server and not the Receiver
   */
  public void test_GetCacheServersDoesNotReturnReceivers() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    
    vm4.invoke(() -> WANTestBase.createReceiver( lnPort ));
    
    vm4.invoke(() -> WANTestBase.createCacheServer( ));
    
    Map cacheServers = (Map) vm4.invoke(() -> WANTestBase.getCacheServers( ));
    
    assertEquals("Cache.getCacheServers returned incorrect BridgeServers: ", 1, cacheServers.get("BridgeServer"));
    assertEquals("Cache.getCacheServers returned incorrect ReceiverServers: ", 0, cacheServers.get("ReceiverServer"));
  }
  
  /**
   * Added for the defect # 44372.
   * Two VMs are part of the DS. 
   * One VM hosts a Bridge server while the other hosts a Receiver.
   * Expected: Cache.getCacheServers should only return the bridge server and not the Receiver.
   */
  public void test_GetCacheServersDoesNotReturnReceivers_Scenario2() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    
    vm4.invoke(() -> WANTestBase.createReceiver( lnPort ));
    vm5.invoke(createCacheRunnable(lnPort));
    
    vm5.invoke(() -> WANTestBase.createCacheServer( ));
    
    Map cacheServers_vm4 = (Map) vm4.invoke(() -> WANTestBase.getCacheServers( ));
    Map cacheServers_vm5 = (Map) vm5.invoke(() -> WANTestBase.getCacheServers( ));
    
    assertEquals("Cache.getCacheServers on vm4 returned incorrect BridgeServers: ", 0, cacheServers_vm4.get("BridgeServer"));
    assertEquals("Cache.getCacheServers on vm4 returned incorrect ReceiverServers: ", 0, cacheServers_vm4.get("ReceiverServer"));
    
    assertEquals("Cache.getCacheServers on vm5 returned incorrect BridgeServers: ", 1, cacheServers_vm5.get("BridgeServer"));
    assertEquals("Cache.getCacheServers on vm5 returned incorrect ReceiverServers: ", 0, cacheServers_vm5.get("ReceiverServer"));

  }
  
  
  // dispatcher threads are same across all the nodes for ParallelGatewaySender
  /*
   * We are allowing number of dispatcher threads for parallel sender to differ
   * on number of machines
   */ 
  public void DISABLED_testDispatcherThreadsForParallelGatewaySender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
      true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY ));

    // dispatcher threads
    try {
      vm2.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY ));
      fail("Expected IllegateStateException : dispatcher threads Should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage().contains("because another cache has the same Gateway Sender defined with dispatcherThread"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
  
  
  // dispatcher threads are same across all the nodes for ParallelGatewaySender
  /*
   * For Parallel sender, thread policy is not supported whcih is checked at the
   * time of sender creation. policy KEY and Partition are same for PGS. Hence
   * disabling the tests
   */
  public void DISABLED_testOrderPolicyForParallelGatewaySender() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm1.invoke(createCacheRunnable(lnPort));
    vm2.invoke(createCacheRunnable(lnPort));

    vm1.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
      true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY ));

    // dispatcher threads
    try {
      vm2.invoke(() -> WANTestBase.createConcurrentSender( "ln", 2,
        true, 100, 10, false, false, null, true, 5, OrderPolicy.PARTITION ));
      fail("Expected IllegateStateException : order policy Should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage().contains("because another cache has the same Gateway Sender defined with orderPolicy"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }

  public void testBug50434_RR_Serial() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm2.invoke(createReceiverReplicatedRegion());

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    
    vm4.invoke(createReceiverReplicatedRegion());
    vm5.invoke(createReceiverReplicatedRegion());
    vm6.invoke(createReceiverReplicatedRegion());
    vm7.invoke(createReceiverReplicatedRegion());

    vm4.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_RR", "ln"));
    
    vm5.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_RR", "ln"));

    try {
      vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
          10 ));
      fail("Expected GatewaySenderConfigurationException : Sender Ids should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof GatewaySenderConfigurationException)
          || !(e.getCause().getMessage()
              .contains("For region across all members, gateway sender ids should be same."))) {
        Assert.fail("Expected GatewaySenderConfigurationException", e);
      }
    }
  }
  
  public void testBug50434_RR_SerialAsyncEventQueue() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm5.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm6.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm7.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));

    vm4.invoke(createReceiverReplicatedRegion());
    vm5.invoke(createReceiverReplicatedRegion());
    vm6.invoke(createReceiverReplicatedRegion());
    vm7.invoke(createReceiverReplicatedRegion());

    vm4.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_RR", "ln" ));

    vm5.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_RR", "ln" ));

    try {
      vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));
      fail("Expected GatewaySenderConfigurationException : AsyncEvent queue IDs should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof GatewaySenderConfigurationException)
          || !(e.getCause().getMessage()
              .contains("For region across all members, AsyncEvent queue IDs should be same."))) {
        Assert.fail("Expected GatewaySenderConfigurationException", e);
      }
    }
  }

  protected SerializableRunnableIF createReceiverReplicatedRegion() {
    return () -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() );
  }
  
  public void testBug50434_RR_Serial_Pass() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm2.invoke(createReceiverReplicatedRegion());

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    
    vm4.invoke(createReceiverReplicatedRegion());
    vm5.invoke(createReceiverReplicatedRegion());
    vm6.invoke(createReceiverReplicatedRegion());
    vm7.invoke(createReceiverReplicatedRegion());

    vm4.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_RR", "ln"));
    
    vm5.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_RR", "ln"));
    
    vm6.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_RR", "ln"));
    
    vm7.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_RR", "ln"));
    
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        10));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 10));
  }
  
  public void testBug50434_RR_SerialAsyncEventQueue_Pass() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm5.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm6.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm7.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));

    vm4.invoke(createReceiverReplicatedRegion());
    vm5.invoke(createReceiverReplicatedRegion());
    vm6.invoke(createReceiverReplicatedRegion());
    vm7.invoke(createReceiverReplicatedRegion());

    vm4.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_RR", "ln" ));
    vm5.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_RR", "ln" ));
    vm6.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_RR", "ln" ));
    vm7.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_RR", "ln" ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));

    vm4.invoke(() -> WANTestBase.validateAsyncEventListener(
        "ln", 1000 ));// primary sender
    vm5.invoke(() -> WANTestBase.validateAsyncEventListener(
        "ln", 0 ));// secondary
    vm6.invoke(() -> WANTestBase.validateAsyncEventListener(
        "ln", 0 ));// secondary
    vm7.invoke(() -> WANTestBase.validateAsyncEventListener(
        "ln", 0 ));// secondary
  }
  
  public void testBug50434_PR_Serial() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_RR", null, 1, 100, isOffHeap() ));

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
      false, 100, 10, false, false, null, true ));
    vm6.invoke(() -> WANTestBase.createSender( "ln", 2,
      false, 100, 10, false, false, null, true ));
    vm7.invoke(() -> WANTestBase.createSender( "ln", 2,
      false, 100, 10, false, false, null, true ));
    
    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    vm7.invoke(() -> WANTestBase.startSender( "ln" ));
    
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    
    vm5.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));

    try {
      vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR",
        10));
      fail("Expected GatewaySenderConfigurationException : Sender Ids should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof GatewaySenderConfigurationException)
          || !(e.getCause().getMessage()
              .contains("For region across all members, gateway sender ids should be same."))) {
        Assert.fail("Expected GatewaySenderConfigurationException", e);
      }
    }
  }
  
  public void testBug50434_PR_SerialAsyncEventQueue() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm5.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm6.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm7.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));
    vm5.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));

    try {
      vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR",
        1000 ));
      fail("Expected GatewaySenderConfigurationException : AsyncEvent queue IDs should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof GatewaySenderConfigurationException)
          || !(e.getCause().getMessage()
              .contains("For region across all members, AsyncEvent queue IDs should be same."))) {
        Assert.fail("Expected GatewaySenderConfigurationException", e);
      }
    }
  }
  
  public void testBug50434_PR_Serial_Pass() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
      false, 100, 10, false, false, null, true ));
    vm6.invoke(() -> WANTestBase.createSender( "ln", 2,
      false, 100, 10, false, false, null, true ));
    vm7.invoke(() -> WANTestBase.createSender( "ln", 2,
      false, 100, 10, false, false, null, true ));
    
    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    vm7.invoke(() -> WANTestBase.startSender( "ln" ));
    
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    vm5.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    vm6.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    vm7.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR",
        10));

    vm4.invoke(() -> WANTestBase.validateQueueContents( "ln",
      0));
    
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_PR", 10));
  }
  
  public void testBug50434_PR_SerialAsyncEventQueue_Pass() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm5.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm6.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));
    vm7.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        false, 100, 100, false, false, null, false ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 100, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));
    vm5.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));
    vm6.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));
    vm7.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR",
        1000 ));

    vm4.invoke(() -> WANTestBase.validateAsyncEventListener(
        "ln", 1000 ));// primary sender
    vm5.invoke(() -> WANTestBase.validateAsyncEventListener(
        "ln", 0 ));// secondary
    vm6.invoke(() -> WANTestBase.validateAsyncEventListener(
        "ln", 0 ));// secondary
    vm7.invoke(() -> WANTestBase.validateAsyncEventListener(
        "ln", 0 ));// secondary
  }
  
  public void testBug50434_PR_Parallel() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 10, isOffHeap()));

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    
    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 10, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 10, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 10, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 10, isOffHeap()));

    vm4.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    vm5.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    
    vm4.invoke(() -> WANTestBase.waitForSenderRunningState( "ln" ));
    
    try {
      vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR",
        10));
      fail("Expected GatewaySenderConfigurationException : Sender Ids should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof GatewaySenderConfigurationException)
          || !(e.getCause().getMessage()
              .contains("For region across all members, gateway sender ids should be same."))) {
        Assert.fail("Expected GatewaySenderConfigurationException", e);
      }
    }
  }
  
  public void testBug50434_PR_ParallelAsyncEventQueue() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm5.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm6.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm7.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 10, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 10, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 10, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 10, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));
    vm5.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));
    
    try {
      vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR",
        10));
      fail("Expected GatewaySenderConfigurationException : AsyncEvent queue IDs should match");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof GatewaySenderConfigurationException)
          || !(e.getCause().getMessage()
              .contains("For region across all members, AsyncEvent queue IDs should be same."))) {
        Assert.fail("Expected GatewaySenderConfigurationException", e);
      }
    }
  }
  
  public void testBug50434_PR_Parallel_pass() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 10, isOffHeap()));

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        true, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    vm6.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));
    vm7.invoke(() -> WANTestBase.createSender( "ln", 2,
      true, 100, 10, false, false, null, true ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 10, isOffHeap()));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 10, isOffHeap()));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 10, isOffHeap()));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
      getTestMethodName() + "_PR", null, 1, 10, isOffHeap()));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));
    vm6.invoke(() -> WANTestBase.startSender( "ln" ));
    vm7.invoke(() -> WANTestBase.startSender( "ln" ));
    
    vm4.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    vm5.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    vm6.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    vm7.invoke(() -> WANTestBase.addSenderThroughAttributesMutator(
      getTestMethodName() + "_PR", "ln"));
    
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR",
        10));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_PR", 10));
  }
  
  public void testBug50434_PR_ParallelAsyncEventQueue_Pass() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm5.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm6.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));
    vm7.invoke(() -> WANTestBase.createAsyncEventQueue( "ln",
        true, 100, 100, false, false, null, false ));

    vm4.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 10, isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 10, isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 10, isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 1, 10, isOffHeap() ));

    vm4.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));
    vm5.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));
    vm6.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));
    vm7.invoke(() -> WANTestBase.addAsyncEventQueueThroughAttributesMutator( getTestMethodName() + "_PR", "ln" ));
    
    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR",
        256 ));
    
    vm4.invoke(() -> WANTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm5.invoke(() -> WANTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm6.invoke(() -> WANTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    vm7.invoke(() -> WANTestBase.waitForAsyncQueueToGetEmpty( "ln" ));
    
    int vm4size = (Integer)vm4.invoke(() -> WANTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm5size = (Integer)vm5.invoke(() -> WANTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm6size = (Integer)vm6.invoke(() -> WANTestBase.getAsyncEventListenerMapSize( "ln"));
    int vm7size = (Integer)vm7.invoke(() -> WANTestBase.getAsyncEventListenerMapSize( "ln"));
    
    assertEquals(vm4size + vm5size + vm6size + vm7size, 256);
  }
  
  public void testBug51367_WrongBindAddressOnGatewayReceiver() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm2.invoke(() -> WANTestBase.createReceiverWithBindAddress( lnPort ));
  }
  
  
  public void testBug50247_NonPersistentSenderWithPersistentRegion()
      throws Exception {
    IgnoredException.addIgnoredException("could not get remote locator information");
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    
    try {
      vm4.invoke(() -> WANTestBase.createSender( "ln1", 2,
          true, 10, 100, false, false, null, false ));
      vm4.invoke(() -> WANTestBase.createPartitionedRegionWithPersistence( getTestMethodName() + "_PR", "ln1", 1, 100 ));
      fail("Expected GatewaySenderException with incompatible gateway sender ids and region");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof GatewaySenderException)
          || !(e.getCause().getMessage()
              .contains("can not be attached to persistent region "))) {
        Assert.fail(
            "Expected GatewaySenderException with incompatible gateway sender ids and region",
            e);
      }
    }

    try {
      vm5.invoke(() -> WANTestBase.createPartitionedRegionWithPersistence( getTestMethodName() + "_PR", "ln1", 1, 100 ));
      vm5.invoke(() -> WANTestBase.createSender( "ln1", 2,
          true, 10, 100, false, false, null, false ));
      fail("Expected GatewaySenderException with incompatible gateway sender ids and region");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof GatewaySenderException)
          || !(e.getCause().getMessage()
              .contains("can not be attached to persistent region "))) {
        Assert.fail(
            "Expected GatewaySenderException with incompatible gateway sender ids and region",
            e);
      }
    }
  }
  
  /**
   * Test configuration::
   * 
   * Region: Replicated WAN: Serial Number of WAN sites: 2 Region persistence
   * enabled: false Async channel persistence enabled: false
   */
  public void testReplicatedSerialAsyncEventQueueWith2WANSites() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));
    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    // ------------ START - CREATE CACHE, REGION ON LOCAL SITE ------------//
    vm4.invoke(createCacheRunnable(lnPort));
    vm5.invoke(createCacheRunnable(lnPort));
    vm6.invoke(createCacheRunnable(lnPort));
    vm7.invoke(createCacheRunnable(lnPort));

    vm4.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));
    vm5.invoke(() -> WANTestBase.createSender( "ln", 2,
        false, 100, 10, false, false, null, true ));

    vm4.invoke(() -> WANTestBase.createAsyncEventQueue(
        "lnAsync", false, 100, 100, false, false, null, false ));
    vm5.invoke(() -> WANTestBase.createAsyncEventQueue(
        "lnAsync", false, 100, 100, false, false, null, false ));
    vm6.invoke(() -> WANTestBase.createAsyncEventQueue(
        "lnAsync", false, 100, 100, false, false, null, false ));
    vm7.invoke(() -> WANTestBase.createAsyncEventQueue(
        "lnAsync", false, 100, 100, false, false, null, false ));

    vm4.invoke(() -> WANTestBase.startSender( "ln" ));
    vm5.invoke(() -> WANTestBase.startSender( "ln" ));

    vm4.invoke(() -> WANTestBase.createReplicatedRegionWithSenderAndAsyncEventQueue(
            getTestMethodName() + "_RR", "ln", "lnAsync", isOffHeap() ));
    vm5.invoke(() -> WANTestBase.createReplicatedRegionWithSenderAndAsyncEventQueue(
            getTestMethodName() + "_RR", "ln", "lnAsync", isOffHeap() ));
    vm6.invoke(() -> WANTestBase.createReplicatedRegionWithSenderAndAsyncEventQueue(
            getTestMethodName() + "_RR", "ln", "lnAsync", isOffHeap() ));
    vm7.invoke(() -> WANTestBase.createReplicatedRegionWithSenderAndAsyncEventQueue(
            getTestMethodName() + "_RR", "ln", "lnAsync", isOffHeap() ));
    // ------------- END - CREATE CACHE, REGION ON LOCAL SITE -------------//

    // ------------- START - CREATE CACHE ON REMOTE SITE ---------------//
    vm2.invoke(() -> WANTestBase.createReceiver( nyPort ));
    vm3.invoke(() -> WANTestBase.createReceiver( nyPort ));

    vm2.invoke(() -> WANTestBase.createSender( "ny", 1,
        false, 100, 10, false, false, null, true ));
    vm3.invoke(() -> WANTestBase.createSender( "ny", 1,
        false, 100, 10, false, false, null, true ));

    vm2.invoke(() -> WANTestBase.createAsyncEventQueue(
        "nyAsync", false, 100, 100, false, false, null, false ));
    vm3.invoke(() -> WANTestBase.createAsyncEventQueue(
        "nyAsync", false, 100, 100, false, false, null, false ));

    vm2.invoke(() -> WANTestBase.startSender( "ny" ));
    vm3.invoke(() -> WANTestBase.startSender( "ny" ));

    vm2.invoke(() -> WANTestBase.createReplicatedRegionWithSenderAndAsyncEventQueue(
            getTestMethodName() + "_RR", "ny", "nyAsync", isOffHeap() ));
    vm3.invoke(() -> WANTestBase.createReplicatedRegionWithSenderAndAsyncEventQueue(
            getTestMethodName() + "_RR", "ny", "nyAsync", isOffHeap() ));

    // ------------- END - CREATE CACHE, REGION ON REMOTE SITE -------------//

    vm4.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR",
        1000 ));

    // validate AsyncEventListener on local site
    vm4.invoke(() -> WANTestBase.validateAsyncEventListener( "lnAsync", 1000 ));// primary sender
    vm5.invoke(() -> WANTestBase.validateAsyncEventListener( "lnAsync", 0 ));// secondary
    vm6.invoke(() -> WANTestBase.validateAsyncEventListener( "lnAsync", 0 ));// secondary
    vm7.invoke(() -> WANTestBase.validateAsyncEventListener( "lnAsync", 0 ));// secondary

    // validate region size on remote site
    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));
    vm3.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 1000 ));

    // validate AsyncEventListener on remote site
    vm2.invoke(() -> WANTestBase.validateAsyncEventListener( "nyAsync", 1000 ));// primary sender
    vm3.invoke(() -> WANTestBase.validateAsyncEventListener( "nyAsync", 0 ));// secondary

  }
}
