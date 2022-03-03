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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class ReplicatedRegion_ParallelWANPropagationDUnitTest extends WANTestBase {

  public ReplicatedRegion_ParallelWANPropagationDUnitTest() {
    super();
    // TODO Auto-generated constructor stub
  }

  final String expectedExceptions = null;


  private static final long serialVersionUID = 1L;

  @Test
  public void test_DR_PGS_1Nodes_Put_Receiver() throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      createCacheInVMs(nyPort, vm2);
      vm2.invoke(WANTestBase::createReceiver);
      vm2.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

      createCacheInVMs(lnPort, vm4);

      vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));

      vm4.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));

      vm4.invoke(() -> WANTestBase.startSender("ln1"));
      fail(
          "Expected GatewaySenderConfigException where parallel gateway sender can not be used with replicated region");
    } catch (Exception e) {
      if (!e.getCause().getMessage().contains("can not be used with replicated region")) {
        fail(
            "Expected GatewaySenderConfigException where parallel gateway sender can not be used with replicated region");
      }
    }
  }

  /*
   * 1. Validate that parallelGatewaySenderId can be added to distributed region Region distributed
   * ack/noack + PGS 1. Find out the restrictions on totalNumBuckets on shadowPR 2. Find out the
   * restrictions on redundancy on shadowPR 3. Find out the restrictions on localMaxMemory on
   * shadowPR 4. Find out the best way user will specify PR attributes to PGS 5. Find out the
   * restrictions on ordering. 6. put on region populates the queue 7. put on region reaches to
   * remote site. Dispatcher works as expected 8. m1 and m2 has DR(ack/noack). put on DR from m1
   * populates queue on both m1 and m2. Validate that remote site got all the events 9. m1 and m2
   * has DR(ack/noack). create/put/destroy/operations populates the queue. Validate that remote site
   * got correct events 10. m1 and m2 has DR(ack/noack). localDestroy is called on m1's DR. This
   * locally destroys M1's shadowPr 11. m1 and m2 has DR(ack/noack). destroy is called on m1's DR.
   * This destroys entire shadowPr on m1 and m2 12. m1 and m2 has DR(ack/noack). close Region is
   * called on m1's DR. This locally destroys shadowPr on m1 13. m1 and m2 has DR(ack/noack).
   * cache.close on m1'. This locally destroys shadowPr on m1 14. Validate HA scenario does not
   * cause any event loss 15. PDX events of DR are propagated to remote sites 16. validate stats 17:
   * PR and DR regions with same name.. Can this be created. If yes then how to differentiate these
   * 2 different shadowPR. 18. test for redundancy. FOR SPR's redundancy will be equal to the number
   * of nodes where DR is present. Max is 3. I know this needs to be figure it out at runtime. 19.
   * test without providing diskStoreName..I suspect some problem with this code. diskStoreName=null
   * looks like this is not handled very well. need to verify 20. ParallelGatewaySenderQueue#addPR
   * method has multiple check for inPersistenceEnabled. Can's we do it with only one check.
   */

  /**
   * Test to validate that created parallel gatewaySenders id can be added to distributed region
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_PGS_Started_DR_CREATED_NO_RECEIVER() throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      vm4.invoke(() -> WANTestBase.createCache(lnPort));
      /*
       * ExpectedException exp1 = addExpectedException(GatewaySenderException.class .getName(),
       * vm4); ExpectedException exp2 = addExpectedException(InterruptedException.class .getName(),
       * vm4); try {
       */ vm4.invoke(
          () -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, false));
      vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));
      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
      vm4.invoke(() -> WANTestBase.validateQueueContents("ln1", 1000));

      /*
       * } finally { exp1.remove(); exp2.remove(); }
       */ } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }

  /**
   * Test to validate that distributed region with given parallelGatewaySender id is created first
   * and then a same parallelGatewaySender is created a single put in DR is enqueued in
   * parallelQueue Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade
   * support queue name is changed to old style 2>Common parallel sender for different non colocated
   * regions is not supported in 8.0 so no need to bother about
   * ParallelGatewaySenderQueue#convertPathToName 3> We have to enabled it in next release 4>
   * Version based rolling upgrade support should be provided. based on the version of the gemfire
   * QSTRING should be used between 8.0 and version prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_CREATED_PGS_STARTED_NO_RECEIVER() throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      vm4.invoke(() -> WANTestBase.createCache(lnPort));
      vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));
      /*
       * ExpectedException exp1 = addExpectedException(GatewaySenderException.class .getName(),
       * vm4); ExpectedException exp2 = addExpectedException(InterruptedException.class .getName(),
       * vm4); try {
       */
      vm4.invoke(
          () -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, false));
      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
      vm4.invoke(() -> WANTestBase.validateQueueContents("ln1", 1000));
      /*
       * } finally { exp1.remove(); exp2.remove(); }
       */ } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_PGS_1Node_Put_ValidateQueue_No_Receiver() throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      vm4.invoke(() -> WANTestBase.createCache(lnPort));

      vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));

      /*
       * ExpectedException exp1 = addExpectedException(GatewaySenderException.class .getName(),
       * vm4); ExpectedException exp2 = addExpectedException(InterruptedException.class .getName(),
       * vm4); try {
       */
      vm4.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));
      vm4.invoke(() -> WANTestBase.startSender("ln1"));

      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 10000));

      vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 10000));
      vm4.invoke(() -> WANTestBase.validateQueueContents("ln1", 10000));
      /*
       * } finally { exp1.remove(); exp2.remove(); }
       */
    } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_PGS_2Nodes_Put_ValidateQueue_No_Receiver() throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      vm4.invoke(() -> WANTestBase.createCache(lnPort));
      vm5.invoke(() -> WANTestBase.createCache(lnPort));

      vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));
      vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));

      /*
       * ExpectedException exp1 = addExpectedException( GatewaySenderException.class.getName());
       * ExpectedException exp2 = addExpectedException( InterruptedException.class.getName());
       * ExpectedException exp3 = addExpectedException( CacheClosedException.class.getName()); try {
       */ vm4.invoke(
          () -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));
      vm5.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));

      startSenderInVMs("ln1", vm4, vm5);

      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

      vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
      vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

      vm4.invoke(() -> WANTestBase.validateQueueContents("ln1", 1000));
      vm5.invoke(() -> WANTestBase.validateQueueContents("ln1", 1000));

      /*
       * } finally { exp1.remove(); exp2.remove(); exp3.remove(); }
       */
    } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }

  // public void test_DR_PGS_ORDERPOLICY_PARTITION_EXPECTException(){
  //
  // }
  // public void test_DR_PGS_DISKSTORE_NAME_PROVIDED_VALIDATE_DISK(){
  //
  // }
  // public void test_DR_PGS_DISKSTORE_NAME_NOT_PROVIDED_VALIDATE_DISK(){
  //
  // }
  //
  // public void test_DR_PGS_START_STOP_START(){
  //
  // }
  //
  // public void test_DR_PGS_PERSISTENCE_START_STOP_START(){
  //
  // }
  //
  // public void test_DR_PGS_START_PAUSE_STOP(){
  //
  // }
  //
  // public void test_DR_PGS_START_PAUSE_RESUME_VALIDATE_RECEIVER(){
  //
  // }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_PGS_1Nodes_Put_Receiver_2() throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      createCacheInVMs(nyPort, vm2);
      vm2.invoke(WANTestBase::createReceiver);
      vm2.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

      createCacheInVMs(lnPort, vm4);

      vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));

      vm4.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));

      vm4.invoke(() -> WANTestBase.startSender("ln1"));

      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

      vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

      vm4.invoke(() -> WANTestBase.validateQueueContents("ln1", 0));

      vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_PGS_2Nodes_Put_Receiver() throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      createCacheInVMs(nyPort, vm2);
      vm2.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
      vm2.invoke(WANTestBase::createReceiver);

      createCacheInVMs(lnPort, vm4, vm5);

      vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));
      vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));

      vm4.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));
      vm5.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));

      startSenderInVMs("ln1", vm4, vm5);

      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

      vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
      vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

      vm4.invoke(() -> WANTestBase.validateQueueContents("ln1", 0));
      vm5.invoke(() -> WANTestBase.validateQueueContents("ln1", 0));

      vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_PGS_2Nodes_EMPTY_Put_Receiver() throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      vm2.invoke(() -> WANTestBase.createCache(nyPort));
      vm2.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
      vm2.invoke(WANTestBase::createReceiver);

      vm4.invoke(() -> WANTestBase.createCache(lnPort));
      vm5.invoke(() -> WANTestBase.createCache(lnPort));

      vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          Scope.DISTRIBUTED_ACK, DataPolicy.EMPTY, isOffHeap()));
      vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          Scope.DISTRIBUTED_ACK, DataPolicy.REPLICATE, isOffHeap()));

      vm4.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));
      vm5.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));

      startSenderInVMs("ln1", vm4, vm5);

      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

      // vm4.invoke(() -> WANTestBase.validateRegionSize( testName + "_RR",
      // 1000 ));
      vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

      vm4.invoke(() -> WANTestBase.validateQueueContents("ln1", 0));
      vm5.invoke(() -> WANTestBase.validateQueueContents("ln1", 0));

      vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_PR_PGS_4Nodes_Put_Receiver_2Nodes() throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      createCacheInVMs(nyPort, vm2, vm3);
      vm2.invoke(WANTestBase::createReceiver);
      vm3.invoke(WANTestBase::createReceiver);

      vm2.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
      vm3.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

      vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1,
          100, isOffHeap()));
      vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 1,
          100, isOffHeap()));

      createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

      vm4.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1,
          100, isOffHeap()));
      vm5.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1,
          100, isOffHeap()));
      vm6.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1,
          100, isOffHeap()));
      vm7.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 1,
          100, isOffHeap()));

      vm4.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
      vm5.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
      vm6.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
      vm7.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

      vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 100, false, false, null, true));
      vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 100, false, false, null, true));
      vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 100, false, false, null, true));
      vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 10, 100, false, false, null, true));

      startSenderInVMs("ln", vm4, vm5, vm6, vm7);

      vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
      vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
      vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
      vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
      vm5.invoke(() -> WANTestBase.doNextPuts(getTestMethodName() + "_PR", 1000, 2000));

      vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
      vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

      vm4.invoke(() -> WANTestBase.validateQueueContents("ln", 0));
      vm5.invoke(() -> WANTestBase.validateQueueContents("ln", 0));

      /*
       * ExpectedException exp1 = addExpectedException(CacheClosedException.class .getName()); try {
       */
      vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
      vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
      vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
      vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 1000));
      /*
       * } finally { exp1.remove(); }
       */ } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_PGS_NOMANUALSTART_4Nodes_Put_ValidateReceiver() throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      createCacheInVMs(nyPort, vm2);
      vm2.invoke(WANTestBase::createReceiver);
      vm2.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

      createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

      vm4.invoke(
          () -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, false));
      vm5.invoke(
          () -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, false));
      vm6.invoke(
          () -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, false));
      vm7.invoke(
          () -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, false));

      vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));
      vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));
      vm6.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));
      vm7.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          isOffHeap()));

      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

      vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
      vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
      vm6.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
      vm7.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));


      vm4.invoke(() -> WANTestBase.validateQueueContents("ln1", 0));
      vm5.invoke(() -> WANTestBase.validateQueueContents("ln1", 0));
      vm6.invoke(() -> WANTestBase.validateQueueContents("ln1", 0));
      vm7.invoke(() -> WANTestBase.validateQueueContents("ln1", 0));

      /*
       * ExpectedException exp1 = addExpectedException(CacheClosedException.class .getName()); try {
       */
      vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
      /*
       * } finally { exp1.remove(); }
       */
    } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_PGS_4Nodes_Put_CLOSE4NODESCACHE_RECREATE_PUT_ValidateReceiver()
      throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      createCacheInVMs(nyPort, vm2, vm3);
      createReceiverInVMs(vm2, vm3);

      createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

      vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
      vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
      vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
      vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

      vm4.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
      vm5.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
      vm6.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
      vm7.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

      startSenderInVMs("ln", vm4, vm5, vm6, vm7);

      vm2.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
      vm3.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

      // before doing any puts, let the senders be running in order to ensure
      // that
      // not a single event will be lost
      vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
      vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
      vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
      vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));
      vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

      /*
       * ExpectedException exp1 = addExpectedException(CacheClosedException.class .getName()); try {
       */
      vm4.invoke(() -> WANTestBase.killSender());
      vm5.invoke(() -> WANTestBase.killSender());
      vm6.invoke(() -> WANTestBase.killSender());
      vm7.invoke(() -> WANTestBase.killSender());
      /*
       * } finally { exp1.remove(); }
       */

      createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

      vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
      vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
      vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
      vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

      vm4.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
      vm5.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
      vm6.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
      vm7.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

      startSenderInVMs("ln", vm4, vm5, vm6, vm7);

      vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
      vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
      vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
      vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
      // ------------------------------------------------------------------------------------

      vm4.invoke(() -> WANTestBase.doNextPuts(getTestMethodName() + "_RR", 1000, 2000));

      // verify all buckets drained on all sender nodes.
      vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
      vm5.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
      vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
      vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

      /*
       * exp1 = addExpectedException(CacheClosedException.class.getName()); try {
       */
      vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 2000));
      vm3.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 2000));
      /*
       * } finally { exp1.remove(); }
       */
    } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }

  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_NO_ACK_PGS_2Nodes_Put_ValidateQueue_Receiver() throws Exception {
    try {
      Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
      Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

      createCacheInVMs(nyPort, vm2);
      vm2.invoke(
          () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));
      vm2.invoke(WANTestBase::createReceiver);

      createCacheInVMs(lnPort, vm4, vm5);

      vm4.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          Scope.DISTRIBUTED_NO_ACK, DataPolicy.REPLICATE, isOffHeap()));
      vm5.invoke(() -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln1",
          Scope.DISTRIBUTED_NO_ACK, DataPolicy.REPLICATE, isOffHeap()));

      vm4.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));
      vm5.invoke(() -> WANTestBase.createSender("ln1", 2, true, 10, 100, false, false, null, true));

      startSenderInVMs("ln1", vm4, vm5);

      vm4.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 1000));

      vm4.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
      vm5.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));

      vm4.invoke(() -> WANTestBase.validateQueueContents("ln1", 0));
      vm5.invoke(() -> WANTestBase.validateQueueContents("ln1", 0));

      vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
    } catch (Exception e) {
      Assert.fail("Unexpected exception", e);
    }
  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_PGS_2NODES_1NODESDOWN_Validate_Receiver() throws Exception {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    startSenderInVMs("ln", vm4, vm5);

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    Thread.sleep(60000);

    /*
     * ExpectedException exp1 = addExpectedException(CacheClosedException.class .getName()); try {
     */
    AsyncInvocation inv1 = vm4.invokeAsync(() -> ReplicatedRegion_ParallelWANPropagationDUnitTest
        .doPuts0(getTestMethodName() + "_RR", 1000));
    Wait.pause(1000);
    AsyncInvocation inv2 = vm5.invokeAsync(() -> WANTestBase.killSender());
    try {
      inv1.join();
      inv2.join();
    } catch (Exception e) {
      Assert.fail("UnExpected Exception", e);
    }
    /*
     * } finally { exp1.remove(); }
     */

    Integer size = vm4.invoke(() -> WANTestBase.getQueueContentSize("ln"));
    LogWriterUtils.getLogWriter().info("The size of the queue is in vm4 " + size);


    vm4.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    size = vm4.invoke(() -> WANTestBase.getQueueContentSize("ln"));
    LogWriterUtils.getLogWriter().info("The size of the queue is in vm4 " + size);

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 1000));
  }

  /**
   * Below test is disabled intentionally 1> In this release 8.0, for rolling upgrade support queue
   * name is changed to old style 2>Common parallel sender for different non colocated regions is
   * not supported in 8.0 so no need to bother about ParallelGatewaySenderQueue#convertPathToName 3>
   * We have to enabled it in next release 4> Version based rolling upgrade support should be
   * provided. based on the version of the gemfire QSTRING should be used between 8.0 and version
   * prior to 8.0
   */
  @Ignore
  @Test
  public void test_DR_PGS_4NODES_2NODESDOWN_Validate_Receiver() throws Exception {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm5.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm6.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));
    vm7.invoke(() -> WANTestBase.createSender("ln", 2, true, 100, 10, false, false, null, true));

    vm4.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm5.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm6.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm7.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm4.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm5.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm6.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));
    vm7.invoke(() -> WANTestBase.waitForSenderRunningState("ln"));

    Thread.sleep(60000);
    /*
     * ExpectedException exp1 = addExpectedException(CacheClosedException.class .getName()); try
     */ {
      AsyncInvocation inv1 = vm7.invokeAsync(() -> ReplicatedRegion_ParallelWANPropagationDUnitTest
          .doPuts0(getTestMethodName() + "_RR", 10000));
      Thread.sleep(1000);
      AsyncInvocation inv2 = vm4.invokeAsync(() -> WANTestBase.killSender());
      Thread.sleep(2000);
      AsyncInvocation inv3 = vm6.invokeAsync(() -> ReplicatedRegion_ParallelWANPropagationDUnitTest
          .doPuts1(getTestMethodName() + "_RR", 10000));
      Thread.sleep(1500);
      AsyncInvocation inv4 = vm5.invokeAsync(() -> WANTestBase.killSender());
      try {
        inv1.join();
        inv2.join();
        inv3.join();
        inv4.join();
      } catch (Exception e) {
        Assert.fail("UnExpected Exception", e);
      }
    } /*
       * finally { exp1.remove(); }
       */

    vm6.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));
    vm7.invoke(() -> WANTestBase.validateParallelSenderQueueAllBucketsDrained("ln"));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 10000));

  }

  public static void doPuts0(String regionName, int numPuts) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(CacheClosedException.class.getName());
    try {

      Region r = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(r);
      for (long i = 0; i < numPuts; i++) {
        LogWriterUtils.getLogWriter().info("Put : key : " + i);
        r.put(i, "0_" + i);
      }
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void doPuts1(String regionName, int numPuts) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(CacheClosedException.class.getName());
    try {

      Region r = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(r);
      for (long i = 0; i < numPuts; i++) {
        LogWriterUtils.getLogWriter().info("Put : key : " + i);
        r.put(i, "1_" + i);
      }
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  public static void doPuts2(String regionName, int numPuts) {
    IgnoredException exp =
        IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
    IgnoredException exp1 =
        IgnoredException.addIgnoredException(CacheClosedException.class.getName());
    try {
      Region r = cache.getRegion(SEPARATOR + regionName);
      assertNotNull(r);
      for (long i = 0; i < numPuts; i++) {
        LogWriterUtils.getLogWriter().info("Put : key : " + i);
        r.put(i, "2_" + i);
      }
    } finally {
      exp.remove();
      exp1.remove();
    }
  }

  /**
   * Test to validate that put on DR with no ack on multiple nodes are propagated to parallelQueue
   * on multiple nodes
   */

  /**
   * Test to validate that the single put in DR is propagated to remote site through
   * parallelGatewaySender
   */


}
