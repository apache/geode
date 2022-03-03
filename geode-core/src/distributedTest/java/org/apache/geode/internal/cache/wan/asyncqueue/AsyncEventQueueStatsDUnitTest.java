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
package org.apache.geode.internal.cache.wan.asyncqueue;

import static junit.framework.TestCase.assertNotNull;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.SenderIdMonitor;
import org.apache.geode.internal.cache.wan.AsyncEventQueueTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.categories.WanTest;

@Category({AEQTest.class})
public class AsyncEventQueueStatsDUnitTest extends AsyncEventQueueTestBase {

  private static final long serialVersionUID = 1L;

  public AsyncEventQueueStatsDUnitTest() {
    super();
  }

  /**
   * Normal replication scenario
   */
  @Test
  public void testReplicatedSerialPropagation() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm3.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm4.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        false, null, false));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        false, null, false));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        false, null, false));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        false, null, false));

    vm1.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm3.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm4.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 1000));// primary
                                                                                     // sender
    Wait.pause(2000);// give some time for system to become stable

    vm1.invoke(
        () -> AsyncEventQueueTestBase.checkAsyncEventQueueStats("ln", 0, 0, 1000, 1000, 1000));
    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueBatchStats("ln", 10));
    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueStats("ln", 0, 0, 1000, 0, 0));
    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueBatchStats("ln", 0));
  }

  /**
   * Two listeners added to the same RR.
   */
  @Category(WanTest.class)
  @Test
  public void testAsyncStatsTwoListeners() throws Exception {
    Integer lnPort = createFirstLocatorWithDSId(1);

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm3.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm4.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln1", false, 100, 100, false,
        false, null, false));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln1", false, 100, 100, false,
        false, null, false));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln1", false, 100, 100, false,
        false, null, false));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln1", false, 100, 100, false,
        false, null, false));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln2", false, 100, 100, false,
        false, null, false));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln2", false, 100, 100, false,
        false, null, false));
    vm3.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln2", false, 100, 100, false,
        false, null, false));
    vm4.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln2", false, 100, 100, false,
        false, null, false));

    vm1.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR", "ln1,ln2", isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR", "ln1,ln2", isOffHeap()));
    vm3.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR", "ln1,ln2", isOffHeap()));
    vm4.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR", "ln1,ln2", isOffHeap()));

    vm1.invoke(
        () -> AsyncEventQueueTestBase.pauseAsyncEventQueueAndWaitForDispatcherToPause("ln1"));
    vm2.invoke(
        () -> AsyncEventQueueTestBase.pauseAsyncEventQueueAndWaitForDispatcherToPause("ln1"));
    vm3.invoke(
        () -> AsyncEventQueueTestBase.pauseAsyncEventQueueAndWaitForDispatcherToPause("ln1"));
    vm4.invoke(
        () -> AsyncEventQueueTestBase.pauseAsyncEventQueueAndWaitForDispatcherToPause("ln1"));
    vm1.invoke(
        () -> AsyncEventQueueTestBase.pauseAsyncEventQueueAndWaitForDispatcherToPause("ln2"));
    vm2.invoke(
        () -> AsyncEventQueueTestBase.pauseAsyncEventQueueAndWaitForDispatcherToPause("ln2"));
    vm3.invoke(
        () -> AsyncEventQueueTestBase.pauseAsyncEventQueueAndWaitForDispatcherToPause("ln2"));
    vm4.invoke(
        () -> AsyncEventQueueTestBase.pauseAsyncEventQueueAndWaitForDispatcherToPause("ln2"));

    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts(getTestMethodName() + "_RR", 1000));

    vm1.invoke(
        () -> AsyncEventQueueTestBase.checkAsyncEventQueueStats("ln1", 1000, 0, 1000, 1000, 0));
    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueStats("ln2", 1000, 0, 1000, 0, 0));

    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue("ln1"));
    vm2.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue("ln1"));
    vm3.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue("ln1"));
    vm4.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue("ln1"));
    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue("ln2"));
    vm2.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue("ln2"));
    vm3.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue("ln2"));
    vm4.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue("ln2"));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln1", 1000));
    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln2", 1000));
    Wait.pause(2000);// give some time for system to become stable

    vm1.invoke(
        () -> AsyncEventQueueTestBase.checkAsyncEventQueueStats("ln1", 0, 0, 1000, 1000, 1000));
    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueBatchStats("ln1", 10));
    vm1.invoke(
        () -> AsyncEventQueueTestBase.checkAsyncEventQueueStats("ln2", 0, 0, 1000, 1000, 1000));
    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueBatchStats("ln2", 10));
    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueStats("ln1", 0, 0, 1000, 0, 0));
    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueBatchStats("ln1", 0));
    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueStats("ln2", 0, 0, 1000, 0, 0));
    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueBatchStats("ln2", 0));
  }

  /**
   * HA scenario: kill one vm when puts are in progress on the other vm.
   */
  @Test
  public void testReplicatedSerialPropagationHA() throws Exception {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm3.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm4.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        false, null, false));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        false, null, false));

    vm1.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm3.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm4.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));

    AsyncInvocation inv1 =
        vm2.invokeAsync(() -> AsyncEventQueueTestBase.doPuts(getTestMethodName() + "_RR", 10000));
    Wait.pause(2000);
    AsyncInvocation inv2 = vm1.invokeAsync(() -> AsyncEventQueueTestBase.killAsyncEventQueue("ln"));
    Boolean isKilled = Boolean.FALSE;
    try {
      isKilled = (Boolean) inv2.getResult();
    } catch (Throwable e) {
      fail("Unexpected exception while killing a AsyncEventQueue");
    }
    AsyncInvocation inv3 = null;
    if (!isKilled) {
      inv3 = vm2.invokeAsync(() -> AsyncEventQueueTestBase.killSender("ln"));
      inv3.join();
    }
    inv1.join();
    inv2.join();
    Wait.pause(2000);// give some time for system to become stable
    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueStats_Failover("ln", 10000));
  }

  /**
   * Two regions attached to same AsyncEventQueue
   */
  @Test
  public void testReplicatedSerialPropagationUnprocessedEvents() throws Exception {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm3.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm4.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        false, null, false));
    vm2.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        false, null, false));

    // create one RR (RR_1) on local site
    vm1.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR_1", "ln", isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR_1", "ln", isOffHeap()));
    vm3.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR_1", "ln", isOffHeap()));
    vm4.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR_1", "ln", isOffHeap()));

    // create another RR (RR_2) on local site
    vm1.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR_2", "ln", isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR_2", "ln", isOffHeap()));
    vm3.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR_2", "ln", isOffHeap()));
    vm4.invoke(() -> AsyncEventQueueTestBase.createReplicatedRegionWithAsyncEventQueue(
        getTestMethodName() + "_RR_2", "ln", isOffHeap()));

    // start puts in RR_1 in another thread
    vm1.invoke(() -> AsyncEventQueueTestBase.doPuts(getTestMethodName() + "_RR_1", 1000));
    // do puts in RR_2 in main thread
    vm1.invoke(() -> AsyncEventQueueTestBase.doPutsFrom(getTestMethodName() + "_RR_2", 1000, 1500));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 1500));

    Wait.pause(2000);// give some time for system to become stable
    vm1.invoke(
        () -> AsyncEventQueueTestBase.checkAsyncEventQueueStats("ln", 0, 0, 1500, 1500, 1500));
    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueUnprocessedStats("ln", 0));


    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueStats("ln", 0, 0, 1500, 0, 0));
    vm2.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueUnprocessedStats("ln", 1500));
  }

  /**
   * Test with conflation enabled
   */
  @Test
  public void testSerialPropagationConflation() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm3.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm4.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, true,
        false, null, false));

    vm1.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm3.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm4.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm1.invoke(() -> AsyncEventQueueTestBase.pauseAsyncEventQueue("ln"));
    // pause at least for the batchTimeInterval to make sure that the AsyncEventQueue is actually
    // paused
    Wait.pause(2000);

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for (int i = 0; i < 1000; i++) {
      keyValues.put(i, i);
    }

    vm1.invoke(
        () -> AsyncEventQueueTestBase.putGivenKeyValue(getTestMethodName() + "_RR", keyValues));
    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize("ln", keyValues.size()));

    for (int i = 0; i < 500; i++) {
      updateKeyValues.put(i, i + "_updated");
    }

    // Put the update events and check the queue size.
    // There should be no conflation with the previous create events.
    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(getTestMethodName() + "_RR",
        updateKeyValues));
    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize("ln",
        keyValues.size() + updateKeyValues.size()));

    // Put the update events again and check the queue size.
    // There should be conflation with the previous update events.
    vm1.invoke(() -> AsyncEventQueueTestBase.putGivenKeyValue(getTestMethodName() + "_RR",
        updateKeyValues));
    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueSize("ln",
        keyValues.size() + updateKeyValues.size()));

    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 0));

    vm1.invoke(() -> AsyncEventQueueTestBase.resumeAsyncEventQueue("ln"));
    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 1000));

    Wait.pause(2000);// give some time for system to become stable
    vm1.invoke(
        () -> AsyncEventQueueTestBase.checkAsyncEventQueueStats("ln", 0, 0, 2000, 2000, 1000));
    vm1.invoke(() -> AsyncEventQueueTestBase.checkAsyncEventQueueConflatedStats("ln", 500));
  }

  @Test
  public void testPartitionedProxyShouldHaveSameAEQId() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        true, null, false));

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        getTestMethodName() + "_PR", "ln", isOffHeap()));
    vm2.invoke(() -> AsyncEventQueueTestBase
        .createPartitionedRegionAccessorWithAsyncEventQueue(getTestMethodName() + "_PR", "ln"));

    vm2.invoke(() -> AsyncEventQueueTestBase.doPuts(getTestMethodName() + "_PR", 100));
    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 100));
  }

  @Test
  public void testPartitionedProxyWithoutSameAEQIdShouldLogWarning() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        true, null, false));

    String regionName = getTestMethodName() + "_PR";
    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        regionName, "ln", isOffHeap()));
    vm2.invoke(() -> {
      AttributesFactory fact = new AttributesFactory();
      PartitionAttributesFactory pfact = new PartitionAttributesFactory();
      pfact.setTotalNumBuckets(16);
      pfact.setLocalMaxMemory(0);
      fact.setPartitionAttributes(pfact.create());
      Region r = cache.createRegionFactory(fact.create()).create(regionName);
      assertNotNull(r);
    });

    vm1.invoke(() -> verifyIdConsistencyWarning(regionName, false, false));
    vm2.invoke(() -> {
      AsyncEventQueueTestBase.doPuts(regionName, 100);
    });
    vm1.invoke(() -> verifyIdConsistencyWarning(regionName, true, false));
  }

  @Test
  public void testReplicatedProxyShouldHaveSameAEQId() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        false, null, false));

    vm1.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln", isOffHeap()));
    vm2.invoke(() -> {
      AttributesFactory fact = new AttributesFactory();
      fact.addAsyncEventQueueId("ln");
      fact.setDataPolicy(DataPolicy.EMPTY);
      fact.setOffHeap(isOffHeap());
      RegionFactory regionFactory = cache.createRegionFactory(fact.create());
      Region r = regionFactory.create(getTestMethodName() + "_RR");
      assertNotNull(r);
    });

    vm2.invoke(() -> AsyncEventQueueTestBase.doPuts(getTestMethodName() + "_RR", 100));
    vm1.invoke(() -> AsyncEventQueueTestBase.validateAsyncEventListener("ln", 100));
  }

  @Test
  public void testReplicatedProxyWithoutSameAEQIdToLogWarning() {
    Integer lnPort =
        vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));

    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm2.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", false, 100, 100, false,
        false, null, false));

    String regionName = getTestMethodName() + "_RR";
    vm1.invoke(() -> AsyncEventQueueTestBase
        .createReplicatedRegionWithAsyncEventQueue(regionName, "ln", isOffHeap()));
    vm2.invoke(() -> {
      AttributesFactory fact = new AttributesFactory();
      fact.setDataPolicy(DataPolicy.EMPTY);
      fact.setOffHeap(isOffHeap());
      RegionFactory regionFactory = cache.createRegionFactory(fact.create());
      Region r = regionFactory.create(regionName);
      assertNotNull(r);
    });

    vm1.invoke(() -> verifyIdConsistencyWarning(regionName, false, false));
    vm2.invoke(() -> {
      AsyncEventQueueTestBase.doPuts(regionName, 100);
    });
    vm1.invoke(() -> verifyIdConsistencyWarning(regionName, true, false));
  }

  private void verifyIdConsistencyWarning(String regionName, boolean expected,
      boolean gatewaySenderId) {
    Region r = cache.getRegion(SEPARATOR + regionName);
    SenderIdMonitor senderIdMonitor = getSenderIdMonitor(r);
    if (gatewaySenderId) {
      assertThat(senderIdMonitor.getGatewaySenderIdsDifferWarningMessage()).isEqualTo(expected);
    } else {
      assertThat(senderIdMonitor.getAsyncQueueIdsDifferWarningMessage()).isEqualTo(expected);
    }
  }

  private SenderIdMonitor getSenderIdMonitor(Region r) {
    if (r instanceof DistributedRegion) {
      return ((DistributedRegion) r).getSenderIdMonitor();
    } else if (r instanceof PartitionedRegion) {
      return ((PartitionedRegion) r).getSenderIdMonitor();
    } else {
      throw new IllegalStateException(
          "expected region to be distributed or partitioned but it was: " + r.getClass());
    }
  }
}
