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

import static java.util.Arrays.asList;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.cache30.MyGatewayTransportFilter1;
import org.apache.geode.cache30.MyGatewayTransportFilter2;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.SenderIdMonitor;
import org.apache.geode.internal.cache.wan.AsyncEventQueueConfigurationException;
import org.apache.geode.internal.cache.wan.Filter70;
import org.apache.geode.internal.cache.wan.GatewayReceiverException;
import org.apache.geode.internal.cache.wan.GatewaySenderConfigurationException;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.MyGatewayTransportFilter3;
import org.apache.geode.internal.cache.wan.MyGatewayTransportFilter4;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class WanValidationsDUnitTest extends WANTestBase {

  public WanValidationsDUnitTest() {
    super();
  }

  private SenderIdMonitor getSenderIdMonitor(Region<Object, Object> r) {
    if (r instanceof DistributedRegion) {
      return ((DistributedRegion) r).getSenderIdMonitor();
    } else if (r instanceof PartitionedRegion) {
      return ((PartitionedRegion) r).getSenderIdMonitor();
    } else {
      throw new IllegalStateException(
          "expected region to be distributed or partitioned but it was: " + r.getClass());
    }
  }

  private void verifyIdConsistencyWarning(String regionName, boolean expected,
      boolean gatewaySenderId) {
    Region<Object, Object> r = cache.getRegion(SEPARATOR + regionName);
    SenderIdMonitor senderIdMonitor = getSenderIdMonitor(r);

    if (gatewaySenderId) {
      assertThat(senderIdMonitor.getGatewaySenderIdsDifferWarningMessage()).isEqualTo(expected);
    } else {
      assertThat(senderIdMonitor.getAsyncQueueIdsDifferWarningMessage()).isEqualTo(expected);
    }
  }

  private void verifyIdConsistencyWarningOnVms(String regionName, boolean expected,
      boolean gatewaySenderId) {
    for (VM vm : asList(vm4, vm5, vm6, vm7)) {
      vm.invoke(() -> verifyIdConsistencyWarning(regionName, expected, gatewaySenderId));
    }
  }

  private void verifyGatewaySenderIdWarning(String regionName) {
    verifyIdConsistencyWarningOnVms(regionName, true, true);
  }

  private void verifyNoAsyncEventQueueIdWarning(String regionName) {
    verifyIdConsistencyWarningOnVms(regionName, false, false);
  }

  private void verifyAsyncEventQueueIdWarning(String regionName) {
    verifyIdConsistencyWarningOnVms(regionName, true, false);
  }

  private void verifyNoGatewaySenderIdWarning(String regionName) {
    verifyIdConsistencyWarningOnVms(regionName, false, true);
  }

  /**
   * Test to make sure that serial sender Ids configured in Distributed Region is same across all DR
   * nodes TODO : Should this validation hold tru now. Discuss. If I have 2 members on Which DR is
   * defined. But sender is defined on only one member. How can I add the instance on the sender in
   * Region which does not have a sender. I can bypass the existing validation for the DR with
   * SerialGatewaySender. But for PR with SerialGatewaySender, we need to send the adjunct message.
   * Find out the way to send the adjunct message to the member on which serialGatewaySender is
   * available.
   */
  @Test
  public void replicateRegionCreationShouldFailWhenSerialGatewaySenderIdsAreDifferentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> {
      createSender("ln1", 2, false, 10, 100, false, false, null, true);
      createSender("ln2", 2, false, 10, 100, false, false, null, true);
    });

    vm5.invoke(() -> {
      createSender("ln2", 2, false, 10, 100, false, false, null, true);
      createSender("ln3", 2, false, 10, 100, false, false, null, true);
    });

    vm4.invoke(() -> createReplicatedRegion(getTestMethodName() + "_RR", "ln1,ln2", isOffHeap()));

    vm5.invoke(() -> {
      assertThatThrownBy(
          () -> createReplicatedRegion(getTestMethodName() + "_RR", "ln2,ln3", isOffHeap()))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("Cannot create Region");
    });
  }

  @Test
  public void replicateRegionCreationShouldFailWhenAsyncEventQueueIdsAreDifferentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> createReplicatedRegionWithAsyncEventQueue(getTestMethodName() + "_RR", "ln1",
        isOffHeap()));
    vm5.invoke(() -> {
      assertThatThrownBy(() -> createReplicatedRegionWithAsyncEventQueue(
          getTestMethodName() + "_RR", "ln2", isOffHeap()))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("Cannot create Region");
    });
  }

  /**
   * Test to make sure that serial sender Ids configured in partitioned regions should be same
   * across all PR members
   */
  @Test
  public void partitionRegionCreationShouldFailWhenSerialGatewaySenderIdsAreDifferentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> {
      createSender("ln1", 2, false, 10, 100, false, false, null, true);
      createSender("ln2", 2, false, 10, 100, false, false, null, true);
    });

    vm5.invoke(() -> {
      createSender("ln2", 2, false, 10, 100, false, false, null, true);
      createSender("ln3", 2, false, 10, 100, false, false, null, true);
    });

    vm4.invoke(
        () -> createPartitionedRegion(getTestMethodName() + "_PR", "ln1,ln2", 1, 100, isOffHeap()));

    vm5.invoke(() -> {
      assertThatThrownBy(() -> createPartitionedRegion(getTestMethodName() + "_PR", "ln2,ln3", 1,
          100, isOffHeap()))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("Cannot create Region");
    });
  }

  /**
   * Test to make sure that parallel sender Ids configured in partitioned regions should be same
   * across all PR members
   */
  @Test
  public void partitionRegionCreationShouldFailWhenParallelGatewaySenderIdsAreDifferentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> {
      createSender("ln1", 2, true, 10, 100, false, false, null, true);
      createSender("ln2", 2, true, 10, 100, false, false, null, true);
    });

    vm5.invoke(() -> {
      createSender("ln2", 2, true, 10, 100, false, false, null, true);
      createSender("ln3", 2, true, 10, 100, false, false, null, true);
    });

    vm4.invoke(
        () -> createPartitionedRegion(getTestMethodName() + "_PR", "ln1,ln2", 1, 100, isOffHeap()));
    vm5.invoke(() -> {
      assertThatThrownBy(() -> createPartitionedRegion(getTestMethodName() + "_PR", "ln2,ln3", 1,
          100, isOffHeap()))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("Cannot create Region");
    });
  }

  /**
   * Test to make sure that same parallel gateway sender id can not be used by 2 non co-located PRs
   */
  @Test
  public void sameParallelGatewaySenderCanNotBeAttachedToDifferentPartitionRegionsWhenTheyAreNotCoLocated() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1);

    vm1.invoke(() -> {
      createSender("ln1_Parallel", 2, true, 10, 100, false, false, null, true);
      createSender("ln2_Parallel", 2, true, 10, 100, false, false, null, true);
      createPartitionedRegionWithSerialParallelSenderIds(getTestMethodName() + "_PR1", null,
          "ln1_Parallel,ln2_Parallel", null, isOffHeap());
      assertThatThrownBy(() -> createPartitionedRegionWithSerialParallelSenderIds(
          getTestMethodName() + "_PR2", null, "ln1_Parallel,ln2_Parallel", null, isOffHeap()))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("cannot have the same parallel gateway sender id");
    });
  }

  @Test
  public void sameParallelGatewaySenderCanBeAttachedToDifferentPartitionRegionsWhenTheyAreCoLocated() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1);

    vm1.invoke(() -> {
      createSender("ln1_Parallel", 2, true, 10, 100, false, false, null, true);
      createSender("ln2_Parallel", 2, true, 10, 100, false, false, null, true);
      createPartitionedRegionWithSerialParallelSenderIds(getTestMethodName() + "_PR1", null,
          "ln1_Parallel", null, isOffHeap());
      createPartitionedRegionWithSerialParallelSenderIds(getTestMethodName() + "_PR2", null,
          "ln1_Parallel,ln2_Parallel", getTestMethodName() + "_PR1", isOffHeap());
    });
  }

  /**
   * Validate that if Colocated partitioned region doesn't want to add a PGS even if its parent has
   * one then it is fine
   */
  @Test
  public void coLocatedPartitionRegionDoesNotNeedToHaveParallelGatewaySenderUsedByRootPartitionRegion() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1);

    vm1.invoke(() -> {
      createSender("ln1_Parallel", 2, true, 10, 100, false, false, null, true);
      createSender("ln2_Parallel", 2, true, 10, 100, false, false, null, true);
      createPartitionedRegionWithSerialParallelSenderIds(getTestMethodName() + "_PR1", null,
          "ln1_Parallel", null, isOffHeap());
      createPartitionedRegionWithSerialParallelSenderIds(getTestMethodName() + "_PR2", null, null,
          getTestMethodName() + "_PR1", isOffHeap());
    });
  }

  /**
   * Validate that if Colocated partitioned region has a subset of PGS then it is fine.
   */
  @Test
  public void coLocatedPartitionRegionCanHaveSubSetOfParallelGatewaySendersUsedByRootPartitionRegion() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1);

    vm1.invoke(() -> {
      createSender("ln1_Parallel", 2, true, 10, 100, false, false, null, true);
      createSender("ln2_Parallel", 2, true, 10, 100, false, false, null, true);
      createPartitionedRegionWithSerialParallelSenderIds(getTestMethodName() + "_PR1", null,
          "ln1_Parallel,ln2_Parallel", null, isOffHeap());
      createPartitionedRegionWithSerialParallelSenderIds(getTestMethodName() + "_PR2", null,
          "ln1_Parallel", getTestMethodName() + "_PR1", isOffHeap());
    });
  }

  /**
   * Validate that if Colocated partitioned region has a superset of PGS then Exception is thrown.
   */
  @Test
  public void coLocatedPartitionRegionCanHaveSuperSetOfParallelGatewaySendersUsedByRootPartitionRegion() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1);

    vm1.invoke(() -> {
      createSender("ln1_Parallel", 2, true, 10, 100, false, false, null, true);
      createSender("ln2_Parallel", 2, true, 10, 100, false, false, null, true);
      createSender("ln3_Parallel", 2, true, 10, 100, false, false, null, true);
      createPartitionedRegionWithSerialParallelSenderIds(getTestMethodName() + "_PR1", null,
          "ln1_Parallel,ln2_Parallel", null, isOffHeap());
      createPartitionedRegionWithSerialParallelSenderIds(getTestMethodName() + "_PR2", null,
          "ln1_Parallel,ln2_Parallel,ln3_Parallel", getTestMethodName() + "_PR1", isOffHeap());
    });
  }

  /**
   * SerialGatewaySender and ParallelGatewaySender with same name is not allowed
   */
  @Test
  public void gatewaySenderIdShouldBeUnique() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1);

    vm1.invoke(() -> {
      createSenderForValidations("ln", 2, false, 100, false, false, null, null, true, false);
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, true, 100, false, false, null,
          null, true, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining("is already defined in this cache");
    });
  }

  // sender with same name should be either serial or parallel but not both.
  @Test
  public void canNotCreateParallelGatewaySenderIfAnotherSerialGatewaySenderHasBeenCreatedWithSameId() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    vm1.invoke(() -> createSenderForValidations("ln", 2, false, 100, false, false, null, null, true,
        false));
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, true, 100, false, false, null,
          null, true, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same sender as serial gateway sender");
    });
  }

  // sender with same name should be either serial or parallel but not both.
  @Test
  public void canNotCreateSerialGatewaySenderIfAnotherParallelGatewaySenderHasBeenCreatedWithSameId() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    vm1.invoke(() -> createSenderForValidations("ln", 2, true, 100, false, false, null, null, true,
        false));

    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, false, 100, false, false, null,
          null, true, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same sender as parallel gateway sender");
    });
  }

  /**
   * A single VM hosts a cache server as well as a Receiver.
   * Expected: Cache.getCacheServer should return only the cache server and not the Receiver.
   */
  @Test
  public void getCacheServersShouldNotReturnGatewayReceivers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm4);

    vm4.invoke(() -> {
      createReceiver();
      createCacheServer();
    });

    @SuppressWarnings("rawtypes")
    Map cacheServers = vm4.invoke(WANTestBase::getCacheServers);
    assertThat(cacheServers.get("BridgeServer"))
        .as("Cache.getCacheServers returned incorrect BridgeServers")
        .isEqualTo(1);
    assertThat(cacheServers.get("ReceiverServer"))
        .as("Cache.getCacheServers returned incorrect ReceiverServers")
        .isEqualTo(0);
  }

  /**
   * Two VMs are part of the DS. One VM hosts a cache server while the other hosts a Receiver.
   * Expected: Cache.getCacheServers should only return the bridge server and not the Receiver.
   */
  @Test
  public void getCacheServersShouldNotReturnGatewayReceivers_Scenario2() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm4);
    vm4.invoke(WANTestBase::createReceiver);

    createCacheInVMs(lnPort, vm5);
    vm5.invoke(WANTestBase::createCacheServer);

    @SuppressWarnings("rawtypes")
    Map cacheServers_vm4 = vm4.invoke(WANTestBase::getCacheServers);
    assertThat(cacheServers_vm4.get("BridgeServer"))
        .as("Cache.getCacheServers on vm4 returned incorrect BridgeServers")
        .isEqualTo(0);
    assertThat(cacheServers_vm4.get("ReceiverServer"))
        .as("Cache.getCacheServers on vm4 returned incorrect ReceiverServers")
        .isEqualTo(0);

    @SuppressWarnings("rawtypes")
    Map cacheServers_vm5 = vm5.invoke(WANTestBase::getCacheServers);
    assertThat(cacheServers_vm5.get("BridgeServer"))
        .as("Cache.getCacheServers on vm4 returned incorrect BridgeServers")
        .isEqualTo(1);
    assertThat(cacheServers_vm5.get("ReceiverServer"))
        .as("Cache.getCacheServers on vm4 returned incorrect ReceiverServers")
        .isEqualTo(0);
  }

  // isBatchConflation should be same across the same sender
  @Test
  public void batchConflationShouldBeConsistentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    vm1.invoke(() -> createSenderForValidations("ln", 2, false, 100, false, false, null, null, true,
        false));
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, false, 100, true, false, null,
          null, true, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "another cache has the same Gateway Sender defined with isBatchConflationEnabled");
    });
  }

  // remote ds ids should be same
  @Test
  public void distributedSystemIdShouldBeConsistentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    vm1.invoke(() -> createSenderForValidations("ln", 2, false, 100, false, false, null, null, true,
        false));
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 3, false, 100, false, false, null,
          null, true, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same Gateway Sender defined with remote ds id");
    });
  }

  // isPersistentEnabled should be same across the same sender
  @Test
  public void isPersistentShouldBeConsistentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    vm1.invoke(() -> createSenderForValidations("ln", 2, false, 100, false, false, null, null, true,
        false));
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, false, 100, false, true, null,
          null, true, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same Gateway Sender defined with isPersistentEnabled");
    });
  }

  @Test
  public void alertThresholdShouldBeConsistentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    vm1.invoke(() -> createSenderForValidations("ln", 2, false, 100, false, false, null, null, true,
        false));
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, false, 50, false, false, null,
          null, true, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same Gateway Sender defined with alertThreshold");
    });
  }

  @Test
  public void manualStartShouldBeConsistentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    vm1.invoke(() -> createSenderForValidations("ln", 2, false, 100, false, false, null, null, true,
        false));
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, false, 100, false, false, null,
          null, false, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same Gateway Sender defined with manual start");
    });
  }

  @Test
  public void gatewayEventFiltersShouldBeConsistentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    List<GatewayEventFilter> eventFilters = new ArrayList<>();
    eventFilters.add(new MyGatewayEventFilter());
    vm1.invoke(() -> createSenderForValidations("ln", 2, false, 100, false, false, eventFilters,
        null, true, false));

    eventFilters.clear();
    eventFilters.add(new Filter70());
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, false, 100, false, false,
          eventFilters, null, true, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same Gateway Sender defined with GatewayEventFilters");

    });
  }

  @Test
  public void gatewayEventFiltersShouldBeConsistentAcrossMembers_Scenario2() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    ArrayList<GatewayEventFilter> eventFilters = new ArrayList<>();
    eventFilters.add(new MyGatewayEventFilter());
    vm1.invoke(() -> createSenderForValidations("ln", 2, false, 100, false, false, eventFilters,
        null, true, false));

    eventFilters.clear();
    eventFilters.add(new MyGatewayEventFilter());
    eventFilters.add(new Filter70());
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, false, 100, false, false,
          eventFilters, null, true, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same Gateway Sender defined with GatewayEventFilters");
    });
  }

  @Test
  public void gatewayTransportFiltersShouldBeConsistentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    ArrayList<GatewayTransportFilter> transportFilters = new ArrayList<>();
    transportFilters.add(new MyGatewayTransportFilter1());
    transportFilters.add(new MyGatewayTransportFilter2());
    vm1.invoke(() -> createSenderForValidations("ln", 2, false, 100, false, false, null,
        transportFilters, true, false));

    transportFilters.clear();
    transportFilters.add(new MyGatewayTransportFilter3());
    transportFilters.add(new MyGatewayTransportFilter4());
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, false, 100, false, false, null,
          transportFilters, true, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same Gateway Sender defined with GatewayTransportFilters");
    });
  }

  @Test
  public void gatewayTransportFilterOrderShouldBeConsistentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    ArrayList<GatewayTransportFilter> transportFilters = new ArrayList<>();
    transportFilters.add(new MyGatewayTransportFilter1());
    transportFilters.add(new MyGatewayTransportFilter2());
    vm1.invoke(() -> createSenderForValidations("ln", 2, false, 100, false, false, null,
        transportFilters, true, false));

    transportFilters.clear();
    transportFilters.add(new MyGatewayTransportFilter2());
    transportFilters.add(new MyGatewayTransportFilter1());
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, false, 100, false, false, null,
          transportFilters, true, false))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same Gateway Sender defined with GatewayTransportFilters");
    });
  }

  @Test
  public void diskSynchronizationPolicyShouldBeConsistentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    vm1.invoke(() -> createSenderForValidations("ln", 2, false, 100, false, false, null, null, true,
        false));
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createSenderForValidations("ln", 2, false, 100, false, false, null,
          null, true, true))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same Gateway Sender defined with isDiskSynchronous");
    });
  }

  /*
   * Dispatcher threads are same across all the nodes for ParallelGatewaySender.
   * Ignored because we are now allowing number of dispatcher threads for parallel sender to differ
   * on different members.
   */
  @Test
  @Ignore
  public void dispatcherThreadsForParallelGatewaySenderShouldBeConsistentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    vm1.invoke(() -> createConcurrentSender("ln", 2, true, 100, 10, false, false, null, true, 5,
        OrderPolicy.KEY));
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
          true, 4, OrderPolicy.KEY))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same Gateway Sender defined with dispatcherThread");
    });
  }

  /*
   * For Parallel sender, thread policy is not supported which is checked at the time of sender
   * creation. policy KEY and Partition are same for PGS. Hence disabling the tests
   */
  @Test
  @Ignore
  public void orderPolicyForParallelGatewaySenderShouldBeConsistentAcrossMembers() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm1, vm2);

    vm1.invoke(() -> createConcurrentSender("ln", 2, true, 100, 10, false, false, null, true, 5,
        OrderPolicy.KEY));
    vm2.invoke(() -> {
      assertThatThrownBy(() -> createConcurrentSender("ln", 2, true, 100, 10, false, false, null,
          true, 5, OrderPolicy.PARTITION))
              .isInstanceOf(IllegalStateException.class)
              .hasMessageContaining(
                  "because another cache has the same Gateway Sender defined with orderPolicy");
    });
  }

  @Test
  public void warningShouldBeLoggedWhenSerialGatewaySenderIsNotConfiguredAcrossAllMembersHostingTheReplicateRegionTroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> {
      createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap());
      createReceiver();
    });

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    vm4.invoke(() -> {
      createSender("ln", 2, false, 100, 10, false, false, null, true);
      startSender("ln");
    });

    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm
        .invoke(() -> createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap())));

    String regionName = getTestMethodName() + "_RR";
    vm4.invoke(() -> addSenderThroughAttributesMutator(regionName, "ln"));
    vm5.invoke(() -> addSenderThroughAttributesMutator(regionName, "ln"));

    verifyNoGatewaySenderIdWarning(regionName);
    vm4.invoke(() -> doPuts(regionName, 10));
    verifyGatewaySenderIdWarning(regionName);

    // now add the sender on the other vms so they will be consistent
    asList(vm6, vm7)
        .forEach(vm -> vm.invoke(() -> addSenderThroughAttributesMutator(regionName, "ln")));
    vm4.invoke(() -> doPuts(regionName, 10));
    verifyNoGatewaySenderIdWarning(regionName);
  }

  @Test
  public void warningShouldBeLoggedWhenSerialAsyncEventQueueIsNotConfiguredAcrossAllMembersHostingTheReplicateRegionTroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createAsyncEventQueue("ln", false, 100, 100, false, false, null, false);
      createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap());
    }));

    String regionName = getTestMethodName() + "_RR";
    asList(vm4, vm5).forEach(vm -> vm.invoke(() -> {
      addAsyncEventQueueThroughAttributesMutator(regionName, "ln");
    }));

    verifyNoAsyncEventQueueIdWarning(regionName);
    vm4.invoke(() -> doPuts(regionName, 1000));
    verifyAsyncEventQueueIdWarning(regionName);

    asList(vm6, vm7).forEach(vm -> vm.invoke(() -> {
      addAsyncEventQueueThroughAttributesMutator(regionName, "ln");
    }));

    vm4.invoke(() -> doPuts(regionName, 1000));
    verifyNoAsyncEventQueueIdWarning(regionName);
  }

  @Test
  public void warningShouldBeLoggedWhenSerialGatewaySenderIsNotConfiguredAcrossAllMembersHostingThePartitionRegionTroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> {
      createReceiver();
      createPartitionedRegion(getTestMethodName() + "_RR", null, 1, 100, isOffHeap());
    });

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createSender("ln", 2, false, 100, 10, false, false, null, true);
    }));
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    String regionName = getTestMethodName() + "_PR";
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createPartitionedRegion(regionName, null, 3, 100, isOffHeap());
    }));
    asList(vm4, vm5).forEach(vm -> vm.invoke(() -> {
      addSenderThroughAttributesMutator(regionName, "ln");
    }));
    vm4.invoke(() -> waitForSenderRunningState("ln"));

    verifyNoGatewaySenderIdWarning(regionName);
    vm4.invoke(() -> doPuts(regionName, 10));
    verifyGatewaySenderIdWarning(regionName);

    // now add the sender on the other vms so they will be consistent
    asList(vm6, vm7).forEach(vm -> vm.invoke(() -> {
      addSenderThroughAttributesMutator(regionName, "ln");
    }));
    vm4.invoke(() -> doPuts(regionName, 10));
    verifyNoGatewaySenderIdWarning(regionName);
  }

  @Test
  public void warningShouldBeLoggedWhenSerialAsyncEventQueueIsNotConfiguredAcrossAllMembersHostingThePartitionRegionTroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createAsyncEventQueue("ln", false, 100, 100, false, false, null, false);
    }));

    String regionName = getTestMethodName() + "_PR";
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createPartitionedRegion(regionName, null, 3, 100, isOffHeap());
    }));
    asList(vm4, vm5).forEach(vm -> vm.invoke(() -> {
      addAsyncEventQueueThroughAttributesMutator(regionName, "ln");
    }));

    verifyNoAsyncEventQueueIdWarning(regionName);
    vm4.invoke(() -> doPuts(regionName, 1000));
    verifyAsyncEventQueueIdWarning(regionName);

    asList(vm6, vm7).forEach(vm -> vm.invoke(() -> {
      addAsyncEventQueueThroughAttributesMutator(regionName, "ln");
    }));
    vm4.invoke(() -> doPuts(regionName, 1000));
    verifyNoAsyncEventQueueIdWarning(regionName);
  }

  @Test
  public void warningShouldBeLoggedWhenParallelGatewaySenderIsNotConfiguredAcrossAllMembersHostingThePartitionRegionTroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> {
      createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 10, isOffHeap());
      createReceiver();
    });

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    asList(vm4, vm5).forEach(vm -> vm.invoke(() -> {
      createSender("ln", 2, true, 100, 10, false, false, null, true);
    }));
    startSenderInVMs("ln", vm4, vm5);

    String regionName = getTestMethodName() + "_PR";
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createPartitionedRegion(regionName, null, 3, 10, isOffHeap());
    }));
    asList(vm4, vm5).forEach(vm -> vm.invoke(() -> {
      addSenderThroughAttributesMutator(regionName, "ln");
    }));

    vm4.invoke(() -> waitForSenderRunningState("ln"));
    verifyNoGatewaySenderIdWarning(regionName);
    vm4.invoke(() -> doPuts(regionName, 10));
    verifyGatewaySenderIdWarning(regionName);

    // now add the sender on the other vms so they will be consistent
    asList(vm6, vm7).forEach(vm -> vm.invoke(() -> {
      addSenderThroughAttributesMutator(regionName, "ln");
    }));

    vm4.invoke(() -> doPuts(regionName, 10));
    verifyNoGatewaySenderIdWarning(regionName);
  }

  @Test
  public void warningShouldBeLoggedWhenParallelAsyncEventQueueIsNotConfiguredAcrossAllMembersHostingThePartitionRegionTroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    String regionName = getTestMethodName() + "_PR";
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createAsyncEventQueue("ln", true, 100, 100, false, false, null, false);
      createPartitionedRegion(regionName, null, 3, 10, isOffHeap());
    }));

    asList(vm4, vm5).forEach(vm -> vm.invoke(() -> {
      addAsyncEventQueueThroughAttributesMutator(regionName, "ln");
    }));

    verifyNoAsyncEventQueueIdWarning(regionName);
    vm4.invoke(() -> doPuts(regionName, 10));
    verifyAsyncEventQueueIdWarning(regionName);
  }

  @Test
  public void serialGatewaySenderShouldBeSuccessfullyAttachedToReplicateRegionTroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> {
      createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap());
      createReceiver();
    });

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    vm4.invoke(() -> {
      createSender("ln", 2, false, 100, 10, false, false, null, true);
      startSender("ln");
    });

    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap());
    }));

    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      addSenderThroughAttributesMutator(getTestMethodName() + "_RR", "ln");
    }));

    vm4.invoke(() -> {
      waitForSenderRunningState("ln");
      doPuts(getTestMethodName() + "_RR", 10);
    });

    vm2.invoke(() -> validateRegionSize(getTestMethodName() + "_RR", 10));
  }

  @Test
  public void serialAsyncEventQueueShouldBeSuccessfullyAttachedToReplicateRegionTroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createAsyncEventQueue("ln", false, 100, 100, false, false, null, false);
      createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap());
    }));

    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      addAsyncEventQueueThroughAttributesMutator(getTestMethodName() + "_RR", "ln");
    }));

    vm4.invoke(() -> doPuts(getTestMethodName() + "_RR", 1000));
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      addAsyncEventQueueThroughAttributesMutator(getTestMethodName() + "_RR", "ln");
    }));

    // primary sender
    vm4.invoke(() -> validateAsyncEventListener("ln", 1000));

    // secondary senders
    asList(vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      validateAsyncEventListener("ln", 0);
    }));
  }

  @Test
  public void serialGatewaySenderShouldBeSuccessfullyAttachedToPartitionRegionTroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> {
      createReceiver();
      createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100, isOffHeap());
    });

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createSender("ln", 2, false, 100, 10, false, false, null, true);
    }));
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100, isOffHeap());
    }));
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      addSenderThroughAttributesMutator(getTestMethodName() + "_PR", "ln");
    }));

    vm4.invoke(() -> {
      waitForSenderRunningState("ln");
      doPuts(getTestMethodName() + "_PR", 10);
      validateQueueContents("ln", 0);
    });

    vm2.invoke(() -> validateRegionSize(getTestMethodName() + "_PR", 10));
  }

  @Test
  public void serialAsyncEventQueueShouldBeSuccessfullyAttachedToPartitionRegionTroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createAsyncEventQueue("ln", false, 100, 100, false, false, null, false);
      createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 100, isOffHeap());
    }));
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      addAsyncEventQueueThroughAttributesMutator(getTestMethodName() + "_PR", "ln");
    }));

    vm4.invoke(() -> doPuts(getTestMethodName() + "_PR", 1000));

    // primary sender
    vm4.invoke(() -> validateAsyncEventListener("ln", 1000));

    // secondary senders
    asList(vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      validateAsyncEventListener("ln", 0);
    }));
  }

  @Test
  public void parallelAsyncEventQueueShouldBeSuccessfullyAttachedToPartitionRegionTroughAttributesMutator() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createAsyncEventQueue("ln", true, 100, 100, false, false, null, false);
      createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 10, isOffHeap());
    }));
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      addAsyncEventQueueThroughAttributesMutator(getTestMethodName() + "_PR", "ln");
    }));

    vm4.invoke(() -> doPuts(getTestMethodName() + "_PR", 256));
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      waitForAsyncQueueToGetEmpty("ln");
    }));

    int vm4size = vm4.invoke(() -> getAsyncEventListenerMapSize("ln"));
    int vm5size = vm5.invoke(() -> getAsyncEventListenerMapSize("ln"));
    int vm6size = vm6.invoke(() -> getAsyncEventListenerMapSize("ln"));
    int vm7size = vm7.invoke(() -> getAsyncEventListenerMapSize("ln"));

    assertThat(vm4size + vm5size + vm6size + vm7size).isEqualTo(256);
  }

  @Test
  public void parallelGatewaySenderAssociationToReplicateRegionTroughAttributesMutatorShouldFail() {
    IgnoredException.addIgnoredException("could not get remote locator information");
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));

    createCacheInVMs(lnPort, vm1);
    String regionName = getTestMethodName() + "_RR";
    String gatewaySenderId = getTestMethodName() + "_gateway";
    vm1.invoke(() -> {
      createReplicatedRegion(regionName, null, isOffHeap());
      createSender(gatewaySenderId, 2, true, 100, 10, false, false, null, false);

      assertThatThrownBy(() -> addSenderThroughAttributesMutator(regionName, gatewaySenderId))
          .isInstanceOf(GatewaySenderConfigurationException.class)
          .hasMessage("Parallel Gateway Sender " + gatewaySenderId
              + " can not be used with replicated region " + SEPARATOR + regionName);
    });
  }

  @Test
  public void parallelAsyncEventQueueAssociationToReplicateRegionTroughAttributesMutatorShouldFail() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));

    createCacheInVMs(lnPort, vm1);
    String regionName = getTestMethodName() + "_RR";
    String asyncEventQueueId = getTestMethodName() + "_asyncEventQueue";
    vm1.invoke(() -> {
      createReplicatedRegion(regionName, null, isOffHeap());
      createAsyncEventQueue(asyncEventQueueId, true, 100, 100, false, false, null, false);

      assertThatThrownBy(
          () -> addAsyncEventQueueThroughAttributesMutator(regionName, asyncEventQueueId))
              .isInstanceOf(AsyncEventQueueConfigurationException.class)
              .hasMessage("Parallel Async Event Queue " + asyncEventQueueId
                  + " can not be used with replicated region " + SEPARATOR + regionName);
    });
  }

  @Test
  public void whenSendersAreAddedUsingAttributesMutatorThenEventsMustBeSuccessfullyReceivedByRemoteSite() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> {
      createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 10, isOffHeap());
      createReceiver();
    });

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createSender("ln", 2, true, 100, 10, false, false, null, true);
      createPartitionedRegion(getTestMethodName() + "_PR", null, 1, 10, isOffHeap());
    }));
    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      addSenderThroughAttributesMutator(getTestMethodName() + "_PR", "ln");
    }));

    vm4.invoke(() -> doPuts(getTestMethodName() + "_PR", 10));
    vm2.invoke(() -> validateRegionSize(getTestMethodName() + "_PR", 10));
  }

  @Test
  public void gatewayReceiverCreationFailsAndDoesNotHangWhenConfiguredBindAddressIsNotUsable() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));

    vm2.invoke(() -> {
      Properties props = getDistributedSystemProperties();
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, "localhost[" + lnPort + "]");
      cache = new CacheFactory(props).create();

      GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
      int port = AvailablePortHelper.getRandomAvailableTCPPort();
      fact.setStartPort(port);
      fact.setEndPort(port);
      fact.setManualStart(true);
      fact.setBindAddress("200.112.204.10");
      GatewayReceiver receiver = fact.create();
      assertThatThrownBy(receiver::start)
          .isInstanceOf(GatewayReceiverException.class)
          .hasMessageContaining("No available free port found in the given range");
    });
  }

  @Test
  public void testBug50247_NonPersistentSenderWithPersistentRegion() {
    IgnoredException.addIgnoredException("could not get remote locator information");
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> createSender("ln1", 2, true, 10, 100, false, false, null, false));
    assertThatThrownBy(() -> vm4.invoke(
        () -> createPartitionedRegionWithPersistence(getTestMethodName() + "_PR", "ln1", 1, 100)))
            .withFailMessage(
                "Expected GatewaySenderException with incompatible gateway sender ids and region")
            .hasRootCauseInstanceOf(GatewaySenderException.class)
            .hasStackTraceContaining("can not be attached to persistent region ");

    vm5.invoke(
        () -> createPartitionedRegionWithPersistence(getTestMethodName() + "_PR", "ln1", 1, 100));
    assertThatThrownBy(
        () -> vm5.invoke(() -> createSender("ln1", 2, true, 10, 100, false, false, null, false)))
            .withFailMessage(
                "Expected GatewaySenderException with incompatible gateway sender ids and region")
            .hasRootCauseInstanceOf(GatewaySenderException.class)
            .hasStackTraceContaining("can not be attached to persistent region ");
  }

  /**
   * Test configuration:
   * Region: Replicated
   * WAN: Serial
   * Number of WAN sites: 2
   * Region persistence enabled: false
   * Async channel persistence enabled: false
   */
  @Test
  public void testReplicatedSerialAsyncEventQueueWith2WANSites() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    // ------------ START - CREATE CACHE, REGION ON LOCAL SITE ------------//
    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);
    asList(vm4, vm5).forEach(vm -> vm.invoke(() -> {
      createSender("ln", 2, false, 100, 10, false, false, null, true);
    }));
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createAsyncEventQueue("lnAsync", false, 100, 100, false, false, null, false);
    }));
    startSenderInVMs("ln", vm4, vm5);
    asList(vm4, vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      createReplicatedRegionWithSenderAndAsyncEventQueue(getTestMethodName() + "_RR", "ln",
          "lnAsync", isOffHeap());
    }));
    // ------------- END - CREATE CACHE, REGION ON LOCAL SITE -------------//

    // ------------- START - CREATE CACHE ON REMOTE SITE ---------------//
    createCacheInVMs(nyPort, vm2, vm3);
    createReceiverInVMs(vm2, vm3);

    asList(vm2, vm3).forEach(vm -> vm.invoke(() -> {
      createSender("ny", 1, false, 100, 10, false, false, null, true);
      createAsyncEventQueue("nyAsync", false, 100, 100, false, false, null, false);
    }));
    startSenderInVMs("ny", vm2, vm3);
    asList(vm2, vm3).forEach(vm -> vm.invoke(() -> {
      createReplicatedRegionWithSenderAndAsyncEventQueue(getTestMethodName() + "_RR", "ny",
          "nyAsync", isOffHeap());
    }));
    // ------------- END - CREATE CACHE, REGION ON REMOTE SITE -------------//

    vm4.invoke(() -> doPuts(getTestMethodName() + "_RR", 1000));

    // validate AsyncEventListener on local site
    // primary sender
    vm4.invoke(() -> validateAsyncEventListener("lnAsync", 1000));

    // secondary senders
    asList(vm5, vm6, vm7).forEach(vm -> vm.invoke(() -> {
      validateAsyncEventListener("lnAsync", 0);
    }));

    // validate region size on remote site
    asList(vm2, vm3).forEach(vm -> vm.invoke(() -> {
      validateRegionSize(getTestMethodName() + "_RR", 1000);
    }));

    // validate AsyncEventListener on remote site
    vm2.invoke(() -> validateAsyncEventListener("nyAsync", 1000));// primary sender
    vm3.invoke(() -> validateAsyncEventListener("nyAsync", 0)); // secondary sender
  }
}
