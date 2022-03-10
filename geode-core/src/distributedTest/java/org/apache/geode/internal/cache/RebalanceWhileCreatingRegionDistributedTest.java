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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.RemoveBucketMessage;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedBlackboard;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({SecurityTest.class})
public class RebalanceWhileCreatingRegionDistributedTest implements Serializable {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedBlackboard blackboard = new DistributedBlackboard();

  private static final Logger logger = LogService.getLogger();

  @Test
  public void testRebalanceDuringRegionCreation() throws Exception {
    // Init Blackboard
    blackboard.initBlackboard();

    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start servers
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM accessor = cluster.startServerVM(4, locatorPort);

    // Add DistributionMessageObserver
    String regionName = testName.getMethodName();
    Stream.of(server1, server2, accessor)
        .forEach(server -> server.invoke(() -> addDistributionMessageObserver(regionName)));

    // Create regions in each server
    server1.invoke(() -> createRegion(regionName, RegionShortcut.PARTITION));
    server2.invoke(() -> createRegion(regionName, RegionShortcut.PARTITION));

    // Asynchronously wait to create the proxy region in the accessor
    accessor.invokeAsync(() -> waitToCreateProxyRegion(regionName));

    // Connect client1
    ClientVM client1 =
        cluster.startClientVM(5, c -> c.withServerConnection(server1.getPort(), server2.getPort()));

    // Do puts
    client1.invoke(() -> {
      Region<Integer, Integer> region =
          ClusterStartupRule.clientCacheRule.createProxyRegion(regionName);
      IntStream.range(0, 3).forEach(i -> region.put(i, i));
    });

    // Start server3
    MemberVM server3 = cluster.startServerVM(3, locatorPort);

    // Create region in server3
    server3.invoke(() -> createRegion(regionName, RegionShortcut.PARTITION));

    // Add DistributionMessageObserver to server3
    server3.invoke(() -> addDistributionMessageObserver(regionName));

    // Rebalance
    server1.invoke(() -> ClusterStartupRule.getCache().getResourceManager().createRebalanceFactory()
        .start().getResults());

    // Stop server3
    server3.invoke(() -> ClusterStartupRule.getCache().close());

    // Connect client to accessor
    ClientVM client2 =
        cluster.startClientVM(6, c -> c.withServerConnection(accessor.getPort())
            .withCacheSetup(cf -> cf.setPoolReadTimeout(20000)));

    // Do puts
    client2.invoke(() -> {
      Region<Integer, Integer> region =
          ClusterStartupRule.clientCacheRule.createProxyRegion(regionName);
      IntStream.range(0, 3).forEach(i -> region.put(i, i));
    });
  }

  @Test
  public void testMoveSingleBucketDuringRegionCreation() throws Exception {
    // Init Blackboard
    blackboard.initBlackboard();

    // Start Locator
    MemberVM locator = cluster.startLocatorVM(0);

    // Start servers
    int locatorPort = locator.getPort();
    MemberVM server1 = cluster.startServerVM(1, locatorPort);
    MemberVM server2 = cluster.startServerVM(2, locatorPort);
    MemberVM accessor = cluster.startServerVM(3, locatorPort);

    // Add DistributionMessageObserver
    String regionName = testName.getMethodName();
    Stream.of(server1, server2, accessor)
        .forEach(server -> server.invoke(() -> addDistributionMessageObserver(regionName)));

    // Create regions in each server
    InternalDistributedMember source = server1.invoke(() -> {
      createSingleBucketRegion(regionName, RegionShortcut.PARTITION);
      Region<Integer, Integer> region =
          ClusterStartupRule.getCache().getRegion(regionName);
      region.put(123, 123);
      PartitionedRegionDataStore partitionedRegionDataStore =
          ((PartitionedRegion) region).getDataStore();
      // Make sure server1 has the primary bucket
      assertThat(partitionedRegionDataStore).isNotNull();
      assertThat(partitionedRegionDataStore.getNumberOfPrimaryBucketsManaged()).isEqualTo(1);
      return InternalDistributedSystem.getAnyInstance().getDistributedMember();
    });

    InternalDistributedMember destination = server2.invoke(() -> {
      createSingleBucketRegion(regionName, RegionShortcut.PARTITION);
      Region<Integer, Integer> region =
          ClusterStartupRule.getCache().getRegion(regionName);
      PartitionedRegionDataStore partitionedRegionDataStore =
          ((PartitionedRegion) region).getDataStore();
      // Make sure server2 does not have primary bucket
      assertThat(partitionedRegionDataStore).isNotNull();
      assertThat(partitionedRegionDataStore.getNumberOfPrimaryBucketsManaged()).isEqualTo(0);
      return InternalDistributedSystem.getAnyInstance().getDistributedMember();
    });

    // Asynchronously wait to create the proxy region in the accessor
    AsyncInvocation asyncInvocation = accessor.invokeAsync(() -> {
      waitToCreateSingleBucketProxyRegion(regionName);
    });

    // Move the primary bucket from server1 to server2 and close the cache in the end
    server2.invoke(() -> {
      PartitionedRegion partitionedRegion =
          (PartitionedRegion) ClusterStartupRule.getCache().getRegion(regionName);
      PartitionedRegionDataStore partitionedRegionDataStore = partitionedRegion.getDataStore();
      partitionedRegionDataStore.moveBucket(0, source, true);
      ClusterStartupRule.getCache().close();
    });

    asyncInvocation.get();

    // Make sure the accessor knows that the primary bucket has moved to server2
    accessor.invoke(() -> {
      PartitionedRegion pr =
          (PartitionedRegion) ClusterStartupRule.getCache().getRegion(regionName);
      assertThat(pr.getRegionAdvisor().getBucket(0).getBucketAdvisor().getProfile(source))
          .isNull();
    });
  }

  private void createRegion(String regionName, RegionShortcut shortcut) {
    PartitionAttributesFactory<Integer, Integer> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(0);
    paf.setTotalNumBuckets(3);
    if (shortcut.isProxy()) {
      paf.setLocalMaxMemory(0);
    }

    RegionFactory<Integer, Integer> rf =
        ClusterStartupRule.getCache().createRegionFactory(shortcut);
    rf.setPartitionAttributes(paf.create());

    rf.create(regionName);
  }

  private void createSingleBucketRegion(String regionName, RegionShortcut shortcut) {
    PartitionAttributesFactory<Integer, Integer> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(0);
    paf.setTotalNumBuckets(1);
    if (shortcut.isProxy()) {
      paf.setLocalMaxMemory(0);
    }

    RegionFactory<Integer, Integer> rf =
        ClusterStartupRule.getCache().createRegionFactory(shortcut);
    rf.setPartitionAttributes(paf.create());

    rf.create(regionName);
  }

  private void waitToCreateProxyRegion(String regionName) throws Exception {
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion about to wait for Before_RemoveBucketMessage gate");
    blackboard.waitForGate("Before_RemoveBucketMessage");
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion done wait for Before_RemoveBucketMessage gate");
    createRegion(regionName, RegionShortcut.PARTITION_PROXY);
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion about to signal After_CreateProxyRegion gate");
    blackboard.signalGate("After_CreateProxyRegion");
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion done signal After_CreateProxyRegion gate");
  }

  private void waitToCreateSingleBucketProxyRegion(String regionName) throws Exception {
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion about to wait for Before_RemoveBucketMessage gate");
    blackboard.waitForGate("Before_RemoveBucketMessage");
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion done wait for Before_RemoveBucketMessage gate");
    createSingleBucketRegion(regionName, RegionShortcut.PARTITION_PROXY);
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion about to signal After_CreateProxyRegion gate");
    blackboard.signalGate("After_CreateProxyRegion");
    logger.info(
        "RebalanceWhileCreatingRegionDistributedTest.waitToCreateRegion done signal After_CreateProxyRegion gate");
  }

  private void addDistributionMessageObserver(String regionName) {
    DistributionMessageObserver.setInstance(new TestDistributionMessageObserver(regionName));
  }

  class TestDistributionMessageObserver extends DistributionMessageObserver {

    private final String regionName;

    public TestDistributionMessageObserver(String regionName) {
      this.regionName = regionName;
    }

    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof RemoveBucketMessage) {
        logger.info(
            "TestDistributionMessageObserver.beforeProcessMessage about to signal Before_RemoveBucketMessage gate");
        blackboard.signalGate("Before_RemoveBucketMessage");
        logger.info(
            "TestDistributionMessageObserver.beforeProcessMessage done signal Before_RemoveBucketMessage gate");
      }
    }

    public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof DestroyRegionOperation.DestroyRegionMessage) {
        DestroyRegionOperation.DestroyRegionMessage drm =
            (DestroyRegionOperation.DestroyRegionMessage) message;
        if (drm.regionPath.contains(regionName)) {
          logger.info(
              "TestDistributionMessageObserver.beforeSendMessage about to wait for After_CreateProxyRegion gate regionName={}",
              drm.regionPath);
          try {
            blackboard.waitForGate("After_CreateProxyRegion");
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          logger.info(
              "TestDistributionMessageObserver.beforeSendMessage done wait for After_CreateProxyRegion gate regionName={}",
              drm.regionPath);
        }
      }
    }
  }
}
