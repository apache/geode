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
package org.apache.geode.internal.cache.partitioned;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.DistributedTestUtils.crashDistributedSystem;
import static org.apache.geode.test.dunit.DistributedTestUtils.getAllDistributedSystemProperties;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Verifies that bucket creation completes even after requester crashes.
 *
 * <p>
 * TRAC #39356: Missing PR buckets with HA
 */
@Category(RegionsTest.class)
@SuppressWarnings("serial")
public class BucketCreationCrashCompletesRegressionTest implements Serializable {

  private String regionName;

  private VM vm0;
  private VM vm1;
  private VM vm2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);

    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();

    vm0.invoke(() -> createCache(getDistributedSystemProperties()));
    vm1.invoke(() -> createCache(getDistributedSystemProperties()));
    vm2.invoke(() -> createCache(getDistributedSystemProperties()));
  }

  /**
   * This tests the case where the VM forcing other VMs to create a bucket crashes while creating
   * the bucket.
   */
  @Test
  public void testCrashWhileCreatingABucket() {
    vm1.invoke(() -> createPartitionedRegionWithObserver());
    vm2.invoke(() -> createPartitionedRegionWithObserver());

    vm0.invoke(() -> createAccessorAndCrash());

    vm1.invoke(() -> verifyBucketsAfterAccessorCrashes());
    vm2.invoke(() -> verifyBucketsAfterAccessorCrashes());
  }

  /**
   * A test to make sure that we cannot move a bucket to a member which already hosts the bucket,
   * thereby reducing our redundancy.
   */
  @Test
  public void testMoveBucketToHostThatHasTheBucketAlready() {
    vm0.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createPartitionedRegion());

    // Create a bucket
    vm0.invoke(() -> {
      createBucket();
    });

    InternalDistributedMember member1 = vm1.invoke(() -> getCache().getMyId());

    // Move the bucket
    vm0.invoke(() -> {
      verifyCannotMoveBucketToExistingHost(member1);
    });
  }

  private void createPartitionedRegionWithObserver() {
    DistributionMessageObserver.setInstance(new MyRegionObserver());

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(1);
    partitionAttributesFactory.setRecoveryDelay(0);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  private void createAccessorAndCrash() {
    PartitionAttributesFactory<String, String> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setRedundantCopies(1);
    partitionAttributesFactory.setLocalMaxMemory(0);

    RegionFactory<String, String> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    Region<String, String> region = regionFactory.create(regionName);

    // trigger the creation of a bucket, which should trigger the destruction of this VM.
    assertThatThrownBy(() -> region.put("ping", "pong")).isInstanceOf(CancelException.class);
  }

  private boolean hasBucketOwners(PartitionedRegion partitionedRegion, int bucketId) {
    try {
      return partitionedRegion.getBucketOwnersForValidation(bucketId) != null;
    } catch (ForceReattemptException e) {
      return false;
    }
  }

  private void verifyBucketsAfterAccessorCrashes() throws ForceReattemptException {
    PartitionedRegion partitionedRegion = (PartitionedRegion) getCache().getRegion(regionName);
    for (int i = 0; i < partitionedRegion.getAttributes().getPartitionAttributes()
        .getTotalNumBuckets(); i++) {
      int bucketId = i;

      await().until(() -> hasBucketOwners(partitionedRegion, bucketId));

      List owners = partitionedRegion.getBucketOwnersForValidation(bucketId);
      assertThat(owners).isNotNull();
      if (owners.isEmpty()) {
        continue;
      }
      assertThat(owners).hasSize(2);
    }
  }

  private void createPartitionedRegion() {
    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(1);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setStartupRecoveryDelay(-1);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  private void createBucket() {
    Region<Integer, String> region = getCache().getRegion(regionName);
    region.put(0, "A");
  }

  private void verifyCannotMoveBucketToExistingHost(InternalDistributedMember member1) {
    PartitionedRegion partitionedRegion = (PartitionedRegion) getCache().getRegion(regionName);
    Set<InternalDistributedMember> bucketOwners =
        partitionedRegion.getRegionAdvisor().getBucketOwners(0);

    assertThat(bucketOwners).hasSize(2);

    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();

    assertThat(dataStore.isManagingBucket(0)).isTrue();
    // try to move the bucket from the other member to this one. This should
    // fail because we already have the bucket
    assertThat(dataStore.moveBucket(0, member1, true)).isFalse();
    assertThat(partitionedRegion.getRegionAdvisor().getBucketOwners(0)).isEqualTo(bucketOwners);
  }

  private void crashServer() {
    crashDistributedSystem(cacheRule.getSystem());
  }

  private InternalCache getCache() {
    return cacheRule.getCache();
  }

  private void createCache(Properties config) {
    cacheRule.createCache(config);
  }

  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.put(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    return getAllDistributedSystemProperties(config);
  }

  private class MyRegionObserver extends DistributionMessageObserver implements Serializable {

    @Override
    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof ManageBucketMessage) {
        vm0.invoke(() -> {
          crashServer();
        });
      }
    }
  }
}
