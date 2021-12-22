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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.internal.cache.PartitionedRegion.RETRY_TIMEOUT_PROPERTY;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;


public class PartitionedRegionHADUnitTest extends CacheTestCase {

  private static final String PR_ZERO_REDUNDANCY = "PR_ZeroRedundancy";
  private static final String PR_ONE_REDUNDANCY = "PR_SingleRedundancy";
  private static final String RETRY_TIMEOUT_VALUE = "20000";

  private String regionName;
  private String prNamePrefix;
  private int numberOfRegions;
  private int totalNumBuckets;
  private int redundantCopies;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  private VM datastoreVM0;
  private VM datastoreVM2;
  private VM accessorVM3;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    regionName = getUniqueName();
    prNamePrefix = "PR-";
    numberOfRegions = 1;
    totalNumBuckets = 5;
    redundantCopies = 0;

    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);

    datastoreVM0 = vm0;
    datastoreVM2 = vm2;
    accessorVM3 = vm3;
  }

  /**
   * Test to ensure that we have proper bucket failover, with no data loss, in the face of
   * sequential cache.close() events.
   */
  @Test
  public void testBucketFailOverDuringCacheClose() throws Exception {
    Boolean value = Boolean.TRUE;

    vm2.invoke(() -> createPartitionedRegion());
    vm3.invoke(() -> createPartitionedRegion());

    vm3.invoke(() -> {
      Cache cache = getCache();
      PartitionedRegion partitionedRegion = (PartitionedRegion) cache.getRegion(regionName);
      assertThat(partitionedRegion).isEmpty();

      // Create keys such that all buckets are created, Integer works well
      // assuming buckets are allocated on the mod of the key hashCode, x 2 just to be safe
      int numEntries = partitionedRegion.getTotalNumberOfBuckets() * 2;

      for (int i = numEntries; i >= 0; --i) {
        partitionedRegion.put(i, value);
      }

      assertThat(partitionedRegion).hasSize(numEntries + 1);
      assertThat(partitionedRegion.getRegionAdvisor().getBucketSet())
          .hasSize(partitionedRegion.getTotalNumberOfBuckets());
    });

    vm3.invoke(() -> validateEntries(value));
    vm2.invoke(() -> validateEntries(value));

    // origin VM down!
    vm2.invoke(() -> getCache().close());
    // origin down, but no data loss
    vm3.invoke(() -> validateEntries(value));

    // get back to the desired redundancy
    vm0.invoke(() -> createPartitionedRegion());
    // verify no data loss
    vm0.invoke(() -> validateEntries(value));

    // 2nd oldest VM down!
    vm3.invoke(() -> getCache().close());
    // 2nd down, but no data loss
    vm0.invoke(() -> validateEntries(value));

    // get back (for 2nd time) to desired redundancy
    vm1.invoke(() -> createPartitionedRegion());
    // verify no data loss
    vm1.invoke(() -> validateEntries(value));
    vm0.invoke(() -> validateEntries(value));
  }

  @Test
  public void testGrabBackupBuckets() throws Exception {
    redundantCopies = 1;

    datastoreVM0.invoke(() -> createPRsAndAwaitRecovery(200, redundantCopies, totalNumBuckets));

    // Do put operations on these 2 PRs asynchronously (this test does not currently do this)

    try (IgnoredException ie = addIgnoredException(PartitionedRegionStorageException.class)) {
      datastoreVM0.invoke(() -> putsInDatastoreVM0());
    }

    // At this point redundancy criterion is not meet.
    // now if we create PRs on more VMs, it should create those "supposed to
    // be redundant" buckets on these nodes, if it can accommodate the data
    // (localMaxMemory>0).
    datastoreVM2.invoke(() -> createPRsAndAwaitRecovery(200, redundantCopies, totalNumBuckets));

    datastoreVM0.invoke(() -> putsInDatastoreVM0());

    accessorVM3.invoke(() -> {
      Cache cache = getCache();
      for (int i = 0; i < numberOfRegions; i++) {
        String regionName = prNamePrefix + i;
        createPartitionedRegion(cache, regionName, 0, redundantCopies, totalNumBuckets);
      }
    });

    for (int i = 0; i < numberOfRegions; i++) {
      final int whichRegion = i;

      int vm2LBRsize =
          (Integer) datastoreVM2.invoke(() -> validateLocalBucket2RegionMapSize(whichRegion));
      int vm3LBRsize =
          (Integer) accessorVM3.invoke(() -> validateLocalBucket2RegionMapSize(whichRegion));

      // This would mean that up coming node didn't pick up any buckets
      assertThat(vm2LBRsize).isGreaterThan(0);

      // This accessor should NOT have picked up any buckets.
      assertThat(vm3LBRsize).isEqualTo(0);

      int vm2B2Nsize = (Integer) datastoreVM2.invoke(() -> validateBucketsOnNode(whichRegion));
      assertThat(vm2LBRsize).isEqualTo(vm2B2Nsize);
    }
  }

  /**
   * This verifies the Bucket Regions on the basis of redundantCopies set in RegionAttributes.
   */
  @Test
  public void testBucketsScope() throws Exception {
    // Create PRs on only 2 VMs
    vm0.invoke(() -> createPRs(PR_ZERO_REDUNDANCY, PR_ONE_REDUNDANCY));
    vm1.invoke(() -> createPRs(PR_ZERO_REDUNDANCY, PR_ONE_REDUNDANCY));

    // Do put operations on these 2 PRs asynchronously (test does not currently do this)

    vm0.invoke(() -> {
      Cache cache = getCache();
      Region<Integer, Integer> regionZeroRedundancy = cache.getRegion(PR_ZERO_REDUNDANCY);
      for (int k = 0; k < 10; k++) {
        regionZeroRedundancy.put(k, k);
      }

      Region<Integer, Integer> regionOneRedundancy = cache.getRegion(PR_ONE_REDUNDANCY);
      for (int k = 0; k < 10; k++) {
        regionOneRedundancy.put(k, k);
      }
    });

    vm0.invoke(() -> validateBucketScope(PR_ZERO_REDUNDANCY, PR_ONE_REDUNDANCY));
    vm1.invoke(() -> validateBucketScope(PR_ZERO_REDUNDANCY, PR_ONE_REDUNDANCY));
  }

  private void validateBucketScope(String prZeroRedundancy, String prSingleRedundancy) {
    Cache cache = getCache();

    PartitionedRegion regionZeroRedundancy = (PartitionedRegion) cache.getRegion(prZeroRedundancy);

    for (BucketRegion bucket : regionZeroRedundancy.getDataStore().getLocalBucket2RegionMap()
        .values()) {
      assertThat(bucket.getAttributes().getScope().isDistributedAck()).isTrue();
    }

    PartitionedRegion regionOneRedundancy = (PartitionedRegion) cache.getRegion(prSingleRedundancy);

    for (Region bucket : regionOneRedundancy.getDataStore().getLocalBucket2RegionMap().values()) {
      assertThat(bucket.getAttributes().getDataPolicy()).isSameAs(DataPolicy.REPLICATE);
    }
  }

  private void createPRs(String prZeroRedundancy, String prOneRedundancy) {
    Cache cache = getCache();

    // RedundantCopies = 0 , Scope = DISTRIBUTED_ACK
    createPartitionedRegion(cache, prZeroRedundancy, 200, 0, totalNumBuckets);
    // RedundantCopies > 0 , Scope = DISTRIBUTED_ACK
    createPartitionedRegion(cache, prOneRedundancy, 200, 1, totalNumBuckets);
  }

  private Object validateBucketsOnNode(int whichRegion) {
    int containsNode = 0;
    Cache cache = getCache();
    PartitionedRegion partitionedRegion =
        (PartitionedRegion) cache.getRegion(prNamePrefix + whichRegion);
    RegionAdvisor regionAdvisor = partitionedRegion.getRegionAdvisor();

    try {
      for (int bucketId : regionAdvisor.getBucketSet()) {
        Set<InternalDistributedMember> nodeList = regionAdvisor.getBucketOwners(bucketId);
        if (nodeList != null && nodeList.contains(partitionedRegion.getMyId())) {
          containsNode++;
        }
      }
    } catch (NoSuchElementException ignored) {
    }

    return containsNode;
  }

  private Object validateLocalBucket2RegionMapSize(int whichRegion) {
    int size = 0;
    Cache cache = getCache();
    PartitionedRegion partitionedRegion =
        (PartitionedRegion) cache.getRegion(prNamePrefix + whichRegion);
    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
    if (dataStore != null) {
      size = dataStore.getBucketsManaged();
    }
    return size;
  }

  private void putsInDatastoreVM0() {
    Cache cache = getCache();
    for (int i = 0; i < numberOfRegions; i++) {
      Region<String, String> region = cache.getRegion(prNamePrefix + i);
      for (int k = 0; k < 10; k++) {
        region.put(i + prNamePrefix + k, prNamePrefix + k);
      }
    }
  }

  private void createPRsAndAwaitRecovery(int localMaxMemory, int redundancy, int totalNumBuckets)
      throws InterruptedException {
    CountDownLatch recoveryDone = new CountDownLatch(numberOfRegions);

    ResourceObserver waitForRecovery = new ResourceObserverAdapter() {
      @Override
      public void rebalancingOrRecoveryFinished(Region region) {
        recoveryDone.countDown();
      }
    };

    InternalResourceManager.setResourceObserver(waitForRecovery);
    String originalValue = setSystemProperty(RETRY_TIMEOUT_PROPERTY, RETRY_TIMEOUT_VALUE);
    try {
      Cache cache = getCache();
      for (int i = 0; i < numberOfRegions; i++) {
        String regionName = prNamePrefix + i;
        createPartitionedRegion(cache, regionName, localMaxMemory, redundancy, totalNumBuckets);
      }
      assertThat(recoveryDone.await(1, MINUTES)).isTrue();
    } finally {
      InternalResourceManager.setResourceObserver(null);
      setSystemProperty(RETRY_TIMEOUT_PROPERTY, originalValue);
    }
  }

  private String setSystemProperty(String property, String value) {
    if (value == null) {
      return System.clearProperty(property);
    } else {
      return System.setProperty(property, value);
    }
  }

  private void validateEntries(boolean value) {
    Cache cache = getCache();
    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    for (int i = region.getTotalNumberOfBuckets() * 2; i >= 0; --i) {
      assertThat(region).containsKey(i);
      assertThat(region.get(i)).isEqualTo(value);
    }
  }

  private void createPartitionedRegion() throws InterruptedException {
    CountDownLatch rebalancingFinished = new CountDownLatch(1);

    ResourceObserver waitForRebalancing = new ResourceObserverAdapter() {
      @Override
      public void rebalancingOrRecoveryFinished(Region region) {
        rebalancingFinished.countDown();
      }
    };

    InternalResourceManager.setResourceObserver(waitForRebalancing);
    try {
      Cache cache = getCache();
      Region partitionedRegion = createPartitionedRegion(cache, regionName, 20, 1, totalNumBuckets);
      assertThat(rebalancingFinished.await(1, MINUTES)).isTrue();
      assertThat(partitionedRegion).isNotNull();
    } finally {
      InternalResourceManager.setResourceObserver(null);
    }
  }

  private PartitionedRegion createPartitionedRegion(Cache cache, String regionName,
      int localMaxMemory, int redundancy, int totalNumBuckets) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);
    partitionAttributesFactory.setRedundantCopies(redundancy);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
    return (PartitionedRegion) regionFactory.create(regionName);
  }
}
