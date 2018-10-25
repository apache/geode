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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectFromDS;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InitialImageOperation.RequestImageMessage;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * RegressionTests extracted from {@link PersistentPartitionedRegionDistributedTest}.
 */
@SuppressWarnings("serial,unused")
public class PersistentPartitionedRegionRegressionTest implements Serializable {

  private static final int NUM_BUCKETS = 15;
  private static final AtomicBoolean CRASHED = new AtomicBoolean();

  private String partitionedRegionName;

  private VM vm0;
  private VM vm1;
  private VM vm2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedDiskDirRule diskDirRule = new DistributedDiskDirRule();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);

    String uniqueName = getClass().getSimpleName() + "-" + testName.getMethodName();
    partitionedRegionName = uniqueName + "-partitionedRegion";
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> InternalResourceManager.setResourceObserver(null));
  }

  /**
   * RegressionTest for bug 44184
   *
   * <p>
   * TRAC #44184: Custom Expiration causes issues with PR recovery
   */
  @Test
  public void recoversFromDiskWithCustomExpiration() {
    createPartitionedRegionWithCustomEntryIdleTimeout();

    createData(0, 1, "a", partitionedRegionName);

    Set<Integer> vm0Buckets = getBucketList(partitionedRegionName);

    getCache().close();

    createPartitionedRegionWithCustomEntryIdleTimeout();

    assertThat(getBucketList(partitionedRegionName)).isEqualTo(vm0Buckets);

    checkData(0, 1, "a", partitionedRegionName);
  }

  /**
   * RegressionTest: replace buckets which are offline on A by creating them on C. We then
   * shutdown C and restart A, which recovers those buckets.
   *
   * <p>
   * TRAC 41340: data inconsistency after disk recovery from persistent PR
   */
  @Test
  public void bucketsRecoverAfterRestartingOriginalServer() throws Exception {
    vm0.invoke(() -> createPartitionedRegion(1, 0, 113, true));
    vm1.invoke(() -> createPartitionedRegion(1, 0, 113, true));

    vm0.invoke(() -> createData(0, NUM_BUCKETS, "a", partitionedRegionName));

    // This should do nothing because we have satisfied redundancy.
    vm2.invoke(() -> createPartitionedRegion(1, 0, 113, true));
    assertThat(vm2.invoke(() -> getBucketList(partitionedRegionName))).isEmpty();

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList(partitionedRegionName));
    Set<Integer> bucketsLost = vm1.invoke(() -> getBucketList(partitionedRegionName));

    vm1.invoke(() -> getCache().close());

    // VM2 should pick up the slack
    vm2.invoke(() -> awaitBucketRecovery(bucketsLost));

    vm0.invoke(() -> createData(0, NUM_BUCKETS, "b", partitionedRegionName));

    // VM1 should recover, but it shouldn't host the bucket anymore
    vm1.invoke(() -> createPartitionedRegion(1, 0, 113, true));

    // The data shouldn't be affected.
    vm1.invoke(() -> checkData(0, NUM_BUCKETS, "b", partitionedRegionName));

    vm2.invoke(() -> getCache().close());

    // The buckets should move back to vm1.
    vm1.invoke(() -> awaitBucketRecovery(bucketsLost));

    assertThat(vm0.invoke(() -> getBucketList(partitionedRegionName))).isEqualTo(bucketsOnVM0);
    assertThat(vm1.invoke(() -> getBucketList(partitionedRegionName))).isEqualTo(bucketsOnVM0);

    // The data shouldn't be affected.
    vm1.invoke(() -> checkData(0, NUM_BUCKETS, "b", partitionedRegionName));

    // restart everything, and make sure it comes back correctly.

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());

    AsyncInvocation<Void> createPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createPartitionedRegion(1, -1, 113, true));
    AsyncInvocation<Void> createPartitionedRegionOnVM1 =
        vm1.invokeAsync(() -> createPartitionedRegion(1, -1, 113, true));

    createPartitionedRegionOnVM0.await(2, MINUTES);
    createPartitionedRegionOnVM1.await(2, MINUTES);

    // The data shouldn't be affected.
    vm1.invoke(() -> checkData(0, NUM_BUCKETS, "b", partitionedRegionName));
    assertThat(vm0.invoke(() -> getBucketList(partitionedRegionName))).isEqualTo(bucketsOnVM0);
    assertThat(vm1.invoke(() -> getBucketList(partitionedRegionName))).isEqualTo(bucketsOnVM0);
  }

  /**
   * RegressionTest that we don't record our old member ID as offline, preventing redundancy
   * recovery in the future.
   *
   * <p>
   * TRAC 41341: Redundancy not restored after reinitializing after locally destroying persistent PR
   */
  @Test
  public void recoversRedundancyAfterRecreatingRegion() {
    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    vm0.invoke(() -> createData(0, 1, "a", partitionedRegionName));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList(partitionedRegionName));
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList(partitionedRegionName));

    assertThat(bucketsOnVM0).containsOnly(0);
    assertThat(bucketsOnVM1).containsOnly(0);

    vm1.invoke(() -> getCache().close());

    // This shouldn't create any buckets, because we know there are offline copies
    vm2.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    assertThat(vm0.invoke(() -> getOfflineMembers(0))).hasSize(1);
    // Note, vm2 will consider vm1 as "online" because vm2 doesn't host the bucket
    assertThat(vm2.invoke(() -> getOnlineMembers(0))).hasSize(2);

    Set<Integer> bucketsOnVM2 = vm2.invoke(() -> getBucketList(partitionedRegionName));
    assertThat(bucketsOnVM2).isEmpty();

    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    // Make sure we restored the buckets in the right place
    assertThat(vm0.invoke(() -> getBucketList(partitionedRegionName))).isEqualTo(bucketsOnVM0);
    assertThat(vm1.invoke(() -> getBucketList(partitionedRegionName))).isEqualTo(bucketsOnVM1);
    assertThat(vm2.invoke(() -> getBucketList(partitionedRegionName))).isEmpty();

    assertThat(vm0.invoke(() -> getOfflineMembers(0))).isEmpty();
    assertThat(vm1.invoke(() -> getOfflineMembers(0))).isEmpty();

    InternalDistributedMember memberVM1 = vm1.invoke(() -> getInternalDistributedMember());
    vm2.invoke(() -> {
      PartitionedRegion region = (PartitionedRegion) getCache().getRegion(partitionedRegionName);
      assertThat(region.getDataStore().moveBucket(0, memberVM1, false)).isTrue();
    });

    assertThat(vm0.invoke(() -> getOfflineMembers(0))).isEmpty();
    assertThat(vm1.invoke(() -> getOfflineMembers(0))).isEmpty();
    assertThat(vm2.invoke(() -> getOfflineMembers(0))).isEmpty();

    assertThat(vm0.invoke(() -> getBucketList(partitionedRegionName))).containsOnly(0);
    assertThat(vm1.invoke(() -> getBucketList(partitionedRegionName))).isEmpty();
    assertThat(vm2.invoke(() -> getBucketList(partitionedRegionName))).containsOnly(0);

    // Destroy VM2
    vm2.invoke(() -> getCache().getRegion(partitionedRegionName).localDestroyRegion());

    assertThat(vm0.invoke(() -> getOfflineMembers(0))).isEmpty();
    assertThat(vm1.invoke(() -> getOfflineMembers(0))).isEmpty();

    // Close down VM 1
    vm1.invoke(() -> getCache().close());

    assertThat(vm0.invoke(() -> getOfflineMembers(0))).isEmpty();

    // This should recover redundancy, because vm2 was destroyed
    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    assertThat(vm0.invoke(() -> getBucketList(partitionedRegionName))).containsOnly(0);
    assertThat(vm1.invoke(() -> getBucketList(partitionedRegionName))).containsOnly(0);

    assertThat(vm0.invoke(() -> getOfflineMembers(0))).isEmpty();
    assertThat(vm1.invoke(() -> getOfflineMembers(0))).isEmpty();
  }

  /**
   * RegressionTest for bug 41336
   *
   * <p>
   * TRAC #41336: Unexpected PartitionOfflineException
   */
  @Test
  public void recoversAfterBucketCreationCrashes() throws Exception {
    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
        @Override
        public void beforeSendMessage(ClusterDistributionManager dm, DistributionMessage message) {
          if (message instanceof ManageBucketMessage.ManageBucketReplyMessage) {
            Cache cache = getCache();
            disconnectFromDS();
            await().until(() -> cache.isClosed());
            CRASHED.set(true);
          }
        }
      });
    });

    vm0.invoke(() -> createPartitionedRegion(0, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(0, -1, 113, true));

    vm1.invoke(() -> createData(0, 4, "a", partitionedRegionName));

    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList(partitionedRegionName));
    assertThat(bucketsOnVM1).hasSize(4);

    vm1.invoke(() -> checkData(0, 4, "a", partitionedRegionName));

    // wait till cache is completely shutdown before trying to create the region again. otherwise
    // deadlock situation might happen.
    vm0.invoke(() -> await().until(() -> CRASHED.get()));
    vm0.invoke(() -> createPartitionedRegion(0, -1, 113, true));
    vm0.invoke(() -> checkData(0, 4, "a", partitionedRegionName));

    assertThat(vm1.invoke(() -> getBucketList(partitionedRegionName))).isEqualTo(bucketsOnVM1);
    assertThat(vm0.invoke(() -> getBucketList(partitionedRegionName))).isEmpty();

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());

    AsyncInvocation<Void> createPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createPartitionedRegion(0, -1, 113, true));
    AsyncInvocation<Void> createPartitionedRegionOnVM1 =
        vm1.invokeAsync(() -> createPartitionedRegion(0, -1, 113, true));

    createPartitionedRegionOnVM0.await(2, MINUTES);
    createPartitionedRegionOnVM1.await(2, MINUTES);

    vm0.invoke(() -> checkData(0, 4, "a", partitionedRegionName));
    assertThat(vm1.invoke(() -> getBucketList(partitionedRegionName))).isEqualTo(bucketsOnVM1);
    assertThat(vm0.invoke(() -> getBucketList(partitionedRegionName))).isEmpty();
  }

  /**
   * RegressionTest for bug 42226. <br>
   * 1. Member A has the bucket <br>
   * 2. Member B starts creating the bucket. It tells member A that it hosts the bucket <br>
   * 3. Member A crashes <br>
   * 4. Member B destroys the bucket and throws a partition offline exception, because it wasn't
   * able to complete initialization. <br>
   * 5. Member A recovers, and gets stuck waiting for member B.
   *
   * <p>
   * TRAC 42226: recycled VM hangs during re-start while waiting for Partition to come online (after
   * Controller VM sees unexpected PartitionOffLineException while doing ops)
   */
  @Test
  public void doesNotWaitForPreviousInstanceOfOnlineServer() {
    // Add a hook to disconnect from the distributed system when the initial image message shows up.
    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof RequestImageMessage) {
            RequestImageMessage requestImageMessage = (RequestImageMessage) message;
            // Don't disconnect until we see a bucket
            if (requestImageMessage.regionPath.contains("_B_")) {
              DistributionMessageObserver.setInstance(null);
              disconnectFromDS();
            }
          }
        }

        @Override
        public void afterProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          // nothing
        }
      });
    });

    vm0.invoke(() -> createPartitionedRegion(1, 0, 1, true));

    // Make sure we create a bucket
    vm0.invoke(() -> createData(0, 1, "a", partitionedRegionName));

    // This should recover redundancy, which should cause vm0 to disconnect

    try (IgnoredException ie = addIgnoredException(PartitionOfflineException.class)) {
      vm1.invoke(() -> createPartitionedRegion(1, 0, 1, true));

      // Make sure get a partition offline exception
      vm1.invoke(() -> {
        assertThatThrownBy(() -> createData(0, 1, "a", partitionedRegionName))
            .isInstanceOf(PartitionOfflineException.class);
      });
    }

    // Make sure vm0 is really disconnected (avoids a race with the observer).
    vm0.invoke(() -> disconnectFromDS());

    // This should recreate the bucket
    vm0.invoke(() -> createPartitionedRegion(1, 0, 1, true));

    vm1.invoke(() -> checkData(0, 1, "a", partitionedRegionName));
  }

  /**
   * RegressionTest for bug 41436. If the GII source crashes before the GII is complete, we need to
   * make sure that later we can recover redundancy.
   *
   * <p>
   * TRAC #41436: PR redundancy lost with HA (expected n members in primary list, but found n-1)
   */
  @Test
  public void recoversBucketsAfterCrashDuringGii() {
    addIgnoredException(PartitionOfflineException.class);

    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    vm0.invoke(() -> createData(0, 1, "value", partitionedRegionName));

    // Add an observer which will close the cache when the GII starts
    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof RequestImageMessage) {
            RequestImageMessage requestImageMessage = (RequestImageMessage) message;
            if (requestImageMessage.regionPath.contains("_0")) {
              DistributionMessageObserver.setInstance(null);
              getCache().close();
            }
          }
        }
      });
    });

    // createPR(vm1, 1);
    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    // Make sure vm1 didn't create the bucket
    assertThat(vm1.invoke(() -> getBucketList(partitionedRegionName))).isEmpty();

    // createPR(vm0, 1, 0);
    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    // Make sure vm0 recovers the bucket
    assertThat(vm0.invoke(() -> getBucketList(partitionedRegionName))).containsOnly(0);

    // vm1 should satisfy redundancy for the bucket as well
    assertThat(vm1.invoke(() -> getBucketList(partitionedRegionName))).containsOnly(0);
  }

  /**
   * Another RegressionTest for bug 41436. If the GII source crashes before the GII is complete, we
   * need to make sure that later we can recover redundancy.
   *
   * <p>
   * In this test case, we bring the GII down before we bring the source back up, to make sure the
   * source still discovers that the GII target is no longer hosting the bucket.
   *
   * <p>
   * TRAC #41436: PR redundancy lost with HA (expected n members in primary list, but found n-1)
   */
  @Test
  public void recoversBucketsAfterRestartAfterCrashDuringGii() throws Exception {
    addIgnoredException(PartitionOfflineException.class);

    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    vm0.invoke(() -> createData(0, 1, "value", partitionedRegionName));

    // Add an observer which will close the cache when the GII starts
    vm0.invoke(() -> {
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {
        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof RequestImageMessage) {
            RequestImageMessage requestImageMessage = (RequestImageMessage) message;
            if (requestImageMessage.regionPath.contains("_0")) {
              DistributionMessageObserver.setInstance(null);
              getCache().close();
            }
          }
        }

      });
    });

    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    // Make sure vm1 didn't create the bucket
    assertThat(vm1.invoke(() -> getBucketList(partitionedRegionName))).isEmpty();

    vm1.invoke(() -> getCache().close());

    AsyncInvocation<Void> createPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createPartitionedRegion(1, -1, 113, true));
    createPartitionedRegionOnVM0.join(SECONDS.toMillis(1));
    // vm0 should get stuck waiting for vm1 to recover from disk,
    // because vm0 thinks vm1 has the bucket
    assertThat(createPartitionedRegionOnVM0.isAlive()).isTrue();
    assertThat(createPartitionedRegionOnVM0).isNotCancelled().isNotDone();

    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    // Make sure vm0 recovers the bucket
    assertThat(vm0.invoke(() -> getBucketList(partitionedRegionName))).containsOnly(0);

    // vm1 should satisfy redundancy for the bucket as well
    await().untilAsserted(() -> {
      assertThat(vm1.invoke(() -> getBucketList(partitionedRegionName))).containsOnly(0);
    });
  }

  private InternalDistributedMember getInternalDistributedMember() {
    return getCache().getInternalDistributedSystem().getDistributedMember();
  }

  private Set<PersistentMemberID> getOfflineMembers(final int bucketId) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(partitionedRegionName);
    return region.getRegionAdvisor().getProxyBucketArray()[bucketId].getPersistenceAdvisor()
        .getMembershipView().getOfflineMembers();
  }

  private Set<PersistentMemberID> getOnlineMembers(final int bucketId) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(partitionedRegionName);
    return region.getRegionAdvisor().getProxyBucketArray()[bucketId].getPersistenceAdvisor()
        .getPersistedOnlineOrEqualMembers();
  }

  private void awaitBucketRecovery(Set<Integer> bucketsLost) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(partitionedRegionName);
    PartitionedRegionDataStore dataStore = region.getDataStore();

    await().until(() -> bucketsLost.equals(dataStore.getAllLocalBucketIds()));
  }

  private void createPartitionedRegion(final int redundancy, final int recoveryDelay,
      final int numBuckets, final boolean synchronous) throws InterruptedException {
    CountDownLatch recoveryDone = new CountDownLatch(1);

    if (redundancy > 0) {
      ResourceObserver observer = new ResourceObserverAdapter() {
        @Override
        public void recoveryFinished(Region region) {
          recoveryDone.countDown();
        }
      };

      InternalResourceManager.setResourceObserver(observer);
    } else {
      recoveryDone.countDown();
    }

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundancy);
    partitionAttributesFactory.setRecoveryDelay(recoveryDelay);
    partitionAttributesFactory.setTotalNumBuckets(numBuckets);
    partitionAttributesFactory.setLocalMaxMemory(500);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskSynchronous(synchronous);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(partitionedRegionName);

    recoveryDone.await(2, MINUTES);
  }

  private void createData(final int startKey, final int endKey, final String value,
      final String regionName) {
    Region<Integer, String> region = getCache().getRegion(regionName);
    for (int i = startKey; i < endKey; i++) {
      region.put(i, value);
    }
  }

  private Set<Integer> getBucketList(final String regionName) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);
    return new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
  }

  private void checkData(final int startKey, final int endKey, final String value,
      final String regionName) {
    Region<Integer, String> region = getCache().getRegion(regionName);
    for (int i = startKey; i < endKey; i++) {
      assertThat(region.get(i)).isEqualTo(value);
    }
  }

  private void createPartitionedRegionWithCustomEntryIdleTimeout() {
    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setCustomEntryIdleTimeout(new TestCustomExpiration<>());
    regionFactory.setEntryIdleTimeout(new ExpirationAttributes(60, ExpirationAction.INVALIDATE));

    regionFactory.create(partitionedRegionName);
  }

  private InternalCache getCache() {
    return cacheRule.getOrCreateCache();
  }

  private static class TestCustomExpiration<K, V> implements CustomExpiry<K, V> {

    @Override
    public void close() {
      // do nothing
    }

    @Override
    public ExpirationAttributes getExpiry(Region.Entry entry) {
      return new ExpirationAttributes(
          (entry.getKey().hashCode() + entry.getValue().hashCode()) % 100,
          ExpirationAction.INVALIDATE);
    }
  }
}
