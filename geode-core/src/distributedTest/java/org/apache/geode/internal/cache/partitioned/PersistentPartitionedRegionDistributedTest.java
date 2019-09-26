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
import static org.apache.geode.admin.AdminDistributedSystemFactory.defineDistributedSystem;
import static org.apache.geode.admin.AdminDistributedSystemFactory.getDistributedSystem;
import static org.apache.geode.cache.EvictionAction.OVERFLOW_TO_DISK;
import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.PARTITION_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PROXY;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.persistence.ConflictingPersistentDataException;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.cache.persistence.RevokeFailedException;
import org.apache.geode.cache.persistence.RevokedPersistentDataException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheClosingDistributionMessageObserver;
import org.apache.geode.internal.cache.DiskRegion;
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
 * Tests the basic use cases for PR persistence.
 */
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial,unused")
public class PersistentPartitionedRegionDistributedTest implements Serializable {

  private static final int NUM_BUCKETS = 15;

  private String partitionedRegionName;
  private String parentRegion1Name;
  private String parentRegion2Name;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule =
      CacheRule.builder().addConfig(getDistributedSystemProperties()).build();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedDiskDirRule diskDirRule = new DistributedDiskDirRule();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    String uniqueName = getClass().getSimpleName() + "-" + testName.getMethodName();
    partitionedRegionName = uniqueName + "-partitionedRegion";
    parentRegion1Name = "parent1";
    parentRegion2Name = "parent2";
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> {
      InternalResourceManager.setResourceObserver(null);
      DistributionMessageObserver.setInstance(null);
    });
  }

  private Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.setProperty(SERIALIZABLE_OBJECT_FILTER, TestFunction.class.getName());
    return config;
  }

  /**
   * A simple test case that we are actually persisting with a PR.
   */
  @Test
  public void recoversFromDisk() throws Exception {
    createPartitionedRegion(0, -1, 113, true);

    createData(0, 1, "a");

    Set<Integer> vm0Buckets = getBucketList();

    getCache().close();

    createPartitionedRegion(0, -1, 113, true);

    assertThat(getBucketList()).isEqualTo(vm0Buckets);

    checkData(0, 1, "a");

    getCache().getRegion(partitionedRegionName).localDestroyRegion();

    getCache().close();

    createPartitionedRegion(0, -1, 113, true);

    // Make sure the data is now missing
    checkData(0, 1, null);
  }

  @Test
  public void numberOfBucketsIsImmutable() throws Exception {
    createPartitionedRegion(0, 0, 5, true);
    createData(0, 5, "a");
    getCache().close();

    addIgnoredException(IllegalStateException.class);
    addIgnoredException(DiskAccessException.class);

    assertThatThrownBy(() -> createPartitionedRegion(0, 0, 2, true))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining(
            String.format(
                "For partition region %s,total-num-buckets %s should not be changed. Previous configured number is %s.",
                "/" + partitionedRegionName, 2, 5));

    getCache().close();

    assertThatThrownBy(() -> createPartitionedRegion(0, 0, 10, true))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining(
            String.format(
                "For partition region %s,total-num-buckets %s should not be changed. Previous configured number is %s.",
                "/" + partitionedRegionName, 10, 5));
  }

  @Test
  public void missingDiskStoreCanBeRevoked() throws Exception {
    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    int numBuckets = 50;
    vm0.invoke(() -> createData(0, numBuckets, "a"));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());
    assertThat(bucketsOnVM1).isEqualTo(bucketsOnVM0);

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> createData(0, numBuckets, "b"));

    vm1.invoke(() -> getCache().close());

    AsyncInvocation<Void> createPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createPartitionedRegion(1, -1, 113, true));
    // Note: Make sure that vm0 is waiting for vm1 to recover
    // If VM(0) recovers early, that is a problem, because vm1 has newer data
    createPartitionedRegionOnVM0.join(SECONDS.toMillis(1));
    assertThat(createPartitionedRegionOnVM0.isAlive()).isTrue();

    vm2.invoke(() -> {
      DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
      AdminDistributedSystem adminDS = getDistributedSystem(config);
      adminDS.connect();
      try {
        adminDS.waitToBeConnected(MINUTES.toMillis(2));

        await().until(() -> {
          Set<PersistentID> missingIds = adminDS.getMissingPersistentMembers();
          if (missingIds.size() != 1) {
            return false;
          }
          for (PersistentID missingId : missingIds) {
            adminDS.revokePersistentMember(missingId.getUUID());
          }
          return true;
        });

      } finally {
        adminDS.disconnect();
      }
    });

    createPartitionedRegionOnVM0.await(2, MINUTES);

    assertThat(vm0.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM0);

    vm0.invoke(() -> {
      checkData(0, numBuckets, "a");
      createData(numBuckets, 113, "b");
      checkData(numBuckets, 113, "b");
    });

    addIgnoredException(RevokedPersistentDataException.class);

    vm1.invoke(() -> {
      assertThatThrownBy(() -> createPartitionedRegion(1, -1, 113, true))
          .isInstanceOf(RevokedPersistentDataException.class);
    });
  }

  /**
   * Test to make sure that we can recover from a complete system shutdown
   */
  @Test
  @Parameters({"0", "1"})
  @TestCaseName("{method}({params})")
  public void recoversFromDiskAfterCompleteShutdown(int redundancy) throws Exception {
    int numBuckets = 50;

    vm0.invoke(() -> createPartitionedRegion(redundancy, -1, numBuckets, true));
    vm1.invoke(() -> createPartitionedRegion(redundancy, -1, numBuckets, true));
    vm2.invoke(() -> createPartitionedRegion(redundancy, -1, numBuckets, true));

    vm0.invoke(() -> createData(0, numBuckets, "a"));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM2 = vm2.invoke(() -> getBucketList());

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());
    vm2.invoke(() -> getCache().close());

    AsyncInvocation<Void> createPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createPartitionedRegion(redundancy, -1, numBuckets, true));
    AsyncInvocation<Void> createPartitionedRegionOnVM1 =
        vm1.invokeAsync(() -> createPartitionedRegion(redundancy, -1, numBuckets, true));
    AsyncInvocation<Void> createPartitionedRegionOnVM2 =
        vm2.invokeAsync(() -> createPartitionedRegion(redundancy, -1, numBuckets, true));

    createPartitionedRegionOnVM0.await(2, MINUTES);
    createPartitionedRegionOnVM1.await(2, MINUTES);
    createPartitionedRegionOnVM2.await(2, MINUTES);

    assertThat(vm0.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM0);
    assertThat(vm1.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM1);
    assertThat(vm2.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM2);

    vm0.invoke(() -> {
      checkData(0, numBuckets, "a");
      createData(numBuckets, 113, "b");
      checkData(numBuckets, 113, "b");
    });

    // I think the following is sort of a TODO...
    // TRAC #43476: CREATE TABLE freeze on a 2-member distributed system (with exact commands)
    // RegressionTest for bug 43476 - make sure a destroy cleans up proxy bucket regions.

    vm0.invoke(() -> getCache().getRegion(partitionedRegionName).localDestroyRegion());
    vm1.invoke(() -> getCache().getRegion(partitionedRegionName).localDestroyRegion());
    vm2.invoke(() -> getCache().getRegion(partitionedRegionName).localDestroyRegion());

    createPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createPartitionedRegion(redundancy, -1, numBuckets, true));
    createPartitionedRegionOnVM1 =
        vm1.invokeAsync(() -> createPartitionedRegion(redundancy, -1, numBuckets, true));
    createPartitionedRegionOnVM2 =
        vm2.invokeAsync(() -> createPartitionedRegion(redundancy, -1, numBuckets, true));

    createPartitionedRegionOnVM0.await(2, MINUTES);
    createPartitionedRegionOnVM1.await(2, MINUTES);
    createPartitionedRegionOnVM2.await(2, MINUTES);

    vm0.invoke(() -> checkData(0, numBuckets, null));
  }

  /**
   * Note: This test is only valid for
   * {@link AdminDistributedSystem#revokePersistentMember(InetAddress, String)} which is deprecated
   * and does not exist in GFSH. GFSH only supports revoking by UUID which cannot be revoked
   * <bold>before</bold> a member starts up and is missing the diskStore identified by the UUID.
   */
  @Test
  public void missingDiskStoreCanBeRevokedBeforeStartingServer() throws Exception {
    int numBuckets = 50;

    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    vm0.invoke(() -> createData(0, numBuckets, "a"));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());
    assertThat(bucketsOnVM1).isEqualTo(bucketsOnVM0);

    // This should fail with a revocation failed message
    try (IgnoredException ie = addIgnoredException(RevokeFailedException.class)) {
      vm2.invoke("revoke disk store should fail", () -> {
        assertThatThrownBy(() -> {
          DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
          AdminDistributedSystem adminDS = getDistributedSystem(config);
          adminDS.connect();

          try {
            adminDS.waitToBeConnected(MINUTES.toMillis(2));
            adminDS.revokePersistentMember(getFirstInet4Address(), null);
          } finally {
            adminDS.disconnect();
          }
        }).isInstanceOf(ReplyException.class).hasCauseInstanceOf(RevokeFailedException.class);
      });
    }

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> createData(0, numBuckets, "b"));

    String diskDirPathOnVM1 = diskDirRule.getDiskDirFor(vm1).getAbsolutePath();
    vm1.invoke(() -> getCache().close());

    vm0.invoke(() -> {
      getCache();
    });

    vm2.invoke(() -> {
      DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
      AdminDistributedSystem adminDS = getDistributedSystem(config);
      adminDS.connect();
      try {
        adminDS.waitToBeConnected(MINUTES.toMillis(2));
        adminDS.revokePersistentMember(getFirstInet4Address(), diskDirPathOnVM1);
      } finally {
        adminDS.disconnect();
      }
    });

    vm0.invoke(() -> {
      createPartitionedRegion(1, -1, 113, true);
      assertThat(getBucketList()).isEqualTo(bucketsOnVM0);

      checkData(0, numBuckets, "a");
      createData(numBuckets, 113, "b");
      checkData(numBuckets, 113, "b");
    });

    try (IgnoredException ie = addIgnoredException(RevokedPersistentDataException.class)) {
      vm1.invoke(() -> {
        assertThatThrownBy(() -> createPartitionedRegion(1, -1, 113, true))
            .isInstanceOf(RevokedPersistentDataException.class);
      });
    }
  }

  /**
   * Test that we wait for missing data to come back if the redundancy was 0.
   */
  @Test
  public void waitsForMissingDiskStoreWhenRedundancyIsZero() throws Exception {
    vm0.invoke(() -> createPartitionedRegion(0, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(0, -1, 113, true));

    vm0.invoke(() -> createData(0, NUM_BUCKETS, "a"));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());

    int bucketOnVM0 = bucketsOnVM0.iterator().next();
    int bucketOnVM1 = bucketsOnVM1.iterator().next();

    vm1.invoke(() -> getCache().close());

    try (IgnoredException ie = addIgnoredException(PartitionOfflineException.class)) {
      vm0.invoke(() -> checkReadWriteOperationsWithOfflineMember(bucketOnVM0, bucketOnVM1));

      // Make sure that a newly created member is informed about the offline member
      vm2.invoke(() -> createPartitionedRegion(0, -1, 113, true));
      vm2.invoke(() -> checkReadWriteOperationsWithOfflineMember(bucketOnVM0, bucketOnVM1));
    }

    // This should work, because these are new buckets
    vm0.invoke(() -> createData(NUM_BUCKETS, 113, "a"));

    vm1.invoke(() -> createPartitionedRegion(0, -1, 113, true));

    // The data should be back online now.
    vm0.invoke(() -> checkData(0, 113, "a"));
  }

  /**
   * Test to make sure that we recreate a bucket if a member is destroyed
   */
  @Test
  public void recreatesBucketIfHostIsDestroyed() {
    vm0.invoke(() -> createPartitionedRegion(0, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(0, -1, 113, true));

    vm0.invoke(() -> createData(0, NUM_BUCKETS, "a"));

    Set<Integer> bucketOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketOnVM1 = vm1.invoke(() -> getBucketList());

    int bucketIdOnVM0 = bucketOnVM0.iterator().next();
    int bucketIdOnVM1 = bucketOnVM1.iterator().next();

    vm1.invoke(() -> getCache().getRegion(partitionedRegionName).localDestroyRegion());

    vm0.invoke(() -> {
      // This should work, because this bucket is still available.
      checkData(bucketIdOnVM0, bucketIdOnVM0 + 1, "a");

      // This should find that the data is missing, because we destroyed that bucket
      checkData(bucketIdOnVM1, bucketIdOnVM1 + 1, null);

      // We should be able to recreate that bucket
      createData(bucketIdOnVM1, bucketIdOnVM1 + 1, "b");
    });

    vm1.invoke(() -> createPartitionedRegion(0, -1, 113, true));

    vm0.invoke(() -> {
      // The data should still be available
      checkData(bucketIdOnVM0, bucketIdOnVM0 + 1, "a");
      checkData(bucketIdOnVM1, bucketIdOnVM1 + 1, "b");

      // This bucket should now be in vm0, because we recreated it there
      assertThat(getBucketList()).contains(bucketIdOnVM1);
    });
  }

  /**
   * Test to make sure that we recreate a bucket if a member is destroyed
   */
  @Test
  public void recreatesBucketIfHostIsDestroyedAndRedundancyIsOne() {
    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    vm0.invoke(() -> createData(0, NUM_BUCKETS, "a"));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());
    assertThat(bucketsOnVM1).isEqualTo(bucketsOnVM0);

    int bucketIdOnVM0 = bucketsOnVM0.iterator().next();
    vm1.invoke(() -> getCache().getRegion(partitionedRegionName).localDestroyRegion());

    // This should work, because this bucket is still available.
    vm0.invoke(() -> checkData(bucketIdOnVM0, bucketIdOnVM0 + 1, "a"));

    vm2.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    Set<Integer> vm2Buckets = vm2.invoke(() -> getBucketList());

    // VM 2 should have created a copy of all of the buckets
    assertThat(vm2Buckets).isEqualTo(bucketsOnVM0);
  }

  /**
   * Test to make sure that we recreate a bucket if a member is revoked
   */
  @Test
  public void recreatesBucketIfDiskStoreIsRevokedAndRedundancyIsZero() {
    vm0.invoke(() -> createPartitionedRegion(0, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(0, -1, 113, true));

    vm0.invoke(() -> createData(0, NUM_BUCKETS, "a"));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());

    int bucketOnVM0 = bucketsOnVM0.iterator().next();
    int bucketOnVM1 = bucketsOnVM1.iterator().next();

    vm1.invoke(() -> getCache().close());

    vm0.invoke(() -> {
      // This should work, because this bucket is still available.
      checkData(bucketOnVM0, bucketOnVM0 + 1, "a");

      try (IgnoredException ie = addIgnoredException("PartitionOfflineException", vm0)) {
        assertThatThrownBy(() -> checkData(bucketOnVM1, bucketOnVM1 + 1, "a"))
            .isInstanceOf(PartitionOfflineException.class);

        assertThatThrownBy(() -> checkData(bucketOnVM1, bucketOnVM1 + 1, "b"))
            .isInstanceOf(PartitionOfflineException.class);
      }

      // This should work, because these are new buckets
      createData(NUM_BUCKETS, 113, "a");
    });

    vm2.invoke(() -> revokeKnownMissingMembers(1));

    vm2.invoke(() -> {
      createPartitionedRegion(0, -1, 113, true);

      // We should be able to use that missing bucket now
      checkData(bucketOnVM1, bucketOnVM1 + 1, null);
      createData(bucketOnVM1, bucketOnVM1 + 1, "a");
      checkData(bucketOnVM1, bucketOnVM1 + 1, "a");
    });

    vm1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(RevokedPersistentDataException.class)) {
        assertThatThrownBy(() -> createPartitionedRegion(0, -1, 113, true))
            .isInstanceOf(RevokedPersistentDataException.class);
      }
    });
  }

  /**
   * Test to make sure that we recreate a bucket if a member is revoked
   */
  @Test
  public void recreatesBucketIfDiskStoreIsRevokedAndRedundancyIsOne() throws Exception {
    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    vm0.invoke(() -> createData(0, NUM_BUCKETS, "a"));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());
    assertThat(bucketsOnVM1).isEqualTo(bucketsOnVM0);

    vm1.invoke(() -> getCache().close());

    vm0.invoke(() -> {
      // This should work, because this bucket is still available.
      checkData(0, NUM_BUCKETS, "a");

      createData(0, NUM_BUCKETS, "b");
    });

    vm2.invoke(() -> {
      revokeKnownMissingMembers(1);

      // This should make a copy of all of the buckets, because we have revoked VM1.
      createPartitionedRegion(1, -1, 113, true);

      Set<Integer> bucketsOnVM2 = vm2.invoke(() -> getBucketList());
      assertThat(bucketsOnVM2).isEqualTo(bucketsOnVM1);
    });

    vm1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(RevokedPersistentDataException.class)) {
        assertThatThrownBy(() -> createPartitionedRegion(1, -1, 113, true))
            .isInstanceOf(RevokedPersistentDataException.class);
      }
    });

    // Test that we can bounce vm0 and vm1, and still get a RevokedPersistentDataException
    // when vm1 tries to recover
    vm0.invoke(() -> getCache().close());
    vm2.invoke(() -> getCache().close());

    AsyncInvocation<Void> createPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createPartitionedRegion(1, -1, 113, true));
    AsyncInvocation<Void> createPartitionedRegionOnVM2 =
        vm2.invokeAsync(() -> createPartitionedRegion(1, -1, 113, true));

    createPartitionedRegionOnVM0.await(2, MINUTES);
    createPartitionedRegionOnVM2.await(2, MINUTES);

    vm1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(RevokedPersistentDataException.class)) {
        assertThatThrownBy(() -> createPartitionedRegion(1, -1, 113, true))
            .isInstanceOf(RevokedPersistentDataException.class);
      }
    });

    // The data shouldn't be affected.
    vm2.invoke(() -> checkData(0, NUM_BUCKETS, "b"));
  }

  /**
   * Test to make sure that we recreate a bucket if a member is revoked, and that we do it
   * immediately if recovery delay is set to 0.
   */
  @Test
  public void recreatesBucketImmediatelyIfDiskStoreIsRevokedAndRecoveryDelayIsZero()
      throws Exception {
    vm0.invoke(() -> createPartitionedRegion(1, 0, 113, true));
    vm1.invoke(() -> createPartitionedRegion(1, 0, 113, true));

    vm0.invoke(() -> createData(0, NUM_BUCKETS, "a"));

    // This should do nothing because we have satisfied redundancy.
    vm2.invoke(() -> createPartitionedRegion(1, 0, 113, true));
    assertThat(vm2.invoke(() -> getBucketList())).isEmpty();

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsLost = vm1.invoke(() -> getBucketList());

    vm1.invoke(() -> getCache().close());

    // VM2 should pick up the slack

    await().until(() -> {
      Set<Integer> vm2Buckets = vm2.invoke(() -> getBucketList());
      return bucketsLost.equals(vm2Buckets);
    });

    vm0.invoke(() -> createData(0, NUM_BUCKETS, "b"));

    // VM1 should recover, but it shouldn't host the bucket anymore
    vm1.invoke(() -> createPartitionedRegion(1, 0, 113, true));

    // The data shouldn't be affected.
    vm1.invoke(() -> checkData(0, NUM_BUCKETS, "b"));

    // restart everything, and make sure it comes back correctly.

    vm1.invoke(() -> getCache().close());
    vm0.invoke(() -> getCache().close());
    vm2.invoke(() -> getCache().close());

    AsyncInvocation<Void> createPartitionedRegionOnVM1 =
        vm1.invokeAsync(() -> createPartitionedRegion(1, 0, 113, true));
    AsyncInvocation<Void> createPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createPartitionedRegion(1, 0, 113, true));

    // Make sure we wait for vm2, because it's got the latest copy of the bucket
    createPartitionedRegionOnVM1.join(SECONDS.toMillis(1));
    assertThat(createPartitionedRegionOnVM1.isAlive()).isTrue();

    AsyncInvocation<Void> createPartitionedRegionOnVM2 =
        vm2.invokeAsync(() -> createPartitionedRegion(1, 0, 113, true));

    createPartitionedRegionOnVM2.await(2, MINUTES);
    createPartitionedRegionOnVM0.await(2, MINUTES);
    createPartitionedRegionOnVM1.await(2, MINUTES);

    // The data shouldn't be affected.
    vm1.invoke(() -> checkData(0, NUM_BUCKETS, "b"));
    assertThat(vm1.invoke(() -> getBucketList())).isEmpty();
    assertThat(vm0.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM0);
    assertThat(vm2.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM0);
  }

  /**
   * Test the with redundancy 1, we restore the same buckets when the missing member comes back
   * online.
   */
  @Test
  public void restoresSameBucketsWhenMissingDiskStoreReturnsOnline() {
    vm0.invoke(() -> createPartitionedRegion(1, 0, 113, true));
    vm1.invoke(() -> createPartitionedRegion(1, 0, 113, true));

    vm0.invoke(() -> createData(0, NUM_BUCKETS, "a"));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());

    vm1.invoke(() -> getCache().close());

    // This should work, because this bucket is still available.
    vm0.invoke(() -> {
      checkData(0, NUM_BUCKETS, "a");

      Region<Integer, String> region = getCache().getRegion(partitionedRegionName);
      for (int i = 0; i < NUM_BUCKETS / 2; i++) {
        region.destroy(i);
      }

      createData(NUM_BUCKETS / 2, NUM_BUCKETS, "b");
    });

    // This shouldn't create any buckets, because we know there are offline copies
    vm2.invoke(() -> createPartitionedRegion(1, 0, 113, true));

    Set<Integer> bucketsOnVM2 = vm2.invoke(() -> getBucketList());
    assertThat(bucketsOnVM2).isEmpty();

    vm1.invoke(() -> {
      createPartitionedRegion(1, 0, 113, true);

      // The data should be back online now. vm1 should have received the latest copy of the data.
      checkData(0, NUM_BUCKETS / 2, null);
      checkData(NUM_BUCKETS / 2, NUM_BUCKETS, "b");
    });

    // Make sure we restored the buckets in the right place
    assertThat(vm0.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM0);
    assertThat(vm1.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM1);
    assertThat(vm2.invoke(() -> getBucketList())).isEmpty();
  }

  @Test
  public void bucketCanBeMovedMultipleTimes() throws Exception {
    int redundancy = 0;

    vm0.invoke(() -> {
      createPartitionedRegion(redundancy, -1, 113, true);
      createData(0, 2, "a");
    });

    vm1.invoke(() -> createPartitionedRegion(redundancy, -1, 113, true));
    vm2.invoke(() -> createPartitionedRegion(redundancy, -1, 113, true));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM2 = vm2.invoke(() -> getBucketList());

    InternalDistributedMember memberVM0 = vm0.invoke(() -> getInternalDistributedMember());
    InternalDistributedMember memberVM1 = vm1.invoke(() -> getInternalDistributedMember());
    InternalDistributedMember memberVM2 = vm2.invoke(() -> getInternalDistributedMember());

    vm1.invoke(() -> moveBucketToMe(0, memberVM0));
    vm2.invoke(() -> moveBucketToMe(0, memberVM1));
    vm0.invoke(() -> createData(113, 114, "a"));
    vm0.invoke(() -> moveBucketToMe(0, memberVM2));

    vm0.invoke(() -> createData(226, 227, "a"));

    assertThat(vm0.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM0);
    assertThat(vm1.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM1);
    assertThat(vm2.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM2);

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());
    vm2.invoke(() -> getCache().close());

    AsyncInvocation<Void> createPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createPartitionedRegion(redundancy, -1, 113, true));
    AsyncInvocation<Void> createPartitionedRegionOnVM1 =
        vm1.invokeAsync(() -> createPartitionedRegion(redundancy, -1, 113, true));
    AsyncInvocation<Void> createPartitionedRegionOnVM2 =
        vm2.invokeAsync(() -> createPartitionedRegion(redundancy, -1, 113, true));

    createPartitionedRegionOnVM0.await(2, MINUTES);
    createPartitionedRegionOnVM1.await(2, MINUTES);
    createPartitionedRegionOnVM2.await(2, MINUTES);

    assertThat(vm2.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM2);
    assertThat(vm1.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM1);
    assertThat(vm0.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM0);

    vm0.invoke(() -> {
      checkData(0, 2, "a");
      checkData(113, 114, "a");
      checkData(226, 227, "a");
    });
  }

  @Test
  public void recoversAfterCleanStop() throws Exception {
    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    vm0.invoke(() -> createData(0, 1, "a"));

    vm1.invoke(() -> fakeCleanShutdown(0));
    vm0.invoke(() -> fakeCleanShutdown(0));

    AsyncInvocation<Void> createPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createPartitionedRegion(1, -1, 113, true));
    // Note: Make sure that vm0 is waiting for vm1 and vm2 to recover
    // If VM(0) recovers early, that is a problem, because we can now longer do a clean restart
    createPartitionedRegionOnVM0.join(SECONDS.toMillis(1));
    assertThat(createPartitionedRegionOnVM0.isAlive()).isTrue();

    AsyncInvocation<Void> createPartitionedRegionOnVM1 =
        vm1.invokeAsync(() -> createPartitionedRegion(1, -1, 113, true));

    createPartitionedRegionOnVM0.await(2, MINUTES);
    createPartitionedRegionOnVM1.await(2, MINUTES);

    vm0.invoke(() -> checkData(0, 1, "a"));
    vm1.invoke(() -> checkData(0, 1, "a"));

    vm0.invoke(() -> checkRecoveredFromDisk(0, true));
    vm1.invoke(() -> checkRecoveredFromDisk(0, true));

    vm0.invoke(() -> getCache().getRegion(partitionedRegionName).close());
    vm1.invoke(() -> getCache().getRegion(partitionedRegionName).close());

    createPartitionedRegionOnVM0 = vm0.invokeAsync(() -> createPartitionedRegion(1, -1, 113, true));
    createPartitionedRegionOnVM1 = vm1.invokeAsync(() -> createPartitionedRegion(1, -1, 113, true));

    createPartitionedRegionOnVM0.await(2, MINUTES);
    createPartitionedRegionOnVM1.await(2, MINUTES);

    vm0.invoke(() -> checkData(0, 1, "a"));
    vm1.invoke(() -> checkData(0, 1, "a"));

    vm0.invoke(() -> checkRecoveredFromDisk(0, false));
    vm1.invoke(() -> checkRecoveredFromDisk(0, true));
  }

  /**
   * This test is in here just to test to make sure that we don't get a suspect string with an
   * exception during cache closure.
   */
  @Test
  public void doesNotLogSuspectStringDuringCacheClosure() {
    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION_OVERFLOW);
    regionFactory.setEvictionAttributes(createLRUEntryAttributes(50, OVERFLOW_TO_DISK));

    regionFactory.create(partitionedRegionName);

    getCache().close();
  }

  @Test
  public void partitionedRegionsCanBeNested() throws Exception {
    vm0.invoke(() -> createNestedPartitionedRegion());
    vm1.invoke(() -> createNestedPartitionedRegion());
    vm2.invoke(() -> createNestedPartitionedRegion());

    int numBuckets = 50;
    vm0.invoke(() -> {
      createDataFor(0, numBuckets, "a", parentRegion1Name + SEPARATOR + partitionedRegionName);
      createDataFor(0, numBuckets, "b", parentRegion2Name + SEPARATOR + partitionedRegionName);
    });

    vm2.invoke(() -> {
      checkDataFor(0, numBuckets, "a", parentRegion1Name + SEPARATOR + partitionedRegionName);
      checkDataFor(0, numBuckets, "b", parentRegion2Name + SEPARATOR + partitionedRegionName);
    });

    Set<Integer> region1BucketsOnVM0 =
        vm0.invoke(() -> getBucketListFor(parentRegion1Name + SEPARATOR + partitionedRegionName));
    Set<Integer> region1BucketsOnVM1 =
        vm1.invoke(() -> getBucketListFor(parentRegion1Name + SEPARATOR + partitionedRegionName));
    Set<Integer> region1BucketsOnVM2 =
        vm2.invoke(() -> getBucketListFor(parentRegion1Name + SEPARATOR + partitionedRegionName));

    Set<Integer> region2BucketsOnVM0 =
        vm0.invoke(() -> getBucketListFor(parentRegion2Name + SEPARATOR + partitionedRegionName));
    Set<Integer> region2BucketsOnVM1 =
        vm1.invoke(() -> getBucketListFor(parentRegion2Name + SEPARATOR + partitionedRegionName));
    Set<Integer> region2BucketsOnVM2 =
        vm2.invoke(() -> getBucketListFor(parentRegion2Name + SEPARATOR + partitionedRegionName));

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());
    vm2.invoke(() -> getCache().close());

    AsyncInvocation<Void> createNestedPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createNestedPartitionedRegion());
    // Note: Make sure that vm0 is waiting for vm1 and vm2 to recover
    // If VM(0) recovers early, that is a problem, because vm1 has newer data
    createNestedPartitionedRegionOnVM0.join(SECONDS.toMillis(1));
    assertThat(createNestedPartitionedRegionOnVM0.isAlive()).isTrue();

    AsyncInvocation<Void> createNestedPartitionedRegionOnVM1 =
        vm1.invokeAsync(() -> createNestedPartitionedRegion());
    AsyncInvocation<Void> createNestedPartitionedRegionOnVM2 =
        vm2.invokeAsync(() -> createNestedPartitionedRegion());

    createNestedPartitionedRegionOnVM0.await(2, MINUTES);
    createNestedPartitionedRegionOnVM1.await(2, MINUTES);
    createNestedPartitionedRegionOnVM2.await(2, MINUTES);

    assertThat(
        vm0.invoke(() -> getBucketListFor(parentRegion1Name + SEPARATOR + partitionedRegionName)))
            .isEqualTo(region1BucketsOnVM0);
    assertThat(
        vm1.invoke(() -> getBucketListFor(parentRegion1Name + SEPARATOR + partitionedRegionName)))
            .isEqualTo(region1BucketsOnVM1);
    assertThat(
        vm2.invoke(() -> getBucketListFor(parentRegion1Name + SEPARATOR + partitionedRegionName)))
            .isEqualTo(region1BucketsOnVM2);

    assertThat(
        vm0.invoke(() -> getBucketListFor(parentRegion2Name + SEPARATOR + partitionedRegionName)))
            .isEqualTo(region2BucketsOnVM0);
    assertThat(
        vm1.invoke(() -> getBucketListFor(parentRegion2Name + SEPARATOR + partitionedRegionName)))
            .isEqualTo(region2BucketsOnVM1);
    assertThat(
        vm2.invoke(() -> getBucketListFor(parentRegion2Name + SEPARATOR + partitionedRegionName)))
            .isEqualTo(region2BucketsOnVM2);

    vm0.invoke(() -> {
      checkDataFor(0, numBuckets, "a", parentRegion1Name + SEPARATOR + partitionedRegionName);
      checkDataFor(0, numBuckets, "b", parentRegion2Name + SEPARATOR + partitionedRegionName);
    });

    vm1.invoke(() -> {
      createDataFor(numBuckets, 113, "c", parentRegion1Name + SEPARATOR + partitionedRegionName);
      createDataFor(numBuckets, 113, "d", parentRegion2Name + SEPARATOR + partitionedRegionName);
    });

    vm2.invoke(() -> {
      checkDataFor(numBuckets, 113, "c", parentRegion1Name + SEPARATOR + partitionedRegionName);
      checkDataFor(numBuckets, 113, "d", parentRegion2Name + SEPARATOR + partitionedRegionName);
    });
  }

  @Test
  public void recoversFromCloseDuringRegionOperation() throws Exception {
    vm0.invoke(() -> createPartitionedRegion(1, -1, 1, true));
    vm1.invoke(() -> createPartitionedRegion(1, -1, 1, true));

    vm1.invoke(() -> {
      Region<Integer, Integer> region = getCache().getRegion(partitionedRegionName);
      region.put(0, -1);
    });

    // Try to make sure there are some operations in flight while closing the cache
    AsyncInvocation<Integer> createPartitionedRegionWithPutsOnVM0 = vm0.invokeAsync(() -> {
      Region<Integer, Integer> region = getCache().getRegion(partitionedRegionName);

      int i = 0;
      while (true) {
        try {
          region.put(0, i);
          i++;
        } catch (CacheClosedException e) {
          break;
        } catch (DistributedSystemDisconnectedException e) {
          // remove this check when GEODE-5457 is resolved
          break;
        }
      }

      return i - 1;
    });

    vm0.invoke(() -> {
      Region<Integer, Integer> region = getCache().getRegion(partitionedRegionName);
      await().until(() -> region.get(0) > 0);
    });
    vm1.invoke(() -> {
      Region<Integer, Integer> region = getCache().getRegion(partitionedRegionName);
      await().until(() -> region.get(0) > 0);
    });

    AsyncInvocation<Void> closeCacheOnVM0 = vm0.invokeAsync(() -> getCache().close());
    AsyncInvocation<Void> closeCacheOnVM1 = vm1.invokeAsync(() -> getCache().close());

    // wait for the close to finish
    closeCacheOnVM0.await(2, MINUTES);
    closeCacheOnVM1.await(2, MINUTES);

    int lastValue = createPartitionedRegionWithPutsOnVM0.get();

    AsyncInvocation<Void> createPartitionedRegionOnVM0 =
        vm0.invokeAsync(() -> createPartitionedRegion(1, -1, 1, true));
    AsyncInvocation<Void> createPartitionedRegionOnVM1 =
        vm1.invokeAsync(() -> createPartitionedRegion(1, -1, 1, true));

    createPartitionedRegionOnVM0.await(2, MINUTES);
    createPartitionedRegionOnVM1.await(2, MINUTES);

    int valueOnVM0 = vm0.invoke(() -> {
      Region<Integer, Integer> region = getCache().getRegion(partitionedRegionName);
      return region.get(0);
    });
    int valueOnVM1 = vm1.invoke(() -> {
      Region<Integer, Integer> region = getCache().getRegion(partitionedRegionName);
      return region.get(0);
    });

    assertThat(valueOnVM1).isEqualTo(valueOnVM0);
    assertThat(valueOnVM0 == lastValue || valueOnVM0 == lastValue + 1)
        .as("value = " + valueOnVM0 + ", lastValue=" + lastValue).isTrue();
  }

  /**
   * A test to make sure that we allow the PR to be used after at least one copy of every bucket is
   * recovered, but before the secondaries are initialized.
   */
  @Test
  public void regionIsUsableBeforeRedundancyRecovery() throws Exception {
    int redundancy = 1;
    int numBuckets = 20;

    vm0.invoke(() -> createPartitionedRegion(redundancy, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(redundancy, -1, 113, true));
    vm2.invoke(() -> createPartitionedRegion(redundancy, -1, 113, true));

    vm0.invoke(() -> createData(0, numBuckets, "a"));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM2 = vm2.invoke(() -> getBucketList());

    vm0.invoke(() -> getCache().close());
    vm1.invoke(() -> getCache().close());
    vm2.invoke(() -> getCache().close());

    try {
      vm0.invoke(() -> slowGII());
      vm1.invoke(() -> slowGII());
      vm2.invoke(() -> slowGII());

      AsyncInvocation<Void> createPartitionedRegionOnVM0 =
          vm0.invokeAsync(() -> createPartitionedRegionWithPersistence(redundancy));
      AsyncInvocation<Void> createPartitionedRegionOnVM1 =
          vm1.invokeAsync(() -> createPartitionedRegionWithPersistence(redundancy));
      AsyncInvocation<Void> createPartitionedRegionOnVM2 =
          vm2.invokeAsync(() -> createPartitionedRegionWithPersistence(redundancy));

      createPartitionedRegionOnVM0.await(2, MINUTES);
      createPartitionedRegionOnVM1.await(2, MINUTES);
      createPartitionedRegionOnVM2.await(2, MINUTES);

      // Make sure all of the primary are available.
      vm0.invoke(() -> {
        checkData(0, numBuckets, "a");
        createData(113, 113 + numBuckets, "b");
      });

      // But none of the secondaries
      Set<Integer> initialBucketsOnVM0 = vm0.invoke(() -> getBucketList());
      Set<Integer> initialBucketsOnVM1 = vm1.invoke(() -> getBucketList());
      Set<Integer> initialBucketsOnVM2 = vm2.invoke(() -> getBucketList());

      assertThat(
          initialBucketsOnVM0.size() + initialBucketsOnVM1.size() + initialBucketsOnVM2.size())
              .as("vm0=" + initialBucketsOnVM0 + ",vm1=" + initialBucketsOnVM1 + "vm2="
                  + initialBucketsOnVM2)
              .isEqualTo(numBuckets);
    } finally {
      // Reset the slow GII flag, and wait for the redundant buckets to be recovered.
      AsyncInvocation<Void> resetSlowGiiOnVM0 = vm0.invokeAsync(() -> resetSlowGII());
      AsyncInvocation<Void> resetSlowGiiOnVM1 = vm1.invokeAsync(() -> resetSlowGII());
      AsyncInvocation<Void> resetSlowGiiOnVM2 = vm2.invokeAsync(() -> resetSlowGII());

      resetSlowGiiOnVM0.await(2, MINUTES);
      resetSlowGiiOnVM1.await(2, MINUTES);
      resetSlowGiiOnVM2.await(2, MINUTES);
    }

    // Now we better have all of the buckets
    assertThat(vm0.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM0);
    assertThat(vm1.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM1);
    assertThat(vm2.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM2);

    // Make sure the members see the data recovered from disk in those secondary buckets
    vm0.invoke(() -> checkData(0, numBuckets, "a"));
    vm1.invoke(() -> checkData(0, numBuckets, "a"));

    // Make sure the members see the new updates in those secondary buckets
    vm0.invoke(() -> checkData(113, 113 + numBuckets, "b"));
    vm1.invoke(() -> checkData(113, 113 + numBuckets, "b"));
  }


  @Test
  public void cacheIsClosedWhenConflictingPersistentDataExceptionIsThrown() throws Exception {
    vm0.invoke(() -> {
      createPartitionedRegion(0, -1, 113, true);
      // create some buckets
      createData(0, 2, "a");
      getCache().getRegion(partitionedRegionName).close();
    });

    vm1.invoke(() -> createPartitionedRegion(0, -1, 113, true));
    // create an overlapping bucket
    vm1.invoke(() -> createData(0, 2, "a"));

    try (IgnoredException ie1 = addIgnoredException(CacheClosedException.class);
        IgnoredException ie2 = addIgnoredException(ConflictingPersistentDataException.class)) {
      // This results in ConflictingPersistentDataException. As part of GEODE-2918, the cache is
      // closed when ConflictingPersistentDataException is encountered.
      vm0.invoke(() -> {
        Cache cache = getCache();
        Throwable expectedException =
            catchThrowable(() -> createPartitionedRegion(0, -1, 113, true));

        assertThat(thrownByDiskRecoveryDueToConflictingPersistentDataException(expectedException)
            || thrownByAsyncFlusherThreadDueToConflictingPersistentDataException(expectedException))
                .isTrue();

        // Wait for the cache to completely close
        cache.close();
      });
    }

    // if the following line fails then review the old try-catch block which used to be here
    vm1.invoke(() -> createData(0, 1, "a"));

    vm1.invoke(() -> getCache().getRegion(partitionedRegionName).close());

    // This should succeed, vm0 should not have persisted any view information from vm1
    vm0.invoke(() -> {
      createPartitionedRegion(0, -1, 113, true);
      checkData(0, 2, "a");
      checkData(2, 3, null);
    });
  }

  @Test
  public void cacheIsClosedWhenConflictingPersistentDataExceptionIsThrownAndRedundancyIsOne() {
    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    // create some buckets
    vm0.invoke(() -> createData(0, 2, "a"));
    vm0.invoke(() -> getCache().getRegion(partitionedRegionName).close());

    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    // create an overlapping bucket
    vm1.invoke(() -> createData(1, 2, "a"));

    addIgnoredException(ConflictingPersistentDataException.class);
    addIgnoredException(CacheClosedException.class);

    vm0.invoke(() -> {
      Throwable expectedException = catchThrowable(() -> createPartitionedRegion(1, -1, 113, true));

      assertThat(thrownByDiskRecoveryDueToConflictingPersistentDataException(expectedException)
          || thrownByAsyncFlusherThreadDueToConflictingPersistentDataException(expectedException))
              .isTrue();
    });
  }

  /**
   * Test to make sure that primaries are rebalanced after recovering from disk.
   */
  @Test
  public void primariesAreRebalancedAfterRecoveringFromDisk() {
    int numBuckets = 30;

    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    vm1.invoke(() -> createPartitionedRegion(1, -1, 113, true));
    vm2.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    vm0.invoke(() -> createData(0, numBuckets, "a"));

    Set<Integer> bucketsOnVM0 = vm0.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM1 = vm1.invoke(() -> getBucketList());
    Set<Integer> bucketsOnVM2 = vm2.invoke(() -> getBucketList());

    // We expect to see 10 primaries on each node since we have 30 buckets
    Set<Integer> primariesOnVM0 = vm0.invoke(() -> getPrimaryBucketList());
    assertThat(primariesOnVM0).hasSize(10);
    Set<Integer> primariesOnVM1 = vm1.invoke(() -> getPrimaryBucketList());
    assertThat(primariesOnVM1).hasSize(10);
    Set<Integer> primariesOnVM2 = vm2.invoke(() -> getPrimaryBucketList());
    assertThat(primariesOnVM2).hasSize(10);

    // bounce vm0
    vm0.invoke(() -> getCache().close());
    vm0.invoke(() -> createPartitionedRegion(1, -1, 113, true));

    vm0.invoke(() -> waitForBucketRecovery(bucketsOnVM0));
    assertThat(vm0.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM0);
    assertThat(vm1.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM1);
    assertThat(vm2.invoke(() -> getBucketList())).isEqualTo(bucketsOnVM2);

    /*
     * Though we make best effort to get the primaries evenly distributed after bouncing the VM. In
     * some instances one primary could end up with 9 primaries such as GEODE-1056. And as asserts
     * fail fast we don`t get to verify if other vm`s end up having 11 primaries. So rather than
     * asserting for 10 primaries in each VM, try asserting total primaries.
     */
    primariesOnVM0 = vm0.invoke(() -> getPrimaryBucketList());
    primariesOnVM1 = vm1.invoke(() -> getPrimaryBucketList());
    primariesOnVM2 = vm2.invoke(() -> getPrimaryBucketList());
    int totalPrimaries = primariesOnVM0.size() + primariesOnVM1.size() + primariesOnVM2.size();
    assertThat(totalPrimaries).isEqualTo(numBuckets);

    /*
     * As worst case the primaries should be 1 less than evenly being distributed, so assert
     * primaries to be between 9 and 11 (both inclusive).
     */
    assertThat(primariesOnVM0.size()).isBetween(9, 11);
    assertThat(primariesOnVM0.size()).isBetween(9, 11);
    assertThat(primariesOnVM0.size()).isBetween(9, 11);
  }

  @Test
  public void partitionedRegionProxySupportsConcurrencyChecksEnabled() {
    vm0.invoke(() -> createPRProxy());
    vm1.invoke(() -> createPRWithConcurrencyChecksEnabled());
    vm2.invoke(() -> createPRWithConcurrencyChecksEnabled());
    vm3.invoke(() -> createPRProxy());

    vm0.invoke(() -> {
      Region<?, ?> region = getCache().getRegion(partitionedRegionName);
      assertThat(region.getAttributes().getConcurrencyChecksEnabled()).isTrue();
    });
    vm3.invoke(() -> {
      Region<?, ?> region = getCache().getRegion(partitionedRegionName);
      assertThat(region.getAttributes().getConcurrencyChecksEnabled()).isTrue();
    });
  }

  @Test
  public void proxyRegionsAllowedWithPersistentPartitionedRegions() {
    vm1.invoke(() -> {
      getCache().createRegionFactory(PARTITION_PROXY).create(partitionedRegionName);
    });
    vm2.invoke(() -> {
      Region<?, ?> region =
          getCache().createRegionFactory(PARTITION_PERSISTENT).create(partitionedRegionName);
      assertThat(region.getAttributes().getConcurrencyChecksEnabled()).isTrue();
    });
    vm3.invoke(() -> {
      getCache().createRegionFactory(PARTITION_PROXY).create(partitionedRegionName);
    });

    vm1.invoke(() -> {
      Region<?, ?> region = getCache().getRegion(partitionedRegionName);
      assertThat(region.getAttributes().getConcurrencyChecksEnabled()).isTrue();
    });
    vm3.invoke(() -> {
      Region<?, ?> region = getCache().getRegion(partitionedRegionName);
      assertThat(region.getAttributes().getConcurrencyChecksEnabled()).isTrue();
    });
  }

  @Test
  public void replicateAllowedWithPersistentReplicates() {
    vm1.invoke(() -> {
      getCache().createRegionFactory(REPLICATE_PERSISTENT).create(partitionedRegionName);
    });
    vm2.invoke(() -> {
      assertThatCode(() -> getCache().createRegionFactory(REPLICATE).create(partitionedRegionName))
          .doesNotThrowAnyException();
    });
    vm3.invoke(() -> {
      assertThatCode(
          () -> getCache().createRegionFactory(REPLICATE_PERSISTENT).create(partitionedRegionName))
              .doesNotThrowAnyException();
    });
  }

  @Test
  public void rebalanceWithMembersOfflineDoesNotResultInMissingDiskStores() throws Exception {
    int numBuckets = 12;

    vm0.invoke(() -> createPartitionedRegion(1, -1, numBuckets, true));
    vm1.invoke(() -> createPartitionedRegion(1, -1, numBuckets, true));
    vm2.invoke(() -> createPartitionedRegion(1, -1, numBuckets, true));
    vm3.invoke(() -> createPartitionedRegion(1, -1, numBuckets, true));

    vm0.invoke(() -> createData(0, numBuckets, "a"));

    // Stop vm0, rebalance, stop vm1, rebalance
    vm0.invoke(() -> getCache().close());
    rebalance(vm3);
    vm1.invoke(() -> getCache().close());
    rebalance(vm3);

    // Check missing disk stores (should include vm0 and vm1)
    Set<PersistentID> missingMembers = getMissingPersistentMembers(vm3);
    assertThat(missingMembers).hasSize(2);

    vm1.invoke(() -> createPartitionedRegion(1, -1, numBuckets, true));
    vm0.invoke(() -> createPartitionedRegion(1, -1, numBuckets, true));


    missingMembers = getMissingPersistentMembers(vm3);
    assertThat(missingMembers).isEmpty();
  }

  /**
   * The purpose of this test is to ensure no data loss occurs when a secondary fails to get
   * updated bucket images due to failed GII. We want to ensure the secondary's disk region
   * is not unintentially deleted when the GII fails.
   *
   * Tests the following scenario to ensure data is not lost:
   * 1. Create a persistent partitioned region on server 1 with redundancy 1
   * (Redundancy will not be satisfied initially)
   * 2. Add data to the region
   * 3. Create the region on server 2 which will cause creation of secondary buckets
   * 4. Close the cache on server 2.
   * 5. Install a DistributedMessageObserver which forces a cache close
   * during a bucket move from server 1 to server 2.
   * 6. Restart server 2. The listener installed in step 2 will cause
   * server 1 to crash when server 2 requests the new bucket images.
   * 7. Revoke server 1's persistent membership
   * 8. Perform gets on server 2 to ensure data consistency after it takes over as primary
   */
  @Test
  public void testCacheCloseDuringBucketMoveDoesntCauseDataLoss()
      throws ExecutionException, InterruptedException {
    int redundantCopies = 1;
    int recoveryDelay = -1;
    int numBuckets = 6;
    boolean diskSynchronous = true;

    vm0.invoke(() -> {
      createPartitionedRegion(redundantCopies, recoveryDelay, numBuckets, diskSynchronous);
      createData(0, numBuckets, "a");
    });

    vm1.invoke(() -> {
      createPartitionedRegion(redundantCopies, recoveryDelay, numBuckets, diskSynchronous);
      getCache().close();
    });

    vm0.invoke(() -> {
      DistributionMessageObserver
          .setInstance(new CacheClosingDistributionMessageObserver(
              "_B__" + partitionedRegionName + "_", getCache()));
    });

    // Need to invoke this async because vm1 will wait for vm0 to come back online
    // unless we explicitly revoke it.
    AsyncInvocation<Object> createRegionAsync = vm1.invokeAsync(() -> {
      createPartitionedRegion(redundantCopies, recoveryDelay, numBuckets, diskSynchronous);
    });

    vm1.invoke(() -> {
      revokeKnownMissingMembers(1);
      for (int i = 0; i < numBuckets; ++i) {
        assertEquals(getCache().getRegion(partitionedRegionName).get(i), "a");
      }
    });

    createRegionAsync.get();
  }

  private void rebalance(VM vm) {
    vm.invoke(() -> getCache().getResourceManager().createRebalanceFactory().start().getResults());
  }

  private void revokePersistentMember(PersistentID missingMember, VM vm) {
    vm.invoke(() -> AdminDistributedSystemImpl
        .revokePersistentMember(getCache().getDistributionManager(), missingMember.getUUID()));
  }

  private Set<PersistentID> getMissingPersistentMembers(VM vm) {
    return vm.invoke(() -> AdminDistributedSystemImpl
        .getMissingPersistentMembers(getCache().getDistributionManager()));
  }

  private void moveBucketToMe(final int bucketId, final InternalDistributedMember sourceMember) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(partitionedRegionName);
    assertThat(region.getDataStore().moveBucket(bucketId, sourceMember, false)).isTrue();
  }

  private InternalDistributedMember getInternalDistributedMember() {
    return getCache().getInternalDistributedSystem().getDistributedMember();
  }

  private void createPRProxy() {
    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION_PROXY);
    regionFactory.create(partitionedRegionName);
  }

  private void createPRWithConcurrencyChecksEnabled() {
    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
    Region<?, ?> region = regionFactory.create(partitionedRegionName);

    assertThat(region.getAttributes().getConcurrencyChecksEnabled()).isTrue();
  }

  private Set<Integer> getPrimaryBucketList() {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(partitionedRegionName);
    return new TreeSet<>(region.getDataStore().getAllLocalPrimaryBucketIds());
  }

  private void waitForBucketRecovery(final Set<Integer> lostBuckets) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(partitionedRegionName);
    PartitionedRegionDataStore dataStore = region.getDataStore();

    await().until(() -> lostBuckets.equals(dataStore.getAllLocalBucketIds()));
  }

  private void createPartitionedRegionWithPersistence(final int redundancy) {
    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundancy);
    partitionAttributesFactory.setRecoveryDelay(-1);
    partitionAttributesFactory.setTotalNumBuckets(113);
    partitionAttributesFactory.setLocalMaxMemory(500);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskSynchronous(true);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(partitionedRegionName);
  }

  private void resetSlowGII() throws InterruptedException {
    BlockGIIMessageObserver messageObserver =
        (BlockGIIMessageObserver) DistributionMessageObserver.setInstance(null);
    RecoveryObserver recoveryObserver =
        (RecoveryObserver) InternalResourceManager.getResourceObserver();
    messageObserver.unblock();
    recoveryObserver.await(2, MINUTES);
    InternalResourceManager.setResourceObserver(null);
  }

  private void slowGII() {
    InternalResourceManager.setResourceObserver(new RecoveryObserver(partitionedRegionName));
    DistributionMessageObserver.setInstance(new BlockGIIMessageObserver());
  }

  private void createNestedPartitionedRegion() throws InterruptedException {
    // Wait for both nested PRs to be created
    CountDownLatch recoveryDone = new CountDownLatch(2);

    ResourceObserver observer = new ResourceObserverAdapter() {
      @Override
      public void recoveryFinished(final Region region) {
        recoveryDone.countDown();
      }
    };

    InternalResourceManager.setResourceObserver(observer);

    RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(REPLICATE);

    Region<?, ?> parentRegion1 = regionFactory.create(parentRegion1Name);
    Region<?, ?> parentRegion2 = regionFactory.create(parentRegion2Name);

    PartitionAttributesFactory<?, ?> partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(1);

    RegionFactory<?, ?> regionFactoryPR =
        getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactoryPR.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactoryPR.createSubregion(parentRegion1, partitionedRegionName);
    regionFactoryPR.createSubregion(parentRegion2, partitionedRegionName);

    recoveryDone.await(2, MINUTES);
  }

  private void fakeCleanShutdown(final int bucketId) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(partitionedRegionName);
    DiskRegion disk = region.getRegionAdvisor().getBucket(bucketId).getDiskRegion();
    for (PersistentMemberID id : disk.getOnlineMembers()) {
      disk.memberOfflineAndEqual(id);
    }
    for (PersistentMemberID id : disk.getOfflineMembers()) {
      disk.memberOfflineAndEqual(id);
    }
    getCache().close();
  }

  private void checkReadWriteOperationsWithOfflineMember(final int bucketOnVM0,
      final int bucketOnVM1) {
    // This should work, because this bucket is still available.
    checkData(bucketOnVM0, bucketOnVM0 + 1, "a");

    assertThatThrownBy(() -> checkData(bucketOnVM1, bucketOnVM1 + 1, null))
        .isInstanceOf(PartitionOfflineException.class);

    Region<?, ?> region = getCache().getRegion(partitionedRegionName);

    assertThatThrownBy(() -> FunctionService.onRegion(region).execute(new TestFunction()))
        .isInstanceOf(PartitionOfflineException.class);

    // This should work, because this bucket is still available.
    assertThatCode(() -> FunctionService.onRegion(region)
        .withFilter(Collections.singleton(bucketOnVM0)).execute(new TestFunction()))
            .doesNotThrowAnyException();

    // This should fail, because this bucket is offline
    assertThatThrownBy(() -> FunctionService.onRegion(region)
        .withFilter(Collections.singleton(bucketOnVM1)).execute(new TestFunction()))
            .isInstanceOf(PartitionOfflineException.class);

    // This should fail, because a bucket is offline
    Set<Integer> filter = new HashSet<>();
    filter.add(bucketOnVM0);
    filter.add(bucketOnVM1);
    assertThatThrownBy(
        () -> FunctionService.onRegion(region).withFilter(filter).execute(new TestFunction()))
            .isInstanceOf(PartitionOfflineException.class);

    // This should fail, because a bucket is offline
    assertThatThrownBy(() -> FunctionService.onRegion(region).execute(new TestFunction()))
        .isInstanceOf(PartitionOfflineException.class);

    assertThatThrownBy(() -> getCache().getQueryService()
        .newQuery("select * from /" + partitionedRegionName).execute())
            .isInstanceOf(PartitionOfflineException.class);

    Set<?> keys = region.keySet();
    assertThatThrownBy(() -> {
      for (Iterator iterator = keys.iterator(); iterator.hasNext();) {
        Object key = iterator.next();
        // nothing
      }
    }).isInstanceOf(PartitionOfflineException.class);

    Collection<?> values = region.values();
    assertThatThrownBy(() -> {
      for (Iterator iterator = values.iterator(); iterator.hasNext();) {
        Object value = iterator.next();
        // nothing
      }
    }).isInstanceOf(PartitionOfflineException.class);

    Set<?> entries = region.entrySet();
    assertThatThrownBy(() -> {
      for (Iterator iterator = entries.iterator(); iterator.hasNext();) {
        Object entry = iterator.next();
        // nothing
      }
    }).isInstanceOf(PartitionOfflineException.class);

    assertThatThrownBy(() -> region.get(bucketOnVM1)).isInstanceOf(PartitionOfflineException.class);

    assertThatThrownBy(() -> region.containsKey(bucketOnVM1))
        .isInstanceOf(PartitionOfflineException.class);

    assertThatThrownBy(() -> region.getEntry(bucketOnVM1))
        .isInstanceOf(PartitionOfflineException.class);

    assertThatThrownBy(() -> region.invalidate(bucketOnVM1))
        .isInstanceOf(PartitionOfflineException.class);

    assertThatThrownBy(() -> region.destroy(bucketOnVM1))
        .isInstanceOf(PartitionOfflineException.class);

    assertThatThrownBy(() -> createData(bucketOnVM1, bucketOnVM1 + 1, "b"))
        .isInstanceOf(PartitionOfflineException.class);
  }

  private void checkRecoveredFromDisk(final int bucketId, boolean recoveredLocally) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(partitionedRegionName);
    DiskRegion disk = region.getRegionAdvisor().getBucket(bucketId).getDiskRegion();
    if (recoveredLocally) {
      assertThat(disk.getStats().getRemoteInitializations()).isEqualTo(0);
      assertThat(disk.getStats().getLocalInitializations()).isEqualTo(1);
    } else {
      assertThat(disk.getStats().getRemoteInitializations()).isEqualTo(1);
      assertThat(disk.getStats().getLocalInitializations()).isEqualTo(0);
    }
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

    RegionFactory<?, ?> regionFactory =
        getCache().createRegionFactory(PARTITION_PERSISTENT);
    regionFactory.setDiskSynchronous(synchronous);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(partitionedRegionName);

    recoveryDone.await(2, MINUTES);
  }

  private void createData(final int startKey, final int endKey, final String value) {
    createDataFor(startKey, endKey, value, partitionedRegionName);
  }

  private void createDataFor(final int startKey, final int endKey, final String value,
      final String regionName) {
    Region<Integer, String> region = getCache().getRegion(regionName);
    for (int i = startKey; i < endKey; i++) {
      region.put(i, value);
    }
  }

  private Set<Integer> getBucketList() {
    return getBucketListFor(partitionedRegionName);
  }

  private Set<Integer> getBucketListFor(final String regionName) {
    PartitionedRegion region = (PartitionedRegion) getCache().getRegion(regionName);
    return new TreeSet<>(region.getDataStore().getAllLocalBucketIds());
  }

  private void checkData(final int startKey, final int endKey, final String value) {
    checkDataFor(startKey, endKey, value, partitionedRegionName);
  }

  private void checkDataFor(final int startKey, final int endKey, final String value,
      final String regionName) {
    Region<?, ?> region = getCache().getRegion(regionName);
    for (int i = startKey; i < endKey; i++) {
      assertThat(region.get(i)).isEqualTo(value);
    }
  }

  private void revokeKnownMissingMembers(final int numExpectedMissing)
      throws AdminException, InterruptedException {
    DistributedSystemConfig config = defineDistributedSystem(getSystem(), "");
    AdminDistributedSystem adminDS = getDistributedSystem(config);
    adminDS.connect();
    try {
      adminDS.waitToBeConnected(MINUTES.toMillis(2));

      await().until(() -> {
        Set<PersistentID> missingIds = adminDS.getMissingPersistentMembers();
        if (missingIds.size() != numExpectedMissing) {
          return false;
        }
        for (PersistentID missingId : missingIds) {
          adminDS.revokePersistentMember(missingId.getUUID());
        }
        return true;
      });


    } finally {
      adminDS.disconnect();
    }
  }

  private static InetAddress getFirstInet4Address() throws SocketException, UnknownHostException {
    for (NetworkInterface netint : Collections.list(NetworkInterface.getNetworkInterfaces())) {
      for (InetAddress inetAddress : Collections.list(netint.getInetAddresses())) {
        if (inetAddress instanceof Inet4Address && !inetAddress.isLoopbackAddress()) {
          return inetAddress;
        }
      }
    }

    // If no INet4Address was found for any of the interfaces above, default to getLocalHost()
    return InetAddress.getLocalHost();
  }

  private InternalCache getCache() {
    return cacheRule.getOrCreateCache();
  }

  private InternalDistributedSystem getSystem() {
    return getCache().getInternalDistributedSystem();
  }

  private boolean thrownByDiskRecoveryDueToConflictingPersistentDataException(
      Throwable expectedException) {
    return expectedException instanceof CacheClosedException
        && expectedException.getCause() instanceof ConflictingPersistentDataException;
  }

  private boolean thrownByAsyncFlusherThreadDueToConflictingPersistentDataException(
      Throwable expectedException) {
    if (expectedException == null) {
      return false;
    }

    Throwable rootExceptionCause = expectedException.getCause();
    Throwable nestedExceptionCause = rootExceptionCause.getCause();

    return expectedException instanceof CacheClosedException
        && (rootExceptionCause instanceof CacheClosedException
            && rootExceptionCause.getMessage().contains(
                "Could not schedule asynchronous write because the flusher thread had been terminated"))
        && nestedExceptionCause instanceof ConflictingPersistentDataException;
  }

  private static class RecoveryObserver extends ResourceObserverAdapter {

    private final String partitionedRegionName;
    private final CountDownLatch recoveryDone = new CountDownLatch(1);

    RecoveryObserver(final String partitionedRegionName) {
      this.partitionedRegionName = partitionedRegionName;
    }

    @Override
    public void rebalancingOrRecoveryFinished(final Region region) {
      if (region.getName().equals(partitionedRegionName)) {
        recoveryDone.countDown();
      }
    }

    void await(final long timeout, final TimeUnit unit) throws InterruptedException {
      recoveryDone.await(timeout, unit);
    }
  }

  private static class TestFunction implements Function, Serializable {

    @Override
    public void execute(final FunctionContext context) {
      context.getResultSender().lastResult(null);
    }

    @Override
    public String getId() {
      return TestFunction.class.getSimpleName();
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  private static class BlockGIIMessageObserver extends DistributionMessageObserver {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void beforeSendMessage(final ClusterDistributionManager dm,
        final DistributionMessage message) {
      if (message instanceof RequestImageMessage) {
        RequestImageMessage requestImageMessage = (RequestImageMessage) message;
        // make sure this is a bucket region doing a GII
        if (requestImageMessage.regionPath.contains("B_")) {
          try {
            latch.await(2, MINUTES);
          } catch (InterruptedException e) {
            throw new Error(e);
          }
        }
      }
    }

    void unblock() {
      latch.countDown();
    }
  }
}
