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
package org.apache.geode.cache30;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.versions.VMVersionTag;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

/**
 * concurrency-control tests for client/server
 */

@SuppressWarnings("serial")
public class PRBucketSynchronizationDUnitTest extends CacheTestCase {

  private static LocalRegion testRegion;

  @After
  public void tearDown() {
    disconnectAllFromDS();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = super.getDistributedSystemProperties();
    config.put(ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION, "false");
    return config;
  }

  @Test
  public void testThatBucketSyncOnPrimaryLoss() {
    doBucketsSyncOnPrimaryLoss(TestType.IN_MEMORY);
  }

  @Test
  public void testThatBucketsSyncOnPrimaryLossWithPersistence() {
    doBucketsSyncOnPrimaryLoss(TestType.PERSISTENT);
  }

  @Test
  public void testThatBucketsSyncOnPrimaryLossWithOverflow() {
    doBucketsSyncOnPrimaryLoss(TestType.OVERFLOW);
  }

  /**
   * We hit this problem in bug #45669. A primary was lost and we did not see secondary buckets
   * perform a delta-GII.
   */
  private void doBucketsSyncOnPrimaryLoss(TestType typeOfTest) {
    IgnoredException.addIgnoredException("killing member's ds");
    IgnoredException.addIgnoredException("killing member's ds");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    Set<VM> verifyVMs = new HashSet<>();

    final String name = getUniqueName() + "Region";

    verifyVMs.add(vm0);
    verifyVMs.add(vm1);
    verifyVMs.add(vm2);

    createRegion(vm0, name, typeOfTest);
    createRegion(vm1, name, typeOfTest);
    createRegion(vm2, name, typeOfTest);

    createEntry1(vm0);

    VM primaryOwner;
    if (isPrimaryForBucket0(vm0)) {
      primaryOwner = vm0;
    } else if (isPrimaryForBucket0(vm1)) {
      primaryOwner = vm1;
    } else {
      primaryOwner = vm2;
    }

    verifyVMs.remove(primaryOwner);

    // cause one of the VMs to throw away the next operation
    VM creatorVM = null;
    InternalDistributedMember primaryID = getId(primaryOwner);
    VersionSource primaryVersionID = getVersionID(primaryOwner);
    for (VM vm : verifyVMs) {
      creatorVM = vm;
      createEntry2(creatorVM, primaryID, primaryVersionID);
      break;
    }

    verifyVMs.remove(creatorVM);

    // Now we crash the primary bucket owner simulating death during distribution.
    // The backup buckets should perform a delta-GII for the lost member and
    // get back in sync
    DistributedTestUtils.crashDistributedSystem(primaryOwner);

    for (VM vm : verifyVMs) {
      verifySynchronized(vm, primaryID);
    }
  }

  private void createEntry1(VM vm) {
    vm.invoke("create entry1", () -> {
      testRegion.create("Object1", 1);
    });
  }

  private boolean isPrimaryForBucket0(VM vm) {
    return vm.invoke("is primary?", () -> {
      PartitionedRegion pr = (PartitionedRegion) testRegion;
      return pr.getDataStore().getLocalBucketById(0).getBucketAdvisor().isPrimary();
    });
  }

  private InternalDistributedMember getId(VM vm) {
    return vm.invoke("get dmID", () -> {
      return testRegion.getCache().getMyId();
    });
  }

  private VersionSource getVersionID(VM vm) {
    return vm.invoke("get versionID", () -> {
      return testRegion.getVersionMember();
    });
  }

  private void createEntry2(VM vm, final InternalDistributedMember primary,
      final VersionSource primaryVersionID) {
    vm.invoke("create entry2", () -> {
      // create a fake event that looks like it came from the primary and apply it to this cache
      PartitionedRegion pr = (PartitionedRegion) testRegion;
      BucketRegion bucket = pr.getDataStore().getLocalBucketById(0);
      VersionTag tag = new VMVersionTag();
      tag.setMemberID(primaryVersionID);
      tag.setRegionVersion(2);
      tag.setEntryVersion(1);
      tag.setIsRemoteForTesting();
      EntryEventImpl event =
          EntryEventImpl.create(bucket, Operation.CREATE, "Object3", true, primary, true, false);
      event.setNewValue(new VMCachedDeserializable("value3", 12));
      event.setVersionTag(tag);
      bucket.getRegionMap().basicPut(event, System.currentTimeMillis(), true, false, null, false,
          false);
      event.release();

      // now create a tombstone so we can be sure these are transferred in delta-GII
      tag = new VMVersionTag();
      tag.setMemberID(primaryVersionID);
      tag.setRegionVersion(3);
      tag.setEntryVersion(1);
      tag.setIsRemoteForTesting();
      event =
          EntryEventImpl.create(bucket, Operation.CREATE, "Object5", true, primary, true, false);
      event.setNewValue(Token.TOMBSTONE);
      event.setVersionTag(tag);
      bucket.getRegionMap().basicPut(event, System.currentTimeMillis(), true, false, null, false,
          false);
      event.release();

      bucket.dumpBackingMap();
      assertTrue("bucket should hold entry Object3 now", bucket.containsKey("Object3"));
    });
  }

  private void verifySynchronized(VM vm, final InternalDistributedMember crashedMember) {
    vm.invoke("check that synchronization happened", () -> {
      PartitionedRegion pr = (PartitionedRegion) testRegion;
      final BucketRegion bucket = pr.getDataStore().getLocalBucketById(0);

      await().until(() -> {
        if (testRegion.getCache().getDistributionManager().isCurrentMember(crashedMember)) {
          return false;
        }
        if (!testRegion.containsKey("Object3")) {
          return false;
        }
        RegionEntry re = bucket.getRegionMap().getEntry("Object5");
        if (re == null) {
          return false;
        }
        if (!re.isTombstone()) {
          return false;
        }
        return true;
      });
    });
  }

  private void createRegion(VM vm, final String regionName, final TestType typeOfTest) {
    vm.invoke(() -> {
      AttributesFactory af = new AttributesFactory();
      af.setDataPolicy(DataPolicy.PARTITION);
      af.setPartitionAttributes(
          (new PartitionAttributesFactory()).setTotalNumBuckets(2).setRedundantCopies(3).create());
      switch (typeOfTest) {
        case IN_MEMORY:
          break;
        case PERSISTENT:
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          break;
        case OVERFLOW:
          af.setEvictionAttributes(
              EvictionAttributes.createLRUEntryAttributes(5, EvictionAction.OVERFLOW_TO_DISK));
          break;
      }
      testRegion = (LocalRegion) createRootRegion(regionName, af.create());
    });
  }

  enum TestType {
    IN_MEMORY, OVERFLOW, PERSISTENT
  }
}
