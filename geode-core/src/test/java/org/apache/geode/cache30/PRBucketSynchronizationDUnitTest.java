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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.distributed.*;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.*;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
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
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;

/**
 * concurrency-control tests for client/server
 * 
 *
 */
@Category(DistributedTest.class)
public class PRBucketSynchronizationDUnitTest extends JUnit4CacheTestCase {
  static enum TestType {
    IN_MEMORY, OVERFLOW, PERSISTENT
  };

  public static LocalRegion TestRegion;

  public PRBucketSynchronizationDUnitTest() {
    super();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    result.put(ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION, "false");
    return result;
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
  public void doBucketsSyncOnPrimaryLoss(TestType typeOfTest) {
    IgnoredException.addIgnoredException("killing member's ds");
    IgnoredException.addIgnoredException("killing member's ds");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    Set<VM> verifyVMs = new HashSet<VM>();

    final String name = this.getUniqueName() + "Region";

    verifyVMs.add(vm0);
    verifyVMs.add(vm1);
    verifyVMs.add(vm2);

    disconnectAllFromDS();
    try {
      createRegion(vm0, name, typeOfTest);
      createRegion(vm1, name, typeOfTest);
      createRegion(vm2, name, typeOfTest);

      createEntry1(vm0);

      VM primaryOwner;
      if (isPrimaryForBucket0(vm0))
        primaryOwner = vm0;
      else if (isPrimaryForBucket0(vm1))
        primaryOwner = vm1;
      else
        primaryOwner = vm2;

      verifyVMs.remove(primaryOwner);

      // cause one of the VMs to throw away the next operation
      VM creatorVM = null;
      InternalDistributedMember primaryID = getID(primaryOwner);
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
    } finally {
      disconnectAllFromDS();
    }
  }


  // private void closeCache(VM vm) {
  // vm.invoke(new SerializableCallable() {
  // public Object call() throws Exception {
  // closeCache();
  // return null;
  // }
  // });
  // }

  private boolean createEntry1(VM vm) {
    return (Boolean) vm.invoke(new SerializableCallable("create entry1") {
      public Object call() {
        TestRegion.create("Object1", Integer.valueOf(1));
        return true;
      }
    });
  }

  private boolean isPrimaryForBucket0(VM vm) {
    return (Boolean) vm.invoke(new SerializableCallable("is primary?") {
      public Object call() {
        PartitionedRegion pr = (PartitionedRegion) TestRegion;
        return pr.getDataStore().getLocalBucketById(0).getBucketAdvisor().isPrimary();
      }
    });
  }

  private InternalDistributedMember getID(VM vm) {
    return (InternalDistributedMember) vm.invoke(new SerializableCallable("get dmID") {
      public Object call() {
        return TestRegion.getCache().getMyId();
      }
    });
  }


  private VersionSource getVersionID(VM vm) {
    return (VersionSource) vm.invoke(new SerializableCallable("get versionID") {
      public Object call() {
        return TestRegion.getVersionMember();
      }
    });
  }

  private boolean createEntry2(VM vm, final InternalDistributedMember primary,
      final VersionSource primaryVersionID) {
    return (Boolean) vm.invoke(new SerializableCallable("create entry2") {
      public Object call() {
        // create a fake event that looks like it came from the primary and apply it to
        // this cache
        PartitionedRegion pr = (PartitionedRegion) TestRegion;
        BucketRegion bucket = pr.getDataStore().getLocalBucketById(0);
        VersionTag tag = new VMVersionTag();
        tag.setMemberID(primaryVersionID);
        tag.setRegionVersion(2);
        tag.setEntryVersion(1);
        tag.setIsRemoteForTesting();
        EntryEventImpl event =
            EntryEventImpl.create(bucket, Operation.CREATE, "Object3", true, primary, true, false);
        LogWriterUtils.getLogWriter().info("applying this event to the cache: " + event);
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
        LogWriterUtils.getLogWriter().info("applying this event to the cache: " + event);
        bucket.getRegionMap().basicPut(event, System.currentTimeMillis(), true, false, null, false,
            false);
        event.release();

        bucket.dumpBackingMap();
        LogWriterUtils.getLogWriter()
            .info("bucket version vector is now " + bucket.getVersionVector().fullToString());
        assertTrue("bucket should hold entry Object3 now", bucket.containsKey("Object3"));
        return true;
      }
    });
  }

  private void verifySynchronized(VM vm, final InternalDistributedMember crashedMember) {
    vm.invoke(new SerializableCallable("check that synchronization happened") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion) TestRegion;
        final BucketRegion bucket = pr.getDataStore().getLocalBucketById(0);
        Wait.waitForCriterion(new WaitCriterion() {
          String waitingFor = "primary is still in membership view: " + crashedMember;
          boolean dumped = false;

          public boolean done() {
            if (TestRegion.getCache().getDistributionManager().isCurrentMember(crashedMember)) {
              LogWriterUtils.getLogWriter().info(waitingFor);
              return false;
            }
            if (!TestRegion.containsKey("Object3")) {
              waitingFor = "entry for Object3 not found";
              LogWriterUtils.getLogWriter().info(waitingFor);
              return false;
            }
            RegionEntry re = bucket.getRegionMap().getEntry("Object5");
            if (re == null) {
              if (!dumped) {
                dumped = true;
                bucket.dumpBackingMap();
              }
              waitingFor = "entry for Object5 not found";
              LogWriterUtils.getLogWriter().info(waitingFor);
              return false;
            }
            if (!re.isTombstone()) {
              if (!dumped) {
                dumped = true;
                bucket.dumpBackingMap();
              }
              waitingFor = "Object5 is not a tombstone but should be: " + re;
              LogWriterUtils.getLogWriter().info(waitingFor);
              return false;
            }
            return true;
          }

          public String description() {
            return waitingFor;
          }

        }, 30000, 5000, true);
        return null;
      }
    });
  }


  private void createRegion(VM vm, final String regionName, final TestType typeOfTest) {
    SerializableCallable createRegion = new SerializableCallable() {
      @SuppressWarnings("deprecation")
      public Object call() throws Exception {
        // TombstoneService.VERBOSE = true;
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PARTITION);
        af.setPartitionAttributes((new PartitionAttributesFactory()).setTotalNumBuckets(2)
            .setRedundantCopies(3).create());
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
        TestRegion = (LocalRegion) createRootRegion(regionName, af.create());
        return null;
      }
    };
    vm.invoke(createRegion);
  }

}
