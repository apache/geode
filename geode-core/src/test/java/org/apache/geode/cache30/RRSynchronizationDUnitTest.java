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

import java.util.*;

import org.apache.geode.distributed.*;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.*;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.versions.VMVersionTag;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.dunit.*;

/**
 * concurrency-control tests for client/server
 * 
 *
 */
@Category(DistributedTest.class)
public class RRSynchronizationDUnitTest extends JUnit4CacheTestCase {
  static enum TestType {
    IN_MEMORY, OVERFLOW, PERSISTENT
  };

  public static LocalRegion TestRegion;

  public RRSynchronizationDUnitTest() {
    super();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    result.put(ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION, "false");
    return result;
  }

  @Test
  public void testThatRegionsSyncOnPeerLoss() {
    doRegionsSyncOnPeerLoss(TestType.IN_MEMORY);
  }

  @Test
  public void testThatRegionsSyncOnPeerLossWithPersistence() {
    doRegionsSyncOnPeerLoss(TestType.PERSISTENT);
  }

  @Test
  public void testThatRegionsSyncOnPeerLossWithOverflow() {
    doRegionsSyncOnPeerLoss(TestType.OVERFLOW);
  }


  /**
   * We hit this problem in bug #45669. delta-GII was not being distributed in the 7.0 release.
   */
  public void doRegionsSyncOnPeerLoss(TestType typeOfTest) {
    IgnoredException.addIgnoredException("killing member's ds");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final String name = this.getUniqueName() + "Region";

    disconnectAllFromDS();

    try {
      createRegion(vm0, name, typeOfTest);
      createRegion(vm1, name, typeOfTest);
      createRegion(vm2, name, typeOfTest);

      createEntry1(vm0);


      // cause one of the VMs to throw away the next operation
      InternalDistributedMember crashedID = getID(vm0);
      VersionSource crashedVersionID = getVersionID(vm0);
      createEntry2(vm1, crashedID, crashedVersionID);

      // Now we crash the member who "modified" vm1's cache.
      // The other replicates should perform a delta-GII for the lost member and
      // get back in sync
      DistributedTestUtils.crashDistributedSystem(vm0);

      verifySynchronized(vm2, crashedID);
    } finally {
      disconnectAllFromDS();
    }
  }


  private boolean createEntry1(VM vm) {
    return (Boolean) vm.invoke(new SerializableCallable("create entry1") {
      public Object call() {
        TestRegion.create("Object1", Integer.valueOf(1));
        return true;
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

  private boolean createEntry2(VM vm, final InternalDistributedMember forMember,
      final VersionSource memberVersionID) {
    return (Boolean) vm.invoke(new SerializableCallable("create entry2") {
      public Object call() {
        // create a fake event that looks like it came from the lost member and apply it to
        // this cache
        DistributedRegion dr = (DistributedRegion) TestRegion;
        VersionTag tag = new VMVersionTag();
        tag.setMemberID(memberVersionID);
        tag.setRegionVersion(2);
        tag.setEntryVersion(1);
        tag.setIsRemoteForTesting();
        EntryEventImpl event =
            EntryEventImpl.create(dr, Operation.CREATE, "Object3", true, forMember, true, false);
        LogWriterUtils.getLogWriter().info("applying this event to the cache: " + event);
        event.setNewValue(new VMCachedDeserializable("value3", 12));
        event.setVersionTag(tag);
        dr.getRegionMap().basicPut(event, System.currentTimeMillis(), true, false, null, false,
            false);
        event.release();

        // now create a tombstone so we can be sure these are transferred in delta-GII
        tag = new VMVersionTag();
        tag.setMemberID(memberVersionID);
        tag.setRegionVersion(3);
        tag.setEntryVersion(1);
        tag.setIsRemoteForTesting();
        event =
            EntryEventImpl.create(dr, Operation.CREATE, "Object5", true, forMember, true, false);
        event.setNewValue(Token.TOMBSTONE);
        event.setVersionTag(tag);
        LogWriterUtils.getLogWriter().info("applying this event to the cache: " + event);
        dr.getRegionMap().basicPut(event, System.currentTimeMillis(), true, false, null, false,
            false);
        event.release();

        dr.dumpBackingMap();
        LogWriterUtils.getLogWriter()
            .info("version vector is now " + dr.getVersionVector().fullToString());
        assertTrue("should hold entry Object3 now", dr.containsKey("Object3"));
        return true;
      }
    });
  }

  private void verifySynchronized(VM vm, final InternalDistributedMember crashedMember) {
    vm.invoke(new SerializableCallable("check that synchronization happened") {
      public Object call() throws Exception {
        final DistributedRegion dr = (DistributedRegion) TestRegion;
        Wait.waitForCriterion(new WaitCriterion() {
          String waitingFor = "crashed member is still in membership view: " + crashedMember;
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
            RegionEntry re = dr.getRegionMap().getEntry("Object5");
            if (re == null) {
              if (!dumped) {
                dumped = true;
                dr.dumpBackingMap();
              }
              waitingFor = "entry for Object5 not found";
              LogWriterUtils.getLogWriter().info(waitingFor);
              return false;
            }
            if (!re.isTombstone()) {
              if (!dumped) {
                dumped = true;
                dr.dumpBackingMap();
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
        af.setScope(Scope.DISTRIBUTED_NO_ACK);
        switch (typeOfTest) {
          case IN_MEMORY:
            af.setDataPolicy(DataPolicy.REPLICATE);
            break;
          case PERSISTENT:
            af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
            break;
          case OVERFLOW:
            af.setDataPolicy(DataPolicy.REPLICATE);
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
