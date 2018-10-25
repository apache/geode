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

import java.util.Properties;

import org.junit.After;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.LocalRegion;
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
public class RRSynchronizationDUnitTest extends CacheTestCase {

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
  private void doRegionsSyncOnPeerLoss(TestType typeOfTest) {
    IgnoredException.addIgnoredException("killing member's ds");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final String name = getUniqueName() + "Region";

    createRegion(vm0, name, typeOfTest);
    createRegion(vm1, name, typeOfTest);
    createRegion(vm2, name, typeOfTest);

    createEntry1(vm0);


    // cause one of the VMs to throw away the next operation
    InternalDistributedMember crashedID = getId(vm0);
    VersionSource crashedVersionID = getVersionId(vm0);
    createEntry2(vm1, crashedID, crashedVersionID);

    // Now we crash the member who "modified" vm1's cache.
    // The other replicates should perform a delta-GII for the lost member and
    // get back in sync
    DistributedTestUtils.crashDistributedSystem(vm0);

    verifySynchronized(vm2, crashedID);
  }

  private void createEntry1(VM vm) {
    vm.invoke("create entry1", () -> {
      testRegion.create("Object1", Integer.valueOf(1));
    });
  }

  private InternalDistributedMember getId(VM vm) {
    return vm.invoke("get dmId", () -> {
      return testRegion.getCache().getMyId();
    });
  }

  private VersionSource getVersionId(VM vm) {
    return vm.invoke("get versionID", () -> {
      return testRegion.getVersionMember();
    });
  }

  private void createEntry2(VM vm, final InternalDistributedMember forMember,
      final VersionSource memberVersionID) {
    vm.invoke("create entry2", () -> {
      // create a fake event that looks like it came from the lost member and apply it to this cache
      DistributedRegion dr = (DistributedRegion) testRegion;
      VersionTag tag = new VMVersionTag();
      tag.setMemberID(memberVersionID);
      tag.setRegionVersion(2);
      tag.setEntryVersion(1);
      tag.setIsRemoteForTesting();
      EntryEventImpl event =
          EntryEventImpl.create(dr, Operation.CREATE, "Object3", true, forMember, true, false);
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
      event = EntryEventImpl.create(dr, Operation.CREATE, "Object5", true, forMember, true, false);
      event.setNewValue(Token.TOMBSTONE);
      event.setVersionTag(tag);
      dr.getRegionMap().basicPut(event, System.currentTimeMillis(), true, false, null, false,
          false);
      event.release();

      dr.dumpBackingMap();
      assertTrue("should hold entry Object3 now", dr.containsKey("Object3"));
    });
  }

  private void verifySynchronized(VM vm, final InternalDistributedMember crashedMember) {
    vm.invoke("check that synchronization happened", () -> {
      final DistributedRegion dr = (DistributedRegion) testRegion;
      await().until(() -> {

        if (testRegion.getCache().getDistributionManager().isCurrentMember(crashedMember)) {
          return false;
        }
        if (!testRegion.containsKey("Object3")) {
          return false;
        }
        RegionEntry re = dr.getRegionMap().getEntry("Object5");
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
      testRegion = (LocalRegion) createRootRegion(regionName, af.create());
    });
  }

  enum TestType {
    IN_MEMORY, OVERFLOW, PERSISTENT
  }
}
