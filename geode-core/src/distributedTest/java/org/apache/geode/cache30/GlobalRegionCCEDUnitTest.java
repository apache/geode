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

import static org.apache.geode.distributed.ConfigurationProperties.CONSERVE_SOCKETS;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.TombstoneService;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

/**
 * This test is only for GLOBAL REPLICATE Regions. Tests are similar to
 * {@link DistributedAckRegionCCEDUnitTest}.
 */

public class GlobalRegionCCEDUnitTest extends GlobalRegionDUnitTest {

  @Override
  protected boolean supportsTransactions() {
    return true;
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties p = super.getDistributedSystemProperties();
    p.put(CONSERVE_SOCKETS, "false");
    if (distributedSystemID > 0) {
      p.put(DISTRIBUTED_SYSTEM_ID, "" + distributedSystemID);
    }
    return p;
  }

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  @Override
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    return factory.create();
  }

  @Override
  protected RegionAttributes getRegionAttributes(String type) {
    RegionAttributes ra = getCache().getRegionAttributes(type);
    if (ra == null) {
      throw new IllegalStateException("The region shortcut " + type + " has been removed.");
    }
    AttributesFactory factory = new AttributesFactory(ra);
    factory.setConcurrencyChecksEnabled(true);
    factory.setScope(Scope.GLOBAL);
    return factory.create();
  }

  @Ignore("replicates don't allow local destroy")
  @Override
  @Test
  public void testLocalDestroy() throws InterruptedException {
    // replicates don't allow local destroy
  }

  @Ignore("replicates don't allow local destroy")
  @Override
  @Test
  public void testEntryTtlLocalDestroy() throws InterruptedException {
    // replicates don't allow local destroy
  }

  /**
   * This test creates a server cache in vm0 and a peer cache in vm1. It then tests to see if GII
   * transferred tombstones to vm1 like it's supposed to. A client cache is created in vm2 and the
   * same sort of check is performed for register-interest.
   */
  @Test
  public void testGIISendsTombstones() throws Exception {
    versionTestGIISendsTombstones();
  }

  // TODO: delete this unused method
  protected void do_version_recovery_if_necessary(final VM vm0, final VM vm1, final VM vm2,
      final Object[] params) {
    // do nothing here
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  @Test
  public void testConcurrentEvents() throws Exception {
    versionTestConcurrentEvents();
  }

  // these tests for clear() take too long with global regions and cause false dunit hangs
  // on older machines

  @Ignore("TODO: takes too long with global regions and cause false dunit hangs")
  @Test
  public void testClearWithConcurrentEvents() throws Exception {
    versionTestClearWithConcurrentEvents();
  }

  @Ignore("TODO: takes too long with global regions and cause false dunit hangs")
  @Test
  public void testClearWithConcurrentEventsAsync() throws Exception {
    versionTestClearWithConcurrentEventsAsync();
  }

  @Ignore("TODO: takes too long with global regions and cause false dunit hangs")
  @Test
  public void testClearOnNonReplicateWithConcurrentEvents() throws Exception {
    versionTestClearOnNonReplicateWithConcurrentEvents();
  }

  @Test
  public void testTombstones() {
    versionTestTombstones();
  }

  /**
   * make sure that an operation performed on a new region entry created after a tombstone has been
   * reaped is accepted by another member that has yet to reap the tombstone
   */
  @Test
  public void testTombstoneExpirationRace() {
    VM vm0 = Host.getHost(0).getVM(0);
    VM vm1 = Host.getHost(0).getVM(1);
    // VM vm2 = Host.getHost(0).getVM(2);

    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable("Create Region") {
      public void run() {
        try {
          RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
          CCRegion = (LocalRegion) f.create(name);
          CCRegion.put("cckey0", "ccvalue");
          CCRegion.put("cckey0", "ccvalue"); // version number will end up at 4
        } catch (CacheException ex) {
          Assert.fail("While creating region", ex);
        }
      }
    };
    vm0.invoke(createRegion);
    vm1.invoke(createRegion);
    // vm2.invoke(createRegion);
    vm1.invoke(new SerializableRunnable("Create local tombstone and adjust time") {
      public void run() {
        // make the entry for cckey0 a tombstone in this VM and set its
        // modification time to be older than the tombstone GC interval. This
        // means it could be in the process of being reaped by distributed-GC
        RegionEntry entry = CCRegion.getRegionEntry("cckey0");
        VersionTag tag = entry.getVersionStamp().asVersionTag();
        assertTrue(tag.getEntryVersion() > 1);
        tag.setVersionTimeStamp(
            System.currentTimeMillis() - TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT - 1000);
        entry.getVersionStamp().setVersionTimeStamp(tag.getVersionTimeStamp());
        try {
          entry.makeTombstone(CCRegion, tag);
        } catch (RegionClearedException e) {
          Assert.fail("region was mysteriously cleared during unit testing", e);
        }
      }
    });
    // now remove the entry on vm0, simulating that it initiated a GC, and
    // perform a CREATE with a new version number
    vm0.invoke(new SerializableRunnable(
        "Locally destroy the entry and do a create that will be propagated with v1") {
      public void run() {
        CCRegion.getRegionMap().removeEntry("cckey0", CCRegion.getRegionEntry("cckey0"), true);
        if (CCRegion.getRegionEntry("ckey0") != null) {
          fail("expected removEntry to remove the entry from the region's map");
        }
        CCRegion.put("cckey0", "updateAfterReap");
      }
    });
    vm1.invoke(new SerializableRunnable("Check that the create() was applied") {
      public void run() {
        RegionEntry entry = CCRegion.getRegionEntry("cckey0");
        assertTrue(entry.getVersionStamp().getEntryVersion() == 1);
      }
    });
    disconnectAllFromDS();
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  @Ignore("Disabling due to bug #52347")
  @Test
  public void testConcurrentEventsOnEmptyRegion() {
    versionTestConcurrentEventsOnEmptyRegion();
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  @Ignore("TODO: reenable this test")
  @Test
  public void testConcurrentEventsOnNonReplicatedRegion() {
    // Shobhit: Just commenting out for now as it is being fixed by Bruce.
    // TODO: uncomment the test asa the bug is fixed.
    // versionTestConcurrentEventsOnNonReplicatedRegion();
  }

  @Test
  public void testGetAllWithVersions() {
    versionTestGetAllWithVersions();
  }

}
