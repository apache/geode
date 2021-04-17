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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.TombstoneService;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.dunit.SerializableRunnable;

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
  public <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.GLOBAL);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    return factory.create();
  }

  @Override
  protected <K, V> RegionAttributes<K, V> getRegionAttributes(String type) {
    RegionAttributes<K, V> ra = getCache().getRegionAttributes(type);
    if (ra == null) {
      throw new IllegalStateException("The region shortcut " + type + " has been removed.");
    }
    AttributesFactory<K, V> factory = new AttributesFactory<>(ra);
    factory.setConcurrencyChecksEnabled(true);
    factory.setScope(Scope.GLOBAL);
    return factory.create();
  }

  @Ignore("replicates don't allow local destroy")
  @Override
  @Test
  public void testLocalDestroy() {
    // replicates don't allow local destroy
  }

  @Ignore("replicates don't allow local destroy")
  @Override
  @Test
  public void testEntryTtlLocalDestroy() {
    // replicates don't allow local destroy
  }

  /**
   * This test creates a server cache in vm0 and a peer cache in vm1. It then tests to see if GII
   * transferred tombstones to vm1 like it's supposed to. A client cache is created in vm2 and the
   * same sort of check is performed for register-interest.
   */
  @Test
  public void testGIISendsTombstones() {
    versionTestGIISendsTombstones();
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
  public void testClearWithConcurrentEvents() {
    versionTestClearWithConcurrentEvents();
  }

  @Ignore("TODO: takes too long with global regions and cause false dunit hangs")
  @Test
  public void testClearWithConcurrentEventsAsync() {
    versionTestClearWithConcurrentEventsAsync();
  }

  @Ignore("TODO: takes too long with global regions and cause false dunit hangs")
  @Test
  public void testClearOnNonReplicateWithConcurrentEvents() {
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

    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable() {
      @Override
      public void run() {
        RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
        CCRegion = (LocalRegion) f.create(name);
        // noinspection OverwrittenKey
        CCRegion.put("cckey0", "ccvalue");
        // noinspection OverwrittenKey
        CCRegion.put("cckey0", "ccvalue"); // version number will end up at 4
      }
    };
    vm0.invoke("Create Region", createRegion);
    vm1.invoke("Create Region", createRegion);
    vm1.invoke("Create local tombstone and adjust time", () -> {
      // make the entry for cckey0 a tombstone in this VM and set its
      // modification time to be older than the tombstone GC interval. This
      // means it could be in the process of being reaped by distributed-GC
      RegionEntry entry = CCRegion.getRegionEntry("cckey0");
      VersionTag tag = entry.getVersionStamp().asVersionTag();
      assertTrue(tag.getEntryVersion() > 1);
      tag.setVersionTimeStamp(
          System.currentTimeMillis() - TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT - 1000);
      entry.getVersionStamp().setVersionTimeStamp(tag.getVersionTimeStamp());
      entry.makeTombstone(CCRegion, tag);
    });
    // now remove the entry on vm0, simulating that it initiated a GC, and
    // perform a CREATE with a new version number
    vm0.invoke("Locally destroy the entry and do a create that will be propagated with v1", () -> {
      CCRegion.getRegionMap().removeEntry("cckey0", CCRegion.getRegionEntry("cckey0"), true);

      assertThat(CCRegion.getRegionEntry("ckey0"))
          .describedAs("expected removeEntry to remove the entry from the region's map").isNull();

      CCRegion.put("cckey0", "updateAfterReap");
    });
    vm1.invoke("Check that the create() was applied", () -> {
      RegionEntry entry = CCRegion.getRegionEntry("cckey0");
      assertThat(entry.getVersionStamp().getEntryVersion()).isEqualTo(1);
    });
    disconnectAllFromDS();
  }


  @Test
  public void testGetAllWithVersions() {
    versionTestGetAllWithVersions();
  }

}
