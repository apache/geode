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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.Token.Tombstone;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * This test creates an entry and verifies if {@link UpdateOperation} applies correctly on it after
 * various other operations have been applied on it (Including destroy when entry is a
 * {@link Tombstone}).
 *
 * @since GemFire 7.0.1
 *
 */
public class UpdateVersionJUnitTest {

  private static final String regionName = "versionedregion";

  private VersionTagHolder createNewEvent(LocalRegion region, VersionTag tag, Object key) {
    VersionTagHolder updateTimeStampEvent = new VersionTagHolder(tag);
    updateTimeStampEvent.setOperation(Operation.UPDATE_VERSION_STAMP);
    updateTimeStampEvent.setRegion(region);
    if (region instanceof PartitionedRegion) {
      updateTimeStampEvent.setKeyInfo(region.getKeyInfo(key));
    } else {
      updateTimeStampEvent.setKeyInfo(new KeyInfo(key, null, 0));
    }
    updateTimeStampEvent.setGenerateCallbacks(false);
    updateTimeStampEvent.distributedMember = region.getSystem().getDistributedMember();
    updateTimeStampEvent.setNewEventId(region.getSystem());
    return updateTimeStampEvent;
  }

  /**
   * Tests for LocalRegion.
   */

  @Test
  public void testUpdateVersionAfterCreate() {

    Cache cache = new CacheFactory().set(MCAST_PORT, "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

    try {
      region.create("key-1", "value-1");

      Entry entry = region.getEntry("key-1");
      assertTrue(entry instanceof NonTXEntry);
      RegionEntry regionEntry = ((NonTXEntry) entry).getRegionEntry();

      VersionStamp stamp = regionEntry.getVersionStamp();

      // Create a duplicate entry version tag from stamp with newer time-stamp.
      VersionTag tag = VersionTag.create(stamp.getMemberID());

      int entryVersion = stamp.getEntryVersion();
      VersionSource member = stamp.getMemberID();
      int dsid = stamp.getDistributedSystemId();
      long time = System.currentTimeMillis() + 1;

      tag.setEntryVersion(entryVersion);
      tag.setDistributedSystemId(dsid);
      tag.setVersionTimeStamp(time);
      tag.setIsGatewayTag(true);

      assertTrue(region instanceof LocalRegion);

      EntryEventImpl event = createNewEvent((LocalRegion) region, tag, entry.getKey());

      ((LocalRegion) region).basicUpdateEntryVersion(event);

      // Verify the new stamp
      entry = region.getEntry("key-1");
      assertTrue(entry instanceof NonTXEntry);
      regionEntry = ((NonTXEntry) entry).getRegionEntry();

      stamp = regionEntry.getVersionStamp();
      assertEquals("Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
          time, stamp.getVersionTimeStamp());
      assertEquals(++entryVersion, stamp.getEntryVersion());
      assertEquals(member, stamp.getMemberID());
      assertEquals(dsid, stamp.getDistributedSystemId());
    } finally {
      region.destroyRegion();
      cache.close();
    }
  }

  @Test
  public void testUpdateVersionAfterUpdate() {
    Cache cache = new CacheFactory().set(MCAST_PORT, "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

    try {
      region.create("key-1", "value-1");
      try {
        Thread.sleep(10);
      } catch (InterruptedException ignored) {
      }
      region.put("key-1", "value-2");

      Entry entry = region.getEntry("key-1");
      assertTrue(entry instanceof NonTXEntry);
      RegionEntry regionEntry = ((NonTXEntry) entry).getRegionEntry();

      VersionStamp stamp = regionEntry.getVersionStamp();

      // Create a duplicate entry version tag from stamp with newer time-stamp.
      VersionTag tag = VersionTag.create(stamp.getMemberID());

      int entryVersion = stamp.getEntryVersion();
      VersionSource member = stamp.getMemberID();
      int dsid = stamp.getDistributedSystemId();
      long time = System.currentTimeMillis() + 1; // Just in case if clock hasn't ticked.

      tag.setEntryVersion(entryVersion);
      tag.setDistributedSystemId(dsid);
      tag.setVersionTimeStamp(time);
      tag.setIsGatewayTag(true);

      assertTrue(region instanceof LocalRegion);

      EntryEventImpl event = createNewEvent((LocalRegion) region, tag, entry.getKey());

      ((LocalRegion) region).basicUpdateEntryVersion(event);

      // Verify the new stamp
      entry = region.getEntry("key-1");
      assertTrue(entry instanceof NonTXEntry);
      regionEntry = ((NonTXEntry) entry).getRegionEntry();

      stamp = regionEntry.getVersionStamp();
      assertEquals("Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
          time, stamp.getVersionTimeStamp());
      assertEquals(++entryVersion, stamp.getEntryVersion());
      assertEquals(member, stamp.getMemberID());
      assertEquals(dsid, stamp.getDistributedSystemId());
    } finally {
      region.destroyRegion();
      cache.close();
    }
  }

  @Test
  public void testUpdateVersionAfterDestroy() {

    Cache cache = new CacheFactory().set(MCAST_PORT, "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

    try {

      region.create("key-1", "value-1");
      try {
        Thread.sleep(10);
      } catch (InterruptedException ignored) {
      }
      region.destroy("key-1");

      assertTrue(region instanceof LocalRegion);

      Entry entry = ((LocalRegion) region).getEntry("key-1", true);

      assertTrue(entry instanceof NonTXEntry);
      RegionEntry regionEntry = ((NonTXEntry) entry).getRegionEntry();

      VersionStamp stamp = regionEntry.getVersionStamp();

      // Create a duplicate entry version tag from stamp with newer time-stamp.
      VersionTag tag = VersionTag.create(stamp.getMemberID());

      int entryVersion = stamp.getEntryVersion();
      VersionSource member = stamp.getMemberID();
      int dsid = stamp.getDistributedSystemId();
      long time = System.currentTimeMillis() + 1;

      tag.setEntryVersion(entryVersion);
      tag.setDistributedSystemId(dsid);
      tag.setVersionTimeStamp(time);
      tag.setIsGatewayTag(true);

      EntryEventImpl event = createNewEvent((LocalRegion) region, tag, "key-1");

      ((LocalRegion) region).basicUpdateEntryVersion(event);

      // Verify the new stamp
      entry = ((LocalRegion) region).getEntry("key-1", true);
      assertTrue(entry instanceof NonTXEntry);
      regionEntry = ((NonTXEntry) entry).getRegionEntry();

      stamp = regionEntry.getVersionStamp();
      assertEquals("Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
          time, stamp.getVersionTimeStamp());
      assertEquals(++entryVersion, stamp.getEntryVersion());
      assertEquals(member, stamp.getMemberID());
      assertEquals(dsid, stamp.getDistributedSystemId());
    } finally {
      region.destroyRegion();
      cache.close();
    }
  }

  /**
   * Tests for Partitioned Region.
   */

  @Test
  public void testUpdateVersionAfterCreateOnPR() {

    Cache cache = new CacheFactory().set(MCAST_PORT, "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);

    try {
      region.create("key-1", "value-1");

      Entry entry = region.getEntry("key-1");
      assertTrue(entry instanceof EntrySnapshot);
      RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

      VersionStamp stamp = regionEntry.getVersionStamp();

      // Create a duplicate entry version tag from stamp with newer time-stamp.
      VersionTag tag = VersionTag.create(stamp.getMemberID());

      int entryVersion = stamp.getEntryVersion();
      VersionSource member = stamp.getMemberID();
      int dsid = stamp.getDistributedSystemId();
      long time = System.currentTimeMillis();

      tag.setEntryVersion(entryVersion);
      tag.setDistributedSystemId(dsid);
      tag.setVersionTimeStamp(time);
      tag.setIsGatewayTag(true);

      assertTrue(region instanceof PartitionedRegion);

      EntryEventImpl event = createNewEvent((PartitionedRegion) region, tag, entry.getKey());

      ((PartitionedRegion) region).basicUpdateEntryVersion(event);

      // Verify the new stamp
      entry = region.getEntry("key-1");
      assertTrue(entry instanceof EntrySnapshot);
      regionEntry = ((EntrySnapshot) entry).getRegionEntry();

      stamp = regionEntry.getVersionStamp();
      assertEquals("Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
          time, stamp.getVersionTimeStamp());
      assertEquals(++entryVersion, stamp.getEntryVersion());
      assertEquals(member, stamp.getMemberID());
      assertEquals(dsid, stamp.getDistributedSystemId());
    } finally {
      region.destroyRegion();
      cache.close();
    }
  }

  @Test
  public void testUpdateVersionAfterUpdateOnPR() {
    Cache cache = new CacheFactory().set(MCAST_PORT, "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);

    try {
      region.create("key-1", "value-1");
      try {
        Thread.sleep(10);
      } catch (InterruptedException ignored) {
      }
      region.put("key-1", "value-2");

      Entry entry = region.getEntry("key-1");
      assertTrue(entry instanceof EntrySnapshot);
      RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

      VersionStamp stamp = regionEntry.getVersionStamp();

      // Create a duplicate entry version tag from stamp with newer time-stamp.
      VersionTag tag = VersionTag.create(stamp.getMemberID());

      int entryVersion = stamp.getEntryVersion();
      VersionSource member = stamp.getMemberID();
      int dsid = stamp.getDistributedSystemId();
      long time = System.currentTimeMillis();

      tag.setEntryVersion(entryVersion);
      tag.setDistributedSystemId(dsid);
      tag.setVersionTimeStamp(time);
      tag.setIsGatewayTag(true);

      assertTrue(region instanceof PartitionedRegion);

      EntryEventImpl event = createNewEvent((PartitionedRegion) region, tag, entry.getKey());

      ((PartitionedRegion) region).basicUpdateEntryVersion(event);

      // Verify the new stamp
      entry = region.getEntry("key-1");
      assertTrue(entry instanceof EntrySnapshot);
      regionEntry = ((EntrySnapshot) entry).getRegionEntry();

      stamp = regionEntry.getVersionStamp();
      assertEquals("Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
          time, stamp.getVersionTimeStamp());
      assertEquals(++entryVersion, stamp.getEntryVersion());
      assertEquals(member, stamp.getMemberID());
      assertEquals(dsid, stamp.getDistributedSystemId());
    } finally {
      region.destroyRegion();
      cache.close();
    }
  }

  @Test
  public void testUpdateVersionAfterDestroyOnPR() {

    Cache cache = new CacheFactory().set(MCAST_PORT, "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);

    try {

      region.create("key-1", "value-1");
      try {
        Thread.sleep(10);
      } catch (InterruptedException ignored) {
      }
      region.destroy("key-1");

      assertTrue(region instanceof PartitionedRegion);
      Entry entry = ((PartitionedRegion) region).getEntry("key-1", true);
      assertTrue(entry instanceof EntrySnapshot);
      RegionEntry regionEntry = ((EntrySnapshot) entry).getRegionEntry();

      VersionStamp stamp = regionEntry.getVersionStamp();

      // Create a duplicate entry version tag from stamp with newer time-stamp.
      VersionTag tag = VersionTag.create(stamp.getMemberID());

      int entryVersion = stamp.getEntryVersion();
      VersionSource member = stamp.getMemberID();
      int dsid = stamp.getDistributedSystemId();
      long time = System.currentTimeMillis();

      tag.setEntryVersion(entryVersion);
      tag.setDistributedSystemId(dsid);
      tag.setVersionTimeStamp(time);
      tag.setIsGatewayTag(true);

      assertTrue(region instanceof PartitionedRegion);

      EntryEventImpl event = createNewEvent((PartitionedRegion) region, tag, "key-1");

      ((PartitionedRegion) region).basicUpdateEntryVersion(event);

      // Verify the new stamp
      entry = ((PartitionedRegion) region).getEntry("key-1", true);
      assertTrue(entry instanceof EntrySnapshot);
      regionEntry = ((EntrySnapshot) entry).getRegionEntry();

      stamp = regionEntry.getVersionStamp();
      assertEquals("Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
          time, stamp.getVersionTimeStamp());
      assertEquals(++entryVersion, stamp.getEntryVersion());
      assertEquals(member, stamp.getMemberID());
      assertEquals(dsid, stamp.getDistributedSystemId());
    } finally {
      region.destroyRegion();
      cache.close();
    }
  }
}
