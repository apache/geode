/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.internal.cache.LocalRegion.NonTXEntry;
import com.gemstone.gemfire.internal.cache.Token.Tombstone;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionStamp;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This test creates an entry and verifies if {@link UpdateOperation} applies
 * correctly on it after various other operations have been applied on it
 * (Including destroy when entry is a {@link Tombstone}).
 * 
 * @author Shobhit Agarwal
 * @since 7.0.1
 *
 */
@Category(IntegrationTest.class)
public class UpdateVersionJUnitTest {

  private static final String regionName = "versionedregion";

  private EntryEventImpl createNewEvent(LocalRegion region, VersionTag tag, Object key) {
    EntryEventImpl updateTimeStampEvent = EntryEventImpl.createVersionTagHolder(tag);
    updateTimeStampEvent.setOperation(Operation.UPDATE_VERSION_STAMP);
    updateTimeStampEvent.setRegion(region);
    if (region instanceof PartitionedRegion) {
      updateTimeStampEvent.setKeyInfo(((PartitionedRegion)region).getKeyInfo(key));
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
    
    Cache cache = new CacheFactory().set("mcast-port", "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

    try {
      region.create("key-1", "value-1");
  
      Entry entry = region.getEntry("key-1");
      assertTrue(entry instanceof NonTXEntry);
      RegionEntry regionEntry = ((NonTXEntry)entry).getRegionEntry();
  
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
      
      EntryEventImpl event = createNewEvent((LocalRegion)region, tag, entry.getKey());
      
      ((LocalRegion)region).basicUpdateEntryVersion(event);
  
      // Verify the new stamp
      entry = region.getEntry("key-1");
      assertTrue(entry instanceof NonTXEntry);
      regionEntry = ((NonTXEntry)entry).getRegionEntry();
  
      stamp = regionEntry.getVersionStamp();
      assertEquals(
          "Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
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
    Cache cache = new CacheFactory().set("mcast-port", "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

    try {
      region.create("key-1", "value-1");
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) { }
      region.put("key-1", "value-2");
  
      Entry entry = region.getEntry("key-1");
      assertTrue(entry instanceof NonTXEntry);
      RegionEntry regionEntry = ((NonTXEntry)entry).getRegionEntry();
  
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
      
      EntryEventImpl event = createNewEvent((LocalRegion)region, tag, entry.getKey());
      
      ((LocalRegion)region).basicUpdateEntryVersion(event);
  
      // Verify the new stamp
      entry = region.getEntry("key-1");
      assertTrue(entry instanceof NonTXEntry);
      regionEntry = ((NonTXEntry)entry).getRegionEntry();
  
      stamp = regionEntry.getVersionStamp();
      assertEquals(
          "Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
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

    Cache cache = new CacheFactory().set("mcast-port", "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);

    try {
      
      region.create("key-1", "value-1");
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) { }
      region.destroy("key-1");
  
      assertTrue(region instanceof LocalRegion);
      
      Entry entry = ((LocalRegion)region).getEntry("key-1", true);
      
      assertTrue(entry instanceof NonTXEntry);
      RegionEntry regionEntry = ((NonTXEntry)entry).getRegionEntry();
  
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
      
      EntryEventImpl event = createNewEvent((LocalRegion)region, tag, "key-1");
      
      ((LocalRegion)region).basicUpdateEntryVersion(event);
  
      // Verify the new stamp
      entry = ((LocalRegion)region).getEntry("key-1", true);
      assertTrue(entry instanceof NonTXEntry);
      regionEntry = ((NonTXEntry)entry).getRegionEntry();
  
      stamp = regionEntry.getVersionStamp();
      assertEquals(
          "Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
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
    
    Cache cache = new CacheFactory().set("mcast-port", "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);

    try {
      region.create("key-1", "value-1");
  
      Entry entry = region.getEntry("key-1");
      assertTrue(entry instanceof EntrySnapshot);
      RegionEntry regionEntry = ((EntrySnapshot)entry).getRegionEntry();
  
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
      
      EntryEventImpl event = createNewEvent((PartitionedRegion)region, tag, entry.getKey());
      
      ((PartitionedRegion)region).basicUpdateEntryVersion(event);
  
      // Verify the new stamp
      entry = region.getEntry("key-1");
      assertTrue(entry instanceof EntrySnapshot);
      regionEntry = ((EntrySnapshot)entry).getRegionEntry();
  
      stamp = regionEntry.getVersionStamp();
      assertEquals(
          "Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
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
    Cache cache = new CacheFactory().set("mcast-port", "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);

    try {
      region.create("key-1", "value-1");
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) { }
      region.put("key-1", "value-2");
  
      Entry entry = region.getEntry("key-1");
      assertTrue(entry instanceof EntrySnapshot);
      RegionEntry regionEntry = ((EntrySnapshot)entry).getRegionEntry();
  
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
      
      EntryEventImpl event = createNewEvent((PartitionedRegion)region, tag, entry.getKey());
      
      ((PartitionedRegion)region).basicUpdateEntryVersion(event);
  
      // Verify the new stamp
      entry = region.getEntry("key-1");
      assertTrue(entry instanceof EntrySnapshot);
      regionEntry = ((EntrySnapshot)entry).getRegionEntry();
  
      stamp = regionEntry.getVersionStamp();
      assertEquals(
          "Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
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

    Cache cache = new CacheFactory().set("mcast-port", "0").create();
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION).create(regionName);

    try {
      
      region.create("key-1", "value-1");
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) { }
      region.destroy("key-1");
  
      assertTrue(region instanceof PartitionedRegion);
      Entry entry = ((PartitionedRegion)region).getEntry("key-1", true);
      assertTrue(entry instanceof EntrySnapshot);
      RegionEntry regionEntry = ((EntrySnapshot)entry).getRegionEntry();
  
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
      
      EntryEventImpl event = createNewEvent((PartitionedRegion)region, tag, "key-1");
      
      ((PartitionedRegion)region).basicUpdateEntryVersion(event);
  
      // Verify the new stamp
      entry = ((PartitionedRegion)region).getEntry("key-1", true);
      assertTrue(entry instanceof EntrySnapshot);
      regionEntry = ((EntrySnapshot)entry).getRegionEntry();
  
      stamp = regionEntry.getVersionStamp();
      assertEquals(
          "Time stamp did NOT get updated by UPDATE_VERSION operation on LocalRegion",
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
