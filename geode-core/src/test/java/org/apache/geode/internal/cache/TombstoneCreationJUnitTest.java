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

import org.apache.geode.cache.*;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.entries.VersionedThinRegionEntryHeap;
import org.apache.geode.internal.cache.entries.VersionedThinRegionEntryHeapObjectKey;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.net.InetAddress;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.*;

@Category(IntegrationTest.class)
public class TombstoneCreationJUnitTest {
  @Rule
  public TestName nameRule = new TestName();

  @After
  public void tearDown() {
    InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
    if (system != null) {
      system.disconnect();
    }
  }

  @Test
  public void testDestroyCreatesTombstone() throws Exception {
    String name = nameRule.getMethodName();
    Properties props = new Properties();
    props.put(LOCATORS, "");
    props.put(MCAST_PORT, "0");
    props.put(LOG_LEVEL, "config");
    GemFireCacheImpl cache =
        (GemFireCacheImpl) CacheFactory.create(DistributedSystem.connect(props));
    RegionFactory f = cache.createRegionFactory(RegionShortcut.REPLICATE);
    DistributedRegion region = (DistributedRegion) f.create(name);

    EntryEventImpl ev = EntryEventImpl.create(region, Operation.DESTROY, "myDestroyedKey", null,
        null, true, new InternalDistributedMember(InetAddress.getLocalHost(), 1234));
    VersionTag tag = VersionTag.create((InternalDistributedMember) ev.getDistributedMember());
    tag.setIsRemoteForTesting();
    tag.setEntryVersion(2);
    tag.setRegionVersion(12345);
    tag.setVersionTimeStamp(System.currentTimeMillis());
    tag.setDistributedSystemId(1);
    ev.setVersionTag(tag);
    cache.getLogger().info(
        "destroyThread is trying to destroy the entry: " + region.getRegionEntry("myDestroyedKey"));
    region.basicDestroy(ev, false, null); // expectedOldValue not supported on
    RegionEntry entry = region.getRegionEntry("myDestroyedKey");
    Assert.assertTrue(entry != null, "expected to find a region entry for myDestroyedKey");
    Assert.assertTrue(entry.isTombstone(),
        "expected entry to be found and be a tombstone but it is " + entry);

  }

  /**
   * In bug #47868 a thread puts a REMOVED_PHASE1 entry in the map but is unable to lock the entry
   * before a Destroy thread gets it. The Destroy thread did not apply its operation but threw an
   * EntryNotFoundException. It is supposed to create a Tombstone.
   * 
   * @throws Exception
   */
  @Test
  public void testConcurrentCreateAndDestroy() throws Exception {
    String name = nameRule.getMethodName();
    Properties props = new Properties();
    props.put(LOCATORS, "");
    props.put(MCAST_PORT, "0");
    props.put(LOG_LEVEL, "config");
    final GemFireCacheImpl cache =
        (GemFireCacheImpl) CacheFactory.create(DistributedSystem.connect(props));
    RegionFactory f = cache.createRegionFactory(RegionShortcut.REPLICATE);
    final DistributedRegion region = (DistributedRegion) f.create(name);

    // simulate a put() getting into AbstractRegionMap.basicPut() and creating an entry
    // that has not yet been initialized with values. Then do a destroy that will encounter
    // the entry
    String key = "destroyedKey1";
    VersionedThinRegionEntryHeap entry =
        new VersionedThinRegionEntryHeapObjectKey(region, key, Token.REMOVED_PHASE1);
    ((AbstractRegionMap) region.getRegionMap()).putEntryIfAbsentForTest(entry);
    cache.getLogger().info("entry inserted into cache: " + entry);

    EntryEventImpl ev = EntryEventImpl.create(region, Operation.DESTROY, key, null, null, true,
        new InternalDistributedMember(InetAddress.getLocalHost(), 1234));
    VersionTag tag = VersionTag.create((InternalDistributedMember) ev.getDistributedMember());
    tag.setIsRemoteForTesting();
    tag.setEntryVersion(2);
    tag.setRegionVersion(12345);
    tag.setVersionTimeStamp(System.currentTimeMillis());
    tag.setDistributedSystemId(1);
    ev.setVersionTag(tag);
    cache.getLogger()
        .info("destroyThread is trying to destroy the entry: " + region.getRegionEntry(key));
    region.basicDestroy(ev, false, null); // expectedOldValue not supported on
    entry = (VersionedThinRegionEntryHeap) region.getRegionEntry(key);
    region.dumpBackingMap();
    Assert.assertTrue(entry != null, "expected to find a region entry for " + key);
    Assert.assertTrue(entry.isTombstone(),
        "expected entry to be found and be a tombstone but it is " + entry);
    Assert.assertTrue(entry.getVersionStamp().getEntryVersion() == tag.getEntryVersion(),
        "expected " + tag.getEntryVersion() + " but found "
            + entry.getVersionStamp().getEntryVersion());


    RegionMap map = region.getRegionMap();
    tag = entry.asVersionTag();
    map.removeTombstone(entry, tag, false, true);

    // now do an op that has local origin
    entry = new VersionedThinRegionEntryHeapObjectKey(region, key, Token.REMOVED_PHASE1);
    ((AbstractRegionMap) region.getRegionMap()).putEntryIfAbsentForTest(entry);
    cache.getLogger().info("entry inserted into cache: " + entry);

    ev = EntryEventImpl.create(region, Operation.DESTROY, key, null, null, false, cache.getMyId());
    tag = VersionTag.create((InternalDistributedMember) ev.getDistributedMember());
    tag.setEntryVersion(2);
    tag.setRegionVersion(12345);
    tag.setVersionTimeStamp(System.currentTimeMillis());
    tag.setDistributedSystemId(1);
    ev.setVersionTag(tag);
    cache.getLogger()
        .info("destroyThread is trying to destroy the entry: " + region.getRegionEntry(key));
    boolean caught = false;
    try {
      region.basicDestroy(ev, false, null); // expectedOldValue not supported on
    } catch (EntryNotFoundException e) {
      caught = true;
    }
    Assert.assertTrue(caught,
        "expected an EntryNotFoundException for origin=local destroy operation");
  }



  // bug #51184 - cache accepts an event to overwrite an expired
  // tombstone but the tombstone is actually more recent than
  // the event
  @Test
  public void testOlderEventIgnoredEvenIfTombstoneHasExpired() throws Exception {
    String name = nameRule.getMethodName();
    Properties props = new Properties();
    props.put(LOCATORS, "");
    props.put(MCAST_PORT, "0");
    props.put(LOG_LEVEL, "config");
    final GemFireCacheImpl cache =
        (GemFireCacheImpl) CacheFactory.create(DistributedSystem.connect(props));
    RegionFactory f = cache.createRegionFactory(RegionShortcut.REPLICATE);
    final DistributedRegion region = (DistributedRegion) f.create(name);

    // simulate a put() getting into AbstractRegionMap.basicPut() and creating an entry
    // that has not yet been initialized with values. Then do a destroy that will encounter
    // the entry
    String key = "destroyedKey1";
    VersionedThinRegionEntryHeap entry =
        new VersionedThinRegionEntryHeapObjectKey(region, key, Token.REMOVED_PHASE1);
    entry.setLastModified(
        System.currentTimeMillis() - (2 * TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT));
    ((AbstractRegionMap) region.getRegionMap()).putEntryIfAbsentForTest(entry);
    cache.getLogger().info("entry inserted into cache: " + entry);

    EntryEventImpl ev = EntryEventImpl.create(region, Operation.DESTROY, key, null, null, true,
        new InternalDistributedMember(InetAddress.getLocalHost(), 1234));
    VersionTag tag = VersionTag.create((InternalDistributedMember) ev.getDistributedMember());
    tag.setIsRemoteForTesting();
    tag.setEntryVersion(3);
    tag.setRegionVersion(12345);
    tag.setVersionTimeStamp(
        System.currentTimeMillis() - TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT);
    tag.setDistributedSystemId(1);
    ev.setVersionTag(tag);
    cache.getLogger().info("trying to destroy the entry: " + region.getRegionEntry(key));
    region.basicDestroy(ev, false, null); // expectedOldValue not supported on
    entry = (VersionedThinRegionEntryHeap) region.getRegionEntry(key);
    region.dumpBackingMap();
    Assert.assertTrue(entry != null, "expected to find a region entry for " + key);
    Assert.assertTrue(entry.isTombstone(),
        "expected entry to be found and be a tombstone but it is " + entry);
    Assert.assertTrue(entry.getVersionStamp().getEntryVersion() == tag.getEntryVersion(),
        "expected " + tag.getEntryVersion() + " but found "
            + entry.getVersionStamp().getEntryVersion());

    // pause to let the clock change
    Thread.sleep(100);

    ev = EntryEventImpl.create(region, Operation.UPDATE, key, null, null, true,
        new InternalDistributedMember(InetAddress.getLocalHost(), 1234));
    tag = VersionTag.create((InternalDistributedMember) ev.getDistributedMember());
    tag.setIsRemoteForTesting();
    tag.setEntryVersion(1);
    tag.setRegionVersion(12340);
    tag.setVersionTimeStamp(
        System.currentTimeMillis() - TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT - 10000);
    tag.setDistributedSystemId(1);
    ev.setVersionTag(tag);
    cache.getLogger()
        .info("trying to update the entry with an older event: " + region.getRegionEntry(key));
    region.virtualPut(ev, false, true, null, false, tag.getVersionTimeStamp(), true);
    entry = (VersionedThinRegionEntryHeap) region.getRegionEntry(key);
    region.dumpBackingMap();
    Assert.assertTrue(entry != null, "expected to find a region entry for " + key);
    Assert.assertTrue(entry.isTombstone(),
        "expected entry to be found and be a tombstone but it is " + entry);
    Assert.assertTrue(entry.getVersionStamp().getEntryVersion() == 3,
        "expected 3" + " but found " + entry.getVersionStamp().getEntryVersion());

  }

}
