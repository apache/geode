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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.DiskRegionStats;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.eviction.CountLRUEviction;
import org.apache.geode.internal.cache.eviction.EvictionCounters;

/**
 * Tests the functionality of cache regions whose contents may be written to disk.
 *
 *
 * @since GemFire 3.2
 */
public class DiskRegionIntegrationTest {

  private static final String REGION_NAME = "testRegion";
  private static final int MAX_ENTRIES = 5;

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private Cache cache;
  private Region<Integer, String> region;
  private DiskStore diskStore;
  private DiskRegion diskRegion;
  private File diskDir;

  @Before
  public void setup() throws IOException {
    cache = new CacheFactory().create();
    diskDir = tempDir.newFolder();

    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setDiskDirs(new File[] {diskDir});
    diskStore = dsf.create(REGION_NAME);

    region =
        cache.<Integer, String>createRegionFactory(RegionShortcut.LOCAL).setDiskSynchronous(true)
            .setDiskStoreName(diskStore.getName()).setEvictionAttributes(EvictionAttributes
                .createLRUEntryAttributes(MAX_ENTRIES, EvictionAction.OVERFLOW_TO_DISK))
            .create(REGION_NAME);
    diskRegion = ((InternalRegion) region).getDiskRegion();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  /**
   * Returns the <code>EvictionStatistics</code> for the given region
   */
  private EvictionCounters getLRUStats(Region region) {
    final LocalRegion l = (LocalRegion) region;
    return l.getEvictionController().getCounters();
  }

  //////// Test Methods

  /**
   * Tests that data overflows correctly to a disk region
   */
  @Test
  public void testDiskRegionOverflow() {
    DiskRegionStats diskStats = diskRegion.getStats();
    EvictionCounters lruStats = getLRUStats(region);

    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, "value"));

    assertEquals(1, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(1, lruStats.getEvictions());
    assertEquals(1, diskStats.getNumOverflowOnDisk());
    assertEquals(MAX_ENTRIES, diskStats.getNumEntriesInVM());

    region.get(0);

    assertEquals(2, diskStats.getWrites());
    assertEquals(1, diskStats.getReads());
    assertEquals(2, lruStats.getEvictions());
  }

  /**
   * Overflows a region and makes sure that gets of recently-used objects do not cause faults.
   */
  @Test
  public void testNoFaults() {
    DiskRegionStats diskStats = diskRegion.getStats();
    EvictionCounters lruStats = getLRUStats(region);

    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, "value"));

    long firstEvictions = lruStats.getEvictions();
    long firstReads = diskStats.getReads();

    IntStream.range(1, MAX_ENTRIES + 1).forEach((key) -> {
      region.get(key);
      assertEquals("Key " + key + " caused an eviction", firstEvictions, lruStats.getEvictions());
      assertEquals("Key " + key + " caused an eviction", firstReads, diskStats.getReads());
    });
  }

  /**
   * Tests destroying entries in an overflow region
   */
  @Test
  public void testDestroy() {
    DiskRegionStats diskStats = diskRegion.getStats();
    EvictionCounters lruStats = getLRUStats(region);

    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, "value"));
    assertEquals(0, diskStats.getRemoves());
    assertEquals(1, lruStats.getEvictions());

    // Destroying entries in memory should have no effect on the disk
    IntStream.range(1, MAX_ENTRIES + 1).forEach(region::destroy);
    assertEquals(0, diskStats.getRemoves());
    assertEquals(1, lruStats.getEvictions());

    // Destroying entry overflowed to disk should cause remove on disk
    region.destroy(0);
    assertEquals(1, diskStats.getRemoves());
    assertEquals(1, lruStats.getEvictions());
    assertEquals(0, region.keySet().size());
  }

  /**
   * Tests cache listeners in an overflow region are invoked and that their events are reasonable.
   */
  @Test
  public void testCacheEvents() {
    TestCacheListener listener = new TestCacheListener() {
      @Override
      public void afterCreate2(EntryEvent event) {}
    };

    region.getAttributesMutator().addCacheListener(listener);

    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, "value"));
    assertTrue(listener.wasInvoked());

    listener = new TestCacheListener() {
      @Override
      public void close2() {}
    };

    region.getAttributesMutator().addCacheListener(listener);

    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.get(i, "value"));

    assertFalse(listener.wasInvoked());

    listener = new TestCacheListener() {
      @Override
      public void afterUpdate2(EntryEvent event) {
        assertEquals(null, event.getOldValue());
        assertEquals(false, event.isOldValueAvailable());
      }
    };

    region.getAttributesMutator().addCacheListener(listener);

    region.put(0, "value");
    assertTrue(listener.wasInvoked());
  }

  /**
   * Tests iterating over all of the values when some have been overflowed.
   */
  @Test
  public void testValues() {
    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, "value"));

    Collection values = region.values();
    assertEquals(MAX_ENTRIES + 1, values.size());

    for (Object value : values) {
      assertEquals("value", value);
    }
  }

  /**
   * Tests for region.evictValue().
   */
  @Test
  public void testRegionEvictValue() {
    region = cache.<Integer, String>createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT)
        .setDiskSynchronous(true).setDiskStoreName(diskStore.getName()).create(REGION_NAME + 2);
    diskRegion = ((InternalRegion) region).getDiskRegion();

    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, "value"));
    ((LocalRegion) region).evictValue(2);

    Object value = ((LocalRegion) region).getValueInVM(2);
    assertNull(value);
  }

  /**
   * Tests calling region.evictValue() on region with eviction-attribute set.
   */
  @Test
  public void testEvictValueOnRegionWithEvictionAttributes() {
    region = cache.<Integer, String>createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT)
        .setDiskSynchronous(true)
        .setDiskStoreName(diskStore.getName()).setEvictionAttributes(EvictionAttributes
            .createLRUEntryAttributes(MAX_ENTRIES, EvictionAction.OVERFLOW_TO_DISK))
        .create(REGION_NAME + 2);
    diskRegion = ((InternalRegion) region).getDiskRegion();

    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, "value"));
    assertThatThrownBy(() -> ((LocalRegion) region).evictValue(3))
        .isInstanceOf(IllegalStateException.class);
  }

  /**
   * Tests that the disk region statistics are updated correctly for persist backup regions.
   */
  @Test
  public void testBackupStatistics() {
    region = cache.<Integer, String>createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT)
        .setScope(Scope.LOCAL).setDiskSynchronous(true).setDiskStoreName(diskStore.getName())
        .create(REGION_NAME + 2);
    diskRegion = ((InternalRegion) region).getDiskRegion();

    DiskRegionStats diskStats = diskRegion.getStats();

    assertEquals(0, diskStats.getWrites());
    assertEquals(0, diskStats.getNumEntriesInVM());
    assertEquals(0, diskStats.getReads());
    assertEquals(0, diskStats.getNumOverflowOnDisk());

    region.put(0, "value");

    assertEquals(1, diskStats.getWrites());
    assertEquals(1, diskStats.getNumEntriesInVM());
    assertEquals(0, diskStats.getReads());
    assertEquals(0, diskStats.getNumOverflowOnDisk());

    region.put(1, "value");

    assertEquals(2, diskStats.getWrites());
    assertEquals(2, diskStats.getNumEntriesInVM());
    assertEquals(0, diskStats.getReads());
    assertEquals(0, diskStats.getNumOverflowOnDisk());

    region.localDestroy(1);

    // destroy becomes a tombstone
    assertEquals(3, diskStats.getWrites());
    assertEquals(1, diskStats.getNumEntriesInVM());
    assertEquals(0, diskStats.getReads());

    region.put(2, "value");
    region.localDestroy(2);
    assertEquals(1, region.keySet().size());
  }

  public void assertArrayEquals(Object expected, Object v) {
    assertEquals(expected.getClass(), v.getClass());
    int vLength = Array.getLength(v);
    assertEquals(Array.getLength(expected), vLength);
    for (int i = 0; i < vLength; i++) {
      assertEquals(Array.get(expected, i), Array.get(v, i));
    }
  }

  @Test
  public void testBackup() {
    region = cache.<Integer, String>createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT)
        .setScope(Scope.LOCAL).setDiskSynchronous(true)
        .setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK))
        .setDiskStoreName(diskStore.getName()).create(REGION_NAME + 2);
    diskRegion = ((InternalRegion) region).getDiskRegion();

    region.put(0, "destroyed");
    region.localDestroy(0);


    // test invalidate
    region.put(1, "invalidated");
    region.invalidate(1);
    assertTrue(region.containsKey(1) && !region.containsValueForKey(1));

    // test local-invalidate
    region.put(2, "localInvalidate");
    region.localInvalidate(2);
    assertTrue(region.containsKey(2) && !region.containsValueForKey(2));

    // test modification
    region.put(3, "originalValue");
    region.put(3, "modified");
    assertEquals("modified", region.get(3));
    assertEquals(3, region.keySet().size());

    cache.close();
    cache = new CacheFactory().create();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setDiskDirs(new File[] {diskDir});
    diskStore = dsf.create(REGION_NAME);

    region = cache.<Integer, String>createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT)
        .setScope(Scope.LOCAL).setDiskSynchronous(true)
        .setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK))
        .setDiskStoreName(diskStore.getName()).create(REGION_NAME + 2);

    assertEquals(3, region.keySet().size());
    assertTrue(region.containsKey(1) && !region.containsValueForKey(1));
    assertTrue(region.containsKey(2) && !region.containsValueForKey(2));
    assertEquals("modified", region.get(3));
  }

  /**
   * Tests getting the {@linkplain org.apache.geode.cache.Region.Entry#getValue values} of region
   * entries that have been overflowed.
   */
  @Test
  public void testRegionEntryValues() {
    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, Integer.toString(i)));

    Set<Region.Entry<?, ?>> entries = region.entrySet(false);
    assertEquals(MAX_ENTRIES + 1, entries.size());

    for (Region.Entry<?, ?> entry : entries) {
      assertEquals(entry.getKey(), Integer.parseInt((String) entry.getValue()));
    }
  }

  /**
   * Tests that once an overflowed entry is {@linkplain Region#invalidate invalidated} its value is
   * gone.
   */
  @Test
  public void testInvalidate() {
    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, "value"));

    region.invalidate(0);
    assertNull(region.get(0));
  }

  /**
   * Tests that the updated value gets overflowed
   */
  @Test
  public void testOverflowUpdatedValue() {
    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, "value"));

    region.put(0, "newValue");
    assertEquals("newValue", region.get(0));

    // Iterate over a bunch of stuff to cause the updated entry to be
    // overflowed
    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.get(i));

    // Make sure that the updated value got written to disk
    assertEquals("newValue", region.get(0));
  }

  /**
   * Tests that the "test hook" {@link DiskRegionStats} work as advertised.
   */
  @Test
  public void testTestHookStatistics() {
    diskRegion = ((InternalRegion) region).getDiskRegion();

    DiskRegionStats diskStats = diskRegion.getStats();

    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, "value"));

    assertEquals(1, diskStats.getNumOverflowOnDisk());

    // Net change of zero
    region.get(0);
    assertEquals(((LocalRegion) region).entryCount(),
        diskStats.getNumEntriesInVM() + diskStats.getNumOverflowOnDisk());
    assertEquals(MAX_ENTRIES, diskStats.getNumEntriesInVM());
    assertEquals(1, diskStats.getNumOverflowOnDisk());

    // Kick out 4 entries
    region.put(MAX_ENTRIES + 1, "value");
    region.put(MAX_ENTRIES + 2, "value");
    region.put(MAX_ENTRIES + 3, "value");
    region.put(MAX_ENTRIES + 4, "value");
    assertEquals(((LocalRegion) region).entryCount(),
        diskStats.getNumEntriesInVM() + diskStats.getNumOverflowOnDisk());
    assertEquals(MAX_ENTRIES, diskStats.getNumEntriesInVM());
    assertEquals(5, diskStats.getNumOverflowOnDisk());

    // Make sure invalidate of inVM entry changes inVM count but not disk
    region.invalidate(MAX_ENTRIES + 1);
    assertEquals(((LocalRegion) region).entryCount() - 1,
        diskStats.getNumEntriesInVM() + diskStats.getNumOverflowOnDisk());
    assertEquals(MAX_ENTRIES - 1, diskStats.getNumEntriesInVM());
    assertEquals(5, diskStats.getNumOverflowOnDisk());

    // Make sure local-invalidate of inVM entry changes inVM count but not disk
    region.localInvalidate(MAX_ENTRIES + 2);
    assertEquals(((LocalRegion) region).entryCount() - 2,
        diskStats.getNumEntriesInVM() + diskStats.getNumOverflowOnDisk());
    assertEquals(MAX_ENTRIES - 2, diskStats.getNumEntriesInVM());
    assertEquals(5, diskStats.getNumOverflowOnDisk());

    // Make sure destroy of invalid entry does not change inVM or onDisk but changes entry count
    region.destroy(MAX_ENTRIES + 1);
    // ((LocalRegion)region).dumpBackingMap();
    assertEquals(((LocalRegion) region).entryCount() - 1,
        diskStats.getNumEntriesInVM() + diskStats.getNumOverflowOnDisk());
    assertEquals(MAX_ENTRIES - 2, diskStats.getNumEntriesInVM());
    assertEquals(5, diskStats.getNumOverflowOnDisk());

    // Make sure destroy of inVM entry does change inVM but not onDisk
    region.destroy(MAX_ENTRIES + 3);
    assertEquals(((LocalRegion) region).entryCount() - 1,
        diskStats.getNumEntriesInVM() + diskStats.getNumOverflowOnDisk());
    assertEquals(MAX_ENTRIES - 3, diskStats.getNumEntriesInVM());
    assertEquals(5, diskStats.getNumOverflowOnDisk());

    // Destroy an entry that has been overflowed
    region.destroy(1);
    assertEquals(((LocalRegion) region).entryCount() - 1,
        diskStats.getNumEntriesInVM() + diskStats.getNumOverflowOnDisk());
    assertEquals(MAX_ENTRIES - 3, diskStats.getNumEntriesInVM());
    assertEquals(4, diskStats.getNumOverflowOnDisk());
  }

  /**
   * Tests the {@link LocalRegion#getValueInVM getValueInVM} and {@link LocalRegion#getValueOnDisk
   * getValueOnDisk} methods that were added for testing.
   */
  @Test
  public void testLowLevelGetMethods() {
    IntStream.range(0, MAX_ENTRIES + 1).forEach((i) -> region.put(i, "value"));

    Integer key = 0;
    assertNull(((LocalRegion) region).getValueInVM(key));
    String valueOnDisk = (String) ((LocalRegion) region).getValueOnDisk(key);
    assertNotNull(valueOnDisk);
    assertEquals("value", valueOnDisk);

    region.get(key);

    CachedDeserializable cd = (CachedDeserializable) ((LocalRegion) region).getValueInVM(key);
    valueOnDisk = (String) cd.getValue();
    assertNotNull(valueOnDisk);
    assertEquals("value", valueOnDisk);

    valueOnDisk = (String) ((LocalRegion) region).getValueOnDisk(key);
    assertNotNull(valueOnDisk);
    assertEquals("value", valueOnDisk);
  }

  /**
   * Tests disk overflow with an entry-based {@link CountLRUEviction}.
   */
  @Test
  public void testLRUCapacityController() throws CacheException {

    DiskRegionStats diskStats = diskRegion.getStats();
    EvictionCounters lruStats = getLRUStats(region);

    IntStream.range(0, MAX_ENTRIES).forEach((i) -> region.put(i, "value"));

    assertEquals(0, lruStats.getEvictions());
    assertEquals(0, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(0, diskStats.getNumOverflowOnDisk());

    // Add a new value
    region.put(MAX_ENTRIES, "value");
    assertEquals(MAX_ENTRIES, lruStats.getCounter());
    assertEquals(1, lruStats.getEvictions());
    assertEquals(1, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(1, diskStats.getNumOverflowOnDisk());
    assertEquals(MAX_ENTRIES, diskStats.getNumEntriesInVM());

    // Add another new value
    region.put(MAX_ENTRIES + 1, "value");
    assertEquals(MAX_ENTRIES, lruStats.getCounter());
    assertEquals(2, lruStats.getEvictions());
    assertEquals(2, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(2, diskStats.getNumOverflowOnDisk());
    assertEquals(MAX_ENTRIES, diskStats.getNumEntriesInVM());

    // Replace a value
    region.put(MAX_ENTRIES - 1, "value");
    assertEquals(MAX_ENTRIES, lruStats.getCounter());
    assertEquals(2, lruStats.getEvictions());
    assertEquals(2, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(2, diskStats.getNumOverflowOnDisk());
    assertEquals(MAX_ENTRIES, diskStats.getNumEntriesInVM());
  }

  /**
   * Tests a disk-based region with an {@link CountLRUEviction} with size 1 and an eviction action
   * of "overflow".
   */
  @Test
  public void testLRUCacheLoader() throws CacheException {
    region.getAttributesMutator().setCacheLoader(new CacheLoader() {
      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        return "LOADED VALUE";
      }

      @Override
      public void close() {}

    });

    EvictionCounters lruStats = getLRUStats(region);

    IntStream.range(0, MAX_ENTRIES).forEach((i) -> region.put(i, "value"));
    assertEquals(MAX_ENTRIES, lruStats.getCounter());
    assertEquals(0, lruStats.getEvictions());

    IntStream.range(1, MAX_ENTRIES).forEach((i) -> region.put(i, "value"));

    region.put(MAX_ENTRIES, "value");

    assertEquals(MAX_ENTRIES, lruStats.getCounter());
    assertEquals(1, lruStats.getEvictions());

    region.get(MAX_ENTRIES + 1);
    assertEquals(MAX_ENTRIES, lruStats.getCounter());
    assertEquals(2, lruStats.getEvictions());
  }
}
