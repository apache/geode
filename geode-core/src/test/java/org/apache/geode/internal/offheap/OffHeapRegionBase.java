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
package org.apache.geode.internal.offheap;

import org.apache.geode.OutOfOffHeapMemoryException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.compression.Compressor;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.entries.OffHeapRegionEntry;
import org.apache.geode.internal.offheap.annotations.OffHeapIdentifier;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.WaitCriterion;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * Basic test of regions that use off heap storage. Subclasses exist for the different types of
 * offheap store.
 * 
 *
 */
public abstract class OffHeapRegionBase {

  public abstract void configureOffHeapStorage();

  public abstract void unconfigureOffHeapStorage();

  public abstract int perObjectOverhead();

  private GemFireCacheImpl createCache() {
    return createCache(false);
  }

  private GemFireCacheImpl createCache(boolean isPersistent) {
    configureOffHeapStorage();
    Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ConfigurationProperties.OFF_HEAP_MEMORY_SIZE, getOffHeapMemorySize());
    GemFireCacheImpl result =
        (GemFireCacheImpl) new CacheFactory(props).setPdxPersistent(isPersistent).create();
    unconfigureOffHeapStorage();
    return result;
  }

  @After
  public void cleanUp() {
    File dir = new File(".");
    File[] files = dir.listFiles(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("BACKUP");
      }

    });
    for (File file : files) {
      file.delete();
    }
  }

  private void closeCache(GemFireCacheImpl gfc, boolean keepOffHeapAllocated) {
    gfc.close();
    if (!keepOffHeapAllocated) {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
    // TODO cleanup default disk store files
  }

  protected abstract String getOffHeapMemorySize();

  @Test
  public void testSizeAllocation() {
    // prevent cache from closing in reaction to ooom
    System.setProperty(OffHeapStorage.STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY, "true");
    GemFireCacheImpl gfc = createCache();
    try {
      MemoryAllocator ma = gfc.getOffHeapStore();
      assertNotNull(ma);
      final long offHeapSize = ma.getFreeMemory();
      assertEquals(0, ma.getUsedMemory());
      StoredObject mc1 = ma.allocate(64);
      assertEquals(64 + perObjectOverhead(), ma.getUsedMemory());
      assertEquals(offHeapSize - (64 + perObjectOverhead()), ma.getFreeMemory());
      mc1.release();
      assertEquals(offHeapSize, ma.getFreeMemory());
      assertEquals(0, ma.getUsedMemory());
      // do an allocation larger than the slab size
      // TODO: currently the defragment will produce slabs bigger than the max slab size
      // (see the todo comment on defragment() in FreeListManager).
      // So we request 20m here since that it the total size.
      try {
        ma.allocate(1024 * 1024 * 20);
        fail("Expected an out of heap exception");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      assertEquals(0, ma.getUsedMemory());
      assertFalse(gfc.isClosed());
    } finally {
      System.clearProperty(OffHeapStorage.STAY_CONNECTED_ON_OUTOFOFFHEAPMEMORY_PROPERTY);
      closeCache(gfc, false);
    }
  }

  public void keep_testOutOfOffHeapMemoryErrorClosesCache() {
    // this test is redundant but may be useful
    final GemFireCacheImpl gfc = createCache();
    try {
      MemoryAllocator ma = gfc.getOffHeapStore();
      assertNotNull(ma);
      final long offHeapSize = ma.getFreeMemory();
      assertEquals(0, ma.getUsedMemory());
      StoredObject mc1 = ma.allocate(64);
      assertEquals(64 + perObjectOverhead(), ma.getUsedMemory());
      assertEquals(offHeapSize - (64 + perObjectOverhead()), ma.getFreeMemory());
      mc1.release();
      assertEquals(offHeapSize, ma.getFreeMemory());
      assertEquals(0, ma.getUsedMemory());
      // do an allocation larger than the slab size
      try {
        ma.allocate(1024 * 1024 * 10);
        fail("Expected an out of heap exception");
      } catch (OutOfOffHeapMemoryException expected) {
        // passed
      }
      assertEquals(0, ma.getUsedMemory());

      final WaitCriterion waitForDisconnect = new WaitCriterion() {
        public boolean done() {
          return gfc.isClosed();
        }

        public String description() {
          return "Waiting for disconnect to complete";
        }
      };
      org.apache.geode.test.dunit.Wait.waitForCriterion(waitForDisconnect, 10 * 1000, 100, true);

      assertTrue(gfc.isClosed());
    } finally {
      closeCache(gfc, false);
    }
  }

  @Test
  public void testByteArrayAllocation() {
    GemFireCacheImpl gfc = createCache();
    try {
      MemoryAllocator ma = gfc.getOffHeapStore();
      assertNotNull(ma);
      final long offHeapSize = ma.getFreeMemory();
      assertEquals(0, ma.getUsedMemory());
      byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
      StoredObject mc1 = (StoredObject) ma.allocateAndInitialize(data, false, false);
      assertEquals(data.length + perObjectOverhead(), ma.getUsedMemory());
      assertEquals(offHeapSize - (data.length + perObjectOverhead()), ma.getFreeMemory());
      byte[] data2 = new byte[data.length];
      mc1.readDataBytes(0, data2);
      assertTrue(Arrays.equals(data, data2));
      mc1.release();
      assertEquals(offHeapSize, ma.getFreeMemory());
      assertEquals(0, ma.getUsedMemory());
      // try some small byte[] that don't need to be stored off heap.
      data = new byte[] {1, 2, 3, 4, 5, 6, 7};
      StoredObject so1 = ma.allocateAndInitialize(data, false, false);
      assertEquals(0, ma.getUsedMemory());
      assertEquals(offHeapSize, ma.getFreeMemory());
      data2 = new byte[data.length];
      data2 = (byte[]) so1.getDeserializedForReading();
      assertTrue(Arrays.equals(data, data2));
    } finally {
      closeCache(gfc, false);
    }
  }

  private void doRegionTest(final RegionShortcut rs, final String rName) {
    doRegionTest(rs, rName, false/* compressed */);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void doRegionTest(final RegionShortcut rs, final String rName, boolean compressed) {
    boolean isPersistent = rs == RegionShortcut.LOCAL_PERSISTENT
        || rs == RegionShortcut.REPLICATE_PERSISTENT || rs == RegionShortcut.PARTITION_PERSISTENT;
    GemFireCacheImpl gfc = createCache(isPersistent);
    Region r = null;
    try {
      gfc.setCopyOnRead(true);
      final MemoryAllocator ma = gfc.getOffHeapStore();
      assertNotNull(ma);
      Awaitility.await().atMost(60, TimeUnit.SECONDS)
          .until(() -> assertEquals(0, ma.getUsedMemory()));
      Compressor compressor = null;
      if (compressed) {
        compressor = SnappyCompressor.getDefaultInstance();
      }
      r = gfc.createRegionFactory(rs).setOffHeap(true).setCompressor(compressor).create(rName);
      assertEquals(true, r.isEmpty());
      assertEquals(0, ma.getUsedMemory());
      Object data = new Integer(123456789);
      r.put("key1", data);
      // System.out.println("After put of Integer value off heap used memory=" +
      // ma.getUsedMemory());
      assertTrue(ma.getUsedMemory() == 0);
      assertEquals(data, r.get("key1"));
      r.invalidate("key1");
      assertEquals(0, ma.getUsedMemory());
      r.put("key1", data);
      assertTrue(ma.getUsedMemory() == 0);
      long usedBeforeUpdate = ma.getUsedMemory();
      r.put("key1", data);
      assertEquals(usedBeforeUpdate, ma.getUsedMemory());
      assertEquals(data, r.get("key1"));
      r.destroy("key1");
      assertEquals(0, ma.getUsedMemory());

      data = new Long(0x007FFFFFL);
      r.put("key1", data);
      if (!compressed) {
        assertTrue(ma.getUsedMemory() == 0);
      }
      assertEquals(data, r.get("key1"));
      data = new Long(0xFF8000000L);
      r.put("key1", data);
      if (!compressed) {
        assertTrue(ma.getUsedMemory() == 0);
      }
      assertEquals(data, r.get("key1"));


      // now lets set data to something that will be stored offheap
      data = new Long(Long.MAX_VALUE);
      r.put("key1", data);
      assertEquals(data, r.get("key1"));
      // System.out.println("After put of Integer value off heap used memory=" +
      // ma.getUsedMemory());
      assertTrue(ma.getUsedMemory() > 0);
      data = new Long(Long.MIN_VALUE);
      r.put("key1", data);
      assertEquals(data, r.get("key1"));
      // System.out.println("After put of Integer value off heap used memory=" +
      // ma.getUsedMemory());
      assertTrue(ma.getUsedMemory() > 0);
      r.invalidate("key1");
      assertEquals(0, ma.getUsedMemory());
      r.put("key1", data);
      assertTrue(ma.getUsedMemory() > 0);
      usedBeforeUpdate = ma.getUsedMemory();
      r.put("key1", data);
      assertEquals(usedBeforeUpdate, ma.getUsedMemory());
      assertEquals(data, r.get("key1"));
      r.destroy("key1");
      assertEquals(0, ma.getUsedMemory());

      // confirm that byte[] do use off heap
      {
        byte[] originalBytes = new byte[1024];
        Object oldV = r.put("byteArray", originalBytes);
        long startUsedMemory = ma.getUsedMemory();
        assertEquals(null, oldV);
        byte[] readBytes = (byte[]) r.get("byteArray");
        if (originalBytes == readBytes) {
          fail("Expected different byte[] identity");
        }
        if (!Arrays.equals(readBytes, originalBytes)) {
          fail("Expected byte array contents to be equal");
        }
        assertEquals(startUsedMemory, ma.getUsedMemory());
        oldV = r.put("byteArray", originalBytes);
        if (!compressed) {
          assertEquals(null, oldV); // we default to old value being null for offheap
        }
        assertEquals(startUsedMemory, ma.getUsedMemory());

        readBytes = (byte[]) r.putIfAbsent("byteArray", originalBytes);
        if (originalBytes == readBytes) {
          fail("Expected different byte[] identity");
        }
        if (!Arrays.equals(readBytes, originalBytes)) {
          fail("Expected byte array contents to be equal");
        }
        assertEquals(startUsedMemory, ma.getUsedMemory());
        if (!r.replace("byteArray", readBytes, originalBytes)) {
          fail("Expected replace to happen");
        }
        assertEquals(startUsedMemory, ma.getUsedMemory());
        byte[] otherBytes = new byte[1024];
        otherBytes[1023] = 1;
        if (r.replace("byteArray", otherBytes, originalBytes)) {
          fail("Expected replace to not happen");
        }
        if (r.replace("byteArray", "bogus string", originalBytes)) {
          fail("Expected replace to not happen");
        }
        if (r.remove("byteArray", "bogus string")) {
          fail("Expected remove to not happen");
        }
        assertEquals(startUsedMemory, ma.getUsedMemory());

        if (!r.remove("byteArray", originalBytes)) {
          fail("Expected remove to happen");
        }
        assertEquals(0, ma.getUsedMemory());
        oldV = r.putIfAbsent("byteArray", "string value");
        assertEquals(null, oldV);
        assertEquals("string value", r.get("byteArray"));
        if (r.replace("byteArray", "string valuE", originalBytes)) {
          fail("Expected replace to not happen");
        }
        if (!r.replace("byteArray", "string value", originalBytes)) {
          fail("Expected replace to happen");
        }
        oldV = r.destroy("byteArray"); // we default to old value being null for offheap
        if (!compressed) {
          assertEquals(null, oldV);
        }
        MyCacheListener listener = new MyCacheListener();
        r.getAttributesMutator().addCacheListener(listener);
        try {
          Object valueToReplace = "string value1";
          r.put("byteArray", valueToReplace);
          assertEquals(null, listener.ohOldValue);
          if (!compressed) {
            assertEquals("string value1", listener.ohNewValue.getDeserializedForReading());
            valueToReplace = listener.ohNewValue;
          }
          if (!r.replace("byteArray", valueToReplace, "string value2")) {
            fail("expected replace to happen");
          }
          if (!compressed) {
            assertEquals("string value2", listener.ohNewValue.getDeserializedForReading());
            assertEquals("string value1", listener.ohOldValue.getDeserializedForReading());
          }
          // make sure that a custom equals/hashCode are not used when comparing values.

        } finally {
          r.getAttributesMutator().removeCacheListener(listener);
        }
      }
      assertTrue(ma.getUsedMemory() > 0);
      {
        Object key = "MyValueWithPartialEquals";
        MyValueWithPartialEquals v1 = new MyValueWithPartialEquals("s1");
        MyValueWithPartialEquals v2 = new MyValueWithPartialEquals("s2");
        MyValueWithPartialEquals v3 = new MyValueWithPartialEquals("s1");
        r.put(key, v1);
        try {
          if (r.replace(key, v2, "should not happen")) {
            fail("v1 should not be equal to v2 on an offheap region");
          }
          if (!r.replace(key, v3, "should happen")) {
            fail("v1 should be equal to v3 on an offheap region");
          }
          r.put(key, v1);
          if (r.remove(key, v2)) {
            fail("v1 should not be equal to v2 on an offheap region");
          }
          if (!r.remove(key, v3)) {
            fail("v1 should be equal to v3 on an offheap region");
          }
        } finally {
          r.remove(key);
        }
      }
      {
        Object key = "MyPdxWithPartialEquals";
        MyPdxWithPartialEquals v1 = new MyPdxWithPartialEquals("s", "1");
        MyPdxWithPartialEquals v2 = new MyPdxWithPartialEquals("s", "2");
        MyPdxWithPartialEquals v3 = new MyPdxWithPartialEquals("t", "1");
        r.put(key, v1);
        try {
          if (r.replace(key, v3, "should not happen")) {
            fail("v1 should not be equal to v3 on an offheap region");
          }
          if (!r.replace(key, v2, "should happen")) {
            fail("v1 should be equal to v2 on an offheap region");
          }
          r.put(key, v1);
          if (r.remove(key, v3)) {
            fail("v1 should not be equal to v3 on an offheap region");
          }
          if (!r.remove(key, v2)) {
            fail("v1 should be equal to v2 on an offheap region");
          }
        } finally {
          r.remove(key);
        }
      }
      byte[] value = new byte[1024];
      /* while (value != null) */ {
        r.put("byteArray", value);
      }
      r.remove("byteArray");
      assertEquals(0, ma.getUsedMemory());

      r.put("key1", data);
      assertTrue(ma.getUsedMemory() > 0);
      r.invalidateRegion();
      assertEquals(0, ma.getUsedMemory());

      r.put("key1", data);
      assertTrue(ma.getUsedMemory() > 0);
      try {
        r.clear();
        Awaitility.await().atMost(60, TimeUnit.SECONDS)
            .until(() -> assertEquals(0, ma.getUsedMemory()));
      } catch (UnsupportedOperationException ok) {
      }

      r.put("key1", data);
      assertTrue(ma.getUsedMemory() > 0);
      if (r.getAttributes().getDataPolicy().withPersistence()) {
        r.put("key2", Integer.valueOf(1234567890));
        r.put("key3", new Long(0x007FFFFFL));
        r.put("key4", new Long(0xFF8000000L));
        assertEquals(4, r.size());
        r.close();
        Awaitility.await().atMost(60, TimeUnit.SECONDS)
            .until(() -> assertEquals(0, ma.getUsedMemory()));
        // simple test of recovery
        r = gfc.createRegionFactory(rs).setOffHeap(true).create(rName);
        assertEquals(4, r.size());
        assertEquals(data, r.get("key1"));
        assertEquals(Integer.valueOf(1234567890), r.get("key2"));
        assertEquals(new Long(0x007FFFFFL), r.get("key3"));
        assertEquals(new Long(0xFF8000000L), r.get("key4"));
        closeCache(gfc, true);
        assertEquals(0, ma.getUsedMemory());
        gfc = createCache();
        if (ma != gfc.getOffHeapStore()) {
          fail("identity of offHeapStore changed when cache was recreated");
        }
        r = gfc.createRegionFactory(rs).setOffHeap(true).create(rName);
        assertTrue(ma.getUsedMemory() > 0);
        assertEquals(4, r.size());
        assertEquals(data, r.get("key1"));
        assertEquals(Integer.valueOf(1234567890), r.get("key2"));
        assertEquals(new Long(0x007FFFFFL), r.get("key3"));
        assertEquals(new Long(0xFF8000000L), r.get("key4"));
      }

      r.destroyRegion();
      Awaitility.await().atMost(60, TimeUnit.SECONDS)
          .until(() -> assertEquals(0, ma.getUsedMemory()));
    } finally {
      if (r != null && !r.isDestroyed()) {
        r.destroyRegion();
      }
      closeCache(gfc, false);
    }

  }

  /**
   * This class has an equals that does not compare all its bytes.
   */
  private static class MyValueWithPartialEquals implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String value;

    public MyValueWithPartialEquals(String v) {
      this.value = v;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof MyValueWithPartialEquals) {
        MyValueWithPartialEquals o = (MyValueWithPartialEquals) other;
        // just compare the first char
        return this.value.charAt(0) == o.value.charAt(0);
      } else {
        return false;
      }
    }
  }
  /**
   * This class has an equals that does not compare all its bytes.
   */
  private static class MyPdxWithPartialEquals implements PdxSerializable {
    private String base;
    private String value;

    public MyPdxWithPartialEquals(String b, String v) {
      this.base = b;
      this.value = v;
    }

    public MyPdxWithPartialEquals() {}

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("base", this.base);
      writer.writeString("value", this.value);
      writer.markIdentityField("base");
    }

    @Override
    public void fromData(PdxReader reader) {
      this.base = reader.readString("base");
      this.value = reader.readString("value");
    }
  }

  @SuppressWarnings("rawtypes")
  private static class MyCacheListener extends CacheListenerAdapter {
    @Retained(OffHeapIdentifier.TEST_OFF_HEAP_REGION_BASE_LISTENER)
    public StoredObject ohOldValue;
    @Retained(OffHeapIdentifier.TEST_OFF_HEAP_REGION_BASE_LISTENER)
    public StoredObject ohNewValue;

    /**
     * This method retains both ohOldValue and ohNewValue
     */
    @Retained(OffHeapIdentifier.TEST_OFF_HEAP_REGION_BASE_LISTENER)
    private void setEventData(EntryEvent e) {
      close();
      EntryEventImpl event = (EntryEventImpl) e;
      this.ohOldValue = event.getOffHeapOldValue();
      this.ohNewValue = event.getOffHeapNewValue();
    }

    @Override
    public void afterCreate(EntryEvent e) {
      setEventData(e);
    }

    @Override
    public void afterDestroy(EntryEvent e) {
      setEventData(e);
    }

    @Override
    public void afterInvalidate(EntryEvent e) {
      setEventData(e);
    }

    @Override
    public void afterUpdate(EntryEvent e) {
      setEventData(e);
    }

    @Released(OffHeapIdentifier.TEST_OFF_HEAP_REGION_BASE_LISTENER)
    @Override
    public void close() {
      if (this.ohOldValue instanceof OffHeapStoredObject) {
        ((OffHeapStoredObject) this.ohOldValue).release();
      }
      if (this.ohNewValue instanceof OffHeapStoredObject) {
        ((OffHeapStoredObject) this.ohNewValue).release();
      }
    }
  }

  @Test
  public void testPR() {
    doRegionTest(RegionShortcut.PARTITION, "pr1");
  }

  @Test
  public void testPRCompressed() {
    doRegionTest(RegionShortcut.PARTITION, "pr2", true);
  }

  @Test
  public void testReplicate() {
    doRegionTest(RegionShortcut.REPLICATE, "rep1");
  }

  @Test
  public void testReplicateCompressed() {
    doRegionTest(RegionShortcut.REPLICATE, "rep2", true);
  }

  @Test
  public void testLocal() {
    doRegionTest(RegionShortcut.LOCAL, "local1");
  }

  @Test
  public void testLocalCompressed() {
    doRegionTest(RegionShortcut.LOCAL, "local2", true);
  }

  @Test
  public void testLocalPersistent() {
    doRegionTest(RegionShortcut.LOCAL_PERSISTENT, "localPersist1");
  }

  @Test
  public void testLocalPersistentCompressed() {
    doRegionTest(RegionShortcut.LOCAL_PERSISTENT, "localPersist2", true);
  }

  @Test
  public void testPRPersistent() {
    doRegionTest(RegionShortcut.PARTITION_PERSISTENT, "prPersist1");
  }

  @Test
  public void testPRPersistentCompressed() {
    doRegionTest(RegionShortcut.PARTITION_PERSISTENT, "prPersist2", true);
  }

  @Test
  public void testPersistentChangeFromHeapToOffHeap() {
    GemFireCacheImpl gfc = createCache(true);
    Region r = null;
    final String value = "value big enough to force off-heap storage";
    try {
      r = gfc.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT).setOffHeap(false)
          .create("changedFromHeapToOffHeap");
      r.put("key", value);
    } finally {
      closeCache(gfc, false);
    }
    gfc = createCache(true);
    try {
      r = gfc.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT).setOffHeap(true)
          .create("changedFromHeapToOffHeap");
      assertEquals(true, r.containsKey("key"));
      LocalRegion lr = (LocalRegion) r;
      RegionEntry re = lr.getRegionEntry("key");
      if (!(re instanceof OffHeapRegionEntry)) {
        fail("expected re to be instanceof OffHeapRegionEntry but it was a " + re.getClass());
      }
      assertEquals(value, r.get("key"));
    } finally {
      if (r != null && !r.isDestroyed()) {
        r.destroyRegion();
      }
      closeCache(gfc, false);
    }
  }

  @Test
  public void testPersistentCompressorChange() {
    GemFireCacheImpl gfc = createCache(true);
    Region<Object, Object> r = null;
    String value = "value1";
    String key = "key";

    try {
      r = gfc.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT).setOffHeap(true)
          .setCompressor(new SnappyCompressor()).create("region1");
      r.put(key, value);
    } finally {
      closeCache(gfc, false);
    }

    gfc = createCache(true);
    try {
      r = gfc.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT).setOffHeap(true)
          .setCompressor(null).create("region1");
      assertEquals(true, r.containsKey(key));
      MemoryAllocatorImpl mai = MemoryAllocatorImpl.getAllocator();
      List<OffHeapStoredObject> orphans = mai.getLostChunks();
      if (orphans.size() > 0) {
        fail("expected no orphan detected, but gets orphan size " + orphans.size());
      }
      assertEquals(value, r.get(key));
    } finally {
      if (r != null && !r.isDestroyed()) {
        r.destroyRegion();
      }
      closeCache(gfc, false);
    }
  }
}
