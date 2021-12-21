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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.test.junit.categories.OffHeapTest;

/**
 * Basic integration tests for validating the off-heap implementation.
 *
 */
@Category({OffHeapTest.class})
public class OffHeapValidationJUnitTest {

  private GemFireCacheImpl cache;

  @Before
  public void setUp() throws Exception {
    cache = createCache();
  }

  @After
  public void tearDown() throws Exception {
    closeCache(cache);
  }

  protected GemFireCacheImpl createCache() {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(ConfigurationProperties.OFF_HEAP_MEMORY_SIZE, getOffHeapMemorySize());
    GemFireCacheImpl result = (GemFireCacheImpl) new CacheFactory(props).create();
    return result;
  }

  protected void closeCache(GemFireCacheImpl gfc) {
    gfc.close();
  }

  protected String getOffHeapMemorySize() {
    return "2m";
  }

  protected RegionShortcut getRegionShortcut() {
    return RegionShortcut.REPLICATE;
  }

  protected String getRegionName() {
    return "region1";
  }

  @Test
  public void testMemoryInspection() throws IOException {
    // validate initial state
    MemoryAllocator allocator = cache.getOffHeapStore();
    assertNotNull(allocator);
    MemoryInspector inspector = allocator.getMemoryInspector();
    assertNotNull(inspector);
    inspector.createSnapshot();
    try {
      MemoryBlock firstBlock = inspector.getFirstBlock();
      assertNotNull(firstBlock);
      assertEquals(1024 * 1024 * 2, firstBlock.getBlockSize());
      assertEquals("N/A", firstBlock.getDataType());
      assertEquals(-1, firstBlock.getFreeListId());
      assertTrue(firstBlock.getAddress() > 0);
      assertNull(firstBlock.getNextBlock());
      assertEquals(0, firstBlock.getRefCount());
      assertEquals(0, firstBlock.getSlabId());
      assertEquals(MemoryBlock.State.UNUSED, firstBlock.getState());
      assertFalse(firstBlock.isCompressed());
      assertFalse(firstBlock.isSerialized());
    } finally {
      inspector.clearSnapshot();
    }

    // create off-heap region
    Region<Object, Object> region = cache.createRegionFactory(getRegionShortcut())
        .setOffHeap(true).create(getRegionName());
    Region<Object, Object> compressedRegion = cache.createRegionFactory(getRegionShortcut())
        .setOffHeap(true).setCompressor(SnappyCompressor.getDefaultInstance())
        .create(getRegionName() + "Compressed");

    // perform some ops
    List<ExpectedValues> expected = new ArrayList<>();

    // Chunk.OFF_HEAP_HEADER_SIZE + 4 ?

    putString(region, expected);
    putCompressedString(compressedRegion, expected);
    putDate(region, expected);
    putByteArray(region, expected);
    putCompressedByteArray(compressedRegion, expected);
    putByteArrayArray(region, expected);
    putShortArray(region, expected);
    putStringArray(region, expected);
    putObjectArray(region, expected);
    putArrayList(region, expected);
    putLinkedList(region, expected);
    putHashSet(region, expected);
    putLinkedHashSet(region, expected);
    putHashMap(region, expected);
    putIdentityHashMap(region, expected);
    putHashtable(region, expected);
    putProperties(region, expected);
    putVector(region, expected);
    putStack(region, expected);
    putTreeMap(region, expected);
    putTreeSet(region, expected);
    putClass(region, expected);
    putUUID(region, expected);
    putTimestamp(region, expected);
    putSerializableClass(region, expected);

    // TODO: USER_DATA_SERIALIZABLE

    // TODO: PDX

    // TODO: PDX_ENUM

    // TODO: GEMFIRE_ENUM

    // TODO: PDX_INLINE_ENUM

    // validate inspection
    inspector.createSnapshot();
    try {
      MemoryBlock firstBlock = inspector.getFirstBlock();
      assertEquals(MemoryBlock.State.UNUSED, firstBlock.getState());

      // System.out.println(((MemoryAllocatorImpl)inspector).getSnapshot());

      // sort the ExpectedValues into the same order as the MemberBlocks from inspector
      Collections.sort(expected, new Comparator<ExpectedValues>() {
        @Override
        public int compare(ExpectedValues o1, ExpectedValues o2) {
          return Long.valueOf(o1.memoryAddress).compareTo(o2.memoryAddress);
        }
      });

      int i = 0;
      MemoryBlock block = firstBlock.getNextBlock();
      while (block != null) {
        ExpectedValues values = expected.get(i);
        assertEquals(i + ":" + values.dataType, values.blockSize, block.getBlockSize());
        assertEquals(i + ":" + values.dataType, values.dataType, block.getDataType());
        assertEquals(i + ":" + values.dataType, values.freeListId, block.getFreeListId());
        assertEquals(i + ":" + values.dataType, values.memoryAddress, block.getAddress());
        assertEquals(i + ":" + values.dataType, values.refCount, block.getRefCount());
        assertEquals(i + ":" + values.dataType, values.slabId, block.getSlabId());
        assertEquals(i + ":" + values.dataType, values.isCompressed, block.isCompressed());
        assertEquals(i + ":" + values.dataType, values.isSerialized, block.isSerialized());
        // compare block.getDataValue() but only for String types
        if (values.dataType.equals("java.lang.String")) {
          Object obj = block.getDataValue();
          assertNotNull(block.toString(), obj);
          assertTrue(obj instanceof String);
          assertEquals("this is a string", obj);
        }
        if ((values.dataType.contains("byte [")
            && values.dataType.lastIndexOf('[') == values.dataType.indexOf('['))
            || values.dataType.startsWith("compressed")) {
          assertTrue(
              "for dataType=" + values.dataType + " expected "
                  + Arrays.toString((byte[]) values.dataValue) + " but was "
                  + Arrays.toString((byte[]) block.getDataValue()),
              Arrays.equals((byte[]) values.dataValue, (byte[]) block.getDataValue()));
        } else if (values.dataType.contains("[")) {
          // TODO: multiple dimension arrays or non-byte arrays
        } else if (values.dataValue instanceof Collection) {
          int diff = joint((Collection<?>) values.dataValue, (Collection<?>) block.getDataValue());
          assertEquals(i + ":" + values.dataType, 0, diff);
        } else if (values.dataValue instanceof IdentityHashMap) {
          // TODO
        } else if (values.dataValue instanceof Map) {
          int diff = joint((Map<?, ?>) values.dataValue, (Map<?, ?>) block.getDataValue());
          assertEquals(i + ":" + values.dataType, 0, diff);
        } else {
          assertEquals(i + ":" + values.dataType, values.dataValue, block.getDataValue());
        }
        block = block.getNextBlock();
        i++;
      }
      assertEquals("All blocks: " + inspector.getAllBlocks(), expected.size(), i);
    } finally {
      inspector.clearSnapshot();
    }

    // perform more ops

    // validate more inspection

  }

  /**
   * Returns -1 if c1 is missing an element in c2, 1 if c2 is missing an element in c1, or 0 is they
   * contain the exact same elements.
   *
   * @throws NullPointerException if either c1 or c2 is null
   */
  private static int joint(Collection<?> c1, Collection<?> c2) {
    if (c1.size() < c2.size()) {
      return -1;
    } else if (c2.size() < c1.size()) {
      return 1;
    }
    Collection<Object> c3 = new ArrayList<>();
    c3.addAll(c1);
    c3.removeAll(c2);
    if (c3.size() > 0) {
      return -1;
    }
    c3.addAll(c2);
    c3.removeAll(c1);
    if (c3.size() > 0) {
      return 1;
    }
    return 0;
  }

  /**
   * Returns -1 if m1 is missing a key in m2, 1 if m2 is missing a key in m1, or 0 is they contain
   * the exact same keys.
   *
   * @throws NullPointerException if either c1 or c2 is null
   */
  private static int joint(Map<?, ?> m1, Map<?, ?> m2) {
    if (m1.size() < m2.size()) {
      return -1;
    } else if (m2.size() < m1.size()) {
      return 1;
    }
    Collection<Object> c3 = new ArrayList<>();
    c3.addAll(m1.keySet());
    c3.removeAll(m2.keySet());
    if (c3.size() > 0) {
      return -1;
    }
    c3.addAll(m2.keySet());
    c3.removeAll(m1.keySet());
    if (c3.size() > 0) {
      return 1;
    }
    return 0;
  }

  private long getMemoryAddress(Region region, String key) {
    Object entry = ((LocalRegion) region).getRegionEntry(key).getValue();
    assertTrue(entry instanceof OffHeapStoredObject);
    long memoryAddress = ((OffHeapStoredObject) entry).getAddress();
    assertTrue(memoryAddress > 0);
    return memoryAddress;
  }

  private void putString(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyString";
    String value = "this is a string";
    region.put(key, value);
    expected.add(new ExpectedValues(value, value.length() * 2, "java.lang.String", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putCompressedString(Region<Object, Object> region, List<ExpectedValues> expected)
      throws IOException {
    String key = "keyString";
    String value = "this is a string";
    region.put(key, value);
    HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(value, hdos);
    byte[] uncompressedBytes = hdos.toByteArray();
    byte[] expectedValue = SnappyCompressor.getDefaultInstance().compress(uncompressedBytes);
    expected.add(
        new ExpectedValues(expectedValue, 32, "compressed object of size " + expectedValue.length,
            -1, getMemoryAddress(region, key), 1, 0, true, true));
  }

  private void putDate(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyDate";
    Date value = new Date();
    region.put(key, value);
    expected.add(new ExpectedValues(value, 24, "java.util.Date", -1, getMemoryAddress(region, key),
        1, 0, false, true));
  }

  private void putByteArray(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyByteArray";
    byte[] value = new byte[10];
    region.put(key, value);
    expected.add(new ExpectedValues(value, 24, "byte[10]", -1, getMemoryAddress(region, key), 1, 0,
        false, false));
  }

  private void putCompressedByteArray(Region<Object, Object> region, List<ExpectedValues> expected)
      throws IOException {
    String key = "keyByteArray";
    byte[] value = new byte[10];
    region.put(key, value);
    byte[] expectedValue = SnappyCompressor.getDefaultInstance().compress(value);
    expected
        .add(new ExpectedValues(expectedValue, 24, "compressed byte[" + expectedValue.length + "]",
            -1, getMemoryAddress(region, key), 1, 0, true, false));
  }

  private void putByteArrayArray(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyByteArrayArray";
    byte[][] value = new byte[10][10];
    region.put(key, value);
    expected.add(new ExpectedValues(value, 120, "byte[][]", -1, getMemoryAddress(region, key), 1, 0,
        false, true));
  }

  private void putShortArray(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyShortArray(";
    short[] value = new short[10];
    region.put(key, value);
    expected.add(new ExpectedValues(value, 32, "short[]", -1, getMemoryAddress(region, key), 1, 0,
        false, true));
  }

  private void putStringArray(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyStringArray";
    String[] value = new String[10];
    region.put(key, value);
    expected.add(new ExpectedValues(value, 24, "java.lang.String[]", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putObjectArray(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyObjectArray";
    Object[] value = new Object[10];
    region.put(key, value);
    expected.add(new ExpectedValues(value, 40, "java.lang.Object[]", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putArrayList(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyArrayList";
    ArrayList<Object> value = new ArrayList<>();
    value.add("string 1");
    value.add("string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 32, "java.util.ArrayList", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putLinkedList(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyLinkedList";
    LinkedList<Object> value = new LinkedList<>();
    value.add("string 1");
    value.add("string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 32, "java.util.LinkedList", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putHashSet(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyHashSet";
    HashSet<Object> value = new HashSet<>();
    value.add("string 1");
    value.add("string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 32, "java.util.HashSet", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putLinkedHashSet(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyLinkedHashSet";
    LinkedHashSet<Object> value = new LinkedHashSet<>();
    value.add("string 1");
    value.add("string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 32, "java.util.LinkedHashSet", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putHashMap(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyHashMap";
    HashMap<Object, Object> value = new HashMap<>();
    value.put("1", "string 1");
    value.put("2", "string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 40, "java.util.HashMap", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putIdentityHashMap(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyIdentityHashMap";
    IdentityHashMap<Object, Object> value = new IdentityHashMap<>();
    value.put("1", "string 1");
    value.put("2", "string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 40, "java.util.IdentityHashMap", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putHashtable(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyHashtable";
    Hashtable<Object, Object> value = new Hashtable<>();
    value.put("1", "string 1");
    value.put("2", "string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 40, "java.util.Hashtable", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putProperties(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyProperties";
    Properties value = new Properties();
    value.put("1", "string 1");
    value.put("2", "string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 40, "java.util.Properties", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putVector(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyVector";
    Vector<String> value = new Vector<>();
    value.add("string 1");
    value.add("string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 32, "java.util.Vector", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putStack(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyStack";
    Stack<String> value = new Stack<>();
    value.add("string 1");
    value.add("string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 32, "java.util.Stack", -1, getMemoryAddress(region, key),
        1, 0, false, true));
  }

  private void putTreeMap(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyTreeMap";
    TreeMap<String, String> value = new TreeMap<>();
    value.put("1", "string 1");
    value.put("2", "string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 48, "java.util.TreeMap", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putTreeSet(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyTreeSet";
    TreeSet<String> value = new TreeSet<>();
    value.add("string 1");
    value.add("string 2");
    region.put(key, value);
    expected.add(new ExpectedValues(value, 40, "java.util.TreeSet", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putClass(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyClass";
    Class<String> value = String.class;
    region.put(key, value);
    expected.add(new ExpectedValues(value, 32, "java.lang.Class", -1, getMemoryAddress(region, key),
        1, 0, false, true));
  }

  private void putUUID(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyUUID";
    UUID value = UUID.randomUUID();
    region.put(key, value);
    expected.add(new ExpectedValues(value, 32, "java.util.UUID", -1, getMemoryAddress(region, key),
        1, 0, false, true));
  }

  private void putTimestamp(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keyTimestamp";
    Timestamp value = new Timestamp(System.currentTimeMillis());
    region.put(key, value);
    expected.add(new ExpectedValues(value, 24, "java.sql.Timestamp", -1,
        getMemoryAddress(region, key), 1, 0, false, true));
  }

  private void putSerializableClass(Region<Object, Object> region, List<ExpectedValues> expected) {
    String key = "keySerializableClass";
    SerializableClass value = new SerializableClass();
    region.put(key, value);
    expected.add(
        new ExpectedValues(value, 112, "java.io.Serializable:" + SerializableClass.class.getName(),
            -1, getMemoryAddress(region, key), 1, 0, false, true));
  }

  static class ExpectedValues {
    final Object dataValue;
    final int blockSize;
    final String dataType;
    final int freeListId;
    final long memoryAddress;
    final int refCount;
    final int slabId;
    final boolean isCompressed;
    final boolean isSerialized;

    ExpectedValues(Object dataValue, int blockSize, String dataType, int freeListId,
        long memoryAddress, int refCount, int slabId, boolean isCompressed, boolean isSerialized) {
      this.dataValue = dataValue;
      this.blockSize = blockSize;
      this.dataType = dataType;
      this.freeListId = freeListId;
      this.memoryAddress = memoryAddress;
      this.refCount = refCount;
      this.slabId = slabId;
      this.isCompressed = isCompressed;
      this.isSerialized = isSerialized;
    }
  }

  @SuppressWarnings("serial")
  public static class SerializableClass implements Serializable {
    public boolean equals(Object obj) {
      return obj instanceof SerializableClass;
    }

    public int hashCode() {
      return 42;
    }
  }
}
