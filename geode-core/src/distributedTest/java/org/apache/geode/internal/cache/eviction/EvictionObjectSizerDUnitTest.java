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
package org.apache.geode.internal.cache.eviction;

import static org.apache.geode.internal.JvmSizeUtils.roundUpSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.JvmSizeUtils;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.internal.cache.TestNonSizerObject;
import org.apache.geode.internal.cache.TestObjectSizerImpl;
import org.apache.geode.internal.cache.entries.AbstractLRURegionEntry;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.EvictionTest;

@Category({EvictionTest.class})
@SuppressWarnings("serial")
public class EvictionObjectSizerDUnitTest extends CacheTestCase {

  private static final int maxEnteries = 20;
  private static final int maxSizeInMb = 20;

  private static Cache cache;
  private static Region region;

  @After
  public void tearDown() throws Exception {
    if (cache != null) {
      cache.close();
    }
    cache = null;
    region = null;
  }

  /**
   * Without object sizer
   */
  @Test
  public void testWithoutObjectSizerForHeapLRU() throws Exception {
    prepareScenario(EvictionAlgorithm.LRU_HEAP, null);

    // Size of overhead=
    // 49(HeapLRUCapacityController)((PartitionedRegion)region).getEvictionController()).getPerEntryOverhead()
    // Size of Integer key= 0 (it is inlined)
    // Size of Byte Array(1 MB)= 1024 * 1024 + 8 (byte object) + 4 ( size of object) + 4 (rounded up
    // to nearest word)
    // = 1048592
    // Total Size of each entry should be= 1048592

    putData("PR1", 2, 1);
    int keySize = 0;
    int valueSize =
        JvmSizeUtils.getObjectHeaderSize() + 4 /* array length */ + (1024 * 1024) /* bytes */;
    valueSize = roundUpSize(valueSize);
    int entrySize = keySize + valueSize
        + ((HeapLRUController) ((PartitionedRegion) region).getEvictionController())
            .getPerEntryOverhead();
    verifySize("PR1", 2, entrySize);
    assertEquals(2 * entrySize,
        ((PartitionedRegion) region).getEvictionController().getCounters().getCounter());
  }

  /**
   * With object sizer for standard objects.Key -Integer Value ByteArray
   *
   */
  @Test
  public void testObjectSizerForHeapLRU_StandardObjects() throws Exception {
    prepareScenario(EvictionAlgorithm.LRU_HEAP, new TestObjectSizerImpl());

    // Size of overhead= 49
    // Size of Integer key= 0(inlined)
    // Size of Byte Array(1 MB) + overhead (16 bytes)= 1048592 + 16
    // Total Size of each entry should be= 1048592

    putData("PR1", 2, 1);

    {
      int keySize = 0;
      int valueSize =
          JvmSizeUtils.getObjectHeaderSize() + 4 /* array length */ + (1024 * 1024) /* bytes */;
      valueSize = roundUpSize(valueSize);
      int entrySize = keySize + valueSize
          + ((HeapLRUController) ((PartitionedRegion) region).getEvictionController())
              .getPerEntryOverhead();
      verifySize("PR1", 2, entrySize);
    }

    // Size of overhead= 49
    // Size of Integer key= 0(inlined)
    // Size of Byte Array(2 MB) + overhead= 2097152 + 16
    // Total Size of each entry should be= 2097201

    {
      putData("PR1", 2, 2);
      int keySize = 0;
      int valueSize = JvmSizeUtils.getObjectHeaderSize() + 4 /* array length */
          + (1024 * 1024 * 2) /* bytes */;
      valueSize = roundUpSize(valueSize);
      int entrySize = keySize + valueSize
          + ((HeapLRUController) ((PartitionedRegion) region).getEvictionController())
              .getPerEntryOverhead();
      verifySize("PR1", 2, entrySize);
    }
  }

  /**
   * With object sizer for customized value object implementing ObjectSizer .Key -Integer Value
   * TestNonSizerObject
   */
  @Test
  public void testObjectSizerForHeapLRU_CustomizedNonSizerObject() throws Exception {
    prepareScenario(EvictionAlgorithm.LRU_HEAP, new TestObjectSizerImpl());

    // Size of overhead= 49
    // Size of Integer key= 0(inlined)
    // Size of byte array 0 + size of overhead(16)
    // Total Size of each entry should be= 54
    putCustomizedData(1, new byte[0]);
    {
      int keySize = 0;
      int valueSize = JvmSizeUtils.getObjectHeaderSize() + 4 /* array length */ + 0 /* bytes */;
      valueSize = roundUpSize(valueSize);
      int entrySize = keySize + valueSize
          + ((HeapLRUController) ((PartitionedRegion) region).getEvictionController())
              .getPerEntryOverhead();
      assertEquals(entrySize, getSizeOfCustomizedData(1));
    }

    // Size of overhead= 49
    // Size of Integer key= 0(inlined)
    // Size of byte array 4 + size of overhead(12)
    // Total Size of each entry should be= 59
    putCustomizedData(2, new byte[4]);
    {
      int keySize = 0;
      int valueSize = JvmSizeUtils.getObjectHeaderSize() + 4 /* array length */ + 4 /* bytes */;
      valueSize = roundUpSize(valueSize);
      int entrySize = keySize + valueSize
          + ((HeapLRUController) ((PartitionedRegion) region).getEvictionController())
              .getPerEntryOverhead();
      assertEquals(entrySize, getSizeOfCustomizedData(2));
    }
  }

  /**
   * With object sizer for customized value object implementing ObjectSizer .Key -Integer Value
   * TestObjectSizerImpl
   */
  @Test
  public void testObjectSizerForHeapLRU_CustomizedSizerObject() throws Exception {
    prepareScenario(EvictionAlgorithm.LRU_HEAP, new TestObjectSizerImpl());

    // Size of overhead= 49
    // Size of Integer key= 0(inlined)
    // Size of TestObjectSizerImpl= 160 (serialized size), changed to 156 because package changed to
    // org.apache.geode
    // Total Size of entry should be= 71
    putCustomizedData(1, new TestObjectSizerImpl());
    int expected = (0 + 156 + (Sizeable.PER_OBJECT_OVERHEAD * 2)
        + ((HeapLRUController) ((PartitionedRegion) region).getEvictionController())
            .getPerEntryOverhead());
    assertEquals(expected, getSizeOfCustomizedData(1));
    assertEquals(expected,
        ((PartitionedRegion) region).getEvictionController().getCounters().getCounter());
  }

  /**
   * With object sizer for customized key and value objects.
   */
  @Test
  public void testObjectSizerForHeapLRU_CustomizedSizerObjects() throws Exception {
    prepareScenario(EvictionAlgorithm.LRU_HEAP, new TestObjectSizerImpl());

    // Size of overhead= 49
    // Size of TestNonSizerObject key= 1(customized)
    // Size of TestObjectSizerImpl= 160 (serialized size), changed to 156 because package changed to
    // org.apache.geode
    // Total Size of entry should be= 72
    putCustomizedObjects(new TestNonSizerObject("1"), new TestObjectSizerImpl());
    int expected = (1 + 156 + (Sizeable.PER_OBJECT_OVERHEAD * 2)
        + ((HeapLRUController) ((PartitionedRegion) region).getEvictionController())
            .getPerEntryOverhead());
    assertEquals(expected, getSizeOfCustomizedObject(new TestNonSizerObject("1")));
    assertEquals(expected,
        ((PartitionedRegion) region).getEvictionController().getCounters().getCounter());
  }

  private void prepareScenario(EvictionAlgorithm evictionAlgorithm, ObjectSizer sizer) {
    createMyCache();
    createPartitionedRegion(true, evictionAlgorithm, "PR1", 1, 1, 10000, sizer);
  }

  private void createMyCache() {
    Properties props = new Properties();
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    cache.getResourceManager().setEvictionHeapPercentage(50);
  }

  private static void createPartitionedRegion(boolean setEvictionOn,
      EvictionAlgorithm evictionAlgorithm, String regionName, int totalNoOfBuckets,
      int evictionAction, int evictorInterval, ObjectSizer sizer) {

    final AttributesFactory factory = new AttributesFactory();
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory()
        .setRedundantCopies(totalNoOfBuckets == 4 ? 0 : 1).setTotalNumBuckets(totalNoOfBuckets);
    factory.setConcurrencyChecksEnabled(false);
    factory.setPartitionAttributes(partitionAttributesFactory.create());
    if (setEvictionOn) {
      if (evictionAlgorithm.isLRUHeap()) {
        factory.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(sizer,
            evictionAction == 1 ? EvictionAction.LOCAL_DESTROY : EvictionAction.OVERFLOW_TO_DISK));
      } else if (evictionAlgorithm.isLRUMemory()) {
        factory.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(maxSizeInMb,
            sizer,
            evictionAction == 1 ? EvictionAction.LOCAL_DESTROY : EvictionAction.OVERFLOW_TO_DISK));
      } else {
        factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maxEnteries,
            evictionAction == 1 ? EvictionAction.LOCAL_DESTROY : EvictionAction.OVERFLOW_TO_DISK));
      }
      if (evictionAction == 2) {
        factory.setDiskSynchronous(true);
        final File[] diskDirs = new File[1];
        diskDirs[0] =
            new File("Partitioned_Region_Eviction/" + "LogFile" + "_" + OSProcess.getId());
        diskDirs[0].mkdirs();
        factory.setDiskStoreName(cache.createDiskStoreFactory().setDiskDirs(diskDirs)
            .create("EvictionObjectSizerDUnitTest").getName());
      }
    }

    region = cache.createRegion(regionName, factory.create());
    assertNotNull(region);
    LogWriterUtils.getLogWriter().info("Partitioned Region created Successfully :" + region);
  }

  /**
   * returns data size in bytes
   */
  private static int putData(final String regionName, final int noOfElememts,
      final int sizeOfElement) {
    int result = 0;
    final Region pr = cache.getRegion(regionName);
    for (int counter = 1; counter <= noOfElememts; counter++) {
      byte[] baValue = new byte[sizeOfElement * 1024 * 1024];
      int baSize = CachedDeserializableFactory.getByteSize(baValue);
      result += baSize;
      pr.put(new Integer(counter), baValue);
    }
    return result;
  }

  private static void verifySize(String regionName, int noOfElememts, int entrySize) {
    final Region pr = cache.getRegion(regionName);
    for (final Map.Entry<Integer, BucketRegion> integerBucketRegionEntry : ((PartitionedRegion) pr)
        .getDataStore()
        .getAllLocalBuckets()) {
      final Map.Entry entry = (Map.Entry) integerBucketRegionEntry;
      final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
      if (bucketRegion == null) {
        continue;
      } else {
        RegionMap map = bucketRegion.getRegionMap();
        if (map == null || map.size() == 0) {
          continue;
        }
        LogWriterUtils.getLogWriter().info("Checking for entry in bucket region: " + bucketRegion);
        for (int counter = 1; counter <= noOfElememts; counter++) {
          assertEquals(entrySize,
              ((AbstractLRURegionEntry) map.getEntry(new Integer(counter))).getEntrySize());
        }
      }
    }
  }

  private void putCustomizedData(int counter, Object object) {
    final Region pr = cache.getRegion("PR1");
    pr.put(new Integer(counter), object);

  }

  private void putCustomizedObjects(Object key, Object value) {
    final Region pr = cache.getRegion("PR1");
    pr.put(key, value);

  }

  private int getSizeOfCustomizedData(int counter) {
    final Region pr = cache.getRegion("PR1");
    for (final Map.Entry<Integer, BucketRegion> integerBucketRegionEntry : ((PartitionedRegion) pr)
        .getDataStore()
        .getAllLocalBuckets()) {
      final Map.Entry entry = (Map.Entry) integerBucketRegionEntry;
      final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
      if (bucketRegion == null) {
        continue;
      } else {
        RegionMap map = bucketRegion.getRegionMap();
        return ((AbstractLRURegionEntry) map.getEntry(new Integer(counter))).getEntrySize();
      }
    }
    return 0;
  }

  private int getSizeOfCustomizedObject(Object object) {
    final Region pr = cache.getRegion("PR1");
    for (final Map.Entry<Integer, BucketRegion> integerBucketRegionEntry : ((PartitionedRegion) pr)
        .getDataStore()
        .getAllLocalBuckets()) {
      final Map.Entry entry = (Map.Entry) integerBucketRegionEntry;
      final BucketRegion bucketRegion = (BucketRegion) entry.getValue();
      if (bucketRegion == null) {
        continue;
      } else {
        RegionMap map = bucketRegion.getRegionMap();
        AbstractLRURegionEntry re = (AbstractLRURegionEntry) map.getEntry(object);
        if (re != null) {
          return re.getEntrySize();
        }
      }
    }
    return 0;
  }

}
