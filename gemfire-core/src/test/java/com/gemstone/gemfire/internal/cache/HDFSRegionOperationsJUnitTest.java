/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.junit.FixMethodOrder;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreFactoryImpl;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.cardinality.HyperLogLog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogConfig;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

/**
 * Tests that region operations work as expected when data is in HDFS.
 * This test explicitly clears in-memory ConcurrentHashMap that back
 * AbstractRegionMap before validating region operations.
 * 
 * @author sbawaska
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSRegionOperationsJUnitTest extends TestCase {

  protected Cache cache;
  protected HDFSStore hdfsStore;

  public void setUp() throws Exception {
    Properties props = getDSProps();
    cache = new CacheFactory(props).create();
    System.setProperty(HoplogConfig.ALLOW_LOCAL_HDFS_PROP, "true");
    String storeName = getName()+"-store";
    HDFSStoreFactory hsf = cache.createHDFSStoreFactory();
    hsf.setHomeDir(getName()+"-test");
    hsf.setBatchInterval(getBatchTimeInterval());
    hdfsStore = hsf.create(storeName);
  }

  protected Properties getDSProps() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    props.put("log-level", "config");
    return props;
  }

  public void tearDown() throws Exception {
    for (Region r : cache.rootRegions()) {
      if (r != null) {
        r.close();
      }
    }

    if (cache.getRegion(getName()) != null) {
      cache.getRegion(getName()).destroyRegion();
    }
    DiskStore ds = cache.findDiskStore(null);
    if (ds != null) {
      ds.destroy();
    }
    
    ((HDFSStoreImpl)hdfsStore).getFileSystem().delete(new Path(hdfsStore.getHomeDir()), true);
  }

  protected int getBatchTimeInterval() {
    return 1000;
  }

  protected Region<Integer, String> createRegion(String regionName) {
    RegionFactory<Integer, String> rf = cache.createRegionFactory(RegionShortcut.PARTITION_HDFS);
    PartitionAttributes prAttr = new PartitionAttributesFactory().setTotalNumBuckets(10).create();
    rf.setPartitionAttributes(prAttr);
    rf.setHDFSStoreName(hdfsStore.getName());
    Region<Integer, String> r = rf.create(regionName);
    
    ((PartitionedRegion) r).setQueryHDFS(true);
    return r;
  }

  protected void clearBackingCHM(Region<Integer, String> r) {
    PartitionedRegion pr = (PartitionedRegion)r;
    for (BucketRegion br : pr.getDataStore().getAllLocalBucketRegions()) {
      assertTrue(br.getRegionMap() instanceof HDFSRegionMap);
      ((AbstractRegionMap)br.getRegionMap())._getMap().clear();
      // wait here to make sure that the queue has been flushed
    }
    sleep(pr.getFullPath());
  }

  protected void sleep(String regionPath) {
    String qname = HDFSStoreFactoryImpl.getEventQueueName(regionPath);
    GemFireCacheImpl.getExisting().waitForSenderQueueFlush(qname, true, 30);
  }

  public void test010PUTDMLSupport() {
    Region<Integer, String> r = createRegion(getName());
    SortedOplogStatistics stats = HDFSRegionDirector.getInstance().getHdfsRegionStats("/" + getName());
    assertEquals(0, stats.getRead().getCount());
    for (int i=0; i<100; i++) {
      r.put(i, "value"+i);
    }
    assertEquals(100, stats.getRead().getCount());
    sleep(r.getFullPath());
    clearBackingCHM(r);
    LocalRegion lr = (LocalRegion) r;
    for (int i=0; i<200; i++) {
      EntryEventImpl ev = lr.newPutEntryEvent(i, "value"+i, null);
      lr.validatedPut(ev, System.currentTimeMillis());
    }
    // verify that read count on HDFS does not change
    assertEquals(100, stats.getRead().getCount());
    sleep(r.getFullPath());
    clearBackingCHM(r);
    for (int i=0; i<200; i++) {
      assertEquals("value"+i, r.get(i));
    }
    if (getBatchTimeInterval() > 1000) {
      // reads from async queue
      assertEquals(100, stats.getRead().getCount());
    } else {
      assertEquals(300, stats.getRead().getCount());
    }
  }

  public void test020GetOperationalData() throws Exception {
    Region<Integer, String> r = createRegion(getName());
    SortedOplogStatistics stats = HDFSRegionDirector.getInstance().getHdfsRegionStats("/" + getName());
    assertEquals(0, stats.getRead().getCount());
    for (int i=0; i<100; i++) {
      r.put(i, "value"+i);
    }
    int expectedReadsFromHDFS = 100;
    assertEquals(expectedReadsFromHDFS, stats.getRead().getCount());
    sleep(r.getFullPath());
    clearBackingCHM(r);
    LocalRegion lr = (LocalRegion) r;
    for (int i=0; i<200; i++) {
      if (i < 100) {
        assertEquals("value"+i, r.get(i));
      } else {
        assertNull(r.get(i));
      }
    }
    if (getBatchTimeInterval() > 1000) {
      // reads from async queue
      expectedReadsFromHDFS = 200; // initial 100 + 100 for misses
    } else {
      expectedReadsFromHDFS = 300; // initial 100 + 200 for reads
    }
    assertEquals(expectedReadsFromHDFS, stats.getRead().getCount());
    for (int i=0; i<200; i++) {
      assertNull(lr.get(i, null, true, false, false, null, null, false, false/*allowReadFromHDFS*/));
    }
    // no increase in HDFS reads
    assertEquals(expectedReadsFromHDFS, stats.getRead().getCount());
    
    /**MergeGemXDHDFSToGFE Have not merged this API as this api is not called by any code*/ 
    //   test the dataView API
    //for (int i=0; i<200; i++) {
    //  assertNull(lr.getDataView().getLocally(i, null, i%10, lr, true, true, null, null, false, false/*allowReadFromHDFS*/));
    //}
    // no increase in HDFS reads
    assertEquals(expectedReadsFromHDFS, stats.getRead().getCount());
  }
  
  public void test030RemoveOperationalData() throws Exception {
    Region<Integer, String> r = createRegion(getName());
    SortedOplogStatistics stats = HDFSRegionDirector.getInstance().getHdfsRegionStats("/" + getName());
    assertEquals(0, stats.getRead().getCount());
    for (int i=0; i<100; i++) {
      r.put(i, "value"+i);
    }
    int expectedReadsFromHDFS = 100;
    assertEquals(expectedReadsFromHDFS, stats.getRead().getCount());
    sleep(r.getFullPath());
    PartitionedRegion lr = (PartitionedRegion) r;
    for(int i =0; i < 50; i++) {
      lr.getBucketRegion(i).customEvictDestroy(i);
    }
    for (int i=0; i<200; i++) {
      if (i < 100) {
        assertEquals("value"+i, r.get(i));
      } else {
        assertNull(r.get(i));
      }
    }
    if (getBatchTimeInterval() > 1000) {
      // reads from async queue
      expectedReadsFromHDFS = 200; // initial 100 + 100 for misses
    } else {
      expectedReadsFromHDFS = 250; // initial 100 + 200 for reads + 50 for 
    }
    assertEquals(expectedReadsFromHDFS, stats.getRead().getCount());
    for (int i=0; i<50; i++) {
      assertNull(lr.get(i, null, true, false, false, null,  null, false, false/*allowReadFromHDFS*/));
    }
    for (int i=50; i<100; i++) {
      assertEquals("value"+i, lr.get(i, null, true, false, false, null,null, false, false/*allowReadFromHDFS*/));
    }
    for (int i=100; i<200; i++) {
      assertNull(lr.get(i, null, true, false, false, null,  null, false, false/*allowReadFromHDFS*/));
    }
    // no increase in HDFS reads
    assertEquals(expectedReadsFromHDFS, stats.getRead().getCount());
  }

  public void _test040NoAutoEviction() throws Exception {
    if (!cache.isClosed()) {
      tearDown();
      cache.close();
      System.setProperty("gemfire.disableAutoEviction", "true");
      setUp();
    }
    Region<Integer, String> r = createRegion(getName());
    System.setProperty("gemfire.disableAutoEviction", "false");
    for (int i =0; i<5; i++) {
      r.put(i, "value"+i);
    }
    PartitionedRegion pr = (PartitionedRegion) r;
    BucketRegion br = pr.getBucketRegion(1);
    assertNotNull(br.getAttributes().getEvictionAttributes());
    assertEquals(EvictionAlgorithm.NONE, br.getAttributes().getEvictionAttributes().getAlgorithm());

    GemFireCacheImpl cache = (GemFireCacheImpl) r.getCache();
    assertEquals(0.0f, cache.getResourceManager().getEvictionHeapPercentage());
  }

  public void test050LRURegionAttributesForPR() {
    RegionFactory<Integer, String> rf = cache.createRegionFactory();
    rf.setHDFSStoreName(hdfsStore.getName());
    rf.setDataPolicy(DataPolicy.HDFS_PARTITION);
    verifyLRURegionAttributesForPR(rf.create(getName()));
  }

  public void test060LRURegionAttributesForRegionShortcutPR() {
    verifyLRURegionAttributesForPR(createRegion(getName()));
  }

  private void verifyLRURegionAttributesForPR(Region r) {
    for (int i =0; i<200; i++) {
      r.put(i, "value"+i);
    }
    RegionAttributes<Integer, String> ra = r.getAttributes();
    assertNotNull(ra.getEvictionAttributes());
    // default eviction action for region shortcut
    assertEquals(EvictionAction.OVERFLOW_TO_DISK, ra.getEvictionAttributes().getAction());

    GemFireCacheImpl cache = (GemFireCacheImpl) r.getCache();
    assertEquals(80.0f, cache.getResourceManager().getEvictionHeapPercentage());
    DiskStore ds = cache.findDiskStore(null);
    assertNotNull(ds);
    Set s = cache.getResourceManager().getResourceListeners(ResourceType.HEAP_MEMORY);
    Iterator it = s.iterator();
    boolean regionFound = false;
    while (it.hasNext()) {
      Object o = it.next();
      if (o instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion) o;
        if (getName().equals(pr.getName())) {
          regionFound = true;
        } else {
          continue;
        }
        for (BucketRegion br : pr.getDataStore().getAllLocalBucketRegions()) {
          assertNotNull(br.getAttributes().getEvictionAttributes());
          assertEquals(EvictionAlgorithm.LRU_HEAP, br.getAttributes().getEvictionAttributes().getAlgorithm());
          assertEquals(EvictionAction.OVERFLOW_TO_DISK, br.getAttributes().getEvictionAttributes().getAction());
        }
      }
    }
    assertTrue(regionFound);

  }

  public void test070SizeEstimate() {
    Region<Integer, String> r = createRegion(getName());
    int size = 226;
    Random rand = new Random();
    for (int i=0; i<size; i++) {
      r.put(rand.nextInt(), "value"+i);
    }
    // size before flush
    LocalRegion lr = (LocalRegion) r;
    long estimate = lr.sizeEstimate();
    double err = Math.abs(estimate - size) / (double) size;
    // on a busy system flush might start before we call estimateSize, so rather than equality,
    // test for error margin. fixes bug 49381
    assertTrue("size:"+size+" estimate:"+estimate, err < 0.02 * 10); // each bucket can have an error of 0.02

    // size after flush
    sleep(r.getFullPath());
    estimate = lr.sizeEstimate();
    err = Math.abs(estimate - size) / (double) size;
    assertTrue("size:"+size+" estimate:"+estimate, err < 0.02 * 10); // each bucket can have an error of 0.02
  }

  public void test080PutGet() throws InterruptedException {
    Region<Integer, String> r = createRegion(getName());
    for (int i=0; i<100; i++) {
      r.put(i, "value"+i);
    }
    clearBackingCHM(r);
    for (int i=0; i<100; i++) {
      assertEquals("value"+i, r.get(i));
    }
    
    //Do a put while there are entries in the map
    r.put(0, "value"+0);
    
    r.destroy(1, "value"+1);
  }

  public void test090Delete() {
    Region<Integer, String> r = createRegion(getName());
    for (int i=0; i<11; i++) {
      r.put(i, "value"+i);
    }
    clearBackingCHM(r);
    int delKey = 9;
    r.destroy(delKey);
    assertNull(r.get(delKey));
    assertFalse(r.containsKey(delKey));
  }

  public void test100Invalidate() {
    Region<Integer, String> r = createRegion(getName());
    for (int i=0; i<100; i++) {
      r.put(i, "value"+i);
    }
    clearBackingCHM(r);
    int invKey = 9;
    r.invalidate(invKey);
    assertNull(r.get(invKey));
    assertTrue(r.containsKey(invKey));
  }

  public void test110Size() {
    Region<Integer, String> r = createRegion(getName());
    for (int i=0; i<100; i++) {
      r.put(i, "value"+i);
    }
    clearBackingCHM(r);
    assertEquals(100, r.size());
    r.destroy(45);
    assertEquals(99, r.size());
    r.invalidate(55);
    r.invalidate(65);
    assertEquals(99, r.size());
  }

  public void test120KeyIterator() {
    Region<Integer, String> r = createRegion(getName());
    for (int i=0; i<100; i++) {
      r.put(i, "value"+i);
    }
    clearBackingCHM(r);
    Set<Integer> keys = r.keySet();
    int c = 0;
    for (int i : keys) {
//      assertEquals(c, i);
      c++;
    }
    assertEquals(100, c);
    assertEquals(100, keys.size());
    int delKey = 88;
    r.destroy(delKey);
    r.invalidate(39);
    keys = r.keySet();
    c = 0;
    for (int i : keys) {
      if (c == delKey) {
        c++;
      }
//      assertEquals(c, i);
      c++;
    }
    assertEquals(99, keys.size());
  }

  public void test130EntriesIterator() {
    Region<Integer, String> r = createRegion(getName());
    for (int i=0; i<100; i++) {
      r.put(i, "value"+i);
    }
    clearBackingCHM(r);
    Set<Entry<Integer, String>> entries = r.entrySet();
    int c = 0;
    for (Entry<Integer, String> e : entries) {
//      assertEquals(c, (int) e.getKey());
      assertEquals("value"+e.getKey(), e.getValue());
      c++;
    }
    assertEquals(100, c);
    assertEquals(100, entries.size());
    int delKey = 88;
    r.destroy(delKey);
    int invKey = 39;
    r.invalidate(invKey);
    entries = r.entrySet();
    c = 0;
    for (Entry<Integer, String> e : entries) {
      if (c == delKey) {
        c++;
      } else if (e.getKey() == invKey) {
//        assertEquals(c, (int) e.getKey());
        assertNull(e.getValue());
      } else {
//        assertEquals(c, (int) e.getKey());
        assertEquals("value"+e.getKey(), e.getValue());
      }
      c++;
    }
    assertEquals(99, entries.size());
  }

  public void test140ContainsKey() {
    Region<Integer, String> r = createRegion(getName());
    for (int i=0; i<100; i++) {
      r.put(i, "value"+i);
    }
    clearBackingCHM(r);
    assertTrue(r.containsKey(80));
    r.destroy(80);
    assertFalse(r.containsKey(80));
    r.invalidate(64);
    assertTrue(r.containsKey(64));
  }

  public void test150ContainsValue() {
    Region<Integer, String> r = createRegion(getName());
    for (int i=0; i<100; i++) {
      r.put(i, "value"+i);
    }
    clearBackingCHM(r);
    assertTrue(r.containsValue("value45"));
    r.destroy(45);
    assertFalse(r.containsValue("value45"));
    r.invalidate(64);
    assertFalse(r.containsValue("value64"));
  }

  public void test160DestroyRegion() {
    Region<Integer, String> r = createRegion(getName());
    for (int i=0; i<100; i++) {
      r.put(i, "value"+i);
    }
    clearBackingCHM(r);
    r.destroyRegion();
    try {
      r.get(3);
      fail("expected exception not thrown");
    } catch (RegionDestroyedException expected) {
    }
  }

  public void test170PutIfAbsent() {
    Region<Integer, String> r = createRegion(getName());
    r.put(1, "value1");
    clearBackingCHM(r);
    assertEquals("value1", r.putIfAbsent(1, "value2"));
  }

  public void test180Replace() {
    Region<Integer, String> r = createRegion(getName());
    assertNull(r.replace(1, "value"));
    r.put(1, "value1");
    clearBackingCHM(r);
    assertEquals("value1", r.replace(1, "value2"));
  }

  public void test190ReplaceKVV() {
    Region<Integer, String> r = createRegion(getName());
    assertFalse(r.replace(1, "oldValue", "newValue"));
    r.put(1, "value1");
    clearBackingCHM(r);
    assertTrue(r.replace(1, "value1", "value2"));
  }

  public void test200Accuracy() throws IOException {
    double sum=0.0;
    int iter = 10;
    for (int t=0; t<iter; t++) {
      Random r = new Random();
      HashSet<Integer> vals = new HashSet<Integer>();
      HyperLogLog hll = new HyperLogLog(0.03);
      //HyperLogLog hll = new HyperLogLog(0.1);
      double accuracy = 0.0;
      for (int i = 0; i < 2 * 1000000; i++) {
        int val = r.nextInt();
        vals.add(val);
        hll.offer(val);
      }
      long size = vals.size();
      long est = hll.cardinality();
      
      accuracy = 100.0 * (size - est) / est;
      System.out.printf("Accuracy is %f hll size is %d\n", accuracy, hll.getBytes().length);
      sum+=Math.abs(accuracy);
    }
    double avgAccuracy = sum/(iter*1.0);
    System.out.println("Avg accuracy is:"+avgAccuracy);
    assertTrue(avgAccuracy < 6);
  }
}
