/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreFactoryImpl;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog.HoplogReader;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog.HoplogWriter;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogSetReader.HoplogIterator;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class, HoplogTest.class})
public class HfileSortedOplogJUnitTest extends BaseHoplogTestCase {
	ArrayList<Object> toBeCleaned = new ArrayList<>();
  
  /**
   * Tests hoplog creation using a writer. If this test fails, all the tests wills fail as hoplog
   * creation is the first step
   */
  public void testHoplogWriter() throws Exception {
    String hoplogName = getRandomHoplogName();
    createHoplog(hoplogName, 1);
    FileStatus hoplogStatus = hdfsStore.getFileSystem().getFileStatus(new Path(testDataDir, hoplogName));
    assertNotNull(hoplogStatus);
  }

  /**
   * Tests hoplog deletion.
   */
  public void testDeletion() throws Exception {
    String hoplogName = getRandomHoplogName();
    createHoplog(hoplogName, 1);
    HFileSortedOplog testHoplog = new HFileSortedOplog(hdfsStore, new Path(testDataDir, hoplogName), blockCache, stats, storeStats);

    testHoplog.delete();

    try {
      FileStatus hoplogStatus = hdfsStore.getFileSystem().getFileStatus(new Path(testDataDir, hoplogName));
      // hoplog should not exists. fail if it does
      assertNull("File deletion failed", hoplogStatus);
    } catch (FileNotFoundException e) {
      // exception expected after deletion
    }
  }

  /**
   * Tests hoplog reader creation and key based gets
   */
  public void testHoplogReader() throws Exception {
    String hop1 = getRandomHoplogName();
    Map<String, String> map = createHoplog(hop1, 10);

    HFileSortedOplog testHoplog1 = new HFileSortedOplog(hdfsStore, new Path(testDataDir, hop1), blockCache, stats, storeStats);
    HoplogReader reader = testHoplog1.getReader();
    // verify that each entry put in the hoplog is returned by reader
    for (Entry<String, String> entry : map.entrySet()) {
      byte[] value = reader.read(entry.getKey().getBytes());
      assertNotNull(value);
    }
  }

  /**
   * Tests full iteration on a hoplog. Ensures all inserted keys are returned and no key is missing
   */
  public void testIterator() throws IOException {
    int count = 10;
    ByteArrayComparator bac = new ByteArrayComparator();

    String hoplogName = getRandomHoplogName();
    TreeMap<String, String> sortedMap = createHoplog(hoplogName, count);

    HFileSortedOplog testHoplog = new HFileSortedOplog(hdfsStore, new Path(testDataDir, hoplogName), blockCache, stats, storeStats);
    HoplogReader reader = testHoplog.getReader();

    Iterator<Entry<String, String>> mapIter = sortedMap.entrySet().iterator();
    HoplogIterator<byte[], byte[]> iter = reader.scan();
    for (; iter.hasNext();) {
      byte[] key = iter.next();
      Entry<String, String> entry = mapIter.next();
      assertEquals(0, bac.compare(key, iter.getKey()));
      assertEquals(0, bac.compare(key, entry.getKey().getBytes()));
      assertEquals(0, bac.compare(iter.getValue(), entry.getValue().getBytes()));
      count--;
    }
    assertEquals(0, count);
  }

  /**
   * Tests hoplog iterator. after returning first key, has next should return false and all
   * subsequent next calls should return null
   */
  public void testSingleKVIterator() throws Exception {
    String hoplogName = getRandomHoplogName();
    TreeMap<String, String> map = createHoplog(hoplogName, 1);
    HFileSortedOplog testHoplog = new HFileSortedOplog(hdfsStore, new Path(testDataDir, hoplogName), blockCache, stats, storeStats);
    HoplogReader reader = testHoplog.getReader();

    HoplogIterator<byte[], byte[]> iter = reader.scan();
    assertNull(iter.getKey());
    assertNull(iter.getValue());
    assertTrue(iter.hasNext());
    assertNull(iter.getKey());
    assertNull(iter.getValue());

    Entry<String, String> entry = map.firstEntry();
    iter.next();
    assertNotNull(iter.getKey());
    assertEquals(entry.getKey(), new String(iter.getKey()));
    assertNotNull(iter.getValue());
    assertEquals(entry.getValue(), new String(iter.getValue()));

    assertFalse(iter.hasNext());
    try {
      iter.next();
      fail();
    } catch (NoSuchElementException e) {
    }
  }

  /**
   * Tests iteration on a hoplog with no keys, using a scanner. Scanner should not return any value
   * and hasNext should return false everytime
   */
  public void testEmptyFileIterator() throws Exception {
    String hoplogName = getRandomHoplogName();
    createHoplog(hoplogName, 0);
    HFileSortedOplog testHoplog = new HFileSortedOplog(hdfsStore, new Path(testDataDir, hoplogName), blockCache, stats, storeStats);
    HoplogReader reader = testHoplog.getReader();
    HoplogIterator<byte[], byte[]> iter = reader.scan();
    assertNull(iter.getKey());
    assertNull(iter.getValue());
    assertFalse(iter.hasNext());
    assertNull(iter.getKey());
    assertNull(iter.getValue());
    try {
      iter.next();
      fail();
    } catch (NoSuchElementException e) {
    }
  }

  /**
   * Tests from exclusive iterator
   */
  public void testFromExclusiveIterator() throws Exception {
    fromIterator(false);
  }

  /**
   * Tests from inclusive iterator
   */
  public void testFromInclusiveIterator() throws Exception {
    fromIterator(true);
  }

  /**
   * Tests from condition based iteration. creates hoplog with 10 KVs. Creates a scanner starting at
   * a middle key and verifies the count of KVs iterated on
   */
  public void fromIterator(boolean includeFrom) throws Exception {
    int count = 10;
    ByteArrayComparator bac = new ByteArrayComparator();

    String hoplogName = getRandomHoplogName();
    // sorted map contains the keys inserted in the hoplog for testing
    TreeMap<String, String> sortedMap = createHoplog(hoplogName, count);

    HFileSortedOplog testHoplog = new HFileSortedOplog(hdfsStore, new Path(testDataDir, hoplogName), blockCache, stats, storeStats);
    HoplogReader reader = testHoplog.getReader();

    int middleKey = 4;
    // remove top keys from the sorted map as the hoplog scanner should not
    // return those
    Iterator<Entry<String, String>> mapIter = sortedMap.entrySet().iterator();
    for (int i = 0; i < middleKey; i++) {
      mapIter.next();
      count--;
    }
    if (!includeFrom) {
      mapIter.next();
      count--;
    }

    // keys are like Key-X, for X=0 till X=9. Start iterator at fifth key,
    // key-4. if excluding from key, start at sixth key, key-5.
    HoplogIterator<byte[], byte[]> iter = reader.scan(("key-" + middleKey).getBytes(), includeFrom,
        null, true);

    for (; iter.hasNext();) {
      byte[] key = iter.next();
      Entry<String, String> entry = mapIter.next();
      // make sure the KV returned by iterator match the inserted KV
      assertEquals(0, bac.compare(key, iter.getKey()));
      assertEquals(0, bac.compare(key, entry.getKey().getBytes()));
      assertEquals(0, bac.compare(iter.getValue(), entry.getValue().getBytes()));
      count--;
    }
    assertEquals(0, count);
  }

  /**
   * Tests to exclusive iterator
   */
  public void testToExclusiveIterator() throws Exception {
    toIterator(false);
  }

  /**
   * Tests to inclusive iterator
   */
  public void testToInclusiveIterator() throws Exception {
    toIterator(true);
  }

  /**
   * Tests to condition based iteration. creates hoplog with 10 KVs. Creates a scanner ending at
   * a middle key and verifies the count of KVs iterated on
   */
  public void toIterator(boolean includeTo) throws Exception {
    int count = 10;
    ByteArrayComparator bac = new ByteArrayComparator();
    
    String hoplogName = getRandomHoplogName();
    // sorted map contains the keys inserted in the hoplog for testing
    TreeMap<String, String> sortedMap = createHoplog(hoplogName, count);
    Iterator<Entry<String, String>> mapIter = sortedMap.entrySet().iterator();
    
    HFileSortedOplog testHoplog = new HFileSortedOplog(hdfsStore, new Path(testDataDir, hoplogName), blockCache, stats, storeStats);
    HoplogReader reader = testHoplog.getReader();
    
    int middleKey = 4;
    // keys are like Key-X, for X=0 till X=9. End iterator at fifth key,
    // key-4. if excluding to key, end at fourth key, key-3.
    HoplogIterator<byte[], byte[]> iter = reader.scan(null, true, ("key-" + middleKey).getBytes(), includeTo);
    
    for (; iter.hasNext();) {
      byte[] key = iter.next();
      Entry<String, String> entry = mapIter.next();
      // make sure the KV returned by iterator match the inserted KV
      assertEquals(0, bac.compare(key, iter.getKey()));
      assertEquals(0, bac.compare(key, entry.getKey().getBytes()));
      assertEquals(0, bac.compare(iter.getValue(), entry.getValue().getBytes()));
      
      count --;
    }
    
    if (includeTo) {
      count++;
    }

    assertEquals(10, count + middleKey);
  }
  
  /**
   * Tests whether sortedoplog supports duplicate keys, required when conflation is disabled
   */
  public void testFromToIterator() throws IOException {
    ByteArrayComparator bac = new ByteArrayComparator();
    String hoplogName = getRandomHoplogName();
    HFileSortedOplog hoplog = new HFileSortedOplog(hdfsStore, new Path(testDataDir, hoplogName), blockCache, stats, storeStats);
    
    int count = 5;
    HoplogWriter writer = hoplog.createWriter(5);
    for (int i = 0; i < count; i++) {
      String value = "value-" + (i * 2);
      // even keys key-[0 2 4 6 8]
      writer.append(("key-" + (i * 2)).getBytes(), value.getBytes());
    }
    writer.close();
    
    HoplogReader reader = hoplog.getReader();
    HoplogIterator<byte[], byte[]> iter = reader.scan("key-1".getBytes(), true, "key-7".getBytes(), true);

    for (int i = 2; i < 7; i += 2) {
      assertTrue(iter.hasNext());
      iter.next();
      assertEquals(0, bac.compare(("key-" + i).getBytes(), iter.getKey()));
      assertEquals(0, bac.compare(("value-" + i).getBytes(), iter.getValue()));
      System.out.println(new String(iter.getKey()));
    }
    assertFalse(iter.hasNext());
  }
  
  /**
   * Tests whether sortedoplog supports duplicate keys, required when conflation is disabled
   */
  public void testDuplicateKeys() throws IOException {
    String hoplogName = getRandomHoplogName();
    HFileSortedOplog hoplog = new HFileSortedOplog(hdfsStore, new Path(testDataDir, hoplogName), blockCache, stats, storeStats);

    // write duplicate keys
    int count = 2;
    HoplogWriter writer = hoplog.createWriter(2);
    List<String> values = new ArrayList<String>();
    for(int i = 1; i <= count; i++) {
      String value = "value" + i;
      writer.append("key-1".getBytes(), value.getBytes());
      values.add(value);
    }
    writer.close();

    HoplogReader reader = hoplog.getReader();
    HoplogIterator<byte[], byte[]> scanner = reader.scan();
    for (byte[] key = null; scanner.hasNext();) {
      key = scanner.next();
      count--;
      assertEquals(0, Bytes.compareTo(key, "key-1".getBytes()));
      values.remove(new String(scanner.getValue()));
    }
    assertEquals(0, count);
    assertEquals(0, values.size());
  }
  
  public void testOffsetBasedScan() throws Exception {
    // Each record is 43 bytes. each block is 256 bytes. each block will have 6
    // records
     
    int blocksize = 1 << 8;
    System.setProperty(HoplogConfig.HFILE_BLOCK_SIZE_CONF,
        String.valueOf(blocksize));

    int count = 50;
    String hoplogName = getRandomHoplogName();
    createHoplog(hoplogName, count);

    HFileSortedOplog testHoplog = new HFileSortedOplog(hdfsStore, new Path(
        testDataDir, hoplogName), blockCache, stats, storeStats);

    HoplogReader reader = testHoplog.getReader();
    
    HoplogIterator<byte[], byte[]> scanner = reader.scan(blocksize * 1, blocksize * 2);
    int range1Count = 0;
    String range1EndKey = null;
    for (byte[] key = null; scanner.hasNext();) {
      key = scanner.next();
      range1Count++;
      range1EndKey = new String(key);
    }
    int range1EndKeyNum = Integer.valueOf(range1EndKey.substring("Key-".length()));

    scanner = reader.scan(blocksize * 2, blocksize * 1);
    int range2Count = 0;
    String range2EndKey = null;
    for (byte[] key = null; scanner.hasNext();) {
      key = scanner.next();
      range2Count++;
      range2EndKey = new String(key);
    }
    
    assertEquals(range2EndKey, range1EndKey);
    assertEquals(2, range1Count/range2Count);
    
    scanner = reader.scan(blocksize * 3, blocksize * 1);
    String range3FirstKey = new String(scanner.next());
    
    int range3FirstKeyNum = Integer.valueOf(range3FirstKey.substring("Key-"
        .length()));
    
    // range 3 starts at the end of range 1. so the two keys must be consecutive
    assertEquals(range1EndKeyNum + 1, range3FirstKeyNum);
    
    testHoplog.close();
  }
  
  public void testOffsetScanBeyondFileSize() throws Exception {
    // Each record is 43 bytes. each block is 256 bytes. each block will have 6
    // records
    
    int blocksize = 1 << 8;
    System.setProperty(HoplogConfig.HFILE_BLOCK_SIZE_CONF,
        String.valueOf(blocksize));
    
    int count = 20;
    String hoplogName = getRandomHoplogName();
    createHoplog(hoplogName, count);
    
    HFileSortedOplog testHoplog = new HFileSortedOplog(hdfsStore, new Path(
        testDataDir, hoplogName), blockCache, stats, storeStats);
    
    HoplogReader reader = testHoplog.getReader();
    
    HoplogIterator<byte[], byte[]> scanner = reader.scan(blocksize * 5, blocksize * 2);
    assertFalse(scanner.hasNext());
    
    testHoplog.close();
  }
  
  public void testZeroValueOffsetScan() throws Exception {
    // Each record is 43 bytes. each block is 256 bytes. each block will have 6
    // records
    
    int blocksize = 1 << 8;
    System.setProperty(HoplogConfig.HFILE_BLOCK_SIZE_CONF,
        String.valueOf(blocksize));
    
    int count = 20;
    String hoplogName = getRandomHoplogName();
    createHoplog(hoplogName, count);
    
    HFileSortedOplog testHoplog = new HFileSortedOplog(hdfsStore, new Path(
        testDataDir, hoplogName), blockCache, stats, storeStats);
    
    HoplogReader reader = testHoplog.getReader();
    
    HoplogIterator<byte[], byte[]> scanner = reader.scan(0, blocksize * 2);
    assertTrue(scanner.hasNext());
    int keyNum = Integer.valueOf(new String(scanner.next()).substring("Key-"
        .length()));
    assertEquals(100000, keyNum);

    testHoplog.close();
  }
  
  /*
   * Tests reader succeeds to read data even if FS client is recycled without
   * this reader knowing
   */
  public void testReaderDetectAndUseRecycledFs() throws Exception {
    HDFSStoreFactoryImpl storeFactory = getCloseableLocalHdfsStoreFactory();
    HDFSStoreImpl store = (HDFSStoreImpl) storeFactory.create("Store-1");
    toBeCleaned.add(store);

    HFileSortedOplog hop = new HFileSortedOplog(store, new Path(getName() + "-1-1.hop"), blockCache, stats, storeStats);
    toBeCleaned.add(hop);
    TreeMap<String, String> map = createHoplog(10, hop);

    HoplogReader reader = hop.getReader();
    // verify that each entry put in the hoplog is returned by reader
    for (Entry<String, String> entry : map.entrySet()) {
      byte[] value = reader.read(entry.getKey().getBytes());
      assertNotNull(value);
    }

    cache.getLogger().info("<ExpectedException action=add>java.io.IOException</ExpectedException>");
    try {
      store.getFileSystem().close();
      store.checkAndClearFileSystem();
      
      for (Entry<String, String> entry : map.entrySet()) {
        reader = hop.getReader();
        byte[] value = reader.read(entry.getKey().getBytes());
        assertNotNull(value);
      }
    } finally {
      cache.getLogger().info("<ExpectedException action=remove>java.io.IOException</ExpectedException>");
    }
  }

  public void testNewScannerDetechAndUseRecycledFs() throws Exception {
    HDFSStoreFactoryImpl storeFactory = getCloseableLocalHdfsStoreFactory();
    HDFSStoreImpl store = (HDFSStoreImpl) storeFactory.create("Store-1");
    toBeCleaned.add(store);

    HFileSortedOplog hop = new HFileSortedOplog(store, new Path(getName() + "-1-1.hop"), blockCache, stats, storeStats);
    createHoplog(10, hop);

    HoplogIterator<byte[], byte[]> scanner = hop.getReader().scan();
    // verify that each entry put in the hoplog is returned by reader
    int i = 0;
    while (scanner.hasNext()) {
      byte[] key = scanner.next();
      assertNotNull(key);
      i++;
    }
    assertEquals(10, i);
    // flush block cache
    hop.close(true);
    hop.delete();
    
    hop = new HFileSortedOplog(store, new Path(getName()+"-1-1.hop"), blockCache, stats, storeStats);
		createHoplog(10, hop);
  	toBeCleaned.add(hop);
    hop.getReader();
    
    cache.getLogger().info("<ExpectedException action=add>java.io.IOException</ExpectedException>");
    try {
      store.getFileSystem().close();
      store.checkAndClearFileSystem();
      
      scanner = hop.getReader().scan();
      // verify that each entry put in the hoplog is returned by reader
      i = 0;
      while (scanner.hasNext()) {
        byte[] key = scanner.next();
        assertNotNull(key);
        i++;
      }
      assertEquals(10, i);
    } finally {
      cache.getLogger().info("<ExpectedException action=remove>java.io.IOException</ExpectedException>");
    }
  }
  
  @Override
  protected void tearDown() throws Exception {
    for (Object obj : toBeCleaned) {
      try {
        if (HDFSStoreImpl.class.isInstance(obj)) {
          ((HDFSStoreImpl) obj).clearFolder();
        } else if (AbstractHoplog.class.isInstance(obj)) {
          ((AbstractHoplog) obj).close();
          ((AbstractHoplog) obj).delete();
        }
      } catch (Exception e) {
        System.out.println(e);
      }
    }
    super.tearDown();
  }
    
  private TreeMap<String, String> createHoplog(String hoplogName, int numKeys) throws IOException {
    HFileSortedOplog hoplog = new HFileSortedOplog(hdfsStore, new Path(testDataDir, hoplogName), blockCache, stats, storeStats);
    TreeMap<String, String> map = createHoplog(numKeys, hoplog);
    return map;
  }
}
