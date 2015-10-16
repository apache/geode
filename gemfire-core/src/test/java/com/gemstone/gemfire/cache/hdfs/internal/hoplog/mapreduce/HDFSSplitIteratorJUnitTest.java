/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex.BlockIndexReader;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.hdfs.internal.hoplog.BaseHoplogTestCase;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HFileSortedOplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSSplitIteratorJUnitTest extends BaseHoplogTestCase {
  public void test1Hop1BlockIter() throws Exception {
    Path path = new Path(testDataDir, "region/0/1-1-1.hop");
    Hoplog oplog = new HFileSortedOplog(hdfsStore, path, blockCache, stats,
        storeStats);
    createHoplog(10, oplog);

    Path[] paths = {path};
    long[] starts = {0};
    long[] lengths = {oplog.getSize()};
    HDFSSplitIterator iter = HDFSSplitIterator.newInstance(
        hdfsStore.getFileSystem(), paths, starts, lengths, 0, 0);
    
    int count = 0;
    while (iter.hasNext()) {
      boolean success = iter.next();
      assertTrue(success);
      assertEquals("key-" + count, new String((byte[])iter.getKey()));
      count++;
    }
    assertEquals(10, count);
  }
  
  public void test1HopNBlockIter() throws Exception {
    Path path = new Path(testDataDir, "region/0/1-1-1.hop");
    Hoplog oplog = new HFileSortedOplog(hdfsStore, path,
        blockCache, stats, storeStats);
    createHoplog(2000, oplog);
    
    FileSystem fs = hdfsStore.getFileSystem();
    Reader reader = HFile.createReader(fs, path, new CacheConfig(fs.getConf()));
    BlockIndexReader bir = reader.getDataBlockIndexReader();
    int blockCount = bir.getRootBlockCount();
    reader.close();
    
    // make sure there are more than 1 hfile blocks in the hoplog
    assertTrue(1 < blockCount);

    Path[] paths = {path};
    long half = oplog.getSize()/2;
    long[] starts = {0};
    long[] lengths = {half};
    HDFSSplitIterator iter = HDFSSplitIterator.newInstance(
        hdfsStore.getFileSystem(), paths, starts, lengths, 0, 0);
    
    int count = 0;
    while (iter.hasNext()) {
      boolean success = iter.next();
      assertTrue(success);
      assertEquals("key-" + (count + 100000), new String((byte[])iter.getKey()));
      count++;
    }
    // the number of iterations should be less than number of keys inserted in
    // the hoplog
    assertTrue(count < 2000 && count > 0);

    paths = new Path[] {path};
    starts = new long[] {half + 1};
    lengths = new long[] {oplog.getSize()};
    iter = HDFSSplitIterator.newInstance(
        hdfsStore.getFileSystem(), paths, starts, lengths, 0, 0);
    
    while (iter.hasNext()) {
      boolean success = iter.next();
      assertTrue(success);
      assertEquals("key-" + (count + 100000), new String((byte[])iter.getKey()));
      count++;
    }
    assertEquals(2000, count);

    paths = new Path[] {path, path};
    starts = new long[] {0, half + 1};
    lengths = new long[] {half, oplog.getSize()};
    iter = HDFSSplitIterator.newInstance(
        hdfsStore.getFileSystem(), paths, starts, lengths, 0, 0);
    
    count = 0;
    while (iter.hasNext()) {
      boolean success = iter.next();
      assertTrue(success);
      assertEquals("key-" + (count + 100000), new String((byte[])iter.getKey()));
      count++;
    }
    assertEquals(2000, count);
  }

  /*
   * This tests iterates over 3 hoplog files. The three hoplog files have the
   * same content. Duplicate keys should not get discarded
   */
  public void testNHoplogNBlockIter() throws Exception {
    Path path1 = new Path(testDataDir, "region/0/1-1-1.hop");
    Hoplog oplog = new HFileSortedOplog(hdfsStore, path1,
        blockCache, stats, storeStats);
    createHoplog(2000, oplog);
    
    FileSystem fs = hdfsStore.getFileSystem();
    Reader reader = HFile.createReader(fs, path1, new CacheConfig(fs.getConf()));
    BlockIndexReader bir = reader.getDataBlockIndexReader();
    int blockCount = bir.getRootBlockCount();
    reader.close();
    
    // make sure there are more than 1 hfile blocks in the hoplog
    assertTrue(1 < blockCount);
    
    Path path2 = new Path(testDataDir, "region/0/1-2-1.hop");
    oplog = new HFileSortedOplog(hdfsStore, path2,
        blockCache, stats, storeStats);
    createHoplog(2000, oplog);

    Path path3 = new Path(testDataDir, "region/0/1-3-1.hop");
    oplog = new HFileSortedOplog(hdfsStore, path3,
        blockCache, stats, storeStats);
    createHoplog(2000, oplog);
    
    Path[] paths = {path1, path2, path3, path1, path2, path3};
    long half = oplog.getSize()/2;
    long[] starts = {0, 0, 0, half + 1, half + 1, half + 1};
    long[] lengths = {half, half, half, oplog.getSize(), oplog.getSize(), oplog.getSize()};
    HDFSSplitIterator iter = HDFSSplitIterator.newInstance(
        hdfsStore.getFileSystem(), paths, starts, lengths, 0, 0);
    
    int[] keyCounts = new int[2000];
    while (iter.hasNext()) {
      boolean success = iter.next();
      assertTrue(success);
      String key = new String((byte[])iter.getKey()).substring("key-".length());
      keyCounts[Integer.valueOf(key) - 100000] ++;
    }
    
    for (int i : keyCounts) {
      assertEquals(3, i);
    }
  }
  
  public void testMRLikeNHopIter() throws Exception {
    Path path1 = new Path(testDataDir, "region/0/1-1-1.hop");
    Hoplog oplog = new HFileSortedOplog(hdfsStore, path1,
        blockCache, stats, storeStats);
    createHoplog(10, oplog);
    
    Path path2 = new Path(testDataDir, "region/0/1-2-1.hop");
    oplog = new HFileSortedOplog(hdfsStore, path2,
        blockCache, stats, storeStats);
    createHoplog(10, oplog);
    
    Path path3 = new Path(testDataDir, "region/0/1-3-1.hop");
    oplog = new HFileSortedOplog(hdfsStore, path3,
        blockCache, stats, storeStats);
    createHoplog(10, oplog);
    
    Path[] paths = {path1, path2, path3};
    long[] starts = {0, 0, 0};
    long[] lengths = {oplog.getSize(), oplog.getSize(), oplog.getSize()};
    HDFSSplitIterator iter = HDFSSplitIterator.newInstance(
        hdfsStore.getFileSystem(), paths, starts, lengths, 0, 0);
    
    int[] keyCounts = new int[10];
    while (iter.hasNext()) {
      boolean success = iter.next();
      assertTrue(success);
      // extra has next before key read
      iter.hasNext(); 
      String key = new String((byte[])iter.getKey()).substring("key-".length());
      System.out.println(key);
      keyCounts[Integer.valueOf(key)] ++;
    }
    
    for (int i : keyCounts) {
      assertEquals(3, i);
    }
  }
  
  public void test1Hop1BlockIterSkipDeletedHoplogs() throws Exception {
    FileSystem fs = hdfsStore.getFileSystem();
    Path path = new Path(testDataDir, "region/0/1-1-1.hop");
    Hoplog oplog = new HFileSortedOplog(hdfsStore, path,
        blockCache, stats, storeStats);
    createHoplog(10, oplog);

    Path[] paths = {path};
    long[] starts = {0};
    long[] lengths = {oplog.getSize()};
    
    //Delete the Hoplog file
    fs.delete(path, true);
    
    HDFSSplitIterator iter = HDFSSplitIterator.newInstance(
        hdfsStore.getFileSystem(), paths, starts, lengths, 0, 0);
    assertFalse(iter.hasNext());
    
  }
  
  public void testMRLikeNHopIterSkipDeletedHoplogs() throws Exception {
    FileSystem fs = hdfsStore.getFileSystem();
    //Create Hoplogs
    Path path1 = new Path(testDataDir, "region/0/1-1-1.hop");
    Hoplog oplog = new HFileSortedOplog(hdfsStore, path1,
        blockCache, stats, storeStats);
    createHoplog(10, oplog);
    
    Path path2 = new Path(testDataDir, "region/0/1-2-1.hop");
    oplog = new HFileSortedOplog(hdfsStore, path2,
        blockCache, stats, storeStats);
    createHoplog(10, oplog);
    
    Path path3 = new Path(testDataDir, "region/0/1-3-1.hop");
    oplog = new HFileSortedOplog(hdfsStore, path3,
        blockCache, stats, storeStats);
    createHoplog(10, oplog);
    
    Path[] paths = {path1, path2, path3};
    long[] starts = {0, 0, 0};
    long[] lengths = {oplog.getSize(), oplog.getSize(), oplog.getSize()};
    HDFSSplitIterator iter = HDFSSplitIterator.newInstance(
        hdfsStore.getFileSystem(), paths, starts, lengths, 0, 0);
    int count = 0;
    while (iter.hasNext()) {
      boolean success = iter.next();
      assertTrue(success);
      count++;
    }
    assertEquals(30, count);
    
    for(int i = 0; i < 3; ++i){
      fs.delete(paths[i], true);
      iter = HDFSSplitIterator.newInstance(
          hdfsStore.getFileSystem(), paths, starts, lengths, 0, 0);
      count = 0;
      while (iter.hasNext()) {
        boolean success = iter.next();
        assertTrue(success);
        count++;
      }
      assertEquals(20, count);
      oplog = new HFileSortedOplog(hdfsStore, paths[i],
          blockCache, stats, storeStats);
      createHoplog(10, oplog);
    }
  }
}
