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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.SortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogSetReader.HoplogIterator;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSStatsJUnitTest extends BaseHoplogTestCase {
  public void testStoreUsageStats() throws Exception {
    HoplogOrganizer bucket = regionManager.create(0);
    
    long oldUsage = 0;
    assertEquals(oldUsage, stats.getStoreUsageBytes());

    for (int j = 0; j < 5; j++) {
      ArrayList<TestEvent> items = new ArrayList<TestEvent>();
      for (int i = 0; i < 100; i++) {
        String key = ("key-" + (j * 100 + i));
        String value = ("value-" + System.nanoTime());
        items.add(new TestEvent(key, value));
      }
      bucket.flush(items.iterator(), 100);
    }
    
    assertTrue(0 < stats.getStoreUsageBytes());
    oldUsage = stats.getStoreUsageBytes();
    
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    assertEquals(2, stats.getStoreUsageBytes() / oldUsage);
    
    organizer.close();
    assertEquals(1, stats.getStoreUsageBytes() / oldUsage);
  }
  
  public void testWriteStats() throws Exception {
    HoplogOrganizer bucket = regionManager.create(0);

    // validate flush stats
    // flush and create many hoplogs and execute one compaction cycle also
    // 5 hoplogs, total 500 keys
    assertEquals(0, stats.getFlush().getCount());
    assertEquals(0, stats.getFlush().getBytes());
    assertEquals(0, stats.getActiveFileCount());
    int bytesSent = 0;
    for (int j = 0; j < 5; j++) {
      ArrayList<TestEvent> items = new ArrayList<TestEvent>();
      for (int i = 0; i < 100; i++) {
        String key = ("key-" + (j * 100 + i));
        String value = ("value-" + System.nanoTime());
        items.add(new TestEvent(key, value));
        bytesSent += (key.getBytes().length + value.getBytes().length);
      }
      bucket.flush(items.iterator(), 100);

      // verify stats show
      assertEquals(j + 1, stats.getFlush().getCount());
      assertTrue(stats.getFlush().getBytes() > bytesSent);
      assertEquals(j + 1, stats.getActiveFileCount());
    }

    // verify compaction stats
    assertEquals(0, stats.getMinorCompaction().getCount());
    assertEquals(0, stats.getMinorCompaction().getBytes());
    assertEquals(0, stats.getInactiveFileCount());
    bucket.getCompactor().compact(false, false);
    assertEquals(1, stats.getMinorCompaction().getCount());
    assertEquals(1, stats.getActiveFileCount());
    assertEquals(0, stats.getInactiveFileCount());
    assertEquals(stats.getMinorCompaction().getBytes(), stats.getFlush()
        .getBytes());
  }
  
  public void testInactiveFileStats() throws Exception {
    // steps 
    // create files -> validate active and inactive file count
    // -> increment reference by using scanner-> compact -> verify active and inactive file count 
    HoplogOrganizer bucket = regionManager.create(0);
    assertEquals(0, stats.getActiveFileCount());
    assertEquals(0, stats.getInactiveFileCount());
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int j = 0; j < 5; j++) {
      items.clear();
      for (int i = 0; i < 100; i++) {
        String key = ("key-" + (j * 100 + i));
        String value = ("value-" + System.nanoTime());
        items.add(new TestEvent(key, value));
      }
      bucket.flush(items.iterator(), 100);
    }
    
    assertEquals(5, stats.getActiveFileCount());
    assertEquals(0, stats.getInactiveFileCount());
    
    HoplogIterator<byte[], PersistedEventImpl> scanner = bucket.scan();
    bucket.getCompactor().compact(true, false);
    assertEquals(1, stats.getActiveFileCount());
    assertEquals(5, stats.getInactiveFileCount());
    
    scanner.close();
    assertEquals(1, stats.getActiveFileCount());
    assertEquals(0, stats.getInactiveFileCount());
  }

  public void testReadStats() throws Exception {
    HoplogOrganizer<SortedHoplogPersistedEvent> bucket = regionManager.create(0);

    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 100; i++) {
      items.add(new TestEvent("key-" + i, "value-" + System.nanoTime()));
    }
    bucket.flush(items.iterator(), 100);
    
    // validate read stats
    assertEquals(0, stats.getRead().getCount());
    assertEquals(0, stats.getRead().getBytes());
    // number of bytes read must be greater than size of key and value and must be increasing
    int bytesRead = "key-1".getBytes().length + "value=1233232".getBytes().length;
    for (int i = 0; i < 5; i++) {
      long previousRead = stats.getRead().getBytes();
      PersistedEventImpl e = bucket.read(BlobHelper.serializeToBlob("key-" + i));
      assertNotNull(e);
      assertEquals(i + 1, stats.getRead().getCount());
      assertTrue( (bytesRead + previousRead) < stats.getRead().getBytes());
    }
    
    //Make sure the block cache stats are being updated.
//    assertTrue(storeStats.getBlockCache().getMisses() > 0);
//    assertTrue(storeStats.getBlockCache().getBytesCached() > 0);
//    assertTrue(storeStats.getBlockCache().getCached() > 0);
    
    //Do a duplicate read to make sure we get a hit in the cache
//    bucket.read(BlobHelper.serializeToBlob("key-" + 0));
//    assertTrue(storeStats.getBlockCache().getHits() > 0);
  }

  public void testBloomStats() throws Exception {
    HoplogOrganizer bucket = regionManager.create(0);

    // create 10 hoplogs
    for (int j = 0; j < 5; j++) {
      ArrayList<TestEvent> items = new ArrayList<TestEvent>();
      for (int i = 0; i < 100; i++) {
        String key = ("key-" + (j * 100 + i));
        String value = ("value-" + System.nanoTime());
        items.add(new TestEvent(key, value));
      }
      bucket.flush(items.iterator(), 100);
    }

    // initially bloom stat will be zero
    // reading key in first hop will increase bloom hit by 1 (key 0 to 99)
    // reading key in 5 hoplog will increase bloom hit by 5 (key 400 to 499)
    assertEquals(0, stats.getBloom().getCount());
    bucket.read(BlobHelper.serializeToBlob("key-450"));
    assertEquals(1, stats.getBloom().getCount());
    bucket.read(BlobHelper.serializeToBlob("key-50"));
    assertEquals(6, stats.getBloom().getCount());
  }
  
  public void testScanStats() throws Exception {
    HFileSortedOplog hoplog = new HFileSortedOplog(hdfsStore, new Path(
          testDataDir, "H-1-1.hop"),blockCache, stats, storeStats);
    createHoplog(5, hoplog);
    
    // initially scan stats will be zero. creating a scanner should increase
    // scan iteration stats and bytes. On scanner close scan count should be
    // incremented
    assertEquals(0, stats.getScan().getCount());
    assertEquals(0, stats.getScan().getBytes());
    assertEquals(0, stats.getScan().getTime());
    assertEquals(0, stats.getScan().getIterations());
    assertEquals(0, stats.getScan().getIterationTime());
    
    HoplogIterator<byte[], byte[]> scanner = hoplog.getReader().scan();
    assertEquals(0, stats.getScan().getCount());
    int count = 0;
    for (byte[] bs = null; scanner.hasNext(); ) {
      bs = scanner.next();
      count += bs.length + scanner.getValue().length;
    }
    assertEquals(count, stats.getScan().getBytes());
    assertEquals(5, stats.getScan().getIterations());
    assertTrue(0 < stats.getScan().getIterationTime());
    // getcount will be 0 as scanner.close is not being called
    assertEquals(0, stats.getScan().getCount());
    assertEquals(0, stats.getScan().getTime());
    assertEquals(1, stats.getScan().getInProgress());
    
    scanner.close();
    assertEquals(1, stats.getScan().getCount());
    assertTrue(0 < stats.getScan().getTime());
    assertTrue(stats.getScan().getIterationTime() <= stats.getScan().getTime());
  }
  
  /**
   * Validates two buckets belonging to same region update the same stats
   */
  public void testRegionBucketShareStats() throws Exception {
    HoplogOrganizer bucket1 = regionManager.create(0);
    HoplogOrganizer bucket2 = regionManager.create(1);

    // validate flush stats
    assertEquals(0, stats.getFlush().getCount());
    assertEquals(0, stats.getActiveFileCount());
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 100; i++) {
      items.add(new TestEvent("key-" + i, "value-" + System.nanoTime()));
    }
    bucket1.flush(items.iterator(), 100);
    assertEquals(1, stats.getFlush().getCount());
    assertEquals(1, stats.getActiveFileCount());
    items.clear();

    for (int i = 0; i < 100; i++) {
      items.add(new TestEvent("key-" + i, "value-" + System.nanoTime()));
    }
    bucket2.flush(items.iterator(), 100);
    assertEquals(2, stats.getFlush().getCount());
    assertEquals(2, stats.getActiveFileCount());
  }

  @Override
  protected Cache createCache() {
    CacheFactory cf = new CacheFactory().set("mcast-port", "0")
        .set("log-level", "info")
        .set("enable-time-statistics", "true")
//        .set("statistic-archive-file", "statArchive.gfs")
        ;
    cache = cf.create();

    return cache;
  }
}
