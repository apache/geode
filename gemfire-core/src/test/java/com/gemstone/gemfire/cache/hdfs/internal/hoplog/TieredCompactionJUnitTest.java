/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.QueuedPersistentEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HdfsSortedOplogOrganizer.HoplogCompactor;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogOrganizer.Compactor;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.persistence.soplog.TrackedReference;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

@Category({IntegrationTest.class, HoplogTest.class})
public class TieredCompactionJUnitTest extends BaseHoplogTestCase {
  static long ONE_MB = 1024 * 1024;
  static long TEN_MB = 10 * ONE_MB;
  
  @Override
  protected void configureHdfsStoreFactory() throws Exception {
    super.configureHdfsStoreFactory();
    
    hsf.setMinInputFileCount(3);
    hsf.setMinorCompaction(false);
    hsf.setMajorCompaction(false);
  }
  
  public void testMinorCompaction() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);

    // #1
    ArrayList<QueuedPersistentEvent> items = new ArrayList<QueuedPersistentEvent>();
    items.add(new TestEvent("1", "1"));
    items.add(new TestEvent("2", "1"));
    items.add(new TestEvent("3", "1"));
    items.add(new TestEvent("4", "1"));
    organizer.flush(items.iterator(), items.size());

    // #2
    items.clear();
    items.add(new TestEvent("2", "1"));
    items.add(new TestEvent("4", "1"));
    items.add(new TestEvent("6", "1"));
    items.add(new TestEvent("8", "1"));
    organizer.flush(items.iterator(), items.size());

    // #3
    items.clear();
    items.add(new TestEvent("1", "1"));
    items.add(new TestEvent("3", "1"));
    items.add(new TestEvent("5", "1"));
    items.add(new TestEvent("7", "1"));
    items.add(new TestEvent("9", "1"));
    organizer.flush(items.iterator(), items.size());

    // #4
    items.clear();
    items.add(new TestEvent("0", "1"));
    items.add(new TestEvent("1", "1"));
    items.add(new TestEvent("4", "1"));
    items.add(new TestEvent("5", "1"));
    organizer.flush(items.iterator(), items.size());

    // check file existence in bucket directory, expect 4 hoplgos
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(4, hoplogs.length);

    // After compaction expect 1 hoplog only. It should have the same sequence number as that of the
    // youngest file compacted, which should be 4 in this case
    organizer.getCompactor().compact(false, false);
    hoplogs = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.MINOR_HOPLOG_EXTENSION);
    assertEquals(1, hoplogs.length);
    assertEquals(1, organizer.getSortedOplogs().size());
    Hoplog hoplog = new HFileSortedOplog(hdfsStore, hoplogs[0].getPath(), blockCache, stats, storeStats);
    assertEquals(4, HdfsSortedOplogOrganizer.getSequenceNumber(hoplog));

    // iterate on oplogs to validate data in files
    HoplogSetIterator iter = new HoplogSetIterator(organizer.getSortedOplogs());
    // the iteration pattern for this test should be 0-9:
    // 0 1 4 5 oplog #4
    // 1 3 5 7 9 oplog #3
    // 2 4 6 8 oplog #2
    // 1 2 3 4 oplog #1
    int count = 0;
    for (ByteBuffer keyBB = null; iter.hasNext();) {
      keyBB = iter.next();
      byte[] key = HFileSortedOplog.byteBufferToArray(keyBB);
      assertEquals(String.valueOf(count), BlobHelper.deserializeBlob(key));
      count++;
    }
    assertEquals(10, count);

    // there must be 4 expired hoplogs now
    hoplogs = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(4, hoplogs.length);
    organizer.close();
  }
  
  public void testIterativeMinorCompaction() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);

    // #1
    ArrayList<QueuedPersistentEvent> items = new ArrayList<QueuedPersistentEvent>();
    items.add(new TestEvent("1", "1"));
    items.add(new TestEvent("2", "1"));
    organizer.flush(items.iterator(), items.size());

    items.clear();
    items.add(new TestEvent("1", "2"));
    items.add(new TestEvent("3", "2"));
    organizer.flush(items.iterator(), items.size());

    items.clear();
    items.add(new TestEvent("4", "3"));
    items.add(new TestEvent("5", "3"));
    organizer.flush(items.iterator(), items.size());
    
    // check file existence in bucket directory
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(3, hoplogs.length);

    organizer.getCompactor().compact(false, false);
    
    FileStatus[] expired = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(3, expired.length);
    FileStatus[] valids = HdfsSortedOplogOrganizer.filterValidHoplogs(hoplogs, expired);
    assertEquals(0, valids.length);
    // After compaction expect 1 hoplog only.
    hoplogs = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.MINOR_HOPLOG_EXTENSION);
    assertEquals(1, hoplogs.length);
    
    items.clear();
    items.add(new TestEvent("4", "4"));
    items.add(new TestEvent("6", "4"));
    organizer.flush(items.iterator(), items.size());

    items.clear();
    items.add(new TestEvent("7", "5"));
    items.add(new TestEvent("8", "5"));
    organizer.flush(items.iterator(), items.size());
    
    hoplogs = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(5, hoplogs.length);
    
    organizer.getCompactor().compact(false, false);
    expired = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(6, expired.length);
    valids = HdfsSortedOplogOrganizer.filterValidHoplogs(hoplogs, expired);
    assertEquals(0, valids.length);    
    hoplogs = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.MINOR_HOPLOG_EXTENSION);
    assertEquals(2, hoplogs.length);
    valids = HdfsSortedOplogOrganizer.filterValidHoplogs(hoplogs, expired);
    assertEquals(1, valids.length);
    
    assertEquals("2", organizer.read(BlobHelper.serializeToBlob("1")).getValue());
    assertEquals("1", organizer.read(BlobHelper.serializeToBlob("2")).getValue());
    assertEquals("2", organizer.read(BlobHelper.serializeToBlob("3")).getValue());
    assertEquals("4", organizer.read(BlobHelper.serializeToBlob("4")).getValue());
    assertEquals("3", organizer.read(BlobHelper.serializeToBlob("5")).getValue());
    assertEquals("4", organizer.read(BlobHelper.serializeToBlob("6")).getValue());
    assertEquals("5", organizer.read(BlobHelper.serializeToBlob("7")).getValue());
    organizer.close();
  }

  public void testMajorCompactionWithDelete() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);

    // #1
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent("1", "1"));
    items.add(new TestEvent("2", "1"));
    items.add(new TestEvent("3", "1"));
    items.add(new TestEvent("4", "1"));
    items.add(new TestEvent("4", "10", Operation.DESTROY));
    organizer.flush(items.iterator(), items.size());

    // #2
    items.clear();
    items.add(new TestEvent("2", "1", Operation.DESTROY));
    items.add(new TestEvent("4", "1", Operation.DESTROY));
    items.add(new TestEvent("6", "1", Operation.INVALIDATE));
    items.add(new TestEvent("8", "1"));
    organizer.flush(items.iterator(), items.size());

    // #3
    items.clear();
    items.add(new TestEvent("1", "1"));
    items.add(new TestEvent("3", "1"));
    items.add(new TestEvent("5", "1"));
    items.add(new TestEvent("7", "1"));
    items.add(new TestEvent("9", "1", Operation.DESTROY));
    organizer.flush(items.iterator(), items.size());

    // #4
    items.clear();
    items.add(new TestEvent("0", "1", Operation.DESTROY));
    items.add(new TestEvent("1", "1"));
    items.add(new TestEvent("4", "1"));
    items.add(new TestEvent("5", "1"));
    organizer.flush(items.iterator(), items.size());

    // check file existence in bucket directory, expect 4 hoplgos
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(4, hoplogs.length);

    // After compaction expect 1 hoplog only. It should have the same sequence number as that of the
    // youngest file compacted, which should be 4 in this case
    organizer.getCompactor().compact(true, false);
    hoplogs = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    assertEquals(1, hoplogs.length);
    assertEquals(1, organizer.getSortedOplogs().size());
    Hoplog hoplog = new HFileSortedOplog(hdfsStore, hoplogs[0].getPath(), blockCache, stats, storeStats);
    assertEquals(4, HdfsSortedOplogOrganizer.getSequenceNumber(hoplog));

    // iterate on oplogs to validate data in files
    HoplogSetIterator iter = new HoplogSetIterator(organizer.getSortedOplogs());
    int count = 0;

    // entries in () are destroyed or invalidated
    // 1, 2, 3, 4, (11)
    // (2), (4), (6), 8
    // 1, 3, 5, 7, (9)
    // (0), 1, 4, 5
    String[] expectedValues = { "1", "3", "4", "5", "7", "8" };
    for (ByteBuffer keyBB = null; iter.hasNext();) {
      keyBB = iter.next();
      byte[] key = HFileSortedOplog.byteBufferToArray(keyBB);
      assertEquals(expectedValues[count], BlobHelper.deserializeBlob(key));
      count++;
    }
    assertEquals(6, count);

    // there must be 4 expired hoplogs now
    hoplogs = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(4, hoplogs.length);
    organizer.close();
  }
  
  public void testGainComputation() throws Exception {
    HoplogOrganizer<? extends PersistedEventImpl> organizer = regionManager.create(0);
    HdfsSortedOplogOrganizer bucket = (HdfsSortedOplogOrganizer) organizer;
    ArrayList<TrackedReference<Hoplog>> targets = new ArrayList<TrackedReference<Hoplog>>();
    for (int i = 0; i < 10; i++) {
      targets.add(new TrackedReference<Hoplog>(new TestHoplog(hdfsStore, i * TEN_MB)));
    }    

    // each read has cost 3. Four files read cost is 3 * 4. Reduce read cost of
    // file after compaction
    float expect = (float) ((3 * 4.0 - 3) / (20 + 30 + 40 + 50));
    float result = bucket.computeGain(2, 5, targets);
    assertTrue(Math.abs(expect - result) < (expect/1000));
    
    // each read has cost 3 except 10MB file with read cost 2. 9 files read cost
    // is 3 * 9. Reduce read cost of file after compaction.
    expect = (float) ((3 * 9 - 3 - 1.0) / (10 + 20 + 30 + 40 + 50 + 60 + 70 + 80 + 90));
    result = bucket.computeGain(0, 9, targets);
    assertTrue(Math.abs(expect - result) < (expect/1000));
  }

  public void testGainComputeSmallFile() throws Exception {
    HoplogOrganizer<? extends PersistedEventImpl> organizer = regionManager.create(0);
    HdfsSortedOplogOrganizer bucket = (HdfsSortedOplogOrganizer) organizer;
    
    ArrayList<TrackedReference<Hoplog>> targets = new ArrayList<TrackedReference<Hoplog>>();
    for (int i = 0; i < 10; i++) {
      targets.add(new TrackedReference<Hoplog>(new TestHoplog(hdfsStore, i * TEN_MB / 1024)));
    }

    float result = bucket.computeGain(2, 5, targets);
    assertTrue(Math.abs(8.0 - result) < (1.0/1000));
  }
  
  public void testGainComputeMixedFiles() throws Exception {
    HoplogOrganizer<? extends PersistedEventImpl> organizer = regionManager.create(0);
    HdfsSortedOplogOrganizer bucket = (HdfsSortedOplogOrganizer) organizer;
    
    ArrayList<TrackedReference<Hoplog>> targets = new ArrayList<TrackedReference<Hoplog>>();
    for (int i = 0; i < 10; i++) {
      targets.add(new TrackedReference<Hoplog>(new TestHoplog(hdfsStore, i * TEN_MB / 1024)));
    }
    TestHoplog midHop = (TestHoplog) targets.get(4).get();
    // one more than other files
    midHop.size = 5  * TEN_MB;
    
    float expect = (float) ((4 * 2 - 3 + 1.0) / 50);
    float result = bucket.computeGain(2, 5, targets);
    System.out.println(expect);
    System.out.println(result);
    assertTrue(Math.abs(expect - result) < (expect/1000));
  }
  
  public void testGainComputeBadRatio() throws Exception {
    HoplogOrganizer<? extends PersistedEventImpl> organizer = regionManager.create(0);
    HdfsSortedOplogOrganizer bucket = (HdfsSortedOplogOrganizer) organizer;
    ArrayList<TrackedReference<Hoplog>> targets = new ArrayList<TrackedReference<Hoplog>>();
    for (int i = 0; i < 10; i++) {
      targets.add(new TrackedReference<Hoplog>(new TestHoplog(hdfsStore, i * TEN_MB)));
    }

    TestHoplog firstHop = (TestHoplog) targets.get(2).get();
    // one more than other files
    firstHop.size = (1 + 30 + 40 + 50)  * TEN_MB;
    Float result = bucket.computeGain(2, 5, targets);
    assertNull(result);
  }
  
  public void testMinorCompactionTargetMaxSize() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    HoplogCompactor compactor = (HoplogCompactor) organizer.getCompactor();

    ArrayList<TrackedReference<TestHoplog>> targets = new ArrayList<TrackedReference<TestHoplog>>();
    for (int i = 0; i < 5; i++) {
      TrackedReference<TestHoplog> hop = new TrackedReference<TestHoplog>(new TestHoplog(hdfsStore, TEN_MB + i));
      hop.increment();
      targets.add(hop);
    }
    TrackedReference<TestHoplog> oldestHop = targets.get(targets.size() - 1);
    TestHoplog thirdHop = (TestHoplog) targets.get(2).get();

    // oldest is more than max size is ignored 
    oldestHop.get().size = HDFSStore.DEFAULT_MAX_INPUT_FILE_SIZE_MB * ONE_MB + 100;
    List<TrackedReference<Hoplog>> list = (List<TrackedReference<Hoplog>>) targets.clone();
    compactor.getMinorCompactionTargets(list, -1);
    assertEquals(4, list.size());
    for (TrackedReference<Hoplog> ref : list) {
      assertTrue(((TestHoplog)ref.get()).size - TEN_MB < 5 );
    }
    
    // third is more than max size but is not ignored
    thirdHop.size = HDFSStore.DEFAULT_MAX_INPUT_FILE_SIZE_MB * ONE_MB + 100;
    oldestHop.increment();
    list = (List<TrackedReference<Hoplog>>) targets.clone();
    compactor.getMinorCompactionTargets(list, -1);
    assertEquals(4, list.size());
    int i = 0;
    for (TrackedReference<Hoplog> ref : list) {
      if (i != 2) {
        assertTrue(((TestHoplog) ref.get()).size - TEN_MB < 5);
      } else {
        assertTrue(((TestHoplog) ref.get()).size > HDFSStore.DEFAULT_MAX_INPUT_FILE_SIZE_MB * ONE_MB);
      }
      i++;
    }
  }
  
  public void testAlterMaxInputFileSize() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    HoplogCompactor compactor = (HoplogCompactor) organizer.getCompactor();

    assertTrue(TEN_MB * 2 < hdfsStore.getMaxInputFileSizeMB() * ONE_MB);
    
    ArrayList<TrackedReference<TestHoplog>> targets = new ArrayList<TrackedReference<TestHoplog>>();
    for (int i = 0; i < 5; i++) {
      TrackedReference<TestHoplog> hop = new TrackedReference<TestHoplog>(new TestHoplog(hdfsStore, TEN_MB + i));
      hop.increment();
      targets.add(hop);
    }
    
    List<TrackedReference<Hoplog>> list = (List<TrackedReference<Hoplog>>) targets.clone();
    compactor.getMinorCompactionTargets(list, -1);
    assertEquals(targets.size(), list.size());
    
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    mutator.setMaxInputFileSizeMB(1);
    hdfsStore.alter(mutator);
    
    compactor.getMinorCompactionTargets(list, -1);
    assertEquals(0, list.size());
  }
  
  public void testAlterInputFileCount() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    HoplogCompactor compactor = (HoplogCompactor) organizer.getCompactor();
    
    assertTrue(2 < hdfsStore.getMaxInputFileCount());
    
    ArrayList<TrackedReference<TestHoplog>> targets = new ArrayList<TrackedReference<TestHoplog>>();
    for (int i = 0; i < 5; i++) {
      TrackedReference<TestHoplog> hop = new TrackedReference<TestHoplog>(new TestHoplog(hdfsStore, TEN_MB + i));
      hop.increment();
      targets.add(hop);
    }
    
    List<TrackedReference<Hoplog>> list = (List<TrackedReference<Hoplog>>) targets.clone();
    compactor.getMinorCompactionTargets(list, -1);
    assertEquals(targets.size(), list.size());
    
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    mutator.setMaxInputFileCount(2);
    mutator.setMinInputFileCount(2);
    hdfsStore.alter(mutator);
    
    compactor.getMinorCompactionTargets(list, -1);
    assertEquals(2, list.size());
  }
  
  public void testAlterMajorCompactionInterval() throws Exception {
    final AtomicInteger majorCReqCount = new AtomicInteger(0);
    
    final Compactor compactor = new AbstractCompactor() {
      @Override
      public boolean compact(boolean isMajor, boolean isForced) throws IOException {
        majorCReqCount.incrementAndGet();
        return true;
      }
    };
    
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0) {
      @Override
      public synchronized Compactor getCompactor() {
        return compactor;
      }
    };

    // create hoplog in the past, 90 seconds before current time
    organizer.hoplogCreated(getName(), 0, new TestHoplog(hdfsStore, ONE_MB, System.currentTimeMillis() - 90000));
    TimeUnit.MILLISECONDS.sleep(50);
    organizer.hoplogCreated(getName(), 0, new TestHoplog(hdfsStore, ONE_MB, System.currentTimeMillis() - 90000));
    
    alterMajorCompaction(hdfsStore, true);
    
    List<TrackedReference<Hoplog>> hoplogs = organizer.getSortedOplogs();
    assertEquals(2, hoplogs.size());
    
    organizer.performMaintenance();
    TimeUnit.MILLISECONDS.sleep(100);
    assertEquals(0, majorCReqCount.get());
    
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    mutator.setMajorCompactionInterval(1);
    hdfsStore.alter(mutator);
    
    organizer.performMaintenance();
    TimeUnit.MILLISECONDS.sleep(100);
    assertEquals(1, majorCReqCount.get());
  }

  public void testMinorCompactionTargetMinCount() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    HoplogCompactor compactor = (HoplogCompactor) organizer.getCompactor();
    
    ArrayList<TrackedReference<Hoplog>> targets = new ArrayList<TrackedReference<Hoplog>>();
    for (int i = 0; i < 2; i++) {
      TrackedReference<Hoplog> hop = new TrackedReference<Hoplog>(new TestHoplog(hdfsStore, TEN_MB + i));
      hop.increment();
      targets.add(hop);
    }
    compactor.getMinorCompactionTargets(targets, -1);
    assertEquals(0, targets.size());
  }
  
  public void testMinorCompactionLessTargetsStatsUpdate() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent("1", "1"));
    organizer.flush(items.iterator(), items.size());

    items.clear();
    items.add(new TestEvent("2", "2", Operation.DESTROY));
    organizer.flush(items.iterator(), items.size());
    
    TimeUnit.SECONDS.sleep(1);
    List<TrackedReference<Hoplog>> hoplogs = organizer.getSortedOplogs();
    assertEquals(2, hoplogs.size());
    
    organizer.performMaintenance();
    hoplogs = organizer.getSortedOplogs();
    assertEquals(2, hoplogs.size());
  }
  
  public void testMinorCompactionTargetsOptimizer() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    HoplogCompactor compactor = (HoplogCompactor) organizer.getCompactor();

    ArrayList<TrackedReference<Hoplog>> targets = new ArrayList<TrackedReference<Hoplog>>();
    for (int i = 0; i < 6; i++) {
      TrackedReference<Hoplog> hop = new TrackedReference<Hoplog>(new TestHoplog(hdfsStore, TEN_MB + i));
      hop.increment();
      targets.add(hop);
    }
    List<TrackedReference<Hoplog>> list = (List<TrackedReference<Hoplog>>) targets.clone();
    compactor.getMinorCompactionTargets(list, -1);
    assertEquals(6, list.size());
    
    TestHoplog fifthHop = (TestHoplog) targets.get(4).get();
    // fifth hop needs additional block read as it has more than max keys size 
    fifthHop.size = (HdfsSortedOplogOrganizer.AVG_NUM_KEYS_PER_INDEX_BLOCK * 5 + 1) * 64 * 1024;
    list = (List<TrackedReference<Hoplog>>) targets.clone();
    compactor.getMinorCompactionTargets(list, -1);
    assertEquals(4, list.size());
    for (TrackedReference<Hoplog> ref : list) {
      assertTrue(((TestHoplog)ref.get()).size - TEN_MB < 4 );
    }
  }
  
  public void testTargetsReleasedBadRatio() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    HoplogCompactor compactor = (HoplogCompactor) organizer.getCompactor();

    ArrayList<TrackedReference<Hoplog>> targets = new ArrayList<TrackedReference<Hoplog>>();
    for (int i = 0; i < 3; i++) {
      TrackedReference<Hoplog> hop = new TrackedReference<Hoplog>(new TestHoplog(hdfsStore, TEN_MB + i));
      hop.increment();
      targets.add(hop);
    }
    TestHoplog oldestHop = (TestHoplog) targets.get(2).get();
    oldestHop.size = (1 + 30)  * TEN_MB;
    
    List<TrackedReference<Hoplog>> list = (List<TrackedReference<Hoplog>>) targets.clone();
    compactor.getMinorCompactionTargets(list, -1);
    assertEquals(0, list.size());
    assertEquals(3, targets.size());
    for (TrackedReference<Hoplog> ref : targets) {
      assertEquals(0, ref.uses());
    }
  }
  
  public void testMinorCTargetsIgnoreMajorC() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 7; i++) {
      items.clear();
      items.add(new TestEvent("1" + i, "1" + i));
      organizer.flush(items.iterator(), items.size());
    }
    
    HoplogCompactor compactor = (HoplogCompactor) organizer.getCompactor();
    List<TrackedReference<Hoplog>> targets = organizer.getSortedOplogs();
    compactor.getMinorCompactionTargets(targets, -1);
    assertEquals(7, targets.size());
    
    targets = organizer.getSortedOplogs();
    for (TrackedReference<Hoplog> ref : targets) {
      ref.increment();
    }
    compactor.getMinorCompactionTargets(targets, 2);
    assertEquals((7 - 2), targets.size());
    targets = organizer.getSortedOplogs();
    for (int i = 0; i < targets.size(); i++) {
      if (i + 1 <= (7 - 2)) {
        assertEquals(1, targets.get(i).uses());
      } else {
        assertEquals(0, targets.get(i).uses());
      }
    }
    
    targets = organizer.getSortedOplogs();
    for (TrackedReference<Hoplog> ref : targets) {
      if (ref.uses() == 0) {
        ref.increment();
      }
      assertEquals(1, ref.uses());
    }
    compactor.getMinorCompactionTargets(targets, 7);
    assertEquals(0, targets.size());
    
    targets = organizer.getSortedOplogs();
    for (int i = 0; i < targets.size(); i++) {
      assertEquals(0, targets.get(i).uses());
    }
  }
  
  public void testTargetOverlap() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 7; i++) {
      items.clear();
      items.add(new TestEvent("1" + i, "1" + i));
      organizer.flush(items.iterator(), items.size());
    }
    
    HoplogCompactor compactor = (HoplogCompactor) organizer.getCompactor();
    List<TrackedReference<Hoplog>> targets = organizer.getSortedOplogs();
    assertTrue(compactor.isMinorMajorOverlap(targets, 8));
    assertTrue(compactor.isMinorMajorOverlap(targets, 7));
    assertTrue(compactor.isMinorMajorOverlap(targets, 6));
    assertTrue(compactor.isMinorMajorOverlap(targets, 1));
    assertFalse(compactor.isMinorMajorOverlap(targets, 0));
    assertFalse(compactor.isMinorMajorOverlap(targets, -1));
    
    targets.remove(targets.size() -1); // remove the last one 
    targets.remove(targets.size() -1); // remove the last one again
    assertFalse(compactor.isMinorMajorOverlap(targets, 1));
    assertFalse(compactor.isMinorMajorOverlap(targets, 2));
    assertTrue(compactor.isMinorMajorOverlap(targets, 3));
    
    targets.remove(3); // remove from the middle, seq num 4
    assertTrue(compactor.isMinorMajorOverlap(targets, 4));
    assertTrue(compactor.isMinorMajorOverlap(targets, 3));
  }
  
  public void testSuspendMinorByMajor() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 5; i++) {
      items.clear();
      items.add(new TestEvent("1" + i, "1" + i));
      organizer.flush(items.iterator(), items.size());
    }
    
    HoplogCompactor compactor = (HoplogCompactor) organizer.getCompactor();

    Hoplog hoplog = new HFileSortedOplog(hdfsStore, new Path(testDataDir + "/"
        + getName() + "-" + System.currentTimeMillis() + "-1.ihop.tmp"), blockCache, stats, storeStats);
    compactor.fillCompactionHoplog(false, organizer.getSortedOplogs(), hoplog, -1);
    
    cache.getLogger().info("<ExpectedException action=add>java.lang.InterruptedException</ExpectedException>");
    try {
      compactor.maxMajorCSeqNum.set(3);
      compactor.fillCompactionHoplog(false, organizer.getSortedOplogs(), hoplog, -1);
      fail();
    } catch (InterruptedException e) {
      // expected
    }
    cache.getLogger().info("<ExpectedException action=remove>java.lang.InterruptedException</ExpectedException>");
    organizer.close();
  }
  
  public void testMajorCompactionSetsSeqNum() throws Exception {
    final CountDownLatch compactionStartedLatch = new CountDownLatch(1);
    final CountDownLatch waitLatch = new CountDownLatch(1);
    class MyOrganizer extends HdfsSortedOplogOrganizer {
      final HoplogCompactor compactor = new MyCompactor();
      public MyOrganizer(HdfsRegionManager region, int bucketId) throws IOException {
        super(region, bucketId);
      }
      public synchronized Compactor getCompactor() {
        return compactor;
      }
      class MyCompactor extends HoplogCompactor {
        @Override
        public long fillCompactionHoplog(boolean isMajor,
            List<TrackedReference<Hoplog>> targets, Hoplog output,
            int majorCSeqNum) throws IOException, InterruptedException {
          compactionStartedLatch.countDown();
          waitLatch.await();
          long byteCount = 0;
          try {
            byteCount = super.fillCompactionHoplog(isMajor, targets, output, majorCSeqNum);
          } catch (ForceReattemptException e) {
            // we do not expect this in a unit test. 
          }
          return byteCount;
        }
      }
    }
    
    final HdfsSortedOplogOrganizer organizer = new MyOrganizer(regionManager, 0);
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 3; i++) {
      items.clear();
      items.add(new TestEvent("1" + i, "1" + i));
      organizer.flush(items.iterator(), items.size());
    }
    
    Thread t = new Thread(new Runnable() {
      public void run() {
        try {
          organizer.getCompactor().compact(true, false);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
    t.start();
    compactionStartedLatch.await();
    assertEquals(3, ((HoplogCompactor)organizer.getCompactor()).maxMajorCSeqNum.get());
    waitLatch.countDown();
    t.join();
  }
  
  public void testMinorWatchesMajorsSeqNum() throws Exception {
    final CountDownLatch majorCStartedLatch = new CountDownLatch(1);
    final CountDownLatch majorCWaitLatch = new CountDownLatch(1);
    
    final CountDownLatch minorCStartedLatch = new CountDownLatch(1);
    final List<TrackedReference<Hoplog>> minorTargets = new ArrayList<TrackedReference<Hoplog>>();
    
    class MyOrganizer extends HdfsSortedOplogOrganizer {
      final HoplogCompactor compactor = new MyCompactor();
      public MyOrganizer(HdfsRegionManager region, int bucketId) throws IOException {
        super(region, bucketId);
      }
      public synchronized Compactor getCompactor() {
        return compactor;
      }
      class MyCompactor extends HoplogCompactor {
        @Override
        public long fillCompactionHoplog(boolean isMajor,
            List<TrackedReference<Hoplog>> targets, Hoplog output,
            int majorCSeqNum) throws IOException, InterruptedException {
          if (isMajor) {
            majorCStartedLatch.countDown();
            majorCWaitLatch.await();
          } else {
            minorCStartedLatch.countDown();
            minorTargets.addAll(targets);
          }
          long byteCount =0;
          try {
            byteCount = super.fillCompactionHoplog(isMajor, targets, output, majorCSeqNum);
          } catch (ForceReattemptException e) {
            // we do not expect this in a unit test. 
          }
          return byteCount;
        }
      }
    }
    
    final HdfsSortedOplogOrganizer organizer = new MyOrganizer(regionManager, 0);
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 3; i++) {
      items.clear();
      items.add(new TestEvent("1" + i, "1" + i));
      organizer.flush(items.iterator(), items.size());
    }
    
    Thread majorCThread = new Thread(new Runnable() {
      public void run() {
        try {
          organizer.getCompactor().compact(true, false);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
    majorCThread.start();
    majorCStartedLatch.await();
    assertEquals(3, ((HoplogCompactor)organizer.getCompactor()).maxMajorCSeqNum.get());

    // create more files for minor C
    for (int i = 0; i < 4; i++) {
      items.clear();
      items.add(new TestEvent("1" + i, "1" + i));
      organizer.flush(items.iterator(), items.size());
    }
    
    Thread minorCThread = new Thread(new Runnable() {
      public void run() {
        try {
          organizer.getCompactor().compact(false, false);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
    minorCThread.start();
    minorCThread.join();
    assertEquals(4, minorTargets.size());
    for (TrackedReference<Hoplog> ref : minorTargets) {
      assertTrue(organizer.getSequenceNumber(ref.get()) >= 4);
    }
    
    majorCWaitLatch.countDown();
    majorCThread.join();
  }
  
  public void testTimeBoundedSuspend() throws Exception {
    final AtomicBoolean barrier = new AtomicBoolean(true);
    
    class MyOrganizer extends HdfsSortedOplogOrganizer {
      public MyOrganizer(HdfsRegionManager region, int bucketId) throws IOException {
        super(region, bucketId);
      }
      public synchronized Compactor getCompactor() {
        return new MyCompactor();
      }
      class MyCompactor extends HoplogCompactor {
        public long fillCompactionHoplog(boolean isMajor, List<TrackedReference<Hoplog>> targets, Hoplog output)
            throws IOException, InterruptedException {
          barrier.set(false);
          TimeUnit.SECONDS.sleep(5 * HoplogConfig.SUSPEND_MAX_WAIT_MS_DEFAULT);
          long byteCount =0;
          try {
            byteCount = super.fillCompactionHoplog(isMajor, targets, output, -1);
          } catch (ForceReattemptException e) {
            // we do not expect this in a unit test. 
          }
          return byteCount;
        }
      }
    }
    
    HdfsSortedOplogOrganizer organizer = new MyOrganizer(regionManager, 0);
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 4; i++) {
      items.clear();
      items.add(new TestEvent("1" + i, "1" + i));
      organizer.flush(items.iterator(), items.size());
    }

    final HoplogCompactor compactor = (HoplogCompactor) organizer.getCompactor();
    ExecutorService service = Executors.newCachedThreadPool();
    service.execute(new Runnable() {
      public void run() {
        try {
          compactor.compact(false, false);
        } catch (Exception e) {
        }
      }
    });
    
    final AtomicLong start = new AtomicLong(0);
    final AtomicLong end = new AtomicLong(0);
    service.execute(new Runnable() {
      public void run() {
        while (barrier.get()) {
          try {
            TimeUnit.MILLISECONDS.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        
        start.set(System.currentTimeMillis());
        compactor.suspend();
        end.set(System.currentTimeMillis());
      }
    });
    
    for (long i = 0; i < 5; i++) {
      if (end.get() == 0) {
        TimeUnit.MILLISECONDS.sleep(HoplogConfig.SUSPEND_MAX_WAIT_MS_DEFAULT / 2);
      } else {
        break;
      }
    }
    
    assertTrue(end.get() - start.get() < 100 + HoplogConfig.SUSPEND_MAX_WAIT_MS_DEFAULT);
  }
  
  public static class TestHoplog extends AbstractHoplog {
    long size;
    long creationTime;
    TestHoplog(HDFSStoreImpl store, long size) throws IOException {
      this(store, size, 0);
    }
    
    TestHoplog(HDFSStoreImpl store, long size, long creationTime) throws IOException {
      super(store, new Path("1-" + creationTime + "-1.hop"), null);
      this.size = size;
      this.creationTime = creationTime;
    }
    
    @Override
    public long getSize() {
      return size;
    }
    @Override
    public long getModificationTimeStamp() {
      if (creationTime > 0) {
        return creationTime;
      }
      return super.getModificationTimeStamp();
    }
    @Override
    public String toString() {
      long name = size -  TEN_MB;
      if (name < 0) name = size - (TEN_MB / 1024);
      return name + "";
    }
    public boolean isClosed() {
      return false;
    }
    public void close() throws IOException {
    }
    public HoplogReader getReader() throws IOException {
      return null;
    }
    public HoplogWriter createWriter(int keys) throws IOException {
      return null;
    }
    public void close(boolean clearCache) throws IOException {
    }
  }
}
