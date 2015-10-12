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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.SortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplogOrganizer.HoplogComparator;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogOrganizer.Compactor;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.TieredCompactionJUnitTest.TestHoplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.HoplogUtil;
import com.gemstone.gemfire.internal.cache.persistence.soplog.TrackedReference;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import dunit.DistributedTestCase;
import dunit.DistributedTestCase.ExpectedException;
@Category({IntegrationTest.class, HoplogTest.class})
public class HdfsSortedOplogOrganizerJUnitTest extends BaseHoplogTestCase {
  /**
   * Tests flush operation
   */
  public void testFlush() throws Exception {
    int count = 10;
    int bucketId = (int) System.nanoTime();
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, bucketId);

    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < count; i++) {
      items.add(new TestEvent(("key-" + i), ("value-" + System.nanoTime())));
    }
    organizer.flush(items.iterator(), count);

    // check file existence in bucket directory
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/" + bucketId, 
                      HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);

    // only one hoplog should exists
    assertEquals(1, hoplogs.length);
    
    assertEquals(count, organizer.sizeEstimate());
    assertEquals(0, stats.getActiveReaderCount());
  }

  /**
   * Tests reads from a set of hoplogs containing both valid and stale KVs
   */
  public void testReopen() throws Exception {
    int bucketId = (int) System.nanoTime();
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, bucketId);
    
    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 100; i++) {
      items.add(new TestEvent("" + i, ("1-1")));
    }
    organizer.flush(items.iterator(), items.size());
    
    Hoplog hoplog = organizer.getSortedOplogs().iterator().next().get();
    byte[] keyBytes1 = BlobHelper.serializeToBlob("1");
    hoplog.close();
    
    for (int i = 0; i < 10; i++) {
      Path path = new Path(testDataDir, getName() + "/" + bucketId + "/" + hoplog.getFileName());
      HFileSortedOplog oplog = new HFileSortedOplog(hdfsStore, path, blockCache, stats, storeStats);
      oplog.getReader().read(keyBytes1);
      oplog.close(false);
    }
  }
  
  /**
   * Tests reads from a set of hoplogs containing both valid and stale KVs
   */
  public void testRead() throws Exception {
    doRead(regionManager);
  }
  
//  public void testNewReaderWithNameNodeHA() throws Exception {
//    deleteMiniClusterDir();
//    int nn1port = AvailablePortHelper.getRandomAvailableTCPPort();
//    int nn2port = AvailablePortHelper.getRandomAvailableTCPPort();
//    
//    MiniDFSCluster cluster = initMiniHACluster(nn1port, nn2port);
//    initClientHAConf(nn1port, nn2port);
//    
//    HDFSStoreImpl store1 = (HDFSStoreImpl) hsf.create("Store-1");
//    regionfactory.setHDFSStoreName(store1.getName());
//    Region<Object, Object> region1 = regionfactory.create("region-1");
//    HdfsRegionManager regionManager1 = ((LocalRegion)region1).getHdfsRegionManager();
//    
//    HoplogOrganizer<SortedHoplogPersistedEvent> organizer = doRead(regionManager1);
//    organizer.close();
//    
//    dunit.DistributedTestCase.ExpectedException ex = DistributedTestCase.addExpectedException("java.io.EOFException");
//    NameNode nnode2 = cluster.getNameNode(1);
//    assertTrue(nnode2.isStandbyState());
//    cluster.shutdownNameNode(0);
//    cluster.transitionToActive(1);
//    assertFalse(nnode2.isStandbyState());
//    
//    organizer = new HdfsSortedOplogOrganizer(regionManager1, 0);
//    byte[] keyBytes1 = BlobHelper.serializeToBlob("1");
//    byte[] keyBytes3 = BlobHelper.serializeToBlob("3");
//    byte[] keyBytes4 = BlobHelper.serializeToBlob("4");
//    assertEquals("2-1", organizer.read(keyBytes1).getValue());
//    assertEquals("3-3", organizer.read(keyBytes3).getValue());
//    assertEquals("1-4", organizer.read(keyBytes4).getValue());
//    ex.remove();
//
//    region1.destroyRegion();
//    store1.destroy();
//    cluster.shutdown();
//    FileUtils.deleteDirectory(new File("hdfs-test-cluster"));
//  }
  
//  public void testActiveReaderWithNameNodeHA() throws Exception {
//    deleteMiniClusterDir();
//    int nn1port = AvailablePortHelper.getRandomAvailableTCPPort();
//    int nn2port = AvailablePortHelper.getRandomAvailableTCPPort();
//    
//    MiniDFSCluster cluster = initMiniHACluster(nn1port, nn2port);
//    initClientHAConf(nn1port, nn2port);
//    
//    HDFSStoreImpl store1 = (HDFSStoreImpl) hsf.create("Store-1");
//    regionfactory.setHDFSStoreName(store1.getName());
//    Region<Object, Object> region1 = regionfactory.create("region-1");
//    HdfsRegionManager regionManager1 = ((LocalRegion)region1).getHdfsRegionManager();
//    
//    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager1, 0);
//    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
//    for (int i = 100000; i < 101000; i++) {
//      items.add(new TestEvent(("" + i), (i + " some string " + i)));
//    }
//    organizer.flush(items.iterator(), items.size());
//    organizer.getSortedOplogs().get(0).get().getReader();
//    
//    dunit.DistributedTestCase.ExpectedException ex = DistributedTestCase.addExpectedException("java.io.EOFException");
//    NameNode nnode2 = cluster.getNameNode(1);
//    assertTrue(nnode2.isStandbyState());
//    cluster.shutdownNameNode(0);
//    cluster.transitionToActive(1);
//    assertFalse(nnode2.isStandbyState());
//    
//    for (int i = 100000; i < 100500; i++) {
//      byte[] keyBytes1 = BlobHelper.serializeToBlob("" + i);
//      assertEquals(i + " some string " + i, organizer.read(keyBytes1).getValue());
//    }
//    ex.remove();
//    region1.destroyRegion();
//    store1.destroy();
//    cluster.shutdown();
//    FileUtils.deleteDirectory(new File("hdfs-test-cluster"));
//  }
  
//  public void testFlushWithNameNodeHA() throws Exception {
//    deleteMiniClusterDir();
//    int nn1port = AvailablePortHelper.getRandomAvailableTCPPort();
//    int nn2port = AvailablePortHelper.getRandomAvailableTCPPort();
//    
//    MiniDFSCluster cluster = initMiniHACluster(nn1port, nn2port);
//    
//    initClientHAConf(nn1port, nn2port);
//    HDFSStoreImpl store1 = (HDFSStoreImpl) hsf.create("Store-1");
//    
//    regionfactory.setHDFSStoreName(store1.getName());
//    Region<Object, Object> region1 = regionfactory.create("region-1");
//    HdfsRegionManager regionManager1 = ((LocalRegion)region1).getHdfsRegionManager();
//    
//    HoplogOrganizer<SortedHoplogPersistedEvent> organizer = new HdfsSortedOplogOrganizer(regionManager1, 0);
//    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
//    items.add(new TestEvent(("1"), ("1-1")));
//    organizer.flush(items.iterator(), items.size());
//
//    dunit.DistributedTestCase.ExpectedException ex = DistributedTestCase.addExpectedException("java.io.EOFException");
//    NameNode nnode2 = cluster.getNameNode(1);
//    assertTrue(nnode2.isStandbyState());
//    cluster.shutdownNameNode(0);
//    cluster.transitionToActive(1);
//    assertFalse(nnode2.isStandbyState());
//    
//    items.add(new TestEvent(("4"), ("1-4")));
//    organizer.flush(items.iterator(), items.size());
//    byte[] keyBytes1 = BlobHelper.serializeToBlob("1");
//    byte[] keyBytes4 = BlobHelper.serializeToBlob("4");
//    assertEquals("1-1", organizer.read(keyBytes1).getValue());
//    assertEquals("1-4", organizer.read(keyBytes4).getValue());
//    ex.remove();
//    
//    region1.destroyRegion();
//    store1.destroy();
//    cluster.shutdown();
//    FileUtils.deleteDirectory(new File("hdfs-test-cluster"));
//  }

  public HoplogOrganizer<SortedHoplogPersistedEvent> doRead(HdfsRegionManager rm) throws Exception {
    HoplogOrganizer<SortedHoplogPersistedEvent> organizer = new HdfsSortedOplogOrganizer(rm, 0);

    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent(("1"), ("1-1")));
    items.add(new TestEvent(("4"), ("1-4")));
    organizer.flush(items.iterator(), items.size());

    items.clear();
    items.add(new TestEvent(("1"), ("2-1")));
    items.add(new TestEvent(("3"), ("2-3")));
    organizer.flush(items.iterator(), items.size());

    items.clear();
    items.add(new TestEvent(("3"), ("3-3")));
    items.add(new TestEvent(("5"), ("3-5")));
    organizer.flush(items.iterator(), items.size());

    // check file existence in bucket directory
    FileStatus[] hoplogs = getBucketHoplogs(rm.getStore().getFileSystem(),
        rm.getRegionFolder() + "/" + 0,
        HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);

    // expect 3 files are 3 flushes
    assertEquals(3, hoplogs.length);
    byte[] keyBytes1 = BlobHelper.serializeToBlob("1");
    byte[] keyBytes3 = BlobHelper.serializeToBlob("3");
    byte[] keyBytes4 = BlobHelper.serializeToBlob("4");
    // expect key 1 from hoplog 2
    assertEquals("2-1", organizer.read(keyBytes1).getValue());
    // expect key 3 from hoplog 3
    assertEquals("3-3", organizer.read(keyBytes3).getValue());
    // expect key 4 from hoplog 1
    assertEquals("1-4", organizer.read(keyBytes4).getValue());
    return organizer;
  }

  /**
   * Tests bucket organizer initialization during startup. Existing hoplogs should identified and
   * returned
   */
  public void testHoplogIdentification() throws Exception {
    // create one empty file and one directories in bucket directory
    Path bucketPath = new Path(testDataDir, getName() + "/0");
    FileSystem fs = hdfsStore.getFileSystem();
    fs.createNewFile(new Path(bucketPath, "temp_file"));
    fs.mkdirs(new Path(bucketPath, "temp_dir"));

    // create 2 hoplogs files each of type flush, minor and major hoplog
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    String[] extensions = { HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION,
        HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION,
        HdfsSortedOplogOrganizer.MINOR_HOPLOG_EXTENSION,
        HdfsSortedOplogOrganizer.MINOR_HOPLOG_EXTENSION,
        HdfsSortedOplogOrganizer.MAJOR_HOPLOG_EXTENSION,
        HdfsSortedOplogOrganizer.MAJOR_HOPLOG_EXTENSION};
    for (String string : extensions) {
      Hoplog oplog = organizer.getTmpSortedOplog(null, string);
      createHoplog(0, oplog);
      organizer.makeLegitimate(oplog);
    }

    // create a temp hoplog
    Hoplog oplog = organizer.getTmpSortedOplog(null, HdfsSortedOplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    createHoplog(0, oplog);

    // bucket directory should have 6 hoplogs, 1 temp log, 1 misc file and 1 directory
    FileStatus[] results = fs.listStatus(bucketPath);
    assertEquals(9, results.length);

    // only two are hoplogs
    List<Hoplog> list = organizer.identifyAndLoadSortedOplogs(true);
    assertEquals(6, list.size());
  }

  public void testExpiryMarkerIdentification() throws Exception {
    // epxired hoplogs from the list below should be deleted
    String[] files = {
        "0-1-1231" + AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION,
        "0-2-1232" + AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION,
        "0-3-1233" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION,
        "0-4-1234" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION,
        "0-5-1235" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION };
    
    Path bucketPath = new Path(testDataDir, getName() + "/0");
    FileSystem fs = hdfsStore.getFileSystem();
    for (String file : files) {
      Hoplog oplog = new HFileSortedOplog(hdfsStore, new Path(bucketPath, file),
          blockCache, stats, storeStats);
      createHoplog(10, oplog);
    }

    String marker1 = "0-4-1234"
        + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION
        + AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION;
    fs.createNewFile(new Path(bucketPath, marker1));
    String marker2 = "0-5-1235"
        + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION
        + AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION;
    fs.createNewFile(new Path(bucketPath, marker2));    
    
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/0", "");
    assertEquals(7, hoplogs.length);
    
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(
        regionManager, 0);
    
    FileStatus[] markers = organizer.getExpiryMarkers();
    // one hoplog and one exp marker will be deletion targets
    assertEquals(2, markers.length);
    for (FileStatus marker : markers) {
      String name = marker.getPath().getName();
      assertTrue(name.equals(marker1) || name.equals(marker2));
    }
    organizer.close();
  }
  
  public void testExpiredHoplogCleanup() throws Exception {
    // epxired hoplogs from the list below should be deleted
    String[] files = {
        "0-1-0000" + AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION,
        "0-1-1111" + AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION,
        "0-1-1111" + AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION
        + AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION,
        
        "0-2-0000" + AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION,
        "0-2-2222" + AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION,
        
        "0-3-0000" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION,
        "0-3-3333" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION,
        "0-3-3333" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION
            + AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION,
        
        "0-4-4444" + AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION };
    
    Path bucketPath = new Path(testDataDir, getName() + "/0");
    FileSystem fs = hdfsStore.getFileSystem();
    for (String file : files) {
      if (file.endsWith(AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION)) {
        fs.createNewFile(new Path(bucketPath, file));
        continue;
      }
      Hoplog oplog = new HFileSortedOplog(hdfsStore, new Path(bucketPath, file),
          blockCache, stats, storeStats);
      createHoplog(10, oplog);
    }
    
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/0", "");
    assertEquals(9, hoplogs.length);

    long target = System.currentTimeMillis();
    TimeUnit.SECONDS.sleep(1);
    
    // all but minor compacted files from below this will not be deleted as it
    // is after target delete time
    files = new String[] { 
        "0-4-4444" + AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION
            + AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION,
            
        "0-5-5555" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION
            + AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION,
        "0-5-5555" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION,
        
        "0-6-6666" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION
    };
    for (String file : files) {
      if (file.endsWith(AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION)) {
        fs.createNewFile(new Path(bucketPath, file));
        continue;
      }
      Hoplog oplog = new HFileSortedOplog(hdfsStore, new Path(bucketPath, file),
          blockCache, stats, storeStats);
      createHoplog(10, oplog);
    }
    
    hoplogs = getBucketHoplogs(getName() + "/0", "");
    assertEquals(13, hoplogs.length);
    int hopSize = 0;
    for (FileStatus file : hoplogs) {
      if(file.getLen() > hopSize) {
        hopSize = (int) file.getLen();
      }
    }

    final AtomicInteger behavior = new AtomicInteger(0);
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0) {
      @Override
      protected FileStatus[] getExpiryMarkers() throws IOException {
        if (behavior.get() == 1) {
          ArrayList<FileStatus> markers = new ArrayList<FileStatus>();
          for (FileStatus marker : super.getExpiryMarkers()) {
            markers.add(marker);
          }
          // inject a dummy old expiry marker for major compacted file
          long age = 2 * HDFSStore.DEFAULT_MAJOR_COMPACTION_INTERVAL_MINS * 60 * 1000;
          String markerName = "0-2-2222" + AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION + EXPIRED_HOPLOG_EXTENSION;
          FileStatus marker = new FileStatus(0, false, 1, 1024, System.currentTimeMillis() - age, new Path(bucketPath, markerName));
          markers.add(marker);
          return markers.toArray(new FileStatus[markers.size()]);
        }
        return super.getExpiryMarkers();
      }
    };

    List<FileStatus> list = organizer.getOptimizationTargets(target);
    assertEquals(6, list.size());

    behavior.set(1);
    list = organizer.getOptimizationTargets(target);
    assertEquals(8, list.size());
    
    assertEquals(9 * hopSize, stats.getStoreUsageBytes());
    int count = organizer.deleteExpiredFiles(list);
    assertEquals(8, count);
    assertEquals(5 * hopSize, stats.getStoreUsageBytes());
    
    List<FileStatus> tmp = new ArrayList<FileStatus>(Arrays.asList(hoplogs));
    for (Iterator<FileStatus> iter = tmp.iterator(); iter.hasNext();) {
      hoplogs = getBucketHoplogs(getName() + "/0", "");
      FileStatus file = iter.next();
      for (FileStatus hoplog : hoplogs) {
        if(hoplog.getPath().getName().startsWith("0-5-5555")) {
          fail("this file should have been deleted" + hoplog.getPath().getName());
        }

        if (hoplog.getPath().getName().equals(file.getPath().getName())) {
          iter.remove();
          break;
        }
      }
    }

    assertEquals(7, tmp.size());
    organizer.close();
  }
  
  public void testAlterPurgeInterval() throws Exception {
    // epxired hoplogs from the list below should be deleted
    String[] files = {
        "0-1-0000" + AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION,
        "0-1-1111" + AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION,
        "0-2-2222" + AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION,
        "0-4-4444" + AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION };
    
    Path bucketPath = new Path(testDataDir, getName() + "/0");
    hdfsStore.getFileSystem();
    for (String file : files) {
      Hoplog oplog = new HFileSortedOplog(hdfsStore, new Path(bucketPath, file),
          blockCache, stats, storeStats);
      createHoplog(10, oplog);
    }
    
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/0", "");
    int hopSize = 0;
    for (FileStatus file : hoplogs) {
      if(file.getLen() > hopSize) {
        hopSize = (int) file.getLen();
      }
    }

    final AtomicInteger behavior = new AtomicInteger(0);
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0) {
      @Override
      protected FileStatus[] getExpiryMarkers() throws IOException {
        if (behavior.get() == 1) {
          ArrayList<FileStatus> markers = new ArrayList<FileStatus>();
          // inject dummy old expiry markers
          long age = 120 * 1000; // 120 seconds old
          String markerName = "0-2-2222" + AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION + EXPIRED_HOPLOG_EXTENSION;
          FileStatus marker = new FileStatus(0, false, 1, 1024, System.currentTimeMillis() - age, new Path(bucketPath, markerName));
          markers.add(marker);
          markerName = "0-4-4444" + AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION + EXPIRED_HOPLOG_EXTENSION;
          marker = new FileStatus(0, false, 1, 1024, System.currentTimeMillis() - age, new Path(bucketPath, markerName));
          markers.add(marker);
          return markers.toArray(new FileStatus[markers.size()]);
        }
        return super.getExpiryMarkers();
      }
    };

    behavior.set(1);
    int count = organizer.initiateCleanup();
    assertEquals(0, count);
    
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    mutator.setPurgeInterval(1);
    hdfsStore.alter(mutator);
    count = organizer.initiateCleanup();
    assertEquals(4, count);
  }
  
  public void testInUseExpiredHoplogCleanup() throws Exception {
    Path bucketPath = new Path(testDataDir, getName() + "/0");
    FileSystem fs = hdfsStore.getFileSystem();
    
    String[] files = new String[] {
        "0-1-1231" + AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION,
        "0-2-1232" + AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION,
        "0-3-1233" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION,
        "0-4-1234" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION,
        "0-5-1235" + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION };
    
    for (String file : files) {
      Hoplog oplog = new HFileSortedOplog(hdfsStore, new Path(bucketPath, file),
          blockCache, stats, storeStats);
      createHoplog(10, oplog);
    }
    
    final HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(
        regionManager, 0);
    List<TrackedReference<Hoplog>> hopRefs = organizer.getSortedOplogs();
    assertEquals(files.length, hopRefs.size());
    
    // this is expiry marker for one of the files that will be compacted below.
    // While compaction is going on file deletion should not happen
    files = new String[] { "0-5-1235"
        + AbstractHoplogOrganizer.MINOR_HOPLOG_EXTENSION
        + AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION };
    
    for (String file : files) {
      fs.createNewFile(new Path(bucketPath, file));
    }
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/0", "");
    assertEquals(hopRefs.size() + files.length, hoplogs.length);
    
    TimeUnit.MILLISECONDS.sleep(200);
    long target = System.currentTimeMillis();
    List<FileStatus> list = organizer.getOptimizationTargets(target);
    assertEquals(2, list.size());
    
    for (TrackedReference<Hoplog> ref : hopRefs) {
      ref.increment("test");
    }

    fs.delete(new Path(bucketPath, files[0]), false);
    
    TimeUnit.MILLISECONDS.sleep(50);
    organizer.markSortedOplogForDeletion(hopRefs, false);
    
    list = organizer.getOptimizationTargets(target);
    assertEquals(0, list.size());
    organizer.close();
  }
  
  /**
   * Tests max sequence initialization when file already exists and server starts
   */
  public void testSeqInitialization() throws Exception {
    // create many hoplogs files
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    String[] extensions = { HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION,
        HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION,
        HdfsSortedOplogOrganizer.MINOR_HOPLOG_EXTENSION,
        HdfsSortedOplogOrganizer.MAJOR_HOPLOG_EXTENSION,
        HdfsSortedOplogOrganizer.MAJOR_HOPLOG_EXTENSION};
    for (String string : extensions) {
      Hoplog oplog = organizer.getTmpSortedOplog(null, string);
      createHoplog(1, oplog);
      organizer.makeLegitimate(oplog);
    }

    // a organizer should start creating files starting at 6 as five files already existed
    organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    Hoplog oplog = organizer.getTmpSortedOplog(null, HdfsSortedOplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    createHoplog(1, oplog);
    organizer.makeLegitimate(oplog);
    assertEquals(6, HdfsSortedOplogOrganizer.getSequenceNumber(oplog));
    organizer.close();
  }

  /**
   * Tests temp file creation and making file legitimate
   */
  public void testMakeLegitimate() throws Exception {
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);

    // create empty tmp hoplog
    Hoplog oplog = organizer.getTmpSortedOplog(null, HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    createHoplog(0, oplog);

    Path hoplogPath = new Path(testDataDir, getName() + "/0/" + oplog.getFileName());
    FileSystem fs = hdfsStore.getFileSystem();
    FileStatus hoplogStatus = fs.getFileStatus(hoplogPath);
    assertNotNull(hoplogStatus);

    organizer.makeLegitimate(oplog);

    try {
      hoplogStatus = fs.getFileStatus(hoplogPath);
      assertNull(hoplogStatus);
    } catch (FileNotFoundException e) {
      // tmp file is renamed hence should not exist, exception expected
    }

    assertTrue(oplog.getFileName().endsWith(HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION));
    hoplogPath = new Path(testDataDir, getName() + "/0/" + oplog.getFileName());
    hoplogStatus = fs.getFileStatus(hoplogPath);
    assertNotNull(hoplogStatus);
  }

  /**
   * Tests hoplog file name comparator
   */
  public void testHoplogFileComparator() throws IOException {
    String name1 = "bucket1-10-3.hop";
    String name2 = "bucket1-1-20.hop";
    String name3 = "bucket1-30-201.hop";
    String name4 = "bucket1-100-201.hop";

    TreeSet<TrackedReference<Hoplog>> list = new TreeSet<TrackedReference<Hoplog>>(new HoplogComparator());
    // insert soplog is the list out of expected order
    hdfsStore.getFileSystem();
    list.add(new TrackedReference<Hoplog>(new HFileSortedOplog(hdfsStore, new Path(testDataDir, name2), blockCache, stats, storeStats)));
    list.add(new TrackedReference<Hoplog>(new HFileSortedOplog(hdfsStore, new Path(testDataDir, name4), blockCache, stats, storeStats)));
    list.add(new TrackedReference<Hoplog>(new HFileSortedOplog(hdfsStore, new Path(testDataDir, name1), blockCache, stats, storeStats)));
    list.add(new TrackedReference<Hoplog>(new HFileSortedOplog(hdfsStore, new Path(testDataDir, name3), blockCache, stats, storeStats)));

    Iterator<TrackedReference<Hoplog>> iter = list.iterator();
    assertEquals(name4, iter.next().get().getFileName());
    assertEquals(name3, iter.next().get().getFileName());
    assertEquals(name2, iter.next().get().getFileName());
    assertEquals(name1, iter.next().get().getFileName());
  }
  
  /**
   * Tests clear on a set of hoplogs.
   */
  public void testClear() throws Exception {
    int bucketId = (int) System.nanoTime();
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, bucketId);

    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent(("1"), ("1-1")));
    items.add(new TestEvent(("4"), ("1-4")));
    organizer.flush(items.iterator(), items.size());

    items.clear();
    items.add(new TestEvent(("1"), ("2-1")));
    items.add(new TestEvent(("3"), ("2-3")));
    organizer.flush(items.iterator(), items.size());

    items.clear();
    items.add(new TestEvent(("3"), ("3-3")));
    items.add(new TestEvent(("5"), ("3-5")));
    organizer.flush(items.iterator(), items.size());

    // check file existence in bucket directory
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/" + bucketId, HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);

    // expect 3 files are 3 flushes
    assertEquals(3, hoplogs.length);
    
    organizer.clear();
    
    // check that all files are now expired
    hoplogs = getBucketHoplogs(getName() + "/" + bucketId, HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    FileStatus[] exs = getBucketHoplogs(getName() + "/" + bucketId, HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    FileStatus[] valids = HdfsSortedOplogOrganizer.filterValidHoplogs(hoplogs, exs);
    assertEquals(Collections.EMPTY_LIST, Arrays.asList(valids));
    
    assertEquals(0, stats.getActiveFileCount());
    assertEquals(0, stats.getInactiveFileCount());
  }
  
  public void testFixedIntervalMajorCompaction() throws Exception {
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
    
    regionManager.addOrganizer(0, organizer);
    
    System.setProperty(HoplogConfig.JANITOR_INTERVAL_SECS, "1");
    HDFSRegionDirector.resetJanitor();
    
    alterMajorCompaction(hdfsStore, true);
    
    // create hoplog in the past, 90 seconds before current time
    organizer.hoplogCreated(getName(), 0, new TestHoplog(hdfsStore, 100, System.currentTimeMillis() - 90000));
    TimeUnit.MILLISECONDS.sleep(50);
    organizer.hoplogCreated(getName(), 0, new TestHoplog(hdfsStore, 100, System.currentTimeMillis() - 90000));
    
    List<TrackedReference<Hoplog>> hoplogs = organizer.getSortedOplogs();
    assertEquals(2, hoplogs.size());
    
    for (int i = 0; i < 3; i++) {
      TimeUnit.SECONDS.sleep(1);
      assertEquals(0, majorCReqCount.get());
    }
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    mutator.setMajorCompactionInterval(1);
    hdfsStore.alter(mutator);
    TimeUnit.SECONDS.sleep(5);
    assertTrue(3 < majorCReqCount.get());
  }
  
 
  public void testCorruptHfileBucketFail() throws Exception {
    // create a corrupt file
    FileSystem fs = hdfsStore.getFileSystem();
    for (int i = 0; i < 113; i++) {
      FSDataOutputStream opStream = fs.create(new Path(testDataDir.getName() + "/region-1/" + i + "/1-1-1.hop"));
      opStream.writeBytes("Some random corrupt file");
      opStream.close();
    }
      
    // create region with store
    regionfactory.setHDFSStoreName(HDFS_STORE_NAME);
    Region<Object, Object> region1 = regionfactory.create("region-1");
    ExpectedException ex = DistributedTestCase.addExpectedException("CorruptHFileException");
    try {
      region1.get("key");
      fail("get should have failed with corrupt file error");
    } catch (HDFSIOException e) {
      // expected
    } finally {
      ex.remove();
    }
    
    region1.destroyRegion();
  }

  public void testMaxOpenReaders() throws Exception {
    System.setProperty("hoplog.bucket.max.open.files", "5");
    HoplogOrganizer<? extends PersistedEventImpl> organizer = new HdfsSortedOplogOrganizer(regionManager, 0);

    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 10; i++) {
      items.clear();
      items.add(new TestEvent("" + i, "" + i));
      organizer.flush(items.iterator(), items.size());
    }
    
    HdfsSortedOplogOrganizer bucket = (HdfsSortedOplogOrganizer) organizer;
    List<TrackedReference<Hoplog>> hoplogs = bucket.getSortedOplogs();
    int closedCount = 0 ;
    for (TrackedReference<Hoplog> hoplog : hoplogs) {
      HFileSortedOplog hfile = (HFileSortedOplog) hoplog.get();
      if (hfile.isClosed()) { 
        closedCount++;
      }
    }
    assertEquals(10, closedCount);
    assertEquals(10, stats.getActiveFileCount());
    assertEquals(0, stats.getActiveReaderCount());
    
    byte[] keyBytes1 = BlobHelper.serializeToBlob("1");
    organizer.read(keyBytes1).getValue();
    
    closedCount = 0 ;
    for (TrackedReference<Hoplog> hoplog : hoplogs) {
      HFileSortedOplog hfile = (HFileSortedOplog) hoplog.get();
      if (hfile.isClosed()) { 
        closedCount++;
      }
    }
    assertEquals(5, closedCount);
    assertEquals(10, stats.getActiveFileCount());
    assertEquals(0, stats.getInactiveFileCount());
    assertEquals(5, stats.getActiveReaderCount());
    
    organizer.getCompactor().compact(false, false);
    assertEquals(1, stats.getActiveFileCount());
    assertEquals(0, stats.getActiveReaderCount());
    assertEquals(0, stats.getInactiveFileCount());
  }

  public void testConcurrentReadInactiveClose() throws Exception {
    final HoplogOrganizer<? extends PersistedEventImpl> organizer = regionManager.create(0);
    alterMinorCompaction(hdfsStore, true);

    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < 4; i++) {
      items.clear();
      items.add(new TestEvent("" + i, "" + i));
      organizer.flush(items.iterator(), items.size());
    }
    
    final byte[] keyBytes1 = BlobHelper.serializeToBlob("1");
    class ReadTask implements Runnable {
      public void run() {
        try {
          organizer.read(keyBytes1);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    ScheduledExecutorService[] readers = new ScheduledExecutorService[10];
    for (int i = 0; i < readers.length; i++) {
      readers[i] = Executors.newSingleThreadScheduledExecutor();
      readers[i].scheduleWithFixedDelay(new ReadTask(), 0, 1, TimeUnit.MILLISECONDS);
    }
    
    for (int i = 0; i < 100; i++) {
      items.clear();
      items.add(new TestEvent("" + i, "" + i));
      organizer.flush(items.iterator(), items.size());
    }
    
    for (int i = 0; i < readers.length; i++) {
      readers[i].shutdown();
      readers[i].awaitTermination(1, TimeUnit.SECONDS);
      TimeUnit.MILLISECONDS.sleep(50);
    }
    
    for (int i = 0; i < 20; i++) {
      if (stats.getActiveFileCount() < 4) {
        break;
      }
      organizer.getCompactor().compact(false, false);
    }

    organizer.performMaintenance();
    TimeUnit.SECONDS.sleep(1);
    
    assertTrue("" + stats.getActiveFileCount(), stats.getActiveFileCount() <= 4);
    assertEquals(stats.getActiveReaderCount(), stats.getActiveReaderCount());
    assertEquals(0, stats.getInactiveFileCount());
  }
  
  public void testEmptyBucketCleanup() throws Exception {
    HdfsSortedOplogOrganizer o = new HdfsSortedOplogOrganizer(regionManager, 0);
    long target = System.currentTimeMillis();
    o.getOptimizationTargets(target);
    // making sure empty bucket is not causing IO errors. no assertion needed
    // for this test case.
  }
  
  public void testExpiredFilterAtStartup() throws Exception {
    HdfsSortedOplogOrganizer bucket = new HdfsSortedOplogOrganizer(regionManager, 0);
    
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent(("1"), ("1-1")));
    items.add(new TestEvent(("4"), ("1-4")));
    bucket.flush(items.iterator(), items.size());
    
    items.clear();
    items.add(new TestEvent(("1"), ("2-1")));
    items.add(new TestEvent(("3"), ("2-3")));
    bucket.flush(items.iterator(), items.size());
    
    FileStatus[] files = getBucketHoplogs(getName() + "/" + 0,
        HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(2, files.length);
    
    files = getBucketHoplogs(getName() + "/" + 0,
        HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(0, files.length);
    
    HdfsSortedOplogOrganizer bucket2 = new HdfsSortedOplogOrganizer(regionManager, 0);
    List<TrackedReference<Hoplog>> hoplogs = bucket2.getSortedOplogs();
    assertEquals(2, hoplogs.size());
    
    bucket.clear();
    
    files = getBucketHoplogs(getName() + "/" + 0,
        HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(2, files.length);
    
    files = getBucketHoplogs(getName() + "/" + 0,
        HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(2, files.length);
    
    bucket2 = new HdfsSortedOplogOrganizer(regionManager, 0);
    hoplogs = bucket2.getSortedOplogs();
    assertEquals(0, hoplogs.size());
    
    items.clear();
    items.add(new TestEvent(("1"), ("2-1")));
    items.add(new TestEvent(("3"), ("2-3")));
    bucket.flush(items.iterator(), items.size());
    
    bucket2 = new HdfsSortedOplogOrganizer(regionManager, 0);
    hoplogs = bucket2.getSortedOplogs();
    assertEquals(1, hoplogs.size());
    bucket.close();
    bucket2.close();
  }

  public void testExpireFilterRetartAfterClear() throws Exception {
    HdfsSortedOplogOrganizer bucket = new HdfsSortedOplogOrganizer(regionManager, 0);
    
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent(("1"), ("1-1")));
    items.add(new TestEvent(("4"), ("1-4")));
    bucket.flush(items.iterator(), items.size());

    items.clear();
    items.add(new TestEvent(("1"), ("2-1")));
    items.add(new TestEvent(("3"), ("2-3")));
    bucket.flush(items.iterator(), items.size());
    
    FileStatus[] files = getBucketHoplogs(getName() + "/" + 0,
        HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(2, files.length);
    
    files = getBucketHoplogs(getName() + "/" + 0,
        HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(0, files.length);
    
    HdfsSortedOplogOrganizer bucket2 = new HdfsSortedOplogOrganizer(regionManager, 0);
    List<TrackedReference<Hoplog>> hoplogs = bucket2.getSortedOplogs();
    assertEquals(2, hoplogs.size());
    
    bucket.clear();
    
    files = getBucketHoplogs(getName() + "/" + 0,
        HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(2, files.length);
    
    files = getBucketHoplogs(getName() + "/" + 0,
        HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(2, files.length);
    
    bucket2 = new HdfsSortedOplogOrganizer(regionManager, 0);
    hoplogs = bucket2.getSortedOplogs();
    assertEquals(0, hoplogs.size());
    bucket.close();
    bucket2.close();
  }
  
  /**
   * tests maintenance does not fail even if there are no hoplogs
   */
  public void testNoFileJanitor() throws Exception {
    HoplogOrganizer<? extends PersistedEventImpl> organizer;
    organizer = regionManager.create(0);
    organizer.performMaintenance();
  }
  
  public void testValidHoplogRegex() {
    String[] valid = {"1-1-1.hop", "1-1-1.ihop", "1-1-1.chop"};
    String[] invalid = {"1-1-1.khop", "1-1-1.hop.tmphop", "1-1-1.hop.ehop", "1-1-.hop", "-1-1.hop"};
    
    for (String string : valid) {
      Matcher matcher = HdfsSortedOplogOrganizer.SORTED_HOPLOG_PATTERN.matcher(string);
      assertTrue(matcher.matches());
    }
    
    for (String string : invalid) {
      Matcher matcher = HdfsSortedOplogOrganizer.SORTED_HOPLOG_PATTERN.matcher(string);
      assertFalse(matcher.matches());
    }
  }
  
  public void testOneHoplogMajorCompaction() throws Exception {
    HoplogOrganizer<? extends PersistedEventImpl> organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    alterMajorCompaction(hdfsStore, true);
    
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent(("1"), ("1-1")));
    organizer.flush(items.iterator(),items.size());    
    
    
    FileStatus[] files = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(1, files.length);    
    
    //Minor compaction will not perform on 1 .hop file
    organizer.getCompactor().compact(false, false);
    files = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.MINOR_HOPLOG_EXTENSION);
    assertEquals(0, files.length);
    
    //Major compaction will perform on 1 .hop file
    organizer.getCompactor().compact(true, false);
    files = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.MAJOR_HOPLOG_EXTENSION);     
    assertEquals(1, files.length);
    String hoplogName =files[0].getPath().getName();    
    files = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.MINOR_HOPLOG_EXTENSION);
    assertEquals(0, files.length);
    
    organizer.getCompactor().compact(true, false);
    files = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    assertEquals(1, files.length);
    assertEquals(hoplogName, files[0].getPath().getName());
    
    //Minor compaction does not convert major compacted file
    organizer.getCompactor().compact(false, false);
    files = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.MINOR_HOPLOG_EXTENSION);
    assertEquals(0, files.length);
    
    files = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    assertEquals(1, files.length);
    assertEquals(hoplogName, files[0].getPath().getName());
    
    files = getBucketHoplogs(getName() + "/0", HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(1, files.length);
    assertNotSame(hoplogName + HdfsSortedOplogOrganizer.EXPIRED_HOPLOG_EXTENSION, files[0].getPath().getName() );
  }

  public void testExposeCleanupInterval() throws Exception {
    FileSystem fs = hdfsStore.getFileSystem();
    Path cleanUpIntervalPath = new Path(hdfsStore.getHomeDir(), HoplogConfig.CLEAN_UP_INTERVAL_FILE_NAME);
    assertTrue(fs.exists(cleanUpIntervalPath));
    long interval = HDFSStore.DEFAULT_OLD_FILE_CLEANUP_INTERVAL_MINS
        *60 * 1000;
    assertEquals(interval, HoplogUtil.readCleanUpIntervalMillis(fs,cleanUpIntervalPath));
  }
  
  @Override
  protected void setUp() throws Exception {
    System.setProperty(HoplogConfig.JANITOR_INTERVAL_SECS, "" + HoplogConfig.JANITOR_INTERVAL_SECS_DEFAULT);
    super.setUp();
  }
}

