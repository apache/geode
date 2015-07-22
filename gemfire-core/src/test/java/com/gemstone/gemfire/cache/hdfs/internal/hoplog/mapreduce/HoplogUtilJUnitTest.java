/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.BaseHoplogTestCase;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HdfsSortedOplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogConfig;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogOrganizer;
import com.gemstone.gemfire.internal.cache.persistence.soplog.TrackedReference;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category({IntegrationTest.class, HoplogTest.class})
public class HoplogUtilJUnitTest extends BaseHoplogTestCase {
  Path regionPath = null;
  
  @Override
  protected void configureHdfsStoreFactory() throws Exception {
    super.configureHdfsStoreFactory();
    
    hsf.setInputFileCountMin(3);
    hsf.setMinorCompaction(false);
    hsf.setMajorCompaction(false);
  }
  
  public void testHoplogListingMultiBucket() throws Exception {
    createHoplogs();

    Collection<FileStatus> hoplogs = HoplogUtil.getAllRegionHoplogs(
        regionPath, hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION);

    assertEquals(5, hdfsStore.getFileSystem().listStatus(regionPath).length);
    assertEquals(15, hoplogs.size());
  }

  public void testHoplogListingMixFileTypes() throws Exception {
    createHoplogs();

    HoplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager, 0);
    organizer.getCompactor().compact(false, false);

    Collection<FileStatus> hoplogs = HoplogUtil.getAllRegionHoplogs(
        regionPath, hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION);

    assertEquals(7,
        hdfsStore.getFileSystem().listStatus(new Path(regionPath, "0")).length);
    assertEquals(15, hoplogs.size());
  }

  public void testHoplogListingEmptyBucket() throws Exception {
    createHoplogs();

    hdfsStore.getFileSystem().mkdirs(new Path(regionPath, "100"));

    Collection<FileStatus> hoplogs = HoplogUtil.getAllRegionHoplogs(
        regionPath, hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION);

    assertEquals(6, hdfsStore.getFileSystem().listStatus(regionPath).length);
    assertEquals(15, hoplogs.size());
  }

  public void testHoplogListingInvalidBucket() throws Exception {
    createHoplogs();

    hdfsStore.getFileSystem().rename(new Path(regionPath, "0"),
        new Path(regionPath, "not_a_bucket"));

    Collection<FileStatus> hoplogs = HoplogUtil.getAllRegionHoplogs(
        regionPath, hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION);

    assertEquals(5, hdfsStore.getFileSystem().listStatus(regionPath).length);
    assertEquals(12, hoplogs.size());
  }

  public void testHoplogListingInvalidFiles() throws Exception {
    createHoplogs();

    Path bucketPath = new Path(regionPath, "0");
    FSDataOutputStream stream = hdfsStore.getFileSystem().create(
        new Path(bucketPath, "not_a_hoplog"));
    stream.close();

    Collection<FileStatus> hoplogs = HoplogUtil.getAllRegionHoplogs(
        regionPath, hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION);

    assertEquals(4, hdfsStore.getFileSystem().listStatus(bucketPath).length);
    assertEquals(15, hoplogs.size());
  }

  public void testTimeRange() throws Exception {
    createHoplogs();
    // rename hoplogs for testing purpose
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(
        regionManager, 0);
    List<TrackedReference<Hoplog>> hoplogs = organizer.getSortedOplogs();
    assertEquals(3, hoplogs.size());
    hoplogs.get(0).get().rename("0-300-1.hop");
    hoplogs.get(1).get().rename("0-310-1.hop");
    hoplogs.get(2).get().rename("0-320-1.hop");
    organizer.close();

    organizer = new HdfsSortedOplogOrganizer(regionManager, 3);
    hoplogs = organizer.getSortedOplogs();
    assertEquals(3, hoplogs.size());
    hoplogs.get(0).get().rename("0-600-1.hop");
    hoplogs.get(1).get().rename("0-610-1.hop");
    hoplogs.get(2).get().rename("0-620-1.hop");
    organizer.close();

    organizer = new HdfsSortedOplogOrganizer(regionManager, 6);
    hoplogs = organizer.getSortedOplogs();
    assertEquals(3, hoplogs.size());
    hoplogs.get(0).get().rename("0-100-1.hop");
    hoplogs.get(1).get().rename("0-110-1.hop");
    hoplogs.get(2).get().rename("0-120-1.hop");

    Collection<FileStatus> filtered = HoplogUtil.getRegionHoplogs(
        regionPath, hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION, 300, 305);
    assertEquals(5, filtered.size());
    assertTrue(containsHoplogWithName(filtered, "0-300-1.hop"));
    assertTrue(containsHoplogWithName(filtered, "0-310-1.hop"));
    assertTrue(containsHoplogWithName(filtered, "0-600-1.hop"));

    filtered = HoplogUtil.getRegionHoplogs(regionPath,
        hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION, 250, 310);
    assertEquals(6, filtered.size());
    assertTrue(containsHoplogWithName(filtered, "0-300-1.hop"));
    assertTrue(containsHoplogWithName(filtered, "0-310-1.hop"));
    assertTrue(containsHoplogWithName(filtered, "0-320-1.hop"));

    filtered = HoplogUtil.getRegionHoplogs(regionPath,
        hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION, 301, 311);
    assertEquals(5, filtered.size());
    assertTrue(containsHoplogWithName(filtered, "0-310-1.hop"));
    assertTrue(containsHoplogWithName(filtered, "0-320-1.hop"));

    filtered = HoplogUtil.getRegionHoplogs(regionPath,
        hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION, 301, 309);
    assertEquals(4, filtered.size());
    assertTrue(containsHoplogWithName(filtered, "0-310-1.hop"));
    organizer.close();
  }
  
  public void testExcludeSoonCleanedHoplogs() throws Exception {
    FileSystem fs = hdfsStore.getFileSystem();
    Path cleanUpIntervalPath = new Path(hdfsStore.getHomeDir(), HoplogConfig.CLEAN_UP_INTERVAL_FILE_NAME);
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(
        regionManager, 0);
    //delete the auto generated clean up interval file   
    if (fs.exists(cleanUpIntervalPath)){
      fs.delete(cleanUpIntervalPath, true);
    }
    
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    int count = 10;
    for (int fileCount = 0; fileCount < 3; fileCount++) {
      items.clear();
      for (int itemCount = 0; itemCount < count; itemCount++) {
        items.add(new TestEvent(("key-" + itemCount), "value"));
      }
      organizer.flush(items.iterator(), count);
    }
    List<TrackedReference<Hoplog>> hoplogs = organizer.getSortedOplogs();
    
    for(TrackedReference<Hoplog> hoplog : hoplogs) {
      Path p = new Path(testDataDir, getName() + "/0/" +
          hoplog.get().getFileName() + AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
      fs.createNewFile(p);
    }
    Collection<FileStatus> files = HoplogUtil.getAllRegionHoplogs(
        regionPath, hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(3, files.size());
    
    TimeUnit.MINUTES.sleep(2);
    //No clean up interval file, all expired files will be included
    files = HoplogUtil.getAllRegionHoplogs(
        regionPath, hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(3, files.size());
    
    
    long interval = 1 * 60 * 1000;
    HoplogUtil.exposeCleanupIntervalMillis(fs,cleanUpIntervalPath,interval);
    
    files = HoplogUtil.getAllRegionHoplogs(
        regionPath, hdfsStore.getFileSystem(),
        AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(0, files.size());
    organizer.close();  
  }
  
  
  public void testCheckpointSelection() throws Exception {
    createHoplogs();
    // rename hoplogs for testing purpose
    HdfsSortedOplogOrganizer organizer = new HdfsSortedOplogOrganizer(
        regionManager, 0);
    List<TrackedReference<Hoplog>> hoplogs = organizer.getSortedOplogs();
    assertEquals(3, hoplogs.size());
    hoplogs.get(0).get().rename("0-300-1.chop");
    hoplogs.get(1).get().rename("0-310-1.hop");
    hoplogs.get(2).get().rename("0-320-1.hop"); // checkpoint file
    organizer.close();
    
    organizer = new HdfsSortedOplogOrganizer(regionManager, 3);
    hoplogs = organizer.getSortedOplogs();
    assertEquals(3, hoplogs.size());
    hoplogs.get(0).get().rename("0-600-1.hop");
    hoplogs.get(1).get().rename("0-610-1.chop");
    hoplogs.get(2).get().rename("0-620-1.hop");
    organizer.close();
    
    organizer = new HdfsSortedOplogOrganizer(regionManager, 6);
    hoplogs = organizer.getSortedOplogs();
    assertEquals(3, hoplogs.size());
    hoplogs.get(0).get().rename("0-100-1.hop");
    hoplogs.get(1).get().rename("0-110-1.hop");
    hoplogs.get(2).get().rename("0-120-1.chop");
    
    Collection<FileStatus> filtered = HoplogUtil.filterHoplogs(
        hdfsStore.getFileSystem(), regionPath, 290, 305, false);
    assertEquals(4, filtered.size());
    assertTrue(containsHoplogWithName(filtered, "0-310-1.hop"));
    assertTrue(containsHoplogWithName(filtered, "0-600-1.hop"));
    
    filtered = HoplogUtil.filterHoplogs(hdfsStore.getFileSystem(),
        regionPath, 290, 305, true);
    assertEquals(3, filtered.size());
    assertTrue(containsHoplogWithName(filtered, "0-300-1.chop"));
    assertTrue(containsHoplogWithName(filtered, "0-610-1.chop"));
    assertTrue(containsHoplogWithName(filtered, "0-120-1.chop"));
    organizer.close();
  }
  
  private boolean containsHoplogWithName(Collection<FileStatus> filtered,
      String name) {
    for (FileStatus file : filtered) {
      if (file.getPath().getName().equals(name)) {
        return true;
      }
    }
    return false;
  }

  private void createHoplogs() throws IOException, Exception {
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    int count = 10;
    for (int bucketId = 0; bucketId < 15; bucketId += 3) {
      HoplogOrganizer organizer = new HdfsSortedOplogOrganizer(regionManager,
          bucketId);
      for (int fileCount = 0; fileCount < 3; fileCount++) {
        items.clear();
        for (int itemCount = 0; itemCount < count; itemCount++) {
          items.add(new TestEvent(("key-" + itemCount), "value"));
        }
        organizer.flush(items.iterator(), count);
      }
    }
  }
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    regionPath = new Path(testDataDir, getName());
  }
  
  @Override 
  protected void tearDown() throws Exception{
    FileSystem fs = hdfsStore.getFileSystem();
    Path cleanUpIntervalPath = new Path(hdfsStore.getHomeDir(),HoplogConfig.CLEAN_UP_INTERVAL_FILE_NAME);
    if (fs.exists(cleanUpIntervalPath)){
      fs.delete(cleanUpIntervalPath, true);
    }  
    super.tearDown();
  }
}
