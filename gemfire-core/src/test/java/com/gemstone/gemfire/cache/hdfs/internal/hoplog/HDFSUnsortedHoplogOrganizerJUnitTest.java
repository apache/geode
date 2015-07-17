/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.UnsortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogSetReader.HoplogIterator;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.SequenceFileHoplog.SequenceFileIterator;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test class to test hoplog functionality for streaming ingest 
 * 
 * @author hemantb
 *
 */
@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSUnsortedHoplogOrganizerJUnitTest extends BaseHoplogTestCase {
 
  /**
   * Tests flush operation
   */
  public void testFlush() throws Exception {
    int count = 10;
    int bucketId = (int) System.nanoTime();
    HDFSUnsortedHoplogOrganizer organizer = new HDFSUnsortedHoplogOrganizer(regionManager, bucketId);

    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < count; i++) {
      items.add(new TestEvent(("key-" + i), ("value-" + System.nanoTime())));
    }
    
    organizer.flush(items.iterator(), count);
    organizer.closeCurrentWriter();
    
    // check file existence in bucket directory
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/" + bucketId, 
                      HdfsSortedOplogOrganizer.SEQ_HOPLOG_EXTENSION);

    // only one hoplog should exists
    assertEquals(1, hoplogs.length);
    readSequenceFile(hdfsStore.getFileSystem(), hoplogs[0].getPath(), 0);
  }
  
  public void testAlterRollOverInterval() throws Exception {
    HDFSUnsortedHoplogOrganizer organizer = new HDFSUnsortedHoplogOrganizer(regionManager, 0);
    
    // flush 4 times with small delays. Only one seq file will be created
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int j = 0; j < 3; j++) {
      items.clear();
      for (int i = 0; i < 10; i++) {
        items.add(new TestEvent(("key-" + (i + 10 * j)), ("value-" + System.nanoTime())));
      }
      organizer.flush(items.iterator(), 10);
      TimeUnit.MILLISECONDS.sleep(1100);
    }
    organizer.closeCurrentWriter();
    
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/" + 0,
        HdfsSortedOplogOrganizer.SEQ_HOPLOG_EXTENSION);
    
    // only one hoplog should exists
    assertEquals(1, hoplogs.length);
    readSequenceFile(hdfsStore.getFileSystem(), hoplogs[0].getPath(), 0);
    
    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    mutator.setWriteOnlyFileRolloverInterval(1);
    hdfsStore.alter(mutator);
    
    TimeUnit.MILLISECONDS.sleep(1100);
    for (int j = 0; j < 2; j++) {
      items.clear();
      for (int i = 0; i < 10; i++) {
        items.add(new TestEvent(("key-" + (i + 10 * j)), ("value-" + System.nanoTime())));
      }
      organizer.flush(items.iterator(), 10);
      TimeUnit.MILLISECONDS.sleep(1100);
    }
    organizer.closeCurrentWriter();
    hoplogs = getBucketHoplogs(getName() + "/" + 0,
        HdfsSortedOplogOrganizer.SEQ_HOPLOG_EXTENSION);
    assertEquals(3, hoplogs.length);
  }
  
  public void testSequenceFileScan() throws Exception {
    int count = 10000;
    int bucketId = (int) System.nanoTime();
    HDFSUnsortedHoplogOrganizer organizer = new HDFSUnsortedHoplogOrganizer(regionManager, bucketId);

    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < count; i++) {
      items.add(new TestEvent(("key-" + i), ("value-" + System.nanoTime())));
    }
    
    organizer.flush(items.iterator(), count);
    organizer.closeCurrentWriter();
    
    // check file existence in bucket directory
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/" + bucketId, 
                      HdfsSortedOplogOrganizer.SEQ_HOPLOG_EXTENSION);

    // only one hoplog should exists
    assertEquals(1, hoplogs.length);
    
    SequenceFileDetails sfd = getSequenceFileDetails(hdfsStore.getFileSystem(), hoplogs[0].getPath());
    
    // End position is before a sync. Should read until sync.
    readSequenceFile(hdfsStore.getFileSystem(), hoplogs[0].getPath(), 0, sfd.indexOfKeyBeforeSecondSync ,
        0, sfd.posBeforeSecondSync);
    
    // Start position is inside header. Should start from first key and go to next sync point. 
    readSequenceFile(hdfsStore.getFileSystem(), hoplogs[0].getPath(), 0, sfd.indexOfKeyBeforeSecondSync, 
        10, sfd.posAfterFirstSync);
    
    // Start and end position are between two sync markers. Should not read any keys.    
    readSequenceFile(hdfsStore.getFileSystem(), hoplogs[0].getPath(), 29, 28, 
        sfd.posAfterFirstSync, sfd.posBeforeSecondSync - sfd.posAfterFirstSync);
    
    // Start position is after a sync and End position is beyond the file size. 
    //Should read all the records after the next sync.
    readSequenceFile(hdfsStore.getFileSystem(), hoplogs[0].getPath(), sfd.indexOfKeyAfterFirstSync, 9999, 
        sfd.posBeforeFirstSync, 10000000);
    
    // Should read all the records. 
    readSequenceFile(hdfsStore.getFileSystem(), hoplogs[0].getPath(), 0, 9999, 0, -1);
  }
  
  class SequenceFileDetails {
    public int posBeforeFirstSync;
    public int indexOfKeyBeforeFirstSync;
    
    public int posAfterFirstSync;
    public int indexOfKeyAfterFirstSync; 
    
    public int posBeforeSecondSync;
    public int indexOfKeyBeforeSecondSync;
  }
  
  public SequenceFileDetails getSequenceFileDetails(FileSystem inputFS, Path sequenceFileName) throws Exception {
    SequenceFileDetails fd = new SequenceFileDetails();
    SequenceFileHoplog hoplog = new SequenceFileHoplog(inputFS, sequenceFileName, null);
      
    SequenceFileIterator iter = (SequenceFileIterator)hoplog.getReader().scan();;
    int currentkeyStartPos = 0;
    int cursorPos = 0;
    String currentKey = null;
    boolean firstSyncSeen = false; 
    try {
      while (iter.hasNext()) {
        iter.next();
        currentkeyStartPos = cursorPos;
        currentKey = ((String)CacheServerHelper.deserialize(iter.getKey()));
        cursorPos = (int)iter.getPosition();
        if (iter.syncSeen()){
          if (firstSyncSeen) {
            
            fd.posBeforeSecondSync = currentkeyStartPos;
            fd.indexOfKeyBeforeSecondSync = Integer.parseInt(currentKey.substring(4));
            break;
          } else {
            fd.posBeforeFirstSync = currentkeyStartPos;
            fd.indexOfKeyBeforeFirstSync = Integer.parseInt(currentKey.substring(4));
            
            fd.posAfterFirstSync = cursorPos;
            fd.indexOfKeyAfterFirstSync = Integer.parseInt(currentKey.substring(4)) + 1;
            firstSyncSeen = true;
          }
        }
      }

    } catch (Exception e) {
      assertTrue(e.toString(), false);
    }
    iter.close();
    hoplog.close();
    return fd;
  }
  
  public void testClear() throws Exception {
    int count = 10;
    int bucketId = (int) System.nanoTime();
    HDFSUnsortedHoplogOrganizer organizer = new HDFSUnsortedHoplogOrganizer(regionManager, bucketId);

    // flush and create hoplog
    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    for (int i = 0; i < count; i++) {
      items.add(new TestEvent(("key-" + i), ("value-" + System.nanoTime())));
    }
    organizer.flush(items.iterator(), count);
    organizer.closeCurrentWriter();
    // check file existence in bucket directory
    FileStatus[] hoplogs = getBucketHoplogs(getName() + "/" + bucketId, 
                      AbstractHoplogOrganizer.SEQ_HOPLOG_EXTENSION);
    assertEquals(1, hoplogs.length);
    readSequenceFile(hdfsStore.getFileSystem(), hoplogs[0].getPath(), 0);
    
    
    // write another batch but do not close the data. 
    organizer.flush(items.iterator(), count);
    
    organizer.clear();
    
    hoplogs = getBucketHoplogs(getName() + "/" + bucketId, 
        AbstractHoplogOrganizer.SEQ_HOPLOG_EXTENSION);
    // check file existence in bucket directory
    FileStatus[] expiredhoplogs = getBucketHoplogs(getName() + "/" + bucketId, 
                      AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    
    // two expired hoplog should exists
    assertEquals(2, expiredhoplogs.length);
    assertEquals(2, hoplogs.length);
    // check the expired hops name should be same 
    assertTrue(expiredhoplogs[0].getPath().getName().equals(hoplogs[0].getPath().getName()+ AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION) || 
        expiredhoplogs[1].getPath().getName().equals(hoplogs[0].getPath().getName()+ AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION) );
    assertTrue(expiredhoplogs[0].getPath().getName().equals(hoplogs[1].getPath().getName()+ AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION) || 
        expiredhoplogs[1].getPath().getName().equals(hoplogs[1].getPath().getName()+ AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION) );
    
    // Test that second time clear should be harmless and should not result in extra files. 
    organizer.clear();
    hoplogs = getBucketHoplogs(getName() + "/" + bucketId, 
        AbstractHoplogOrganizer.SEQ_HOPLOG_EXTENSION);
    // check file existence in bucket directory
    expiredhoplogs = getBucketHoplogs(getName() + "/" + bucketId, 
                      AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    
    // two expired hoplog should exists
    assertEquals(2, expiredhoplogs.length);
    assertEquals(2, hoplogs.length);
    // check the expired hops name should be same 
    assertTrue(expiredhoplogs[0].getPath().getName().equals(hoplogs[0].getPath().getName()+ AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION) || 
        expiredhoplogs[1].getPath().getName().equals(hoplogs[0].getPath().getName()+ AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION) );
    assertTrue(expiredhoplogs[0].getPath().getName().equals(hoplogs[1].getPath().getName()+ AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION) || 
        expiredhoplogs[1].getPath().getName().equals(hoplogs[1].getPath().getName()+ AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION) );
    
    
    readSequenceFile(hdfsStore.getFileSystem(), hoplogs[0].getPath(), 0);
    readSequenceFile(hdfsStore.getFileSystem(), hoplogs[1].getPath(), 0);
  }
  
  public void readSequenceFile(FileSystem inputFS, Path sequenceFileName, int index)  throws IOException{
    readSequenceFile(inputFS, sequenceFileName, index, -1, 0, -1);
  }
  /**
   * Reads the sequence file assuming that it has keys and values starting from index that 
   * is specified as parameter. 
   * 
   */
  public void readSequenceFile(FileSystem inputFS, Path sequenceFileName, int index, int endIndex,
      int startoffset, int length) throws IOException {
    SequenceFileHoplog hoplog = new SequenceFileHoplog(inputFS, sequenceFileName, null);
    
    HoplogIterator<byte[], byte[]> iter = null;
    if (length == -1){
      iter = hoplog.getReader().scan();
    }
    else {
      iter = hoplog.getReader().scan(startoffset, length);
    }
    
    try {
      while (iter.hasNext()) {
        iter.next();
        PersistedEventImpl te = UnsortedHoplogPersistedEvent.fromBytes(iter.getValue());
        String stringkey = ((String)CacheServerHelper.deserialize(iter.getKey()));
        assertTrue("Expected key: key-" + index + ". Actual key: " + stringkey , ((String)stringkey).equals("key-" + index));
        index++;
      }
      if (endIndex != -1)
      assertTrue ("The keys should have been until key-"+ endIndex + " but they are until key-"+ (index-1),  index == endIndex + 1) ;
    } catch (Exception e) {
      assertTrue(e.toString(), false);
    }
    iter.close();
    hoplog.close();
 }

}
