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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileStatus;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSCompactionManager.CompactionRequest;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogOrganizer.Compactor;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSCompactionManagerJUnitTest extends BaseHoplogTestCase {
  /**
   * Tests queueing of major and minor compaction requests in respective queues
   */
  public void testMinMajCompactionIsolation() throws Exception {
    // no-op compactor
    Compactor compactor = new AbstractCompactor() {
      Object minor = new Object();
      Object major = new Object();
      public boolean compact(boolean isMajor, boolean isForced) throws IOException {
        try {
          if (isMajor) {
            synchronized (major) {
              major.wait();
            }
          } else {
            synchronized (minor) {
              minor.wait();
            }
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return true;
      }
    };

    // compaction is disabled. all requests will wait in queue
    HDFSCompactionManager instance = HDFSCompactionManager.getInstance(hdfsStore);
    alterMinorCompaction(hdfsStore, true);
    alterMajorCompaction(hdfsStore, true);
    
    assertEquals(0, instance.getMinorCompactor().getActiveCount());
    assertEquals(0, instance.getMajorCompactor().getActiveCount());
    
    //minor request
    CompactionRequest cr = new CompactionRequest("region", 0, compactor, false);
    HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr);
    //major request
    cr = new CompactionRequest("region", 0, compactor, true);
    HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr);
    
    //wait for requests to get in queue
    TimeUnit.MILLISECONDS.sleep(50);
    assertEquals(1, instance.getMinorCompactor().getActiveCount());
    assertEquals(1, instance.getMajorCompactor().getActiveCount());
  }

  /**
   * Tests compaction pause. Once compaction is stopped, requests will 
   * start getting rejected
   */
  public void testAlterAutoMinorCompaction() throws Exception {
    // each new compaction execution increments counter by 1. this way track how many pending tasks
    final AtomicInteger totalExecuted = new AtomicInteger(0);
    Compactor compactor = new AbstractCompactor() {
      public boolean compact(boolean isMajor, boolean isForced) throws IOException {
        totalExecuted.incrementAndGet();
        return true;
      }
    };

    // compaction is enabled. submit requests and after some time counter should be 0
    alterMinorCompaction(hdfsStore, true);
    CompactionRequest cr = new CompactionRequest("region", 0, compactor, false);
    HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr);
    cr = new CompactionRequest("region", 1, compactor, false);
    HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr);

    int totalWait = 20;
    while (totalWait > 0 && 2 != totalExecuted.get()) {
      // wait for operations to complete. The execution will terminate as soon as possible
      System.out.println("waiting one small cycle for dummy request to complete");
      TimeUnit.MILLISECONDS.sleep(50);
      totalWait--;
    }
    assertEquals(2, totalExecuted.get());

    // so compaction works. now disable comapction and submit large number of requests till rejected
    // execution counter should not increase
    alterMinorCompaction(hdfsStore, false);
    boolean success = false;
    int i = 0;
    do {
      cr = new CompactionRequest("region", ++i, compactor, false);
      success = HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr) != null;
    } while (success);

    TimeUnit.MILLISECONDS.sleep(500);
    assertEquals(2, totalExecuted.get());
  }
  public void testAlterAutoMajorCompaction() throws Exception {
    // each new compaction execution increments counter by 1. this way track how many pending tasks
    final AtomicInteger totalExecuted = new AtomicInteger(0);
    Compactor compactor = new AbstractCompactor() {
      public boolean compact(boolean isMajor, boolean isForced) throws IOException {
        totalExecuted.incrementAndGet();
        return true;
      }
    };
    
    // compaction is enabled. submit requests and after some time counter should be 0
    alterMajorCompaction(hdfsStore, true);
    CompactionRequest cr = new CompactionRequest("region", 0, compactor, true);
    HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr);
    cr = new CompactionRequest("region", 1, compactor, true);
    HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr);
    
    int totalWait = 20;
    while (totalWait > 0 && 2 != totalExecuted.get()) {
      // wait for operations to complete. The execution will terminate as soon as possible
      System.out.println("waiting one small cycle for dummy request to complete");
      TimeUnit.MILLISECONDS.sleep(50);
      totalWait--;
    }
    assertEquals(2, totalExecuted.get());
    
    // so compaction works. now disable comapction and submit large number of requests till rejected
    // execution counter should not increase
    alterMajorCompaction(hdfsStore, false);
    boolean success = false;
    int i = 0;
    do {
      cr = new CompactionRequest("region", ++i, compactor, true);
      success = HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr) != null;
      System.out.println("success: " + success);
    } while (success);
    
    TimeUnit.MILLISECONDS.sleep(500);
    assertEquals(2, totalExecuted.get());
  }
  
  /**
   * Tests duplicate compaction requests do not cause rejection
   */
   public void testDuplicateRequests() throws Exception {
    final AtomicBoolean barrierOpen = new AtomicBoolean(false);
    class TestCompactor extends AbstractCompactor {
      AtomicBoolean busy = new AtomicBoolean(false);
      public boolean compact(boolean isMajor, boolean isForced) throws IOException {
        synchronized (barrierOpen) {
          busy.set(true);
          if (barrierOpen.get()) {
            return false;
          }
          try {
            barrierOpen.wait();
          } catch (InterruptedException e) {
            return false;
          }
          busy.set(false);
        }
        return true;
      }
      public boolean isBusy(boolean isMajor) {return busy.get();}
    };
    
    System.setProperty(HoplogConfig.COMPCATION_QUEUE_CAPACITY, "10");

    alterMinorCompaction(hdfsStore, true);
    alterMajorCompaction(hdfsStore, true);
    // capacity is 10, thread num is 2, so only the first 12 request will be
    // submitted
    for (int i = 0; i < 15; i++) {
      CompactionRequest cr = new CompactionRequest("region", i, new TestCompactor(), true);
      boolean success = HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr) != null;
      if (success) {
        assertTrue("failed for " + i, i < 12);
      } else {
        assertTrue("failed for " + i, i >= 12);
      }
    }
    
    synchronized (barrierOpen) {
      barrierOpen.set(true);
      barrierOpen.notifyAll();
    }
    TimeUnit.MILLISECONDS.sleep(100);
    barrierOpen.set(false);
    
    HDFSCompactionManager.getInstance(hdfsStore).reset();
    TestCompactor compactor = new TestCompactor();
    for (int i = 0; i < 10; i++) {
      TimeUnit.MILLISECONDS.sleep(20);
      CompactionRequest cr = new CompactionRequest("region", 0, compactor, true);
      boolean success = HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr) != null;
      if (success) {
        assertTrue("failed for " + i, i < 2);
      } else {
        assertTrue("failed for " + i, i > 0);
      }
    }
  }

  public void testForceCompactionWithAutoDisabled() throws Exception {
    HoplogOrganizer<? extends PersistedEventImpl> organizer = new HdfsSortedOplogOrganizer(regionManager, 0);

    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent(("1"), ("1-1")));
    organizer.flush(items.iterator(), items.size());

    items.clear();
    items.add(new TestEvent(("2"), ("2-1")));
    organizer.flush(items.iterator(), items.size());
    
    FileStatus[] files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(2, files.length);
    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    assertEquals(0, files.length);
    
    CompactionRequest cr = new CompactionRequest(getName(), 0, organizer.getCompactor(), true);
    HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr);
    TimeUnit.MILLISECONDS.sleep(500);

    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    assertEquals(0, files.length);
    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(0, files.length);
    
    organizer.forceCompaction(true);
    TimeUnit.MILLISECONDS.sleep(500);
    
    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    assertEquals(1, files.length);
    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(2, files.length);
  }

  /**
   * Test force major compaction completes on version upgrade even when there is only one hoplog
   */
  public void testForceCompaction() throws Exception {
    HoplogOrganizer<? extends PersistedEventImpl> organizer = new HdfsSortedOplogOrganizer(regionManager, 0);

    ArrayList<TestEvent> items = new ArrayList<TestEvent>();
    items.add(new TestEvent(("1"), ("1-1")));
    organizer.flush(items.iterator(), items.size());

    items.clear();
    items.add(new TestEvent(("2"), ("2-1")));
    organizer.flush(items.iterator(), items.size());
    
    FileStatus[] files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.FLUSH_HOPLOG_EXTENSION);
    assertEquals(2, files.length);
    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    assertEquals(0, files.length);
    
    // isForced is true for user submitted compaction requests (through system procedure)
    // we do not want to compact an already compacted file
    CompactionRequest cr = new CompactionRequest(getName(), 0, organizer.getCompactor(), true, true/*isForced*/);
    Future<CompactionStatus> status = HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr);
    status.get().equals(true);

    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    assertEquals(1, files.length);
    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(2, files.length);

    // second request to force compact does not do anything
    status = HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr);
    status.get().equals(false);
    
    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    assertEquals(1, files.length);
    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(2, files.length);

    // upon version upgrade force compaction is allowed
    cr = new CompactionRequest(getName(), 0, organizer.getCompactor(), true, true, true);
    status = HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr);
    status.get().equals(true);
    
    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.MAJOR_HOPLOG_EXTENSION);
    assertEquals(2, files.length);
    files = getBucketHoplogs(getName() + "/0", AbstractHoplogOrganizer.EXPIRED_HOPLOG_EXTENSION);
    assertEquals(3, files.length); // + 1 for old major hoplog
  }

  /**
   * Test successful sequential submission
   */
  public void testSameBucketSeqRequest() throws Exception {
    final AtomicInteger counter = new AtomicInteger(0);
    Compactor compactor = new AbstractCompactor() {
      public boolean compact(boolean isMajor, boolean isForced) throws IOException {
        counter.set(1);
        return true;
      }
    };

    HDFSCompactionManager.getInstance(hdfsStore).reset();
    alterMinorCompaction(hdfsStore, true);
    alterMajorCompaction(hdfsStore, true);
    CompactionRequest cr = new CompactionRequest("region", 0, compactor, false);
    assertEquals(0, counter.get());
    boolean success = HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr) != null;
    assertEquals(true, success);
    while (!counter.compareAndSet(1, 0)) {
      TimeUnit.MILLISECONDS.sleep(20);
    }
    
    assertEquals(0, counter.get());
    success = HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr) != null;
    assertEquals(true, success);
    for (int i = 0; i < 10; i++) {
      TimeUnit.MILLISECONDS.sleep(20);
      if (counter.get() == 1) {
        break;
      }
    }
    assertEquals(1, counter.get());
  }
  
  public void testAlterMinorThreadsIncrease() throws Exception {
    doAlterCompactionThreads(false, false);
  }
  public void testAlterMinorThreadsDecrease() throws Exception {
    doAlterCompactionThreads(false, true);
  }
  public void testAlterMajorThreadsIncrease() throws Exception {
    doAlterCompactionThreads(true, false);
  }
  public void testAlterMajorThreadsDecrease() throws Exception {
    doAlterCompactionThreads(true, true);
  }
  
  public void doAlterCompactionThreads(final boolean testMajor, boolean decrease) throws Exception {
    final AtomicBoolean barrierOpen = new AtomicBoolean(false);
    final AtomicInteger counter = new AtomicInteger(0);
    class TestCompactor extends AbstractCompactor {
      public boolean compact(boolean isMajor, boolean isForced) throws IOException {
        synchronized (barrierOpen) {
          if ((testMajor && !isMajor)  || (!testMajor && isMajor)) {
            return true;
          }
          if (barrierOpen.get()) {
            return false;
          }
          try {
            barrierOpen.wait();
          } catch (InterruptedException e) {
            return false;
          }
          counter.incrementAndGet();
        }
        return true;
      }
    };
    
    System.setProperty(HoplogConfig.COMPCATION_QUEUE_CAPACITY, "1");

    HDFSStoreMutator mutator = hdfsStore.createHdfsStoreMutator();
    int defaultThreadCount = 10;
    if (testMajor) {
      alterMajorCompaction(hdfsStore, true);
      defaultThreadCount = 2;
      mutator.getCompactionConfigMutator().setMajorCompactionMaxThreads(15);
      if (decrease) {
        mutator.getCompactionConfigMutator().setMajorCompactionMaxThreads(1);
      }
    } else {
      alterMinorCompaction(hdfsStore, true);
      mutator.getCompactionConfigMutator().setMaxThreads(15);
      if (decrease) {
        mutator.getCompactionConfigMutator().setMaxThreads(1);
      }
    }
    
    // capacity is 1, thread num is 10 or 2, so only the first 11 or 3 request will be
    // submitted
    cache.getLogger().info("<ExpectedException action=add>java.util.concurrent.RejectedExecutionException</ExpectedException>");
    for (int i = 0; i < 15; i++) {
      CompactionRequest cr = new CompactionRequest("region", i, new TestCompactor(), testMajor);
      boolean success = HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr) != null;
      if (success) {
        assertTrue("failed for " + i, i <= defaultThreadCount);
      } else {
        assertTrue("failed for " + i, i > defaultThreadCount);
      }
    }
    
    TimeUnit.MILLISECONDS.sleep(500);
    assertEquals(0, counter.get());
    synchronized (barrierOpen) {
      barrierOpen.set(true);
      barrierOpen.notifyAll();
    }
    TimeUnit.MILLISECONDS.sleep(500);
    assertEquals(defaultThreadCount, counter.get());
    
    hdfsStore.alter(mutator);

    counter.set(0);
    barrierOpen.set(false);
    for (int i = 0; i < 15; i++) {
      TimeUnit.MILLISECONDS.sleep(100);
      CompactionRequest cr = new CompactionRequest("region", i, new TestCompactor(), testMajor);
      boolean success = HDFSCompactionManager.getInstance(hdfsStore).submitRequest(cr) != null;
      if (decrease) {
        if (i > 3) {
          assertFalse("failed for " + i, success);
        }
      } else {
        assertTrue("failed for " + i, success);
      }
    }
    TimeUnit.MILLISECONDS.sleep(500);
    synchronized (barrierOpen) {
      barrierOpen.set(true);
      barrierOpen.notifyAll();
    }
    TimeUnit.MILLISECONDS.sleep(500);
    if (decrease) {
      assertTrue(counter.get() < 4);
    } else {
      assertEquals(15, counter.get());
    }

    cache.getLogger().info("<ExpectedException action=remove>java.util.concurrent.RejectedExecutionException</ExpectedException>");
  }
}
