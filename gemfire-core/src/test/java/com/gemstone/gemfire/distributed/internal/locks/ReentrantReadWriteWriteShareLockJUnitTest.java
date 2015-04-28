/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.locks;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.internal.cache.locks.ReentrantReadWriteWriteShareLock;
import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

@Category(UnitTest.class)
public class ReentrantReadWriteWriteShareLockJUnitTest extends TestCase {

  // Neeraj: These variables are actually in
  // ReentrantReadWriteWriteShareLock.CASSync class
  // as static fields. But are not exposed so copied here.
  static final int READ_SHARED_BITS = 16;

  static final int WRITE_EXCLUSIVE_BITS = (Integer.SIZE - READ_SHARED_BITS) / 2;

  static final int WRITE_SHARED_BITS = WRITE_EXCLUSIVE_BITS;

  static final int READ_SHARED_MASK = (1 << READ_SHARED_BITS) - 1;

  static final int MAX_READ_SHARED_COUNT = READ_SHARED_MASK;

  static final int EXCLUSIVE_ONE = (1 << (READ_SHARED_BITS + WRITE_SHARED_BITS));

  static final int WRITE_SHARE_ONE = (1 << READ_SHARED_BITS);

  static final int MAX_WRITE_SHARED_COUNT = (1 << WRITE_SHARED_BITS) - 1;

  static final int MAX_EXCLUSIVE_COUNT = MAX_WRITE_SHARED_COUNT;

  public void setUp() throws Exception {

  }

  public void tearDown() throws Exception {

  }

  public void testMaxReadsMaxWritesAndMaxExclusive() throws Exception {
    Object id = new String("id");
    ReentrantReadWriteWriteShareLock lock = new ReentrantReadWriteWriteShareLock(
        true);

    // Take max possible read locks
    for (int i = 0; i < MAX_READ_SHARED_COUNT; i++) {
      assertTrue(lock.attemptReadLock(0));
    }
    try {
      lock.attemptReadLock(0);
      fail("attempt to acquire read lock after max possible read: "
          + MAX_READ_SHARED_COUNT + " should have failed");
    } catch (InternalGemFireError e) {
      // ignore expected exception
    }

    // Now release all the
    // Take max possible read locks
    for (int i = 0; i < MAX_READ_SHARED_COUNT; i++) {
      lock.releaseReadLock(false);
    }
    // Attempt to again release a read lock will throw
    // IllegalMonitorStateException
    try {
      lock.releaseReadLock(true);
      fail("attempt to release read lock when read count is 0 should have failed");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }

    // Take max possible write share locks
    for (int i = 0; i < MAX_WRITE_SHARED_COUNT; i++) {
      assertTrue(lock.attemptWriteShareLock(0, id));
    }
    try {
      lock.attemptWriteShareLock(0, id);
      fail("attempt to acquire write share lock after max possible write share: "
          + MAX_WRITE_SHARED_COUNT + " should have failed");
    } catch (InternalGemFireError e) {
      // ignore expected exception
    }

    // Now release the write share lock
    lock.releaseWriteShareLock(true, id);
    // Attempt to again release a write share lock should throw
    // IllegalMonitorStateException
    try {
      lock.releaseWriteShareLock(false, id);
      fail("attempt to release write share lock when share counter is reset should have failed");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }

    // Take max possible write ex locks
    for (int i = 0; i < MAX_EXCLUSIVE_COUNT; i++) {
      assertTrue(lock.attemptWriteExclusiveLock(0, id));
    }
    try {
      lock.attemptWriteExclusiveLock(0, id);
      fail("attempt to acquire write ex lock after max possible write ex: "
          + MAX_EXCLUSIVE_COUNT + " should have failed");
    } catch (InternalGemFireError e) {
      // ignore expected exception
    }

    // Now release the write ex lock
    lock.releaseWriteExclusiveLock(true, id);
    // Attempt to again release a write ex lock will throw
    // IllegalMonitorStateException
    try {
      lock.releaseWriteExclusiveLock(true, id);
      fail("attempt to release write share lock when share counter is reset should have failed");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }
  }

  public void testLockBehaviourWithSameThread() throws Exception {
    Object id = new String("id");
    ReentrantReadWriteWriteShareLock lock = new ReentrantReadWriteWriteShareLock(
        true);
    assertTrue(lock.attemptReadLock(0));
    assertTrue(lock.attemptWriteShareLock(0, id));
    assertFalse(lock.attemptWriteExclusiveLock(0, id));

    lock.releaseReadLock(true);
    lock.releaseWriteShareLock(true, id);

    assertTrue(lock.attemptWriteShareLock(0, id));
    assertTrue(lock.attemptReadLock(0));
    assertFalse(lock.attemptWriteExclusiveLock(0, "newid"));
    assertFalse(lock.attemptWriteExclusiveLock(0, id));
    lock.releaseReadLock(false);
    assertTrue(lock.attemptWriteExclusiveLock(0, id));

    lock.releaseWriteExclusiveLock(false, id);

    // try exclusive with releaseAll
    assertTrue(lock.attemptWriteExclusiveLock(0, id));
    assertFalse(lock.attemptReadLock(0));
    assertFalse(lock.attemptWriteShareLock(0, id));
    assertFalse(lock.attemptWriteExclusiveLock(0, "newid"));
    assertTrue(lock.attemptWriteExclusiveLock(0, id));

    lock.releaseWriteExclusiveLock(true, id);
    try {
      lock.releaseWriteExclusiveLock(true, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    try {
      lock.releaseWriteExclusiveLock(false, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }

    // try exclusive without releaseAll
    assertTrue(lock.attemptWriteExclusiveLock(0, id));
    assertFalse(lock.attemptReadLock(0));
    assertFalse(lock.attemptWriteShareLock(0, id));
    assertFalse(lock.attemptWriteExclusiveLock(0, "newid"));
    assertTrue(lock.attemptWriteExclusiveLock(0, id));

    lock.releaseWriteExclusiveLock(false, id);
    lock.releaseWriteExclusiveLock(false, id);
    try {
      lock.releaseWriteExclusiveLock(false, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    try {
      lock.releaseWriteExclusiveLock(true, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
  }

  public void testLockBehaviourWithDIfferentThread() throws Exception {
    Object id = new String("id");
    ReentrantReadWriteWriteShareLock lock = new ReentrantReadWriteWriteShareLock(
        true);
    assertTrue(lock.attemptWriteShareLock(0, id));

    Locker locker = new Locker(lock, id, true, true, "R:S:E", "P:P:P");
    Thread t = new Thread(locker);
    t.start();
    t.join();

    // lock again as it would have been released
    assertTrue(lock.attemptWriteShareLock(0, id));
    locker = new Locker(lock, id, false, true, "R:S:E", "P:F:F");
    Thread t1 = new Thread(locker);
    t1.start();
    t1.join();
  }

  private static volatile boolean failed = false;

  public void _testLockBehaviourQueue() throws Exception {
    Object id = new String("id");
    ReentrantReadWriteWriteShareLock lock = new ReentrantReadWriteWriteShareLock(
        true);
    assertTrue(lock.attemptWriteShareLock(0, id));
    assertTrue(lock.attemptReadLock(0));
    int shareCnt = 0;
    for (int i = 0; i < MAX_READ_SHARED_COUNT * 5; i++) {
      if (i % MAX_WRITE_SHARED_COUNT == 0) {
        // if (shareCnt == MAX_WRITE_SHARED_COUNT) {
        // continue;
        // }
        // shareCnt++;
        // Thread t = new Thread(new WriteShareLocker(lock, id));
        // t.start();
      }
      else {
        Thread t = new Thread(new ReadLocker(lock));
        t.start();
      }
    }
    Thread.sleep(5000);
    lock.releaseWriteShareLock(true, id);
    try {
      lock.releaseWriteShareLock(false, id);
      fail("write share release clears the write bits, so second attempt should fail");
    } catch (IllegalMonitorStateException e) {
      // ignore the exception as expected
    }
    assertFalse(failed);
  }

  private static class ReadLocker implements Runnable {

    private long sleepMillisAfterTakingLock = 2;

    private long waitMillisForTakingLock = 10;

    private ReentrantReadWriteWriteShareLock l;

    public ReadLocker(ReentrantReadWriteWriteShareLock lock) {
      this.l = lock;
    }

    public void run() {
      try {
        if (this.l.attemptReadLock(this.waitMillisForTakingLock)) {
          Thread.sleep(this.sleepMillisAfterTakingLock);
        }
        else {
          failed = true;
        }
      } catch (InterruptedException e) {
        fail("did not expect exception in taking read locks");
      }
      this.l.releaseReadLock(false);
    }
  }

  private static class WriteShareLocker implements Runnable {

    private long sleepMillisAfterTakingLock = 3;

    private long waitMillisForTakingLock = 6;

    private ReentrantReadWriteWriteShareLock l;

    private Object id;

    public WriteShareLocker(ReentrantReadWriteWriteShareLock lock, Object id) {
      this.l = lock;
      this.id = id;
    }

    public void run() {
      try {
        if (this.l.attemptWriteShareLock(waitMillisForTakingLock, this.id)) {
          failed = true;
        }
      } catch (InterruptedException e) {
        fail("did not expect exception in taking read locks");
      }
      // this.l.releaseReadLock();
    }
  }

  private static class Locker implements Runnable {
    private ReentrantReadWriteWriteShareLock l;

    private Object id;

    private boolean tryWithSameId;

    private String[] opsArr;

    private String[] resArr;

    private boolean releasing;

    public Locker(ReentrantReadWriteWriteShareLock lock, Object id,
        boolean sameId, boolean rel, String ops, String results) {
      this.l = lock;
      this.id = id;
      this.tryWithSameId = sameId;
      opsArr = ops.split(":");
      this.releasing = rel;
      this.resArr = results.split(":");
    }

    public void run() {
      try {
        for (int i = 0; i < this.opsArr.length; i++) {
          char ch = this.opsArr[i].charAt(0);
          boolean shouldPass = this.resArr[i].equals("P") ? true : false;
          switch (ch) {
            case 'R':
              if (shouldPass) {
                assertTrue(this.l.attemptReadLock(0));
              }
              else {
                assertFalse(this.l.attemptReadLock(0));
              }
              if (this.releasing && shouldPass) {
                this.l.releaseReadLock(false);
              }
              break;

            case 'S':

              if (shouldPass) {
                if (this.tryWithSameId) {
                  assertTrue(this.l.attemptWriteShareLock(0, this.id));
                }
                else {
                  assertTrue(this.l.attemptWriteShareLock(0, "someId"));
                }
              }
              else {
                assertFalse(this.l.attemptWriteShareLock(0, "someId"));
              }
              if (this.releasing && shouldPass) {
                this.l.releaseWriteShareLock(true, id);
              }
              break;

            case 'E':

              if (shouldPass) {
                if (this.tryWithSameId) {
                  assertTrue(this.l.attemptWriteExclusiveLock(0, this.id));
                }
                else {
                  assertTrue(this.l.attemptWriteExclusiveLock(0, "someid"));
                }
              }
              else {
                assertFalse(this.l.attemptWriteExclusiveLock(0, "someid"));
              }
              if (this.releasing && shouldPass) {
                this.l.releaseWriteExclusiveLock(true, id);
              }
              break;

            default:
              fail("not expected to come to default case");
          }
        }
      } catch (Exception e) {
        fail("not expected to get exception");
      }
    }
  }
  
  private static final class SharedStruct {

    long longVal;

    void init(long val) {
      this.longVal = val;
    }
  }
  
  private static final SharedStruct value = new SharedStruct();
  
  private static AtomicInteger tx1success = new AtomicInteger(0);
  
  public void _testTransactionalBehaviour() throws Exception {
    ReentrantReadWriteWriteShareLock txlock = new ReentrantReadWriteWriteShareLock(true);
    ReadWriteLock readFailLock = new ReentrantReadWriteLock();
    boolean readFails = false;
       
    value.init(0);
    
    // 5 increments
    Runnable Tx1 = new Runnable() {  
      private long startVal;
      private long thisTxVal;
      private Object id;

      public void run() {
        this.start();
        if (this.commit()) {
          tx1success.incrementAndGet();
        }
      }

      private boolean commit() {
        for(int i=0; i< 10; i++) {
          
        }
        return false; 
      }

      private void start() {
        this.id = new Object();
        this.startVal = this.thisTxVal = value.longVal;
      }
    };
    
    Runnable writeTask = new Runnable() {  
      public void run() {
        Object id = new Object();
      }
    };
    
    Runnable writeShareTask = new Runnable() {  
      public void run() {
      }
    };
  }
  
}
