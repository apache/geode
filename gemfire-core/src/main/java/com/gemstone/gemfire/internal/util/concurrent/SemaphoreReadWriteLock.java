/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util.concurrent;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This ReadWriteLock is useful when different threads need to lock
 * and unlock the read lock. This is <b>NOT</b> a reentrant lock.
 * 
 * @author sbawaska
 */
public class SemaphoreReadWriteLock implements ReadWriteLock {

  private SemaphoreReadLock readLock;
  private SemaphoreWriteLock writeLock;

  public SemaphoreReadWriteLock() {
    Semaphore writerSemaphore = new Semaphore(1);
    Semaphore readerSemaphore = new Semaphore(1);
    readLock = new SemaphoreReadLock(readerSemaphore, writerSemaphore);
    writeLock = new SemaphoreWriteLock(writerSemaphore);
  }
  
  @Override
  public Lock readLock() {
    return readLock;
  }

  @Override
  public Lock writeLock() {
    return writeLock;
  }

  public static class SemaphoreReadLock implements Lock {
    private int numReaders = 0;
    private final Semaphore readerSemaphore;
    private final Semaphore writerSemaphore;

    public SemaphoreReadLock(Semaphore readerSemaphore,
        Semaphore writerSemaphore) {
      this.readerSemaphore = readerSemaphore;
      this.writerSemaphore = writerSemaphore;
    }

    @Override
    public void lock() {
      boolean interrupted = false;
      try {
        for (;;) {
          try {
            lockInterruptibly();
            break;
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      readerSemaphore.acquire();
      try {
        numReaders++;
        if (numReaders == 1) {
          writerSemaphore.acquire();
        }
      } finally {
        // in case writeSemaphore.acquire throws Exception
        readerSemaphore.release();
      }
    }

    @Override
    public boolean tryLock() {
      boolean interrupted = false;
      try {
        for (;;) {
          try {
            return tryLock(0, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit)
        throws InterruptedException {
      if (readerSemaphore.tryAcquire(time, unit)) {
        int oldNumReaders = numReaders;
        numReaders++;
        if (numReaders == 1) {
          if (writerSemaphore.tryAcquire(time, unit)) {
            readerSemaphore.release();
            return true;
          } else {
            numReaders = oldNumReaders;
            readerSemaphore.release();
            return false;
          }
        } else {
          readerSemaphore.release();
          return true;
        }
      }
      return false;
    }

    @Override
    public void unlock() {
      for (;;) {
        boolean interrupted = false;
        try {
          readerSemaphore.acquire();
        } catch (InterruptedException e) {
          interrupted = true;
          continue;
        } finally {
          if (interrupted) Thread.currentThread().interrupt();
        }
        numReaders--;
        // The unlock method is forgiving
        if (numReaders <= 0) {
          numReaders = 0;
          if (writerSemaphore.availablePermits() == 0) {
            writerSemaphore.release();
          }
        }
        readerSemaphore.release();
        break;
      }
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }
  }
  
  public static class SemaphoreWriteLock implements Lock {

    private final Semaphore writerSemaphore;

    public SemaphoreWriteLock(Semaphore writerSemaphore) {
      this.writerSemaphore = writerSemaphore;
    }

    @Override
    public void lock() {
      boolean interrupted = false;
      try {
        for(;;) {
          try {
            lockInterruptibly();
            break;
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      writerSemaphore.acquire();
    }

    @Override
    public boolean tryLock() {
      return writerSemaphore.tryAcquire();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit)
        throws InterruptedException {
      return writerSemaphore.tryAcquire(time, unit);
    }

    @Override
    public void unlock() {
      writerSemaphore.release();
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }
    
  }
}
