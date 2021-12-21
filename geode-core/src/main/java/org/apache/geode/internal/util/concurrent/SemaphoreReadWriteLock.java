/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.util.concurrent;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This ReadWriteLock is useful when different threads need to lock and unlock the read lock. This
 * is <b>NOT</b> a reentrant lock.
 *
 */
public class SemaphoreReadWriteLock implements ReadWriteLock {

  private final SemaphoreReadLock readLock;
  private final SemaphoreWriteLock writeLock;

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

    public SemaphoreReadLock(Semaphore readerSemaphore, Semaphore writerSemaphore) {
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
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
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
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
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
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
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
        for (;;) {
          try {
            lockInterruptibly();
            break;
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
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
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
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
