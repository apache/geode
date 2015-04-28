/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.locks;

import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

/**
 * This class would be used by GemFire transactions to lock entries.
 * 
 * Three types of lock can be obtained.
 * 
 * 1. Write exclusive (write_ex_l) 2. Write share (write_sh_l) 3. Read (read_l)
 * 
 * write_ex_l lock will disallow an attempt to acquire any kind of lock.
 * 
 * write_sh_l lock will disallow any other attempt to write_ex_l, write_sh_l but
 * will allow read_l.
 * 
 * read_l will disallow any attempt to write_ex_l but would allow write_sh_l 
 * and read_l
 * 
 * The write_ex_l and read_l has the normal read write lock semantics but a
 * write_sh_l will be a hint that the guarded resource can be read but attempt
 * to write it should be avoided
 * 
 * In addition to this, this lock provides an option to wait for a lock or to
 * return immediately on failure.
 * 
 * 
 * @author kneeraj
 * @since post 6.5
 */
public class ReentrantReadWriteWriteShareLock {

  static final int READ_MODE = 0x0;

  static final int WRITE_SHARED_MODE = 0x1;

  static final int WRITE_EXCLUSIVE_MODE = 0x2;

  static final int MODE_MASK = 0x3;

  static final int RELEASE_ALL_MASK = 0x8;

  private CASSync sync;

  private static int WAIT_THRESHOLD;

  private static InternalDistributedSystem DSYS;

  static {
    DSYS = InternalDistributedSystem.getConnectedInstance();
    if (DSYS != null) {
      final int timeoutSecs = DSYS.getConfig().getAckWaitThreshold();
      if (timeoutSecs > 0) {
        WAIT_THRESHOLD = timeoutSecs;
      }
      else {
        WAIT_THRESHOLD = -1;
      }
    }
    else {
      WAIT_THRESHOLD = -1;
    }
  }

  public ReentrantReadWriteWriteShareLock(boolean allowLockUpgrade) {
    this.sync = new CASSync(allowLockUpgrade);
  }

  public void resetOwnerId() {
    this.sync.ownerId = null;
  }

  /**
   * An attempt to readLock should return immediately(successfully) irrespective
   * of the number of read locks and write share lock already taken. It should
   * fail only when there are already max number of allowed read locks taken or
   * a write_ex_l has been taken.
   * 
   * @param msecs
   *          - '-1' means wait indefinitely, '0' return immediately
   *          irrespective of success or failure. timeoutmillis will try for max
   *          that much time in case of failure
   * @return - true if lock acquired successfully, false otherwise
   * @throws InterruptedException
   */
  public boolean attemptReadLock(long msecs) throws InterruptedException {
    return lockInSharedMode(this.sync, msecs, READ_MODE, null);
  }

  public void releaseReadLock(boolean releaseAll) {
    this.sync.releaseShared(releaseAll ? READ_MODE | RELEASE_ALL_MASK
        : READ_MODE, null);
  }

  /**
   * An attempt to writeShareLock should succeed when there are no
   * writeExclusiveLock already taken.
   * 
   * @param msecs
   *          - '-1' means wait indefinitely, '0' return immediately
   *          irrespective of success or failure. timeoutmillis will try for max
   *          that much time in case of failure
   * @param id
   *          - The id which will be used to check re-entrancy instead of thread
   *          id
   * @return - true if lock acquired successfully, false otherwise
   * @throws InterruptedException
   */
  public boolean attemptWriteShareLock(long msecs, Object id)
      throws InterruptedException {
    return lockInSharedMode(this.sync, msecs, WRITE_SHARED_MODE, id);
  }

  public void releaseWriteShareLock(boolean releaseAll, Object id) {
    // Neeraj: log
    this.sync.releaseShared(releaseAll ? WRITE_SHARED_MODE | RELEASE_ALL_MASK
        : WRITE_SHARED_MODE, id);
  }

  /**
   * An attempt to writeExclusiveLock should succeed when there are no readLocks
   * already taken or a writeShareLock is already taken or a writeExclusiveLock
   * is already taken.
   * 
   * @param msecs
   *          - '-1' means wait indefinitely, '0' return immediately
   *          irrespective of success or failure. timeoutmillis will try for max
   *          that much time in case of failure
   * @param id
   *          - The id which will be used to check re-entrancy instead of thread
   *          id
   * @return - true if lock acquired successfully, false otherwise
   * @throws InterruptedException
   */
  public boolean attemptWriteExclusiveLock(long msecs, Object id)
      throws InterruptedException {
    if (msecs < 0) {
      msecs = Long.MAX_VALUE;
    }
    else if (msecs == 0) {
      // return immediately after trying for lock
      return this.sync.tryAcquire(WRITE_EXCLUSIVE_MODE, id);
    }

    // we do this in units of "ack-wait-threshold" to allow writing log
    // messages in case lock acquire has not succeeded so far; also check for
    // CancelCriterion

    final long timeoutMillis;
    if (WAIT_THRESHOLD > 0) {
      timeoutMillis = TimeUnit.SECONDS.toMillis(WAIT_THRESHOLD);
    }
    else {
      timeoutMillis = msecs;
    }
    boolean res = false;
    while (msecs > timeoutMillis) {
      if (this.sync.tryAcquireNanos(WRITE_EXCLUSIVE_MODE, id,
          TimeUnit.MILLISECONDS.toNanos(timeoutMillis))) {
        res = true;
        break;
      }

      DSYS.getCancelCriterion().checkCancelInProgress(null);
      msecs -= timeoutMillis;
    }
    if (!res) {
      res = this.sync.tryAcquireNanos(WRITE_EXCLUSIVE_MODE, null,
          TimeUnit.MILLISECONDS.toNanos(msecs));
    }
    // Neeraj: log
    return res;
  }

  public void releaseWriteExclusiveLock(boolean releaseAll, Object id) {
    // Neeraj: log
    this.sync.release(releaseAll ? WRITE_EXCLUSIVE_MODE | RELEASE_ALL_MASK
        : WRITE_EXCLUSIVE_MODE, id);
  }

  public static void clearStatics() {
    DSYS = null;
    WAIT_THRESHOLD = -1;
  }

  private static boolean lockInSharedMode(CASSync sync, long msecs,
      int sharedMode, Object id) throws InterruptedException {
    if (msecs < 0) {
      msecs = Long.MAX_VALUE;
    }
    else if (msecs == 0) {
      // return immediately after trying for lock
      return sync.tryAcquireShared(sharedMode, id) >= 0;
    }

    // we do this in units of "ack-wait-threshold" to allow writing log
    // messages in case lock acquire has not succeeded so far; also check for
    // CancelCriterion

    final long timeoutMillis;
    if (WAIT_THRESHOLD > 0) {
      timeoutMillis = TimeUnit.SECONDS.toMillis(WAIT_THRESHOLD);
    }
    else {
      timeoutMillis = msecs;
    }
    boolean res = false;
    while (msecs > timeoutMillis) {
      if (sync.tryAcquireSharedNanos(sharedMode, id, TimeUnit.MILLISECONDS
          .toNanos(timeoutMillis))) {
        res = true;
        break;
      }

      DSYS.getCancelCriterion().checkCancelInProgress(null);
      msecs -= timeoutMillis;
    }
    if (!res) {
      res = sync.tryAcquireSharedNanos(sharedMode, null, TimeUnit.MILLISECONDS
          .toNanos(msecs));
    }
    // Neeraj: log
    return res;
  }

  /**
   * Actual implementation of @link{GFEAbstractQueuedSynchronizer} class
   * @author kneeraj
   *
   */
  @SuppressWarnings("serial")
  static final class CASSync extends GFEAbstractQueuedSynchronizer {

    static final int READ_SHARED_BITS = 16;

    static final int WRITE_EXCLUSIVE_BITS = (Integer.SIZE - READ_SHARED_BITS) / 2;

    static final int WRITE_SHARED_BITS = WRITE_EXCLUSIVE_BITS;

    static final int READ_SHARED_MASK = (1 << READ_SHARED_BITS) - 1;

    static final int MAX_READ_SHARED_COUNT = READ_SHARED_MASK;

    static final int EXCLUSIVE_ONE = (1 << (READ_SHARED_BITS + WRITE_SHARED_BITS));

    static final int WRITE_SHARE_ONE = (1 << READ_SHARED_BITS);

    static final int MAX_WRITE_SHARED_COUNT = (1 << WRITE_SHARED_BITS) - 1;

    static final int MAX_EXCLUSIVE_COUNT = MAX_WRITE_SHARED_COUNT;

    static final int WRITE_SHARED_MASK = (EXCLUSIVE_ONE - 1) ^ READ_SHARED_MASK;

    static final int WRITE_EXCLUSIVE_MASK = 0xffffffff ^ (WRITE_SHARED_MASK | READ_SHARED_MASK);

    private final boolean allowUpgradeOfWriteShare;

    private Object ownerId;

    private static int exclusiveCount(int c) {
      return (WRITE_EXCLUSIVE_MASK & c) >>> (READ_SHARED_BITS + WRITE_SHARED_BITS);
    }

    private static int writeSharedCount(int c) {
      return (WRITE_SHARED_MASK & c) >> READ_SHARED_BITS;
    }

    private static int readSharedCount(int c) {
      return (READ_SHARED_MASK & c);
    }

    public CASSync(boolean allowUpgradeOfWriteShare) {
      this.allowUpgradeOfWriteShare = allowUpgradeOfWriteShare;
    }

    protected boolean tryAcquire(int arg, Object id) {
      assert arg == WRITE_EXCLUSIVE_MODE;

      for (;;) {
        final int currentState = getState();
        final int currentHolds = exclusiveCount(currentState);
        final int currentWriteSharedHolds = writeSharedCount(currentState);
        final int currentReadHolds = readSharedCount(currentState);
        if (currentReadHolds > 0) {
          // Neeraj: log
          return false;
        }
        if (currentWriteSharedHolds > 0) {
          if (!id.equals(this.ownerId)) {
            return false;
          }
          if (this.allowUpgradeOfWriteShare) {
            // Neeraj: This is the case of upgrade as the ids are equal we will
            // allow this.
            if (currentHolds > 0) {
              if (currentHolds == MAX_EXCLUSIVE_COUNT) {
                throw new InternalGemFireError(
                    "Maximum write lock count exceeded!");
              }
            }
            // Neeraj: incrementing exclusive and decrementing write share
            int newState = currentState + EXCLUSIVE_ONE - WRITE_SHARE_ONE;
            if (compareAndSetState(currentState, newState)) {
              break;
            }
            continue;
          }
          else {
            return false;
          }
        }

        if (currentHolds > 0) {
          if (currentHolds == MAX_EXCLUSIVE_COUNT) {
            throw new InternalGemFireError("Maximum write lock count exceeded!");
          }
          if (!id.equals(this.ownerId)) {
            return false;
          }
        }

        if (compareAndSetState(currentState, currentState + EXCLUSIVE_ONE)) {
          break;
        }
      }
      this.ownerId = id;
      return true;
    }

    protected boolean tryRelease(int arg, Object id) {
      assert ((arg & MODE_MASK) == WRITE_EXCLUSIVE_MODE);
      assert id != null;
      
      if (!id.equals(this.ownerId)) {
        throw new InternalGemFireError(
            "an attempt to release lock by a non owner");
      }
      final boolean releaseAll = (arg & RELEASE_ALL_MASK) == RELEASE_ALL_MASK;
      for (;;) {
        final int currentState = getState();
        final int writeExclusiveCount = exclusiveCount(currentState);
        if (writeExclusiveCount == 0) {
          throw new IllegalMonitorStateException();
        }
        int newState;
        if (releaseAll) {
          newState = currentState & ~WRITE_EXCLUSIVE_MASK;
        }
        else {
          newState = currentState - EXCLUSIVE_ONE;
        }
        if (compareAndSetState(currentState, newState)) {
          return true;
        }
      }
    }

    protected int tryAcquireShared(int arg, Object id) {
      assert (arg == READ_MODE || arg == WRITE_SHARED_MODE);

      for (;;) {
        final int currentState = getState();
        final int currentHolds = exclusiveCount(currentState);
        final int currentWriteSharedHolds = writeSharedCount(currentState);
        final int currentReadHolds = readSharedCount(currentState);

        if (currentHolds > 0) {
          return -1;
        }
        if (arg == READ_MODE) {
          if (currentReadHolds == MAX_READ_SHARED_COUNT) {
            throw new InternalGemFireError("Maximum read lock count exceeded!");
          }
          if (compareAndSetState(currentState, currentState + 1)) {
            // Neeraj: success. But we need to return 0 when no further read
            // will succeed else some +ve number
            if (currentReadHolds + 1 == MAX_READ_SHARED_COUNT) {
              return 0;
            }
            return 1;
          }
        }
        else {
          // Neeraj: This is write shared mode
          if (currentWriteSharedHolds > 0) {
            if (!id.equals(this.ownerId)) {
              return -1;
            }
            if (currentWriteSharedHolds == MAX_WRITE_SHARED_COUNT) {
              throw new InternalGemFireError(
                  "Maximum write share lock count exceeded!");
            }
          }
          if (compareAndSetState(currentState, currentState + WRITE_SHARE_ONE)) {
            if (currentWriteSharedHolds + 1 == MAX_WRITE_SHARED_COUNT) {
              this.ownerId = id;
              return 0;
            }
            this.ownerId = id;
            return 1;
          }
        }
      }
    }

    protected boolean tryReleaseShared(int arg, Object id) {
      assert ((arg & MODE_MASK) == WRITE_SHARED_MODE
          || (arg & MODE_MASK) == READ_MODE);

      final boolean releaseAll = (arg & RELEASE_ALL_MASK) == RELEASE_ALL_MASK;
      for (;;) {
        final int currentState = getState();
        if ((arg & MODE_MASK) == WRITE_SHARED_MODE) {
          assert id != null;
          if (!id.equals(this.ownerId)) {
            throw new InternalGemFireError(
                "an attempt to release lock by a non owner");
          }
          // clear write share bits
          final int writeSharedCount = writeSharedCount(currentState);
          if (writeSharedCount == 0) {
            throw new IllegalMonitorStateException();
          }
          int newState;
          if (releaseAll) {
            newState = currentState & ~WRITE_SHARED_MASK;
          }
          else {
            newState = currentState - WRITE_SHARE_ONE;
          }
          if (compareAndSetState(currentState, newState)) {
            return true;
          }
        }
        else {
          final int readCount = readSharedCount(currentState);
          if (readCount == 0) {
            throw new IllegalMonitorStateException();
          }
          int newState;
          if (releaseAll) {
            newState = currentState & ~READ_SHARED_MASK;
          }
          else {
            newState = currentState - 1;
          }
          if (compareAndSetState(currentState, newState)) {
            return true;
          }
        }
      }
    }

    protected boolean isHeldExclusively() {
      throw new UnsupportedOperationException();
    }
  }
}
