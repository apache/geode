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
package org.apache.geode.internal.cache;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.Logger;
import org.jgroups.annotations.GuardedBy;

import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.logging.CoreLoggingExecutors;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * ExpiryTask represents a timeout event for expiration
 */
public abstract class ExpiryTask extends SystemTimer.SystemTimerTask {

  private static final Logger logger = LogService.getLogger();

  private LocalRegion region; // no longer final so cancel can null it out see bug 37574

  @MakeNotStatic
  private static final ExecutorService executor;

  static {
    // default to inline expiry to fix bug 37115
    int nThreads = Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "EXPIRY_THREADS", 0);
    if (nThreads > 0) {
      executor = CoreLoggingExecutors.newThreadPoolWithSynchronousFeed("Expiry ",
          (Runnable command) -> doExpiryThread(command),
          nThreads);
    } else {
      executor = null;
    }
  }

  private static void doExpiryThread(Runnable command) {
    ConnectionTable.threadWantsSharedResources();
    try {
      command.run();
    } finally {
      ConnectionTable.releaseThreadsSockets();
    }
  }

  protected ExpiryTask(LocalRegion region) {
    this.region = region;
  }

  protected abstract ExpirationAttributes getIdleAttributes();

  protected abstract ExpirationAttributes getTTLAttributes();

  /**
   * @return the absolute time (ms since Jan 1, 1970) at which this region expires, due to either
   *         time-to-live or idle-timeout (whichever will occur first), or 0 if neither are used.
   */
  public long getExpirationTime() throws EntryNotFoundException {
    long ttl = getTTLExpirationTime();
    long idle = getIdleExpirationTime();
    if (ttl == 0) {
      return idle;
    } else if (idle == 0) {
      return ttl;
    }
    return Math.min(ttl, idle);
  }

  /** Return the absolute time when TTL expiration occurs, or 0 if not used */
  public long getTTLExpirationTime() throws EntryNotFoundException {
    long ttl = getTTLAttributes().getTimeout();
    long tilt = 0;
    if (ttl > 0) {
      if (getLocalRegion() != null && !getLocalRegion().EXPIRY_UNITS_MS) {
        ttl *= 1000;
      }
      tilt = getLastModifiedTime() + ttl;
    }
    return tilt;
  }

  /** Return the absolute time when idle expiration occurs, or 0 if not used */
  public long getIdleExpirationTime() throws EntryNotFoundException {
    long idle = getIdleTimeoutInMillis();
    if (idle > 0) {
      return getLastAccessedTime() + idle;
    }
    return 0L;
  }

  protected long getIdleTimeoutInMillis() {
    long idle = getIdleAttributes().getTimeout();
    if (idle > 0) {
      if (getLocalRegion() != null && !getLocalRegion().EXPIRY_UNITS_MS) {
        idle *= 1000;
      }
    }
    return idle;
  }

  /**
   * Returns the number of milliseconds until this task should expire. The return value will never
   * be negative.
   */
  long getExpiryMillis() throws EntryNotFoundException {
    long extm = getExpirationTime() - getNow();
    if (extm < 0L)
      return 0L;
    else
      return extm;
  }

  /**
   * Return true if current task could have expired. Return false if expiration is impossible.
   */
  protected boolean isExpirationPossible() throws EntryNotFoundException {
    long expTime = getExpirationTime();
    if (expTime > 0L) {
      long now = getNow();
      if (now >= expTime) {
        if (isIdleExpiredOnOthers()) {
          return true;
        } else {
          // our last access time was reset so recheck
          expTime = getExpirationTime();
          if (expTime > 0L && now >= expTime) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Added for GEODE-3764.
   *
   * @return true if other members last access time indicates we have expired
   */
  protected boolean isIdleExpiredOnOthers() throws EntryNotFoundException {
    // by default return true since we don't need to check with others
    return true;
  }

  /**
   * Returns false if the region reliability state does not allow this expiry task to fire.
   */
  protected boolean isExpirationAllowed() {
    return getLocalRegion().isExpirationAllowed(this);
  }

  protected void performTimeout() throws CacheException {
    if (logger.isDebugEnabled()) {
      logger.debug("{}.performTimeout(): getExpirationTime() returns {}", this.toString(),
          getExpirationTime());
    }

    getLocalRegion().performExpiryTimeout(this);
  }

  protected abstract void basicPerformTimeout(boolean isPending) throws CacheException;

  @GuardedBy("suspendLock")
  @MakeNotStatic
  private static boolean expirationSuspended = false;

  @MakeNotStatic
  private static final Object suspendLock = new Object();

  /**
   * Test method that causes expiration to be suspended until permitExpiration is called.
   *
   * @since GemFire 5.0
   */
  public static void suspendExpiration() {
    synchronized (suspendLock) {
      expirationSuspended = true;
    }
  }

  public static void permitExpiration() {
    synchronized (suspendLock) {
      expirationSuspended = false;
      suspendLock.notifyAll();
    }
  }

  /**
   * Wait until permission is given for expiration to be done. Tests are allowed to suspend
   * expiration.
   *
   * @since GemFire 5.0
   */
  private void waitOnExpirationSuspension() {
    for (;;) {
      getLocalRegion().getCancelCriterion().checkCancelInProgress(null);
      synchronized (suspendLock) {
        boolean interrupted = Thread.interrupted();
        try {
          while (expirationSuspended) {
            suspendLock.wait();
          }
          break;
        } catch (InterruptedException ex) {
          interrupted = true;
          getLocalRegion().getCancelCriterion().checkCancelInProgress(null);
          // keep going, we can't cancel
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // synchronized
    } // for
  }

  protected boolean expire(boolean isPending) throws CacheException {
    ExpirationAction action = getAction();
    if (action == null)
      return false;
    boolean result = expire(action, isPending);
    if (result && expiryTaskListener != null) {
      expiryTaskListener.afterExpire(this);
    }
    return result;
  }

  /**
   * Why did this expire?
   *
   * @return the action to perform or null if NONE
   */
  protected ExpirationAction getAction() {
    long ttl = getTTLExpirationTime();
    long idle = getIdleExpirationTime();
    if (ttl == 0) {
      if (idle == 0)
        return null;
      return getIdleAttributes().getAction();
    }
    if (idle == 0) {
      // we know ttl != 0
      return getTTLAttributes().getAction();
    }
    // Neither is 0
    if (idle < ttl) {
      return getIdleAttributes().getAction();
    }
    return getTTLAttributes().getAction();
  }

  /** Returns true if the ExpirationAction is a distributed action. */
  protected boolean isDistributedAction() {
    ExpirationAction action = getAction();
    return action != null && (action.isInvalidate() || action.isDestroy());
  }

  LocalRegion getLocalRegion() {
    return this.region;
  }


  protected boolean expire(ExpirationAction action, boolean isPending) throws CacheException {
    if (action.isInvalidate())
      return invalidate();
    if (action.isDestroy())
      return destroy(isPending);
    if (action.isLocalInvalidate())
      return localInvalidate();
    if (action.isLocalDestroy())
      return localDestroy();
    throw new InternalGemFireError(
        String.format("unrecognized expiration action: %s", action));
  }

  /**
   * Cancel this task
   */
  @Override
  public boolean cancel() {
    boolean superCancel = super.cancel();
    LocalRegion lr = getLocalRegion();
    if (lr != null) {
      if (superCancel) {
        this.region = null; // this is the only place it is nulled
      }
    }
    return superCancel;
  }

  /**
   * An ExpiryTask is sent run() to perform its task. Note that this run() method should never throw
   * an exception - otherwise, it takes out the java.util.Timer thread, causing an exception
   * whenever we try to schedule more expiration tasks.
   */
  @Override
  public void run2() {
    try {
      if (executor != null) {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            runInThreadPool();
          }
        });
      } else {
        // inline
        runInThreadPool();
      }
    } catch (RejectedExecutionException ex) {
      try {
        if (logger.isDebugEnabled()) {
          logger.debug("Rejected execution in expiration task", ex);
        }
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        // for surviving and debugging exceptions getting the logger
        t.printStackTrace();
      }
    } catch (CancelException e) {
      return; // just bail
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable ex) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.fatal("Exception in expiration task", ex);
    }
  }

  protected void runInThreadPool() {
    try {
      if (isCacheClosing() || getLocalRegion().isClosed() || getLocalRegion().isDestroyed()) {
        return;
      }
      waitOnExpirationSuspension();
      if (logger.isTraceEnabled()) {
        logger.trace("{} is fired", this);
      }

      // do our work...
      performTimeout();
    } catch (RegionDestroyedException re) {
      // Ignore - our job is done
    } catch (EntryNotFoundException ex) {
      // Ignore
    } catch (CancelException ex) {
      // ignore
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable ex) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.fatal("Exception in expiration task", ex);
    } finally {
      if (expiryTaskListener != null) {
        expiryTaskListener.afterTaskRan(this);
      }
    }
  }

  protected boolean isCacheClosing() {
    return getLocalRegion().getCache().isClosed();
  }

  /**
   * Reschedule (or not) this task for later consideration
   */
  protected abstract void reschedule() throws CacheException;

  @Override
  public String toString() {
    String expTtl = "<unavailable>";
    String expIdle = "<unavailable>";
    try {
      if (getTTLAttributes() != null) {
        expTtl = String.valueOf(getTTLExpirationTime());
      }
      if (getIdleAttributes() != null) {
        expIdle = String.valueOf(getIdleExpirationTime());
      }
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
    }
    return super.toString() + " for " + getLocalRegion() + ", ttl expiration time: " + expTtl
        + ", idle expiration time: " + expIdle + ("[now:" + calculateNow() + "]");
  }

  ////// Abstract methods ///////

  protected abstract long getLastModifiedTime() throws EntryNotFoundException;

  protected abstract long getLastAccessedTime() throws EntryNotFoundException;

  protected abstract boolean invalidate() throws CacheException;

  protected abstract boolean destroy(boolean isPending) throws CacheException;

  protected abstract boolean localInvalidate() throws EntryNotFoundException;

  protected abstract boolean localDestroy() throws CacheException;

  protected abstract void addExpiryTask() throws EntryNotFoundException;

  public abstract boolean isPending();

  public abstract Object getKey();

  private static final ThreadLocal<Long> now = new ThreadLocal<Long>();

  /**
   * To reduce the number of times we need to call calculateNow, you can call this method to set now
   * in a thread local. When the run returns the thread local is cleared.
   */
  static void doWithNowSet(LocalRegion lr, Runnable runnable) {
    now.set(calculateNow(lr.getCache()));
    try {
      runnable.run();
    } finally {
      now.remove();
    }
  }

  /**
   * Returns the current time in milliseconds. If the current thread has set the now thread local
   * then that time is return. Otherwise now is calculated and returned.
   *
   * @return the current time in milliseconds
   */
  protected long getNow() {
    long result;
    Long tl = now.get();
    if (tl != null) {
      result = tl.longValue();
    } else {
      result = calculateNow();
    }
    return result;
  }

  public long calculateNow() {
    return calculateNow(getLocalRegion().getCache());
  }

  public static long calculateNow(InternalCache cache) {
    if (cache != null) {
      // Use cache.cacheTimeMillis here. See bug 52267.
      InternalDistributedSystem ids = cache.getInternalDistributedSystem();
      if (ids != null) {
        return ids.getClock().cacheTimeMillis();
      }
    }
    return 0L;
  }

  // Should only be set by unit tests
  @MutableForTesting
  public static ExpiryTaskListener expiryTaskListener;

  /**
   * Used by tests to determine if events related to an ExpiryTask have happened.
   */
  public interface ExpiryTaskListener {
    /**
     * Called after entry is schedule for expiration.
     */
    void afterSchedule(ExpiryTask et);

    /**
     * Called after the given expiry task has run. This means that the time it was originally
     * scheduled to run has elapsed and the scheduler has run the task. While running the task it
     * may decide to expire it or reschedule it.
     */
    void afterTaskRan(ExpiryTask et);

    /**
     * Called after the given expiry task has been rescheduled. afterTaskRan can still be called on
     * the same task. In some cases a task is rescheduled without expiring it. In others it is
     * expired and rescheduled.
     */
    void afterReschedule(ExpiryTask et);

    /**
     * Called after the given expiry task has expired.
     */
    void afterExpire(ExpiryTask et);

    /**
     * Called when task has been canceled
     */
    void afterCancel(ExpiryTask et);

  }
}
