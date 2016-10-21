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

/**
 * ExpiryTask represents a timeout event for expiration
 */

import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.*;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.PooledExecutorWithDMStats;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class ExpiryTask extends SystemTimer.SystemTimerTask {

  private static final Logger logger = LogService.getLogger();

  private LocalRegion region; // no longer final so cancel can null it out see bug 37574

  private static final ThreadPoolExecutor executor;

  static {
    // default to inline expiry to fix bug 37115
    int nThreads =
        Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "EXPIRY_THREADS", 0).intValue();
    if (nThreads > 0) {
      ThreadFactory tf = new ThreadFactory() {
        private int nextId = 0;

        public Thread newThread(final Runnable command) {
          String name = "Expiration threads";
          final ThreadGroup group = LoggingThreadGroup.createThreadGroup(name);
          final Runnable r = new Runnable() {
            public void run() {
              ConnectionTable.threadWantsSharedResources();
              try {
                command.run();
              } finally {
                ConnectionTable.releaseThreadsSockets();
              }
            }
          };
          Thread thread = new Thread(group, r, "Expiry " + nextId++);
          thread.setDaemon(true);
          return thread;
        }
      };
      // LinkedBlockingQueue q = new LinkedBlockingQueue();
      SynchronousQueue q = new SynchronousQueue();
      executor = new PooledExecutorWithDMStats(q, nThreads, tf);
    } else {
      executor = null;
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
  public final long getTTLExpirationTime() throws EntryNotFoundException {
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
  public final long getIdleExpirationTime() throws EntryNotFoundException {
    long idle = getIdleAttributes().getTimeout();
    long tilt = 0;
    if (idle > 0) {
      if (getLocalRegion() != null && !getLocalRegion().EXPIRY_UNITS_MS) {
        idle *= 1000;
      }
      tilt = getLastAccessedTime() + idle;
    }
    return tilt;
  }

  /**
   * Returns the number of milliseconds until this task should expire. The return value will never
   * be negative.
   */
  final long getExpiryMillis() throws EntryNotFoundException {
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
    if (expTime > 0L && getNow() >= expTime) {
      return true;
    }
    return false;
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

  /**
   * @guarded.By suspendLock
   */
  private static boolean expirationSuspended = false;
  private static final Object suspendLock = new Object();

  /**
   * Test method that causes expiration to be suspended until permitExpiration is called.
   * 
   * @since GemFire 5.0
   */
  public final static void suspendExpiration() {
    synchronized (suspendLock) {
      expirationSuspended = true;
    }
  }

  public final static void permitExpiration() {
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
  private final void waitOnExpirationSuspension() {
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

  protected final boolean expire(boolean isPending) throws CacheException {
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

  final LocalRegion getLocalRegion() {
    return this.region;
  }


  protected final boolean expire(ExpirationAction action, boolean isPending) throws CacheException {
    if (action.isInvalidate())
      return invalidate();
    if (action.isDestroy())
      return destroy(isPending);
    if (action.isLocalInvalidate())
      return localInvalidate();
    if (action.isLocalDestroy())
      return localDestroy();
    throw new InternalGemFireError(
        LocalizedStrings.ExpiryTask_UNRECOGNIZED_EXPIRATION_ACTION_0.toLocalizedString(action));
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
  public final void run2() {
    try {
      if (executor != null) {
        executor.execute(new Runnable() {
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
      logger.fatal(
          LocalizedMessage.create(LocalizedStrings.ExpiryTask_EXCEPTION_IN_EXPIRATION_TASK), ex);
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
      logger.fatal(
          LocalizedMessage.create(LocalizedStrings.ExpiryTask_EXCEPTION_IN_EXPIRATION_TASK), ex);
    } finally {
      if (expiryTaskListener != null) {
        expiryTaskListener.afterTaskRan(this);
      }
    }
  }

  protected boolean isCacheClosing() {
    return ((GemFireCacheImpl) getLocalRegion().getCache()).isClosed();
  }

  /**
   * Reschedule (or not) this task for later consideration
   */
  abstract protected void reschedule() throws CacheException;

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
   * To reduce the number of times we need to call System.currentTimeMillis you can call this method
   * to set a thread local. Make sure and call {@link #clearNow()} in a finally block after calling
   * this method.
   */
  public static void setNow() {
    now.set(calculateNow());
  }

  private static long calculateNow() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      // Use cache.cacheTimeMillis here. See bug 52267.
      InternalDistributedSystem ids = cache.getDistributedSystem();
      if (ids != null) {
        return ids.getClock().cacheTimeMillis();
      }
    }
    return 0L;
  }

  /**
   * Call this method after a thread has called {@link #setNow()} once you are done calling code
   * that may call {@link #getNow()}.
   */
  public static void clearNow() {
    now.remove();
  }

  /**
   * Returns the current time in milliseconds. If the current thread has called {@link #setNow()}
   * then that time is return.
   * 
   * @return the current time in milliseconds
   */
  public static long getNow() {
    long result;
    Long tl = now.get();
    if (tl != null) {
      result = tl.longValue();
    } else {
      result = calculateNow();
    }
    return result;
  }

  // Should only be set by unit tests
  public static ExpiryTaskListener expiryTaskListener;

  /**
   * Used by tests to determine if events related to an ExpiryTask have happened.
   */
  public interface ExpiryTaskListener {
    /**
     * Called after entry is schedule for expiration.
     */
    public void afterSchedule(ExpiryTask et);

    /**
     * Called after the given expiry task has run. This means that the time it was originally
     * scheduled to run has elapsed and the scheduler has run the task. While running the task it
     * may decide to expire it or reschedule it.
     */
    public void afterTaskRan(ExpiryTask et);

    /**
     * Called after the given expiry task has been rescheduled. afterTaskRan can still be called on
     * the same task. In some cases a task is rescheduled without expiring it. In others it is
     * expired and rescheduled.
     */
    public void afterReschedule(ExpiryTask et);

    /**
     * Called after the given expiry task has expired.
     */
    public void afterExpire(ExpiryTask et);

    /**
     * Called when task has been canceled
     */
    public void afterCancel(ExpiryTask et);

  }
}
