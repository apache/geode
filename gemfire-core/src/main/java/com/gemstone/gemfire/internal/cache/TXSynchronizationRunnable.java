/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * TXSynchronizationThread manages beforeCompletion and afterCompletion
 * calls on behalf of a client cache.  The thread should be instantiated
 * with a Runnable that invokes beforeCompletion behavior.  Then you
 * must invoke runSecondRunnable() with another Runnable that invokes
 * afterCompletion behavior. 
 * 
 * @author Bruce Schuchardt
 * @since 6.6
 *
 */
public class TXSynchronizationRunnable implements Runnable {
  private static final Logger logger = LogService.getLogger();

  private Runnable firstRunnable;
  private final Object firstRunnableSync = new Object();
  private boolean firstRunnableCompleted;
  
  private Runnable secondRunnable;
  private final Object secondRunnableSync = new Object();
  private boolean secondRunnableCompleted;
  
  private boolean abort;

  public TXSynchronizationRunnable(Runnable beforeCompletion) {
    this.firstRunnable = beforeCompletion;
  }

  public void run() {
    synchronized(this.firstRunnableSync) {
      try {
        this.firstRunnable.run();
      } finally {
        if (logger.isTraceEnabled()) {
          logger.trace("beforeCompletion notification completed");
        }
        this.firstRunnableCompleted = true;
        this.firstRunnable = null;
        this.firstRunnableSync.notify();
      }
    }
    synchronized(this.secondRunnableSync){ 
      // TODO there should be a transaction timeout that keeps this thread
      // from sitting around forever in the event the client goes away
      final boolean isTraceEnabled = logger.isTraceEnabled();
      while (this.secondRunnable == null && !this.abort) {
        try {
          if (isTraceEnabled) {
            logger.trace("waiting for afterCompletion notification");
          }
          this.secondRunnableSync.wait(1000);
        } catch (InterruptedException e) {
          // eat the interrupt and check for exit conditions
        }
        if (this.secondRunnable == null) {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          if (cache == null || cache.getCancelCriterion().cancelInProgress() != null) {
            return;
          }
        }
      }
      if (isTraceEnabled) {
        logger.trace("executing afterCompletion notification");
      }
      try {
        if (!this.abort) {
          this.secondRunnable.run();
        }
      } finally {
        if (isTraceEnabled) {
          logger.trace("afterCompletion notification completed");
        }
        this.secondRunnableCompleted = true;
        this.secondRunnable = null;
        this.secondRunnableSync.notify();
      }
    }
  }
  
  /**
   * wait for the initial beforeCompletion step to finish
   */
  public void waitForFirstExecution() {
    synchronized(this.firstRunnableSync) {
      while(!this.firstRunnableCompleted) {
        try {
          this.firstRunnableSync.wait(1000);
        } catch (InterruptedException e) {
          // eat the interrupt and check for exit conditions
        }
        // we really need the Cache Server's cancel criterion here, not the cache's
        // but who knows how to get it?
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        if (cache == null) {
          return;
        }
        cache.getCancelCriterion().checkCancelInProgress(null);
      }
    }
  }

  /**
   * run the afterCompletion portion of synchronization.  This method
   * schedules execution of the given runnable and then waits for it to
   * finish running
   * @param r
   */
  public void runSecondRunnable(Runnable r) {
    synchronized(this.secondRunnableSync){ 
      this.secondRunnable = r;
      this.secondRunnableSync.notify();
      while(!this.secondRunnableCompleted && !this.abort) {
        try {
          this.secondRunnableSync.wait(1000);
        } catch (InterruptedException e) {
          // eat the interrupt and check for exit conditions
        }
        // we really need the Cache Server's cancel criterion here, not the cache's
        // but who knows how to get it?
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        if (cache == null) {
          return;
        }
        cache.getCancelCriterion().checkCancelInProgress(null);
      }
    }
  }
  
  /**
   * stop waiting for an afterCompletion to arrive and just exit
   */
  public void abort() {
    synchronized(this.secondRunnableSync) {
      this.abort = true;
    }
  }
}
