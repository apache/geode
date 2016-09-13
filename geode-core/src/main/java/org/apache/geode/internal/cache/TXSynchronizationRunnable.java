/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.LogService;

/**
 * TXSynchronizationThread manages beforeCompletion and afterCompletion
 * calls on behalf of a client cache.  The thread should be instantiated
 * with a Runnable that invokes beforeCompletion behavior.  Then you
 * must invoke runSecondRunnable() with another Runnable that invokes
 * afterCompletion behavior. 
 * 
 * @since GemFire 6.6
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
          if (cache == null || cache.getCancelCriterion().isCancelInProgress()) {
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
