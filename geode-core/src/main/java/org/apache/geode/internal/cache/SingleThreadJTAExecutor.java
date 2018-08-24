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

import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.internal.logging.LogService;

/**
 * TXStateSynchronizationThread manages beforeCompletion and afterCompletion calls.
 * The thread should be instantiated with a Runnable that invokes beforeCompletion behavior.
 * Then you must invoke executeAfterCompletion() with another Runnable that invokes afterCompletion
 * behavior.
 *
 * @since Geode 1.6.0
 */
public class SingleThreadJTAExecutor {
  private static final Logger logger = LogService.getLogger();

  private final Object beforeCompletionSync = new Object();
  private boolean beforeCompletionStarted;
  private boolean beforeCompletionFinished;
  private SynchronizationCommitConflictException beforeCompletionException;

  private final Object afterCompletionSync = new Object();
  private boolean afterCompletionStarted;
  private boolean afterCompletionFinished;
  private int afterCompletionStatus = -1;
  private boolean afterCompletionCancelled;
  private RuntimeException afterCompletionException;

  public SingleThreadJTAExecutor() {}

  void doOps(TXState txState) {
    doBeforeCompletionOp(txState);
    doAfterCompletionOp(txState);
  }

  void doBeforeCompletionOp(TXState txState) {
    synchronized (beforeCompletionSync) {
      try {
        txState.doBeforeCompletion();
      } catch (SynchronizationCommitConflictException exception) {
        beforeCompletionException = exception;
      } finally {
        if (logger.isDebugEnabled()) {
          logger.debug("beforeCompletion notification completed");
        }
        beforeCompletionFinished = true;
        beforeCompletionSync.notifyAll();
      }
    }
  }

  boolean isBeforeCompletionStarted() {
    synchronized (beforeCompletionSync) {
      return beforeCompletionStarted;
    }
  }

  boolean isAfterCompletionStarted() {
    synchronized (afterCompletionSync) {
      return afterCompletionStarted;
    }
  }

  boolean isBeforeCompletionFinished() {
    synchronized (beforeCompletionSync) {
      return beforeCompletionFinished;
    }
  }

  boolean isAfterCompletionFinished() {
    synchronized (afterCompletionSync) {
      return afterCompletionFinished;
    }
  }

  public void executeBeforeCompletion(TXState txState, Executor executor) {
    executor.execute(() -> doOps(txState));

    synchronized (beforeCompletionSync) {
      beforeCompletionStarted = true;
      while (!beforeCompletionFinished) {
        try {
          beforeCompletionSync.wait(1000);
        } catch (InterruptedException ignore) {
          // eat the interrupt and check for exit conditions
        }
        txState.getCache().getCancelCriterion().checkCancelInProgress(null);
      }
      if (getBeforeCompletionException() != null) {
        throw getBeforeCompletionException();
      }
    }
  }

  SynchronizationCommitConflictException getBeforeCompletionException() {
    return beforeCompletionException;
  }

  private void doAfterCompletionOp(TXState txState) {
    synchronized (afterCompletionSync) {
      // there should be a transaction timeout that keeps this thread
      // from sitting around forever if the client goes away
      // The above was done by setting afterCompletionCancelled in txState
      // during cleanup. When client departed, the transaction/JTA
      // will be timed out and cleanup code will be executed.
      final boolean isDebugEnabled = logger.isDebugEnabled();
      while (afterCompletionStatus == -1 && !afterCompletionCancelled) {
        try {
          if (isDebugEnabled) {
            logger.debug("waiting for afterCompletion notification");
          }
          afterCompletionSync.wait(1000);
        } catch (InterruptedException ignore) {
          // eat the interrupt and check for exit conditions
        }
      }
      afterCompletionStarted = true;
      if (isDebugEnabled) {
        logger.debug("executing afterCompletion notification");
      }
      try {
        if (!afterCompletionCancelled) {
          txState.doAfterCompletion(afterCompletionStatus);
        } else {
          txState.doCleanup();
        }
      } catch (RuntimeException exception) {
        afterCompletionException = exception;
      } finally {
        if (isDebugEnabled) {
          logger.debug("afterCompletion notification completed");
        }
        afterCompletionFinished = true;
        afterCompletionSync.notifyAll();
      }
    }
  }

  public void executeAfterCompletion(TXState txState, int status) {
    synchronized (afterCompletionSync) {
      afterCompletionStatus = status;
      afterCompletionSync.notifyAll();
      waitForAfterCompletionToFinish(txState);
      if (getAfterCompletionException() != null) {
        throw getAfterCompletionException();
      }
    }
  }

  private void waitForAfterCompletionToFinish(TXState txState) {
    while (!afterCompletionFinished) {
      try {
        afterCompletionSync.wait(1000);
      } catch (InterruptedException ignore) {
        // eat the interrupt and check for exit conditions
      }
      txState.getCache().getCancelCriterion().checkCancelInProgress(null);
    }
  }

  RuntimeException getAfterCompletionException() {
    return afterCompletionException;
  }

  /**
   * stop waiting for an afterCompletion to arrive and just exit
   */
  public void cleanup(TXState txState) {
    synchronized (afterCompletionSync) {
      afterCompletionCancelled = true;
      afterCompletionSync.notifyAll();
      waitForAfterCompletionToFinish(txState);
    }
  }

  public boolean shouldDoCleanup() {
    return isBeforeCompletionStarted() && !isAfterCompletionStarted();
  }
}
