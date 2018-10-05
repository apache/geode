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

import java.util.function.BooleanSupplier;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.logging.LogService;

public class AfterCompletion {
  private static final Logger logger = LogService.getLogger();

  private boolean started;
  private boolean finished;
  private RuntimeException exception;

  private enum Action {
    COMMIT, ROLLBACK, CANCEL
  };

  private Action action;

  public synchronized void doOp(TXState txState, CancelCriterion cancelCriterion) {
    // there should be a transaction timeout that keeps this thread
    // from sitting around forever if the client goes away
    // The above was done by setting afterCompletionCancelled in txState
    // during cleanup. When client departed, the transaction/JTA
    // will be timed out and cleanup code will be executed.
    try {
      waitForExecuteOrCancel(cancelCriterion);
    } catch (RuntimeException | Error ignore) {
      action = Action.CANCEL;
    }
    started = true;
    logger.debug("executing afterCompletion notification");
    try {
      switch (action) {
        case CANCEL:
          txState.doCleanup();
          break;
        case COMMIT:
          txState.doAfterCompletionCommit();
          break;
        case ROLLBACK:
          txState.doAfterCompletionRollback();
          break;
      }
    } catch (RuntimeException exception) {
      this.exception = exception;
    } finally {
      logger.debug("afterCompletion notification completed");
      finished = true;
      notifyAll();
    }
  }

  private void waitForExecuteOrCancel(CancelCriterion cancelCriterion) {
    waitForCondition(cancelCriterion, () -> action != null);
  }

  private synchronized void waitForCondition(CancelCriterion cancelCriterion,
      BooleanSupplier condition) {
    while (!condition.getAsBoolean()) {
      if (cancelCriterion != null) {
        cancelCriterion.checkCancelInProgress(null);
      }
      try {
        logger.debug("waiting for notification");
        wait(1000);
      } catch (InterruptedException ignore) {
        // eat the interrupt and check for exit conditions
      }
    }
  }

  public void executeCommit() {
    executeAction(Action.COMMIT);
  }

  public void executeRollback() {
    executeAction(Action.ROLLBACK);
  }

  private synchronized void executeAction(Action action) {
    this.action = action;
    signalAndWaitForDoOp();
    if (exception != null) {
      throw exception;
    }

  }

  private void signalAndWaitForDoOp() {
    notifyAll();
    waitUntilFinished();
  }

  private void waitUntilFinished() {
    waitForCondition(null, () -> finished);
  }

  public synchronized void cancel() {
    action = Action.CANCEL;
    signalAndWaitForDoOp();
  }

  public synchronized boolean isStarted() {
    return started;
  }
}
