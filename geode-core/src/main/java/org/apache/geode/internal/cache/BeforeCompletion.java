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

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.internal.logging.LogService;

public class BeforeCompletion {
  private static final Logger logger = LogService.getLogger();

  private boolean started;
  private boolean finished;
  private RuntimeException exception;

  public synchronized void doOp(TXState txState) {
    try {
      txState.doBeforeCompletion();
    } catch (SynchronizationCommitConflictException exception) {
      this.exception = exception;
    } catch (CacheClosedException exception) {
      this.exception = new TransactionDataNodeHasDepartedException(exception);
    } catch (RuntimeException exception) {
      this.exception = new TransactionException(exception);
    } finally {
      logger.debug("beforeCompletion notification completed");
      finished = true;
      notifyAll();
    }
  }

  public synchronized void execute(CancelCriterion cancelCriterion) {
    started = true;
    waitUntilFinished(cancelCriterion);
    if (exception != null) {
      throw exception;
    }
  }

  private void waitUntilFinished(CancelCriterion cancelCriterion) {
    while (!finished) {
      cancelCriterion.checkCancelInProgress(null);
      try {
        wait(1000);
      } catch (InterruptedException ignore) {
        // eat the interrupt and check for exit conditions
      }
    }
  }

  public synchronized boolean isStarted() {
    return started;
  }

  public synchronized boolean isFinished() {
    return finished;
  }
}
