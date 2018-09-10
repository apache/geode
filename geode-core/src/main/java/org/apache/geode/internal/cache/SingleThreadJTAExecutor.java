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

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.logging.LogService;

/**
 * This class ensures that beforeCompletion and afterCompletion are executed in the same thread.
 *
 * @since Geode 1.7.0
 */
public class SingleThreadJTAExecutor {
  private static final Logger logger = LogService.getLogger();

  private final BeforeCompletion beforeCompletion;
  private final AfterCompletion afterCompletion;

  public SingleThreadJTAExecutor() {
    this(new BeforeCompletion(), new AfterCompletion());
  }

  public SingleThreadJTAExecutor(BeforeCompletion beforeCompletion,
      AfterCompletion afterCompletion) {
    this.beforeCompletion = beforeCompletion;
    this.afterCompletion = afterCompletion;
  }

  void doOps(TXState txState, CancelCriterion cancelCriterion) {
    try {
      beforeCompletion.doOp(txState);
    } finally {
      afterCompletion.doOp(txState, cancelCriterion);
    }
  }

  public void executeBeforeCompletion(TXState txState, Executor executor,
      CancelCriterion cancelCriterion) {
    executor.execute(() -> doOps(txState, cancelCriterion));

    beforeCompletion.execute(cancelCriterion);
  }

  public void executeAfterCompletionCommit() {
    afterCompletion.executeCommit();
  }

  public void executeAfterCompletionRollback() {
    afterCompletion.executeRollback();
  }

  /**
   * stop waiting for an afterCompletion to arrive and just exit
   */
  public void cleanup() {
    afterCompletion.cancel();
  }

  public boolean shouldDoCleanup() {
    return beforeCompletion.isFinished() && !afterCompletion.isStarted();
  }
}
