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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;

public class AfterCompletionTest {
  private AfterCompletion afterCompletion;
  private CancelCriterion cancelCriterion;
  private TXState txState;
  private Thread doOpThread;

  @Before
  public void setup() {
    afterCompletion = new AfterCompletion();
    cancelCriterion = mock(CancelCriterion.class);
    txState = mock(TXState.class);
  }

  @Test
  public void isStartedReturnsFalseIfNotExecuted() {
    assertThat(afterCompletion.isStarted()).isFalse();
  }

  @Test
  public void isStartedReturnsTrueIfExecuted() {
    startDoOp();

    afterCompletion.executeCommit();

    verifyDoOpFinished();
    assertThat(afterCompletion.isStarted()).isTrue();
  }

  @Test
  public void executeCallsDoAfterCompletionCommit() {
    startDoOp();

    afterCompletion.executeCommit();
    verifyDoOpFinished();
    verify(txState).doAfterCompletionCommit();
  }

  @Test
  public void executeThrowsDoAfterCompletionCommitThrows() {
    startDoOp();
    doThrow(new RuntimeException()).when(txState).doAfterCompletionCommit();

    assertThatThrownBy(() -> afterCompletion.executeCommit())
        .isInstanceOf(RuntimeException.class);

    verifyDoOpFinished();
  }

  @Test
  public void executeCallsDoAfterCompletionRollback() {
    startDoOp();

    afterCompletion.executeRollback();
    verifyDoOpFinished();
    verify(txState).doAfterCompletionRollback();
  }

  @Test
  public void executeThrowsDoAfterCompletionRollbackThrows() {
    startDoOp();
    doThrow(new RuntimeException()).when(txState).doAfterCompletionRollback();

    assertThatThrownBy(() -> afterCompletion.executeRollback())
        .isInstanceOf(RuntimeException.class);

    verifyDoOpFinished();
  }

  @Test
  public void doOpInvokesDoCleanupIfCancelCriteriaThrows() {
    doThrow(new RuntimeException()).when(cancelCriterion).checkCancelInProgress(null);

    afterCompletion.doOp(txState, cancelCriterion);

    verify(txState).doCleanup();
  }

  @Test
  public void cancelCallsDoCleanup() {
    startDoOp();

    afterCompletion.cancel();
    verifyDoOpFinished();
    verify(txState).doCleanup();
  }

  private void startDoOp() {
    doOpThread = new Thread(() -> afterCompletion.doOp(txState, cancelCriterion));
    doOpThread.start();
    await()
        .untilAsserted(() -> verify(cancelCriterion, atLeastOnce()).checkCancelInProgress(null));

  }

  private void verifyDoOpFinished() {
    await().until(() -> !doOpThread.isAlive());
  }

}
