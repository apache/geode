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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import javax.transaction.Status;

import org.awaitility.Awaitility;
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
  public void executeThrowsIfCancelCriterionThrows() {
    doThrow(new RuntimeException()).when(cancelCriterion).checkCancelInProgress(null);

    assertThatThrownBy(() -> afterCompletion.execute(cancelCriterion, Status.STATUS_COMMITTED))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void cancelThrowsIfCancelCriterionThrows() {
    doThrow(new RuntimeException()).when(cancelCriterion).checkCancelInProgress(null);

    assertThatThrownBy(() -> afterCompletion.cancel(cancelCriterion))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void isStartedReturnsFalseIfNotExecuted() {
    assertThat(afterCompletion.isStarted()).isFalse();
  }

  @Test
  public void isStartedReturnsTrueIfExecuted() {
    startDoOp();

    afterCompletion.execute(cancelCriterion, Status.STATUS_COMMITTED);

    verifyDoOpFinished();
    assertThat(afterCompletion.isStarted()).isTrue();
  }

  @Test
  public void executeCallsDoAfterCompletion() {
    startDoOp();

    afterCompletion.execute(cancelCriterion, Status.STATUS_COMMITTED);
    verifyDoOpFinished();
    verify(txState, times(1)).doAfterCompletion(eq(Status.STATUS_COMMITTED));
  }

  @Test
  public void executeThrowsDoAfterCompletionThrows() {
    startDoOp();
    doThrow(new RuntimeException()).when(txState).doAfterCompletion(Status.STATUS_COMMITTED);

    assertThatThrownBy(() -> afterCompletion.execute(cancelCriterion, Status.STATUS_COMMITTED))
        .isInstanceOf(RuntimeException.class);

    verifyDoOpFinished();
  }

  @Test
  public void cancelCallsDoCleanup() {
    startDoOp();

    afterCompletion.cancel(cancelCriterion);
    verifyDoOpFinished();
    verify(txState, times(1)).doCleanup();
  }

  @Test
  public void cancelThrowsDoCleanupThrows() {
    startDoOp();
    doThrow(new RuntimeException()).when(txState).doCleanup();

    assertThatThrownBy(() -> afterCompletion.cancel(cancelCriterion))
        .isInstanceOf(RuntimeException.class);

    verifyDoOpFinished();
  }

  private void startDoOp() {
    doOpThread = new Thread(() -> afterCompletion.doOp(txState, cancelCriterion));
    doOpThread.start();
    Awaitility.await().atMost(60, TimeUnit.SECONDS)
        .untilAsserted(() -> verify(cancelCriterion, times(1)).checkCancelInProgress(null));

  }

  private void verifyDoOpFinished() {
    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> !doOpThread.isAlive());
  }

}
