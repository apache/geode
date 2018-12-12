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
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionException;

public class BeforeCompletionTest {

  private BeforeCompletion beforeCompletion;
  private CancelCriterion cancelCriterion;
  private TXState txState;

  @Before
  public void setup() {
    beforeCompletion = new BeforeCompletion();
    cancelCriterion = mock(CancelCriterion.class);
    txState = mock(TXState.class);
  }

  @Test
  public void executeThrowsExceptionIfDoOpFailedWithException() {
    doThrow(new SynchronizationCommitConflictException("")).when(txState).doBeforeCompletion();

    beforeCompletion.doOp(txState);

    assertThatThrownBy(() -> beforeCompletion.execute(cancelCriterion))
        .isInstanceOf(SynchronizationCommitConflictException.class);
  }

  @Test
  public void executeThrowsTransactionDataNodeHasDepartedExceptionIfDoOpFailedWithCacheClosedException() {
    doThrow(new CacheClosedException("")).when(txState).doBeforeCompletion();

    beforeCompletion.doOp(txState);

    assertThatThrownBy(() -> beforeCompletion.execute(cancelCriterion))
        .isInstanceOf(TransactionDataNodeHasDepartedException.class);
  }

  @Test
  public void executeThrowsTransactionExceptionIfDoOpFailedWithRuntimeException() {
    doThrow(new RuntimeException("")).when(txState).doBeforeCompletion();

    beforeCompletion.doOp(txState);

    assertThatThrownBy(() -> beforeCompletion.execute(cancelCriterion))
        .isInstanceOf(TransactionException.class);
  }


  @Test
  public void doOpCallsDoBeforeCompletion() {
    beforeCompletion.doOp(txState);

    verify(txState).doBeforeCompletion();
  }

  @Test
  public void isStartedReturnsFalseIfNotExecuted() {
    assertThat(beforeCompletion.isStarted()).isFalse();
  }

  @Test
  public void isStartedReturnsTrueIfExecuted() {
    beforeCompletion.doOp(txState);
    beforeCompletion.execute(cancelCriterion);

    assertThat(beforeCompletion.isStarted()).isTrue();
  }

  @Test
  public void executeThrowsIfCancelCriterionThrows() {
    doThrow(new RuntimeException()).when(cancelCriterion).checkCancelInProgress(null);

    assertThatThrownBy(() -> beforeCompletion.execute(cancelCriterion))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void executeWaitsUntilDoOpFinish() {
    Thread thread = new Thread(() -> beforeCompletion.execute(cancelCriterion));
    thread.start();
    // give the thread a chance to get past the "finished" check by waiting until
    // checkCancelInProgress is called
    await()
        .untilAsserted(() -> verify(cancelCriterion, atLeastOnce()).checkCancelInProgress(null));

    beforeCompletion.doOp(txState);

    await().until(() -> !(thread.isAlive()));
  }

}
