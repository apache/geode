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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.transaction.Status;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.cache.TransactionException;

public class SingleThreadJTAExecutorTest {
  private TXState txState;
  private SingleThreadJTAExecutor singleThreadJTAExecutor;
  private ExecutorService executor;

  @Before
  public void setup() {
    txState = mock(TXState.class, RETURNS_DEEP_STUBS);
    executor = Executors.newSingleThreadExecutor();
  }

  @Test
  public void executeBeforeCompletionCallsDoBeforeCompletion() {
    singleThreadJTAExecutor = new SingleThreadJTAExecutor();

    singleThreadJTAExecutor.executeBeforeCompletion(txState, executor);

    verify(txState, times(1)).doBeforeCompletion();

    assertThat(singleThreadJTAExecutor.isBeforeCompletionFinished()).isTrue();
  }

  @Test(expected = SynchronizationCommitConflictException.class)
  public void executeBeforeCompletionThrowsExceptionIfBeforeCompletionFailed() {
    singleThreadJTAExecutor = new SingleThreadJTAExecutor();
    doThrow(new SynchronizationCommitConflictException("")).when(txState).doBeforeCompletion();

    singleThreadJTAExecutor.executeBeforeCompletion(txState, executor);

    verify(txState, times(1)).doBeforeCompletion();
    assertThat(singleThreadJTAExecutor.isBeforeCompletionFinished()).isTrue();
  }

  @Test
  public void executeAfterCompletionCallsDoAfterCompletion() {
    singleThreadJTAExecutor = new SingleThreadJTAExecutor();
    int status = Status.STATUS_COMMITTED;

    singleThreadJTAExecutor.executeBeforeCompletion(txState, executor);
    singleThreadJTAExecutor.executeAfterCompletion(txState, status);

    verify(txState, times(1)).doBeforeCompletion();
    verify(txState, times(1)).doAfterCompletion(status);
    assertThat(singleThreadJTAExecutor.isBeforeCompletionFinished()).isTrue();
  }

  @Test
  public void executeAfterCompletionThrowsExceptionIfAfterCompletionFailed() {
    singleThreadJTAExecutor = new SingleThreadJTAExecutor();
    int status = Status.STATUS_COMMITTED;
    TransactionException exception = new TransactionException("");
    doThrow(exception).when(txState).doAfterCompletion(status);

    singleThreadJTAExecutor.executeBeforeCompletion(txState, executor);

    assertThatThrownBy(() -> singleThreadJTAExecutor.executeAfterCompletion(txState, status))
        .isSameAs(exception);
    verify(txState, times(1)).doBeforeCompletion();
    verify(txState, times(1)).doAfterCompletion(status);
  }

  @Test
  public void executorThreadNoLongerWaitForAfterCompletionIfTXStateIsCleanedUp() {
    singleThreadJTAExecutor = new SingleThreadJTAExecutor();

    singleThreadJTAExecutor.executeBeforeCompletion(txState, executor);
    singleThreadJTAExecutor.cleanup(txState);

    verify(txState, times(1)).doBeforeCompletion();
    assertThat(singleThreadJTAExecutor.isBeforeCompletionFinished()).isTrue();
    assertThat(singleThreadJTAExecutor.isAfterCompletionFinished()).isTrue();
  }

}
