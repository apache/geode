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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.transaction.Status;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionException;

public class TXStateTest {
  private TXStateProxyImpl txStateProxy;
  private CommitConflictException exception;
  private TransactionDataNodeHasDepartedException transactionDataNodeHasDepartedException;

  @Before
  public void setup() {
    txStateProxy = mock(TXStateProxyImpl.class);
    exception = new CommitConflictException("");
    transactionDataNodeHasDepartedException = new TransactionDataNodeHasDepartedException("");

    when(txStateProxy.getTxMgr()).thenReturn(mock(TXManagerImpl.class));
  }


  @Test
  public void beforeCompletionThrowsIfReserveAndCheckFails() {
    TXState txState = spy(new TXState(txStateProxy, true));
    doThrow(exception).when(txState).reserveAndCheck();

    assertThatThrownBy(() -> txState.beforeCompletion())
        .isInstanceOf(SynchronizationCommitConflictException.class);
  }


  @Test
  public void afterCompletionThrowsIfCommitFails() {
    TXState txState = spy(new TXState(txStateProxy, true));
    doReturn(mock(InternalCache.class)).when(txState).getCache();
    doReturn(true).when(txState).wasBeforeCompletionCalled();
    txState.reserveAndCheck();
    doThrow(transactionDataNodeHasDepartedException).when(txState).commit();

    assertThatThrownBy(() -> txState.afterCompletion(Status.STATUS_COMMITTED))
        .isSameAs(transactionDataNodeHasDepartedException);
  }

  @Test
  public void afterCompletionThrowsTransactionExceptionIfCommitFailedCommitConflictException() {
    TXState txState = spy(new TXState(txStateProxy, true));
    doReturn(mock(InternalCache.class)).when(txState).getCache();
    doReturn(true).when(txState).wasBeforeCompletionCalled();
    doThrow(exception).when(txState).commit();

    assertThatThrownBy(() -> txState.afterCompletion(Status.STATUS_COMMITTED))
        .isInstanceOf(TransactionException.class);
  }

  @Test
  public void afterCompletionCanCommitJTA() {
    TXState txState = spy(new TXState(txStateProxy, false));
    doReturn(mock(InternalCache.class)).when(txState).getCache();
    txState.reserveAndCheck();
    txState.closed = true;
    doReturn(true).when(txState).wasBeforeCompletionCalled();
    txState.afterCompletion(Status.STATUS_COMMITTED);

    assertThat(txState.locks).isNull();
    verify(txState, times(1)).saveTXCommitMessageForClientFailover();
  }

  @Test
  public void afterCompletionCanRollbackJTA() {
    TXState txState = spy(new TXState(txStateProxy, true));
    txState.afterCompletion(Status.STATUS_ROLLEDBACK);

    verify(txState, times(1)).rollback();
    verify(txState, times(1)).saveTXCommitMessageForClientFailover();
  }

}
