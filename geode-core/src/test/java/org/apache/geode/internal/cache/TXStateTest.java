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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.transaction.Status;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.FailedSynchronizationException;
import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionException;

public class TXStateTest {
  private TXStateProxyImpl txStateProxy;
  private CommitConflictException exception;
  private TransactionDataNodeHasDepartedException transactionDataNodeHasDepartedException;
  private SingleThreadJTAExecutor executor;

  @Before
  public void setup() {
    txStateProxy = mock(TXStateProxyImpl.class, RETURNS_DEEP_STUBS);
    exception = new CommitConflictException("");
    transactionDataNodeHasDepartedException = new TransactionDataNodeHasDepartedException("");
    executor = mock(SingleThreadJTAExecutor.class);

    when(txStateProxy.getTxMgr()).thenReturn(mock(TXManagerImpl.class));
  }

  @Test
  public void doBeforeCompletionThrowsIfReserveAndCheckFails() {
    TXState txState = spy(new TXState(txStateProxy, true));
    doThrow(exception).when(txState).reserveAndCheck();

    assertThatThrownBy(() -> txState.doBeforeCompletion())
        .isInstanceOf(SynchronizationCommitConflictException.class);
  }

  @Test
  public void doAfterCompletionThrowsIfCommitFails() {
    TXState txState = spy(new TXState(txStateProxy, true));
    txState.reserveAndCheck();
    doThrow(transactionDataNodeHasDepartedException).when(txState).commit();

    assertThatThrownBy(() -> txState.doAfterCompletionCommit())
        .isSameAs(transactionDataNodeHasDepartedException);
  }

  @Test
  public void doAfterCompletionCanCommitJTA() {
    TXState txState = spy(new TXState(txStateProxy, false));
    txState.reserveAndCheck();
    txState.closed = true;
    txState.doAfterCompletionCommit();

    assertThat(txState.locks).isNull();
    verify(txState, times(1)).saveTXCommitMessageForClientFailover();
  }

  @Test(expected = FailedSynchronizationException.class)
  public void afterCompletionThrowsExceptionIfBeforeCompletionNotCalled() {
    TXState txState = new TXState(txStateProxy, true);
    txState.afterCompletion(Status.STATUS_COMMITTED);
  }

  @Test
  public void afterCompletionInvokesExecuteAfterCompletionCommitIfBeforeCompletionCalled() {
    TXState txState = spy(new TXState(txStateProxy, true, executor));
    doReturn(true).when(txState).wasBeforeCompletionCalled();

    txState.afterCompletion(Status.STATUS_COMMITTED);

    verify(executor, times(1)).executeAfterCompletionCommit();
  }

  @Test
  public void afterCompletionThrowsWithUnexpectedStatusIfBeforeCompletionCalled() {
    TXState txState = spy(new TXState(txStateProxy, true, executor));
    doReturn(true).when(txState).wasBeforeCompletionCalled();

    Throwable thrown = catchThrowable(() -> txState.afterCompletion(Status.STATUS_NO_TRANSACTION));

    assertThat(thrown).isInstanceOf(TransactionException.class);
  }

  @Test
  public void afterCompletionInvokesExecuteAfterCompletionRollbackIfBeforeCompletionCalled() {
    TXState txState = spy(new TXState(txStateProxy, true, executor));
    doReturn(true).when(txState).wasBeforeCompletionCalled();

    txState.afterCompletion(Status.STATUS_ROLLEDBACK);

    verify(executor, times(1)).executeAfterCompletionRollback();
  }

  @Test
  public void afterCompletionCanRollbackJTA() {
    TXState txState = spy(new TXState(txStateProxy, true));
    txState.afterCompletion(Status.STATUS_ROLLEDBACK);

    verify(txState, times(1)).rollback();
    verify(txState, times(1)).saveTXCommitMessageForClientFailover();
  }

  @Test
  public void closeWillCleanupIfLocksObtained() {
    TXState txState = spy(new TXState(txStateProxy, false));
    txState.closed = false;
    txState.locks = mock(TXLockRequest.class);
    TXRegionState regionState1 = mock(TXRegionState.class);
    TXRegionState regionState2 = mock(TXRegionState.class);
    InternalRegion region1 = mock(InternalRegion.class);
    InternalRegion region2 = mock(InternalRegion.class);
    txState.regions.put(region1, regionState1);
    txState.regions.put(region2, regionState2);
    doReturn(mock(InternalCache.class)).when(txState).getCache();

    txState.close();

    assertThat(txState.closed).isEqualTo(true);
    verify(txState, times(1)).cleanup();
    verify(regionState1, times(1)).cleanup(region1);
    verify(regionState2, times(1)).cleanup(region2);
  }

  @Test
  public void closeWillCloseTXRegionStatesIfLocksNotObtained() {
    TXState txState = spy(new TXState(txStateProxy, false));
    txState.closed = false;
    // txState.locks = mock(TXLockRequest.class);
    TXRegionState regionState1 = mock(TXRegionState.class);
    TXRegionState regionState2 = mock(TXRegionState.class);
    InternalRegion region1 = mock(InternalRegion.class);
    InternalRegion region2 = mock(InternalRegion.class);
    txState.regions.put(region1, regionState1);
    txState.regions.put(region2, regionState2);
    doReturn(mock(InternalCache.class)).when(txState).getCache();

    txState.close();

    assertThat(txState.closed).isEqualTo(true);
    verify(txState, never()).cleanup();
    verify(regionState1, times(1)).close();
    verify(regionState2, times(1)).close();
  }

  @Test
  public void getOriginatingMemberReturnsNullIfNotOriginatedFromClient() {
    TXState txState = spy(new TXState(txStateProxy, false));

    assertThat(txState.getOriginatingMember()).isSameAs(txStateProxy.getOnBehalfOfClientMember());
  }

}
