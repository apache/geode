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


import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.FailedSynchronizationException;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.SynchronizationCommitConflictException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

public class TXStateTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private TXStateProxyImpl txStateProxy;
  private CommitConflictException exception;
  private TransactionDataNodeHasDepartedException transactionDataNodeHasDepartedException;
  private SingleThreadJTAExecutor executor;
  private InternalCache cache;
  private InternalDistributedSystem internalDistributedSystem;
  private final EntryEventImpl event = mock(EntryEventImpl.class);
  private final EventID eventID = mock(EventID.class);

  @Before
  public void setup() {
    txStateProxy = mock(TXStateProxyImpl.class, RETURNS_DEEP_STUBS);
    exception = new CommitConflictException("");
    transactionDataNodeHasDepartedException = new TransactionDataNodeHasDepartedException("");
    executor = mock(SingleThreadJTAExecutor.class);
    cache = mock(InternalCache.class);
    internalDistributedSystem = mock(InternalDistributedSystem.class);

    when(txStateProxy.getTxMgr()).thenReturn(mock(TXManagerImpl.class));
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(event.getEventId()).thenReturn(eventID);
  }

  @Test
  public void doBeforeCompletionThrowsIfReserveAndCheckFails() {
    TXState txState = spy(new TXState(txStateProxy, true, disabledClock()));
    doThrow(exception).when(txState).reserveAndCheck();

    assertThatThrownBy(() -> txState.doBeforeCompletion())
        .isInstanceOf(SynchronizationCommitConflictException.class);
  }

  @Test
  public void doAfterCompletionThrowsIfCommitFails() {
    TXState txState = spy(new TXState(txStateProxy, true, disabledClock()));
    txState.reserveAndCheck();
    doThrow(transactionDataNodeHasDepartedException).when(txState).commit();

    assertThatThrownBy(() -> txState.doAfterCompletionCommit())
        .isSameAs(transactionDataNodeHasDepartedException);
  }

  @Test
  public void doAfterCompletionCanCommitJTA() {
    TXState txState = spy(new TXState(txStateProxy, false, disabledClock()));
    txState.reserveAndCheck();
    txState.closed = true;
    txState.doAfterCompletionCommit();

    assertThat(txState.locks).isNull();
    verify(txState, times(1)).saveTXCommitMessageForClientFailover();
  }

  @Test(expected = FailedSynchronizationException.class)
  public void afterCompletionThrowsExceptionIfBeforeCompletionNotCalled() {
    TXState txState = new TXState(txStateProxy, true, disabledClock());
    txState.afterCompletion(Status.STATUS_COMMITTED);
  }

  @Test
  public void afterCompletionInvokesExecuteAfterCompletionCommitIfBeforeCompletionCalled() {
    TXState txState = spy(new TXState(txStateProxy, true, executor, disabledClock()));
    doReturn(true).when(txState).wasBeforeCompletionCalled();

    txState.afterCompletion(Status.STATUS_COMMITTED);

    verify(executor, times(1)).executeAfterCompletionCommit();
  }

  @Test
  public void afterCompletionThrowsWithUnexpectedStatusIfBeforeCompletionCalled() {
    TXState txState = spy(new TXState(txStateProxy, true, executor, disabledClock()));
    doReturn(true).when(txState).wasBeforeCompletionCalled();

    Throwable thrown = catchThrowable(() -> txState.afterCompletion(Status.STATUS_NO_TRANSACTION));

    assertThat(thrown).isInstanceOf(TransactionException.class);
  }

  @Test
  public void afterCompletionInvokesExecuteAfterCompletionRollbackIfBeforeCompletionCalled() {
    TXState txState = spy(new TXState(txStateProxy, true, executor, disabledClock()));
    doReturn(true).when(txState).wasBeforeCompletionCalled();

    txState.afterCompletion(Status.STATUS_ROLLEDBACK);

    verify(executor, times(1)).executeAfterCompletionRollback();
  }

  @Test
  public void afterCompletionCanRollbackJTA() {
    TXState txState = spy(new TXState(txStateProxy, true, disabledClock()));
    txState.afterCompletion(Status.STATUS_ROLLEDBACK);

    verify(txState, times(1)).rollback();
    verify(txState, times(1)).saveTXCommitMessageForClientFailover();
  }

  @Test
  public void closeWillCleanupIfLocksObtained() {
    TXState txState = spy(new TXState(txStateProxy, false, disabledClock()));
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
    TXState txState = spy(new TXState(txStateProxy, false, disabledClock()));
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
    TXState txState = spy(new TXState(txStateProxy, false, disabledClock()));

    assertThat(txState.getOriginatingMember()).isSameAs(txStateProxy.getOnBehalfOfClientMember());
  }

  @Test
  public void txReadEntryDoesNotCleanupTXEntriesIfRegionCreateReadEntryReturnsNull() {
    TXState txState = spy(new TXState(txStateProxy, true, disabledClock()));
    KeyInfo keyInfo = mock(KeyInfo.class);
    Object key = new Object();
    InternalRegion internalRegion = mock(InternalRegion.class);
    InternalRegion dataRegion = mock(InternalRegion.class);
    TXRegionState txRegionState = mock(TXRegionState.class);
    when(internalRegion.getDataRegionForWrite(keyInfo)).thenReturn(dataRegion);
    when(txState.txReadRegion(dataRegion)).thenReturn(txRegionState);
    when(keyInfo.getKey()).thenReturn(key);
    when(txRegionState.readEntry(key)).thenReturn(null);
    when(dataRegion.createReadEntry(txRegionState, keyInfo, false)).thenReturn(null);

    assertThat(txState.txReadEntry(keyInfo, internalRegion, true, null, false)).isNull();
    verify(txRegionState, never()).cleanupNonDirtyEntries(dataRegion);
  }

  @Test
  public void txReadEntryDoesNotCleanupTXEntriesIfEntryNotFound() {
    TXState txState = spy(new TXState(txStateProxy, true, disabledClock()));
    KeyInfo keyInfo = mock(KeyInfo.class);
    Object key = new Object();
    Object expectedValue = new Object();
    InternalRegion internalRegion = mock(InternalRegion.class);
    InternalRegion dataRegion = mock(InternalRegion.class);
    TXRegionState txRegionState = mock(TXRegionState.class);
    TXEntryState txEntryState = mock(TXEntryState.class);
    when(internalRegion.getDataRegionForWrite(keyInfo)).thenReturn(dataRegion);
    when(internalRegion.getAttributes()).thenReturn(mock(RegionAttributes.class));
    when(internalRegion.getCache()).thenReturn(mock(InternalCache.class));
    when(txState.txReadRegion(dataRegion)).thenReturn(txRegionState);
    when(keyInfo.getKey()).thenReturn(key);
    when(txRegionState.readEntry(key)).thenReturn(txEntryState);
    when(txEntryState.getNearSidePendingValue()).thenReturn("currentVal");

    assertThatThrownBy(
        () -> txState.txReadEntry(keyInfo, internalRegion, true, expectedValue, false))
            .isInstanceOf(EntryNotFoundException.class);
    verify(txRegionState, never()).cleanupNonDirtyEntries(internalRegion);
  }

  @Test
  public void doCleanupContinuesWhenReleasingLockGotIllegalArgumentExceptionIfCacheIsClosing() {
    TXState txState = spy(new TXState(txStateProxy, false, disabledClock()));
    txState.locks = mock(TXLockRequest.class);
    doReturn(cache).when(txStateProxy).getCache();
    doThrow(new IllegalArgumentException()).when(txState.locks).cleanup(internalDistributedSystem);
    when(cache.isClosed()).thenReturn(true);
    TXRegionState regionState1 = mock(TXRegionState.class);
    InternalRegion region1 = mock(InternalRegion.class);
    txState.regions.put(region1, regionState1);

    txState.doCleanup();

    verify(regionState1).cleanup(region1);
  }

  @Test
  public void doCleanupContinuesWhenReleasingLockGotIllegalMonitorStateExceptionIfCacheIsClosing() {
    TXState txState = spy(new TXState(txStateProxy, false, disabledClock()));
    txState.locks = mock(TXLockRequest.class);
    doReturn(cache).when(txStateProxy).getCache();
    doThrow(new IllegalMonitorStateException()).when(txState.locks)
        .cleanup(internalDistributedSystem);
    when(cache.isClosed()).thenReturn(true);
    TXRegionState regionState1 = mock(TXRegionState.class);
    InternalRegion region1 = mock(InternalRegion.class);
    txState.regions.put(region1, regionState1);

    txState.doCleanup();

    verify(regionState1).cleanup(region1);
  }

  @Test
  public void doCleanupThrowsWhenReleasingLockGotIllegalArgumentExceptionIfCacheIsNotClosing() {
    TXState txState = spy(new TXState(txStateProxy, false, disabledClock()));
    txState.locks = mock(TXLockRequest.class);
    doReturn(cache).when(txStateProxy).getCache();
    doThrow(new IllegalArgumentException()).when(txState.locks).cleanup(internalDistributedSystem);
    when(cache.isClosed()).thenReturn(false);
    TXRegionState regionState1 = mock(TXRegionState.class);
    InternalRegion region1 = mock(InternalRegion.class);
    txState.regions.put(region1, regionState1);

    Throwable thrown = catchThrowable(() -> txState.doCleanup());

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
    verify(regionState1).cleanup(region1);
  }

  @Test
  public void doCleanupThrowsWhenReleasingLockGotIllegalMonitorStateExceptionIfCacheIsNotClosing() {
    TXState txState = spy(new TXState(txStateProxy, false, disabledClock()));
    txState.locks = mock(TXLockRequest.class);
    doReturn(cache).when(txStateProxy).getCache();
    doThrow(new IllegalMonitorStateException()).when(txState.locks)
        .cleanup(internalDistributedSystem);
    when(cache.isClosed()).thenReturn(false);
    TXRegionState regionState1 = mock(TXRegionState.class);
    InternalRegion region1 = mock(InternalRegion.class);
    txState.regions.put(region1, regionState1);

    Throwable thrown = catchThrowable(() -> txState.doCleanup());

    assertThat(thrown).isInstanceOf(IllegalMonitorStateException.class);
    verify(regionState1).cleanup(region1);
  }

  @Test
  public void getRecordedResultsReturnsFalseIfRecordedFalse() {
    TXState txState = spy(new TXState(txStateProxy, true, disabledClock()));
    txState.recordEventAndResult(event, false);

    assertThat(txState.getRecordedResult(event)).isFalse();
  }

  @Test
  public void getRecordedResultsReturnsTrueIfRecordedTrue() {
    TXState txState = spy(new TXState(txStateProxy, true, disabledClock()));
    txState.recordEventAndResult(event, true);

    assertThat(txState.getRecordedResult(event)).isTrue();
  }

  @Test
  public void getRecordedResultOrExceptionThrowsIfRecordedException() {
    expectedException.expect(TransactionDataRebalancedException.class);
    TXState txState = spy(new TXState(txStateProxy, true, disabledClock()));
    txState.recordEventAndResult(event, false);
    txState.recordEventException(event, new TransactionDataRebalancedException(""));

    txState.getRecordedResultOrException(event);
  }

  @Test
  public void getRecordedResultOrExceptionReturnFalseIfRecordedFalse() {
    TXState txState = spy(new TXState(txStateProxy, true, disabledClock()));
    txState.recordEventAndResult(event, false);

    assertThat(txState.getRecordedResultOrException(event)).isFalse();
  }

  @Test
  public void getRecordedResultOrExceptionReturnTrueIfRecordedTrue() {
    TXState txState = spy(new TXState(txStateProxy, true, disabledClock()));
    txState.recordEventAndResult(event, true);

    assertThat(txState.getRecordedResultOrException(event)).isTrue();
  }

  @Test
  public void txPutEntryRecordExceptionIfFailedWithTransactionDataRebalancedException() {
    expectedException.expect(TransactionDataRebalancedException.class);
    TXState txState = spy(new TXState(txStateProxy, true, disabledClock()));
    boolean ifNew = true;
    boolean requireOldValue = false;
    boolean checkResources = false;
    TransactionDataRebalancedException exception = new TransactionDataRebalancedException("");
    InternalRegion region = mock(InternalRegion.class);
    when(event.getRegion()).thenReturn(region);
    doThrow(exception).when(txState).txWriteEntry(region, event, ifNew, requireOldValue, null);

    assertThat(txState.txPutEntry(event, ifNew, requireOldValue, checkResources, null)).isFalse();

    verify(txState, never()).getRecordedResultOrException(event);
    verify(txState).recordEventException(event, exception);
  }
}
