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
package org.apache.geode.distributed.internal.locks;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class DLockGrantorTest {
  private DLockService dLockService;
  private DLockGrantor grantor;
  private final DLockBatchId batchId = mock(DLockBatchId.class);
  private final DLockBatch batch = mock(DLockBatch.class);
  private final DLockRequestProcessor.DLockRequestMessage request = mock(
      DLockRequestProcessor.DLockRequestMessage.class);
  private final InternalDistributedMember owner = mock(InternalDistributedMember.class);

  @Before
  public void setup() {
    dLockService = mock(DLockService.class, RETURNS_DEEP_STUBS);
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(dLockService.getDistributionManager()).thenReturn(distributionManager);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(distributionManager.getCancelCriterion()).thenReturn(cancelCriterion);
    grantor = DLockGrantor.createGrantor(dLockService, 1);
  }

  @Test
  public void handleLockBatchThrowsIfRequesterHasDeparted() {
    DLockLessorDepartureHandler handler = mock(DLockLessorDepartureHandler.class);
    InternalDistributedMember requester = mock(InternalDistributedMember.class);
    DLockRequestProcessor.DLockRequestMessage requestMessage =
        mock(DLockRequestProcessor.DLockRequestMessage.class);
    when(dLockService.getDLockLessorDepartureHandler()).thenReturn(handler);
    DLockBatch lockBatch = mock(DLockBatch.class);
    when(requestMessage.getObjectName()).thenReturn(lockBatch);
    when(lockBatch.getOwner()).thenReturn(requester);

    grantor.makeReady(true);
    grantor.getLockBatches(requester);

    assertThatThrownBy(() -> grantor.handleLockBatch(requestMessage)).isInstanceOf(
        TransactionDataNodeHasDepartedException.class);
  }

  @Test
  public void recordMemberDepartedTimeRecords() {
    grantor.recordMemberDepartedTime(owner);

    assertThat(grantor.getMembersDepartedTimeRecords()).containsKey(owner);
  }

  @Test
  public void recordMemberDepartedTimeRemovesExpiredMembers() {
    DLockGrantor spy = spy(grantor);
    long currentTime = System.currentTimeMillis();
    doReturn(currentTime).doReturn(currentTime).doReturn(currentTime + 1 + DAYS.toMillis(1))
        .when(spy).getCurrentTime();

    for (int i = 0; i < 2; i++) {
      spy.recordMemberDepartedTime(mock(InternalDistributedMember.class));
    }
    assertThat(spy.getMembersDepartedTimeRecords().size()).isEqualTo(2);

    spy.recordMemberDepartedTime(owner);

    assertThat(spy.getMembersDepartedTimeRecords().size()).isEqualTo(1);
    assertThat(spy.getMembersDepartedTimeRecords()).containsKey(owner);
  }

  @Test
  public void cleanupSuspendStateIfRequestHasTimedOut() throws Exception {
    DLockGrantor spy = spy(grantor);
    spy.makeReady(true);
    when(request.checkForTimeout()).thenReturn(true);
    doNothing().when(spy).cleanupSuspendState(request);

    spy.handleLockBatch(request);

    verify(spy).cleanupSuspendState(request);
    verify(spy, never()).acquireDestroyReadLock(0);
  }

  @Test
  public void handleLockBatchThrowsIfCanNotAcquireDestroyReadLock() throws Exception {
    DLockGrantor spy = spy(grantor);
    spy.makeReady(true);
    doReturn(false).when(spy).acquireDestroyReadLock(0);
    doNothing().when(spy).waitUntilDestroyed();
    doReturn(true).when(spy).isDestroyed();

    assertThatThrownBy(() -> spy.handleLockBatch(request))
        .isInstanceOf(LockGrantorDestroyedException.class);

    verify(spy).waitUntilDestroyed();
    verify(spy).checkDestroyed();
    verify(spy, never()).releaseDestroyReadLock();
  }

  @Test
  public void handleLockBatchMakesReservation() throws Exception {
    DLockGrantor spy = spy(grantor);
    spy.makeReady(true);
    when(request.getObjectName()).thenReturn(batch);
    when(batch.getOwner()).thenReturn(owner);
    when(batch.getBatchId()).thenReturn(batchId);
    doNothing().when(spy).makeReservation(batch);

    spy.handleLockBatch(request);

    assertThat(spy.batchLocks.get(batchId)).isEqualTo(batch);
    verify(spy).checkIfHostDeparted(owner);
    verify(spy).makeReservation(batch);
    verify(spy).releaseDestroyReadLock();
    verify(request).respondWithGrant(Long.MAX_VALUE);
  }

  @Test
  public void handleLockBatchRespondWithTryLockFailedIfMakeReservationFailed() throws Exception {
    DLockGrantor spy = spy(grantor);
    spy.makeReady(true);
    when(request.getObjectName()).thenReturn(batch);
    when(batch.getOwner()).thenReturn(owner);
    when(batch.getBatchId()).thenReturn(batchId);
    String exceptionMessage = "failed";
    doThrow(new CommitConflictException(exceptionMessage)).when(spy).makeReservation(batch);

    spy.handleLockBatch(request);

    assertThat(spy.batchLocks.get(batchId)).isEqualTo(null);
    verify(spy).checkIfHostDeparted(owner);
    verify(spy).releaseDestroyReadLock();
    verify(request).respondWithTryLockFailed(exceptionMessage);
  }

  @Test
  public void getLockBatchThrowsIfCanNotAcquireDestroyReadLock() throws Exception {
    DLockGrantor spy = spy(grantor);
    spy.makeReady(true);
    doReturn(false).when(spy).acquireDestroyReadLock(0);
    doNothing().when(spy).waitUntilDestroyed();
    doReturn(true).when(spy).isDestroyed();

    assertThatThrownBy(() -> spy.getLockBatch(batchId))
        .isInstanceOf(LockGrantorDestroyedException.class);

    verify(spy).waitUntilDestroyed();
    verify(spy).checkDestroyed();
    verify(spy, never()).releaseDestroyReadLock();
  }

  @Test
  public void getLockBatchReturnsCorrectBatchLock() throws Exception {
    DLockGrantor spy = spy(grantor);
    spy.makeReady(true);
    spy.batchLocks.put(batchId, batch);

    assertThat(spy.getLockBatch(batchId)).isEqualTo(batch);

    verify(spy).releaseDestroyReadLock();
  }

  @Test
  public void updateLockBatchThrowsIfCanNotAcquireDestroyReadLock() throws Exception {
    DLockGrantor spy = spy(grantor);
    spy.makeReady(true);
    doReturn(false).when(spy).acquireDestroyReadLock(0);
    doNothing().when(spy).waitUntilDestroyed();
    doReturn(true).when(spy).isDestroyed();

    assertThatThrownBy(() -> spy.updateLockBatch(batchId, batch))
        .isInstanceOf(LockGrantorDestroyedException.class);

    verify(spy).waitUntilDestroyed();
    verify(spy).checkDestroyed();
    verify(spy, never()).releaseDestroyReadLock();
  }

  @Test
  public void updateLockBatchUpdates() throws Exception {
    DLockGrantor spy = spy(grantor);
    spy.makeReady(true);
    spy.batchLocks.put(batchId, batch);
    DLockBatch newBatch = mock(DLockBatch.class);

    spy.updateLockBatch(batchId, newBatch);

    assertThat(spy.batchLocks.get(batchId)).isEqualTo(newBatch);

    verify(spy).releaseDestroyReadLock();
  }

  @Test
  public void updateLockBatchDoesNotUpdateIfNoExistingBatch() throws Exception {
    DLockGrantor spy = spy(grantor);
    spy.makeReady(true);
    DLockBatch newBatch = mock(DLockBatch.class);

    spy.updateLockBatch(batchId, newBatch);

    assertThat(spy.batchLocks.get(batchId)).isNull();

    verify(spy).releaseDestroyReadLock();
  }


  @Test
  public void releaseLockBatchThrowsIfCanNotAcquireDestroyReadLock() throws Exception {
    DLockGrantor spy = spy(grantor);
    spy.makeReady(true);
    doReturn(false).when(spy).acquireDestroyReadLock(0);
    doNothing().when(spy).waitUntilDestroyed();
    doReturn(true).when(spy).isDestroyed();

    assertThatThrownBy(() -> spy.releaseLockBatch(batchId, owner))
        .isInstanceOf(LockGrantorDestroyedException.class);

    verify(spy).waitUntilDestroyed();
    verify(spy).checkDestroyed();
    verify(spy, never()).releaseDestroyReadLock();
  }

  @Test
  public void releaseLockBatchReleaseReservation() throws Exception {
    DLockGrantor spy = spy(grantor);
    spy.makeReady(true);
    spy.batchLocks.put(batchId, batch);
    doNothing().when(spy).releaseReservation(batch);

    spy.releaseLockBatch(batchId, null);

    assertThat(spy.batchLocks.size()).isEqualTo(0);
    verify(spy).releaseReservation(batch);
    verify(spy).releaseDestroyReadLock();
  }
}
