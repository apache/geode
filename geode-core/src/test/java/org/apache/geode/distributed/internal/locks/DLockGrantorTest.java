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

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
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
    when(dLockService.getDLockLessorDepartureHandler())
        .thenReturn(mock(DLockLessorDepartureHandler.class));
    grantor = spy(DLockGrantor.createGrantor(dLockService, 1));
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
    long currentTime = System.currentTimeMillis();
    doReturn(currentTime).doReturn(currentTime).doReturn(currentTime + 1 + DAYS.toMillis(1))
        .when(grantor).getCurrentTime();

    for (int i = 0; i < 2; i++) {
      grantor.recordMemberDepartedTime(mock(InternalDistributedMember.class));
    }
    assertThat(grantor.getMembersDepartedTimeRecords().size()).isEqualTo(2);

    grantor.recordMemberDepartedTime(owner);

    assertThat(grantor.getMembersDepartedTimeRecords().size()).isEqualTo(1);
    assertThat(grantor.getMembersDepartedTimeRecords()).containsKey(owner);
  }

  @Test
  public void cleanupSuspendStateIfRequestHasTimedOut() throws Exception {
    grantor.makeReady(true);
    when(request.checkForTimeout()).thenReturn(true);
    doNothing().when(grantor).cleanupSuspendState(request);

    grantor.handleLockBatch(request);

    verify(grantor).cleanupSuspendState(request);
    verify(grantor, never()).acquireDestroyReadLock(0);
  }

  @Test
  public void handleLockBatchThrowsIfCanNotAcquireDestroyReadLock() throws Exception {
    grantor.makeReady(true);
    doReturn(false).when(grantor).acquireDestroyReadLock(0);
    doNothing().when(grantor).waitUntilDestroyed();
    doReturn(true).when(grantor).isDestroyed();

    assertThatThrownBy(() -> grantor.handleLockBatch(request))
        .isInstanceOf(LockGrantorDestroyedException.class);

    verify(grantor).waitUntilDestroyed();
    verify(grantor).checkDestroyed();
    verify(grantor, never()).releaseDestroyReadLock();
  }

  @Test
  public void handleLockBatchMakesReservation() throws Exception {
    grantor.makeReady(true);
    when(request.getObjectName()).thenReturn(batch);
    when(batch.getOwner()).thenReturn(owner);
    when(batch.getBatchId()).thenReturn(batchId);
    doNothing().when(grantor).makeReservation(batch);

    grantor.handleLockBatch(request);

    assertThat(grantor.batchLocks.get(batchId)).isEqualTo(batch);
    verify(grantor).checkIfHostDeparted(owner);
    verify(grantor).makeReservation(batch);
    verify(grantor).releaseDestroyReadLock();
    verify(request).respondWithGrant(Long.MAX_VALUE);
  }

  @Test
  public void handleLockBatchRespondWithTryLockFailedIfMakeReservationFailed() throws Exception {
    grantor.makeReady(true);
    when(request.getObjectName()).thenReturn(batch);
    when(batch.getOwner()).thenReturn(owner);
    when(batch.getBatchId()).thenReturn(batchId);
    String exceptionMessage = "failed";
    doThrow(new CommitConflictException(exceptionMessage)).when(grantor).makeReservation(batch);

    grantor.handleLockBatch(request);

    assertThat(grantor.batchLocks.get(batchId)).isEqualTo(null);
    verify(grantor).checkIfHostDeparted(owner);
    verify(grantor).releaseDestroyReadLock();
    verify(request).respondWithTryLockFailed(exceptionMessage);
  }

  @Test
  public void getLockBatchThrowsIfCanNotAcquireDestroyReadLock() throws Exception {
    grantor.makeReady(true);
    doReturn(false).when(grantor).acquireDestroyReadLock(0);
    doNothing().when(grantor).waitUntilDestroyed();
    doReturn(true).when(grantor).isDestroyed();

    assertThatThrownBy(() -> grantor.getLockBatch(batchId))
        .isInstanceOf(LockGrantorDestroyedException.class);

    verify(grantor).waitUntilDestroyed();
    verify(grantor).checkDestroyed();
    verify(grantor, never()).releaseDestroyReadLock();
  }

  @Test
  public void getLockBatchReturnsCorrectBatchLock() throws Exception {
    grantor.makeReady(true);
    grantor.batchLocks.put(batchId, batch);

    assertThat(grantor.getLockBatch(batchId)).isEqualTo(batch);

    verify(grantor).releaseDestroyReadLock();
  }

  @Test
  public void updateLockBatchThrowsIfCanNotAcquireDestroyReadLock() throws Exception {
    grantor.makeReady(true);
    doReturn(false).when(grantor).acquireDestroyReadLock(0);
    doNothing().when(grantor).waitUntilDestroyed();
    doReturn(true).when(grantor).isDestroyed();

    assertThatThrownBy(() -> grantor.updateLockBatch(batchId, batch))
        .isInstanceOf(LockGrantorDestroyedException.class);

    verify(grantor).waitUntilDestroyed();
    verify(grantor).checkDestroyed();
    verify(grantor, never()).releaseDestroyReadLock();
  }

  @Test
  public void updateLockBatchUpdates() throws Exception {
    grantor.makeReady(true);
    grantor.batchLocks.put(batchId, batch);
    DLockBatch newBatch = mock(DLockBatch.class);

    grantor.updateLockBatch(batchId, newBatch);

    assertThat(grantor.batchLocks.get(batchId)).isEqualTo(newBatch);

    verify(grantor).releaseDestroyReadLock();
  }

  @Test
  public void updateLockBatchDoesNotUpdateIfNoExistingBatch() throws Exception {
    grantor.makeReady(true);
    DLockBatch newBatch = mock(DLockBatch.class);

    grantor.updateLockBatch(batchId, newBatch);

    assertThat(grantor.batchLocks.get(batchId)).isNull();

    verify(grantor).releaseDestroyReadLock();
  }


  @Test
  public void releaseLockBatchThrowsIfCanNotAcquireDestroyReadLock() throws Exception {
    grantor.makeReady(true);
    doReturn(false).when(grantor).acquireDestroyReadLock(0);
    doNothing().when(grantor).waitUntilDestroyed();
    doReturn(true).when(grantor).isDestroyed();

    assertThatThrownBy(() -> grantor.releaseLockBatch(batchId, owner))
        .isInstanceOf(LockGrantorDestroyedException.class);

    verify(grantor).waitUntilDestroyed();
    verify(grantor).checkDestroyed();
    verify(grantor, never()).releaseDestroyReadLock();
  }

  @Test
  public void releaseLockBatchReleaseReservation() throws Exception {
    grantor.makeReady(true);
    grantor.batchLocks.put(batchId, batch);
    doNothing().when(grantor).releaseReservation(batch);

    grantor.releaseLockBatch(batchId, null);

    assertThat(grantor.batchLocks.size()).isEqualTo(0);
    verify(grantor).releaseReservation(batch);
    verify(grantor).releaseDestroyReadLock();
  }
}
