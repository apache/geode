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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class DLockGrantorTest {
  private DLockService dLockService;
  private DLockGrantor grantor;

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
    InternalDistributedMember owner = mock(InternalDistributedMember.class);
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

    InternalDistributedMember owner = mock(InternalDistributedMember.class);
    spy.recordMemberDepartedTime(owner);

    assertThat(spy.getMembersDepartedTimeRecords().size()).isEqualTo(1);
    assertThat(spy.getMembersDepartedTimeRecords()).containsKey(owner);
  }

}
