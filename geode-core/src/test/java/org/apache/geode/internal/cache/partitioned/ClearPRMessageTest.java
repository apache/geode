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
package org.apache.geode.internal.cache.partitioned;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PartitionedRegionStats;

public class ClearPRMessageTest {

  ClearPRMessage message;
  PartitionedRegion region;
  PartitionedRegionDataStore dataStore;
  BucketRegion bucketRegion;

  @Before
  public void setup() throws ForceReattemptException {
    message = spy(new ClearPRMessage());
    region = mock(PartitionedRegion.class, RETURNS_DEEP_STUBS);
    dataStore = mock(PartitionedRegionDataStore.class);
    when(region.getDataStore()).thenReturn(dataStore);
    bucketRegion = mock(BucketRegion.class);
    when(dataStore.getInitializedBucketForId(any(), any())).thenReturn(bucketRegion);
  }

  @Test
  public void doLocalClearThrowsExceptionWhenBucketIsNotPrimaryAtFirstCheck() {
    when(bucketRegion.isPrimary()).thenReturn(false);

    assertThatThrownBy(() -> message.doLocalClear(region))
        .isInstanceOf(ForceReattemptException.class)
        .hasMessageContaining(ClearPRMessage.BUCKET_NON_PRIMARY_MESSAGE);
  }

  @Test
  public void doLocalClearThrowsExceptionWhenLockCannotBeObtained() {
    DistributedLockService mockLockService = mock(DistributedLockService.class);
    doReturn(mockLockService).when(message).getPartitionRegionLockService();

    when(mockLockService.lock(anyString(), anyLong(), anyLong())).thenReturn(false);
    when(bucketRegion.isPrimary()).thenReturn(true);

    assertThatThrownBy(() -> message.doLocalClear(region))
        .isInstanceOf(ForceReattemptException.class)
        .hasMessageContaining(ClearPRMessage.BUCKET_REGION_LOCK_UNAVAILABLE_MESSAGE);
  }

  @Test
  public void doLocalClearThrowsExceptionWhenBucketIsNotPrimaryAfterObtainingLock() {
    DistributedLockService mockLockService = mock(DistributedLockService.class);
    doReturn(mockLockService).when(message).getPartitionRegionLockService();

    // Be primary on the first check, then be not primary on the second check
    when(bucketRegion.isPrimary()).thenReturn(true).thenReturn(false);
    when(mockLockService.lock(any(), anyLong(), anyLong())).thenReturn(true);

    assertThatThrownBy(() -> message.doLocalClear(region))
        .isInstanceOf(ForceReattemptException.class)
        .hasMessageContaining(ClearPRMessage.BUCKET_NON_PRIMARY_MESSAGE);
    // Confirm that we actually obtained and released the lock
    verify(mockLockService, times(1)).lock(any(), anyLong(), anyLong());
    verify(mockLockService, times(1)).unlock(any());
  }

  @Test
  public void doLocalClearThrowsForceReattemptExceptionWhenAnExceptionIsThrownDuringClearOperation() {
    DistributedLockService mockLockService = mock(DistributedLockService.class);
    doReturn(mockLockService).when(message).getPartitionRegionLockService();
    NullPointerException exception = new NullPointerException("Error encountered");
    doThrow(exception).when(bucketRegion).cmnClearRegion(any(), anyBoolean(), anyBoolean());

    // Be primary on the first check, then be not primary on the second check
    when(bucketRegion.isPrimary()).thenReturn(true);
    when(mockLockService.lock(any(), anyLong(), anyLong())).thenReturn(true);

    assertThatThrownBy(() -> message.doLocalClear(region))
        .isInstanceOf(ForceReattemptException.class)
        .hasMessageContaining(ClearPRMessage.EXCEPTION_THROWN_DURING_CLEAR_OPERATION);

    // Confirm that cmnClearRegion was called
    verify(bucketRegion, times(1)).cmnClearRegion(any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void doLocalClearInvokesCmnClearRegionWhenBucketIsPrimaryAndLockIsObtained()
      throws ForceReattemptException {
    DistributedLockService mockLockService = mock(DistributedLockService.class);
    doReturn(mockLockService).when(message).getPartitionRegionLockService();


    // Be primary on the first check, then be not primary on the second check
    when(bucketRegion.isPrimary()).thenReturn(true);
    when(mockLockService.lock(any(), anyLong(), anyLong())).thenReturn(true);
    assertThat(message.doLocalClear(region)).isTrue();

    // Confirm that cmnClearRegion was called
    verify(bucketRegion, times(1)).cmnClearRegion(any(), anyBoolean(), anyBoolean());

    // Confirm that we actually obtained and released the lock
    verify(mockLockService, times(1)).lock(any(), anyLong(), anyLong());
    verify(mockLockService, times(1)).unlock(any());
  }

  @Test
  public void initMessageSetsReplyProcessorCorrectlyWithDefinedReplyProcessor() {
    InternalDistributedMember sender = mock(InternalDistributedMember.class);

    Set<InternalDistributedMember> recipients = new HashSet<>();
    recipients.add(sender);

    ClearPRMessage.ClearResponse mockProcessor = mock(ClearPRMessage.ClearResponse.class);
    int mockProcessorId = 5;
    when(mockProcessor.getProcessorId()).thenReturn(mockProcessorId);

    message.initMessage(region, recipients, mockProcessor);

    verify(mockProcessor, times(1)).enableSevereAlertProcessing();
    assertThat(message.getProcessorId()).isEqualTo(mockProcessorId);
  }

  @Test
  public void initMessageSetsProcessorIdToZeroWithNullProcessor() {
    message.initMessage(region, null, null);

    assertThat(message.getProcessorId()).isEqualTo(0);
  }

  @Test
  public void sendThrowsExceptionIfPutOutgoingMethodReturnsNonNullSetOfFailures() {
    InternalDistributedMember recipient = mock(InternalDistributedMember.class);

    DistributionManager distributionManager = mock(DistributionManager.class);
    when(region.getDistributionManager()).thenReturn(distributionManager);

    doNothing().when(message).initMessage(any(), any(), any());
    Set<InternalDistributedMember> failures = new HashSet<>();
    failures.add(recipient);

    when(distributionManager.putOutgoing(message)).thenReturn(failures);

    assertThatThrownBy(() -> message.send(recipient, region))
        .isInstanceOf(ForceReattemptException.class)
        .hasMessageContaining("Failed sending <" + message + ">");
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void operateOnPartitionedRegionCallsSendReplyWithNoExceptionWhenDoLocalClearSucceeds()
      throws ForceReattemptException {
    ClusterDistributionManager distributionManager = mock(ClusterDistributionManager.class);
    InternalDistributedMember sender = mock(InternalDistributedMember.class);
    int processorId = 1000;
    int startTime = 0;

    doReturn(true).when(message).doLocalClear(region);
    doReturn(sender).when(message).getSender();
    doReturn(processorId).when(message).getProcessorId();

    // We don't want to deal with mocking the behavior of sendReply() in this test, so we mock it to
    // do nothing and verify later that it was called with proper input
    doNothing().when(message).sendReply(any(), anyInt(), any(), any(), any(), anyLong());

    message.operateOnPartitionedRegion(distributionManager, region, startTime);

    verify(message, times(1)).sendReply(sender, processorId, distributionManager, null, region,
        startTime);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void operateOnPartitionedRegionCallsSendReplyWithExceptionWhenDoLocalClearFailsWithException()
      throws ForceReattemptException {
    ClusterDistributionManager distributionManager = mock(ClusterDistributionManager.class);
    InternalDistributedMember sender = mock(InternalDistributedMember.class);
    int processorId = 1000;
    int startTime = 0;
    ForceReattemptException exception =
        new ForceReattemptException(ClearPRMessage.BUCKET_NON_PRIMARY_MESSAGE);

    doThrow(exception).when(message).doLocalClear(region);
    doReturn(sender).when(message).getSender();
    doReturn(processorId).when(message).getProcessorId();

    // We don't want to deal with mocking the behavior of sendReply() in this test, so we mock it to
    // do nothing and verify later that it was called with proper input
    doNothing().when(message).sendReply(any(), anyInt(), any(), any(), any(), anyLong());

    message.operateOnPartitionedRegion(distributionManager, region, startTime);

    verify(message, times(1)).sendReply(any(), anyInt(), any(), notNull(), any(), anyLong());
  }

  @Test
  public void sendReplyEndsMessageProcessingIfWeHaveARegionAndHaveStartedProcessing() {
    DistributionManager distributionManager = mock(DistributionManager.class);
    InternalDistributedMember recipient = mock(InternalDistributedMember.class);
    PartitionedRegionStats partitionedRegionStats = mock(PartitionedRegionStats.class);
    when(region.getPrStats()).thenReturn(partitionedRegionStats);

    int processorId = 1000;
    int startTime = 10000;
    ReplyException exception = new ReplyException(ClearPRMessage.BUCKET_NON_PRIMARY_MESSAGE);

    ReplySender replySender = mock(ReplySender.class);
    doReturn(replySender).when(message).getReplySender(distributionManager);

    message.sendReply(recipient, processorId, distributionManager, exception, region, startTime);

    verify(partitionedRegionStats, times(1)).endPartitionMessagesProcessing(startTime);
  }

  @Test
  public void sendReplyDoesNotEndMessageProcessingIfStartTimeIsZero() {
    DistributionManager distributionManager = mock(DistributionManager.class);
    InternalDistributedMember recipient = mock(InternalDistributedMember.class);
    PartitionedRegionStats partitionedRegionStats = mock(PartitionedRegionStats.class);
    when(region.getPrStats()).thenReturn(partitionedRegionStats);

    int processorId = 1000;
    int startTime = 0;
    ReplyException exception = new ReplyException(ClearPRMessage.BUCKET_NON_PRIMARY_MESSAGE);

    ReplySender replySender = mock(ReplySender.class);
    doReturn(replySender).when(message).getReplySender(distributionManager);

    message.sendReply(recipient, processorId, distributionManager, exception, region, startTime);

    verify(partitionedRegionStats, times(0)).endPartitionMessagesProcessing(startTime);
  }

  @Test
  public void clearReplyMessageProcessCallsSetResponseIfReplyProcessorIsInstanceOfClearResponse() {
    DistributionManager distributionManager = mock(DistributionManager.class);
    DMStats mockStats = mock(DMStats.class);
    when(distributionManager.getStats()).thenReturn(mockStats);
    ClearPRMessage.ClearReplyMessage clearReplyMessage = new ClearPRMessage.ClearReplyMessage();
    ClearPRMessage.ClearResponse mockProcessor = mock(ClearPRMessage.ClearResponse.class);

    clearReplyMessage.process(distributionManager, mockProcessor);

    verify(mockProcessor, times(1)).setResponse(clearReplyMessage);
  }
}
