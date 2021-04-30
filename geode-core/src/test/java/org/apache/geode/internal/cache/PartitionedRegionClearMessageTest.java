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

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegionClearMessage.OperationType;

public class PartitionedRegionClearMessageTest {

  private Collection<InternalDistributedMember> recipients;
  private DistributionManager distributionManager;
  private PartitionedRegion partitionedRegion;
  private ReplyProcessor21 replyProcessor21;
  private Object callbackArgument;
  private EventID eventId;
  private RegionEventFactory regionEventFactory;

  @Before
  public void setUp() {
    recipients = emptySet();
    distributionManager = mock(DistributionManager.class);
    partitionedRegion = mock(PartitionedRegion.class);
    replyProcessor21 = mock(ReplyProcessor21.class);
    callbackArgument = new Object();
    eventId = mock(EventID.class);
    regionEventFactory = mock(RegionEventFactory.class);
  }

  @Test
  public void construction_throwsNullPointerExceptionIfRecipientsIsNull() {
    Throwable thrown = catchThrowable(() -> {
      new PartitionedRegionClearMessage(null, distributionManager, 1,
          replyProcessor21, OperationType.OP_PR_CLEAR, callbackArgument, eventId, false,
          regionEventFactory);
    });

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void construction_findsAllDependencies() {
    boolean isTransactionDistributed = true;
    int regionId = 10;
    InternalCache cache = mock(InternalCache.class);
    RegionEventImpl regionEvent = mock(RegionEventImpl.class);
    TXManagerImpl txManager = mock(TXManagerImpl.class);
    when(cache.getTxManager()).thenReturn(txManager);
    when(partitionedRegion.getCache()).thenReturn(cache);
    when(partitionedRegion.getDistributionManager()).thenReturn(distributionManager);
    when(partitionedRegion.getPRId()).thenReturn(regionId);
    when(regionEvent.getEventId()).thenReturn(eventId);
    when(regionEvent.getRawCallbackArgument()).thenReturn(callbackArgument);
    when(txManager.isDistributed()).thenReturn(isTransactionDistributed);

    PartitionedRegionClearMessage message = new PartitionedRegionClearMessage(recipients,
        partitionedRegion,
        replyProcessor21,
        OperationType.OP_PR_CLEAR,
        regionEvent);

    assertThat(message.getDistributionManagerForTesting()).isSameAs(distributionManager);
    assertThat(message.getCallbackArgumentForTesting()).isSameAs(callbackArgument);
    assertThat(message.getRegionId()).isEqualTo(regionId);
    assertThat(message.getEventID()).isEqualTo(eventId);
    assertThat(message.isTransactionDistributed()).isEqualTo(isTransactionDistributed);

    RegionEventFactory regionEventFactory = message.getRegionEventFactoryForTesting();
    RegionEvent<?, ?> created =
        regionEventFactory.create(partitionedRegion, Operation.DESTROY, callbackArgument, false,
            mock(DistributedMember.class), mock(EventID.class));
    assertThat(created).isInstanceOf(RegionEventImpl.class);
  }

  @Test
  public void construction_setsTransactionDistributed() {
    boolean isTransactionDistributed = true;
    PartitionedRegionClearMessage message =
        new PartitionedRegionClearMessage(recipients, distributionManager, 1,
            replyProcessor21, OperationType.OP_PR_CLEAR, callbackArgument, eventId,
            isTransactionDistributed, regionEventFactory);

    boolean value = message.isTransactionDistributed();

    assertThat(value).isEqualTo(isTransactionDistributed);
  }

  @Test
  public void getEventID_returnsTheEventId() {
    PartitionedRegionClearMessage message =
        new PartitionedRegionClearMessage(recipients, distributionManager, 1,
            replyProcessor21, OperationType.OP_PR_CLEAR, callbackArgument, eventId, false,
            regionEventFactory);

    EventID value = message.getEventID();

    assertThat(value).isSameAs(eventId);
  }

  @Test
  public void getOperationType_returnsTheOperationType() {
    PartitionedRegionClearMessage message =
        new PartitionedRegionClearMessage(recipients, distributionManager, 1,
            replyProcessor21, OperationType.OP_PR_CLEAR, callbackArgument, eventId, false,
            regionEventFactory);

    OperationType value = message.getOperationType();

    assertThat(value).isSameAs(OperationType.OP_PR_CLEAR);
  }

  @Test
  public void send_putsOutgoing() {
    PartitionedRegionClearMessage message =
        new PartitionedRegionClearMessage(recipients, distributionManager, 1,
            replyProcessor21, OperationType.OP_PR_CLEAR, callbackArgument, eventId, false,
            regionEventFactory);

    message.send();

    verify(distributionManager).putOutgoing(message);
  }

  @Test
  public void processCheckForPR_returnsForceReattemptException_whenRegionIsNotInitialized() {
    DistributionAdvisor distributionAdvisor = mock(DistributionAdvisor.class);
    when(distributionAdvisor.isInitialized()).thenReturn(false);
    when(partitionedRegion.getDistributionAdvisor()).thenReturn(distributionAdvisor);
    PartitionedRegionClearMessage message =
        new PartitionedRegionClearMessage(recipients, distributionManager, 1,
            replyProcessor21, OperationType.OP_PR_CLEAR, callbackArgument, eventId, false,
            regionEventFactory);

    Throwable throwable = message.processCheckForPR(partitionedRegion, distributionManager);

    assertThat(throwable)
        .isInstanceOf(ForceReattemptException.class)
        .hasMessageContaining("could not find partitioned region with Id");
  }

  @Test
  public void processCheckForPR_returnsNull_whenRegionIsNull() {
    PartitionedRegionClearMessage message =
        new PartitionedRegionClearMessage(recipients, distributionManager, 1,
            replyProcessor21, OperationType.OP_PR_CLEAR, callbackArgument, eventId, false,
            regionEventFactory);

    Throwable throwable = message.processCheckForPR(null, distributionManager);

    assertThat(throwable).isNull();
  }

  @Test
  public void processCheckForPR_returnsNull_whenRegionIsInitialized() {
    DistributionAdvisor distributionAdvisor = mock(DistributionAdvisor.class);
    when(distributionAdvisor.isInitialized()).thenReturn(true);
    when(partitionedRegion.getDistributionAdvisor()).thenReturn(distributionAdvisor);
    PartitionedRegionClearMessage message =
        new PartitionedRegionClearMessage(recipients, distributionManager, 1,
            replyProcessor21, OperationType.OP_PR_CLEAR, callbackArgument, eventId, false,
            regionEventFactory);

    Throwable throwable = message.processCheckForPR(null, distributionManager);

    assertThat(throwable).isNull();
  }

  @Test
  public void operateOnPartitionedRegion_returnsTrue_whenRegionIsNull() {
    ClusterDistributionManager clusterDistributionManager = mock(ClusterDistributionManager.class);
    PartitionedRegionClear partitionedRegionClear = mock(PartitionedRegionClear.class);
    when(partitionedRegion.getPartitionedRegionClear()).thenReturn(partitionedRegionClear);
    when(partitionedRegionClear.clearRegionLocal(any())).thenReturn(emptySet());
    PartitionedRegionClearMessage message =
        new PartitionedRegionClearMessage(recipients, distributionManager, 1,
            replyProcessor21, OperationType.OP_PR_CLEAR, callbackArgument, eventId, false,
            regionEventFactory);

    boolean result =
        message.operateOnPartitionedRegion(clusterDistributionManager, partitionedRegion, 30);

    assertThat(result).isTrue();
  }

  @Test
  public void operateOnPartitionedRegion_returnsTrue_whenRegionIsDestroyed() {
    ClusterDistributionManager clusterDistributionManager = mock(ClusterDistributionManager.class);
    when(partitionedRegion.isDestroyed()).thenReturn(true);
    PartitionedRegionClearMessage message =
        new PartitionedRegionClearMessage(recipients, distributionManager, 1,
            replyProcessor21, OperationType.OP_PR_CLEAR, callbackArgument, eventId, false,
            regionEventFactory);

    boolean result =
        message.operateOnPartitionedRegion(clusterDistributionManager, partitionedRegion, 30);

    assertThat(result).isTrue();
  }

  @Test
  public void operateOnPartitionedRegion_obtainsClearLockLocal_whenOperationTypeIs_OP_LOCK_FOR_PR_CLEAR() {
    ClusterDistributionManager clusterDistributionManager = mock(ClusterDistributionManager.class);
    PartitionedRegionClear partitionedRegionClear = mock(PartitionedRegionClear.class);
    when(partitionedRegion.getPartitionedRegionClear()).thenReturn(partitionedRegionClear);
    PartitionedRegionClearMessage message = new PartitionedRegionClearMessage(recipients,
        clusterDistributionManager, 1, replyProcessor21,
        OperationType.OP_LOCK_FOR_PR_CLEAR, callbackArgument, eventId, false,
        regionEventFactory);

    boolean result =
        message.operateOnPartitionedRegion(clusterDistributionManager, partitionedRegion, 30);

    assertThat(result).isTrue();
    verify(partitionedRegionClear).obtainClearLockLocal(any());
  }

  @Test
  public void operateOnPartitionedRegion_releasesClearLockLocal_whenOperationTypeIs_OP_UNLOCK_FOR_PR_CLEAR() {
    ClusterDistributionManager clusterDistributionManager = mock(ClusterDistributionManager.class);
    PartitionedRegionClear partitionedRegionClear = mock(PartitionedRegionClear.class);
    when(partitionedRegion.getPartitionedRegionClear()).thenReturn(partitionedRegionClear);
    PartitionedRegionClearMessage message = new PartitionedRegionClearMessage(recipients,
        clusterDistributionManager, 1, replyProcessor21,
        OperationType.OP_UNLOCK_FOR_PR_CLEAR, callbackArgument, eventId, false,
        regionEventFactory);

    boolean result =
        message.operateOnPartitionedRegion(clusterDistributionManager, partitionedRegion, 30);

    assertThat(result).isTrue();
    verify(partitionedRegionClear).releaseClearLockLocal();
  }

  @Test
  public void operateOnPartitionedRegion_clearsRegionLocal_whenOperationTypeIs_OP_PR_CLEAR() {
    ClusterDistributionManager clusterDistributionManager = mock(ClusterDistributionManager.class);
    PartitionedRegionClear partitionedRegionClear = mock(PartitionedRegionClear.class);
    when(partitionedRegion.getPartitionedRegionClear())
        .thenReturn(partitionedRegionClear);
    when(regionEventFactory.create(any(), any(), any(), anyBoolean(), any(), any()))
        .thenReturn(mock(RegionEventImpl.class));
    PartitionedRegionClearMessage message = new PartitionedRegionClearMessage(recipients,
        clusterDistributionManager, 1, replyProcessor21,
        OperationType.OP_PR_CLEAR, callbackArgument, eventId, false,
        regionEventFactory);

    boolean result =
        message.operateOnPartitionedRegion(clusterDistributionManager, partitionedRegion, 30);

    assertThat(result).isTrue();
    verify(partitionedRegionClear).clearRegionLocal(any(RegionEventImpl.class));
  }
}
