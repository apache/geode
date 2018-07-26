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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PartitionedRegionStats;

public class ManageBackupBucketReplyMessageTest {
  @Mock
  private ClusterDistributionManager distributionManager;
  @Mock
  private PartitionedRegion partitionedRegion;
  @Mock
  private PartitionedRegionDataStore partitionedRegionDataStore;
  @Mock
  private PartitionedRegionStats partitionedRegionStats;
  @Mock
  private InternalDistributedMember source;
  @Mock
  private InternalDistributedMember recipent;
  @Mock
  private ReplyProcessor21 processor;
  @Captor
  private ArgumentCaptor<ManageBackupBucketMessage.ManageBackupBucketReplyMessage> replyMessage;
  private final int bucketId = 15;
  private final int regionId = 2;
  private final boolean isReblance = true;
  private final boolean replaceOfflineDate = false;
  private final boolean forecCreation = true;

  @Before
  public void setup() {
    initMocks(this);

    when(partitionedRegion.getDataStore()).thenReturn(partitionedRegionDataStore);
    when(partitionedRegion.getPrStats()).thenReturn(partitionedRegionStats);
  }

  @Test
  public void sendStillInitializingReplyIfColocatedRegionsAreInitializing() {
    ManageBackupBucketMessage message = new ManageBackupBucketMessage();

    when(partitionedRegionDataStore.isPartitionedRegionReady(partitionedRegion, bucketId))
        .thenReturn(false);

    message.operateOnPartitionedRegion(distributionManager, partitionedRegion, 1);

    verify(distributionManager, times(1)).putOutgoing(replyMessage.capture());

    assertThat(replyMessage.getValue().isAcceptedBucket()).isFalse();
    assertThat(replyMessage.getValue().isNotYetInitialized()).isTrue();
  }

  @Test
  public void sendRefusalReplyIfDataStoreNotGrabbedBucket() {
    ManageBackupBucketMessage message = spy(new ManageBackupBucketMessage(recipent, regionId,
        processor, bucketId, isReblance, replaceOfflineDate, source, forecCreation));

    when(partitionedRegionDataStore.isPartitionedRegionReady(partitionedRegion, bucketId))
        .thenReturn(true);
    when(partitionedRegionDataStore.grabBucket(bucketId, source, forecCreation, replaceOfflineDate,
        isReblance, null, false)).thenReturn(PartitionedRegionDataStore.CreateBucketResult.FAILED);
    doReturn(recipent).when(message).getSender();

    message.operateOnPartitionedRegion(distributionManager, partitionedRegion, 1);

    verify(distributionManager, times(1)).putOutgoing(replyMessage.capture());

    assertThat(replyMessage.getValue().isAcceptedBucket()).isFalse();
    assertThat(replyMessage.getValue().isNotYetInitialized()).isFalse();
  }

  @Test
  public void sendAcceptanceReplyIfDataStoreGrabbedBucket() {
    ManageBackupBucketMessage message = spy(new ManageBackupBucketMessage(recipent, regionId,
        processor, bucketId, isReblance, replaceOfflineDate, source, forecCreation));

    when(partitionedRegionDataStore.isPartitionedRegionReady(partitionedRegion, bucketId))
        .thenReturn(true);
    when(partitionedRegionDataStore.grabBucket(bucketId, source, forecCreation, replaceOfflineDate,
        isReblance, null, false)).thenReturn(PartitionedRegionDataStore.CreateBucketResult.CREATED);
    doReturn(recipent).when(message).getSender();

    message.operateOnPartitionedRegion(distributionManager, partitionedRegion, 1);

    verify(distributionManager, times(1)).putOutgoing(replyMessage.capture());

    assertThat(replyMessage.getValue().isAcceptedBucket()).isTrue();
    assertThat(replyMessage.getValue().isNotYetInitialized()).isFalse();
  }

}
