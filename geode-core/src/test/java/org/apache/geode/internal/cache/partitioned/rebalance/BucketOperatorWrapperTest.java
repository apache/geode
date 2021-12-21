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
package org.apache.geode.internal.cache.partitioned.rebalance;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.PartitionRebalanceDetailsImpl;
import org.apache.geode.internal.cache.control.ResourceManagerStats;
import org.apache.geode.internal.cache.partitioned.rebalance.BucketOperator.Completion;

public class BucketOperatorWrapperTest {

  private ResourceManagerStats stats;
  private PartitionedRegion leaderRegion;
  private PartitionedRegion colocatedRegion;
  private Set<PartitionRebalanceDetailsImpl> rebalanceDetails;
  private BucketOperatorWrapper wrapper;
  private BucketOperatorImpl delegate;

  private Map<String, Long> colocatedRegionBytes;
  private final int bucketId = 1;
  private InternalDistributedMember sourceMember, targetMember;

  private static final String PR_LEADER_REGION_NAME = "leadregion1";
  private static final String PR_COLOCATED_REGION_NAME = "coloregion1";

  @Before
  public void setUp() throws UnknownHostException {
    colocatedRegionBytes = new HashMap<String, Long>();
    colocatedRegionBytes.put(PR_LEADER_REGION_NAME, 100L);
    colocatedRegionBytes.put(PR_COLOCATED_REGION_NAME, 50L);

    sourceMember = new InternalDistributedMember(InetAddress.getByName("127.0.0.1"), 1);
    targetMember = new InternalDistributedMember(InetAddress.getByName("127.0.0.2"), 1);

    stats = mock(ResourceManagerStats.class);
    doNothing().when(stats).startBucketCreate(anyInt());
    doNothing().when(stats).endBucketCreate(anyInt(), anyBoolean(), anyLong(), anyLong());

    leaderRegion = mock(PartitionedRegion.class);
    doReturn(PR_LEADER_REGION_NAME).when(leaderRegion).getFullPath();
    colocatedRegion = mock(PartitionedRegion.class);
    doReturn(PR_COLOCATED_REGION_NAME).when(colocatedRegion).getFullPath();

    rebalanceDetails = new HashSet<PartitionRebalanceDetailsImpl>();
    PartitionRebalanceDetailsImpl details = spy(new PartitionRebalanceDetailsImpl(leaderRegion));
    rebalanceDetails.add(details);

    delegate = mock(BucketOperatorImpl.class);

    wrapper = new BucketOperatorWrapper(delegate, rebalanceDetails, stats, leaderRegion);
  }

  @Test
  public void bucketWrapperShouldDelegateCreateBucketToEnclosedOperator() {
    Completion completionSentToWrapper = mock(Completion.class);

    doNothing().when(delegate).createRedundantBucket(targetMember, bucketId, colocatedRegionBytes,
        completionSentToWrapper);

    wrapper.createRedundantBucket(targetMember, bucketId, colocatedRegionBytes,
        completionSentToWrapper);

    verify(delegate, times(1)).createRedundantBucket(eq(targetMember), eq(bucketId),
        eq(colocatedRegionBytes), any(Completion.class));
  }

  @Test
  public void bucketWrapperShouldRecordNumberOfBucketsCreatedIfCreateBucketSucceeds() {
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        // 3rd argument is Completion object sent to BucketOperatorImpl.createRedundantBucket
        ((Completion) invocation.getArguments()[3]).onSuccess();
        return null;
      }
    }).when(delegate).createRedundantBucket(eq(targetMember), eq(bucketId),
        eq(colocatedRegionBytes), any(Completion.class));

    Completion completionSentToWrapper = mock(Completion.class);
    wrapper.createRedundantBucket(targetMember, bucketId, colocatedRegionBytes,
        completionSentToWrapper);

    // verify create buckets is recorded
    for (PartitionRebalanceDetailsImpl details : rebalanceDetails) {
      if (details.getRegionPath().equalsIgnoreCase(PR_LEADER_REGION_NAME)) {
        verify(details, times(1)).incCreates(eq(colocatedRegionBytes.get(PR_LEADER_REGION_NAME)),
            anyLong());
      } else if (details.getRegionPath().equals(PR_COLOCATED_REGION_NAME)) {
        verify(details, times(1)).incTransfers(colocatedRegionBytes.get(PR_COLOCATED_REGION_NAME),
            0); // elapsed is recorded only if its leader
      }
    }
  }

  @Test
  public void bucketWrapperShouldNotRecordNumberOfBucketsCreatedIfCreateBucketFails() {
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        // 3rd argument is Completion object sent to BucketOperatorImpl.createRedundantBucket
        ((Completion) invocation.getArguments()[3]).onFailure();
        return null;
      }
    }).when(delegate).createRedundantBucket(eq(targetMember), eq(bucketId),
        eq(colocatedRegionBytes), any(Completion.class));

    Completion completionSentToWrapper = mock(Completion.class);
    wrapper.createRedundantBucket(targetMember, bucketId, colocatedRegionBytes,
        completionSentToWrapper);

    // verify create buckets is not recorded
    for (PartitionRebalanceDetailsImpl details : rebalanceDetails) {
      verify(details, times(0)).incTransfers(anyLong(), anyLong());
    }
  }

  @Test
  public void bucketWrapperShouldInvokeOnFailureWhenCreateBucketFails() {
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        // 3rd argument is Completion object sent to BucketOperatorImpl.createRedundantBucket
        ((Completion) invocation.getArguments()[3]).onFailure();
        return null;
      }
    }).when(delegate).createRedundantBucket(eq(targetMember), eq(bucketId),
        eq(colocatedRegionBytes), any(Completion.class));

    Completion completionSentToWrapper = mock(Completion.class);
    wrapper.createRedundantBucket(targetMember, bucketId, colocatedRegionBytes,
        completionSentToWrapper);

    // verify onFailure is invoked
    verify(completionSentToWrapper, times(1)).onFailure();
  }

  @Test
  public void bucketWrapperShouldInvokeOnSuccessWhenCreateBucketSucceeds() {
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        // 3rd argument is Completion object sent to BucketOperatorImpl.createRedundantBucket
        ((Completion) invocation.getArguments()[3]).onSuccess();
        return null;
      }
    }).when(delegate).createRedundantBucket(eq(targetMember), eq(bucketId),
        eq(colocatedRegionBytes), any(Completion.class));

    Completion completionSentToWrapper = mock(Completion.class);
    wrapper.createRedundantBucket(targetMember, bucketId, colocatedRegionBytes,
        completionSentToWrapper);

    verify(completionSentToWrapper, times(1)).onSuccess();
  }

  @Test
  public void bucketWrapperShouldDelegateMoveBucketToEnclosedOperator() {
    doReturn(true).when(delegate).moveBucket(sourceMember, targetMember, bucketId,
        colocatedRegionBytes);

    wrapper.moveBucket(sourceMember, targetMember, bucketId, colocatedRegionBytes);

    // verify the delegate is invoked
    verify(delegate, times(1)).moveBucket(sourceMember, targetMember, bucketId,
        colocatedRegionBytes);

    // verify we recorded necessary stats
    verify(stats, times(1)).startBucketTransfer(anyInt());
    verify(stats, times(1)).endBucketTransfer(anyInt(), anyBoolean(), anyLong(), anyLong());
  }

  @Test
  public void bucketWrapperShouldRecordBytesTransferredPerRegionAfterMoveBucketIsSuccessful() {
    doReturn(true).when(delegate).moveBucket(sourceMember, targetMember, bucketId,
        colocatedRegionBytes);

    wrapper.moveBucket(sourceMember, targetMember, bucketId, colocatedRegionBytes);

    // verify the details is updated with bytes transfered
    for (PartitionRebalanceDetailsImpl details : rebalanceDetails) {
      if (details.getRegionPath().equalsIgnoreCase(PR_LEADER_REGION_NAME)) {
        verify(details, times(1)).incTransfers(eq(colocatedRegionBytes.get(PR_LEADER_REGION_NAME)),
            anyLong());
      } else if (details.getRegionPath().equals(PR_COLOCATED_REGION_NAME)) {
        verify(details, times(1)).incTransfers(colocatedRegionBytes.get(PR_COLOCATED_REGION_NAME),
            0); // elapsed is recorded only if its leader
      }
    }

    // verify we recorded necessary stats
    verify(stats, times(1)).startBucketTransfer(anyInt());
    verify(stats, times(1)).endBucketTransfer(anyInt(), anyBoolean(), anyLong(), anyLong());
  }

  @Test
  public void bucketWrapperShouldDoNotRecordBytesTransferedIfMoveBucketFails() {
    doReturn(false).when(delegate).moveBucket(sourceMember, targetMember, bucketId,
        colocatedRegionBytes);

    wrapper.moveBucket(sourceMember, targetMember, bucketId, colocatedRegionBytes);

    // verify the details is not updated with bytes transfered
    for (PartitionRebalanceDetailsImpl details : rebalanceDetails) {
      verify(details, times(0)).incTransfers(anyLong(), anyLong());
    }

    // verify we recorded necessary stats
    verify(stats, times(1)).startBucketTransfer(anyInt());
    verify(stats, times(1)).endBucketTransfer(anyInt(), anyBoolean(), anyLong(), anyLong());
  }

  @Test
  public void bucketWrapperShouldDelegateRemoveBucketToEnclosedOperator() {
    wrapper.removeBucket(targetMember, bucketId, colocatedRegionBytes);

    // verify the delegate is invoked
    verify(delegate, times(1)).removeBucket(targetMember, bucketId, colocatedRegionBytes);

    // verify we recorded necessary stats
    verify(stats, times(1)).startBucketRemove(anyInt());
    verify(stats, times(1)).endBucketRemove(anyInt(), anyBoolean(), anyLong(), anyLong());
  }

  @Test
  public void bucketWrapperShouldRecordBucketRemovesPerRegionAfterRemoveBucketIsSuccessful() {
    doReturn(true).when(delegate).removeBucket(targetMember, bucketId, colocatedRegionBytes);

    wrapper.removeBucket(targetMember, bucketId, colocatedRegionBytes);

    // verify the details is updated with bytes transfered
    for (PartitionRebalanceDetailsImpl details : rebalanceDetails) {
      if (details.getRegionPath().equalsIgnoreCase(PR_LEADER_REGION_NAME)) {
        verify(details, times(1)).incRemoves((eq(colocatedRegionBytes.get(PR_LEADER_REGION_NAME))),
            anyLong());
      } else if (details.getRegionPath().equals(PR_COLOCATED_REGION_NAME)) {
        verify(details, times(1)).incRemoves(colocatedRegionBytes.get(PR_COLOCATED_REGION_NAME), 0); // elapsed
      }
      // is
      // recorded
      // only
      // if
      // its
      // leader
    }

    // verify we recorded necessary stats
    verify(stats, times(1)).startBucketRemove(anyInt());
    verify(stats, times(1)).endBucketRemove(anyInt(), anyBoolean(), anyLong(), anyLong());
  }

  @Test
  public void bucketWrapperShouldDoNotRecordBucketRemovesIfMoveBucketFails() {
    doReturn(false).when(delegate).removeBucket(targetMember, bucketId, colocatedRegionBytes);

    wrapper.removeBucket(targetMember, bucketId, colocatedRegionBytes);

    // verify the details is not updated with bytes transfered
    for (PartitionRebalanceDetailsImpl details : rebalanceDetails) {
      verify(details, times(0)).incTransfers(anyLong(), anyLong());
    }

    // verify we recorded necessary stats
    verify(stats, times(1)).startBucketRemove(anyInt());
    verify(stats, times(1)).endBucketRemove(anyInt(), anyBoolean(), anyLong(), anyLong());
  }

  @Test
  public void bucketWrapperShouldDelegateMovePrimaryToEnclosedOperator() {
    wrapper.movePrimary(sourceMember, targetMember, bucketId);

    // verify the delegate is invoked
    verify(delegate, times(1)).movePrimary(sourceMember, targetMember, bucketId);

    // verify we recorded necessary stats
    verify(stats, times(1)).startPrimaryTransfer(anyInt());
    verify(stats, times(1)).endPrimaryTransfer(anyInt(), anyBoolean(), anyLong());
  }

  @Test
  public void bucketWrapperShouldRecordPrimaryTransfersPerRegionAfterMovePrimaryIsSuccessful() {
    doReturn(true).when(delegate).movePrimary(sourceMember, targetMember, bucketId);

    wrapper.movePrimary(sourceMember, targetMember, bucketId);

    // verify the details is updated with bytes transfered
    for (PartitionRebalanceDetailsImpl details : rebalanceDetails) {
      if (details.getRegionPath().equalsIgnoreCase(PR_LEADER_REGION_NAME)) {
        verify(details, times(1)).incPrimaryTransfers(anyLong());
      } else if (details.getRegionPath().equals(PR_COLOCATED_REGION_NAME)) {
        verify(details, times(1)).incPrimaryTransfers(0); // elapsed is recorded only if its leader
      }
    }

    // verify we recorded necessary stats
    verify(stats, times(1)).startPrimaryTransfer(anyInt());
    verify(stats, times(1)).endPrimaryTransfer(anyInt(), anyBoolean(), anyLong());
  }

  @Test
  public void bucketWrapperShouldNotRecordPrimaryTransfersPerRegionAfterMovePrimaryFails() {
    doReturn(false).when(delegate).movePrimary(sourceMember, targetMember, bucketId);

    wrapper.movePrimary(sourceMember, targetMember, bucketId);

    // verify the details is not updated with bytes transfered
    for (PartitionRebalanceDetailsImpl details : rebalanceDetails) {
      verify(details, times(0)).incTransfers(anyLong(), anyLong());
    }

    // verify we recorded necessary stats
    verify(stats, times(1)).startPrimaryTransfer(anyInt());
    verify(stats, times(1)).endPrimaryTransfer(anyInt(), anyBoolean(), anyLong());
  }
}
