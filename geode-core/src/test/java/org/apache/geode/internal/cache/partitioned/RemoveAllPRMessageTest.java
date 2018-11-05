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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalDataView;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PrimaryBucketException;

public class RemoveAllPRMessageTest {
  private PartitionedRegion partitionedRegion;
  private PartitionedRegionDataStore dataStore;
  private BucketRegion bucketRegion;
  private Object[] keys;
  private DistributedRemoveAllOperation.RemoveAllEntryData entryData;

  private final int bucketId = 1;

  @Before
  public void setup() throws Exception {
    partitionedRegion = mock(PartitionedRegion.class, RETURNS_DEEP_STUBS);
    dataStore = mock(PartitionedRegionDataStore.class);
    bucketRegion = mock(BucketRegion.class, RETURNS_DEEP_STUBS);
    keys = new Object[] {1};
    entryData = mock(DistributedRemoveAllOperation.RemoveAllEntryData.class);

    when(partitionedRegion.getDataStore()).thenReturn(dataStore);
    when(dataStore.getInitializedBucketForId(null, bucketId)).thenReturn(bucketRegion);
    when(entryData.getEventID()).thenReturn(mock(EventID.class));
  }

  @Test
  public void shouldBeMockable() throws Exception {
    RemoveAllPRMessage mockRemoveAllPRMessage = mock(RemoveAllPRMessage.class);
    StringBuilder stringBuilder = new StringBuilder();

    mockRemoveAllPRMessage.appendFields(stringBuilder);

    verify(mockRemoveAllPRMessage, times(1)).appendFields(stringBuilder);
  }


  @Test
  public void doPostRemoveAllCallsCheckReadinessBeforeAndAfter() throws Exception {
    DistributedRemoveAllOperation distributedRemoveAllOperation =
        mock(DistributedRemoveAllOperation.class);
    InternalDataView internalDataView = mock(InternalDataView.class);
    when(bucketRegion.getDataView()).thenReturn(internalDataView);
    RemoveAllPRMessage removeAllPRMessage = new RemoveAllPRMessage();

    removeAllPRMessage.doPostRemoveAll(partitionedRegion, distributedRemoveAllOperation,
        bucketRegion, true);

    InOrder inOrder = inOrder(partitionedRegion, internalDataView);
    inOrder.verify(partitionedRegion).checkReadiness();
    inOrder.verify(internalDataView).postRemoveAll(any(), any(), any());
    inOrder.verify(partitionedRegion).checkReadiness();
  }

  @Test(expected = PrimaryBucketException.class)
  public void lockedKeysAreRemoved() throws Exception {
    RemoveAllPRMessage message = spy(new RemoveAllPRMessage(bucketId, 1, false, false, true, null));
    message.addEntry(entryData);
    doReturn(keys).when(message).getKeysToBeLocked();
    when(bucketRegion.waitUntilLocked(keys)).thenReturn(true);
    when(bucketRegion.doLockForPrimary(false)).thenThrow(new PrimaryBucketException());

    message.doLocalRemoveAll(partitionedRegion, mock(InternalDistributedMember.class), true);

    verify(bucketRegion).removeAndNotifyKeys(eq(keys));
  }

  @Test
  public void removeAndNotifyKeysIsNotInvokedIfKeysNotLocked() throws Exception {
    RemoveAllPRMessage message = spy(new RemoveAllPRMessage(bucketId, 1, false, false, true, null));
    message.addEntry(entryData);
    doReturn(keys).when(message).getKeysToBeLocked();
    RegionDestroyedException regionDestroyedException = new RegionDestroyedException("", "");
    when(bucketRegion.waitUntilLocked(keys)).thenThrow(regionDestroyedException);

    message.doLocalRemoveAll(partitionedRegion, mock(InternalDistributedMember.class), true);

    verify(bucketRegion, never()).removeAndNotifyKeys(eq(keys));
    verify(dataStore).checkRegionDestroyedOnBucket(eq(bucketRegion), eq(true),
        eq(regionDestroyedException));
  }
}
