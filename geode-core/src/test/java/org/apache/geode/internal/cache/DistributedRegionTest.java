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
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;


public class DistributedRegionTest {
  private RegionVersionVector vector;
  private RegionVersionHolder holder;
  private VersionSource lostMemberVersionID;
  private InternalDistributedMember member;

  @Before
  public void setup() {
    vector = mock(RegionVersionVector.class);
    holder = mock(RegionVersionHolder.class);
    lostMemberVersionID = mock(VersionSource.class);
    member = mock(InternalDistributedMember.class);
  }

  @Test
  public void shouldBeMockable() throws Exception {
    DistributedRegion mockDistributedRegion = mock(DistributedRegion.class);
    EntryEventImpl mockEntryEventImpl = mock(EntryEventImpl.class);
    Object returnValue = new Object();

    when(mockDistributedRegion.validatedDestroy(anyObject(), eq(mockEntryEventImpl)))
        .thenReturn(returnValue);

    assertThat(mockDistributedRegion.validatedDestroy(new Object(), mockEntryEventImpl))
        .isSameAs(returnValue);
  }

  @Test
  public void cleanUpAfterFailedInitialImageHoldsLockForClear() {
    DistributedRegion distributedRegion = mock(DistributedRegion.class, RETURNS_DEEP_STUBS);
    RegionMap regionMap = mock(RegionMap.class);

    doCallRealMethod().when(distributedRegion).cleanUpAfterFailedGII(false);
    when(distributedRegion.getRegionMap()).thenReturn(regionMap);
    when(regionMap.isEmpty()).thenReturn(false);

    distributedRegion.cleanUpAfterFailedGII(false);

    verify(distributedRegion).lockFailedInitialImageWriteLock();
    verify(distributedRegion).closeEntries();
    verify(distributedRegion).unlockFailedInitialImageWriteLock();
  }

  @Test
  public void cleanUpAfterFailedInitialImageDoesNotCloseEntriesIfIsPersistentRegionAndRecoveredFromDisk() {
    DistributedRegion distributedRegion = mock(DistributedRegion.class);
    DiskRegion diskRegion = mock(DiskRegion.class);

    doCallRealMethod().when(distributedRegion).cleanUpAfterFailedGII(true);
    when(distributedRegion.getDiskRegion()).thenReturn(diskRegion);
    when(diskRegion.isBackup()).thenReturn(true);

    distributedRegion.cleanUpAfterFailedGII(true);

    verify(diskRegion).resetRecoveredEntries(eq(distributedRegion));
    verify(distributedRegion, never()).closeEntries();
  }

  @Test
  public void lockHeldWhenRegionIsNotInitialized() {
    DistributedRegion distributedRegion = mock(DistributedRegion.class);
    doCallRealMethod().when(distributedRegion).lockWhenRegionIsInitializing();
    when(distributedRegion.isInitialized()).thenReturn(false);

    assertThat(distributedRegion.lockWhenRegionIsInitializing()).isTrue();
    verify(distributedRegion).lockFailedInitialImageReadLock();
  }

  @Test
  public void lockNotHeldWhenRegionIsInitialized() {
    DistributedRegion distributedRegion = mock(DistributedRegion.class);
    doCallRealMethod().when(distributedRegion).lockWhenRegionIsInitializing();
    when(distributedRegion.isInitialized()).thenReturn(true);

    assertThat(distributedRegion.lockWhenRegionIsInitializing()).isFalse();
    verify(distributedRegion, never()).lockFailedInitialImageReadLock();
  }

  @Test
  public void versionHolderInvokesSetRegionSynchronizeScheduledIfVectorContainsLostMemberID() {
    DistributedRegion distributedRegion = mock(DistributedRegion.class);
    when(distributedRegion.getVersionVector()).thenReturn(vector);
    when(vector.getHolderForMember(lostMemberVersionID)).thenReturn(holder);
    doCallRealMethod().when(distributedRegion).setRegionSynchronizeScheduled(lostMemberVersionID);

    distributedRegion.setRegionSynchronizeScheduled(lostMemberVersionID);

    verify(holder).setRegionSynchronizeScheduled();
  }

  @Test
  public void versionHolderInvokesSetRegionSynchronizeScheduledOrDoneIfNotIfVectorContainsLostMemberID() {
    DistributedRegion distributedRegion = mock(DistributedRegion.class);
    when(distributedRegion.getVersionVector()).thenReturn(vector);
    when(vector.getHolderForMember(lostMemberVersionID)).thenReturn(holder);
    doCallRealMethod().when(distributedRegion)
        .setRegionSynchronizedWithIfNotScheduled(lostMemberVersionID);
    when(holder.setRegionSynchronizeScheduledOrDoneIfNot()).thenReturn(true);

    assertThat(distributedRegion.setRegionSynchronizedWithIfNotScheduled(lostMemberVersionID))
        .isTrue();

    verify(holder).setRegionSynchronizeScheduledOrDoneIfNot();
  }

  @Test
  public void setRegionSynchronizedWithIfNotScheduledReturnsFalseIfVectorDoesNotContainLostMemberID() {
    DistributedRegion distributedRegion = mock(DistributedRegion.class);
    when(distributedRegion.getVersionVector()).thenReturn(vector);
    when(vector.getHolderForMember(lostMemberVersionID)).thenReturn(holder);

    assertThat(distributedRegion.setRegionSynchronizedWithIfNotScheduled(lostMemberVersionID))
        .isFalse();

    verify(holder, never()).setRegionSynchronizeScheduledOrDoneIfNot();
  }

  @Test
  public void regionSyncInvokedInPerformSynchronizeForLostMemberTaskAfterRegionInitialized() {
    DistributedRegion distributedRegion = mock(DistributedRegion.class);
    when(distributedRegion.getDataPolicy()).thenReturn(mock(DataPolicy.class));
    when(distributedRegion.isInitializedWithWait()).thenReturn(true);
    doCallRealMethod().when(distributedRegion).performSynchronizeForLostMemberTask(member,
        lostMemberVersionID);
    InOrder inOrder = inOrder(distributedRegion);

    distributedRegion.performSynchronizeForLostMemberTask(member, lostMemberVersionID);

    inOrder.verify(distributedRegion).isInitializedWithWait();
    inOrder.verify(distributedRegion).synchronizeForLostMember(member, lostMemberVersionID);
  }

  @Test
  public void regionSyncNotInvokedInPerformSynchronizeForLostMemberTaskIfRegionNotInitialized() {
    DistributedRegion distributedRegion = mock(DistributedRegion.class);
    when(distributedRegion.getDataPolicy()).thenReturn(mock(DataPolicy.class));
    when(distributedRegion.isInitializedWithWait()).thenReturn(false);
    doCallRealMethod().when(distributedRegion).performSynchronizeForLostMemberTask(member,
        lostMemberVersionID);

    distributedRegion.performSynchronizeForLostMemberTask(member, lostMemberVersionID);

    verify(distributedRegion, never()).synchronizeForLostMember(member, lostMemberVersionID);
  }
}
