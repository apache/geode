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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;



public class DistributedRegionTest {

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
}
