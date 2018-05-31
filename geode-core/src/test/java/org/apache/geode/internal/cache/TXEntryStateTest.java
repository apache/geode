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

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TXEntryStateTest {

  @Test(expected = TransactionDataRebalancedException.class)
  public void checkConflictThrowsTransactionDataRebalancedExceptionIfBucketIsMoved() {
    BucketRegion region = mock(BucketRegion.class);
    TXEntryState txEntryState = spy(new TXEntryState());

    when(region.isUsedForPartitionedRegionBucket()).thenReturn(true);
    when(region.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    doThrow(new RegionDestroyedException("any", "any")).when(region).checkReadiness();

    when(txEntryState.isDirty()).thenReturn(true);
    txEntryState.checkForConflict(region, "anyKey");
  }

  @Test(expected = CommitConflictException.class)
  public void checkConflictThrowsCommitConflictExceptionIfEncounterCacheRuntimeException() {
    BucketRegion region = mock(BucketRegion.class);
    TXEntryState txEntryState = spy(new TXEntryState());

    when(region.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(region.basicGetEntry("key")).thenThrow(mock(CacheRuntimeException.class));

    when(txEntryState.isDirty()).thenReturn(true);
    txEntryState.checkForConflict(region, "key");
  }

}
