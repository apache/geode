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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PartitionedRegionException;

public class ClearPRMessageTest {

  ClearPRMessage message;
  PartitionedRegion region;
  PartitionedRegionDataStore dataStore;
  BucketRegion bucketRegion;

  @Before
  public void setup() throws ForceReattemptException {
    message = spy(new ClearPRMessage());
    region = mock(PartitionedRegion.class);
    dataStore = mock(PartitionedRegionDataStore.class);
    when(region.getDataStore()).thenReturn(dataStore);
    bucketRegion = mock(BucketRegion.class);
    when(dataStore.getInitializedBucketForId(any(), any())).thenReturn(bucketRegion);
  }

  @Test
  public void doLocalClearThrowsExceptionWhenBucketIsNotPrimaryAtFirstCheck() {
    when(bucketRegion.isPrimary()).thenReturn(false);

    assertThatThrownBy(() -> message.doLocalClear(region))
        .isInstanceOf(PartitionedRegionException.class)
        .hasMessageContaining("We're not primary!");
  }

  @Test
  public void doLocalClearThrowsExceptionWhenLockCannotBeObtained() {
    DistributedLockService mockLockService = mock(DistributedLockService.class);
    doReturn(mockLockService).when(message).getPartitionRegionLockService();

    when(mockLockService.lock(anyString(), anyLong(), anyLong())).thenReturn(false);
    when(bucketRegion.isPrimary()).thenReturn(true);

    assertThatThrownBy(() -> message.doLocalClear(region))
        .isInstanceOf(PartitionedRegionException.class)
        .hasMessageContaining("We couldn't lock!");
  }

  @Test
  public void doLocalClearThrowsExceptionWhenBucketIsNotPrimaryAfterObtainingLock() {
    DistributedLockService mockLockService = mock(DistributedLockService.class);
    doReturn(mockLockService).when(message).getPartitionRegionLockService();

    // Be primary on the first check, then be not primary on the second check
    when(bucketRegion.isPrimary()).thenReturn(true).thenReturn(false);
    when(mockLockService.lock(any(), anyLong(), anyLong())).thenReturn(true);

    assertThatThrownBy(() -> message.doLocalClear(region))
        .isInstanceOf(PartitionedRegionException.class)
        .hasMessageContaining("We're not primary!");
    // Confirm that we actually obtained and released the lock
    verify(mockLockService, times(1)).lock(any(), anyLong(), anyLong());
    verify(mockLockService, times(1)).unlock(any());
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
    // verify(bucketRegion, times(1)).cmnClearRegion(any(), any(), any());

    // Confirm that we actually obtained and released the lock
    verify(mockLockService, times(1)).lock(any(), anyLong(), anyLong());
    verify(mockLockService, times(1)).unlock(any());
  }
}
