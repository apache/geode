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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.internal.cache.PartitionedRegion.BucketLock;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberView;

public class BucketPersistenceAdvisorTest {

  private final PersistentMemberID persistentID = mock(PersistentMemberID.class);
  private final PersistentMemberView mockStorage = mock(PersistentMemberView.class);
  private final BucketPersistenceAdvisor bucketPersistenceAdvisor = new BucketPersistenceAdvisor(
      mock(CacheDistributionAdvisor.class), mock(DistributedLockService.class), mockStorage,
      "/region", mock(DiskRegionStats.class), mock(PersistentMemberManager.class),
      mock(BucketLock.class), mock(ProxyBucketRegion.class));

  @Test
  public void shouldBeMockable() throws Exception {
    BucketPersistenceAdvisor mockBucketAdvisor = mock(BucketPersistenceAdvisor.class);
    when(mockBucketAdvisor.isRecovering()).thenReturn(true);
    assertTrue(mockBucketAdvisor.isRecovering());
  }

  @Test
  public void atomicCreationShouldNotBeSetForPersistentRegion() throws Exception {
    when(mockStorage.getMyPersistentID()).thenReturn(persistentID);

    bucketPersistenceAdvisor.setAtomicCreation(true);
    assertFalse(bucketPersistenceAdvisor.isAtomicCreation());
  }

  @Test
  public void atomicCreationShouldNotBeSetAfterEndBucketCreation() {
    BucketPersistenceAdvisor spy = spy(bucketPersistenceAdvisor);
    spy.setAtomicCreation(true);
    doNothing().when(spy).superSetOnline(false, true, persistentID);

    spy.endBucketCreation(persistentID);

    assertFalse(spy.isAtomicCreation());
  }

  @Test
  public void atomicCreationShouldBeResetAfterEndBucketCreation() {
    BucketPersistenceAdvisor spy = spy(bucketPersistenceAdvisor);
    spy.setAtomicCreation(true);
    doThrow(new ReplyException()).when(spy).superSetOnline(false, true, persistentID);

    Throwable thrown = catchThrowable(() -> spy.endBucketCreation(persistentID));
    assertThat(thrown).isInstanceOf(ReplyException.class);
    assertTrue(spy.isAtomicCreation());
  }

}
