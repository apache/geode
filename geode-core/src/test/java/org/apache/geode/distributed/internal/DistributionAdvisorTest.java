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
package org.apache.geode.distributed.internal;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.versions.VersionSource;


public class DistributionAdvisorTest {
  private DistributionAdvisor distributionAdvisor;
  private InternalDistributedMember member;
  private DistributedRegion distributedRegion;
  private DistributionAdvisor.Profile profile;
  private VersionSource lostVersionID;
  private PersistentMemberID persistentMemberID;
  private final long delay = 100;
  private DataPolicy dataPolicy;

  @Before
  public void setup() {
    distributionAdvisor = mock(DistributionAdvisor.class);
    member = mock(InternalDistributedMember.class);
    distributedRegion = mock(DistributedRegion.class);
    profile = mock(CacheDistributionAdvisor.CacheProfile.class);
    lostVersionID = mock(VersionSource.class);
    persistentMemberID = mock(PersistentMemberID.class);
    dataPolicy = mock(DataPolicy.class);

    when(distributionAdvisor.getRegionForDeltaGII()).thenReturn(distributedRegion);
    when(distributionAdvisor.getDelay(distributedRegion)).thenReturn(delay);
    when(distributedRegion.getDataPolicy()).thenReturn(dataPolicy);
    when(distributedRegion.getConcurrencyChecksEnabled()).thenReturn(true);
    when(distributedRegion.isInitializedWithWait()).thenReturn(true);
  }

  @Test
  public void shouldBeMockable() throws Exception {
    DistributionAdvisor mockDistributionAdvisor = mock(DistributionAdvisor.class);
    mockDistributionAdvisor.initialize();
    verify(mockDistributionAdvisor, times(1)).initialize();
  }

  @Test
  public void regionSyncScheduledForLostMember() {
    when(dataPolicy.withPersistence()).thenReturn(false);
    doCallRealMethod().when(distributionAdvisor).syncForCrashedMember(member, profile);

    distributionAdvisor.syncForCrashedMember(member, profile);

    verify(distributedRegion).scheduleSynchronizeForLostMember(member, member, delay);
    verify(distributedRegion).setRegionSynchronizeScheduled(member);
  }

  @Test
  public void regionSyncScheduledForLostPersistentMember() {
    when(distributionAdvisor.getPersistentID((CacheDistributionAdvisor.CacheProfile) profile))
        .thenReturn(persistentMemberID);
    when(persistentMemberID.getVersionMember()).thenReturn(lostVersionID);
    when(dataPolicy.withPersistence()).thenReturn(true);
    doCallRealMethod().when(distributionAdvisor).syncForCrashedMember(member, profile);

    distributionAdvisor.syncForCrashedMember(member, profile);

    verify(distributedRegion).scheduleSynchronizeForLostMember(member, lostVersionID, delay);
    verify(distributedRegion).setRegionSynchronizeScheduled(lostVersionID);
  }

  @Test
  public void regionSyncNotInvokedIfLostMemberIsAnEmptyAccessorOfPersistentReplicateRegion() {
    when(dataPolicy.withPersistence()).thenReturn(true);
    when(distributedRegion.getPersistentID()).thenReturn(null);
    doCallRealMethod().when(distributionAdvisor).syncForCrashedMember(member, profile);

    distributionAdvisor.syncForCrashedMember(member, profile);

    verify(distributedRegion, never()).scheduleSynchronizeForLostMember(member, lostVersionID,
        delay);
    verify(distributedRegion, never()).setRegionSynchronizeScheduled(lostVersionID);
  }
}
