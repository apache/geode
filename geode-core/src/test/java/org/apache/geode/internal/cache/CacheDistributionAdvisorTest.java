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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;


public class CacheDistributionAdvisorTest {
  DistributionAdvisor advisor;

  @Before
  public void setUp() {
    CacheDistributionAdvisee advisee = mock(CacheDistributionAdvisee.class);
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(advisee.getDistributionManager()).thenReturn(distributionManager);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(advisee.getCancelCriterion()).thenReturn(cancelCriterion);
    advisor =
        CacheDistributionAdvisor.createCacheDistributionAdvisor(advisee);
    when(advisee.getDistributionAdvisor()).thenReturn(advisor);

  }

  @Test
  public void testAdviseAllEventsOrCached() {
    CacheProfile profile = mock(CacheProfile.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(profile.getId()).thenReturn(member);
    when(profile.getInRecovery()).thenReturn(false);
    when(profile.cachedOrAllEventsWithListener()).thenReturn(true);
    when(profile.getDistributedMember()).thenReturn(member);

    advisor.putProfile(profile, true);
    Set<InternalDistributedMember> targets1 =
        ((CacheDistributionAdvisor) advisor).adviseAllEventsOrCached();

    Set<InternalDistributedMember> targets2 =
        ((CacheDistributionAdvisor) advisor).adviseAllEventsOrCached();

    Set<InternalDistributedMember> targets3 =
        ((CacheDistributionAdvisor) advisor).adviseAllEventsOrCached();

    verify(profile, times(1)).getInRecovery();
    verify(profile, times(1)).cachedOrAllEventsWithListener();

  }

  @Test
  public void testAdviseAllEventsOrCached2() {
    CacheProfile profile = mock(CacheProfile.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(profile.getId()).thenReturn(member);
    when(profile.getInRecovery()).thenReturn(false);
    when(profile.cachedOrAllEventsWithListener()).thenReturn(true);
    when(profile.getDistributedMember()).thenReturn(member);
    CacheProfile profile2 = mock(CacheProfile.class);
    InternalDistributedMember member2 = mock(InternalDistributedMember.class);
    when(profile2.getId()).thenReturn(member2);
    when(profile2.getInRecovery()).thenReturn(false);
    when(profile2.cachedOrAllEventsWithListener()).thenReturn(true);
    when(profile2.getDistributedMember()).thenReturn(member2);

    advisor.putProfile(profile, true);
    Set<InternalDistributedMember> targets1 =
        ((CacheDistributionAdvisor) advisor).adviseAllEventsOrCached();

    advisor.putProfile(profile2, true);
    Set<InternalDistributedMember> targets2 =
        ((CacheDistributionAdvisor) advisor).adviseAllEventsOrCached();

    Set<InternalDistributedMember> targets3 =
        ((CacheDistributionAdvisor) advisor).adviseAllEventsOrCached();

    verify(profile, times(2)).getInRecovery();
    verify(profile, times(2)).cachedOrAllEventsWithListener();
    verify(profile2, times(1)).getInRecovery();
    verify(profile2, times(1)).cachedOrAllEventsWithListener();

  }


  @Test
  public void testAdviseUpdate() {
    CacheProfile profile = mock(CacheProfile.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    EntryEventImpl event = mock(EntryEventImpl.class);
    when(event.hasNewValue()).thenReturn(false);
    when(event.getOperation()).thenReturn(Operation.CREATE);

    when(profile.getId()).thenReturn(member);
    when(profile.cachedOrAllEventsWithListener()).thenReturn(true);
    when(profile.getDistributedMember()).thenReturn(member);
    when(profile.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);

    advisor.putProfile(profile, true);
    Set<InternalDistributedMember> targets1 =
        ((CacheDistributionAdvisor) advisor).adviseUpdate(event);

    Set<InternalDistributedMember> targets2 =
        ((CacheDistributionAdvisor) advisor).adviseUpdate(event);

    Set<InternalDistributedMember> targets3 =
        ((CacheDistributionAdvisor) advisor).adviseUpdate(event);

    verify(profile, times(1)).getDataPolicy();
  }

  @Test
  public void testAdviseUpdate2() {
    CacheProfile profile = mock(CacheProfile.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    EntryEventImpl event = mock(EntryEventImpl.class);
    when(event.hasNewValue()).thenReturn(false);
    when(event.getOperation()).thenReturn(Operation.CREATE);

    when(profile.getId()).thenReturn(member);
    when(profile.cachedOrAllEventsWithListener()).thenReturn(true);
    when(profile.getDistributedMember()).thenReturn(member);
    when(profile.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);

    CacheProfile profile2 = mock(CacheProfile.class);
    InternalDistributedMember member2 = mock(InternalDistributedMember.class);
    when(profile2.getId()).thenReturn(member2);
    when(profile2.cachedOrAllEventsWithListener()).thenReturn(true);
    when(profile2.getDistributedMember()).thenReturn(member2);
    when(profile2.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);

    when(event.hasNewValue()).thenReturn(false);
    when(event.getOperation()).thenReturn(Operation.CREATE);

    advisor.putProfile(profile, true);
    Set<InternalDistributedMember> targets1 =
        ((CacheDistributionAdvisor) advisor).adviseUpdate(event);

    advisor.putProfile(profile2, true);
    Set<InternalDistributedMember> targets2 =
        ((CacheDistributionAdvisor) advisor).adviseUpdate(event);

    Set<InternalDistributedMember> targets3 =
        ((CacheDistributionAdvisor) advisor).adviseUpdate(event);

    verify(profile, times(2)).getDataPolicy();
    verify(profile2, times(1)).getDataPolicy();

  }


}
