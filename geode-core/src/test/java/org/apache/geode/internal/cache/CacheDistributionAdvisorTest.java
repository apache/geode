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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;

/**
 * Unit test for CacheDistributionAdvisor
 */
public class CacheDistributionAdvisorTest {

  @Test
  public void adviseSameGatewaySenderIdsReturnsInputAndNonMatchingProfileIds() {
    CacheDistributionAdvisor advisor = createCacheDistributionAdvisor();
    CacheProfile profile = new CacheProfile();
    advisor.putProfile(profile, true);
    Set<String> input = Collections.emptySet();

    List<Set<String>> result = advisor.adviseSameGatewaySenderIds(input);

    assertThat(result).isEmpty();
  }

  @Test
  public void adviseSameGatewaySenderIdsReturnsEmptySetGivenOneProfileThatMatchesInput() {
    CacheDistributionAdvisor advisor = createCacheDistributionAdvisor();
    CacheProfile profile = new CacheProfile();
    Set<String> profileIds = new HashSet<>();
    profileIds.add("profileId1");
    profile.gatewaySenderIds = profileIds;
    advisor.putProfile(profile, true);
    Set<String> input = new HashSet<>();
    input.add("inputId1");

    List<Set<String>> result = advisor.adviseSameGatewaySenderIds(input);

    assertThat(result).containsExactly(input, profileIds);
  }

  @Test
  public void adviseSameGatewaySenderIdsReturnsInputAndNonMatchingProfileIdsGivenMultipleProfiles() {
    Set<String> input = new HashSet<>();
    input.add("inputId1");
    CacheDistributionAdvisor advisor = createCacheDistributionAdvisor();
    InternalDistributedMember memberId1 = mock(InternalDistributedMember.class);
    CacheProfile profile = new CacheProfile(memberId1, 0);
    Set<String> profileIds = new HashSet<>();
    profileIds.add("profileId1");
    profile.gatewaySenderIds = profileIds;
    advisor.putProfile(profile, true);
    InternalDistributedMember memberId2 = mock(InternalDistributedMember.class);
    CacheProfile notInResult = new CacheProfile(memberId2, 0);
    notInResult.gatewaySenderIds = input;
    advisor.putProfile(notInResult, true);
    InternalDistributedMember memberId3 = mock(InternalDistributedMember.class);
    CacheProfile profile2 = new CacheProfile(memberId3, 0);
    Set<String> profileIds2 = new HashSet<>();
    profileIds.add("profileId2");
    profile2.gatewaySenderIds = profileIds2;
    advisor.putProfile(profile2, true);

    List<Set<String>> result = advisor.adviseSameGatewaySenderIds(input);

    assertThat(result).containsExactly(input, profileIds, input, profileIds2);
  }

  @Test
  public void adviseSameAsyncEventQueueIdsReturnsInputAndNonMatchingProfileIds() {
    CacheDistributionAdvisor advisor = createCacheDistributionAdvisor();
    CacheProfile profile = new CacheProfile();
    advisor.putProfile(profile, true);
    Set<String> input = Collections.emptySet();

    List<Set<String>> result = advisor.adviseSameAsyncEventQueueIds(input);

    assertThat(result).isEmpty();
  }

  @Test
  public void adviseSameAsyncEventQueueIdsReturnsEmptySetGivenOneProfileThatMatchesInput() {
    CacheDistributionAdvisor advisor = createCacheDistributionAdvisor();
    CacheProfile profile = new CacheProfile();
    Set<String> profileIds = new HashSet<>();
    profileIds.add("profileId1");
    profile.asyncEventQueueIds = profileIds;
    advisor.putProfile(profile, true);
    Set<String> input = new HashSet<>();
    input.add("inputId1");

    List<Set<String>> result = advisor.adviseSameAsyncEventQueueIds(input);

    assertThat(result).containsExactly(input, profileIds);
  }

  @Test
  public void adviseSameAsyncEventQueueIdsReturnsInputAndNonMatchingProfileIdsGivenMultipleProfiles() {
    Set<String> input = new HashSet<>();
    input.add("inputId1");
    CacheDistributionAdvisor advisor = createCacheDistributionAdvisor();
    InternalDistributedMember memberId1 = mock(InternalDistributedMember.class);
    CacheProfile profile = new CacheProfile(memberId1, 0);
    Set<String> profileIds = new HashSet<>();
    profileIds.add("profileId1");
    profile.asyncEventQueueIds = profileIds;
    advisor.putProfile(profile, true);
    InternalDistributedMember memberId2 = mock(InternalDistributedMember.class);
    CacheProfile notInResult = new CacheProfile(memberId2, 0);
    notInResult.asyncEventQueueIds = input;
    advisor.putProfile(notInResult, true);
    InternalDistributedMember memberId3 = mock(InternalDistributedMember.class);
    CacheProfile profile2 = new CacheProfile(memberId3, 0);
    Set<String> profileIds2 = new HashSet<>();
    profileIds.add("profileId2");
    profile2.asyncEventQueueIds = profileIds2;
    advisor.putProfile(profile2, true);

    List<Set<String>> result = advisor.adviseSameAsyncEventQueueIds(input);

    assertThat(result).containsExactly(input, profileIds, input, profileIds2);
  }

  private CacheDistributionAdvisor createCacheDistributionAdvisor() {
    DistributionAdvisor advisor = mock(DistributionAdvisor.class);
    CacheDistributionAdvisee advisee = mock(CacheDistributionAdvisee.class);
    when(advisee.getDistributionAdvisor()).thenReturn(advisor);
    DistributionManager distributionManager = mock(DistributionManager.class);
    when(advisee.getDistributionManager()).thenReturn(distributionManager);
    return CacheDistributionAdvisor.createCacheDistributionAdvisor(advisee);
  }
}
