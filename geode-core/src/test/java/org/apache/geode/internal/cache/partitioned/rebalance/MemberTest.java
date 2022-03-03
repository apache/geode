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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.rebalance.model.AddressComparor;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Bucket;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Member;
import org.apache.geode.internal.cache.partitioned.rebalance.model.RefusalReason;

public class MemberTest {
  private final AddressComparor addressComparor = mock(AddressComparor.class);
  private final InternalDistributedMember memberId = mock(InternalDistributedMember.class);
  private final InternalDistributedMember otherMemberId = mock(InternalDistributedMember.class);

  @Test
  public void testCanDeleteWhenNotLastInZone() {
    doReturn(true).when(addressComparor).enforceUniqueZones();
    doReturn(true).when(addressComparor).areSameZone(
        ArgumentMatchers.any(InternalDistributedMember.class),
        ArgumentMatchers.any(InternalDistributedMember.class));
    Set<Member> membersHostingBucket = new HashSet<>();
    Bucket bucket = mock(Bucket.class);
    ClusterDistributionManager clusterDistributionManager = mock(ClusterDistributionManager.class);

    Member memberUnderTest = new Member(addressComparor, memberId, false, false);
    Member otherMember = new Member(addressComparor, otherMemberId, false, false);
    membersHostingBucket.add(memberUnderTest);
    membersHostingBucket.add(otherMember);
    when(bucket.getMembersHosting()).thenReturn(membersHostingBucket);

    Mockito.when(clusterDistributionManager.getRedundancyZone(memberId)).thenReturn("zoneA");
    Mockito.when(clusterDistributionManager.getRedundancyZone(otherMemberId)).thenReturn("zoneA");

    assertThat(memberUnderTest.canDelete(bucket, clusterDistributionManager))
        .isEqualTo(RefusalReason.NONE);
  }

  @Test
  public void testCanDeleteWhenLastInZone() {
    doReturn(true).when(addressComparor).enforceUniqueZones();
    doReturn(true).when(addressComparor).areSameZone(
        ArgumentMatchers.any(InternalDistributedMember.class),
        ArgumentMatchers.any(InternalDistributedMember.class));
    Set<Member> membersHostingBucket = new HashSet<>();
    Bucket bucket = mock(Bucket.class);
    ClusterDistributionManager clusterDistributionManager = mock(ClusterDistributionManager.class);

    Member memberUnderTest = new Member(addressComparor, memberId, false, false);
    membersHostingBucket.add(memberUnderTest);
    when(bucket.getMembersHosting()).thenReturn(membersHostingBucket);

    Mockito.when(clusterDistributionManager.getRedundancyZone(memberId)).thenReturn("zoneA");

    assertThat(memberUnderTest.canDelete(bucket, clusterDistributionManager))
        .isEqualTo(RefusalReason.LAST_MEMBER_IN_ZONE);
  }


  @Test
  public void testCanDeleteWhenNoZone() {
    doReturn(true).when(addressComparor).enforceUniqueZones();
    doReturn(true).when(addressComparor).areSameZone(
        ArgumentMatchers.any(InternalDistributedMember.class),
        ArgumentMatchers.any(InternalDistributedMember.class));
    Set<Member> membersHostingBucket = new HashSet<>();
    Bucket bucket = mock(Bucket.class);
    ClusterDistributionManager clusterDistributionManager = mock(ClusterDistributionManager.class);
    Member memberUnderTest = new Member(addressComparor, memberId, false, false);
    Member otherMember = new Member(addressComparor, otherMemberId, false, false);
    membersHostingBucket.add(memberUnderTest);
    membersHostingBucket.add(otherMember);
    when(bucket.getMembersHosting()).thenReturn(membersHostingBucket);

    Mockito.when(clusterDistributionManager.getRedundancyZone(memberId)).thenReturn(null);
    Mockito.when(clusterDistributionManager.getRedundancyZone(otherMemberId)).thenReturn(null);

    assertThat(memberUnderTest.canDelete(bucket, clusterDistributionManager))
        .isEqualTo(RefusalReason.NONE);
  }

}
