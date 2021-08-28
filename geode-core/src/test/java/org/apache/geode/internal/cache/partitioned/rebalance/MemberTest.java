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

import org.junit.Test;
import org.mockito.ArgumentMatchers;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.rebalance.model.AddressComparor;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Bucket;
import org.apache.geode.internal.cache.partitioned.rebalance.model.Member;
import org.apache.geode.internal.cache.partitioned.rebalance.model.RefusalReason;

public class MemberTest {
  AddressComparor addressComparor = mock(AddressComparor.class);
  InternalDistributedMember memberId = mock(InternalDistributedMember.class);
  InternalDistributedMember sourceId = mock(InternalDistributedMember.class);

  @Test
  public void testCanDeleteWhenInSameZone() {
    doReturn(true).when(addressComparor).enforceUniqueZones();
    doReturn(true).when(addressComparor).areSameZone(
        ArgumentMatchers.any(InternalDistributedMember.class),
        ArgumentMatchers.any(InternalDistributedMember.class));
    Bucket bucket = mock(Bucket.class);
    Member memberUnderTest = new Member(addressComparor, memberId, false, false);
    assertThat(memberUnderTest.canDelete(bucket, sourceId, true)).isEqualTo(RefusalReason.NONE);
  }


  @Test
  public void testCanDeleteWhenInDifferentZone() {
    doReturn(true).when(addressComparor).enforceUniqueZones();
    doReturn(false).when(addressComparor).areSameZone(
        ArgumentMatchers.any(InternalDistributedMember.class),
        ArgumentMatchers.any(InternalDistributedMember.class));
    Bucket bucket = mock(Bucket.class);
    Member memberUnderTest = new Member(addressComparor, memberId, false, false);
    assertThat(memberUnderTest.canDelete(bucket, sourceId, true))
        .isEqualTo(RefusalReason.DIFFERENT_ZONE);
  }


  @Test
  public void testWillAcceptBucket() {
    doReturn(true).when(addressComparor).enforceUniqueZones();
    doReturn(true).when(addressComparor).areSameZone(
        ArgumentMatchers.any(InternalDistributedMember.class),
        ArgumentMatchers.any(InternalDistributedMember.class));
    Bucket bucket = mock(Bucket.class);
    Member memberUnderTest = new Member(addressComparor, memberId, false, false);
    Member sourceMember = new Member(addressComparor, sourceId, false, false);

    assertThat(memberUnderTest.willAcceptBucket(bucket, sourceMember, true))
        .isEqualTo(RefusalReason.NONE);

  }

  @Test
  public void testWillAcceptBucketWithNullSourceMember() {
    doReturn(true).when(addressComparor).enforceUniqueZones();
    doReturn(true).when(addressComparor).areSameZone(
        ArgumentMatchers.any(InternalDistributedMember.class),
        ArgumentMatchers.any(InternalDistributedMember.class));
    Bucket bucket = mock(Bucket.class);
    Member memberUnderTest = new Member(addressComparor, memberId, false, false);
    Member sourceMember = new Member(addressComparor, sourceId, false, false);

    assertThat(memberUnderTest.willAcceptBucket(bucket, null, true)).isEqualTo(RefusalReason.NONE);

  }


}
