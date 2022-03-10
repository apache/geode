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

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class DestroyRegionOperationJUnitTest {

  @Test
  public void testGetRecipients() {
    InternalDistributedMember internalDistributedMember = mock(InternalDistributedMember.class);
    BucketRegion bucketRegion = mock(BucketRegion.class);
    InternalCache internalCache = mock(InternalCache.class);
    DistributedSystem distributedSystem = mock(DistributedSystem.class);
    when(bucketRegion.getCache()).thenReturn(internalCache);
    when(internalCache.getDistributedSystem()).thenReturn(distributedSystem);
    InternalDistributedMember member1 = mock(InternalDistributedMember.class);
    InternalDistributedMember member2 = mock(InternalDistributedMember.class);
    Set<InternalDistributedMember> members = new HashSet<>();
    members.add(member1);
    members.add(member2);
    when(bucketRegion.getDestroyRegionRecipients()).thenReturn(members);

    RegionEventImpl event = new RegionEventImpl(bucketRegion, Operation.REGION_LOCAL_DESTROY, null,
        false, internalDistributedMember,
        false);
    DestroyRegionOperation destroyRegionOperation = new DestroyRegionOperation(event, true);
    assertThat(destroyRegionOperation.getRecipients()).isEqualTo(members);

    DistributedRegion distributedRegion = mock(DistributedRegion.class);
    when(distributedRegion.getCache()).thenReturn(internalCache);
    Set<InternalDistributedMember> member = new HashSet<>();
    member.add(member1);
    when(distributedRegion.getDestroyRegionRecipients()).thenReturn(member);

    event = new RegionEventImpl(distributedRegion, Operation.REGION_DESTROY, null,
        false, internalDistributedMember, false);
    destroyRegionOperation = new DestroyRegionOperation(event, true);
    assertThat(destroyRegionOperation.getRecipients()).isEqualTo(member);
  }
}
