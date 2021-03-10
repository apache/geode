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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.tier.MessageType;


public class DistributedRemoveAllOperationTest {

  @Test
  public void shouldBeMockable() throws Exception {
    DistributedRemoveAllOperation mockDistributedRemoveAllOperation =
        mock(DistributedRemoveAllOperation.class);
    EntryEventImpl mockEntryEventImpl = mock(EntryEventImpl.class);

    when(mockDistributedRemoveAllOperation.getBaseEvent()).thenReturn(mockEntryEventImpl);

    assertThat(mockDistributedRemoveAllOperation.getBaseEvent()).isSameAs(mockEntryEventImpl);
  }

  @Test
  public void testRemoveDestroyTokensFromCqResultKeys() {
    EntryEventImpl baseEvent = mock(EntryEventImpl.class);
    int putAllPRDataSize = 1;
    DistributedRemoveAllOperation distributedRemoveAllOperation =
        new DistributedRemoveAllOperation(baseEvent, putAllPRDataSize, false);
    EntryEventImpl entryEvent = mock(EntryEventImpl.class);
    Object key = new Object();
    when(entryEvent.getKey()).thenReturn(key);
    distributedRemoveAllOperation.addEntry(entryEvent);
    FilterRoutingInfo filterRoutingInfo = mock(FilterRoutingInfo.class);
    InternalDistributedMember internalDistributedMember = mock(InternalDistributedMember.class);
    Set<InternalDistributedMember> memberSet = new HashSet<>();
    memberSet.add(internalDistributedMember);
    when(filterRoutingInfo.getMembers()).thenReturn(memberSet);
    FilterRoutingInfo.FilterInfo filterInfo = mock(FilterRoutingInfo.FilterInfo.class);
    when(filterRoutingInfo.getFilterInfo(internalDistributedMember)).thenReturn(filterInfo);
    HashMap hashMap = new HashMap();
    hashMap.put(1L, MessageType.LOCAL_DESTROY);
    when(filterInfo.getCQs()).thenReturn(hashMap);
    BucketRegion bucketRegion = mock(BucketRegion.class);
    when(baseEvent.getRegion()).thenReturn(bucketRegion);
    InternalCache internalCache = mock(InternalCache.class);
    RegionAttributes regionAttributes = mock(RegionAttributes.class);
    when(bucketRegion.getAttributes()).thenReturn(regionAttributes);
    when(regionAttributes.getDataPolicy()).thenReturn(DataPolicy.DEFAULT);
    when(bucketRegion.getCache()).thenReturn(internalCache);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    when(internalCache.getDistributedSystem()).thenReturn(internalDistributedSystem);
    CqService cqService = mock(CqService.class);
    when(internalCache.getCqService()).thenReturn(cqService);
    PartitionedRegion partitionedRegion = mock(PartitionedRegion.class);
    when(bucketRegion.getPartitionedRegion()).thenReturn(partitionedRegion);
    when(bucketRegion.getKeyInfo(any(), any(), any())).thenReturn(new KeyInfo(key, null, null));
    CacheDistributionAdvisor cacheDistributionAdvisor = mock(CacheDistributionAdvisor.class);
    when(partitionedRegion.getCacheDistributionAdvisor()).thenReturn(cacheDistributionAdvisor);
    CacheDistributionAdvisor.CacheProfile cacheProfile =
        mock(CacheDistributionAdvisor.CacheProfile.class);
    FilterProfile filterProfile = mock(FilterProfile.class);
    cacheProfile.filterProfile = filterProfile;
    when(filterProfile.isLocalProfile()).thenReturn(false);
    ServerCQ serverCQ = mock(ServerCQ.class);
    when(serverCQ.getFilterID()).thenReturn(new Long(1L));
    Map cqMap = new HashMap();
    cqMap.put("1", serverCQ);
    when(filterProfile.getCqMap()).thenReturn(cqMap);
    when(cacheDistributionAdvisor.getProfile(internalDistributedMember)).thenReturn(cacheProfile);
    doNothing().when(serverCQ).removeFromCqResultKeys(isA(Object.class), isA(Boolean.class));
    distributedRemoveAllOperation.removeDestroyTokensFromCqResultKeys(filterRoutingInfo);
    verify(serverCQ, times(1)).removeFromCqResultKeys(key, true);
  }
}
