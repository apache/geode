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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LossAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.partitioned.LockObject;
import org.apache.geode.test.fake.Fakes;

public class BucketRegionTest {
  private RegionAttributes regionAttributes;
  private PartitionedRegion partitionedRegion;
  private InternalCache cache;
  private InternalRegionArguments internalRegionArgs;
  private DataPolicy dataPolicy;

  private final String regionName = "name";

  @Before
  @SuppressWarnings("deprecation")
  public void setup() {
    regionAttributes = mock(RegionAttributes.class);
    partitionedRegion = mock(PartitionedRegion.class);
    cache = Fakes.cache();
    internalRegionArgs = mock(InternalRegionArguments.class);
    dataPolicy = mock(DataPolicy.class);
    EvictionAttributesImpl evictionAttributes = mock(EvictionAttributesImpl.class);
    ExpirationAttributes expirationAttributes = mock(ExpirationAttributes.class);
    MembershipAttributes membershipAttributes = mock(MembershipAttributes.class);
    Scope scope = mock(Scope.class);
    BucketAdvisor bucketAdvisor = mock(BucketAdvisor.class);

    when(regionAttributes.getEvictionAttributes()).thenReturn(evictionAttributes);
    when(regionAttributes.getRegionTimeToLive()).thenReturn(expirationAttributes);
    when(regionAttributes.getRegionIdleTimeout()).thenReturn(expirationAttributes);
    when(regionAttributes.getEntryTimeToLive()).thenReturn(expirationAttributes);
    when(regionAttributes.getEntryIdleTimeout()).thenReturn(expirationAttributes);
    when(regionAttributes.getDataPolicy()).thenReturn(dataPolicy);
    when(regionAttributes.getDiskStoreName()).thenReturn("store");
    when(regionAttributes.getConcurrencyLevel()).thenReturn(16);
    when(regionAttributes.getLoadFactor()).thenReturn(0.75f);
    when(regionAttributes.getMembershipAttributes()).thenReturn(membershipAttributes);
    when(regionAttributes.getScope()).thenReturn(scope);
    when(partitionedRegion.getFullPath()).thenReturn("parent");
    when(internalRegionArgs.getPartitionedRegion()).thenReturn(partitionedRegion);
    when(internalRegionArgs.isUsedForPartitionedRegionBucket()).thenReturn(true);
    when(internalRegionArgs.getBucketAdvisor()).thenReturn(bucketAdvisor);
    when(evictionAttributes.getAlgorithm()).thenReturn(EvictionAlgorithm.NONE);
    when(membershipAttributes.getLossAction()).thenReturn(mock(LossAction.class));
    when(scope.isDistributedAck()).thenReturn(true);
    when(dataPolicy.withReplication()).thenReturn(true);
    when(bucketAdvisor.getProxyBucketRegion()).thenReturn(mock(ProxyBucketRegion.class));
  }

  @Test(expected = RegionDestroyedException.class)
  public void waitUntilLockedThrowsIfFoundLockAndPartitionedRegionIsClosing() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    Integer[] keys = {1};
    doReturn(mock(LockObject.class)).when(bucketRegion).searchAndLock(keys);
    doNothing().doThrow(new RegionDestroyedException("", "")).when(partitionedRegion)
        .checkReadiness();

    bucketRegion.waitUntilLocked(keys);
  }

  @Test(expected = RegionDestroyedException.class)
  public void waitUntilLockedThrowsIfNotFoundLockAndPartitionedRegionIsClosing() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    Integer[] keys = {1};
    doReturn(null).when(bucketRegion).searchAndLock(keys);
    doThrow(new RegionDestroyedException("", "")).when(partitionedRegion).checkReadiness();

    bucketRegion.waitUntilLocked(keys);
  }
}
