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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LossAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.Operation;
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
  private EntryEventImpl event;
  private BucketAdvisor bucketAdvisor;
  private Operation operation;

  private final String regionName = "name";
  private final Object[] keys = new Object[] {1};
  private final RegionDestroyedException regionDestroyedException =
      new RegionDestroyedException("", "");

  @Before
  @SuppressWarnings("deprecation")
  public void setup() {
    regionAttributes = mock(RegionAttributes.class);
    partitionedRegion = mock(PartitionedRegion.class);
    cache = Fakes.cache();
    internalRegionArgs = mock(InternalRegionArguments.class);
    dataPolicy = mock(DataPolicy.class);
    event = mock(EntryEventImpl.class, RETURNS_DEEP_STUBS);
    EvictionAttributesImpl evictionAttributes = mock(EvictionAttributesImpl.class);
    ExpirationAttributes expirationAttributes = mock(ExpirationAttributes.class);
    MembershipAttributes membershipAttributes = mock(MembershipAttributes.class);
    Scope scope = mock(Scope.class);
    bucketAdvisor = mock(BucketAdvisor.class, RETURNS_DEEP_STUBS);
    operation = mock(Operation.class);

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
    when(event.getOperation()).thenReturn(operation);
    when(operation.isDistributed()).thenReturn(true);
  }

  @Test(expected = RegionDestroyedException.class)
  public void waitUntilLockedThrowsIfFoundLockAndPartitionedRegionIsClosing() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    Integer[] keys = {1};
    doReturn(mock(LockObject.class)).when(bucketRegion).searchAndLock(keys);
    doThrow(regionDestroyedException).when(partitionedRegion)
        .checkReadiness();

    bucketRegion.waitUntilLocked(keys);
  }

  @Test
  public void waitUntilLockedReturnsTrueIfNoOtherThreadLockedKeys() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    Integer[] keys = {1};
    doReturn(null).when(bucketRegion).searchAndLock(keys);

    assertThat(bucketRegion.waitUntilLocked(keys)).isTrue();
  }

  @Test(expected = RegionDestroyedException.class)
  public void basicPutEntryDoesNotReleaseLockIfKeysAndPrimaryNotLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doThrow(regionDestroyedException).when(bucketRegion).lockKeysAndPrimary(event);

    bucketRegion.basicPutEntry(event, 1);

    verify(bucketRegion, never()).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test
  public void basicPutEntryReleaseLockIfKeysAndPrimaryLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doReturn(true).when(bucketRegion).lockKeysAndPrimary(event);
    doReturn(mock(AbstractRegionMap.class)).when(bucketRegion).getRegionMap();

    bucketRegion.basicPutEntry(event, 1);

    verify(bucketRegion).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test(expected = RegionDestroyedException.class)
  public void virtualPutDoesNotReleaseLockIfKeysAndPrimaryNotLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doThrow(regionDestroyedException).when(bucketRegion).lockKeysAndPrimary(event);

    bucketRegion.virtualPut(event, false, true, null, false, 1, true);

    verify(bucketRegion, never()).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test
  public void virtualPutReleaseLockIfKeysAndPrimaryLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doReturn(true).when(bucketRegion).lockKeysAndPrimary(event);
    doReturn(true).when(bucketRegion).hasSeenEvent(event);

    bucketRegion.virtualPut(event, false, true, null, false, 1, true);

    verify(bucketRegion).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test(expected = RegionDestroyedException.class)
  public void basicDestroyDoesNotReleaseLockIfKeysAndPrimaryNotLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doThrow(regionDestroyedException).when(bucketRegion).lockKeysAndPrimary(event);

    bucketRegion.basicDestroy(event, false, null);

    verify(bucketRegion, never()).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test
  public void basicDestroyReleaseLockIfKeysAndPrimaryLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doReturn(true).when(bucketRegion).lockKeysAndPrimary(event);
    doReturn(true).when(bucketRegion).hasSeenEvent(event);

    bucketRegion.basicDestroy(event, false, null);

    verify(bucketRegion).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test(expected = RegionDestroyedException.class)
  public void basicUpdateEntryVersionDoesNotReleaseLockIfKeysAndPrimaryNotLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doThrow(regionDestroyedException).when(bucketRegion).lockKeysAndPrimary(event);
    when(event.getRegion()).thenReturn(bucketRegion);
    doReturn(true).when(bucketRegion).hasSeenEvent(event);
    doReturn(mock(AbstractRegionMap.class)).when(bucketRegion).getRegionMap();

    bucketRegion.basicUpdateEntryVersion(event);

    verify(bucketRegion, never()).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test
  public void basicUpdateEntryVersionReleaseLockIfKeysAndPrimaryLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doReturn(true).when(bucketRegion).lockKeysAndPrimary(event);
    when(event.getRegion()).thenReturn(bucketRegion);
    doReturn(true).when(bucketRegion).hasSeenEvent(event);
    doReturn(mock(AbstractRegionMap.class)).when(bucketRegion).getRegionMap();

    bucketRegion.basicUpdateEntryVersion(event);

    verify(bucketRegion).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test(expected = RegionDestroyedException.class)
  public void basicInvalidateDoesNotReleaseLockIfKeysAndPrimaryNotLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doThrow(regionDestroyedException).when(bucketRegion).lockKeysAndPrimary(event);

    bucketRegion.basicInvalidate(event, false, false);

    verify(bucketRegion, never()).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test
  public void basicInvalidateReleaseLockIfKeysAndPrimaryLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doReturn(true).when(bucketRegion).lockKeysAndPrimary(event);
    doReturn(true).when(bucketRegion).hasSeenEvent(event);

    bucketRegion.basicInvalidate(event, false, false);

    verify(bucketRegion).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test
  public void lockKeysAndPrimaryReturnFalseIfDoesNotNeedWriteLock() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doReturn(false).when(bucketRegion).needWriteLock(event);

    assertThat(bucketRegion.lockKeysAndPrimary(event)).isFalse();
  }

  @Test(expected = RegionDestroyedException.class)
  public void lockKeysAndPrimaryThrowsIfWaitUntilLockedThrows() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doReturn(keys).when(bucketRegion).getKeysToBeLocked(event);
    doThrow(regionDestroyedException).when(bucketRegion).waitUntilLocked(keys);

    bucketRegion.lockKeysAndPrimary(event);
  }

  @Test(expected = PrimaryBucketException.class)
  public void lockKeysAndPrimaryReleaseLockHeldIfDoLockForPrimaryThrows() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doReturn(keys).when(bucketRegion).getKeysToBeLocked(event);
    doReturn(true).when(bucketRegion).waitUntilLocked(keys);
    doThrow(new PrimaryBucketException()).when(bucketRegion).doLockForPrimary(false);

    bucketRegion.lockKeysAndPrimary(event);

    verify(bucketRegion).removeAndNotifyKeys(keys);
  }

  @Test
  public void lockKeysAndPrimaryReleaseLockHeldIfDoesNotLockForPrimary() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs));
    doReturn(keys).when(bucketRegion).getKeysToBeLocked(event);
    doReturn(true).when(bucketRegion).waitUntilLocked(keys);
    doReturn(true).when(bucketRegion).doLockForPrimary(false);

    bucketRegion.lockKeysAndPrimary(event);

    verify(bucketRegion, never()).removeAndNotifyKeys(keys);
  }

}
