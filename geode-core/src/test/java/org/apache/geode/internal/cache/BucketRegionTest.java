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

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.Delta;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LossAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.partitioned.LockObject;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientRegistrationEventQueueManager;
import org.apache.geode.internal.cache.tier.sockets.ConnectionListener;
import org.apache.geode.internal.statistics.StatisticsClock;
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
  private CacheClientProxy proxy;

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
    proxy = mock(CacheClientProxy.class);

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
    when(partitionedRegion.getPrStats()).thenReturn(mock(PartitionedRegionStats.class));
    when(partitionedRegion.getDataStore()).thenReturn(mock(PartitionedRegionDataStore.class));
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

  @Test
  public void ensureCachedDeserializable_isCreated() {
    TXId txId = mock(TXId.class);
    when(txId.getMemberId()).thenReturn(mock(InternalDistributedMember.class));
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));

    bucketRegion.txApplyPut(Operation.CREATE, "key", "value", false,
        txId, null, null, null, new ArrayList<>(), null, null, null, null, 1);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(bucketRegion, times(1)).superTxApplyPut(any(), any(), captor.capture(),
        eq(false), any(), isNull(), isNull(), isNull(), any(), isNull(), isNull(), isNull(),
        isNull(), eq(1L));

    assertThat(captor.getValue()).isInstanceOf(VMCachedDeserializable.class);
  }

  @Test
  public void ensureCachedDeserializable_isNotCreatedForExistingCachedDeserializable() {
    TXId txId = mock(TXId.class);
    when(txId.getMemberId()).thenReturn(mock(InternalDistributedMember.class));
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));

    CachedDeserializable newValue = mock(CachedDeserializable.class);
    bucketRegion.txApplyPut(Operation.CREATE, "key", newValue, false,
        txId, null, null, null, new ArrayList<>(), null, null, null, null, 1);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(bucketRegion, times(1)).superTxApplyPut(any(), any(), captor.capture(),
        eq(false), any(), isNull(), isNull(), isNull(), any(), isNull(), isNull(), isNull(),
        isNull(), eq(1L));

    assertThat(captor.getValue()).isEqualTo(newValue);
  }

  @Test
  public void ensureCachedDeserializable_isNotCreatedForByteArray() {
    TXId txId = mock(TXId.class);
    when(txId.getMemberId()).thenReturn(mock(InternalDistributedMember.class));
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));

    byte[] newValue = new byte[] {0};
    bucketRegion.txApplyPut(Operation.CREATE, "key", newValue, false,
        txId, null, null, null, new ArrayList<>(), null, null, null, null, 1);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(bucketRegion, times(1)).superTxApplyPut(any(), any(), captor.capture(),
        eq(false), any(), isNull(), isNull(), isNull(), any(), isNull(), isNull(), isNull(),
        isNull(), eq(1L));

    assertThat(captor.getValue()).isEqualTo(newValue);
  }

  @Test
  public void ensureCachedDeserializable_isNotCreatedForInvalidToken() {
    TXId txId = mock(TXId.class);
    when(txId.getMemberId()).thenReturn(mock(InternalDistributedMember.class));
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));

    Token newValue = Token.INVALID;
    bucketRegion.txApplyPut(Operation.CREATE, "key", newValue, false,
        txId, null, null, null, new ArrayList<>(), null, null, null, null, 1);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(bucketRegion, times(1)).superTxApplyPut(any(), any(), captor.capture(),
        eq(false), any(), isNull(), isNull(), isNull(), any(), isNull(), isNull(), isNull(),
        isNull(), eq(1L));

    assertThat(captor.getValue()).isEqualTo(newValue);
  }

  @Test
  public void ensureCachedDeserializable_isCreatedForDelta() {
    TXId txId = mock(TXId.class);
    when(txId.getMemberId()).thenReturn(mock(InternalDistributedMember.class));
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    when(partitionedRegion.getObjectSizer()).thenReturn(o -> 1);

    Delta newValue = mock(Delta.class);
    bucketRegion.txApplyPut(Operation.CREATE, "key", newValue, false,
        txId, null, null, null, new ArrayList<>(), null, null, null, null, 1);

    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    verify(bucketRegion, times(1)).superTxApplyPut(any(), any(), captor.capture(),
        eq(false), any(), isNull(), isNull(), isNull(), any(), isNull(), isNull(), isNull(),
        isNull(), eq(1L));

    Object rawValue = captor.getValue();
    assertThat(rawValue).isInstanceOf(VMCachedDeserializable.class);

    VMCachedDeserializable value = (VMCachedDeserializable) rawValue;
    assertThat(value.getValue()).isEqualTo(newValue);
  }

  @Test(expected = RegionDestroyedException.class)
  public void waitUntilLockedThrowsIfFoundLockAndPartitionedRegionIsClosing() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
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
            cache, internalRegionArgs, disabledClock()));
    Integer[] keys = {1};
    doReturn(null).when(bucketRegion).searchAndLock(keys);

    assertThat(bucketRegion.waitUntilLocked(keys)).isTrue();
  }

  @Test(expected = RegionDestroyedException.class)
  public void basicPutEntryDoesNotReleaseLockIfKeysAndPrimaryNotLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doThrow(regionDestroyedException).when(bucketRegion).lockKeysAndPrimary(event);

    bucketRegion.basicPutEntry(event, 1);

    verify(bucketRegion, never()).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test
  public void basicPutEntryReleaseLockIfKeysAndPrimaryLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(true).when(bucketRegion).lockKeysAndPrimary(event);
    doReturn(mock(AbstractRegionMap.class)).when(bucketRegion).getRegionMap();

    bucketRegion.basicPutEntry(event, 1);

    verify(bucketRegion).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test(expected = RegionDestroyedException.class)
  public void virtualPutDoesNotReleaseLockIfKeysAndPrimaryNotLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doThrow(regionDestroyedException).when(bucketRegion).lockKeysAndPrimary(event);

    bucketRegion.virtualPut(event, false, true, null, false, 1, true);

    verify(bucketRegion, never()).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test
  public void virtualPutReleaseLockIfKeysAndPrimaryLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(true).when(bucketRegion).lockKeysAndPrimary(event);
    doReturn(true).when(bucketRegion).hasSeenEvent(event);

    bucketRegion.virtualPut(event, false, true, null, false, 1, true);

    verify(bucketRegion).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test(expected = RegionDestroyedException.class)
  public void basicDestroyDoesNotReleaseLockIfKeysAndPrimaryNotLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doThrow(regionDestroyedException).when(bucketRegion).lockKeysAndPrimary(event);

    bucketRegion.basicDestroy(event, false, null);

    verify(bucketRegion, never()).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test
  public void basicDestroyReleaseLockIfKeysAndPrimaryLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(true).when(bucketRegion).lockKeysAndPrimary(event);
    doReturn(true).when(bucketRegion).hasSeenEvent(event);

    bucketRegion.basicDestroy(event, false, null);

    verify(bucketRegion).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test(expected = RegionDestroyedException.class)
  public void basicUpdateEntryVersionDoesNotReleaseLockIfKeysAndPrimaryNotLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
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
            cache, internalRegionArgs, disabledClock()));
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
            cache, internalRegionArgs, disabledClock()));
    doThrow(regionDestroyedException).when(bucketRegion).lockKeysAndPrimary(event);

    bucketRegion.basicInvalidate(event, false, false);

    verify(bucketRegion, never()).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test
  public void basicInvalidateReleaseLockIfKeysAndPrimaryLocked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(true).when(bucketRegion).lockKeysAndPrimary(event);
    doReturn(true).when(bucketRegion).hasSeenEvent(event);

    bucketRegion.basicInvalidate(event, false, false);

    verify(bucketRegion).releaseLockForKeysAndPrimary(eq(event));
  }

  @Test
  public void lockKeysAndPrimaryReturnFalseIfDoesNotNeedWriteLock() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).needWriteLock(event);

    assertThat(bucketRegion.lockKeysAndPrimary(event)).isFalse();
  }

  @Test(expected = RegionDestroyedException.class)
  public void lockKeysAndPrimaryThrowsIfWaitUntilLockedThrows() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(keys).when(bucketRegion).getKeysToBeLocked(event);
    doThrow(regionDestroyedException).when(bucketRegion).waitUntilLocked(keys);

    bucketRegion.lockKeysAndPrimary(event);
  }

  @Test(expected = PrimaryBucketException.class)
  public void lockKeysAndPrimaryReleaseLockHeldIfDoLockForPrimaryThrows() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
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
            cache, internalRegionArgs, disabledClock()));
    doReturn(keys).when(bucketRegion).getKeysToBeLocked(event);
    doReturn(true).when(bucketRegion).waitUntilLocked(keys);
    doReturn(true).when(bucketRegion).doLockForPrimary(false);

    bucketRegion.lockKeysAndPrimary(event);

    verify(bucketRegion, never()).removeAndNotifyKeys(keys);
  }

  @Test
  public void testDoNotNotifyClientsOfTombstoneGCNoCacheClientNotifier() {

    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));

    Map regionGCVersions = new HashMap();
    Set keysRemoved = new HashSet();
    EventID eventID = new EventID();
    FilterRoutingInfo.FilterInfo routing = new FilterRoutingInfo.FilterInfo();

    bucketRegion.notifyClientsOfTombstoneGC(regionGCVersions, keysRemoved, eventID, routing);
    verify(bucketRegion, never()).getFilterProfile();
  }


  @Test
  public void testDoNotNotifyClientsOfTombstoneGCNoProxy() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));

    Map regionGCVersions = new HashMap();
    Set keysRemoved = new HashSet();
    EventID eventID = new EventID();
    FilterRoutingInfo.FilterInfo routing = null;
    doReturn(mock(SystemTimer.class)).when(cache).getCCPTimer();

    CacheClientNotifier ccn =
        CacheClientNotifier.getInstance(cache, mock(ClientRegistrationEventQueueManager.class),
            mock(StatisticsClock.class),
            mock(CacheServerStats.class), 10, 10, mock(ConnectionListener.class), null, true);

    bucketRegion.notifyClientsOfTombstoneGC(regionGCVersions, keysRemoved, eventID, routing);
    verify(bucketRegion, never()).getFilterProfile();

    doReturn(Collections.emptyList()).when(cache).getCacheServers();
    ccn.shutdown(111);
  }

  @Test
  public void testNotifyClientsOfTombstoneGC() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));

    Map regionGCVersions = new HashMap();
    Set keysRemoved = new HashSet();
    EventID eventID = new EventID();
    FilterRoutingInfo.FilterInfo routing = null;
    doReturn(mock(SystemTimer.class)).when(cache).getCCPTimer();

    CacheClientNotifier ccn =
        CacheClientNotifier.getInstance(cache, mock(ClientRegistrationEventQueueManager.class),
            mock(StatisticsClock.class),
            mock(CacheServerStats.class), 10, 10, mock(ConnectionListener.class), null, true);

    doReturn(mock(ClientProxyMembershipID.class)).when(proxy).getProxyID();
    ccn.addClientProxyToMap(proxy);
    doReturn(null).when(bucketRegion).getFilterProfile();

    bucketRegion.notifyClientsOfTombstoneGC(regionGCVersions, keysRemoved, eventID, routing);

    doReturn(Collections.emptyList()).when(cache).getCacheServers();
    doReturn(111L).when(proxy).getAcceptorId();

    ccn.shutdown(111);
  }

  @Test
  public void invokeTXCallbacksDoesNotInvokeCallbacksIfEventIsNotGenerateCallbacks() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(false).when(event).isGenerateCallbacks();

    bucketRegion.invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, event, false);

    verify(partitionedRegion, never()).invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, event,
        false);
  }

  @Test
  public void invokeTXCallbacksDoesNotInvokeCallbacksIfPartitionedRegionIsNotInitialized() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(true).when(event).isGenerateCallbacks();
    doReturn(false).when(partitionedRegion).isInitialized();
    doReturn(true).when(partitionedRegion).shouldDispatchListenerEvent();

    bucketRegion.invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, event, false);

    verify(partitionedRegion, never()).invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, event,
        false);
  }

  @Test
  public void invokeTXCallbacksIsInvoked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(true).when(event).isGenerateCallbacks();
    doReturn(true).when(partitionedRegion).isInitialized();
    doReturn(true).when(partitionedRegion).shouldDispatchListenerEvent();
    doReturn(event).when(bucketRegion).createEventForPR(event);

    bucketRegion.invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, event, true);

    verify(partitionedRegion).invokeTXCallbacks(EnumListenerEvent.AFTER_UPDATE, event,
        true);
  }

  @Test
  public void invokeDestroyCallbacksDoesNotInvokeCallbacksIfEventIsNotGenerateCallbacks() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(false).when(event).isGenerateCallbacks();

    bucketRegion.invokeDestroyCallbacks(EnumListenerEvent.AFTER_DESTROY, event, false, false);

    verify(partitionedRegion, never()).invokeDestroyCallbacks(EnumListenerEvent.AFTER_DESTROY,
        event, false, false);
  }

  @Test
  public void invokeDestroyCallbacksDoesNotInvokeCallbacksIfPartitionedRegionIsNotInitialized() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(true).when(event).isGenerateCallbacks();
    doReturn(false).when(partitionedRegion).isInitialized();
    doReturn(true).when(partitionedRegion).shouldDispatchListenerEvent();

    CacheClientNotifier.resetInstance();

    bucketRegion.invokeDestroyCallbacks(EnumListenerEvent.AFTER_DESTROY, event, false, false);

    verify(partitionedRegion, never()).invokeDestroyCallbacks(EnumListenerEvent.AFTER_DESTROY,
        event, false, false);
  }

  @Test
  public void invokeDestroyCallbacksIsInvoked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(true).when(event).isGenerateCallbacks();
    doReturn(true).when(partitionedRegion).isInitialized();
    doReturn(true).when(partitionedRegion).shouldDispatchListenerEvent();
    doReturn(event).when(bucketRegion).createEventForPR(event);

    bucketRegion.invokeDestroyCallbacks(EnumListenerEvent.AFTER_DESTROY, event, true, false);

    verify(partitionedRegion).invokeDestroyCallbacks(EnumListenerEvent.AFTER_DESTROY, event,
        true, false);
  }

  @Test
  public void invokeInvalidateCallbacksDoesNotInvokeCallbacksIfEventIsNotGenerateCallbacks() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(false).when(event).isGenerateCallbacks();

    bucketRegion.invokeInvalidateCallbacks(EnumListenerEvent.AFTER_INVALIDATE, event, false);

    verify(partitionedRegion, never()).invokeInvalidateCallbacks(EnumListenerEvent.AFTER_INVALIDATE,
        event, false);
  }

  @Test
  public void invokeInvalidateCallbacksDoesNotInvokeCallbacksIfPartitionedRegionIsNotInitialized() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(true).when(event).isGenerateCallbacks();
    doReturn(false).when(partitionedRegion).isInitialized();
    doReturn(true).when(partitionedRegion).shouldDispatchListenerEvent();

    CacheClientNotifier.resetInstance();

    bucketRegion.invokeInvalidateCallbacks(EnumListenerEvent.AFTER_INVALIDATE, event, false);

    verify(partitionedRegion, never()).invokeInvalidateCallbacks(EnumListenerEvent.AFTER_INVALIDATE,
        event, false);
  }

  @Test
  public void invokeInvalidateCallbacksIsInvoked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(true).when(event).isGenerateCallbacks();
    doReturn(true).when(partitionedRegion).isInitialized();
    doReturn(true).when(partitionedRegion).shouldDispatchListenerEvent();
    doReturn(event).when(bucketRegion).createEventForPR(event);

    bucketRegion.invokeInvalidateCallbacks(EnumListenerEvent.AFTER_INVALIDATE, event, true);

    verify(partitionedRegion).invokeInvalidateCallbacks(EnumListenerEvent.AFTER_INVALIDATE, event,
        true);
  }

  @Test
  public void invokePutCallbacksDoesNotInvokeCallbacksIfEventIsNotGenerateCallbacks() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(false).when(event).isGenerateCallbacks();

    bucketRegion.invokePutCallbacks(EnumListenerEvent.AFTER_CREATE, event, false, false);

    verify(partitionedRegion, never()).invokePutCallbacks(EnumListenerEvent.AFTER_CREATE, event,
        false, false);
  }

  @Test
  public void invokePutCallbacksDoesNotInvokeCallbacksIfPartitionedRegionIsNotInitialized() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(true).when(event).isGenerateCallbacks();
    doReturn(false).when(partitionedRegion).isInitialized();
    doReturn(true).when(partitionedRegion).shouldDispatchListenerEvent();

    CacheClientNotifier.resetInstance();

    bucketRegion.invokePutCallbacks(EnumListenerEvent.AFTER_CREATE, event, false, false);

    verify(partitionedRegion, never()).invokePutCallbacks(EnumListenerEvent.AFTER_CREATE, event,
        false, false);
  }

  @Test
  public void invokePutCallbacksIsInvoked() {
    BucketRegion bucketRegion =
        spy(new BucketRegion(regionName, regionAttributes, partitionedRegion,
            cache, internalRegionArgs, disabledClock()));
    doReturn(false).when(bucketRegion).isInitialized();
    doReturn(true).when(event).isGenerateCallbacks();
    doReturn(true).when(partitionedRegion).isInitialized();
    doReturn(true).when(partitionedRegion).shouldDispatchListenerEvent();
    doReturn(event).when(bucketRegion).createEventForPR(event);

    bucketRegion.invokePutCallbacks(EnumListenerEvent.AFTER_CREATE, event, true, false);

    verify(partitionedRegion).invokePutCallbacks(EnumListenerEvent.AFTER_CREATE, event,
        true, false);
  }
}
