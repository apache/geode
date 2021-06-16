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

import static org.apache.geode.internal.cache.PartitionedRegionHelper.PR_ROOT_REGION_NAME;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.PartitionMessageDistribution;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.partitioned.RegionAdvisorFactory;
import org.apache.geode.internal.cache.partitioned.RetryTimeKeeper;
import org.apache.geode.internal.cache.partitioned.colocation.ColocationLoggerFactory;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;

@MockitoSettings(strictness = STRICT_STUBS)
class PartitionedRegionVirtualPutTest {
  @Mock
  PartitionMessageDistribution distribution;
  @Mock
  PartitionedRegionStats prStats;
  @Mock
  InternalDistributedMember localMember;
  @Mock
  PRHARedundancyProvider redundancyProvider;
  @Mock
  CancelCriterion cacheCancelCriterion;

  private PartitionedRegion region;

  @Nested
  class WithDataStore {
    @Mock
    PartitionedRegionDataStore dataStore;

    @BeforeEach
    void createPartitionedRegion_withDataStore() throws ClassNotFoundException {
      PartitionAttributes<?, ?> partitionAttributes = new PartitionAttributesFactory<>()
          .setLocalMaxMemory(1) // with dataStore
          .create();

      initializePartitionedRegion(createDataStoreFactory(), partitionAttributes,
          mock(RegionAdvisor.class),
          mock(RetryTimeKeeper.class));
    }

    @Test
    void createsLocally_ifOkToCreateEntry() throws ForceReattemptException {
      int bucketId = 12;
      String key = "create-event-key";
      BucketRegion bucketRegion = createBucketOnLocalMember(key, bucketId);

      EntryEventImpl event = createEntryEvent(key, bucketId);
      boolean isOkToCreateEntry = true;
      boolean isOkToUpdateEntry = false;
      boolean requireOldValue = false;
      long lastModified = 9; // ignored because isOkToCreateEntry == true

      region.virtualPut(event, isOkToCreateEntry, isOkToUpdateEntry, null, requireOldValue,
          lastModified, false, false, false);

      verify(dataStore)
          .createLocally(same(bucketRegion), same(event), eq(isOkToCreateEntry),
              eq(isOkToUpdateEntry), eq(requireOldValue), eq(0L));
    }

    @Test
    void putsLocally_ifNotOkToCreateEntry() throws ForceReattemptException {
      int bucketId = 34;
      String key = "put-event-key";
      BucketRegion bucketRegion = createBucketOnLocalMember(key, bucketId);
      EntryEventImpl event = createEntryEvent(key, bucketId);

      boolean isOkToCreateEntry = false;
      boolean isOkToUpdateEntry = false;
      Object expectedOldValue = "expected old value";
      boolean requireOldValue = false;
      long lastModified = 9;

      when(dataStore.putLocally(any(BucketRegion.class), any(), anyBoolean(), anyBoolean(), any(),
          anyBoolean(), anyLong()))
              .thenReturn(true);

      region.virtualPut(event, isOkToCreateEntry, isOkToUpdateEntry, expectedOldValue,
          requireOldValue, lastModified, false, false, false);

      verify(dataStore)
          .putLocally(same(bucketRegion), same(event), eq(isOkToCreateEntry), eq(isOkToUpdateEntry),
              eq(expectedOldValue), eq(requireOldValue), eq(lastModified));
    }

    @Test
    void returnsFalseIfConcurrentCacheModificationExceptionIsThrown()
        throws ForceReattemptException {
      int bucketId = 12;
      String key = "create-event-key";

      when(dataStore.getInitializedBucketForId(eq((Object) key), eq(bucketId)))
          .thenThrow(new ConcurrentCacheModificationException("testing"));
      when(redundancyProvider.createBucketAtomically(eq(bucketId), anyInt(), anyBoolean(), any()))
          .thenReturn(localMember);

      EntryEventImpl event = createEntryEvent(key, bucketId);
      boolean isOkToCreateEntry = true;
      boolean isOkToUpdateEntry = false;
      boolean requireOldValue = false;
      long lastModified = 9;

      boolean result =
          region.virtualPut(event, isOkToCreateEntry, isOkToUpdateEntry, null, requireOldValue,
              lastModified, false, false, false);

      assertThat(result).isFalse();

      verify(event).isConcurrencyConflict(true);
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @EnumSource()
    void retriesWhenDataStoreThrowsRetriableException(ExceptionCausingRetry exceptionCausingRetry)
        throws ForceReattemptException {
      int bucketId = 12;
      String key = "create-event-key";
      BucketRegion bucketRegion = mock(BucketRegion.class);

      when(dataStore.getInitializedBucketForId(eq((Object) key), eq(bucketId)))
          .thenReturn(bucketRegion);
      when(redundancyProvider.createBucketAtomically(eq(bucketId), anyInt(), anyBoolean(), any()))
          .thenReturn(localMember);

      EntryEventImpl event = createEntryEvent(key, bucketId, Operation.CREATE);
      boolean isOkToCreateEntry = true;
      boolean isOkToUpdateEntry = false;
      boolean requireOldValue = false;
      long lastModified = 9;

      boolean expectedResult = true;
      when(dataStore.createLocally(
          any(), any(), anyBoolean(), anyBoolean(), anyBoolean(), anyLong()))
              .thenThrow(exceptionCausingRetry.throwable())
              .thenThrow(exceptionCausingRetry.throwable())
              .thenThrow(exceptionCausingRetry.throwable())
              .thenReturn(expectedResult);

      boolean result =
          region.virtualPut(event, isOkToCreateEntry, isOkToUpdateEntry, null, requireOldValue,
              lastModified, false, false, false);

      assertThat(result).isEqualTo(expectedResult);

      verify(dataStore, times(4)) // The first 3 calls throw, the last succeeds
          .createLocally(same(bucketRegion), same(event), eq(isOkToCreateEntry),
              eq(isOkToUpdateEntry), eq(requireOldValue), eq(0L));
    }

    @Test
    void retriesAfterForceReattemptExceptionIfKeyIsValid() throws ForceReattemptException {
      int bucketId = 12;
      String key = "create-event-key";
      BucketRegion bucketRegion = mock(BucketRegion.class);

      when(dataStore.getInitializedBucketForId(eq((Object) key), eq(bucketId)))
          .thenReturn(bucketRegion);
      when(redundancyProvider.createBucketAtomically(eq(bucketId), anyInt(), anyBoolean(), any()))
          .thenReturn(localMember);

      boolean expectedResult = true;
      ForceReattemptException forceReattemptException = mock(ForceReattemptException.class);
      when(dataStore.createLocally(
          any(), any(), anyBoolean(), anyBoolean(), anyBoolean(), anyLong()))
              .thenThrow(forceReattemptException)
              .thenReturn(expectedResult);

      EntryEventImpl event = createEntryEvent(key, bucketId, Operation.CREATE);

      boolean result =
          region.virtualPut(event, true, false, null,
              false, 9, false, false, false);

      assertThat(result).isEqualTo(expectedResult);

      verify(forceReattemptException).checkKey(key);
      verify(event).setPossibleDuplicate(true);
    }

    @Test
    void propagatesExceptionThrownByKeyValidationWhenHandlingForceReattemptException()
        throws ForceReattemptException {
      int bucketId = 12;
      String key = "create-event-key";
      BucketRegion bucketRegion = mock(BucketRegion.class);

      when(dataStore.getInitializedBucketForId(eq((Object) key), eq(bucketId)))
          .thenReturn(bucketRegion);
      when(redundancyProvider.createBucketAtomically(eq(bucketId), anyInt(), anyBoolean(), any()))
          .thenReturn(localMember);

      ForceReattemptException forceReattemptException = mock(ForceReattemptException.class);
      PartitionedRegionException thrownByKeyValidation = new PartitionedRegionException("testing");

      doThrow(thrownByKeyValidation)
          .when(forceReattemptException).checkKey(any());
      when(dataStore.createLocally(
          any(), any(), anyBoolean(), anyBoolean(), anyBoolean(), anyLong()))
              .thenThrow(forceReattemptException);

      EntryEventImpl event = createEntryEvent(key, bucketId);

      Throwable thrown = catchThrowable(() -> {
        region.virtualPut(event, true, false, null, false, 9, false,
            false, false);
      });

      assertThat(thrown).isSameAs(thrownByKeyValidation);
      verify(event, never()).setPossibleDuplicate(anyBoolean());
    }

    @Test
    void propagatesExceptionThrownByCheckReadinessWhenHandlingForceReattemptException()
        throws ForceReattemptException {
      int bucketId = 12;
      String key = "create-event-key";
      BucketRegion bucketRegion = mock(BucketRegion.class);
      ForceReattemptException forceReattemptException = mock(ForceReattemptException.class);
      RuntimeException thrownByCheckReadiness = new RuntimeException("testing");

      doThrow(thrownByCheckReadiness).when(cacheCancelCriterion).checkCancelInProgress(any());

      when(dataStore.createLocally(
          any(), any(), anyBoolean(), anyBoolean(), anyBoolean(), anyLong()))
              .thenThrow(forceReattemptException);
      when(dataStore.getInitializedBucketForId(eq((Object) key), eq(bucketId)))
          .thenReturn(bucketRegion);
      when(redundancyProvider.createBucketAtomically(eq(bucketId), anyInt(), anyBoolean(), any()))
          .thenReturn(localMember);

      EntryEventImpl event = createEntryEvent(key, bucketId);

      Throwable thrown = catchThrowable(() -> {
        region.virtualPut(event, true, false, null, false, 9, false,
            false, false);
      });

      assertThat(thrown).isSameAs(thrownByCheckReadiness);
      verify(event, never()).setPossibleDuplicate(anyBoolean());
    }

    private PartitionedRegionDataStoreFactory createDataStoreFactory() {
      PartitionedRegionDataStoreFactory dataStoreFactory =
          mock(PartitionedRegionDataStoreFactory.class);
      when(dataStoreFactory.create(any()))
          .thenReturn(dataStore);
      return dataStoreFactory;
    }

    private BucketRegion createBucketOnLocalMember(Object key, int bucketId)
        throws ForceReattemptException {
      BucketRegion bucketRegion = mock(BucketRegion.class);

      when(dataStore.getInitializedBucketForId(eq(key), eq(bucketId)))
          .thenReturn(bucketRegion);
      when(redundancyProvider.createBucketAtomically(eq(bucketId), anyInt(), anyBoolean(), any()))
          .thenReturn(localMember);

      return bucketRegion;
    }
  }

  @Nested
  class WithoutDataStore {
    @Mock
    InternalDistributedMember memberWithDataStore;
    @Mock
    RegionAdvisor regionAdvisor;
    @Mock
    RetryTimeKeeper retryTimeKeeper;

    @BeforeEach
    void createPartitionedRegion_withoutDataStore() throws ClassNotFoundException {
      PartitionAttributes<?, ?> partitionAttributes = new PartitionAttributesFactory<>()
          .setLocalMaxMemory(0) // without dataStore
          .create();

      initializePartitionedRegion(null, partitionAttributes, regionAdvisor, retryTimeKeeper);
    }

    @Test
    void createsRemotely_ifOkToCreateEntry() throws ForceReattemptException {
      int bucketId = 12;
      String key = "create-event-key";
      EntryEventImpl event = createEntryEvent(key, bucketId);
      when(regionAdvisor.getPrimaryMemberForBucket(bucketId))
          .thenReturn(memberWithDataStore);

      boolean isOkToCreateEntry = true;
      long lastModified = 9; // ignored because isOkToCreateEntry == true
      boolean isOkToUpdateEntry = false;
      boolean requireOldValue = false;

      region.virtualPut(event, isOkToCreateEntry, isOkToUpdateEntry, null, requireOldValue,
          lastModified, false, false, false);

      verify(distribution).createRemotely(
          same(region), same(prStats), same(memberWithDataStore), same(event), eq(requireOldValue));
    }

    @Test
    void putsRemotely_ifNotOkToCreateEntry() throws ForceReattemptException {
      int bucketId = 12;
      String key = "create-event-key";
      EntryEventImpl event = createEntryEvent(key, bucketId);
      when(regionAdvisor.getPrimaryMemberForBucket(bucketId))
          .thenReturn(memberWithDataStore);

      when(distribution.putRemotely(any(), any(), any(), any(), anyBoolean(), anyBoolean(), any(),
          anyBoolean()))
              .thenReturn(true);

      boolean isOkToCreateEntry = false;
      long lastModified = 9; // ignored because isOkToCreateEntry == true
      boolean isOkToUpdateEntry = false;
      boolean requireOldValue = false;
      Object expectedOldValue = null;

      region.virtualPut(event, isOkToCreateEntry, isOkToUpdateEntry, expectedOldValue,
          requireOldValue, lastModified, false, false, false);

      verify(distribution).putRemotely(
          same(region), same(prStats), same(memberWithDataStore), same(event),
          eq(isOkToCreateEntry),
          eq(isOkToUpdateEntry), eq(expectedOldValue), eq(requireOldValue));
    }

    @Test
    void retriesWhenMemberWithDataStoreJoinsLater() throws ForceReattemptException {
      int bucketId = 12;
      String key = "create-event-key";
      EntryEventImpl event = createEntryEvent(key, bucketId);
      when(redundancyProvider.createBucketOnDataStore(eq(bucketId), anyInt(), any()))
          .thenReturn(null)
          .thenReturn(memberWithDataStore);

      boolean isOkToCreateEntry = true;
      long lastModified = 9; // ignored because isOkToCreateEntry == true
      boolean isOkToUpdateEntry = false;
      boolean requireOldValue = false;

      region.virtualPut(event, isOkToCreateEntry, isOkToUpdateEntry, null, requireOldValue,
          lastModified, false, false, false);

      verify(distribution).createRemotely(
          same(region), same(prStats), same(memberWithDataStore), same(event), eq(requireOldValue));
    }

    @ParameterizedTest(name = "{displayName} {0}")
    @EnumSource()
    void retriesWhenDistributionThrowsRetriableException(
        ExceptionCausingRetry exceptionCausingRetry)
        throws ForceReattemptException {
      int bucketId = 12;
      String key = "create-event-key";
      EntryEventImpl event = createEntryEvent(key, bucketId, Operation.CREATE);
      when(regionAdvisor.getPrimaryMemberForBucket(bucketId))
          .thenReturn(memberWithDataStore);
      boolean isOkToCreateEntry = true;
      long lastModified = 9; // ignored because isOkToCreateEntry == true
      boolean isOkToUpdateEntry = false;
      boolean requireOldValue = false;

      boolean expectedResult = true;
      when(distribution.createRemotely(any(), any(), any(), any(), anyBoolean()))
          .thenThrow(exceptionCausingRetry.throwable())
          .thenThrow(exceptionCausingRetry.throwable())
          .thenThrow(exceptionCausingRetry.throwable())
          .thenReturn(expectedResult);

      boolean result =
          region.virtualPut(event, isOkToCreateEntry, isOkToUpdateEntry, null, requireOldValue,
              lastModified, false, false, false);

      assertThat(result).isEqualTo(expectedResult);

      verify(distribution, times(4)).createRemotely(
          same(region), same(prStats), same(memberWithDataStore), same(event), eq(requireOldValue));
    }

    @Nested
    class ForceReattemptExceptionHandling {
      @Mock
      ForceReattemptException forceReattemptException;

      @BeforeEach
      void configureDistributionToThrowForceReattemptException()
          throws ForceReattemptException {
        when(distribution.createRemotely(any(), any(), any(), any(), anyBoolean()))
            .thenThrow(forceReattemptException)
            .thenReturn(true);
        when(regionAdvisor.getPrimaryMemberForBucket(anyInt()))
            .thenReturn(memberWithDataStore);
      }

      @Test
      void marksEventAsPossibleDuplicateIfKeyIsValid() {
        String key = "create-event-key";
        EntryEventImpl event = createEntryEvent(key, 12, Operation.CREATE);

        region.virtualPut(event, true, false, null, false, 9, false, false, false);

        verify(forceReattemptException).checkKey(key);
        verify(event).setPossibleDuplicate(true);
      }

      @Test
      void propagatesExceptionThrownByKeyValidation() {
        String key = "create-event-key";
        EntryEventImpl event = createEntryEvent(key, 12);

        PartitionedRegionException thrownByKeyValidation =
            new PartitionedRegionException("testing");
        doThrow(thrownByKeyValidation).when(forceReattemptException).checkKey(any());

        Throwable thrown = catchThrowable(() -> {
          region.virtualPut(event, true, false, null, false, 9, false, false, false);
        });

        assertThat(thrown).isSameAs(thrownByKeyValidation);
        verify(event, never()).setPossibleDuplicate(anyBoolean());
      }

      @Test
      void propagatesExceptionThrownByCheckReadiness() {
        String key = "create-event-key";
        EntryEventImpl event = createEntryEvent(key, 12);

        RuntimeException thrownByCheckReadiness = new RuntimeException("testing");
        doThrow(thrownByCheckReadiness).when(cacheCancelCriterion).checkCancelInProgress(any());

        Throwable thrown = catchThrowable(() -> {
          region.virtualPut(event, true, false, null, false, 9, false, false, false);
        });

        assertThat(thrown).isSameAs(thrownByCheckReadiness);
        verify(event, never()).setPossibleDuplicate(anyBoolean());
      }

      @Test
      void timesOutIfOverMaximumRetryTime() {
        String key = "create-event-key";
        int bucketId = 12;
        EntryEventImpl event = createEntryEvent(key, bucketId);

        when(regionAdvisor.getPrimaryMemberForBucket(bucketId))
            .thenReturn(memberWithDataStore);
        when(retryTimeKeeper.overMaximum())
            .thenReturn(true);

        Throwable thrown = catchThrowable(() -> region.virtualPut(event, true, false, null,
            false, 9, false, false, false));

        assertThat(thrown)
            .isInstanceOf(PartitionedRegionStorageException.class);

        verify(event, never()).setPossibleDuplicate(anyBoolean());
      }
    }
  }

  private void initializePartitionedRegion(PartitionedRegionDataStoreFactory dataStoreFactory,
      PartitionAttributes<?, ?> partitionAttributes, RegionAdvisor regionAdvisor,
      RetryTimeKeeper timeKeeper)
      throws ClassNotFoundException {
    InternalCache cache = mock(InternalCache.class);
    RegionAdvisorFactory regionAdvisorFactory = mock(RegionAdvisorFactory.class);
    PartitionedRegionStatsFactory partitionedRegionStatsFactory =
        mock(PartitionedRegionStatsFactory.class);
    DistributedRegion rootRegion = mock(DistributedRegion.class);

    PRHARedundancyProviderFactory redundancyProviderFactory =
        createPRHARedundancyProviderFactory(redundancyProvider);

    initializeCache(cache, localMember, rootRegion);

    initializePartitionedRegionMetaData(cache, partitionedRegionStatsFactory,
        uncheckedCast(rootRegion));

    AttributesFactory<?, ?> attributesFactory = new AttributesFactory<>();
    attributesFactory.setPartitionAttributes(partitionAttributes);

    when(regionAdvisorFactory.create(any()))
        .thenReturn(regionAdvisor);

    region =
        new PartitionedRegion("regionName", attributesFactory.create(), null, cache,
            mock(InternalRegionArguments.class), disabledClock(),
            mock(ColocationLoggerFactory.class), regionAdvisorFactory, mock(InternalDataView.class),
            mock(Node.class), mock(InternalDistributedSystem.class), partitionedRegionStatsFactory,
            mock(SenderIdMonitorFactory.class), redundancyProviderFactory, dataStoreFactory,
            distribution, max -> timeKeeper);

    region.initialize(null, null, null);
  }

  private PRHARedundancyProviderFactory createPRHARedundancyProviderFactory(
      PRHARedundancyProvider redundancyProvider) {
    PRHARedundancyProviderFactory redundancyProviderFactory =
        mock(PRHARedundancyProviderFactory.class);
    when(redundancyProviderFactory.create(any()))
        .thenReturn(redundancyProvider);
    return redundancyProviderFactory;
  }

  private void initializePartitionedRegionMetaData(InternalCache cache,
      PartitionedRegionStatsFactory partitionedRegionStatsFactory,
      Region<String, PartitionRegionConfig> rootRegion) {
    DLockService lockService = mock(DLockService.class);

    when(cache.getPartitionedRegionLockService())
        .thenReturn(lockService);
    when(cache.<String, PartitionRegionConfig>getRegion(eq(PR_ROOT_REGION_NAME), anyBoolean()))
        .thenReturn(rootRegion);

    when(lockService.lock(any(), anyLong(), anyLong()))
        .thenReturn(true);
    when(lockService.lock(any(), anyLong(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean()))
        .thenReturn(true);

    when(partitionedRegionStatsFactory.create(any()))
        .thenReturn(prStats);
  }

  private void initializeCache(InternalCache cache,
      InternalDistributedMember localMember, DistributedRegion rootRegion) {
    DistributionManager distributionManager = mock(DistributionManager.class);
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);

    when(cache.getCancelCriterion())
        .thenReturn(cacheCancelCriterion);
    when(cache.getCachePerfStats())
        .thenReturn(mock(CachePerfStats.class));
    when(cache.getDistributedSystem())
        .thenReturn(system);
    when(cache.getInternalDistributedSystem())
        .thenReturn(system);
    when(cache.getInternalResourceManager())
        .thenReturn(mock(InternalResourceManager.class));

    when(distributionManager.getCancelCriterion())
        .thenReturn(mock(CancelCriterion.class));
    when(distributionManager.getConfig())
        .thenReturn(mock(DistributionConfig.class));
    when(distributionManager.getId())
        .thenReturn(localMember);

    when(rootRegion.getDistributionAdvisor())
        .thenReturn(mock(CacheDistributionAdvisor.class));

    when(system.getClock())
        .thenReturn(mock(DSClock.class));
    when(system.getDistributedMember())
        .thenReturn(localMember);
    when(system.getDistributionManager())
        .thenReturn(distributionManager);
  }

  private static EntryEventImpl createEntryEvent(Object key, int bucketId) {
    EntryEventImpl event = mock(EntryEventImpl.class);
    KeyInfo keyInfo = mock(KeyInfo.class);
    when(event.getKey())
        .thenReturn(key);
    when(event.getKeyInfo())
        .thenReturn(keyInfo);
    when(keyInfo.getBucketId())
        .thenReturn(bucketId);
    return event;
  }

  private static EntryEventImpl createEntryEvent(Object key, int bucketId, Operation operation) {
    EntryEventImpl event = createEntryEvent(key, bucketId);
    when(event.getOperation())
        .thenReturn(operation);
    return event;
  }

  enum ExceptionCausingRetry {
    FORCE_REATTEMPT_EXCEPTION(new ForceReattemptException("testing")),
    PRIMARY_BUCKET_EXCEPTION(new PrimaryBucketException("test"));

    private final Throwable throwable;

    ExceptionCausingRetry(Throwable throwable) {
      this.throwable = throwable;
    }

    Throwable throwable() {
      return throwable;
    }
  }
}
