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

import static java.util.Collections.emptySet;
import static org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId;
import static org.apache.geode.internal.cache.PartitionedRegionHelper.PR_ROOT_REGION_NAME;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.internal.index.IndexCreationData;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.partitioned.colocation.ColocationLoggerFactory;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
@SuppressWarnings({"deprecation", "unchecked", "unused"})
public class PartitionedRegionTest {

  private InternalCache cache;
  private InternalDistributedSystem system;
  private DistributionManager distributionManager;
  private InternalResourceManager resourceManager;
  private AttributesFactory attributesFactory;

  private PartitionedRegion partitionedRegion;

  private final String indexName = "index";
  private final Index index = mock(Index.class);

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() {
    system = mock(InternalDistributedSystem.class);
    distributionManager = mock(DistributionManager.class);
    InternalDistributedMember distributedMember = mock(InternalDistributedMember.class);
    InternalResourceManager resourceManager = mock(InternalResourceManager.class);

    cache = mock(InternalCache.class);
    attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(
        new PartitionAttributesFactory().setTotalNumBuckets(1).setRedundantCopies(1).create());

    when(cache.getDistributedSystem())
        .thenReturn(system);
    when(cache.getInternalDistributedSystem())
        .thenReturn(system);
    when(cache.getInternalResourceManager())
        .thenReturn(resourceManager);
    when(distributionManager.getId())
        .thenReturn(distributedMember);
    when(system.createAtomicStatistics(any(), any()))
        .thenReturn(mock(Statistics.class));
    when(system.getClock())
        .thenReturn(mock(DSClock.class));
    when(system.getDistributedMember())
        .thenReturn(distributedMember);
    when(system.getDistributionManager())
        .thenReturn(distributionManager);

    partitionedRegion = new PartitionedRegion("regionName", attributesFactory.create(), null,
        cache, mock(InternalRegionArguments.class), disabledClock(),
        ColocationLoggerFactory.create(),
        region -> mock(RegionAdvisor.class));
  }

  private Object[] cacheLoaderAndWriter() {
    CacheLoader mockLoader = mock(CacheLoader.class);
    CacheWriter mockWriter = mock(CacheWriter.class);
    return new Object[] {
        new Object[] {mockLoader, null},
        new Object[] {null, mockWriter},
        new Object[] {mockLoader, mockWriter},
        new Object[] {null, null}
    };
  }

  @Test
  public void postDestroyRegionForCacheCloseWillCloseDataStoreStats() {
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    CachePerfStats dataStoreStats = mock(CachePerfStats.class);
    when(dataStore.getCachePerfStats()).thenReturn(dataStoreStats);
    partitionedRegion.setDataStore(dataStore);
    partitionedRegion.setClosed();
    when(cache.getInternalResourceManager(anyBoolean()))
        .thenReturn(mock(InternalResourceManager.class));
    RegionEventImpl event = mock(RegionEventImpl.class);
    when(event.getOperation()).thenReturn(Operation.CACHE_CLOSE);

    partitionedRegion.postDestroyRegion(false, event);

    verify(dataStoreStats).close();
  }

  @Test
  @Parameters(method = "cacheLoaderAndWriter")
  @TestCaseName("{method}(CacheLoader={0}, CacheWriter={1})")
  public void verifyPRConfigUpdatedAfterLoaderUpdate(CacheLoader cacheLoader,
      CacheWriter cacheWriter) {
    // ARRANGE
    PartitionRegionConfig partitionRegionConfig = mock(PartitionRegionConfig.class);
    Region<String, PartitionRegionConfig> partitionedRegionRoot = mock(LocalRegion.class);
    PartitionedRegion.RegionLock regionLock = mock(PartitionedRegion.RegionLock.class);
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    InternalDistributedMember ourMember = spyPartitionedRegion.getDistributionManager().getId();
    InternalDistributedMember otherMember1 = mock(InternalDistributedMember.class);
    InternalDistributedMember otherMember2 = mock(InternalDistributedMember.class);
    Node ourNode = mock(Node.class, "ourNode");
    Node otherNode1 = mock(Node.class, "otherNode1");
    Node otherNode2 = mock(Node.class, "otherNode2");

    when(otherNode1.getMemberId())
        .thenReturn(otherMember1);
    when(otherNode2.getMemberId())
        .thenReturn(otherMember2);
    when(ourNode.getMemberId())
        .thenReturn(ourMember);
    when(ourNode.isCacheLoaderAttached())
        .thenReturn(cacheLoader != null);
    when(ourNode.isCacheWriterAttached())
        .thenReturn(cacheWriter != null);
    when(partitionRegionConfig.getNodes())
        .thenReturn(asSet(otherNode1, ourNode, otherNode2));
    when(partitionedRegionRoot.get(spyPartitionedRegion.getRegionIdentifier()))
        .thenReturn(partitionRegionConfig);
    when(spyPartitionedRegion.getPRRoot())
        .thenReturn(partitionedRegionRoot);

    doReturn(cacheLoader)
        .when(spyPartitionedRegion).basicGetLoader();
    doReturn(cacheWriter)
        .when(spyPartitionedRegion).basicGetWriter();
    doReturn(regionLock)
        .when(spyPartitionedRegion).getRegionLock();

    // ACT
    spyPartitionedRegion.updatePRNodeInformation();

    // ASSERT
    assertThat(partitionRegionConfig.getNodes())
        .contains(ourNode);

    Node verifyOurNode = null;
    for (Node node : partitionRegionConfig.getNodes()) {
      if (node.getMemberId().equals(ourMember)) {
        verifyOurNode = node;
      }
    }
    assertThat(verifyOurNode)
        .withFailMessage("Failed to find " + ourMember + " in " + partitionRegionConfig.getNodes())
        .isNotNull();

    verify(partitionedRegionRoot)
        .get(spyPartitionedRegion.getRegionIdentifier());
    verify(partitionedRegionRoot)
        .put(spyPartitionedRegion.getRegionIdentifier(), partitionRegionConfig);
    verify(spyPartitionedRegion)
        .updatePRConfig(partitionRegionConfig, false);

    assertThat(verifyOurNode.isCacheLoaderAttached())
        .isEqualTo(cacheLoader != null);
    assertThat(verifyOurNode.isCacheWriterAttached())
        .isEqualTo(cacheWriter != null);
  }

  @Test
  public void getBucketNodeForReadOrWriteReturnsPrimaryNodeForRegisterInterest() {
    // ARRANGE
    EntryEventImpl clientEvent = mock(EntryEventImpl.class);
    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);

    when(clientEvent.getOperation())
        .thenReturn(Operation.GET_FOR_REGISTER_INTEREST);

    int bucketId = 0;
    doReturn(primaryMember)
        .when(spyPartitionedRegion).getNodeForBucketWrite(eq(bucketId), isNull());

    // ACT
    InternalDistributedMember memberForRegisterInterestRead =
        spyPartitionedRegion.getBucketNodeForReadOrWrite(bucketId, clientEvent);

    // ASSERT
    assertThat(memberForRegisterInterestRead)
        .isSameAs(primaryMember);
    verify(spyPartitionedRegion)
        .getNodeForBucketWrite(anyInt(), any());
  }

  @Test
  public void getBucketNodeForReadOrWriteReturnsSecondaryNodeForNonRegisterInterest() {
    // ARRANGE
    EntryEventImpl clientEvent = mock(EntryEventImpl.class);
    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);

    when(clientEvent.getOperation())
        .thenReturn(Operation.GET);

    int bucketId = 0;
    doReturn(secondaryMember)
        .when(spyPartitionedRegion).getNodeForBucketRead(eq(bucketId));

    // ACT
    InternalDistributedMember memberForRegisterInterestRead =
        spyPartitionedRegion.getBucketNodeForReadOrWrite(bucketId, clientEvent);

    // ASSERT
    assertThat(memberForRegisterInterestRead)
        .isSameAs(secondaryMember);
    verify(spyPartitionedRegion)
        .getNodeForBucketRead(anyInt());
  }

  @Test
  public void getBucketNodeForReadOrWriteReturnsSecondaryNodeWhenClientEventIsNotPresent() {
    // ARRANGE
    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);

    int bucketId = 0;
    doReturn(secondaryMember)
        .when(spyPartitionedRegion).getNodeForBucketRead(eq(bucketId));

    // ACT
    InternalDistributedMember memberForRegisterInterestRead =
        spyPartitionedRegion.getBucketNodeForReadOrWrite(bucketId, null);

    // ASSERT
    assertThat(memberForRegisterInterestRead)
        .isSameAs(secondaryMember);
    verify(spyPartitionedRegion)
        .getNodeForBucketRead(anyInt());
  }

  @Test
  public void getBucketNodeForReadOrWriteReturnsSecondaryNodeWhenClientEventOperationIsNotPresent() {
    // ARRANGE
    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);

    int bucketId = 0;
    doReturn(secondaryMember)
        .when(spyPartitionedRegion).getNodeForBucketRead(eq(bucketId));

    // ACT
    InternalDistributedMember memberForRegisterInterestRead =
        spyPartitionedRegion.getBucketNodeForReadOrWrite(bucketId, null);

    // ASSERT
    assertThat(memberForRegisterInterestRead)
        .isSameAs(secondaryMember);
    verify(spyPartitionedRegion)
        .getNodeForBucketRead(anyInt());
  }

  @Test
  public void updateBucketMapsForInterestRegistrationWithSetOfKeysFetchesPrimaryBucketsForRead() {
    // ARRANGE
    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);

    doReturn(primaryMember)
        .when(spyPartitionedRegion).getNodeForBucketWrite(anyInt(), isNull());

    HashMap<InternalDistributedMember, HashSet<Integer>> nodeToBuckets = new HashMap<>();

    // ACT
    spyPartitionedRegion.updateNodeToBucketMap(nodeToBuckets, asSet(0, 1));

    // ASSERT
    verify(spyPartitionedRegion, times(2))
        .getNodeForBucketWrite(anyInt(), isNull());
  }

  @Test
  public void updateBucketMapsForInterestRegistrationWithAllKeysFetchesPrimaryBucketsForRead() {
    // ARRANGE
    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);

    doReturn(primaryMember)
        .when(spyPartitionedRegion).getNodeForBucketWrite(anyInt(), isNull());

    HashMap<InternalDistributedMember, HashMap<Integer, HashSet>> nodeToBuckets = new HashMap<>();
    HashMap<Integer, HashSet> bucketKeys = (HashMap) asMapOfSet(0, (HashSet) asSet(0, 1));

    // ACT
    spyPartitionedRegion.updateNodeToBucketMap(nodeToBuckets, bucketKeys);

    // ASSERT
    verify(spyPartitionedRegion)
        .getNodeForBucketWrite(anyInt(), isNull());
  }

  @Test
  public void filterOutNonParallelGatewaySendersShouldReturnCorrectly() {
    // ARRANGE
    GatewaySender parallelSender = mock(GatewaySender.class);
    GatewaySender anotherParallelSender = mock(GatewaySender.class);
    GatewaySender serialSender = mock(GatewaySender.class);

    when(parallelSender.isParallel())
        .thenReturn(true);
    when(parallelSender.getId())
        .thenReturn("parallel");
    when(anotherParallelSender.isParallel())
        .thenReturn(true);
    when(anotherParallelSender.getId())
        .thenReturn("anotherParallel");
    when(serialSender.isParallel())
        .thenReturn(false);
    when(cache.getAllGatewaySenders())
        .thenReturn(asSet(parallelSender, anotherParallelSender, serialSender));

    // ACT/ASSERT
    assertThat(partitionedRegion.filterOutNonParallelGatewaySenders(asSet("serial")))
        .isEmpty();
    // ACT/ASSERT
    assertThat(partitionedRegion.filterOutNonParallelGatewaySenders(asSet("unknownSender")))
        .isEmpty();
    // ACT/ASSERT
    assertThat(partitionedRegion.filterOutNonParallelGatewaySenders(asSet("parallel", "serial")))
        .containsExactly("parallel");
    // ACT/ASSERT
    assertThat(partitionedRegion
        .filterOutNonParallelGatewaySenders(asSet("parallel", "serial", "anotherParallel")))
            .containsExactly("parallel", "anotherParallel");
  }

  @Test
  public void filterOutNonParallelAsyncEventQueuesShouldReturnCorrectly() {
    // ARRANGE
    AsyncEventQueue parallelQueue = mock(AsyncEventQueue.class);
    AsyncEventQueue anotherParallelQueue = mock(AsyncEventQueue.class);
    AsyncEventQueue serialQueue = mock(AsyncEventQueue.class);

    when(parallelQueue.isParallel())
        .thenReturn(true);
    when(parallelQueue.getId())
        .thenReturn(getSenderIdFromAsyncEventQueueId("parallel"));
    when(anotherParallelQueue.isParallel())
        .thenReturn(true);
    when(anotherParallelQueue.getId())
        .thenReturn(getSenderIdFromAsyncEventQueueId("anotherParallel"));
    when(serialQueue.isParallel())
        .thenReturn(false);
    when(cache.getAsyncEventQueues())
        .thenReturn(asSet(parallelQueue, anotherParallelQueue, serialQueue));

    // ACT/ASSERT
    assertThat(partitionedRegion.filterOutNonParallelAsyncEventQueues(asSet("serial")))
        .isEmpty();
    // ACT/ASSERT
    assertThat(partitionedRegion.filterOutNonParallelAsyncEventQueues(asSet("unknownSender")))
        .isEmpty();
    // ACT/ASSERT
    assertThat(partitionedRegion.filterOutNonParallelAsyncEventQueues(asSet("parallel", "serial")))
        .containsExactly("parallel");
    // ACT/ASSERT
    assertThat(partitionedRegion
        .filterOutNonParallelAsyncEventQueues(asSet("parallel", "serial", "anotherParallel")))
            .containsExactly("parallel", "anotherParallel");
  }

  @Test
  public void getLocalSizeDoesNotThrowIfRegionUninitialized() {
    // ARRANGE
    partitionedRegion = new PartitionedRegion("region", attributesFactory.create(), null, cache,
        mock(InternalRegionArguments.class), disabledClock(), ColocationLoggerFactory.create());

    // ACT/ASSERT
    assertThatCode(partitionedRegion::getLocalSize)
        .doesNotThrowAnyException();
  }

  @Test
  public void generatePRIdShouldNotThrowNumberFormatExceptionIfAnErrorOccursWhileReleasingTheLock() {
    // ARRANGE
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    DistributedLockService lockService = mock(DistributedLockService.class);

    when(system.getDistributionManager().getCancelCriterion())
        .thenReturn(mock(CancelCriterion.class));
    when(distributionManager.getOtherDistributionManagerIds())
        .thenReturn(emptySet());

    when(spyPartitionedRegion.getPartitionedRegionLockService())
        .thenReturn(lockService);
    when(lockService.lock(any(), anyLong(), anyLong()))
        .thenReturn(true);
    doThrow(new RuntimeException("for test"))
        .when(lockService).unlock(any());

    // ACT/ASSERT
    assertThatCode(() -> spyPartitionedRegion.generatePRId(system))
        .doesNotThrowAnyException();
  }

  @Test
  public void registerPartitionedRegionShouldHandleLockServiceDestroyedException()
      throws ClassNotFoundException {
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(
        new PartitionAttributesFactory().setTotalNumBuckets(1).setRedundantCopies(1)
            .setLocalMaxMemory(0).create());

    partitionedRegion = new PartitionedRegion("region", attributesFactory.create(), null, cache,
        mock(InternalRegionArguments.class), disabledClock(), ColocationLoggerFactory.create());

    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);

    InternalDistributedMember imageTarget = mock(InternalDistributedMember.class);
    InternalRegionFactory factory = mock(InternalRegionFactory.class);
    DistributedRegion partitionedRegionRoot = mock(DistributedRegion.class);
    CacheDistributionAdvisor cda = mock(CacheDistributionAdvisor.class);
    PartitionedRegion.RegionLock regionLock = mock(PartitionedRegion.RegionLock.class);

    when(cache.createInternalRegionFactory(RegionShortcut.REPLICATE)).thenReturn(factory);
    when(factory.create(PR_ROOT_REGION_NAME)).thenReturn(partitionedRegionRoot);
    doNothing().when(cda).addMembershipListener(any());
    when(partitionedRegionRoot.getDistributionAdvisor()).thenReturn(cda);
    doReturn(regionLock).when(spyPartitionedRegion).getRegionLock();
    doThrow(new LockServiceDestroyedException("Lock Service is destroyed in test")).when(regionLock)
        .lock();
    doThrow(new DistributedSystemDisconnectedException("test")).when(spyPartitionedRegion)
        .cleanupFailedInitialization();

    assertThatThrownBy(() -> spyPartitionedRegion.initialize(null, imageTarget, null))
        .isInstanceOf(PartitionedRegionException.class)
        .hasCauseInstanceOf(DistributedSystemDisconnectedException.class);
  }

  @Test
  public void getDataRegionForWriteThrowsTransactionExceptionIfNotDataStore() {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);

    KeyInfo keyInfo = mock(KeyInfo.class);
    when(keyInfo.getBucketId()).thenReturn(1);
    doReturn(null).when(spyPartitionedRegion).getDataStore();

    Throwable caughtException =
        catchThrowable(() -> spyPartitionedRegion.getDataRegionForWrite(keyInfo));

    assertThat(caughtException).isInstanceOf(TransactionException.class).hasMessage(
        "PartitionedRegion Transactions cannot execute on nodes with local max memory zero");
  }

  @Test
  public void getDataRegionForWriteThrowsTransactionDataRebalancedExceptionIfGetInitializedBucketThrowsForceReattemptException()
      throws Exception {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);

    KeyInfo keyInfo = mock(KeyInfo.class);
    Object key = new Object();
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(keyInfo.getBucketId()).thenReturn(1);
    when(keyInfo.getKey()).thenReturn(key);
    when(keyInfo.isCheckPrimary()).thenReturn(true);
    doReturn(dataStore).when(spyPartitionedRegion).getDataStore();
    doThrow(new ForceReattemptException("")).when(dataStore)
        .getInitializedBucketWithKnownPrimaryForId(key, 1);
    doReturn(mock(InternalDistributedMember.class)).when(spyPartitionedRegion).createBucket(1, 0,
        null);

    Throwable caughtException =
        catchThrowable(() -> spyPartitionedRegion.getDataRegionForWrite(keyInfo));

    assertThat(caughtException).isInstanceOf(TransactionDataRebalancedException.class)
        .hasMessage(PartitionedRegion.DATA_MOVED_BY_REBALANCE);
  }

  @Test
  public void getDataRegionForWriteThrowsTransactionDataRebalancedExceptionIfGetInitializedBucketThrowsRegionDestroyedException()
      throws Exception {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);

    KeyInfo keyInfo = mock(KeyInfo.class);
    Object key = new Object();
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    when(keyInfo.getBucketId()).thenReturn(1);
    when(keyInfo.getKey()).thenReturn(key);
    doReturn(dataStore).when(spyPartitionedRegion).getDataStore();
    doThrow(new RegionDestroyedException("", "")).when(dataStore)
        .getInitializedBucketWithKnownPrimaryForId(key, 1);

    Throwable caughtException =
        catchThrowable(() -> spyPartitionedRegion.getDataRegionForWrite(keyInfo));

    assertThat(caughtException).isInstanceOf(TransactionDataRebalancedException.class)
        .hasMessage(PartitionedRegion.DATA_MOVED_BY_REBALANCE);
  }

  @Test
  public void transactionThrowsTransactionDataRebalancedExceptionIfBucketNotFoundException() {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    ForceReattemptException exception = mock(BucketNotFoundException.class);

    Throwable caughtException =
        catchThrowable(
            () -> spyPartitionedRegion.handleForceReattemptExceptionWithTransaction(exception));

    assertThat(caughtException).isInstanceOf(TransactionDataRebalancedException.class)
        .hasMessage(PartitionedRegion.DATA_MOVED_BY_REBALANCE);
  }

  @Test
  public void transactionThrowsPrimaryBucketExceptionIfForceReattemptExceptionIsCausedByPrimaryBucketException() {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    ForceReattemptException exception = mock(ForceReattemptException.class);
    PrimaryBucketException primaryBucketException = new PrimaryBucketException();
    when(exception.getCause()).thenReturn(primaryBucketException);

    Throwable caughtException =
        catchThrowable(
            () -> spyPartitionedRegion.handleForceReattemptExceptionWithTransaction(exception));

    assertThat(caughtException).isSameAs(primaryBucketException);
  }

  @Test
  public void transactionThrowsTransactionDataRebalancedExceptionIfForceReattemptExceptionIsCausedByTransactionDataRebalancedException() {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    ForceReattemptException exception = mock(ForceReattemptException.class);
    TransactionDataRebalancedException transactionDataRebalancedException =
        new TransactionDataRebalancedException("");
    when(exception.getCause()).thenReturn(transactionDataRebalancedException);

    Throwable caughtException =
        catchThrowable(
            () -> spyPartitionedRegion.handleForceReattemptExceptionWithTransaction(exception));

    assertThat(caughtException).isSameAs(transactionDataRebalancedException);
  }

  @Test
  public void transactionThrowsTransactionDataRebalancedExceptionIfForceReattemptExceptionIsCausedByRegionDestroyedException() {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    ForceReattemptException exception = mock(ForceReattemptException.class);
    RegionDestroyedException regionDestroyedException = new RegionDestroyedException("", "");
    when(exception.getCause()).thenReturn(regionDestroyedException);

    Throwable caughtException =
        catchThrowable(
            () -> spyPartitionedRegion.handleForceReattemptExceptionWithTransaction(exception));

    assertThat(caughtException).isInstanceOf(TransactionDataRebalancedException.class)
        .hasMessage(PartitionedRegion.DATA_MOVED_BY_REBALANCE).hasCause(regionDestroyedException);
  }

  @Test
  public void transactionThrowsTransactionDataRebalancedExceptionIfIsAForceReattemptException() {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    ForceReattemptException exception = mock(ForceReattemptException.class);

    Throwable caughtException =
        catchThrowable(
            () -> spyPartitionedRegion.handleForceReattemptExceptionWithTransaction(exception));

    assertThat(caughtException).isInstanceOf(TransactionDataRebalancedException.class)
        .hasMessage(PartitionedRegion.DATA_MOVED_BY_REBALANCE).hasCause(exception);
  }

  @Test
  public void testGetRegionCreateNotification() {
    partitionedRegion = new PartitionedRegion("region", attributesFactory.create(), null, cache,
        mock(InternalRegionArguments.class), disabledClock(), ColocationLoggerFactory.create());

    assertThat(partitionedRegion.isRegionCreateNotified()).isFalse();

    partitionedRegion.setRegionCreateNotified(true);

    assertThat(partitionedRegion.isRegionCreateNotified()).isTrue();
  }

  @Test
  public void testNotifyRegionCreated() {
    partitionedRegion = new PartitionedRegion("region", attributesFactory.create(), null, cache,
        mock(InternalRegionArguments.class), disabledClock(), ColocationLoggerFactory.create());

    assertThat(partitionedRegion.isRegionCreateNotified()).isFalse();

    partitionedRegion.notifyRegionCreated();

    assertThat(partitionedRegion.isRegionCreateNotified()).isTrue();
  }

  @Test
  public void populateEmptyIndexesThrowsIfBucketRegionDestroyedDueToCacheClose() {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    BucketRegion bucketRegion = mock(BucketRegion.class);
    when(bucketRegion.isDestroyed()).thenReturn(true);

    Set<Index> indexes = setupIndexes();
    ConcurrentMap<Integer, BucketRegion> map = setupBuckets(bucketRegion);
    setupDataStore(spyPartitionedRegion, map);
    CacheClosedException cacheClosedException = new CacheClosedException();
    setupCancelCriterion(cacheClosedException);

    assertThatThrownBy(() -> spyPartitionedRegion.populateEmptyIndexes(indexes, new HashMap<>()))
        .isEqualTo(cacheClosedException);
  }

  @NotNull
  private Set<Index> setupIndexes() {
    Set<Index> indexes = new HashSet<>();
    Index index = mock(Index.class);
    indexes.add(index);
    return indexes;
  }

  @NotNull
  private ConcurrentMap<Integer, BucketRegion> setupBuckets(BucketRegion bucketRegion) {
    ConcurrentMap<Integer, BucketRegion> map = new ConcurrentHashMap<>();
    map.put(1, bucketRegion);
    return map;
  }

  private void setupDataStore(PartitionedRegion spyPartitionedRegion,
      ConcurrentMap<Integer, BucketRegion> map) {
    PartitionedRegionDataStore dataStore = mock(PartitionedRegionDataStore.class);
    doReturn(dataStore).when(spyPartitionedRegion).getDataStore();
    when(dataStore.getAllLocalBuckets()).thenReturn(map.entrySet());
  }

  private void setupCancelCriterion(CacheClosedException cacheClosedException) {
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(cache.getCancelCriterion()).thenReturn(cancelCriterion);
    doThrow(cacheClosedException).when(cancelCriterion).checkCancelInProgress();
  }

  @Test
  public void populateEmptyIndexesReturnsFalseIfIndexManagerPopulateIndexesSuccessfully() {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    IndexManager indexManager = mock(IndexManager.class);
    BucketRegion bucketRegion = mock(BucketRegion.class);
    when(bucketRegion.getIndexManager()).thenReturn(indexManager);

    Set<Index> indexes = setupIndexes();
    ConcurrentMap<Integer, BucketRegion> map = setupBuckets(bucketRegion);
    setupDataStore(spyPartitionedRegion, map);
    doReturn(indexes).when(spyPartitionedRegion).getBucketIndexesForPRIndexes(bucketRegion,
        indexes);

    assertThat(spyPartitionedRegion.populateEmptyIndexes(indexes, new HashMap<>())).isFalse();
  }

  @Test
  public void populateEmptyIndexesReturnsTrueIfIndexManagerPopulateIndexesThrows()
      throws Exception {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    IndexManager indexManager = mock(IndexManager.class);
    BucketRegion bucketRegion = mock(BucketRegion.class);
    when(bucketRegion.getIndexManager()).thenReturn(indexManager);
    MultiIndexCreationException exception = mock(MultiIndexCreationException.class);

    Set<Index> indexes = setupIndexes();
    ConcurrentMap<Integer, BucketRegion> map = setupBuckets(bucketRegion);
    setupDataStore(spyPartitionedRegion, map);
    doReturn(indexes).when(spyPartitionedRegion).getBucketIndexesForPRIndexes(bucketRegion,
        indexes);
    doThrow(exception).when(indexManager).populateIndexes(indexes);

    assertThat(spyPartitionedRegion.populateEmptyIndexes(indexes, new HashMap<>())).isTrue();
  }

  @Test
  public void createEmptyIndexesCanAddIndexToIndexSet() throws Exception {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    HashSet<IndexCreationData> indexDefinitions = setupIndexCreationData();
    Set<Index> indexes = new HashSet<>();
    HashMap<String, Exception> exceptionsMap = new HashMap<>();
    doReturn(index).when(spyPartitionedRegion).createIndex(true, null, indexName, null, null, null,
        false, false);

    assertThat(
        spyPartitionedRegion.createEmptyIndexes(indexDefinitions, true, indexes, exceptionsMap))
            .isFalse();
    assertThat(indexes).contains(index);
    assertThat(exceptionsMap).hasSize(0);
  }

  @Test
  public void createEmptyIndexesThrowsIfCreateIndexFailedWithCacheClosedException()
      throws Exception {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    HashSet<IndexCreationData> indexDefinitions = setupIndexCreationData();
    CacheClosedException exception = new CacheClosedException();
    doThrow(exception).when(spyPartitionedRegion).createIndex(true, null, indexName, null, null,
        null, false, false);

    assertThatThrownBy(() -> spyPartitionedRegion.createEmptyIndexes(indexDefinitions, true,
        new HashSet<>(), new HashMap<>())).isEqualTo(exception);
  }

  @Test
  public void createEmptyIndexesAddsExceptionToMapIfCreateIndexFailed() throws Exception {
    PartitionedRegion spyPartitionedRegion = spy(partitionedRegion);
    HashSet<IndexCreationData> indexDefinitions = setupIndexCreationData();
    Set<Index> indexes = new HashSet<>();
    HashMap<String, Exception> exceptionsMap = new HashMap<>();
    RuntimeException runtimeException = new RuntimeException();
    doThrow(runtimeException).when(spyPartitionedRegion).createIndex(true, null, indexName, null,
        null, null, false, false);

    assertThat(
        spyPartitionedRegion.createEmptyIndexes(indexDefinitions, true, indexes, exceptionsMap))
            .isTrue();
    assertThat(indexes).hasSize(0);
    assertThat(exceptionsMap.get(indexName)).isEqualTo(runtimeException);
  }

  @NotNull
  private HashSet<IndexCreationData> setupIndexCreationData() {
    HashSet<IndexCreationData> indexDefinitions = new HashSet<>();
    IndexCreationData indexCreationData = new IndexCreationData(indexName);
    indexDefinitions.add(indexCreationData);
    return indexDefinitions;
  }

  private static <K> Set<K> asSet(K... values) {
    Set<K> set = new HashSet<>();
    Collections.addAll(set, values);
    return set;
  }

  private static <K, V> Map<K, Set<V>> asMapOfSet(K key, V... values) {
    Map<K, Set<V>> map = new HashMap<>();
    map.put(key, asSet(values));
    return map;
  }
}
