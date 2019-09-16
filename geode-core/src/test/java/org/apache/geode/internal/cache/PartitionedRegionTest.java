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

import static org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.util.VersionedArrayList;
import org.apache.geode.test.fake.Fakes;

@RunWith(JUnitParamsRunner.class)
public class PartitionedRegionTest {
  private InternalCache internalCache;
  private PartitionedRegion partitionedRegion;
  @SuppressWarnings("deprecation")
  private AttributesFactory attributesFactory;

  @Before
  @SuppressWarnings("deprecation")
  public void setup() {
    internalCache = Fakes.cache();

    InternalResourceManager resourceManager =
        mock(InternalResourceManager.class, RETURNS_DEEP_STUBS);
    when(internalCache.getInternalResourceManager()).thenReturn(resourceManager);
    attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(
        new PartitionAttributesFactory().setTotalNumBuckets(1).setRedundantCopies(1).create());
    partitionedRegion = new PartitionedRegion("prTestRegion", attributesFactory.create(), null,
        internalCache, mock(InternalRegionArguments.class), disabledClock());
    DistributedSystem mockDistributedSystem = mock(DistributedSystem.class);
    when(internalCache.getDistributedSystem()).thenReturn(mockDistributedSystem);
    when(mockDistributedSystem.getProperties()).thenReturn(new Properties());
    when(mockDistributedSystem.createAtomicStatistics(any(), any()))
        .thenReturn(mock(Statistics.class));
  }

  @SuppressWarnings("unused")
  private Object[] parametersToTestUpdatePRNodeInformation() {
    CacheLoader mockLoader = mock(CacheLoader.class);
    CacheWriter mockWriter = mock(CacheWriter.class);
    return new Object[] {
        new Object[] {mockLoader, null, (byte) 0x01},
        new Object[] {null, mockWriter, (byte) 0x02},
        new Object[] {mockLoader, mockWriter, (byte) 0x03},
        new Object[] {null, null, (byte) 0x00}
    };
  }

  @Test
  @Parameters(method = "parametersToTestUpdatePRNodeInformation")
  public void verifyPRConfigUpdatedAfterLoaderUpdate(CacheLoader mockLoader, CacheWriter mockWriter,
      @SuppressWarnings("unused") byte configByte) {
    @SuppressWarnings("unchecked")
    Region<String, PartitionRegionConfig> prRoot = mock(LocalRegion.class);
    PartitionRegionConfig mockConfig = mock(PartitionRegionConfig.class);
    PartitionedRegion prSpy = spy(partitionedRegion);

    when(prSpy.getPRRoot()).thenReturn(prRoot);
    when(prRoot.get(prSpy.getRegionIdentifier())).thenReturn(mockConfig);

    InternalDistributedMember ourMember = prSpy.getDistributionManager().getId();
    InternalDistributedMember otherMember1 = mock(InternalDistributedMember.class);
    InternalDistributedMember otherMember2 = mock(InternalDistributedMember.class);
    Node ourNode = mock(Node.class);
    Node otherNode1 = mock(Node.class);
    Node otherNode2 = mock(Node.class);
    when(ourNode.getMemberId()).thenReturn(ourMember);
    when(otherNode1.getMemberId()).thenReturn(otherMember1);
    when(otherNode2.getMemberId()).thenReturn(otherMember2);
    when(ourNode.isCacheLoaderAttached()).thenReturn(mockLoader != null);
    when(ourNode.isCacheWriterAttached()).thenReturn(mockWriter != null);


    VersionedArrayList prNodes = new VersionedArrayList();
    prNodes.add(otherNode1);
    prNodes.add(ourNode);
    prNodes.add(otherNode2);
    when(mockConfig.getNodes()).thenReturn(prNodes.getListCopy());
    when(mockConfig.getPartitionAttrs()).thenReturn(mock(PartitionAttributesImpl.class));

    doReturn(mockLoader).when(prSpy).basicGetLoader();
    doReturn(mockWriter).when(prSpy).basicGetWriter();
    PartitionedRegion.RegionLock mockLock = mock(PartitionedRegion.RegionLock.class);
    doReturn(mockLock).when(prSpy).getRegionLock();

    prSpy.updatePRNodeInformation();

    Node verifyOurNode = null;
    assertThat(mockConfig.getNodes().contains(ourNode)).isTrue();
    for (Node node : mockConfig.getNodes()) {
      if (node.getMemberId().equals(ourMember)) {
        verifyOurNode = node;
      }
    }

    verify(prRoot).get(prSpy.getRegionIdentifier());
    verify(prSpy).updatePRConfig(mockConfig, false);
    verify(prRoot).put(prSpy.getRegionIdentifier(), mockConfig);

    assertThat(verifyOurNode).isNotNull();
    assertThat(verifyOurNode.isCacheLoaderAttached()).isEqualTo(mockLoader != null);
    assertThat(verifyOurNode.isCacheWriterAttached()).isEqualTo(mockWriter != null);
  }

  @Test
  public void getBucketNodeForReadOrWriteReturnsPrimaryNodeForRegisterInterest() {
    int bucketId = 0;
    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    EntryEventImpl clientEvent = mock(EntryEventImpl.class);
    when(clientEvent.getOperation()).thenReturn(Operation.GET_FOR_REGISTER_INTEREST);
    PartitionedRegion spyPR = spy(partitionedRegion);
    doReturn(primaryMember).when(spyPR).getNodeForBucketWrite(eq(bucketId), isNull());
    doReturn(secondaryMember).when(spyPR).getNodeForBucketRead(eq(bucketId));
    InternalDistributedMember memberForRegisterInterestRead =
        spyPR.getBucketNodeForReadOrWrite(bucketId, clientEvent);

    assertThat(memberForRegisterInterestRead).isSameAs(primaryMember);
    verify(spyPR, times(1)).getNodeForBucketWrite(anyInt(), any());
  }

  @Test
  public void getBucketNodeForReadOrWriteReturnsSecondaryNodeForNonRegisterInterest() {
    int bucketId = 0;
    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    EntryEventImpl clientEvent = mock(EntryEventImpl.class);
    when(clientEvent.getOperation()).thenReturn(Operation.GET);
    PartitionedRegion spyPR = spy(partitionedRegion);
    doReturn(primaryMember).when(spyPR).getNodeForBucketWrite(eq(bucketId), isNull());
    doReturn(secondaryMember).when(spyPR).getNodeForBucketRead(eq(bucketId));
    InternalDistributedMember memberForRegisterInterestRead =
        spyPR.getBucketNodeForReadOrWrite(bucketId, clientEvent);

    assertThat(memberForRegisterInterestRead).isSameAs(secondaryMember);
    verify(spyPR, times(1)).getNodeForBucketRead(anyInt());
  }

  @Test
  public void getBucketNodeForReadOrWriteReturnsSecondaryNodeWhenClientEventIsNotPresent() {
    int bucketId = 0;
    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    PartitionedRegion spyPR = spy(partitionedRegion);
    doReturn(primaryMember).when(spyPR).getNodeForBucketWrite(eq(bucketId), isNull());
    doReturn(secondaryMember).when(spyPR).getNodeForBucketRead(eq(bucketId));
    InternalDistributedMember memberForRegisterInterestRead =
        spyPR.getBucketNodeForReadOrWrite(bucketId, null);

    assertThat(memberForRegisterInterestRead).isSameAs(secondaryMember);
    verify(spyPR, times(1)).getNodeForBucketRead(anyInt());
  }

  @Test
  public void getBucketNodeForReadOrWriteReturnsSecondaryNodeWhenClientEventOperationIsNotPresent() {
    int bucketId = 0;
    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    EntryEventImpl clientEvent = mock(EntryEventImpl.class);
    when(clientEvent.getOperation()).thenReturn(null);
    PartitionedRegion spyPR = spy(partitionedRegion);
    doReturn(primaryMember).when(spyPR).getNodeForBucketWrite(eq(bucketId), isNull());
    doReturn(secondaryMember).when(spyPR).getNodeForBucketRead(eq(bucketId));
    InternalDistributedMember memberForRegisterInterestRead =
        spyPR.getBucketNodeForReadOrWrite(bucketId, null);

    assertThat(memberForRegisterInterestRead).isSameAs(secondaryMember);
    verify(spyPR, times(1)).getNodeForBucketRead(anyInt());
  }

  @Test
  public void updateBucketMapsForInterestRegistrationWithSetOfKeysFetchesPrimaryBucketsForRead() {
    Integer[] bucketIds = new Integer[] {0, 1};
    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    PartitionedRegion spyPR = spy(partitionedRegion);
    doReturn(primaryMember).when(spyPR).getNodeForBucketWrite(anyInt(), isNull());
    doReturn(secondaryMember).when(spyPR).getNodeForBucketRead(anyInt());
    HashMap<InternalDistributedMember, HashSet<Integer>> nodeToBuckets = new HashMap<>();
    Set<Integer> buckets = Arrays.stream(bucketIds).collect(Collectors.toCollection(HashSet::new));

    spyPR.updateNodeToBucketMap(nodeToBuckets, buckets);
    verify(spyPR, times(2)).getNodeForBucketWrite(anyInt(), isNull());
  }

  @Test
  public void updateBucketMapsForInterestRegistrationWithAllKeysFetchesPrimaryBucketsForRead() {
    Integer[] bucketIds = new Integer[] {0, 1};

    InternalDistributedMember primaryMember = mock(InternalDistributedMember.class);
    InternalDistributedMember secondaryMember = mock(InternalDistributedMember.class);
    PartitionedRegion spyPR = spy(partitionedRegion);
    doReturn(primaryMember).when(spyPR).getNodeForBucketWrite(anyInt(), isNull());
    doReturn(secondaryMember).when(spyPR).getNodeForBucketRead(anyInt());
    HashMap<InternalDistributedMember, HashMap<Integer, HashSet>> nodeToBuckets = new HashMap<>();
    HashSet buckets = Arrays.stream(bucketIds).collect(Collectors.toCollection(HashSet::new));
    HashMap<Integer, HashSet> bucketKeys = new HashMap<>();
    bucketKeys.put(0, buckets);

    spyPR.updateNodeToBucketMap(nodeToBuckets, bucketKeys);
    verify(spyPR, times(1)).getNodeForBucketWrite(anyInt(), isNull());
  }

  @Test
  public void filterOutNonParallelGatewaySendersShouldReturnCorrectly() {
    GatewaySender parallelSender = mock(GatewaySender.class);
    when(parallelSender.isParallel()).thenReturn(true);
    when(parallelSender.getId()).thenReturn("parallel");
    GatewaySender anotherParallelSender = mock(GatewaySender.class);
    when(anotherParallelSender.isParallel()).thenReturn(true);
    when(anotherParallelSender.getId()).thenReturn("anotherParallel");
    GatewaySender serialSender = mock(GatewaySender.class);
    when(serialSender.isParallel()).thenReturn(false);
    when(serialSender.getId()).thenReturn("serial");
    Set<GatewaySender> mockSenders =
        Stream.of(parallelSender, anotherParallelSender, serialSender).collect(Collectors.toSet());

    when(internalCache.getAllGatewaySenders()).thenReturn(mockSenders);
    assertThat(partitionedRegion
        .filterOutNonParallelGatewaySenders(Stream.of("serial").collect(Collectors.toSet())))
            .isEmpty();
    assertThat(partitionedRegion
        .filterOutNonParallelGatewaySenders(Stream.of("unknownSender").collect(Collectors.toSet())))
            .isEmpty();
    assertThat(partitionedRegion.filterOutNonParallelGatewaySenders(
        Stream.of("parallel", "serial").collect(Collectors.toSet()))).isNotEmpty()
            .containsExactly("parallel");
    assertThat(partitionedRegion.filterOutNonParallelGatewaySenders(
        Stream.of("parallel", "serial", "anotherParallel").collect(Collectors.toSet())))
            .isNotEmpty().containsExactly("parallel", "anotherParallel");
  }

  @Test
  public void filterOutNonParallelAsyncEventQueuesShouldReturnCorrectly() {
    AsyncEventQueue parallelQueue = mock(AsyncEventQueue.class);
    when(parallelQueue.isParallel()).thenReturn(true);
    when(parallelQueue.getId()).thenReturn(getSenderIdFromAsyncEventQueueId("parallel"));
    AsyncEventQueue anotherParallelQueue = mock(AsyncEventQueue.class);
    when(anotherParallelQueue.isParallel()).thenReturn(true);
    when(anotherParallelQueue.getId())
        .thenReturn(getSenderIdFromAsyncEventQueueId("anotherParallel"));
    AsyncEventQueue serialQueue = mock(AsyncEventQueue.class);
    when(serialQueue.isParallel()).thenReturn(false);
    when(serialQueue.getId()).thenReturn(getSenderIdFromAsyncEventQueueId("serial"));
    Set<AsyncEventQueue> mockQueues =
        Stream.of(parallelQueue, anotherParallelQueue, serialQueue).collect(Collectors.toSet());

    when(internalCache.getAsyncEventQueues()).thenReturn(mockQueues);
    assertThat(partitionedRegion
        .filterOutNonParallelAsyncEventQueues(Stream.of("serial").collect(Collectors.toSet())))
            .isEmpty();
    assertThat(partitionedRegion.filterOutNonParallelAsyncEventQueues(
        Stream.of("unknownSender").collect(Collectors.toSet()))).isEmpty();
    assertThat(partitionedRegion.filterOutNonParallelAsyncEventQueues(
        Stream.of("parallel", "serial").collect(Collectors.toSet()))).isNotEmpty()
            .containsExactly("parallel");
    assertThat(partitionedRegion.filterOutNonParallelAsyncEventQueues(
        Stream.of("parallel", "serial", "anotherParallel").collect(Collectors.toSet())))
            .isNotEmpty().containsExactly("parallel", "anotherParallel");
  }

  @Test
  public void getLocalSizeDoesNotThrowIfRegionUninitialized() {
    partitionedRegion = new PartitionedRegion("region", attributesFactory.create(), null,
        internalCache, mock(InternalRegionArguments.class), disabledClock());

    assertThatCode(partitionedRegion::getLocalSize).doesNotThrowAnyException();
  }

  @Test
  // See GEODE-7106
  public void generatePRIdShouldNotThrowNumberFormatExceptionIfAnErrorOccursWhileReleasingTheLock() {
    PartitionedRegion prSpy = spy(partitionedRegion);
    DistributedLockService mockLockService = mock(DistributedLockService.class);
    doReturn(true).when(mockLockService).lock(any(), anyLong(), anyLong());
    doThrow(new RuntimeException("Mock Exception")).when(mockLockService).unlock(any());

    InternalDistributedSystem mockSystem = mock(InternalDistributedSystem.class);
    when(mockSystem.getDistributionManager()).thenReturn(mock(DistributionManager.class));
    when(mockSystem.getDistributionManager().getCancelCriterion())
        .thenReturn(mock(CancelCriterion.class));
    when(mockSystem.getDistributionManager().getOtherDistributionManagerIds())
        .thenReturn(Collections.emptySet());

    doReturn(mockLockService).when(prSpy).getPartitionedRegionLockService();

    assertThatCode(() -> prSpy.generatePRId(mockSystem)).doesNotThrowAnyException();
  }
}
