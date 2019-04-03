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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.util.VersionedArrayList;
import org.apache.geode.test.fake.Fakes;

@RunWith(JUnitParamsRunner.class)
public class PartitionedRegionTest {

  String regionName = "prTestRegion";

  InternalCache internalCache;

  PartitionedRegion partitionedRegion;

  Properties gemfireProperties = new Properties();

  @Before
  public void setup() {
    internalCache = Fakes.cache();

    InternalResourceManager resourceManager =
        mock(InternalResourceManager.class, RETURNS_DEEP_STUBS);
    when(internalCache.getInternalResourceManager()).thenReturn(resourceManager);
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(
        new PartitionAttributesFactory().setTotalNumBuckets(1).setRedundantCopies(1).create());
    partitionedRegion = new PartitionedRegion(regionName, attributesFactory.create(),
        null, internalCache, mock(InternalRegionArguments.class));
    DistributedSystem mockDistributedSystem = mock(DistributedSystem.class);
    when(internalCache.getDistributedSystem()).thenReturn(mockDistributedSystem);
    when(mockDistributedSystem.getProperties()).thenReturn(gemfireProperties);
  }

  @Test
  @Parameters(method = "parametersToTestUpdatePRNodeInformation")
  public void verifyPRConfigUpdatedAfterLoaderUpdate(CacheLoader mockLoader, CacheWriter mockWriter,
      byte configByte) {
    LocalRegion prRoot = mock(LocalRegion.class);
    PartitionRegionConfig mockConfig = mock(PartitionRegionConfig.class);
    PartitionedRegion prSpy = spy(partitionedRegion);

    doReturn(prRoot).when(prSpy).getPRRoot();
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
    when(ourNode.isCacheLoaderAttached()).thenReturn(mockLoader == null ? false : true);
    when(ourNode.isCacheWriterAttached()).thenReturn(mockWriter == null ? false : true);


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
    assertThat(mockConfig.getNodes().contains(ourNode));
    for (Node node : mockConfig.getNodes()) {
      if (node.getMemberId().equals(ourMember)) {
        verifyOurNode = node;
      }
    }

    verify(prRoot).get(prSpy.getRegionIdentifier());
    verify(prSpy).updatePRConfig(mockConfig, false);
    verify(prRoot).put(prSpy.getRegionIdentifier(), mockConfig);

    assertThat(verifyOurNode).isNotNull();
    assertThat(verifyOurNode.isCacheLoaderAttached()).isEqualTo(mockLoader == null ? false : true);
    assertThat(verifyOurNode.isCacheWriterAttached()).isEqualTo(mockWriter == null ? false : true);
  }

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
  public void getBucketNodeForReadOrWriteReturnsPrimaryNodeForRegisterInterest() throws Exception {
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
  public void getBucketNodeForReadOrWriteReturnsSecondaryNodeForNonRegisterInterest()
      throws Exception {
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
  public void getBucketNodeForReadOrWriteReturnsSecondaryNodeWhenClientEventIsNotPresent()
      throws Exception {
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
  public void getBucketNodeForReadOrWriteReturnsSecondaryNodeWhenClientEventOperationIsNotPresent()
      throws Exception {
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
    HashMap<InternalDistributedMember, HashSet<Integer>> nodeToBuckets =
        new HashMap<InternalDistributedMember, HashSet<Integer>>();
    Set buckets = Arrays.stream(bucketIds).collect(Collectors.toCollection(HashSet::new));

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
    HashMap<InternalDistributedMember, HashMap<Integer, HashSet>> nodeToBuckets =
        new HashMap<InternalDistributedMember, HashMap<Integer, HashSet>>();
    HashSet buckets = Arrays.stream(bucketIds).collect(Collectors.toCollection(HashSet::new));
    HashMap<Integer, HashSet> bucketKeys = new HashMap<>();
    bucketKeys.put(Integer.valueOf(0), buckets);

    spyPR.updateNodeToBucketMap(nodeToBuckets, bucketKeys);

    verify(spyPR, times(1)).getNodeForBucketWrite(anyInt(), isNull());
  }

}
