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
package org.apache.geode.internal.cache.control;

import static org.apache.geode.cache.control.RestoreRedundancyResults.Status.ERROR;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionRebalanceOp;
import org.apache.geode.internal.serialization.Version;

public class RestoreRedundancyBuilderImplTest {
  InternalCache cache;
  InternalResourceManager manager;
  ResourceManagerStats stats;
  RestoreRedundancyBuilderImpl builder;
  RestoreRedundancyResults emptyResults;
  long startTime = 5;

  @Before
  public void setUp() {
    cache = mock(InternalCache.class, RETURNS_DEEP_STUBS);
    manager = mock(InternalResourceManager.class);
    stats = mock(ResourceManagerStats.class);
    when(cache.getInternalResourceManager()).thenReturn(manager);
    when(manager.getStats()).thenReturn(stats);
    when(stats.startRestoreRedundancy()).thenReturn(startTime);

    // This allows us to pass the hasMemberOlderThanGeode_1_13_0() method
    InternalDistributedMember currentMember = mock(InternalDistributedMember.class);
    when(currentMember.getVersionObject()).thenReturn(Version.CURRENT);
    Set<DistributedMember> members = Collections.singleton(currentMember);
    when(cache.getMembers()).thenReturn(members);

    builder = spy(new RestoreRedundancyBuilderImpl(cache));

    emptyResults = mock(RestoreRedundancyResults.class);
    doReturn(emptyResults).when(builder).getEmptyRestoreRedundancyResults();
  }

  @Test
  public void doRestoreRedundancyReturnsEmptyResultsWhenRegionDestroyedExceptionIsThrown() {
    PartitionedRegion region = mock(PartitionedRegion.class);
    doThrow(new RegionDestroyedException("message", "/regionPath")).when(builder)
        .getPartitionedRegionRebalanceOp(region);

    assertThat(builder.doRestoreRedundancy(region), is(emptyResults));
  }

  @Test
  public void doRestoreRedundancyAddsRegionResultForRegionIfDetailSetIsEmpty() {
    PartitionedRegion region = mock(PartitionedRegion.class);

    PartitionedRegionRebalanceOp op = mock(PartitionedRegionRebalanceOp.class);
    doReturn(op).when(builder).getPartitionedRegionRebalanceOp(region);
    when(op.execute()).thenReturn(new HashSet<>());

    RestoreRedundancyRegionResult regionResult = mock(RestoreRedundancyRegionResult.class);
    doReturn(regionResult).when(builder).getRegionResult(region);

    builder.doRestoreRedundancy(region);

    verify(emptyResults, times(1)).addRegionResult(regionResult);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void doRestoreRedundancyAddsRegionResultAndPrimaryDetailsWhenDetailSetIsNotEmpty() {
    PartitionedRegion region = mock(PartitionedRegion.class);

    PartitionedRegionRebalanceOp op = mock(PartitionedRegionRebalanceOp.class);
    doReturn(op).when(builder).getPartitionedRegionRebalanceOp(region);

    PartitionRebalanceInfo details1 = mock(PartitionRebalanceInfo.class);
    String regionPath1 = "/region1";
    when(details1.getRegionPath()).thenReturn(regionPath1);
    PartitionRebalanceInfo details2 = mock(PartitionRebalanceInfo.class);
    String regionPath2 = "/region2";
    when(details2.getRegionPath()).thenReturn(regionPath2);

    Set<PartitionRebalanceInfo> detailSet = new HashSet<>();
    detailSet.add(details1);
    detailSet.add(details2);

    when(op.execute()).thenReturn(detailSet);

    PartitionedRegion detailRegion1 = mock(PartitionedRegion.class);
    PartitionedRegion detailRegion2 = mock(PartitionedRegion.class);
    when(cache.getRegion(regionPath1)).thenReturn(detailRegion1);
    when(cache.getRegion(regionPath2)).thenReturn(detailRegion2);

    RestoreRedundancyRegionResult regionResult1 = mock(RestoreRedundancyRegionResult.class);
    RestoreRedundancyRegionResult regionResult2 = mock(RestoreRedundancyRegionResult.class);
    doReturn(regionResult1).when(builder).getRegionResult(detailRegion1);
    doReturn(regionResult2).when(builder).getRegionResult(detailRegion2);

    builder.doRestoreRedundancy(region);

    verify(emptyResults, times(1)).addRegionResult(regionResult1);
    verify(emptyResults, times(1)).addRegionResult(regionResult2);
    verify(emptyResults, times(1)).addPrimaryReassignmentDetails(details1);
    verify(emptyResults, times(1)).addPrimaryReassignmentDetails(details2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void getRestoreRedundancyResultsReturnsCombinedResultsFromAllFutures() {
    CompletableFuture<RestoreRedundancyResults> future1 = mock(CompletableFuture.class);
    RestoreRedundancyResults result1 = mock(RestoreRedundancyResults.class);
    when(future1.join()).thenReturn(result1);
    CompletableFuture<RestoreRedundancyResults> future2 = mock(CompletableFuture.class);
    RestoreRedundancyResults result2 = mock(RestoreRedundancyResults.class);
    when(future2.join()).thenReturn(result2);

    List<CompletableFuture<RestoreRedundancyResults>> futures = new ArrayList<>();
    futures.add(future1);
    futures.add(future2);

    builder.getRestoreRedundancyResults(futures);

    verify(emptyResults, times(1)).addRegionResults(result1);
    verify(emptyResults, times(1)).addRegionResults(result2);
  }

  @Test
  public void startReturnsErrorRestoreRedundancyResultWhenSystemHasOldMember()
      throws ExecutionException, InterruptedException {
    InternalDistributedMember oldMember = mock(InternalDistributedMember.class);
    when(oldMember.getVersionObject()).thenReturn(Version.GEODE_1_12_0);
    Set<DistributedMember> members = Collections.singleton(oldMember);
    when(cache.getMembers()).thenReturn(members);

    CompletableFuture<RestoreRedundancyResults> resultFuture = builder.start();
    RestoreRedundancyResults result = resultFuture.get();

    assertThat(result.getStatus(), is(ERROR));
    assertThat(result.getMessage(), containsString(Version.GEODE_1_13_0.toString()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void startCreatesRedundancyOpFutureForAllIncludedRegions() {
    RegionFilter filter = mock(RegionFilter.class);
    doReturn(filter).when(builder).getRegionFilter();

    PartitionedRegion includeRegion = mock(PartitionedRegion.class);
    PartitionedRegion excludeRegion = mock(PartitionedRegion.class);
    Set<PartitionedRegion> regions = new HashSet<>();
    regions.add(includeRegion);
    regions.add(excludeRegion);
    when(cache.getPartitionedRegions()).thenReturn(regions);

    when(filter.include(includeRegion)).thenReturn(true);
    when(filter.include(excludeRegion)).thenReturn(false);

    CompletableFuture<RestoreRedundancyResults> redundancyOpFuture = mock(CompletableFuture.class);
    doReturn(redundancyOpFuture).when(builder).getRedundancyOpFuture(any());

    CompletableFuture<RestoreRedundancyResults> resultsFuture = mock(CompletableFuture.class);
    doReturn(resultsFuture).when(builder).getResultsFuture(any(), any());

    builder.start();

    verify(builder, times(1)).getRedundancyOpFuture(includeRegion);
    verify(builder, times(0)).getRedundancyOpFuture(excludeRegion);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void startAddsInProgressRestoreRedundancyAndRemovesInProgressRestoreRedundancyAndUpdatesStatsOnCompletion() {
    RegionFilter filter = mock(RegionFilter.class);
    doReturn(filter).when(builder).getRegionFilter();

    PartitionedRegion includeRegion = mock(PartitionedRegion.class);
    when(cache.getPartitionedRegions()).thenReturn(Collections.singleton(includeRegion));

    when(filter.include(includeRegion)).thenReturn(true);

    CompletableFuture<RestoreRedundancyResults> redundancyOpFuture = mock(CompletableFuture.class);
    doReturn(redundancyOpFuture).when(builder).getRedundancyOpFuture(any());

    CompletableFuture<RestoreRedundancyResults> resultsFuture =
        CompletableFuture.completedFuture(null);
    doReturn(resultsFuture).when(builder).getResultsFuture(any(), any());

    builder.start().join();

    verify(manager, times(1)).addInProgressRestoreRedundancy(resultsFuture);
    verify(manager, times(1)).removeInProgressRestoreRedundancy(resultsFuture);
    verify(stats, times(1)).endRestoreRedundancy(startTime);
  }
}
