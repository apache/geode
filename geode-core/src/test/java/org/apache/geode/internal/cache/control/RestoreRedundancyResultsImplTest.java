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

import static org.apache.geode.cache.control.RegionRedundancyStatus.RedundancyStatus.NOT_SATISFIED;
import static org.apache.geode.cache.control.RegionRedundancyStatus.RedundancyStatus.NO_REDUNDANT_COPIES;
import static org.apache.geode.cache.control.RegionRedundancyStatus.RedundancyStatus.SATISFIED;
import static org.apache.geode.cache.control.RestoreRedundancyResults.Status.FAILURE;
import static org.apache.geode.cache.control.RestoreRedundancyResults.Status.SUCCESS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.NO_REDUNDANT_COPIES_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.PRIMARY_TRANSFERS_COMPLETED;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.PRIMARY_TRANSFER_TIME;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_NOT_SATISFIED_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_SATISFIED_FOR_REGIONS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.control.RegionRedundancyStatus;
import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;

public class RestoreRedundancyResultsImplTest {
  private final RegionRedundancyStatus successfulRegionResult =
      mock(RegionRedundancyStatusImpl.class);
  private final String successfulRegionName = "successfulRegion";
  private final RegionRedundancyStatus underRedundancyRegionResult =
      mock(RegionRedundancyStatusImpl.class);
  private final String underRedundancyRegionName = "underRedundancyRegion";
  private final RegionRedundancyStatus zeroRedundancyRegionResult =
      mock(RegionRedundancyStatusImpl.class);
  private final String zeroRedundancyRegionName = "zeroRedundancyRegion";
  private PartitionRebalanceInfo details = mock(PartitionRebalanceInfo.class);
  private int transfersCompleted = 5;
  private long transferTime = 1234;
  private RestoreRedundancyResultsImpl results;

  @Before
  public void setUp() {
    when(successfulRegionResult.getStatus()).thenReturn(SATISFIED);
    when(successfulRegionResult.getRegionName()).thenReturn(successfulRegionName);
    when(underRedundancyRegionResult.getStatus()).thenReturn(NOT_SATISFIED);
    when(underRedundancyRegionResult.getRegionName()).thenReturn(underRedundancyRegionName);
    when(zeroRedundancyRegionResult.getStatus()).thenReturn(NO_REDUNDANT_COPIES);
    when(zeroRedundancyRegionResult.getRegionName()).thenReturn(zeroRedundancyRegionName);
    when(details.getPrimaryTransfersCompleted()).thenReturn(transfersCompleted);
    when(details.getPrimaryTransferTime()).thenReturn(transferTime);
    results = new RestoreRedundancyResultsImpl();
  }

  @Test
  public void getStatusReturnsSuccessWhenAllRegionsHaveFullySatisfiedRedundancy() {
    results.addRegionResult(successfulRegionResult);

    assertThat(results.getStatus(), is(SUCCESS));
  }

  @Test
  public void getStatusReturnsFailureNotAllRegionsHaveFullySatisfiedRedundancy() {
    results.addRegionResult(successfulRegionResult);
    results.addRegionResult(underRedundancyRegionResult);

    assertThat(results.getStatus(), is(FAILURE));
  }

  @Test
  public void getStatusReturnsFailureWhenAtLeastOneRegionHasNoRedundancy() {
    results.addRegionResult(successfulRegionResult);
    results.addRegionResult(zeroRedundancyRegionResult);

    assertThat(results.getStatus(), is(FAILURE));
  }

  @Test
  public void getMessageReturnsStatusForAllRegionsAndPrimaryInfo() {
    results.addRegionResult(successfulRegionResult);
    results.addRegionResult(underRedundancyRegionResult);
    results.addRegionResult(zeroRedundancyRegionResult);

    results.addPrimaryReassignmentDetails(details);

    String message = results.getMessage();
    List<String> messageLines = Arrays.asList(message.split("\n"));

    assertThat(messageLines, contains(NO_REDUNDANT_COPIES_FOR_REGIONS,
        zeroRedundancyRegionResult.toString(),
        REDUNDANCY_NOT_SATISFIED_FOR_REGIONS,
        underRedundancyRegionResult.toString(),
        REDUNDANCY_SATISFIED_FOR_REGIONS,
        successfulRegionResult.toString(),
        PRIMARY_TRANSFERS_COMPLETED + transfersCompleted,
        PRIMARY_TRANSFER_TIME + transferTime));
  }

  @Test
  public void addRegionResultAddsToCorrectInternalMap() {
    results.addRegionResult(zeroRedundancyRegionResult);
    results.addRegionResult(underRedundancyRegionResult);
    results.addRegionResult(successfulRegionResult);

    Map<String, RegionRedundancyStatus> zeroRedundancyResults =
        results.getZeroRedundancyRegionResults();
    assertThat(zeroRedundancyResults.size(), is(1));
    assertThat(zeroRedundancyResults.get(zeroRedundancyRegionName), is(zeroRedundancyRegionResult));

    Map<String, RegionRedundancyStatus> underRedundancyResults =
        results.getUnderRedundancyRegionResults();
    assertThat(underRedundancyResults.size(), is(1));
    assertThat(underRedundancyResults.get(underRedundancyRegionName),
        is(underRedundancyRegionResult));

    Map<String, RegionRedundancyStatus> successfulRegionResults =
        results.getSatisfiedRedundancyRegionResults();
    assertThat(successfulRegionResults.size(), is(1));
    assertThat(successfulRegionResults.get(successfulRegionName), is(successfulRegionResult));
  }

  @Test
  public void addRegionResultsAddsToCorrectInternalMapAndAddsPrimaryReassignmentDetails() {
    RestoreRedundancyResults regionResults = mock(RestoreRedundancyResults.class);
    when(regionResults.getZeroRedundancyRegionResults())
        .thenReturn(Collections.singletonMap(zeroRedundancyRegionName, zeroRedundancyRegionResult));
    when(regionResults.getUnderRedundancyRegionResults()).thenReturn(
        Collections.singletonMap(underRedundancyRegionName, underRedundancyRegionResult));
    when(regionResults.getSatisfiedRedundancyRegionResults())
        .thenReturn(Collections.singletonMap(successfulRegionName, successfulRegionResult));
    when(regionResults.getTotalPrimaryTransfersCompleted()).thenReturn(transfersCompleted);
    when(regionResults.getTotalPrimaryTransferTime()).thenReturn(Duration.ofMillis(transferTime));

    results.addRegionResults(regionResults);

    Map<String, RegionRedundancyStatus> zeroRedundancyResults =
        results.getZeroRedundancyRegionResults();
    assertThat(zeroRedundancyResults.size(), is(1));
    assertThat(zeroRedundancyResults.get(zeroRedundancyRegionName), is(zeroRedundancyRegionResult));

    Map<String, RegionRedundancyStatus> underRedundancyResults =
        results.getUnderRedundancyRegionResults();
    assertThat(underRedundancyResults.size(), is(1));
    assertThat(underRedundancyResults.get(underRedundancyRegionName),
        is(underRedundancyRegionResult));

    Map<String, RegionRedundancyStatus> successfulRegionResults =
        results.getSatisfiedRedundancyRegionResults();
    assertThat(successfulRegionResults.size(), is(1));
    assertThat(successfulRegionResults.get(successfulRegionName), is(successfulRegionResult));

    assertThat(results.getTotalPrimaryTransfersCompleted(), is(transfersCompleted));
    assertThat(results.getTotalPrimaryTransferTime().toMillis(), is(transferTime));
  }

  @Test
  public void addPrimaryDetailsUpdatesValue() {
    assertThat(results.getTotalPrimaryTransfersCompleted(), is(0));
    assertThat(results.getTotalPrimaryTransferTime().toMillis(), is(0L));
    results.addPrimaryReassignmentDetails(details);
    assertThat(results.getTotalPrimaryTransfersCompleted(), is(transfersCompleted));
    assertThat(results.getTotalPrimaryTransferTime().toMillis(), is(transferTime));
    results.addPrimaryReassignmentDetails(details);
    assertThat(results.getTotalPrimaryTransfersCompleted(), is(transfersCompleted * 2));
    assertThat(results.getTotalPrimaryTransferTime().toMillis(), is(transferTime * 2));
  }
}
