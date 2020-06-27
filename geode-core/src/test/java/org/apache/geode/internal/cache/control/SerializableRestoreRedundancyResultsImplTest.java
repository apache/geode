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

import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.NO_REDUNDANT_COPIES_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.PRIMARY_TRANSFERS_COMPLETED;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.PRIMARY_TRANSFER_TIME;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.REDUNDANCY_NOT_SATISFIED_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.REDUNDANCY_SATISFIED_FOR_REGIONS;
import static org.apache.geode.management.runtime.RegionRedundancyStatus.RedundancyStatus.NOT_SATISFIED;
import static org.apache.geode.management.runtime.RegionRedundancyStatus.RedundancyStatus.NO_REDUNDANT_COPIES;
import static org.apache.geode.management.runtime.RegionRedundancyStatus.RedundancyStatus.SATISFIED;
import static org.apache.geode.management.runtime.RestoreRedundancyResults.Status.FAILURE;
import static org.apache.geode.management.runtime.RestoreRedundancyResults.Status.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.management.internal.operation.RegionRedundancyStatusImpl;
import org.apache.geode.management.internal.operation.RestoreRedundancyResultsImpl;
import org.apache.geode.management.runtime.RegionRedundancyStatus;
import org.apache.geode.management.runtime.RestoreRedundancyResults;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class SerializableRestoreRedundancyResultsImplTest {
  public static final String TEST_STRING = "Test";
  private final RegionRedundancyStatus successfulRegionResult =
      mock(SerializableRegionRedundancyStatusImpl.class);
  private final String successfulRegionName = "successfulRegion";
  private final RegionRedundancyStatus underRedundancyRegionResult =
      mock(SerializableRegionRedundancyStatusImpl.class);
  private final String underRedundancyRegionName = "underRedundancyRegion";
  private final RegionRedundancyStatus zeroRedundancyRegionResult =
      mock(SerializableRegionRedundancyStatusImpl.class);
  private final String zeroRedundancyRegionName = "zeroRedundancyRegion";
  private final PartitionRebalanceInfo details = mock(PartitionRebalanceInfo.class);
  private final int transfersCompleted = 5;
  private final long transferTime = 1234;
  private SerializableRestoreRedundancyResultsImpl results;
  private final ObjectMapper geodeMapper = GeodeJsonMapper.getMapper();

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
    results = new SerializableRestoreRedundancyResultsImpl();
  }

  @Test
  public void initialStateIsSuccess() {
    results = new SerializableRestoreRedundancyResultsImpl();
    assertThat(results.getRegionOperationStatus()).isEqualTo(SUCCESS);
  }

  @Test
  public void getStatusReturnsSuccessWhenAllRegionsHaveFullySatisfiedRedundancy() {
    results.addRegionResult(successfulRegionResult);

    assertThat(results.getRegionOperationStatus()).isEqualTo(SUCCESS);
  }

  @Test
  public void getStatusReturnsFailureNotAllRegionsHaveFullySatisfiedRedundancy() {
    results.addRegionResult(successfulRegionResult);
    results.addRegionResult(underRedundancyRegionResult);

    assertThat(results.getRegionOperationStatus()).isEqualTo(FAILURE);
  }

  @Test
  public void getStatusReturnsFailureWhenAtLeastOneRegionHasNoRedundancy() {
    results.addRegionResult(successfulRegionResult);
    results.addRegionResult(zeroRedundancyRegionResult);

    assertThat(results.getRegionOperationStatus()).isEqualTo(FAILURE);
  }

  @Test
  public void getMessageReturnsStatusForAllRegionsAndPrimaryInfo() {
    results.addRegionResult(successfulRegionResult);
    results.addRegionResult(underRedundancyRegionResult);
    results.addRegionResult(zeroRedundancyRegionResult);
    results.addPrimaryReassignmentDetails(details);

    String message = results.getRegionOperationMessage();
    List<String> messageLines = Arrays.asList(message.split(System.lineSeparator()));

    assertThat(messageLines).contains(NO_REDUNDANT_COPIES_FOR_REGIONS,
        zeroRedundancyRegionResult.toString(),
        REDUNDANCY_NOT_SATISFIED_FOR_REGIONS,
        underRedundancyRegionResult.toString(),
        REDUNDANCY_SATISFIED_FOR_REGIONS,
        successfulRegionResult.toString(),
        PRIMARY_TRANSFERS_COMPLETED + transfersCompleted,
        PRIMARY_TRANSFER_TIME + transferTime);
  }

  @Test
  public void addRegionResultAddsToCorrectInternalMap() {
    results.addRegionResult(zeroRedundancyRegionResult);
    results.addRegionResult(underRedundancyRegionResult);
    results.addRegionResult(successfulRegionResult);

    Map<String, RegionRedundancyStatus> zeroRedundancyResults =
        results.getZeroRedundancyRegionResults();
    assertThat(zeroRedundancyResults.size()).isEqualTo(1);
    assertThat(zeroRedundancyResults.get(zeroRedundancyRegionName))
        .isEqualTo(zeroRedundancyRegionResult);

    Map<String, RegionRedundancyStatus> underRedundancyResults =
        results.getUnderRedundancyRegionResults();
    assertThat(underRedundancyResults.size()).isEqualTo(1);
    assertThat(underRedundancyResults.get(underRedundancyRegionName))
        .isEqualTo(underRedundancyRegionResult);

    Map<String, RegionRedundancyStatus> successfulRegionResults =
        results.getSatisfiedRedundancyRegionResults();
    assertThat(successfulRegionResults.size()).isEqualTo(1);
    assertThat(successfulRegionResults.get(successfulRegionName)).isEqualTo(successfulRegionResult);
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
    when(regionResults.getTotalPrimaryTransferTime()).thenReturn(transferTime);

    results.addRegionResults(regionResults);

    Map<String, RegionRedundancyStatus> zeroRedundancyResults =
        results.getZeroRedundancyRegionResults();
    assertThat(zeroRedundancyResults.size()).isEqualTo(1);
    assertThat(zeroRedundancyResults.get(zeroRedundancyRegionName))
        .isEqualTo(zeroRedundancyRegionResult);

    Map<String, RegionRedundancyStatus> underRedundancyResults =
        results.getUnderRedundancyRegionResults();
    assertThat(underRedundancyResults.size()).isEqualTo(1);
    assertThat(underRedundancyResults.get(underRedundancyRegionName))
        .isEqualTo(underRedundancyRegionResult);

    Map<String, RegionRedundancyStatus> successfulRegionResults =
        results.getSatisfiedRedundancyRegionResults();
    assertThat(successfulRegionResults.size()).isEqualTo(1);
    assertThat(successfulRegionResults.get(successfulRegionName)).isEqualTo(successfulRegionResult);

    assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(transfersCompleted);
    assertThat(results.getTotalPrimaryTransferTime()).isEqualTo(transferTime);
  }

  @Test
  public void addPrimaryDetailsUpdatesValue() {
    assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(0);
    assertThat(results.getTotalPrimaryTransferTime()).isEqualTo(0L);
    results.addPrimaryReassignmentDetails(details);
    assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(transfersCompleted);
    assertThat(results.getTotalPrimaryTransferTime()).isEqualTo(transferTime);
    results.addPrimaryReassignmentDetails(details);
    assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(transfersCompleted * 2);
    assertThat(results.getTotalPrimaryTransferTime()).isEqualTo(transferTime * 2);
  }

  @Test
  public void testSerializable() throws JsonProcessingException {

    RestoreRedundancyResultsImpl restoreRedundancyResults = new RestoreRedundancyResultsImpl();
    restoreRedundancyResults.setStatusMessage(TEST_STRING);
    restoreRedundancyResults.setSuccess(true);
    restoreRedundancyResults.setTotalPrimaryTransfersCompleted(150);
    restoreRedundancyResults.setTotalPrimaryTransferTime(250);
    RegionRedundancyStatusImpl regionRedundancyStatus = new RegionRedundancyStatusImpl();
    regionRedundancyStatus.setActualRedundancy(1);
    regionRedundancyStatus.setConfiguredRedundancy(1);
    regionRedundancyStatus.setRegionName("/foo");
    regionRedundancyStatus.setStatus(SATISFIED);
    restoreRedundancyResults.addRegionResult(regionRedundancyStatus);
    String jsonString = geodeMapper.writeValueAsString(restoreRedundancyResults);
    // deserialize the class


    RestoreRedundancyResultsImpl value =
        geodeMapper.readValue(jsonString, RestoreRedundancyResultsImpl.class);

    assertThat(value).usingRecursiveComparison().isEqualTo(restoreRedundancyResults);


  }
}
