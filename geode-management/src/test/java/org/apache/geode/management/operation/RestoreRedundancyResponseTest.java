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
package org.apache.geode.management.operation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.internal.operation.RegionRedundancyStatusSerializableImpl;
import org.apache.geode.management.internal.operation.RestoreRedundancyResponseImpl;
import org.apache.geode.management.runtime.RegionRedundancyStatusSerializable;
import org.apache.geode.management.runtime.RestoreRedundancyResponse;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class RestoreRedundancyResponseTest {
  public static final int TOTAL_PRIMARY_TRANSFERS_COMPLETED = 37;
  public static final int TOTAL_PRIMARY_TRANSFER_TIME = 10;
  public static final String HELLO_TEST = "Hello Test.";
  public static final int ACTUAL_REDUNDANCY = 1;
  public static final int CONFIGURED_REDUNDANCY = 2;
  public static final String REGION_NAME = "Region123";
  private ObjectMapper mapper;

  @Before
  public void setUp() {
    mapper = GeodeJsonMapper.getMapper();
  }


  /**
   * serializeRestoreRedundancyResponse
   * Test that when we serialize a RestoreRedundancyResponseImpl message that
   * it deserializes properly because this is a key message for REST's RestoreRedundancy API
   *
   * @throws Exception - exceptions can be thrown by read and write calls
   */
  @Test
  public void serializeRestoreRedundancyResponse() throws Exception {

    // Construct a response class
    RestoreRedundancyResponseImpl restoreRedundancyResponse =
        new RestoreRedundancyResponseImpl();

    // Set basic data
    restoreRedundancyResponse.setSuccess(true);
    restoreRedundancyResponse.setStatusMessage(HELLO_TEST);
    restoreRedundancyResponse.setTotalPrimaryTransfersCompleted(TOTAL_PRIMARY_TRANSFERS_COMPLETED);
    restoreRedundancyResponse.setTotalPrimaryTransferTime(TOTAL_PRIMARY_TRANSFER_TIME);

    // Set the result lists to have a standard entry
    List<RegionRedundancyStatusSerializable> workingList =
        getRegionRedundancyStatusSerializableList();
    restoreRedundancyResponse.setUnderRedundancyRegionResults(workingList);
    restoreRedundancyResponse.setZeroRedundancyRegionResults(workingList);
    restoreRedundancyResponse.setSatisfiedRedundancyRegionResults(workingList);

    // serialize the class
    String json = mapper.writeValueAsString(restoreRedundancyResponse);

    // deserialize the class
    RestoreRedundancyResponse value =
        mapper.readValue(json, RestoreRedundancyResponseImpl.class);

    // check the basic data
    assertThat(value.getSuccess()).isTrue();
    assertThat(value.getStatusMessage()).isEqualTo(HELLO_TEST);
    assertThat(value.getTotalPrimaryTransfersCompleted())
        .isEqualTo(TOTAL_PRIMARY_TRANSFERS_COMPLETED);
    assertThat(value.getTotalPrimaryTransferTime()).isEqualTo(
        TOTAL_PRIMARY_TRANSFER_TIME);

    // check the lists
    verifyRegionRedundancyStatusSerializableList(workingList);
  }

  /**
   * getRegionRedundancyStatusSerializableList
   * This function builds a list of RegionRedundancyStatusSerializable classes for testing
   *
   * @return List<RegionRedundancyStatusSerializable>
   */
  private List<RegionRedundancyStatusSerializable> getRegionRedundancyStatusSerializableList() {
    RegionRedundancyStatusSerializableImpl regionRedundancyStatusSerializable =
        new RegionRedundancyStatusSerializableImpl();
    regionRedundancyStatusSerializable.setActualRedundancy(ACTUAL_REDUNDANCY);
    regionRedundancyStatusSerializable.setConfiguredRedundancy(CONFIGURED_REDUNDANCY);
    regionRedundancyStatusSerializable.setRegionName(REGION_NAME);
    regionRedundancyStatusSerializable.setStatus(
        RegionRedundancyStatusSerializable.RedundancyStatus.SATISFIED);
    return Collections.singletonList(regionRedundancyStatusSerializable);
  }

  /**
   * verifyRegionRedundancyStatusSerializableList
   *
   * @param workingList - the list of RegionRedundancyStatusSerializable that we want to verify.
   *        it should only be one entry.
   */
  private void verifyRegionRedundancyStatusSerializableList(
      List<RegionRedundancyStatusSerializable> workingList) {
    assertThat(workingList).hasSize(1);
    RegionRedundancyStatusSerializable regionRedundancyStatusSerializable = workingList.get(0);
    assertThat(regionRedundancyStatusSerializable.getActualRedundancy())
        .isEqualTo(ACTUAL_REDUNDANCY);
    assertThat(regionRedundancyStatusSerializable.getConfiguredRedundancy())
        .isEqualTo(CONFIGURED_REDUNDANCY);
    assertThat(regionRedundancyStatusSerializable.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(regionRedundancyStatusSerializable.getStatus())
        .isEqualTo(RegionRedundancyStatusSerializable.RedundancyStatus.SATISFIED);
  }

}
