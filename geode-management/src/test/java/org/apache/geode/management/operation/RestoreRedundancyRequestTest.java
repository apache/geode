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

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.util.internal.GeodeJsonMapper;

public class RestoreRedundancyRequestTest {
  public static final List<String> INCLUDE_REGIONS = Arrays.asList("a", "b");
  public static final List<String> EXCLUDE_REGIONS = Arrays.asList("c", "d");


  private ObjectMapper mapper;

  @Before
  public void setUp() {
    mapper = GeodeJsonMapper.getMapper();
  }

  /**
   * serializeRestoreRedundancyRequest
   *
   * This test is responsible for ensuring that the data being sent to
   * request the restoration of redundancy is serialized and deserialized properly
   * as this is the main message to request the restoration of redundancy via the
   * REST API.
   *
   */
  @Test
  public void serializeRestoreRedundancyRequest() throws Exception {
    // Construct the request
    RestoreRedundancyRequest restoreRedundancyRequest =
        new RestoreRedundancyRequest();

    // Set the data
    restoreRedundancyRequest.setIncludeRegions(INCLUDE_REGIONS);
    restoreRedundancyRequest.setExcludeRegions(EXCLUDE_REGIONS);
    restoreRedundancyRequest.setReassignPrimaries(true);
    restoreRedundancyRequest.setOperator(RestoreRedundancyRequest.RESTORE_REDUNDANCY_OPERATOR);

    // Serialize the class
    String json = mapper.writeValueAsString(restoreRedundancyRequest);

    // Deserialize the class
    RestoreRedundancyRequest value =
        mapper.readValue(json, RestoreRedundancyRequest.class);

    // check the values
    assertThat(value.getIncludeRegions()).isEqualTo(INCLUDE_REGIONS);
    assertThat(value.getExcludeRegions()).isEqualTo(EXCLUDE_REGIONS);
    assertThat(value.getReassignPrimaries()).isTrue();
    assertThat(value.getOperator()).isEqualTo(RestoreRedundancyRequest.RESTORE_REDUNDANCY_OPERATOR);

    // This value is defaulted in the class, that is why we don't set it.
    assertThat(value.getEndpoint())
        .isEqualTo(RestoreRedundancyRequest.RESTORE_REDUNDANCY_REBALANCE_ENDPOINT);
  }
}
