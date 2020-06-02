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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.internal.operation.RegionRedundancyStatusSerializableImpl;
import org.apache.geode.management.runtime.RegionRedundancyStatusSerializable;
import org.apache.geode.util.internal.GeodeJsonMapper;


public class RegionRedundancyStatusSerializableImplTest {
  private ObjectMapper mapper;
  public static final int ACTUAL_REDUNDANCY = 1;
  public static final int CONFIGURED_REDUNDANCY = 2;
  public static final String REGION_NAME = "Region123";

  @Before
  public void setUp() {
    mapper = GeodeJsonMapper.getMapper();
  }

  /**
   * serializeRegionRedundancyStatusSerializableWorks
   * This test's role is to validate that we can successfully serialize and deserialize the
   * RegionRedundancyStatusSerializableImpl class.
   *
   */
  @Test
  public void serializeRegionRedundancyStatusSerializableWorks() throws JsonProcessingException {
    // Construct a RegionRedundancyStatusSerializableImpl
    RegionRedundancyStatusSerializableImpl regionRedundancyStatusSerializable =
        new RegionRedundancyStatusSerializableImpl();

    // Set the data
    regionRedundancyStatusSerializable.setActualRedundancy(ACTUAL_REDUNDANCY);
    regionRedundancyStatusSerializable.setConfiguredRedundancy(CONFIGURED_REDUNDANCY);
    regionRedundancyStatusSerializable.setRegionName(REGION_NAME);
    regionRedundancyStatusSerializable.setStatus(
        RegionRedundancyStatusSerializable.RedundancyStatus.SATISFIED);

    // serialize the class
    String json = mapper.writeValueAsString(regionRedundancyStatusSerializable);

    // deserialize the class
    RegionRedundancyStatusSerializable value =
        mapper.readValue(json, RegionRedundancyStatusSerializableImpl.class);

    // Check that the data matches correctly.
    assertThat(value.getActualRedundancy()).isEqualTo(ACTUAL_REDUNDANCY);
    assertThat(value.getConfiguredRedundancy()).isEqualTo(CONFIGURED_REDUNDANCY);
    assertThat(value.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(value.getStatus())
        .isEqualTo(RegionRedundancyStatusSerializable.RedundancyStatus.SATISFIED);
  }
}
