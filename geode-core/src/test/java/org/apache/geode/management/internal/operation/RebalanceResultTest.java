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
package org.apache.geode.management.internal.operation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.runtime.RebalanceResult;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class RebalanceResultTest {
  private ObjectMapper mapper;

  @Before
  public void setUp() {
    mapper = GeodeJsonMapper.getMapper();
  }

  @Test
  public void serializeRebalanceResult() throws Exception {
    RebalanceResult.PerRegionStats summary = new RebalanceResultImpl.PerRegionStatsImpl();
    Map<String, RebalanceResult.PerRegionStats> results = new LinkedHashMap<>();
    results.put("testRegion", summary);
    RebalanceResultImpl result = new RebalanceResultImpl();
    result.setRebalanceSummary(results);
    String json = mapper.writeValueAsString(result);
    RebalanceResult value = mapper.readValue(json, RebalanceResult.class);
    assertThat(value.getRebalanceStats().get("testRegion").getBucketCreateBytes()).isEqualTo(0);
  }
}
