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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.internal.operation.RebalanceRegionResultImpl;
import org.apache.geode.management.internal.operation.RebalanceResultImpl;
import org.apache.geode.management.runtime.RebalanceRegionResult;
import org.apache.geode.management.runtime.RebalanceResult;
import org.apache.geode.util.internal.GeodeJsonMapper;

public class RebalanceResultTest {
  private ObjectMapper mapper;
  private RebalanceResultImpl result;

  @Before
  public void setUp() {
    mapper = GeodeJsonMapper.getMapper();
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    RebalanceRegionResultImpl summary = new RebalanceRegionResultImpl();
    summary.setRegionName("testRegion");
    List<RebalanceRegionResult> results = Collections.singletonList(summary);
    result = new RebalanceResultImpl();
    result.setRebalanceSummary(results);
  }

  @Test
  public void serializeRebalanceResult() throws Exception {
    String json = mapper.writeValueAsString(result);
    RebalanceResult value = mapper.readValue(json, RebalanceResult.class);
    assertThat(value.getRebalanceRegionResults().get(0).getBucketCreateBytes()).isEqualTo(0);
    assertThat(value.getRebalanceRegionResults().get(0).getRegionName()).isEqualTo("testRegion");
  }

  @Test
  public void toStringRebalanceResult() {
    String toStr = result.toString();
    assertThat(toStr).isEqualTo(
        "{{bucketCreateBytes=0, bucketCreateTimeInMilliseconds=0, bucketCreatesCompleted=0, bucketTransferBytes=0, bucketTransferTimeInMilliseconds=0, bucketTransfersCompleted=0, primaryTransferTimeInMilliseconds=0, primaryTransfersCompleted=0, timeInMilliseconds=0, numOfMembers=0, regionName=testRegion}}");
  }
}
