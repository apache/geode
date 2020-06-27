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

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.runtime.RegionRedundancyStatus;
import org.apache.geode.management.runtime.RestoreRedundancyResults;

public class RestoreRedundancyResultsImplTest {

  private RestoreRedundancyResultsImpl result;

  @Before
  public void before() {
    result = new RestoreRedundancyResultsImpl();
  }

  @Test
  public void initialState() {
    assertThat(result.getRegionOperationStatus())
        .isEqualTo(RestoreRedundancyResults.Status.SUCCESS);
    assertThat(result.getRegionOperationMessage())
        .startsWith("Total primary transfers completed = 0");
    assertThat(result.getSuccess()).isTrue();
    assertThat(result.getStatusMessage()).isNull();
  }

  @Test
  public void resultIsSuccessfulWithOperationFailure() {
    RegionRedundancyStatus status = new RegionRedundancyStatusImpl(1, 1, "test",
        RegionRedundancyStatus.RedundancyStatus.NOT_SATISFIED);
    result.addRegionResult(status);
    assertThat(result.getRegionOperationStatus())
        .isEqualTo(RestoreRedundancyResults.Status.FAILURE);
    assertThat(result.getRegionOperationMessage())
        .startsWith("Redundancy is partially satisfied for regions");
    assertThat(result.getSuccess()).isTrue();
    assertThat(result.getStatusMessage()).isNull();
  }
}
