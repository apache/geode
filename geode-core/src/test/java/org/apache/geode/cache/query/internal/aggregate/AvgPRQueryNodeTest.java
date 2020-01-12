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
package org.apache.geode.cache.query.internal.aggregate;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class AvgPRQueryNodeTest {
  private AvgPRQueryNode avgPRQueryNode;

  @Before
  public void setUp() {
    avgPRQueryNode = new AvgPRQueryNode();
  }

  @Test
  public void accumulateShouldIncreaseAccumulatedCount() {
    avgPRQueryNode.accumulate(new Long[] {2L, 10L});
    avgPRQueryNode.accumulate(new Long[] {3L, 30L});

    assertThat(avgPRQueryNode.getCount()).isEqualTo(5);
    assertThat(avgPRQueryNode.getResult()).isEqualTo(40);
  }

  @Test
  public void terminateShouldCorrectlyComputeAverageUponAccumulatedValues() {
    avgPRQueryNode.accumulate(new Object[] {7L, 43d});
    avgPRQueryNode.accumulate(new Object[] {5L, 273.86d});

    Object result = avgPRQueryNode.terminate();
    assertThat(avgPRQueryNode.getCount()).isEqualTo(12);
    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).floatValue()).isEqualTo((43 + 273.86f) / 12.0f);
  }
}
