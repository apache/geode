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

public class CountPRQueryNodeTest {
  private CountPRQueryNode countPRQueryNode;

  @Before
  public void setUp() {
    countPRQueryNode = new CountPRQueryNode();
  }

  @Test
  public void accumulateShouldComputeIntermediateAdditions() {
    countPRQueryNode.accumulate(2);
    countPRQueryNode.accumulate(3);

    assertThat(countPRQueryNode.getCount()).isEqualTo(5);
  }

  @Test
  public void terminateShouldCorrectlyComputeAggregatedValuesCount() {
    countPRQueryNode.accumulate(50);
    countPRQueryNode.accumulate(50);
    countPRQueryNode.accumulate(23);

    Object result = countPRQueryNode.terminate();
    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).intValue()).isEqualTo(123);
  }
}
