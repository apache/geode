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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

public class SumDistinctPRQueryNodeTest extends DistinctAggregatorTest {

  @Before
  public void setUp() {
    distinctAggregator = new SumDistinctPRQueryNode();
  }

  @Test
  public void accumulateShouldComputeIntermediateAdditions() {
    distinctAggregator.accumulate(new HashSet<>());
    assertThat(distinctAggregator.getDistinct()).isEmpty();

    distinctAggregator.accumulate(new HashSet<>(Arrays.asList(1, 10.12f)));
    assertThat(distinctAggregator.getDistinct()).isNotEmpty().hasSize(2);

    distinctAggregator.accumulate(new HashSet<>(Collections.singletonList(10.12f)));
    assertThat(distinctAggregator.getDistinct()).isNotEmpty().hasSize(2);
  }

  @Test
  public void terminateShouldCorrectlyComputeDistinctValuesAddition() {
    distinctAggregator.accumulate(new HashSet<>());
    distinctAggregator.accumulate(new HashSet<>(Arrays.asList(5, 6, 3)));
    distinctAggregator.accumulate(new HashSet<>(Arrays.asList(3, 7, 8)));

    Object result = distinctAggregator.terminate();
    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).intValue()).isEqualTo(29);
  }
}
