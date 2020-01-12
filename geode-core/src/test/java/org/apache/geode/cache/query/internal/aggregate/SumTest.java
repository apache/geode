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

import org.apache.geode.cache.query.QueryService;

public class SumTest {
  private Sum sum;

  @Before
  public void setUp() {
    sum = new Sum();
  }

  @Test
  public void accumulateShouldIgnoreNull() {
    sum.accumulate(null);

    assertThat(sum.getResult()).isEqualTo(0);
  }

  @Test
  public void accumulateShouldIgnoreUndefined() {
    sum.accumulate(QueryService.UNDEFINED);

    assertThat(sum.getResult()).isEqualTo(0);
  }

  @Test
  public void accumulateShouldComputeIntermediateAdditions() {
    sum.accumulate(1);
    assertThat(sum.getResult()).isEqualTo(1);

    sum.accumulate(25.78);
    assertThat(sum.getResult()).isEqualTo(26.78);

    sum.accumulate(1.22);
    assertThat(sum.getResult()).isEqualTo(28.00);
  }

  @Test
  public void terminateShouldCorrectlyComputeValuesAddition() {
    sum.accumulate(1);
    sum.accumulate(2);
    sum.accumulate(3);
    sum.accumulate(4);
    sum.accumulate(5);
    sum.accumulate(6);
    sum.accumulate(7);
    sum.accumulate(null);
    sum.accumulate(QueryService.UNDEFINED);

    Object result = sum.terminate();
    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).intValue()).isEqualTo(28);
  }
}
