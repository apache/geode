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

public class AvgTest {
  private Avg avg;

  @Before
  public void setUp() {
    avg = new Avg();
  }

  @Test
  public void accumulateShouldIgnoreNull() {
    avg.accumulate(null);

    assertThat(avg.getNum()).isEqualTo(0);
    assertThat(avg.getResult()).isEqualTo(0);
  }

  @Test
  public void accumulateShouldIgnoreUndefined() {
    avg.accumulate(QueryService.UNDEFINED);

    assertThat(avg.getNum()).isEqualTo(0);
    assertThat(avg.getResult()).isEqualTo(0);
  }

  @Test
  public void accumulateShouldIncreaseAccumulatedCount() {
    avg.accumulate(1);
    avg.accumulate(25.78f);

    assertThat(avg.getNum()).isEqualTo(2);
    assertThat(avg.getResult()).isEqualTo(26.78f);
  }

  @Test
  public void terminateShouldCorrectlyComputeAverageUponAccumulatedValues() {
    avg.accumulate(1);
    avg.accumulate(2);
    avg.accumulate(3);
    avg.accumulate(4);
    avg.accumulate(5);
    avg.accumulate(6);
    avg.accumulate(7);
    avg.accumulate(null);
    avg.accumulate(QueryService.UNDEFINED);
    float expected = (1 + 2 + 3 + 4 + 5 + 6 + 7) / 7.0f;

    Object result = avg.terminate();
    assertThat(avg.getNum()).isEqualTo(7);
    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).floatValue()).isEqualTo(expected);
  }
}
