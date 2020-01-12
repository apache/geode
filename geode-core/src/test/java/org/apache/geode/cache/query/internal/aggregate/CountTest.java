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

public class CountTest {
  private Count count;

  @Before
  public void setUp() {
    count = new Count();
  }

  @Test
  public void accumulateShouldIgnoreNull() {
    count.accumulate(null);

    assertThat(count.getCount()).isEqualTo(0);
  }

  @Test
  public void accumulateShouldIgnoreUndefined() {
    count.accumulate(QueryService.UNDEFINED);

    assertThat(count.getCount()).isEqualTo(0);
  }

  @Test
  public void accumulateShouldIncreaseAccumulatedCount() {
    count.accumulate(1);
    count.accumulate(25.78f);

    assertThat(count.getCount()).isEqualTo(2);
  }

  @Test
  public void terminateShouldCorrectlyComputeAverageUponAccumulatedValues() {
    for (int i = 0; i < 10; i++) {
      count.accumulate(i);
    }

    Object result = count.terminate();
    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).intValue()).isEqualTo(10);
  }
}
