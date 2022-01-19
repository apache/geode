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

package org.apache.geode.internal.statistics.meters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.Statistics;

public class DoubleStatisticBindingTest {
  private final Statistics statistics = mock(Statistics.class);

  @Test
  public void add_addsAmountToStat() {
    int statId = 27;
    StatisticBinding binding = new DoubleStatisticBinding(statistics, statId);

    binding.add(1234.8);

    verify(statistics).incDouble(statId, 1234.8);
  }

  @Test
  public void doubleValue_returnsDoubleStatValue() {
    int statId = 27;
    StatisticBinding binding = new DoubleStatisticBinding(statistics, statId);

    when(statistics.getDouble(statId)).thenReturn(2341.9);

    assertThat(binding.doubleValue())
        .isEqualTo(2341.9);
  }

  @Test
  public void longValue_returnsDoubleStatValueAsLong() {
    int statId = 27;
    StatisticBinding binding = new DoubleStatisticBinding(statistics, statId);

    when(statistics.getDouble(statId)).thenReturn(2341.0);

    assertThat(binding.longValue())
        .isEqualTo(2341L);
  }
}
