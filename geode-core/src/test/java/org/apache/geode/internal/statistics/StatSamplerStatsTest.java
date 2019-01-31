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
package org.apache.geode.internal.statistics;

import static org.apache.geode.internal.statistics.StatSamplerStats.jvmPausesId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;

public class StatSamplerStatsTest {
  private static final String TEXT_ID = "statSampler";

  private Statistics statistics;
  private StatSamplerStats statSamplerStats;

  @Before
  public void setUp() {
    StatisticsType statisticsType = StatSamplerStats.getStatisticsType();

    StatisticsManager statisticsManager = mock(StatisticsManager.class);
    StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);

    statistics = spy(new LocalStatisticsImpl(statisticsType, TEXT_ID, 1, 1, false, 0,
        statisticsManager));

    when(statisticsFactory.createStatistics(eq(statisticsType), eq(TEXT_ID), eq(1L)))
        .thenReturn(statistics);

    statSamplerStats = new StatSamplerStats(statisticsFactory, 1);
  }

  @Test
  public void getJvmPausesDelegatesToStatistics() {
    statistics.incLong(jvmPausesId, Integer.MAX_VALUE);

    assertThat(statSamplerStats.getJvmPauses()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incJvmPausesIncrementsJvmPauses() {
    statSamplerStats.incJvmPauses();

    assertThat(statistics.getLong(jvmPausesId)).isEqualTo(1);
  }

  @Test
  public void jvmPausesWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(jvmPausesId, Integer.MAX_VALUE);

    statSamplerStats.incJvmPauses();

    assertThat(statSamplerStats.getJvmPauses()).isPositive();
  }
}
