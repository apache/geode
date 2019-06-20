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
package org.apache.geode.distributed.internal;

import static org.apache.geode.internal.statistics.StatisticDescriptorImpl.createLongGauge;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.internal.statistics.StatisticsTypeImpl;
import org.apache.geode.internal.statistics.StripedStatisticsImpl;

public class MaxLongGaugeTest {
  private MaxLongGauge maxLongGauge;
  private StripedStatisticsImpl fakeStatistics;

  @Before
  public void setup() {
    StatisticDescriptor descriptor =
        createLongGauge("1", "", "", true);
    StatisticDescriptor descriptor2 =
        createLongGauge("2", "", "", true);

    StatisticDescriptor[] descriptors = {descriptor, descriptor2};
    StatisticsTypeImpl statisticsType = new StatisticsTypeImpl("abc", "test",
        descriptors);
    fakeStatistics = new StripedStatisticsImpl(
        statisticsType,
        "def", 12, 10,
        null);
  }

  @Test
  public void recordMax_singleRecord() {
    MaxLongGauge maxLongGauge = new MaxLongGauge(0, fakeStatistics);

    maxLongGauge.recordMax(12);

    assertThat(fakeStatistics.getLong(0)).isEqualTo(12);
  }

  @Test
  public void recordMax_multipleRecords() {
    MaxLongGauge maxLongGauge = new MaxLongGauge(0, fakeStatistics);

    maxLongGauge.recordMax(12);
    maxLongGauge.recordMax(13);

    assertThat(fakeStatistics.getLong(0)).isEqualTo(13);
  }

  @Test
  public void recordMax_recordNothing_ifMaxIsNotExceeded() {
    MaxLongGauge maxLongGauge = new MaxLongGauge(0, fakeStatistics);

    maxLongGauge.recordMax(12);
    maxLongGauge.recordMax(11);

    assertThat(fakeStatistics.getLong(0)).isEqualTo(12);
  }

  @Test
  public void recordMax_ignoresNegatives() {
    MaxLongGauge maxLongGauge = new MaxLongGauge(0, fakeStatistics);

    maxLongGauge.recordMax(-12);

    assertThat(fakeStatistics.getLong(0)).isEqualTo(0);
  }

  @Test
  public void recordMax_ignoresZero() {
    MaxLongGauge maxLongGauge = new MaxLongGauge(0, fakeStatistics);

    maxLongGauge.recordMax(0);

    assertThat(fakeStatistics.getLong(0)).isEqualTo(0);
  }

  @Test
  public void recordMax_usesStatId() {
    MaxLongGauge maxLongGauge = new MaxLongGauge(1, fakeStatistics);

    maxLongGauge.recordMax(17);

    assertThat(fakeStatistics.getLong(1)).isEqualTo(17);
  }
}
