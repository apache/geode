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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.cache.CachePerfStats.Clock;

public class RegionPerfStatsTest {

  private StatisticsFactory statisticsFactory;
  private CachePerfStats cachePerfStats;
  private Clock clock;

  @Before
  public void setUp() {
    statisticsFactory = mock(StatisticsFactory.class);
    cachePerfStats = mock(CachePerfStats.class);
    clock = mock(Clock.class);
  }

  @Test
  public void textIdIsRegionStatsHyphenRegionName() throws Exception {
    String theRegionName = "TheRegionName";

    new RegionPerfStats(statisticsFactory, cachePerfStats,
        theRegionName, clock);

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statisticsFactory).createAtomicStatistics(any(), captor.capture());

    assertThat(captor.getValue()).isEqualTo("RegionStats-" + theRegionName);
  }
}
