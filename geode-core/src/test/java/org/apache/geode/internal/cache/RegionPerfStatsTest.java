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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.geode.cache.DataPolicy.REPLICATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.internal.cache.CachePerfStats.Clock;

public class RegionPerfStatsTest {

  private final DataPolicy dataPolicy = REPLICATE;

  private String regionName;
  private MeterRegistry meterRegistry;
  private Clock clock;

  private RegionPerfStats regionPerfStats;

  @Before
  public void setUp() {
    CachePerfStats.enableClockStats = true;

    Statistics statistics = mock(Statistics.class);
    StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    CachePerfStats cachePerfStats = mock(CachePerfStats.class);
    clock = mock(Clock.class);

    when(statisticsFactory.createAtomicStatistics(any(), any()))
        .thenReturn(statistics);

    regionName = "myRegion";
    meterRegistry = new SimpleMeterRegistry();

    regionPerfStats = new RegionPerfStats(statisticsFactory, cachePerfStats, regionName, dataPolicy,
        meterRegistry, clock);
  }

  @Test
  public void putTimerHasRegionNameTag() {
    Timer timer = meterRegistry.find("cache.region.operations")
        .tag("operation.type", "put")
        .timer();

    assertThat(timer.getId().getTags())
        .contains(Tag.of("region.name", regionName));
  }

  @Test
  public void putTimerHasDataPolicyTag() {
    Timer timer = meterRegistry.find("cache.region.operations")
        .tag("operation.type", "put")
        .timer();

    assertThat(timer.getId().getTags())
        .contains(Tag.of("data.policy", dataPolicy.toString()));
  }

  @Test
  public void registersOnePutTimer() {
    Collection<Timer> timers = meterRegistry.find("cache.region.operations")
        .tag("operation.type", "put")
        .timers();

    assertThat(timers).hasSize(1);
  }

  @Test
  public void getTimerHasRegionNameTag() {
    Timer timer = meterRegistry.find("cache.region.operations")
        .tag("operation.type", "get")
        .timer();

    assertThat(timer.getId().getTags())
        .contains(Tag.of("region.name", regionName));
  }

  @Test
  public void getTimerHasDataPolicyTag() {
    Timer timer = meterRegistry.find("cache.region.operations")
        .tag("operation.type", "get")
        .timer();

    assertThat(timer.getId().getTags())
        .contains(Tag.of("data.policy", dataPolicy.toString()));
  }

  @Test
  public void registersOneGetTimer() {
    Collection<Timer> timers = meterRegistry.find("cache.region.operations")
        .tag("operation.type", "get")
        .timers();

    assertThat(timers).hasSize(1);
  }

  @Test
  public void putIncrementsPutCount() {
    regionPerfStats.endPut(0L, true);

    Timer timer = meterRegistry.find("cache.region.operations")
        .tag("operation.type", "put")
        .timer();

    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  public void putAccumulatesTotalPutTime() {
    long expectedTotalTime = 42L;
    when(clock.getTime()).thenReturn(expectedTotalTime);

    regionPerfStats.endPut(0L, true);

    Timer timer = meterRegistry.find("cache.region.operations")
        .tag("operation.type", "put")
        .timer();

    assertThat(timer.totalTime(NANOSECONDS)).isEqualTo(expectedTotalTime);
  }

  @Test
  public void getIncrementsGetCount() {
    regionPerfStats.endGet(0L, true);

    Timer timer = meterRegistry.find("cache.region.operations")
        .tag("operation.type", "get")
        .timer();

    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  public void getAccumulatesTotalGetTime() {
    long expectedTotalTime = 42L;
    when(clock.getTime()).thenReturn(expectedTotalTime);

    regionPerfStats.endGet(0L, true);

    Timer timer = meterRegistry.find("cache.region.operations")
        .tag("operation.type", "get")
        .timer();

    assertThat(timer.totalTime(NANOSECONDS)).isEqualTo(expectedTotalTime);
  }
}
