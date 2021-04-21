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
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.apache.geode.test.micrometer.MicrometerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.Collection;
import java.util.function.LongSupplier;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.internal.statistics.StatisticsClock;

public class RegionPerfStatsTest {

  private static final String REGION_NAME = "region1";
  private static final String TEXT_ID = "textId";
  private static final DataPolicy DATA_POLICY = DataPolicy.PERSISTENT_REPLICATE;
  private static final long CLOCK_VALUE = 5L;

  private MeterRegistry meterRegistry;
  private CachePerfStats cachePerfStats;
  private InternalRegion region;
  private RegionPerfStats regionPerfStats;
  private Statistics statistics;
  private StatisticsClock statisticsClock;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);
  private StatisticsFactory statisticsFactory;

  @Before
  public void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    cachePerfStats = mock(CachePerfStats.class);
    statistics = mock(Statistics.class);
    region = mock(InternalRegion.class);
    when(region.getName()).thenReturn(REGION_NAME);
    when(region.getDataPolicy()).thenReturn(DATA_POLICY);
    statisticsFactory = mock(StatisticsFactory.class);
    when(statisticsFactory.createAtomicStatistics(any(), any())).thenReturn(statistics);
    statisticsClock = mock(StatisticsClock.class);

    regionPerfStats = new RegionPerfStats(statisticsFactory, TEXT_ID, statisticsClock,
        cachePerfStats, region, meterRegistry);
  }

  @After
  public void closeStats() {
    if (regionPerfStats != null) {
      regionPerfStats.close();
    }
  }

  @Test
  public void createsStatisticsUsingTextId() {
    StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    when(statisticsFactory.createAtomicStatistics(any(), any())).thenReturn(mock(Statistics.class));

    new RegionPerfStats(statisticsFactory, TEXT_ID, disabledClock(), cachePerfStats, region,
        meterRegistry);

    verify(statisticsFactory).createAtomicStatistics(any(), eq(TEXT_ID));
  }

  @Test
  public void constructor_createsEntriesGauge_taggedWithRegionName() {
    assertThat(entriesGauge())
        .as("geode.cache.entries gauge")
        .hasTag("region", REGION_NAME);
  }

  @Test
  public void constructor_createsEntriesGauge_taggedWithDataPolicy() {
    assertThat(entriesGauge())
        .as("geode.cache.entries gauge")
        .hasTag("data.policy", DATA_POLICY.toString());
  }

  @Test
  public void constructor_createsCacheGetsHitTimer_taggedWithRegionName() {
    assertThat(cacheGetsHitTimer())
        .as("geode.cache.gets timer with tag result=hit")
        .hasTag("region", REGION_NAME);
  }

  @Test
  public void constructor_createsCacheGetsMissTimer_taggedWithRegionName() {
    assertThat(cacheGetsMissTimer())
        .as("geode.cache.gets timer with tag result=miss")
        .hasTag("region", REGION_NAME);
  }

  @Test
  public void suppliesEntryCountStatWithRegionLocalSize() {
    ArgumentCaptor<LongSupplier> supplierCaptor = ArgumentCaptor.forClass(LongSupplier.class);

    verify(statistics).setLongSupplier(eq(RegionPerfStats.entryCountId), supplierCaptor.capture());

    when(region.getLocalSize()).thenReturn(92);

    assertThat(supplierCaptor.getValue().getAsLong())
        .as("Accumulated value of supplier")
        .isEqualTo(92);
  }

  @Test
  public void incEntryCount_incrementsCachePerfStatsEntryCount() {
    regionPerfStats.incEntryCount(2);

    verify(cachePerfStats).incEntryCount(2);
  }

  @Test
  public void incPreviouslySeenEvents_incrementsCachePerfStatsPreviouslySeenEvents() {
    regionPerfStats.incPreviouslySeenEvents();

    verify(cachePerfStats).incPreviouslySeenEvents();
  }

  @Test
  public void entryCountGaugeFetchesValueFromRegionLocalSize() {
    when(region.getLocalSize()).thenReturn(3);

    Gauge entriesGauge = meterRegistry
        .find("geode.cache.entries")
        .tag("region", REGION_NAME)
        .gauge();

    assertThat(entriesGauge.value()).isEqualTo(3);
  }

  @Test
  public void endGetForClient_recordsHitTimerCountAndTotalTime_ifCacheHitAndClockEnabled() {
    when(statisticsClock.isEnabled()).thenReturn(true);
    when(statisticsClock.getTime()).thenReturn(CLOCK_VALUE);

    regionPerfStats.endGetForClient(0, false);

    assertThat(cacheGetsHitTimer())
        .as("geode.cache.gets timer with tag result=hit")
        .hasCount(1)
        .hasTotalTime(NANOSECONDS, CLOCK_VALUE);
  }

  @Test
  public void endGetForClient_doesNotRecordMissTimerCountOrTotalTime_ifCacheHitAndClockEnabled() {
    when(statisticsClock.isEnabled()).thenReturn(true);
    when(statisticsClock.getTime()).thenReturn(CLOCK_VALUE);

    regionPerfStats.endGetForClient(0, false);

    assertThat(cacheGetsMissTimer())
        .as("geode.cache.gets timer with tag result=miss")
        .hasCount(0)
        .hasTotalTime(NANOSECONDS, 0);
  }

  @Test
  public void endGetForClient_recordsHitTimerCountOnly_ifCacheHitAndClockDisabled() {
    when(statisticsClock.isEnabled()).thenReturn(false);

    regionPerfStats.endGetForClient(0, false);

    assertThat(cacheGetsHitTimer())
        .as("geode.cache.gets timer with tag result=hit")
        .hasCount(1)
        .hasTotalTime(NANOSECONDS, 0);
  }

  @Test
  public void endGetForClient_recordsMissTimerCountAndTotalTime_ifCacheMissAndClockEnabled() {
    when(statisticsClock.isEnabled()).thenReturn(true);
    when(statisticsClock.getTime()).thenReturn(CLOCK_VALUE);

    regionPerfStats.endGetForClient(0, true);

    assertThat(cacheGetsMissTimer())
        .as("geode.cache.gets timer with tag result=miss")
        .hasCount(1)
        .hasTotalTime(NANOSECONDS, CLOCK_VALUE);
  }

  @Test
  public void endGetForClient_doesNotRecordHitTimerCountOrTotalTime_ifCacheMissAndClockEnabled() {
    when(statisticsClock.isEnabled()).thenReturn(true);
    when(statisticsClock.getTime()).thenReturn(CLOCK_VALUE);

    regionPerfStats.endGetForClient(0, true);

    assertThat(cacheGetsHitTimer())
        .as("geode.cache.gets timer with tag result=hit")
        .hasCount(0)
        .hasTotalTime(NANOSECONDS, 0);
  }

  @Test
  public void endGetForClient_recordsMissTimerCountOnly_ifCacheMissAndClockDisabled() {
    when(statisticsClock.isEnabled()).thenReturn(false);

    regionPerfStats.endGetForClient(0, true);

    assertThat(cacheGetsMissTimer())
        .as("geode.cache.gets timer with tag result=miss")
        .hasCount(1)
        .hasTotalTime(NANOSECONDS, 0);
  }

  @Test
  public void close_removesEntriesGaugeFromTheRegistry() {
    assertThat(metersNamed("geode.cache.entries"))
        .as("entries gauge before closing the stats")
        .hasSize(1);

    regionPerfStats.close();

    assertThat(metersNamed("geode.cache.entries"))
        .as("entries gauge after closing the stats")
        .hasSize(0);

    regionPerfStats = null;
  }

  @Test
  public void close_removesCacheGetsTimersFromTheRegistry() {
    assertThat(metersNamed("geode.cache.gets"))
        .as("cache gets timer before closing the stats")
        .hasSize(2);

    regionPerfStats.close();

    assertThat(metersNamed("geode.cache.gets"))
        .as("cache gets timer after closing the stats")
        .hasSize(0);

    regionPerfStats = null;
  }

  @Test
  public void close_doesNotRemoveMetersItDoesNotOwn() {
    String foreignMeterName = "some.meter.not.created.by.the.gateway.receiver.stats";

    Timer.builder(foreignMeterName)
        .register(meterRegistry);

    regionPerfStats.close();

    assertThat(metersNamed(foreignMeterName))
        .as("foreign meter after closing the stats")
        .hasSize(1);

    regionPerfStats = null;
  }

  @Test
  public void close_closesMeters() {
    Gauge entriesGauge = mock(Gauge.class);
    Timer cacheGetsHitTimer = mock(Timer.class);
    Timer cacheGetsMissTimer = mock(Timer.class);
    regionPerfStats = new RegionPerfStats(statisticsFactory, TEXT_ID, statisticsClock,
        cachePerfStats, region, mock(MeterRegistry.class), entriesGauge, cacheGetsHitTimer,
        cacheGetsMissTimer);

    regionPerfStats.close();

    verify(entriesGauge).close();
    verify(cacheGetsHitTimer).close();
    verify(cacheGetsMissTimer).close();

    regionPerfStats = null;
  }

  private Collection<Meter> metersNamed(String meterName) {
    return meterRegistry
        .find(meterName)
        .meters();
  }

  private Gauge entriesGauge() {
    return meterRegistry
        .find("geode.cache.entries")
        .gauge();
  }

  private Timer cacheGetsHitTimer() {
    return cacheGetsTimer("hit");
  }

  private Timer cacheGetsMissTimer() {
    return cacheGetsTimer("miss");
  }

  private Timer cacheGetsTimer(String resultTagValue) {
    return meterRegistry
        .find("geode.cache.gets")
        .tag("result", resultTagValue)
        .timer();
  }
}
