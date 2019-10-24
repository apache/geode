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

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

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

public class RegionPerfStatsTest {

  private static final String REGION_NAME = "region1";
  private static final String TEXT_ID = "textId";
  private static final DataPolicy DATA_POLICY = DataPolicy.PERSISTENT_REPLICATE;

  private MeterRegistry meterRegistry;
  private CachePerfStats cachePerfStats;
  private InternalRegion region;

  private RegionPerfStats regionPerfStats;
  private RegionPerfStats regionPerfStats2;
  private Statistics statistics;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    cachePerfStats = mock(CachePerfStats.class);
    StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    statistics = mock(Statistics.class);
    region = mock(InternalRegion.class);
    when(region.getName()).thenReturn(REGION_NAME);
    when(region.getDataPolicy()).thenReturn(DATA_POLICY);

    when(statisticsFactory.createAtomicStatistics(any(), any())).thenReturn(statistics);

    regionPerfStats =
        new RegionPerfStats(statisticsFactory, TEXT_ID, disabledClock(), cachePerfStats, region,
            meterRegistry);
  }

  @After
  public void closeStats() {
    if (regionPerfStats != null) {
      regionPerfStats.close();
    }
    if (regionPerfStats2 != null) {
      regionPerfStats2.close();
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
    Gauge entriesGauge = meterRegistry
        .find("geode.cache.entries")
        .gauge();

    assertThat(entriesGauge.getId().getTag("region"))
        .as("region tag")
        .isEqualTo(REGION_NAME);
  }

  @Test
  public void constructor_createsEntriesGauge_taggedWithDataPolicy() {
    Gauge entriesGauge = meterRegistry
        .find("geode.cache.entries")
        .gauge();
    assertThat(entriesGauge.getId().getTag("data.policy"))
        .as("data.policy tag")
        .isEqualTo(DATA_POLICY.toString());
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
  public void entryCountGaugeFetchesValueFromRegionLocalSize() {
    when(region.getLocalSize()).thenReturn(3);

    Gauge entriesGauge = meterRegistry
        .find("geode.cache.entries")
        .tag("region", REGION_NAME)
        .gauge();
    assertThat(entriesGauge.value()).isEqualTo(3);
  }

  @Test
  public void close_removesItsOwnMetersFromTheRegistry() {
    assertThat(meterNamed("geode.cache.entries"))
        .as("entries gauge before closing the stats")
        .isNotNull();

    regionPerfStats.close();

    assertThat(meterNamed("geode.cache.entries"))
        .as("entries gauge after closing the stats")
        .isNull();

    regionPerfStats = null;
  }

  @Test
  public void close_doesNotRemoveMetersItDoesNotOwn() {
    String foreignMeterName = "some.meter.not.created.by.the.gateway.receiver.stats";

    Timer.builder(foreignMeterName)
        .register(meterRegistry);

    regionPerfStats.close();

    assertThat(meterNamed(foreignMeterName))
        .as("foreign meter after closing the stats")
        .isNotNull();

    regionPerfStats = null;
  }

  private Meter meterNamed(String meterName) {
    return meterRegistry
        .find(meterName)
        .meter();
  }
}
