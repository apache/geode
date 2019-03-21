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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.statistics.StatisticsManager;

public class LocalRegionMetricsTest {
  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  private final String myRegion = "MyRegion";
  private MeterRegistry meterRegistry;

  @Mock
  private RegionAttributes<?, ?> regionAttributes;
  @Mock
  private InternalCache cache;
  @Mock
  private InternalRegionArguments internalRegionArgs;
  @Mock
  private InternalDistributedSystem internalDistributedSystem;
  @Mock
  private DataPolicy dataPolicy;
  @Mock
  private RegionMap regionMap;
  @Mock
  private EntryEventImpl entryEvent;
  @Mock
  private InternalDataView internalDataView;
  @Mock
  private StatisticsManager statisticsManager;
  @Mock
  private EntryEventFactory entryEventFactory;

  @Before
  public void setUp() {
    meterRegistry = new SimpleMeterRegistry();

    when(cache.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(cache.getDistributedSystem()).thenReturn(internalDistributedSystem);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);

    when(entryEventFactory.create(any(), any(), any(), any(), any(), anyBoolean(), any()))
        .thenReturn(entryEvent);

    when(internalDistributedSystem.getClock()).thenReturn(mock(DSClock.class));
    when(internalDistributedSystem.getStatisticsManager()).thenReturn(statisticsManager);

    when(regionAttributes.getDataPolicy()).thenReturn(dataPolicy);
    when(regionAttributes.getDiskWriteAttributes())
        .thenReturn(new DiskWriteAttributesImpl(new Properties()));
    when(regionAttributes.getEntryIdleTimeout()).thenReturn(new ExpirationAttributes());
    when(regionAttributes.getEntryTimeToLive()).thenReturn(new ExpirationAttributes());
    when(regionAttributes.getEvictionAttributes()).thenReturn(new EvictionAttributesImpl());
    when(regionAttributes.getRegionIdleTimeout()).thenReturn(new ExpirationAttributes());
    when(regionAttributes.getRegionTimeToLive()).thenReturn(new ExpirationAttributes());

    when(statisticsManager.createAtomicStatistics(any(), any()))
        .thenReturn(mock(Statistics.class));
  }

  @Test
  public void create_timesHowLongItTook() {
    when(cache.getMeterRegistry()).thenReturn(meterRegistry);
    when(entryEvent.setCreate(anyBoolean())).thenReturn(entryEvent);
    when(internalDataView
        .putEntry(any(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyLong(), anyBoolean()))
            .thenReturn(true);

    LocalRegion localRegion =
        new LocalRegion(myRegion, regionAttributes, null, cache, internalRegionArgs,
            internalDataView, (a, b, c) -> regionMap, entryEventFactory);

    localRegion.create("", "");

    Timer createTimer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "create")
        .timer();

    assertThat(createTimer).isNotNull();
    assertThat(createTimer.count()).isEqualTo(1L);
  }

  @Test
  public void createWithCallbackArgument_timesHowLongItTook() {
    when(cache.getMeterRegistry()).thenReturn(meterRegistry);
    when(entryEvent.setCreate(anyBoolean())).thenReturn(entryEvent);
    when(internalDataView
        .putEntry(any(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyLong(), anyBoolean()))
            .thenReturn(true);

    LocalRegion localRegion =
        new LocalRegion(myRegion, regionAttributes, null, cache, internalRegionArgs,
            internalDataView, (a, b, c) -> regionMap, entryEventFactory);

    localRegion.create("", "", null);

    Timer createTimer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "create")
        .timer();

    assertThat(createTimer).isNotNull();
    assertThat(createTimer.count()).isEqualTo(1L);
  }

  @Test
  public void put_timesHowLongItTook() {
    when(cache.getMeterRegistry()).thenReturn(meterRegistry);
    when(internalDataView
        .putEntry(any(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyLong(), anyBoolean()))
            .thenReturn(true);
    when(internalDistributedSystem.getConfig()).thenReturn(mock(DistributionConfig.class));

    LocalRegion localRegion =
        new LocalRegion(myRegion, regionAttributes, null, cache, internalRegionArgs,
            internalDataView, (a, b, c) -> regionMap, entryEventFactory);

    localRegion.put("", "");

    Timer putTimer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "put")
        .timer();

    assertThat(putTimer).isNotNull();
    assertThat(putTimer.count()).isEqualTo(1L);
  }

  @Test
  public void putWithCallback_timesHowLongItTook() {
    when(cache.getMeterRegistry()).thenReturn(meterRegistry);
    when(internalDataView
        .putEntry(any(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyLong(), anyBoolean()))
            .thenReturn(true);
    when(internalDistributedSystem.getConfig()).thenReturn(mock(DistributionConfig.class));

    LocalRegion localRegion =
        new LocalRegion(myRegion, regionAttributes, null, cache, internalRegionArgs,
            internalDataView, (a, b, c) -> regionMap, entryEventFactory);

    localRegion.put("", "", "");

    Timer putTimer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "put")
        .timer();

    assertThat(putTimer).isNotNull();
    assertThat(putTimer.count()).isEqualTo(1L);
  }
}
