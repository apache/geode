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
import static org.mockito.quality.Strictness.LENIENT;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
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
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.statistics.StatisticsManager;

public class LocalRegionMetricsTest {
  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(LENIENT);

  private final String myRegion = "MyRegion";
  private final AtomicLong clockTime = new AtomicLong();

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
  @Mock
  private ServerRegionProxy serverRegionProxy;
  @Mock
  private PoolImpl pool;
  @Mock
  private Clock clock;

  @Before
  public void setUp() {
    meterRegistry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);

    when(cache.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(cache.getDistributedSystem()).thenReturn(internalDistributedSystem);
    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(cache.getMeterRegistry()).thenReturn(meterRegistry);

    when(clock.monotonicTime()).thenAnswer(invocation -> clockTime.incrementAndGet());

    when(entryEvent.setCreate(anyBoolean())).thenReturn(entryEvent);

    when(entryEventFactory.create(any(), any(), any(), any(), any(), anyBoolean(), any()))
        .thenReturn(entryEvent);

    when(internalDataView
        .putEntry(any(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyLong(), anyBoolean()))
            .thenReturn(true);

    when(internalDistributedSystem.getClock()).thenReturn(mock(DSClock.class));
    when(internalDistributedSystem.getConfig()).thenReturn(mock(DistributionConfig.class));
    when(internalDistributedSystem.getStatisticsManager()).thenReturn(statisticsManager);

    when(regionAttributes.getDataPolicy()).thenReturn(dataPolicy);
    when(regionAttributes.getDiskWriteAttributes())
        .thenReturn(new DiskWriteAttributesImpl(new Properties()));
    when(regionAttributes.getEntryIdleTimeout()).thenReturn(new ExpirationAttributes());
    when(regionAttributes.getEntryTimeToLive()).thenReturn(new ExpirationAttributes());
    when(regionAttributes.getEvictionAttributes()).thenReturn(new EvictionAttributesImpl());
    when(regionAttributes.getRegionIdleTimeout()).thenReturn(new ExpirationAttributes());
    when(regionAttributes.getRegionTimeToLive()).thenReturn(new ExpirationAttributes());
    when(regionAttributes.getScope()).thenReturn(Scope.LOCAL);

    when(statisticsManager.createAtomicStatistics(any(), any()))
        .thenReturn(mock(Statistics.class));
  }

  @Test
  public void create_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.create("", "");

    Timer timer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "create")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void createWithCallbackArgument_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.create("", "", null);

    Timer timer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "create")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void put_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.put("", "");

    Timer timer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "put")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void putWithCallback_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.put("", "", "");

    Timer timer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "put")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void putIfAbsent_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.putIfAbsent("", "");

    Timer timer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "put-if-absent")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void putIfAbsentWithCallback_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.putIfAbsent("", "", "");

    Timer timer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "put-if-absent")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void replace_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.replace("", "");

    Timer timer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "replace")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void replaceWithOldAndNewValues_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.replace("", "", "");

    Timer timer = meterRegistry.find("cache.region.operations.puts")
        .tag("region.name", myRegion)
        .tag("put.type", "replace")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void get_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.get("");

    Timer timer = meterRegistry.find("cache.region.operations.gets")
        .tag("region.name", myRegion)
        .tag("get.type", "get")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void getWithCallback_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.get("", "");

    Timer timer = meterRegistry.find("cache.region.operations.gets")
        .tag("region.name", myRegion)
        .tag("get.type", "get")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void getEntry_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.getEntry("");

    Timer timer = meterRegistry.find("cache.region.operations.gets")
        .tag("region.name", myRegion)
        .tag("get.type", "get-entry")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void containsKey_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.containsKey("");

    Timer timer = meterRegistry.find("cache.region.operations.contains")
        .tag("region.name", myRegion)
        .tag("contains.type", "contains-key")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void containsValue_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegion(myRegion);

    localRegion.containsValue("");

    Timer timer = meterRegistry.find("cache.region.operations.contains")
        .tag("region.name", myRegion)
        .tag("contains.type", "contains-value")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void containsKeyOnServer_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegionForClient(myRegion);

    localRegion.containsKeyOnServer("");

    Timer timer = meterRegistry.find("cache.region.operations.contains")
        .tag("region.name", myRegion)
        .tag("contains.type", "contains-key-on-server")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  @Test
  public void containsValueForKey_timesHowLongItTook() {
    LocalRegion localRegion = createLocalRegionForClient(myRegion);

    localRegion.containsValueForKey("");

    Timer timer = meterRegistry.find("cache.region.operations.contains")
        .tag("region.name", myRegion)
        .tag("contains.type", "contains-value-for-key")
        .timer();

    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1L);
    assertThat(timer.totalTime(TimeUnit.NANOSECONDS)).isGreaterThan(0);
  }

  private LocalRegion createLocalRegion(String regionName) {
    return new LocalRegion(regionName, regionAttributes, null, cache, internalRegionArgs,
        internalDataView, (a, b, c) -> regionMap, (a) -> null, entryEventFactory, (a) -> null);
  }

  private LocalRegion createLocalRegionForClient(String regionName) {
    when(regionAttributes.getPoolName()).thenReturn("pool");
    return new LocalRegion(regionName, regionAttributes, null, cache, internalRegionArgs,
        internalDataView, (a, b, c) -> regionMap, (a) -> serverRegionProxy, entryEventFactory,
        (a) -> pool);
  }
}
