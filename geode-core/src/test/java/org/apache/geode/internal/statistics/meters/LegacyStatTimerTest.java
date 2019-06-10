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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

import org.apache.geode.Statistics;

public class LegacyStatTimerTest {
  private final MockClock clock = new MockClock();
  private final MeterRegistry registry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);

  @Test
  public void builder_registersATimerWithTheBuiltId() {
    List<Tag> tagsFromList = new ArrayList<>();
    tagsFromList.add(Tag.of("tag.one", "tag.one.value"));
    tagsFromList.add(Tag.of("tag.two", "tag.two.value"));

    LegacyStatTimer.builder("my.meter.name")
        .tags(tagsFromList)
        .tag("tag.three", "tag.three.value")
        .tag("tag.four", "tag.four.value")
        .tags("tag.five", "tag.five.value", "tag.six", "tag.six.value")
        .description("my meter description")
        .register(registry);

    Timer underlyingTimer = registry.find("my.meter.name").timer();

    assertThat(underlyingTimer)
        .as("underlying timer")
        .isNotNull();

    Meter.Id underlyingTimerId = underlyingTimer.getId();

    assertThat(underlyingTimerId)
        .as("underlying timer ID")
        .isNotNull();

    assertThat(underlyingTimerId.getName())
        .as("underlying timer name")
        .isEqualTo("my.meter.name");

    assertThat(underlyingTimerId.getTags())
        .as("underlying timer tags")
        .containsOnly(
            Tag.of("tag.one", "tag.one.value"),
            Tag.of("tag.two", "tag.two.value"),
            Tag.of("tag.three", "tag.three.value"),
            Tag.of("tag.four", "tag.four.value"),
            Tag.of("tag.five", "tag.five.value"),
            Tag.of("tag.six", "tag.six.value"));

    assertThat(underlyingTimerId.getDescription())
        .as("underlying timer description")
        .isEqualTo("my meter description");
  }

  @Test
  public void hasSameIdAsUnderlyingTimer() {
    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .register(registry);

    Timer underlyingTimer = registry.find("my.meter.name").timer();

    assertThat(legacyStatTimer.getId())
        .isSameAs(underlyingTimer.getId());
  }

  @Test
  public void hasSameBaseTimeUnitAsUnderlyingTimer() {
    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .register(registry);

    Timer underlyingTimer = registry.find("my.meter.name").timer();

    assertThat(legacyStatTimer.baseTimeUnit())
        .isEqualTo(underlyingTimer.baseTimeUnit());
  }

  @Test
  public void recordDuration_recordsToUnderlyingTimer() {
    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .register(registry);

    Timer underlyingTimer = registry.find("my.meter.name").timer();

    legacyStatTimer.record(17, SECONDS);
    legacyStatTimer.record(5, SECONDS);

    assertThat(underlyingTimer.count())
        .as("underlying timer count")
        .isEqualTo(2L);

    assertThat(underlyingTimer.totalTime(SECONDS))
        .as("underlying timer total time")
        .isEqualTo(22.0);
  }

  @Test
  public void recordRunnable_recordsToUnderlyingTimer() {
    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .register(registry);

    Duration duration = Duration.ofNanos(543_234_234_234L);
    Runnable timeConsumingRunnable = () -> clock.add(duration);

    legacyStatTimer.record(timeConsumingRunnable);

    Timer underlyingTimer = registry.find("my.meter.name").timer();

    assertThat(underlyingTimer.count())
        .as("underlying timer count")
        .isEqualTo(1L);

    assertThat(underlyingTimer.totalTime(NANOSECONDS))
        .as("underlying timer total time")
        .isEqualTo((double) duration.toNanos());
  }

  @Test
  public void recordSupplier_recordsToUnderlyingTimer() {
    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .register(registry);

    Duration duration = Duration.ofNanos(543_234_234_234L);
    Supplier<Void> timeConsumingSupplier = () -> {
      clock.add(duration);
      return null;
    };

    legacyStatTimer.record(timeConsumingSupplier);

    Timer underlyingTimer = registry.find("my.meter.name").timer();

    assertThat(underlyingTimer.count())
        .as("underlying timer count")
        .isEqualTo(1L);

    assertThat(underlyingTimer.totalTime(NANOSECONDS))
        .as("underlying timer total time")
        .isEqualTo((double) duration.toNanos());
  }

  @Test
  public void recordCallable_recordsToUnderlyingTimer() throws Exception {
    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .register(registry);

    Duration duration = Duration.ofNanos(543_234_234_234L);
    Callable<Void> timeConsumingCallable = () -> {
      clock.add(duration);
      return null;
    };

    legacyStatTimer.recordCallable(timeConsumingCallable);

    Timer underlyingTimer = registry.find("my.meter.name").timer();

    assertThat(underlyingTimer.count())
        .as("underlying timer count")
        .isEqualTo(1L);

    assertThat(underlyingTimer.totalTime(NANOSECONDS))
        .as("underlying timer total time")
        .isEqualTo((double) duration.toNanos());
  }

  @Test
  public void withDoubleCountStat_recordDuration_incrementsCountStat() {
    Statistics statistics = mock(Statistics.class);
    int countStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .doubleCountStatistic(statistics, countStatId)
        .register(registry);

    legacyStatTimer.record(17, NANOSECONDS);

    verify(statistics).incDouble(countStatId, 1);
  }

  @Test
  public void withDoubleCountStat_recordRunnable_incrementsCountStat() {
    Statistics statistics = mock(Statistics.class);
    int countStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .doubleCountStatistic(statistics, countStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(234L);
    Runnable timeConsumingRunnable = () -> clock.add(duration);

    legacyStatTimer.record(timeConsumingRunnable);

    verify(statistics).incDouble(countStatId, 1);
  }

  @Test
  public void withDoubleCountStat_recordSupplier_incrementsCountStat() {
    Statistics statistics = mock(Statistics.class);
    int countStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .doubleCountStatistic(statistics, countStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(17L);
    Supplier<Void> timeConsumingSupplier = () -> {
      clock.add(duration);
      return null;
    };

    legacyStatTimer.record(timeConsumingSupplier);

    verify(statistics).incDouble(countStatId, 1);
  }

  @Test
  public void withDoubleCountStat_recordCallable_incrementsCountStat() throws Exception {
    Statistics statistics = mock(Statistics.class);
    int countStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .doubleCountStatistic(statistics, countStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(543_234_234_234L);
    Callable<Void> timeConsumingCallable = () -> {
      clock.add(duration);
      return null;
    };

    legacyStatTimer.recordCallable(timeConsumingCallable);

    verify(statistics).incDouble(countStatId, 1);
  }

  @Test
  public void withDoubleCountStat_count_returnsCountStatAsLong() {
    Statistics statistics = mock(Statistics.class);
    int countStatId = 18;
    when(statistics.getDouble(countStatId)).thenReturn(92.0);

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .doubleCountStatistic(statistics, countStatId)
        .register(registry);

    assertThat(legacyStatTimer.count()).isEqualTo(92);
  }

  @Test
  public void withDoubleTimeStat_recordDuration_addsNanosToTimeStat() {
    Statistics statistics = mock(Statistics.class);
    int timeStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .doubleTimeStatistic(statistics, timeStatId)
        .register(registry);

    legacyStatTimer.record(17, SECONDS);

    verify(statistics).incDouble(timeStatId, 17_000_000_000.0);
  }

  @Test
  public void withDoubleTimeStat_recordRunnable_addsNanosToTimeStat() {
    Statistics statistics = mock(Statistics.class);
    int timeStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .doubleTimeStatistic(statistics, timeStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(543_234_234_234L);
    Runnable timeConsumingRunnable = () -> clock.add(duration);

    legacyStatTimer.record(timeConsumingRunnable);

    verify(statistics).incDouble(timeStatId, (double) duration.toNanos());
  }

  @Test
  public void withDoubleTimeStat_recordSupplier_addsNanosToTimeStat() {
    Statistics statistics = mock(Statistics.class);
    int timeStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .doubleTimeStatistic(statistics, timeStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(222L);
    Supplier<Void> timeConsumingSupplier = () -> {
      clock.add(duration);
      return null;
    };

    legacyStatTimer.record(timeConsumingSupplier);

    verify(statistics).incDouble(timeStatId, duration.toNanos());
  }

  @Test
  public void withDoubleTimeStat_recordCallable_addsNanosToTimeStat() throws Exception {
    Statistics statistics = mock(Statistics.class);
    int timeStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .doubleTimeStatistic(statistics, timeStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(222L);
    Callable<Void> timeConsumingCallable = () -> {
      clock.add(duration);
      return null;
    };

    legacyStatTimer.recordCallable(timeConsumingCallable);

    verify(statistics).incDouble(timeStatId, duration.toNanos());
  }

  @Test
  public void withDoubleTimeStat_totalTime_returnsTimeStatInGivenUnits() {
    Statistics statistics = mock(Statistics.class);
    int timeStatId = 18;
    when(statistics.getDouble(timeStatId)).thenReturn(92_123.0);

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .doubleTimeStatistic(statistics, timeStatId)
        .register(registry);

    assertThat(legacyStatTimer.totalTime(TimeUnit.MICROSECONDS)).isEqualTo(92.0);
  }

  @Test
  public void withLongCountStat_recordDuration_incrementsCountStat() {
    Statistics statistics = mock(Statistics.class);
    int countStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .longCountStatistic(statistics, countStatId)
        .register(registry);

    legacyStatTimer.record(17, NANOSECONDS);

    verify(statistics).incLong(countStatId, 1);
  }

  @Test
  public void withLongCountStat_recordRunnable_incrementsCountStat() {
    Statistics statistics = mock(Statistics.class);
    int countStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .longCountStatistic(statistics, countStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(234L);
    Runnable timeConsumingRunnable = () -> clock.add(duration);

    legacyStatTimer.record(timeConsumingRunnable);

    verify(statistics).incLong(countStatId, 1);
  }

  @Test
  public void withLongCountStat_recordSupplier_incrementsCountStat() {
    Statistics statistics = mock(Statistics.class);
    int countStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .longCountStatistic(statistics, countStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(17L);
    Supplier<Void> timeConsumingSupplier = () -> {
      clock.add(duration);
      return null;
    };

    legacyStatTimer.record(timeConsumingSupplier);

    verify(statistics).incLong(countStatId, 1);
  }

  @Test
  public void withLongCountStat_recordCallable_incrementsCountStat() throws Exception {
    Statistics statistics = mock(Statistics.class);
    int countStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .longCountStatistic(statistics, countStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(543_234_234_234L);
    Callable<Void> timeConsumingCallable = () -> {
      clock.add(duration);
      return null;
    };

    legacyStatTimer.recordCallable(timeConsumingCallable);

    verify(statistics).incLong(countStatId, 1);
  }

  @Test
  public void withLongCountStat_count_returnsCountStat() {
    Statistics statistics = mock(Statistics.class);
    int countStatId = 18;
    when(statistics.getLong(countStatId)).thenReturn(123_456_789_000L);

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .longCountStatistic(statistics, countStatId)
        .register(registry);

    assertThat(legacyStatTimer.count()).isEqualTo(123_456_789_000L);
  }

  @Test
  public void withLongTimeStat_recordDuration_addsNanosToTimeStat() {
    Statistics statistics = mock(Statistics.class);
    int timeStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .longTimeStatistic(statistics, timeStatId)
        .register(registry);

    legacyStatTimer.record(17, SECONDS);

    verify(statistics).incLong(timeStatId, 17_000_000_000L);
  }

  @Test
  public void withLongTimeStat_recordRunnable_addsNanosToTimeStat() {
    Statistics statistics = mock(Statistics.class);
    int timeStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .longTimeStatistic(statistics, timeStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(543_234_234_234L);
    Runnable timeConsumingRunnable = () -> clock.add(duration);

    legacyStatTimer.record(timeConsumingRunnable);

    verify(statistics).incLong(timeStatId, duration.toNanos());
  }

  @Test
  public void withLongTimeStat_recordSupplier_addsNanosToTimeStat() {
    Statistics statistics = mock(Statistics.class);
    int timeStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .longTimeStatistic(statistics, timeStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(222L);
    Supplier<Void> timeConsumingSupplier = () -> {
      clock.add(duration);
      return null;
    };

    legacyStatTimer.record(timeConsumingSupplier);

    verify(statistics).incLong(timeStatId, duration.toNanos());
  }

  @Test
  public void withLongTimeStat_recordCallable_addsNanosToTimeStat() throws Exception {
    Statistics statistics = mock(Statistics.class);
    int timeStatId = 18;

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .longTimeStatistic(statistics, timeStatId)
        .register(registry);

    Duration duration = Duration.ofNanos(222L);
    Callable<Void> timeConsumingCallable = () -> {
      clock.add(duration);
      return null;
    };

    legacyStatTimer.recordCallable(timeConsumingCallable);

    verify(statistics).incLong(timeStatId, duration.toNanos());
  }

  @Test
  public void withLongTimeStat_totalTime_returnsTimeStatInGivenUnits() {
    Statistics statistics = mock(Statistics.class);
    int timeStatId = 18;
    when(statistics.getLong(timeStatId)).thenReturn(987_654_321_000L);

    Timer legacyStatTimer = LegacyStatTimer.builder("my.meter.name")
        .longTimeStatistic(statistics, timeStatId)
        .register(registry);

    assertThat(legacyStatTimer.totalTime(TimeUnit.MICROSECONDS)).isEqualTo(987_654_321L);
  }
}
