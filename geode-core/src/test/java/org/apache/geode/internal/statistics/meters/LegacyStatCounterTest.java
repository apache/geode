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

import java.util.ArrayList;
import java.util.List;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

import org.apache.geode.Statistics;

public class LegacyStatCounterTest {
  private final MeterRegistry registry = new SimpleMeterRegistry();

  @Test
  public void builder_registersACounterWithTheBuiltId() {
    List<Tag> tagsFromList = new ArrayList<>();
    tagsFromList.add(Tag.of("tag.one", "tag.one.value"));
    tagsFromList.add(Tag.of("tag.two", "tag.two.value"));

    LegacyStatCounter.builder("my.meter.name")
        .tags(tagsFromList)
        .tag("tag.three", "tag.three.value")
        .tag("tag.four", "tag.four.value")
        .tags("tag.five", "tag.five.value", "tag.six", "tag.six.value")
        .description("my meter description")
        .baseUnit("my meter base unit")
        .register(registry);

    Counter underlyingCounter = registry.find("my.meter.name").counter();

    assertThat(underlyingCounter)
        .as("underlying counter")
        .isNotNull();

    Meter.Id underlyingCounterId = underlyingCounter.getId();

    assertThat(underlyingCounterId)
        .as("underlying counter ID")
        .isNotNull();

    assertThat(underlyingCounterId.getName())
        .as("underlying counter name")
        .isEqualTo("my.meter.name");

    assertThat(underlyingCounterId.getTags())
        .as("underlying counter tags")
        .containsOnly(
            Tag.of("tag.one", "tag.one.value"),
            Tag.of("tag.two", "tag.two.value"),
            Tag.of("tag.three", "tag.three.value"),
            Tag.of("tag.four", "tag.four.value"),
            Tag.of("tag.five", "tag.five.value"),
            Tag.of("tag.six", "tag.six.value"));

    assertThat(underlyingCounterId.getDescription())
        .as("underlying counter description")
        .isEqualTo("my meter description");

    assertThat(underlyingCounterId.getBaseUnit())
        .as("underlying counter base unit")
        .isEqualTo("my meter base unit");
  }

  @Test
  public void hasSameIdAsUnderlyingCounter() {
    Counter legacyStatCounter = LegacyStatCounter.builder("my.meter.name")
        .register(registry);

    Counter underlyingCounter = registry.find("my.meter.name").counter();

    assertThat(legacyStatCounter.getId())
        .isSameAs(underlyingCounter.getId());
  }

  @Test
  public void increment_incrementsUnderlyingCounter() {
    Counter legacyStatCounter = LegacyStatCounter.builder("my.meter.name")
        .register(registry);

    Counter underlyingCounter = registry.find("my.meter.name").counter();

    legacyStatCounter.increment(22.9);

    assertThat(underlyingCounter.count())
        .isEqualTo(22.9);
  }

  @Test
  public void withDoubleStat_increment_incrementsDoubleStat() {
    Statistics statistics = mock(Statistics.class);
    int statId = 93;

    Counter legacyStatCounter = LegacyStatCounter.builder("my.meter.name")
        .doubleStatistic(statistics, statId)
        .register(registry);

    legacyStatCounter.increment(8493.2);

    verify(statistics).incDouble(statId, 8493.2);
  }

  @Test
  public void withDoubleStat_count_readsFromDoubleStat() {
    Statistics statistics = mock(Statistics.class);
    int statId = 93;

    Counter legacyStatCounter = LegacyStatCounter.builder("my.meter.name")
        .doubleStatistic(statistics, statId)
        .register(registry);

    when(statistics.getDouble(statId)).thenReturn(934.7);

    assertThat(legacyStatCounter.count())
        .isEqualTo(934.7);
  }

  @Test
  public void withIntStat_increment_incrementsIntStat() {
    Statistics statistics = mock(Statistics.class);
    int statId = 93;

    Counter legacyStatCounter = LegacyStatCounter.builder("my.meter.name")
        .intStatistic(statistics, statId)
        .register(registry);

    legacyStatCounter.increment(8493.0);

    verify(statistics).incInt(statId, 8493);
  }

  @Test
  public void withIntStat_count_readsFromLongStat() {
    Statistics statistics = mock(Statistics.class);
    int statId = 93;

    Counter legacyStatCounter = LegacyStatCounter.builder("my.meter.name")
        .intStatistic(statistics, statId)
        .register(registry);

    when(statistics.getInt(statId)).thenReturn(47282903);

    assertThat(legacyStatCounter.count())
        .isEqualTo(47282903.0);
  }

  @Test
  public void withLongStat_increment_incrementsLongStat() {
    Statistics statistics = mock(Statistics.class);
    int statId = 93;

    Counter legacyStatCounter = LegacyStatCounter.builder("my.meter.name")
        .longStatistic(statistics, statId)
        .register(registry);

    legacyStatCounter.increment(8493.0);

    verify(statistics).incLong(statId, 8493);
  }

  @Test
  public void withLongStat_count_readsFromLongStat() {
    Statistics statistics = mock(Statistics.class);
    int statId = 93;

    Counter legacyStatCounter = LegacyStatCounter.builder("my.meter.name")
        .longStatistic(statistics, statId)
        .register(registry);

    when(statistics.getLong(statId)).thenReturn(472829034L);

    assertThat(legacyStatCounter.count())
        .isEqualTo(472829034.0);
  }
}
