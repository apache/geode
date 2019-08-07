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
package org.apache.geode.internal.metrics;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

public class CacheMeterRegistryFactoryBindersTest {

  private static final String[] COMMON_TAG_KEYS =
      {"cluster", "member", "host", "member.type"};
  private CompositeMeterRegistry registry;

  @Before
  public void before() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    registry = factory.create(42, "member-name", "host-name", false, "member-type");
    registry.add(new SimpleMeterRegistry());
  }

  @Test
  public void addsJvmMemoryMeters() {
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.buffer.count");
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.buffer.memory.used");
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.buffer.total.capacity");
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.memory.used");
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.memory.committed");
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.memory.max");
  }

  @Test
  public void addsJvmThreadMeters() {
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.threads.peak");
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.threads.daemon");
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.threads.live");
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.BLOCKED)));
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.NEW)));
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.RUNNABLE)));
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.WAITING)));
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.TIMED_WAITING)));
    assertThatMeterExistsWithCommonTags(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.TERMINATED)));
  }

  @Test
  public void addsSystemCpuMeters() {
    assertThatMeterExistsWithCommonTags(Gauge.class, "system.cpu.count");
  }

  @Test
  public void addsProcessUptimeMeters() {
    assertThatMeterExistsWithCommonTags(TimeGauge.class, "process.uptime");
    assertThatMeterExistsWithCommonTags(TimeGauge.class, "process.start.time");
  }

  private static String getTagValue(Thread.State state) {
    return state.name().toLowerCase().replace("_", "-");
  }

  private <T extends Meter> void assertThatMeterExistsWithCommonTags(Class<T> type, String name,
      Tag... customTags) {
    Collection<Meter> meters = registry
        .find(name)
        .tags(asList(customTags))
        .meters();

    assertThat(meters).isNotEmpty();
    assertThat(meters)
        .allMatch(type::isInstance, "instance of " + type);

    meters.forEach(CacheMeterRegistryFactoryBindersTest::assertThatHasCommonTags);
  }

  private static void assertThatHasCommonTags(Meter meter) {
    List<String> keys = meter.getId().getTags().stream().map(Tag::getKey).collect(toList());

    assertThat(keys)
        .as("Tags for meter %s", meter.getId().getName())
        .contains(COMMON_TAG_KEYS);
  }
}
