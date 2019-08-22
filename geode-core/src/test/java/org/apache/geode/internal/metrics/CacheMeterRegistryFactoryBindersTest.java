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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;

public class CacheMeterRegistryFactoryBindersTest {

  private CompositeMeterRegistry registry;

  @Before
  public void before() {
    CacheMeterRegistryFactory factory = new CacheMeterRegistryFactory();

    registry = factory.create(42, "member-name", "host-name");
    registry.add(new SimpleMeterRegistry());
  }

  @Test
  public void verifyThatJvmMemoryBinderMetersExist() {
    assertThatMeterExists(Gauge.class, "jvm.buffer.count");
    assertThatMeterExists(Gauge.class, "jvm.buffer.memory.used");
    assertThatMeterExists(Gauge.class, "jvm.buffer.total.capacity");
    assertThatMeterExists(Gauge.class, "jvm.memory.used");
    assertThatMeterExists(Gauge.class, "jvm.memory.committed");
    assertThatMeterExists(Gauge.class, "jvm.memory.max");
  }

  @Test
  public void verifyThatJvmThreadBinderMetersExist() {
    assertThatMeterExists(Gauge.class, "jvm.threads.peak");
    assertThatMeterExists(Gauge.class, "jvm.threads.daemon");
    assertThatMeterExists(Gauge.class, "jvm.threads.live");
    assertThatMeterExists(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.BLOCKED)));
    assertThatMeterExists(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.NEW)));
    assertThatMeterExists(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.RUNNABLE)));
    assertThatMeterExists(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.WAITING)));
    assertThatMeterExists(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.TIMED_WAITING)));
    assertThatMeterExists(Gauge.class, "jvm.threads.states",
        Tag.of("state", getTagValue(Thread.State.TERMINATED)));
  }

  @Test
  public void verifyThatProcessorBinderMetersExist() {
    assertThatMeterExists(Gauge.class, "system.cpu.count");
  }

  @Test
  public void verifyThatUptimeBinderMetersExist() {
    assertThatMeterExists(TimeGauge.class, "process.uptime");
    assertThatMeterExists(TimeGauge.class, "process.start.time");
  }

  private static String getTagValue(Thread.State state) {
    return state.name().toLowerCase().replace("_", "-");
  }

  private <T extends Meter> void assertThatMeterExists(Class<T> type, String name, Tag... tags) {
    Collection<Meter> meters = registry
        .find(name)
        .tags(Arrays.asList(tags))
        .meters();

    assertThat(meters).isNotEmpty();
    assertThat(meters).first().isInstanceOf(type);
  }
}
