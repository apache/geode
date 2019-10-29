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
package org.apache.geode.metrics.internal;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

public class StandardMeterBinderTest {
  @Test
  public void bindsJvmMemoryMeters() {
    MeterRegistry registry = new SimpleMeterRegistry();

    new StandardMeterBinder().bindTo(registry);

    assertThat(registry.find("jvm.buffer.count").gauge()).isNotNull();
    assertThat(registry.find("jvm.buffer.memory.used").gauge()).isNotNull();
    assertThat(registry.find("jvm.buffer.total.capacity").gauge()).isNotNull();
    assertThat(registry.find("jvm.memory.used").gauge()).isNotNull();
    assertThat(registry.find("jvm.memory.committed").gauge()).isNotNull();
    assertThat(registry.find("jvm.memory.max").gauge()).isNotNull();
  }

  @Test
  public void bindsJvmThreadMeters() {
    MeterRegistry registry = new SimpleMeterRegistry();

    new StandardMeterBinder().bindTo(registry);

    assertThat(registry.find("jvm.threads.peak").gauge()).isNotNull();
    assertThat(registry.find("jvm.threads.daemon").gauge()).isNotNull();
    assertThat(registry.find("jvm.threads.live").gauge()).isNotNull();

    Collection<Gauge> meters = registry.find("jvm.threads.states").gauges();

    List<String> threadStateTagValues = meters.stream()
        .map(meter -> meter.getId().getTag("state"))
        .filter(Objects::nonNull)
        .collect(toList());

    assertThat(threadStateTagValues)
        .hasSameElementsAs(tagValuesForAllThreadStates());
  }

  @Test
  public void bindsProcessUptimeMeters() {
    MeterRegistry registry = new SimpleMeterRegistry();

    new StandardMeterBinder().bindTo(registry);

    assertThat(registry.find("process.uptime").timeGauge()).isNotNull();
    assertThat(registry.find("process.start.time").timeGauge()).isNotNull();
  }

  @Test
  public void bindsSystemCpuMeters() {
    MeterRegistry registry = new SimpleMeterRegistry();

    new StandardMeterBinder().bindTo(registry);

    assertThat(registry.find("system.cpu.count").gauge()).isNotNull();
  }

  private static List<String> tagValuesForAllThreadStates() {
    return Stream.of(Thread.State.values())
        .map(Thread.State::name)
        .map(String::toLowerCase)
        .map(stateName -> stateName.replace("_", "-"))
        .collect(toList());
  }
}
