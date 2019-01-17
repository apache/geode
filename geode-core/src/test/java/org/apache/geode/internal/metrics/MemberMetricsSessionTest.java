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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionConfig;

public class MemberMetricsSessionTest {
  private final DistributionConfig config = mock(DistributionConfig.class);

  @Test
  public void createsARegistry() {
    MemberMetricsSession metricsSession = new MemberMetricsSession(config);

    assertThat(metricsSession.meterRegistry())
        .isNotNull();
  }

  @Test
  public void managedRegistry_startsWithNoDownstreamRegistries() {
    CompositeMeterRegistry managedRegistry = new CompositeMeterRegistry();
    MemberMetricsSession metricsSession = new MemberMetricsSession(managedRegistry, config);

    MeterRegistry primaryRegistry = metricsSession.meterRegistry();
    assertThat(primaryRegistry)
        .isSameAs(managedRegistry);

    Set<MeterRegistry> downstreamRegistries = managedRegistry.getRegistries();

    assertThat(downstreamRegistries)
        .isEmpty();
  }

  @Test
  public void managedRegistry_addsSystemIdTag_toNewMeters() {
    int idFromConfig = 124;
    when(config.getDistributedSystemId()).thenReturn(idFromConfig);

    MemberMetricsSession metricsSession = new MemberMetricsSession(config);
    MeterRegistry managedRegistry = metricsSession.meterRegistry();

    Meter meter = managedRegistry.counter("my.meter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of("DistributedSystemID", String.valueOf(idFromConfig)));
  }

  @Test
  public void managedRegistry_addsMemberNameTag_toNewMeters() {
    String memberNameFromConfig = "the-member-name";
    when(config.getName()).thenReturn(memberNameFromConfig);

    MemberMetricsSession metricsSession = new MemberMetricsSession(config);
    MeterRegistry managedRegistry = metricsSession.meterRegistry();

    Meter meter = managedRegistry.counter("my.meter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of("MemberName", memberNameFromConfig));
  }

  @Test
  public void memberNameTagValueIsEmpty_ifConfigurationMemberNameIsNull() {
    when(config.getName()).thenReturn(null);

    MemberMetricsSession metricsSession = new MemberMetricsSession(config);
    MeterRegistry managedRegistry = metricsSession.meterRegistry();

    Meter meter = managedRegistry.counter("my.counter");

    assertThat(meter.getId().getTags())
        .contains(Tag.of("MemberName", ""));
  }

  @Test
  public void remembersConnectedDownstreamRegistries() {
    CompositeMeterRegistry managedRegistry = new CompositeMeterRegistry();
    MemberMetricsSession metricsSession = new MemberMetricsSession(managedRegistry, config);
    MeterRegistry downstreamRegistry = new SimpleMeterRegistry();

    metricsSession.connectDownstreamRegistry(downstreamRegistry);

    assertThat(managedRegistry.getRegistries())
        .contains(downstreamRegistry);
  }

  @Test
  public void forgetsDisconnectedDownstreamRegistries() {
    CompositeMeterRegistry managedRegistry = new CompositeMeterRegistry();
    MemberMetricsSession metricsSession = new MemberMetricsSession(managedRegistry, config);
    MeterRegistry downstreamRegistry = new SimpleMeterRegistry();
    metricsSession.connectDownstreamRegistry(downstreamRegistry);

    metricsSession.disconnectDownstreamRegistry(downstreamRegistry);

    assertThat(managedRegistry.getRegistries())
        .doesNotContain(downstreamRegistry);
  }

  @Test
  public void connectsExistingMetersToNewDownstreamRegistries() {
    MemberMetricsSession metricsSession = new MemberMetricsSession(config);
    MeterRegistry primaryRegistry = metricsSession.meterRegistry();

    String counterName = "the.counter";
    Counter primaryCounter = primaryRegistry.counter(counterName);

    double amountIncrementedBeforeConnectingDownstreamRegistry = 3.0;
    primaryCounter.increment(amountIncrementedBeforeConnectingDownstreamRegistry);

    MeterRegistry downstreamRegistry = new SimpleMeterRegistry();
    metricsSession.connectDownstreamRegistry(downstreamRegistry);

    Counter downstreamCounter = downstreamRegistry.find(counterName).counter();
    assertThat(downstreamCounter)
        .as("downstream counter after connecting, before incrementing")
        .isNotNull();

    // Note that the newly-created downstream counter starts at zero, ignoring
    // any increments that happened before the downstream registry was added.
    assertThat(downstreamCounter.count())
        .as("downstream counter value after connecting, before incrementing")
        .isNotEqualTo(amountIncrementedBeforeConnectingDownstreamRegistry)
        .isEqualTo(0);

    double amountIncrementedAfterConnectingDownstreamRegistry = 42.0;
    primaryCounter.increment(amountIncrementedAfterConnectingDownstreamRegistry);

    assertThat(downstreamCounter.count())
        .as("downstream counter value after incrementing")
        .isEqualTo(amountIncrementedAfterConnectingDownstreamRegistry);
  }

  @Test
  public void connectsNewMetersToExistingDownstreamRegistries() {
    MemberMetricsSession metricsSession = new MemberMetricsSession(config);
    MeterRegistry primaryRegistry = metricsSession.meterRegistry();

    MeterRegistry downstreamRegistry = new SimpleMeterRegistry();
    metricsSession.connectDownstreamRegistry(downstreamRegistry);

    String counterName = "the.counter";
    Counter newCounter = primaryRegistry.counter(counterName);

    Counter downstreamCounter = downstreamRegistry.find(counterName).counter();
    assertThat(downstreamCounter)
        .as("downstream counter before incrementing")
        .isNotNull();

    assertThat(downstreamCounter.count())
        .as("downstream counter value before incrementing")
        .isEqualTo(newCounter.count())
        .isEqualTo(0);

    double amountIncrementedAfterConnectingDownstreamRegistry = 93.0;
    newCounter.increment(amountIncrementedAfterConnectingDownstreamRegistry);

    assertThat(downstreamCounter.count())
        .as("downstream counter value after incrementing")
        .isEqualTo(newCounter.count());
  }
}
