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
package org.apache.geode.internal.cache.wan;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;

public class GatewayReceiverStatsTest {

  private static final String EVENTS_RECEIVED_COUNTER_NAME =
      "cache.gatewayreceiver.events.received";
  private static final String EVENTS_RECEIVED_STAT_NAME = "eventsReceived";

  private StatisticsType statisticsType;
  private Statistics statistics;
  private MeterRegistry registry;
  private String ownerName;
  private StatisticsFactory factory;

  @Before
  public void setup() {
    ownerName = getClass().getSimpleName();

    statisticsType = mock(StatisticsType.class);
    factory = mock(StatisticsFactory.class);
    statistics = mock(Statistics.class);

    when(factory.createType(any(), any(), any()))
        .thenReturn(statisticsType);
    when(factory.createAtomicStatistics(any(), any()))
        .thenReturn(statistics);

    registry = new CompositeMeterRegistry();

  }

  @Test
  public void incEventsReceived_incrementsTheEventsReceivedStat() {
    int eventsReceivedStatId = 33;

    when(statisticsType.nameToId(EVENTS_RECEIVED_STAT_NAME))
        .thenReturn(eventsReceivedStatId);

    GatewayReceiverStats gatewayReceiverStats =
        GatewayReceiverStats.createGatewayReceiverStats(factory, ownerName, registry);

    int delta = 99;

    gatewayReceiverStats.incEventsReceived(delta);

    verify(statistics).incInt(eventsReceivedStatId, delta);

    gatewayReceiverStats.close();
  }

  @Test
  public void eventsReceivedMeter_getsValueFromEventsReceivedStat() {
    int eventsReceivedId = 543;
    when(statisticsType.nameToId(EVENTS_RECEIVED_STAT_NAME))
        .thenReturn(eventsReceivedId);

    int statValue = 22;
    when(statistics.getInt(eventsReceivedId))
        .thenReturn(statValue);

    GatewayReceiverStats gatewayReceiverStats =
        GatewayReceiverStats.createGatewayReceiverStats(factory, ownerName, registry);

    FunctionCounter eventsReceivedCounter = registry
        .find(EVENTS_RECEIVED_COUNTER_NAME)
        .functionCounter();

    assertThat(eventsReceivedCounter)
        .as("events received counter")
        .isNotNull();

    assertThat(eventsReceivedCounter.count())
        .as("events received count")
        .isEqualTo(statValue);

    gatewayReceiverStats.close();
  }

  @Test
  public void eventsReceivedMeter_descriptionMatchesEventsReceivedStat() {
    GatewayReceiverStats gatewayReceiverStats =
        GatewayReceiverStats.createGatewayReceiverStats(factory, ownerName, registry);

    ArgumentCaptor<String> descriptionCaptor = ArgumentCaptor.forClass(String.class);
    verify(factory)
        .createIntCounter(eq(EVENTS_RECEIVED_STAT_NAME), descriptionCaptor.capture(), any());

    assertThat(meterNamed(EVENTS_RECEIVED_COUNTER_NAME))
        .as("events received counter")
        .isNotNull();

    assertThat(meterNamed(EVENTS_RECEIVED_COUNTER_NAME).getId().getDescription())
        .as("meter description")
        .isEqualTo(descriptionCaptor.getValue());

    gatewayReceiverStats.close();
  }

  @Test
  public void eventsReceivedMeter_unitsMatchesEventsReceivedStat() {
    GatewayReceiverStats gatewayReceiverStats =
        GatewayReceiverStats.createGatewayReceiverStats(factory, ownerName, registry);

    ArgumentCaptor<String> unitsCaptor = ArgumentCaptor.forClass(String.class);
    verify(factory).createIntCounter(eq(EVENTS_RECEIVED_STAT_NAME), any(), unitsCaptor.capture());

    assertThat(meterNamed(EVENTS_RECEIVED_COUNTER_NAME))
        .as("events received counter")
        .isNotNull();

    assertThat(meterNamed(EVENTS_RECEIVED_COUNTER_NAME).getId().getBaseUnit())
        .as("meter base unit")
        .isEqualTo(unitsCaptor.getValue());

    gatewayReceiverStats.close();
  }

  @Test
  public void close_removesItsOwnMetersFromTheRegistry() {
    int eventsReceivedId = 543;
    when(statisticsType.nameToId(EVENTS_RECEIVED_STAT_NAME))
        .thenReturn(eventsReceivedId);

    GatewayReceiverStats gatewayReceiverStats =
        GatewayReceiverStats.createGatewayReceiverStats(factory, ownerName, registry);

    assertThat(meterNamed(EVENTS_RECEIVED_COUNTER_NAME))
        .as("events received counter before closing the stats")
        .isNotNull();

    gatewayReceiverStats.close();

    assertThat(meterNamed(EVENTS_RECEIVED_COUNTER_NAME))
        .as("events received counter after closing the stats")
        .isNull();
  }

  @Test
  public void close_doesNotRemoveMetersItDoesNotOwn() {
    int eventsReceivedId = 543;
    when(statisticsType.nameToId(EVENTS_RECEIVED_STAT_NAME))
        .thenReturn(eventsReceivedId);

    GatewayReceiverStats gatewayReceiverStats =
        GatewayReceiverStats.createGatewayReceiverStats(factory, ownerName, registry);

    String foreignMeterName = "some.meter.not.created.by.the.gateway.receiver.stats";

    Timer.builder(foreignMeterName)
        .register(registry);

    gatewayReceiverStats.close();

    assertThat(meterNamed(foreignMeterName))
        .as("foreign meter after closing the stats")
        .isNotNull();
  }

  private Meter meterNamed(String meterName) {
    return registry
        .find(meterName)
        .meter();
  }
}
