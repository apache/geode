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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.geode.test.micrometer.MicrometerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.quality.Strictness.LENIENT;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.metrics.MetricsPublishingService;

public class InternalDistributedSystemMetricsServiceTest {
  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(LENIENT);

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  InternalDistributedSystem system;

  @Mock
  private CloseableMeterBinder meterBinder;

  @Mock
  private Logger logger;

  @Mock
  private CollectingServiceLoader<MetricsPublishingService> publishingServiceLoader;

  @Mock
  private MetricsService.Builder metricsServiceBuilder;

  private final CompositeMeterRegistry metricsServiceMeterRegistry = new CompositeMeterRegistry();

  private MetricsService metricsService;


  @Before
  public void configureDefaultSystem() {
    when(system.getDistributedMember().getHost()).thenReturn("some-host-name");
    when(system.getName()).thenReturn("some-system-name");
    when(system.getConfig().getDistributedSystemId()).thenReturn(-998);
  }

  @Test
  public void remembersMetricsServiceMeterRegistry() {
    CompositeMeterRegistry theMetricsServiceMeterRegistry = new CompositeMeterRegistry();

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, theMetricsServiceMeterRegistry, emptyList(), meterBinder,
            system, false, false, true);

    assertThat(metricsService.getMeterRegistry())
        .isSameAs(theMetricsServiceMeterRegistry);
  }

  @Test
  public void remembersMetricsServiceBuilder() {
    MetricsService.Builder theMetricsServiceBuilder = mock(MetricsService.Builder.class);

    metricsService =
        new InternalDistributedSystemMetricsService(theMetricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder,
            system, false, false, true);

    assertThat(metricsService.getRebuilder())
        .isSameAs(theMetricsServiceBuilder);
  }

  @Test
  public void throwsNullPointerException_ifSystemNameIsNull() {
    when(system.getName()).thenReturn(null);

    assertThatThrownBy(() -> metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder,
            system, false, false, true)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void throwsIllegalArgumentException_ifMemberHostNameIsEmpty() {
    when(system.getDistributedMember().getHost()).thenReturn("");

    assertThatThrownBy(
        () -> metricsService =
            new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
                publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder,
                system, false, false, true)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void throwsNullPointerException_ifMemberHostNameIsNull() {
    when(system.getDistributedMember().getHost()).thenReturn(null);

    assertThatThrownBy(() -> metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder,
            system, false, false, true)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void meterRegistry_registerMeter_addsMemberTagWithSystemName_ifSystemNameIsNotEmpty() {
    String theSystemName = "non-empty-system-name";
    when(system.getName()).thenReturn(theSystemName);

    MetricsService metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder, system,
            false, false, true);

    Meter meter = metricsService.getMeterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member", theSystemName);
  }

  @Test
  public void meterRegistry_registerMeter_addsNoMemberTag_ifSystemNameIsEmpty() {
    when(system.getName()).thenReturn("");

    MetricsService metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder, system,
            false, false, true);

    Meter meter = metricsService.getMeterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasNoTag("member");
  }

  @Test
  public void meterRegistry_registerMeter_addsHostTagWithMemberHostName_ifHostNameIsNotEmpty() {
    String theHostName = "non-empty-host-name";
    when(system.getDistributedMember().getHost())
        .thenReturn(theHostName);

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder, system,
            false, false, true);

    Meter meter = metricsService.getMeterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasTag("host", theHostName);
  }

  @Test
  public void meterRegistry_registerMeter_addsClusterTagWithSystemId_ifIsNotClient() {
    int theSystemId = 21;
    when(system.getConfig().getDistributedSystemId())
        .thenReturn(theSystemId);

    MetricsService metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder, system,
            false, false, true);

    Meter meter = metricsService.getMeterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasTag("cluster", String.valueOf(theSystemId));
  }

  @Test
  public void meterRegistry_registerMeter_addsNoClusterTag_ifIsClient() {
    when(system.getConfig().getDistributedSystemId())
        .thenReturn(312);

    MetricsService metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder, system,
            true, false, true);

    Meter meter = metricsService.getMeterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasNoTag("cluster");
  }

  @Test
  public void meterRegistry_registerMeter_addsLocatorMemberTypeTag_ifHasLocatorAndHasNoCacheServer() {
    boolean hasCacheServer = false;
    boolean hasLocator = true;


    MetricsService metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder, system,
            true, hasLocator, hasCacheServer);

    Meter meter = metricsService.getMeterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "locator");
  }

  @Test
  public void meterRegistry_registerMeter_addsServerMemberTypeTag_ifHasCacheServerAndHasNoLocator() {
    boolean hasCacheServer = true;
    boolean hasLocator = false;

    MetricsService metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder, system,
            true, hasLocator, hasCacheServer);

    Meter meter = metricsService.getMeterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "server");
  }

  @Test
  public void meterRegistry_registerMeter_addsEmbeddedCacheMemberTypeTag_ifHasNoCacheServerAndHasNoLocator() {
    boolean hasCacheServer = false;
    boolean hasLocator = false;

    MetricsService metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder, system,
            true, hasLocator, hasCacheServer);
    Meter meter = metricsService.getMeterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "embedded-cache");
  }

  @Test
  public void meterRegistry_registerMeter_addsServerLocatorMemberTypeTag_ifHasLocatorAndHasCacheServer() {
    boolean hasLocator = true;
    boolean hasCacheServer = true;

    MetricsService metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder, system,
            true, hasLocator, hasCacheServer);
    Meter meter = metricsService.getMeterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member.type", "server-locator");
  }

  @Test
  public void start_addsPersistentMeterRegistriesToMetricsServiceMeterRegistry() {
    Set<MeterRegistry> thePersistentMeterRegistries = setOf(3, MeterRegistry.class);

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, thePersistentMeterRegistries,
            meterBinder, system, false, false, true);

    metricsService.start();

    assertThat(metricsServiceMeterRegistry.getRegistries())
        .hasSameElementsAs(thePersistentMeterRegistries);
  }

  @Test
  public void start_bindsMeterBinderToMetricsServiceMeterRegistry() {
    CompositeMeterRegistry theMetricsServiceMeterRegistry = new CompositeMeterRegistry();

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, theMetricsServiceMeterRegistry, emptyList(), meterBinder,
            system, false, false, true);

    metricsService.start();

    verify(meterBinder).bindTo(same(theMetricsServiceMeterRegistry));
  }

  @Test
  public void start_startsEachPublishingServiceLoadedByLoader() {
    Collection<MetricsPublishingService> publishingServices =
        setOf(4, MetricsPublishingService.class);

    @SuppressWarnings("unchecked")
    CollectingServiceLoader<MetricsPublishingService> serviceLoader =
        mock(CollectingServiceLoader.class);

    when(serviceLoader.loadServices(MetricsPublishingService.class))
        .thenReturn(publishingServices);

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger, serviceLoader,
            metricsServiceMeterRegistry, emptyList(), meterBinder, system, false, false, true);

    metricsService.start();

    publishingServices.forEach(
        publishingService -> verify(publishingService, times(1)).start(same(metricsService)));
  }

  @Test
  public void start_logsError_ifMetricsPublishingServiceStartThrows() {
    MetricsPublishingService throwingService = mock(MetricsPublishingService.class);
    RuntimeException thrownDuringStart =
        new RuntimeException("thrown by service.start() during test");
    doThrow(thrownDuringStart).when(throwingService).start(any());
    String serviceClassName = throwingService.getClass().getName();

    @SuppressWarnings("unchecked")
    CollectingServiceLoader<MetricsPublishingService> serviceLoader =
        mock(CollectingServiceLoader.class);

    when(serviceLoader.loadServices(MetricsPublishingService.class))
        .thenReturn(singleton(throwingService));

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger, serviceLoader,
            metricsServiceMeterRegistry, emptyList(), meterBinder, system, false, false, true);

    metricsService.start();

    ArgumentCaptor<String> actualMessage = ArgumentCaptor.forClass(String.class);
    verify(logger).error(actualMessage.capture(), same(thrownDuringStart));
    assertThat(actualMessage.getValue())
        .as("Error log message")
        .contains(serviceClassName);
  }

  @Test
  public void removeSubregistry_removesGivenRegistryFromMetricsServiceMeterRegistry() {
    CompositeMeterRegistry theMetricsServiceMeterRegistry = new CompositeMeterRegistry();

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, theMetricsServiceMeterRegistry, emptyList(), meterBinder,
            system, false, false, true);

    metricsService.start();

    MeterRegistry PersistentMeterRegistry = new SimpleMeterRegistry();

    metricsService.addSubregistry(PersistentMeterRegistry);

    metricsService.removeSubregistry(PersistentMeterRegistry);

    assertThat(theMetricsServiceMeterRegistry.getRegistries())
        .doesNotContain(PersistentMeterRegistry);
  }

  @Test
  public void registersCorrespondingMetersWithEachPersistentMeterRegistry() {
    SimpleMeterRegistry persistentMeterRegistry1 = new SimpleMeterRegistry();
    SimpleMeterRegistry persistentMeterRegistry2 = new SimpleMeterRegistry();
    SimpleMeterRegistry persistentMeterRegistry3 = new SimpleMeterRegistry();
    Set<MeterRegistry> persistentMeterRegistries = new HashSet<>();

    persistentMeterRegistries.add(persistentMeterRegistry1);
    persistentMeterRegistries.add(persistentMeterRegistry2);
    persistentMeterRegistries.add(persistentMeterRegistry3);

    StandardMeterBinder binderThatAddsManyMeters = new StandardMeterBinder();

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, persistentMeterRegistries,
            binderThatAddsManyMeters, system, false, false, true);

    metricsService.start();

    MeterRegistry metricsServiceMeterRegistry = metricsService.getMeterRegistry();

    metricsServiceMeterRegistry.counter("my.new.meter");

    List<Meter> expectedMeters = metricsServiceMeterRegistry.getMeters();

    assertHasMeters("persistent registry 1", persistentMeterRegistry1, expectedMeters);
    assertHasMeters("persistent registry 2", persistentMeterRegistry2, expectedMeters);
    assertHasMeters("persistent registry 3", persistentMeterRegistry3, expectedMeters);
  }

  @Test
  public void registersCorrespondingMetersWithEachSessionMeterRegistry() {

    StandardMeterBinder binderThatAddsManyMeters = new StandardMeterBinder();

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptySet(),
            binderThatAddsManyMeters, system, false, false, true);

    metricsService.start();

    SimpleMeterRegistry sessionMeterRegistry1 = new SimpleMeterRegistry();
    SimpleMeterRegistry sessionMeterRegistry2 = new SimpleMeterRegistry();
    SimpleMeterRegistry sessionMeterRegistry3 = new SimpleMeterRegistry();

    metricsService.addSubregistry(sessionMeterRegistry1);
    metricsService.addSubregistry(sessionMeterRegistry2);
    metricsService.addSubregistry(sessionMeterRegistry3);

    MeterRegistry metricsServiceMeterRegistry = metricsService.getMeterRegistry();

    metricsServiceMeterRegistry.counter("my.new.meter");

    List<Meter> expectedMeters = metricsServiceMeterRegistry.getMeters();

    assertHasMeters("session registry 1", sessionMeterRegistry1, expectedMeters);
    assertHasMeters("session registry 2", sessionMeterRegistry2, expectedMeters);
    assertHasMeters("session registry 3", sessionMeterRegistry3, expectedMeters);
  }

  @Test
  public void stop_closesMeterBinder() throws Exception {
    CloseableMeterBinder theMeterBinder = mock(CloseableMeterBinder.class);

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), theMeterBinder,
            system, false, false, true);

    metricsService.start();

    metricsService.stop();

    verify(theMeterBinder).close();
  }

  @Test
  public void stop_stopsEachPublishingService() {
    Collection<MetricsPublishingService> publishingServices =
        setOf(4, MetricsPublishingService.class);

    @SuppressWarnings("unchecked")
    CollectingServiceLoader<MetricsPublishingService> serviceLoader =
        mock(CollectingServiceLoader.class);

    when(serviceLoader.loadServices(MetricsPublishingService.class))
        .thenReturn(publishingServices);

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger, serviceLoader,
            metricsServiceMeterRegistry, emptyList(), meterBinder, system, false, false, true);

    metricsService.start();

    metricsService.stop();

    publishingServices
        .forEach(publishingService -> verify(publishingService, times(1)).stop(metricsService));
  }

  @Test
  public void stop_logsError_ifMetricsPublishingServiceStopThrows() {
    MetricsPublishingService throwingService = mock(MetricsPublishingService.class);

    @SuppressWarnings("unchecked")
    CollectingServiceLoader<MetricsPublishingService> serviceLoader =
        mock(CollectingServiceLoader.class);

    when(serviceLoader.loadServices(MetricsPublishingService.class))
        .thenReturn(singleton(throwingService));

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger, serviceLoader,
            metricsServiceMeterRegistry, emptyList(), meterBinder, system, false, false, true);

    metricsService.start();

    RuntimeException thrownDuringStop =
        new RuntimeException("thrown by service.stop() during test");
    doThrow(thrownDuringStop).when(throwingService).stop(metricsService);

    metricsService.stop();

    String serviceClassName = throwingService.getClass().getName();
    ArgumentCaptor<String> actualMessage = ArgumentCaptor.forClass(String.class);
    verify(logger).error(actualMessage.capture(), same(thrownDuringStop));
    assertThat(actualMessage.getValue())
        .as("Error log message")
        .contains(serviceClassName);
  }

  @Test
  public void stop_removesAllMeterRegistriesFromMetricsServiceMeterRegistry() {
    CompositeMeterRegistry theMetricsServiceMeterRegistry = new CompositeMeterRegistry();

    Set<MeterRegistry> persistentMeterRegistries = setOf(3, MeterRegistry.class);

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, theMetricsServiceMeterRegistry, persistentMeterRegistries,
            meterBinder, system, false, false, true);

    metricsService.start();

    Set<MeterRegistry> sessionMeterRegistries = setOf(3, MeterRegistry.class);
    sessionMeterRegistries.forEach(metricsService::addSubregistry);

    metricsService.stop();

    assertThat(theMetricsServiceMeterRegistry.getRegistries()).isEmpty();
  }

  @Test
  public void stop_doesNotClosePersistentMeterRegistries() {
    Set<MeterRegistry> persistentMeterRegistries = setOf(3, MeterRegistry.class);
    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, persistentMeterRegistries,
            meterBinder, system, false, false, true);

    metricsService.start();

    metricsService.stop();

    persistentMeterRegistries
        .forEach(persistentMeterRegistry -> verify(persistentMeterRegistry, never()).close());
  }

  @Test
  public void stop_doesNotCloseSessionMeterRegistries() {
    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, metricsServiceMeterRegistry, emptyList(), meterBinder, system,
            false, false, true);

    metricsService.start();

    Set<MeterRegistry> sessionMeterRegistries = setOf(3, MeterRegistry.class);
    sessionMeterRegistries.forEach(metricsService::addSubregistry);

    metricsService.stop();

    sessionMeterRegistries.forEach(registry -> verify(registry, never()).close());
  }

  @Test
  public void stop_closesMetricsServiceMeterRegistry() {
    CompositeMeterRegistry theMetricsServiceMeterRegistry = new CompositeMeterRegistry();

    metricsService =
        new InternalDistributedSystemMetricsService(metricsServiceBuilder, logger,
            publishingServiceLoader, theMetricsServiceMeterRegistry, emptyList(), meterBinder,
            system, false, false, true);

    metricsService.start();
    metricsService.stop();

    assertThat(theMetricsServiceMeterRegistry.isClosed())
        .as("Metrics service meter registry is closed")
        .isTrue();
  }

  @After
  public void stopMetricsService() {
    if (metricsService != null) {
      metricsService.stop();
    }
  }

  private static <T> Set<T> setOf(int count, Class<? extends T> type) {
    return IntStream.range(0, count)
        .mapToObj(i -> withSettings().name(type.getSimpleName() + i))
        .map(settings -> mock(type, settings))
        .collect(toSet());
  }

  private static void assertHasMeters(String registryName, MeterRegistry registry,
      List<Meter> expectedMeters) {
    List<Meter.Id> expectedMeterIds = expectedMeters.stream().map(Meter::getId).collect(toList());
    List<Meter.Id> actualMeterIds =
        registry.getMeters().stream().map(Meter::getId).collect(toList());

    assertThat(actualMeterIds)
        .as("IDs of meters in " + registryName)
        .containsAll(expectedMeterIds);
  }
}
