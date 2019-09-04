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

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.geode.internal.util.ArrayUtils.asList;
import static org.apache.geode.test.micrometer.MicrometerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCacheBuilder;
import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.metrics.MetricsPublishingService;

public class CompositeCacheMetricsSessionTest {
  private static final String[] COMMON_TAG_KEYS = {"cluster", "member", "host"};
  private static final Supplier<Set<MeterBinder>> NO_BINDERS = Collections::emptySet;
  private static final Supplier<CompositeMeterRegistry> REGISTRY_SUPPLIER =
      CompositeMeterRegistry::new;
  private static final Set<MeterRegistry> NO_USER_REGISTRIES = emptySet();
  private static final CollectingServiceLoader<MetricsPublishingService> NO_SERVICES =
      serviceClass -> emptySet();
  private Function<Thread.State, String> TO_THREAD_STATE_TAG_VALUE =
      state -> state.name().toLowerCase().replace("_", "-");

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  InternalDistributedSystem system;

  @Mock
  private Logger logger;

  private CacheMetricsSession metricsSession;

  @After
  public void tearDownMetricsSession() {
    if (metricsSession != null) {
      metricsSession.stop();
    }
  }

  @Test
  public void beforeStart_meterRegistry_returnsNull() {
    metricsSession = new CompositeCacheMetricsSession(NO_USER_REGISTRIES);

    assertThat(metricsSession.meterRegistry())
        .isNull();
  }

  @Test
  public void afterStart_meterRegistry_returnsCompositeRegistry() {
    metricsSession = new CompositeCacheMetricsSession(NO_USER_REGISTRIES);

    metricsSession.start(system);

    assertThat(metricsSession.meterRegistry())
        .isInstanceOf(CompositeMeterRegistry.class);
  }

  @Test
  public void start_addsMemberNameCommonTagToCompositeRegistry() {
    metricsSession = new CompositeCacheMetricsSession(NO_USER_REGISTRIES);
    String theMemberName = "the-member-name";
    when(system.getName()).thenReturn(theMemberName);

    metricsSession.start(system);

    Meter meter = metricsSession.meterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasTag("member", theMemberName);
  }

  @Test
  public void start_addsClusterIdCommonTagToCompositeRegistry() {
    metricsSession = new CompositeCacheMetricsSession(NO_USER_REGISTRIES);
    int theSystemId = 21;
    when(system.getConfig().getDistributedSystemId()).thenReturn(theSystemId);

    metricsSession.start(system);

    Meter meter = metricsSession.meterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasTag("cluster", String.valueOf(theSystemId));
  }

  @Test
  public void start_addsHostNameCommonTagToCompositeRegistry() {
    metricsSession = new CompositeCacheMetricsSession(NO_USER_REGISTRIES);

    String theHostName = "the-host-name";
    when(system.getDistributedMember().getHost())
        .thenReturn(theHostName);

    metricsSession.start(system);

    Meter meter = metricsSession.meterRegistry()
        .counter("my.meter");

    assertThat(meter)
        .hasTag("host", theHostName);
  }

  @Test
  public void start_addsJvmMemoryMetersToCompositeRegistry() {
    metricsSession = new CompositeCacheMetricsSession(NO_USER_REGISTRIES);

    metricsSession.start(system);

    MeterRegistry registry = metricsSession.meterRegistry();
    assertThatMeterExistsWithCommonTags(registry, Gauge.class, "jvm.buffer.count");
    assertThatMeterExistsWithCommonTags(registry, Gauge.class, "jvm.buffer.memory.used");
    assertThatMeterExistsWithCommonTags(registry, Gauge.class, "jvm.buffer.total.capacity");
    assertThatMeterExistsWithCommonTags(registry, Gauge.class, "jvm.memory.used");
    assertThatMeterExistsWithCommonTags(registry, Gauge.class, "jvm.memory.committed");
    assertThatMeterExistsWithCommonTags(registry, Gauge.class, "jvm.memory.max");
  }

  @Test
  public void start_addsJvmThreadMetersToCompositeRegistry() {
    metricsSession = new CompositeCacheMetricsSession(NO_USER_REGISTRIES);

    metricsSession.start(system);

    MeterRegistry registry = metricsSession.meterRegistry();
    assertThatMeterExistsWithCommonTags(registry, Gauge.class, "jvm.threads.peak");
    assertThatMeterExistsWithCommonTags(registry, Gauge.class, "jvm.threads.daemon");
    assertThatMeterExistsWithCommonTags(registry, Gauge.class, "jvm.threads.live");
    asList(Thread.State.values()).stream()
        .map(TO_THREAD_STATE_TAG_VALUE)
        .forEach(threadStateTagValue -> assertThatMeterExistsWithCommonTags(registry, Gauge.class,
            "jvm.threads.states", Tag.of("state", threadStateTagValue)));
  }

  @Test
  public void start_addsProcessUptimeMetersToCompositeRegistry() {
    metricsSession = new CompositeCacheMetricsSession(NO_USER_REGISTRIES);

    metricsSession.start(system);

    MeterRegistry registry = metricsSession.meterRegistry();
    assertThatMeterExistsWithCommonTags(registry, TimeGauge.class, "process.uptime");
    assertThatMeterExistsWithCommonTags(registry, TimeGauge.class, "process.start.time");
  }

  @Test
  public void start_addsSystemCpuMetersToCompositeRegistry() {
    metricsSession = new CompositeCacheMetricsSession(NO_USER_REGISTRIES);

    metricsSession.start(system);

    MeterRegistry registry = metricsSession.meterRegistry();
    assertThatMeterExistsWithCommonTags(registry, Gauge.class, "system.cpu.count");
  }

  @Test
  public void start_addsUserRegistriesToComposite() {
    Set<MeterRegistry> userRegistries = setOf(MeterRegistry.class, 3);

    CompositeMeterRegistry composite = new CompositeMeterRegistry();

    metricsSession = new CompositeCacheMetricsSession(logger, supplierOf(composite), NO_SERVICES,
        NO_BINDERS, userRegistries);

    metricsSession.start(system);

    assertThat(composite.getRegistries())
        .containsExactlyInAnyOrderElementsOf(userRegistries);
  }

  @Test
  public void start_addsSessionToSystem() {
    metricsSession = new CompositeCacheMetricsSession(emptySet());

    metricsSession.start(system);

    verify(system).setMetricsSession(same(metricsSession));
  }

  @Test
  public void start_startsEachPublishingServiceLoadedByLoader() {
    Collection<MetricsPublishingService> publishingServices =
        setOf(MetricsPublishingService.class, 4);

    @SuppressWarnings("unchecked")
    CollectingServiceLoader<MetricsPublishingService> serviceLoader =
        mock(CollectingServiceLoader.class);

    when(serviceLoader.loadServices(MetricsPublishingService.class))
        .thenReturn(publishingServices);

    metricsSession = new CompositeCacheMetricsSession(logger, REGISTRY_SUPPLIER, serviceLoader,
        NO_BINDERS, NO_USER_REGISTRIES);

    metricsSession.start(system);

    publishingServices.forEach(
        publishingService -> verify(publishingService, times(1)).start(same(metricsSession)));
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

    metricsSession = new CompositeCacheMetricsSession(logger, REGISTRY_SUPPLIER, serviceLoader,
        NO_BINDERS, NO_USER_REGISTRIES);

    metricsSession.start(system);

    ArgumentCaptor<String> actualMessage = ArgumentCaptor.forClass(String.class);
    verify(logger).error(actualMessage.capture(), same(thrownDuringStart));
    assertThat(actualMessage.getValue())
        .as("Error log message")
        .contains(serviceClassName);
  }

  @Test
  public void addSubregistry_addsRegistryToComposite() {
    CompositeMeterRegistry composite = new CompositeMeterRegistry();

    metricsSession = new CompositeCacheMetricsSession(logger, supplierOf(composite), NO_SERVICES,
        NO_BINDERS, NO_USER_REGISTRIES);

    metricsSession.start(system);

    MeterRegistry userRegistry = mock(MeterRegistry.class);

    metricsSession.addSubregistry(userRegistry);

    assertThat(composite.getRegistries())
        .contains(userRegistry);
  }

  @Test
  public void removeSubregistry_removesRegistryFromComposite() {
    CompositeMeterRegistry composite = new CompositeMeterRegistry();

    metricsSession = new CompositeCacheMetricsSession(logger, supplierOf(composite), NO_SERVICES,
        NO_BINDERS, NO_USER_REGISTRIES);

    metricsSession.start(system);

    MeterRegistry userRegistry = mock(MeterRegistry.class);

    metricsSession.addSubregistry(userRegistry);

    metricsSession.removeSubregistry(userRegistry);

    assertThat(composite.getRegistries())
        .doesNotContain(userRegistry);
  }

  @Test
  public void prepareBuilder_addsUserRegistriesToBuilder() {
    Set<MeterRegistry> userRegistries = setOf(MeterRegistry.class, 3);

    metricsSession = new CompositeCacheMetricsSession(userRegistries);

    InternalCacheBuilder cacheBuilder = mock(InternalCacheBuilder.class);
    metricsSession.prepareToReconstruct(cacheBuilder);

    userRegistries.forEach(userRegistry -> verify(cacheBuilder, times(1))
        .addMeterSubregistry(same(userRegistry)));
  }

  @Test
  public void stop_closesAllAutoClosableBinders() {
    Set<MeterBinder> autoCloseableBinders = setOf(AutoCloseableMeterBinder.class, 4);
    Set<MeterBinder> nonAutoCloseableBinders = setOf(MeterBinder.class, 4);

    Set<MeterBinder> allBinders = new HashSet<>();
    allBinders.addAll(autoCloseableBinders);
    allBinders.addAll(nonAutoCloseableBinders);

    metricsSession = new CompositeCacheMetricsSession(logger, REGISTRY_SUPPLIER, NO_SERVICES,
        supplierOf(allBinders), NO_USER_REGISTRIES);
    metricsSession.start(system);

    metricsSession.stop();

    autoCloseableBinders.forEach(this::assertClosed);
  }

  @Test
  public void stop_logsException_ifBinderCloseThrows() throws Exception {
    AutoCloseableMeterBinder throwingBinder =
        mock(AutoCloseableMeterBinder.class, "throwing-binder");

    Exception thrownByBinder = new Exception("thrown by resource.close()");
    doThrow(thrownByBinder).when(throwingBinder).close();

    metricsSession = new CompositeCacheMetricsSession(logger, REGISTRY_SUPPLIER, NO_SERVICES,
        supplierOf(singleton(throwingBinder)), NO_USER_REGISTRIES);

    metricsSession.start(system);
    metricsSession.stop(); // binder will throw

    verify(logger).warn(any(String.class), same(thrownByBinder));
  }

  @Test
  public void stop_stopsEachPublishingService() {
    Collection<MetricsPublishingService> publishingServices =
        setOf(MetricsPublishingService.class, 4);

    @SuppressWarnings("unchecked")
    CollectingServiceLoader<MetricsPublishingService> serviceLoader =
        mock(CollectingServiceLoader.class);

    when(serviceLoader.loadServices(MetricsPublishingService.class))
        .thenReturn(publishingServices);

    metricsSession = new CompositeCacheMetricsSession(logger, REGISTRY_SUPPLIER, serviceLoader,
        NO_BINDERS, NO_USER_REGISTRIES);

    metricsSession.start(system);

    metricsSession.stop();

    publishingServices.forEach(publishingService -> verify(publishingService, times(1)).stop());
  }

  @Test
  public void stop_doesNotCloseUserRegistries() {
    Set<MeterRegistry> userRegistries = setOf(MeterRegistry.class, 3);
    metricsSession = new CompositeCacheMetricsSession(userRegistries);
    metricsSession.start(system);

    metricsSession.stop();

    userRegistries.forEach(userRegistry -> verify(userRegistry, never()).close());
  }

  @Test
  public void stop_doesNotCloseAddedSubRegistries() {
    metricsSession = new CompositeCacheMetricsSession(NO_USER_REGISTRIES);
    metricsSession.start(system);

    Set<MeterRegistry> addedSubRegistries = setOf(MeterRegistry.class, 3);
    addedSubRegistries.forEach(metricsSession::addSubregistry);

    metricsSession.stop();

    addedSubRegistries.forEach(addedRegistry -> verify(addedRegistry, never()).close());
  }

  @Test
  public void stop_closesCompositeRegistry() {
    CompositeMeterRegistry composite = new CompositeMeterRegistry();

    metricsSession = new CompositeCacheMetricsSession(logger, supplierOf(composite), NO_SERVICES,
        NO_BINDERS, NO_USER_REGISTRIES);

    metricsSession.start(system);
    metricsSession.stop();

    assertThat(composite.isClosed())
        .as("Composite registry is closed")
        .isTrue();
  }

  @Test
  public void stop_logsError_ifMetricsPublishingServiceStopThrows() {
    MetricsPublishingService throwingService = mock(MetricsPublishingService.class);
    RuntimeException thrownDuringStop =
        new RuntimeException("thrown by service.stop() during test");
    doThrow(thrownDuringStop).when(throwingService).stop();
    String serviceClassName = throwingService.getClass().getName();

    @SuppressWarnings("unchecked")
    CollectingServiceLoader<MetricsPublishingService> serviceLoader =
        mock(CollectingServiceLoader.class);

    when(serviceLoader.loadServices(MetricsPublishingService.class))
        .thenReturn(singleton(throwingService));

    metricsSession = new CompositeCacheMetricsSession(logger, REGISTRY_SUPPLIER, serviceLoader,
        NO_BINDERS, NO_USER_REGISTRIES);

    metricsSession.start(system);

    metricsSession.stop();

    ArgumentCaptor<String> actualMessage = ArgumentCaptor.forClass(String.class);
    verify(logger).error(actualMessage.capture(), same(thrownDuringStop));
    assertThat(actualMessage.getValue())
        .as("Error log message")
        .contains(serviceClassName);
  }

  @Test
  public void afterStop_meterRegistry_returnsNull() {
    metricsSession = new CompositeCacheMetricsSession(NO_USER_REGISTRIES);

    metricsSession.start(system);
    metricsSession.stop();

    assertThat(metricsSession.meterRegistry())
        .isNull();
  }

  private <T extends Meter> void assertThatMeterExistsWithCommonTags(
      MeterRegistry registry, Class<T> type, String name,
      Tag... customTags) {
    Collection<Meter> meters = registry
        .find(name)
        .tags(asList(customTags))
        .meters();

    assertThat(meters).isNotEmpty();
    assertThat(meters)
        .allMatch(type::isInstance, "instance of " + type);

    meters.forEach(this::assertThatHasCommonTags);
  }

  private void assertThatHasCommonTags(Meter meter) {
    List<String> keys = meter.getId().getTags().stream().map(Tag::getKey).collect(toList());

    assertThat(keys)
        .as("Tags for meter %s", meter.getId().getName())
        .contains(COMMON_TAG_KEYS);
  }

  private interface AutoCloseableMeterBinder extends MeterBinder, AutoCloseable {
  }

  private void assertClosed(MeterBinder autoCloseableBinder) {
    try {
      verify((AutoCloseable) autoCloseableBinder).close();
    } catch (Exception ignore) {
      // Ignored because mock will not throw
    }
  }

  private static <T> Supplier<T> supplierOf(T instance) {
    return () -> instance;
  }

  private static <T> Set<T> setOf(Class<? extends T> type, int n) {
    return IntStream.range(0, n)
        .mapToObj(
            i -> withSettings().name(type.getSimpleName() + i).defaultAnswer(RETURNS_DEEP_STUBS))
        .map(settings -> mock(type, settings))
        .collect(toSet());
  }
}
