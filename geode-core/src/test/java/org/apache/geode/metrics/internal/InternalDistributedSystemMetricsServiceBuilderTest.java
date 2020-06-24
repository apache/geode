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

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Collection;
import java.util.Set;
import java.util.stream.IntStream;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.metrics.MetricsPublishingService;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;

public class InternalDistributedSystemMetricsServiceBuilderTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock(answer = RETURNS_DEEP_STUBS)
  private InternalDistributedSystem system;

  @Mock
  private InternalDistributedSystemMetricsService.Factory metricsServiceFactory;

  private final InternalDistributedSystemMetricsService.Builder serviceBuilder =
      new InternalDistributedSystemMetricsService.Builder();

  @Test
  public void createsInternalDistributedSystemCacheMetricsService() {
    when(system.getConfig().getDistributedSystemId()).thenReturn(-41);
    when(system.getName()).thenReturn("some-system-name");
    when(system.getDistributedMember().getHost()).thenReturn("some-host-name");

    MetricsService metricsService = serviceBuilder.build(system, new ServiceLoaderModuleService(
        LogService.getLogger()));

    try {
      assertThat(metricsService).isInstanceOf(InternalDistributedSystemMetricsService.class);
    } finally {
      metricsService.stop();
    }
  }

  @Test
  public void usesFactoryToCreateSession_ifFactorySet() {
    MetricsService metricsServiceCreatedByFactory = mock(MetricsService.class);

    when(metricsServiceFactory
        .create(any(), any(), any(), any(), any(), any(), any(), anyBoolean(), anyBoolean(),
            anyBoolean())).thenReturn(metricsServiceCreatedByFactory);

    MetricsService metricsService = serviceBuilder
        .setMetricsServiceFactory(metricsServiceFactory)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    assertThat(metricsService)
        .isSameAs(metricsServiceCreatedByFactory);
  }

  @Test
  public void passesItselfToFactory() {
    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(same(serviceBuilder), any(), any(), any(), any(), any(), any(), anyBoolean(),
            anyBoolean(), anyBoolean());
  }

  @Test
  public void passesGivenServiceLoaderToFactory() {
    CollectingServiceLoader<MetricsPublishingService> theServiceLoader =
        mock(CollectingServiceLoader.class);

    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .setServiceLoader(theServiceLoader)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), same(theServiceLoader), any(), any(), any(), any(), anyBoolean(),
            anyBoolean(), anyBoolean());
  }

  @Test
  public void constructsServiceLoader_ifServiceLoaderNotSet() {
    serviceBuilder
        .setMetricsServiceFactory(metricsServiceFactory)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(CollectingServiceLoader.class), any(), any(), any(), any(),
            anyBoolean(), anyBoolean(), anyBoolean());
  }

  @Test
  public void passesGivenCompositeMeterRegistryToFactoryAsMetricsServiceMeterRegistry() {
    CompositeMeterRegistry theMetricsServiceMeterRegistry = new CompositeMeterRegistry();

    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .setCompositeMeterRegistry(theMetricsServiceMeterRegistry)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(), same(theMetricsServiceMeterRegistry), any(), any(), any(),
            anyBoolean(), anyBoolean(), anyBoolean());
  }

  @Test
  public void constructsCompositeMeterRegistry_ifMetricsServiceMeterRegistryNotSet() {
    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(CompositeMeterRegistry.class), any(), any(), any(),
            anyBoolean(), anyBoolean(), anyBoolean());
  }

  @Test
  public void passesIndividuallyAddedClientMeterRegistriesToFactory() {
    Set<MeterRegistry> individuallyAddedClientMeterRegistries = setOf(3, MeterRegistry.class);

    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory);
    individuallyAddedClientMeterRegistries.forEach(serviceBuilder::addPersistentMeterRegistry);
    serviceBuilder.build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Collection<MeterRegistry>> clientMeterRegistriesPassedToFactory =
        ArgumentCaptor.forClass(Collection.class);

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), clientMeterRegistriesPassedToFactory.capture(), any(),
            any(), anyBoolean(), anyBoolean(), anyBoolean());

    assertThat(clientMeterRegistriesPassedToFactory.getValue())
        .containsExactlyInAnyOrderElementsOf(individuallyAddedClientMeterRegistries);
  }

  @Test
  public void passesBulkAddedClientMeterRegistriesToFactory() {
    Set<MeterRegistry> bulkAddedClientMeterRegistries = setOf(4, MeterRegistry.class);

    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .addPersistentMeterRegistries(bulkAddedClientMeterRegistries)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Collection<MeterRegistry>> clientMeterRegistriesPassedToFactory =
        ArgumentCaptor.forClass(Collection.class);

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), clientMeterRegistriesPassedToFactory.capture(), any(),
            any(), anyBoolean(), anyBoolean(), anyBoolean());

    assertThat(clientMeterRegistriesPassedToFactory.getValue())
        .containsExactlyInAnyOrderElementsOf(bulkAddedClientMeterRegistries);
  }

  @Test
  public void passesEmptyClientMeterRegistriesCollectionToFactory_ifNoClientMeterRegistriesAdded() {
    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Collection<MeterRegistry>> clientMeterRegistriesPassedToFactory =
        ArgumentCaptor.forClass(Collection.class);

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), clientMeterRegistriesPassedToFactory.capture(), any(),
            any(), anyBoolean(), anyBoolean(), anyBoolean());

    assertThat(clientMeterRegistriesPassedToFactory.getValue())
        .isEmpty();
  }

  @Test
  public void passesGivenMeterBinderToFactory() {
    CloseableMeterBinder theBinder = mock(CloseableMeterBinder.class);

    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .setBinder(theBinder)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), any(), same(theBinder), any(), anyBoolean(),
            anyBoolean(), anyBoolean());
  }

  @Test
  public void passesStandardMeterBinderToFactory_ifMeterBinderNotSet() {
    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), any(), any(StandardMeterBinder.class), any(),
            anyBoolean(), anyBoolean(), anyBoolean());
  }

  @Test
  public void passesGivenLoggerToFactory() {
    Logger theLogger = mock(Logger.class);

    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .setLogger(theLogger)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), same(theLogger), any(), any(), any(), any(), any(), anyBoolean(),
            anyBoolean(), anyBoolean());
  }

  @Test
  public void usesGivenLocatorDetectorToDetectLocator() {
    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .setLocatorDetector(() -> true)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), any(), any(), any(), anyBoolean(), eq(true),
            anyBoolean());

    serviceBuilder.setLocatorDetector(() -> false)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), any(), any(), any(), anyBoolean(), eq(false),
            anyBoolean());
  }

  @Test
  public void usesGivenCacheServerDetectorToDetectCacheServer() {
    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .setCacheServerDetector(() -> true)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), any(), any(), any(), anyBoolean(), anyBoolean(),
            eq(true));

    serviceBuilder
        .setCacheServerDetector(() -> false)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), any(), any(), any(), anyBoolean(), anyBoolean(),
            eq(false));
  }

  @Test
  public void constructsLogger_ifLoggerNotSet() {
    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(Logger.class), any(), any(), any(), any(), any(), anyBoolean(),
            anyBoolean(), anyBoolean());
  }

  @Test
  public void passesIsClientToFactory() {
    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .setIsClient(true)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), any(), any(), any(), eq(true), anyBoolean(),
            anyBoolean());
  }

  @Test
  public void passesIsNotClientToFactory_ifIsClientNotGiven() {
    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .build(system, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), any(), any(), any(), eq(false), anyBoolean(),
            anyBoolean());
  }

  @Test
  public void passesGivenSystemToFactory() {
    InternalDistributedSystem theSystem = mock(InternalDistributedSystem.class);

    serviceBuilder.setMetricsServiceFactory(metricsServiceFactory)
        .build(theSystem, new ServiceLoaderModuleService(LogService.getLogger()));

    verify(metricsServiceFactory)
        .create(any(), any(), any(), any(), any(), any(), same(theSystem), anyBoolean(),
            anyBoolean(), anyBoolean());
  }

  private static <T> Set<T> setOf(int count, Class<? extends T> type) {
    return IntStream.range(0, count)
        .mapToObj(i -> withSettings().name(type.getSimpleName() + i))
        .map(settings -> mock(type, settings))
        .collect(toSet());
  }
}
