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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.observation.ObservationRegistry;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.metrics.ObservationPublishingService;

public class InternalDistributedSystemObservationServiceBuilderTest {
  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private InternalDistributedSystem system;

  @Mock
  private InternalDistributedSystemObservationService.Factory observationServiceFactory;

  private final InternalDistributedSystemObservationService.Builder serviceBuilder =
      new InternalDistributedSystemObservationService.Builder();

  @Test
  public void createsInternalDistributedSystemObservationService() {
    ObservationService observationService = serviceBuilder.build(system);

    assertThat(observationService)
        .isInstanceOf(InternalDistributedSystemObservationService.class);
  }

  @Test
  public void usesFactoryToCreateSession_ifFactorySet() {
    ObservationService observationServiceCreatedByFactory = mock(ObservationService.class);
    when(observationServiceFactory.create(any(), any(), any(), any()))
        .thenReturn(observationServiceCreatedByFactory);

    ObservationService observationService = serviceBuilder
        .setObservationServiceFactory(observationServiceFactory)
        .build(system);

    assertThat(observationService)
        .isSameAs(observationServiceCreatedByFactory);
  }

  @Test
  public void passesItselfToFactory() {
    serviceBuilder.setObservationServiceFactory(observationServiceFactory)
        .build(system);

    verify(observationServiceFactory)
        .create(same(serviceBuilder), any(), any(), any());
  }

  @Test
  public void passesGivenServiceLoaderToFactory() {
    CollectingServiceLoader<ObservationPublishingService> serviceLoader =
        mock(CollectingServiceLoader.class);

    serviceBuilder.setObservationServiceFactory(observationServiceFactory)
        .setServiceLoader(serviceLoader)
        .build(system);

    verify(observationServiceFactory)
        .create(any(), any(), same(serviceLoader), any());
  }

  @Test
  public void passesGivenObservationRegistryToFactory() {
    ObservationRegistry observationRegistry = ObservationRegistry.create();

    serviceBuilder.setObservationServiceFactory(observationServiceFactory)
        .setObservationRegistry(observationRegistry)
        .build(system);

    verify(observationServiceFactory)
        .create(any(), any(), any(), same(observationRegistry));
  }

  @Test
  public void passesGivenLoggerToFactory() {
    Logger logger = mock(Logger.class);

    serviceBuilder.setObservationServiceFactory(observationServiceFactory)
        .setLogger(logger)
        .build(system);

    verify(observationServiceFactory)
        .create(any(), same(logger), any(), any());
  }
}
