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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
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

import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.metrics.ObservationPublishingService;

public class InternalDistributedSystemObservationServiceTest {
  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private Logger logger;

  @Mock
  private CollectingServiceLoader<ObservationPublishingService> publishingServiceLoader;

  @Mock
  private ObservationService.Builder observationServiceBuilder;

  private final ObservationRegistry observationRegistry = ObservationRegistry.create();

  @Test
  public void remembersObservationRegistry() {
    ObservationService observationService =
        new InternalDistributedSystemObservationService(observationServiceBuilder, logger,
            publishingServiceLoader, observationRegistry);

    assertThat(observationService.getObservationRegistry())
        .isSameAs(observationRegistry);
  }

  @Test
  public void remembersObservationServiceBuilder() {
    ObservationService.Builder builder = mock(ObservationService.Builder.class);

    ObservationService observationService =
        new InternalDistributedSystemObservationService(builder, logger, publishingServiceLoader,
            observationRegistry);

    assertThat(observationService.getRebuilder())
        .isSameAs(builder);
  }

  @Test
  public void start_loadsAndStartsObservationPublishingServices() {
    ObservationPublishingService publishingService = mock(ObservationPublishingService.class);
    when(publishingServiceLoader.loadServices(ObservationPublishingService.class))
        .thenReturn(singletonList(publishingService));

    ObservationService observationService =
        new InternalDistributedSystemObservationService(observationServiceBuilder, logger,
            publishingServiceLoader, observationRegistry);

    observationService.start();

    verify(publishingService).start(same(observationService));
  }

  @Test
  public void start_logsAndContinues_ifPublishingServiceThrows() {
    ObservationPublishingService publishingService = mock(ObservationPublishingService.class);
    RuntimeException failure = new RuntimeException("start failure");
    when(publishingServiceLoader.loadServices(ObservationPublishingService.class))
        .thenReturn(singletonList(publishingService));
    ObservationService observationService =
        new InternalDistributedSystemObservationService(observationServiceBuilder, logger,
            publishingServiceLoader, observationRegistry);

    doThrow(failure).when(publishingService).start(same(observationService));
    observationService.start();

    verify(logger).error(contains("Exception while starting observation publishing service"),
        same(failure));
  }

  @Test
  public void stop_stopsObservationPublishingServices() {
    ObservationPublishingService publishingService = mock(ObservationPublishingService.class);
    when(publishingServiceLoader.loadServices(ObservationPublishingService.class))
        .thenReturn(singletonList(publishingService));

    ObservationService observationService =
        new InternalDistributedSystemObservationService(observationServiceBuilder, logger,
            publishingServiceLoader, observationRegistry);

    observationService.start();
    observationService.stop();

    verify(publishingService).stop(same(observationService));
  }

  @Test
  public void stop_logsAndContinues_ifPublishingServiceThrows() {
    ObservationPublishingService publishingService = mock(ObservationPublishingService.class);
    RuntimeException failure = new RuntimeException("stop failure");
    when(publishingServiceLoader.loadServices(ObservationPublishingService.class))
        .thenReturn(singletonList(publishingService));

    ObservationService observationService =
        new InternalDistributedSystemObservationService(observationServiceBuilder, logger,
            publishingServiceLoader, observationRegistry);

    observationService.start();
    doThrow(failure).when(publishingService).stop(same(observationService));
    observationService.stop();

    verify(logger).error(contains("Exception while stopping observation publishing service"),
        same(failure));
  }
}
