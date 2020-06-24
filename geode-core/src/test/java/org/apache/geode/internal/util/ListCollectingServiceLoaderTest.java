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
package org.apache.geode.internal.util;


import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.metrics.MetricsPublishingService;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.impl.Success;

public class ListCollectingServiceLoaderTest {
  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Test
  public void loadServices_returnsLoadedServices() {
    MetricsPublishingService service1 = mock(MetricsPublishingService.class);
    MetricsPublishingService service2 = mock(MetricsPublishingService.class);
    MetricsPublishingService service3 = mock(MetricsPublishingService.class);
    Set<Object> expectedServices = new HashSet<>(asList(service1, service2, service3));
    ModuleService moduleService = mock(ModuleService.class);

    when(moduleService.loadService(any())).thenReturn(Success.of(expectedServices));

    ListCollectingServiceLoader<MetricsPublishingService> collectingServiceLoader =
        new ListCollectingServiceLoader<>(moduleService);

    Collection<MetricsPublishingService> actualServices =
        collectingServiceLoader.loadServices(MetricsPublishingService.class);

    assertThat(actualServices)
        .containsExactlyInAnyOrder(expectedServices.toArray(new MetricsPublishingService[0]));
  }

  @Test
  public void loadServices_returnsLoadedServices_whenOneServiceThrows() {
    MetricsPublishingService service1 = mock(MetricsPublishingService.class);
    MetricsPublishingService service3 = mock(MetricsPublishingService.class);
    ModuleService moduleService = mock(ModuleService.class);

    Set<Object> services = new HashSet<>();
    services.add(service1);
    services.add(service3);
    when(moduleService.loadService(any())).thenReturn(Success.of(services));

    ListCollectingServiceLoader<MetricsPublishingService> collectingServiceLoader =
        new ListCollectingServiceLoader<>(moduleService);

    Collection<MetricsPublishingService> actualServices =
        collectingServiceLoader.loadServices(MetricsPublishingService.class);

    assertThat(actualServices)
        .containsExactlyInAnyOrder(service1, service3);
  }
}
