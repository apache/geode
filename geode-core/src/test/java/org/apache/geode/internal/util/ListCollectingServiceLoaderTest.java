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
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.internal.util.ListCollectingServiceLoader.ServiceLoaderWrapper;
import org.apache.geode.metrics.MetricsPublishingService;

public class ListCollectingServiceLoaderTest {
  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private ServiceLoaderWrapper<MetricsPublishingService> serviceLoaderWrapper;

  @Test
  public void loadServices_delegatesLoading() {
    when(serviceLoaderWrapper.iterator()).thenReturn(mock(Iterator.class));

    ListCollectingServiceLoader<MetricsPublishingService> collectingServiceLoader =
        new ListCollectingServiceLoader<>(serviceLoaderWrapper);

    collectingServiceLoader.loadServices(MetricsPublishingService.class);

    InOrder inOrder = inOrder(serviceLoaderWrapper);
    inOrder.verify(serviceLoaderWrapper).load(same(MetricsPublishingService.class));
    inOrder.verify(serviceLoaderWrapper).iterator();
  }

  @Test
  public void loadServices_returnsLoadedServices() {
    MetricsPublishingService service1 = mock(MetricsPublishingService.class);
    MetricsPublishingService service2 = mock(MetricsPublishingService.class);
    MetricsPublishingService service3 = mock(MetricsPublishingService.class);
    List<MetricsPublishingService> expectedServices = asList(service1, service2, service3);

    when(serviceLoaderWrapper.iterator()).thenReturn(expectedServices.iterator());

    ListCollectingServiceLoader<MetricsPublishingService> collectingServiceLoader =
        new ListCollectingServiceLoader<>(serviceLoaderWrapper);

    Collection<MetricsPublishingService> actualServices =
        collectingServiceLoader.loadServices(MetricsPublishingService.class);

    assertThat(actualServices)
        .containsExactlyInAnyOrder(expectedServices.toArray(new MetricsPublishingService[0]));
  }

  @Test
  public void loadServices_returnsLoadedServices_whenOneServiceThrows() {
    MetricsPublishingService service1 = mock(MetricsPublishingService.class);
    MetricsPublishingService service3 = mock(MetricsPublishingService.class);
    Iterator<MetricsPublishingService> iterator = mock(Iterator.class);
    ServiceConfigurationError error = new ServiceConfigurationError("Error message");

    when(iterator.hasNext()).thenReturn(true, true, true, false);
    when(iterator.next()).thenReturn(service1).thenThrow(error).thenReturn(service3);
    when(serviceLoaderWrapper.iterator()).thenReturn(iterator);

    ListCollectingServiceLoader<MetricsPublishingService> collectingServiceLoader =
        new ListCollectingServiceLoader<>(serviceLoaderWrapper);

    Collection<MetricsPublishingService> actualServices =
        collectingServiceLoader.loadServices(MetricsPublishingService.class);

    assertThat(actualServices)
        .containsExactlyInAnyOrder(service1, service3);
  }
}
