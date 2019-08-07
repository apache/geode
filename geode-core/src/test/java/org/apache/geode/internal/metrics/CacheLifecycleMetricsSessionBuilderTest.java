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
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.metrics.CacheLifecycleMetricsSession.Builder;
import org.apache.geode.internal.metrics.CacheLifecycleMetricsSession.CacheLifecycle;
import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.metrics.MetricsPublishingService;

public class CacheLifecycleMetricsSessionBuilderTest {

  private CompositeMeterRegistry registry;
  private Builder builder;

  @Before
  public void setUp() {
    CacheLifecycle cacheLifecycle = mock(CacheLifecycle.class);
    registry = new CompositeMeterRegistry();
    builder = CacheLifecycleMetricsSession
        .builder()
        .setCacheLifecycle(cacheLifecycle);
  }

  @Test
  public void buildsCacheMetricsSession() {
    assertThat(builder.build(registry)).isInstanceOf(CacheLifecycleMetricsSession.class);
  }

  @Test
  public void buildsCacheMetricsSession_withGivenMeterRegistry() {
    CompositeMeterRegistry givenRegistry = new CompositeMeterRegistry();

    CacheLifecycleMetricsSession session = builder
        .build(givenRegistry);

    assertThat(session.meterRegistry()).isSameAs(givenRegistry);
  }

  @Test
  public void addsSessionAsCacheLifecycleListener() {
    CacheLifecycle theCacheLifecycle = mock(CacheLifecycle.class);

    CacheLifecycleMetricsSession session = builder
        .setCacheLifecycle(theCacheLifecycle)
        .build(registry);

    verify(theCacheLifecycle).addListener(same(session));
  }

  @Test
  public void loadsMetricsPublishingServices() {
    CollectingServiceLoader<MetricsPublishingService> theMetricsPublishingServicesLoader =
        mock(CollectingServiceLoader.class);

    builder
        .setServiceLoader(theMetricsPublishingServicesLoader)
        .build(registry);

    verify(theMetricsPublishingServicesLoader).loadServices(MetricsPublishingService.class);
  }

  @Test
  public void buildsCacheMetricsSession_withMetricsPublishingServices() {
    CollectingServiceLoader<MetricsPublishingService> theMetricsPublishingServicesLoader =
        mock(CollectingServiceLoader.class);
    Collection<MetricsPublishingService> theMetricsPublishingServices = Arrays.asList(
        metricsPublishingService("metricsPublishingService1"),
        metricsPublishingService("metricsPublishingService2"),
        metricsPublishingService("metricsPublishingService3"));
    when(theMetricsPublishingServicesLoader.loadServices(MetricsPublishingService.class))
        .thenReturn(theMetricsPublishingServices);

    CacheLifecycleMetricsSession session = builder
        .setServiceLoader(theMetricsPublishingServicesLoader)
        .build(registry);

    assertThat(session.metricsPublishingServices())
        .hasSameElementsAs(theMetricsPublishingServices);
  }

  private MetricsPublishingService metricsPublishingService(String name) {
    return mock(MetricsPublishingService.class, name);
  }
}
