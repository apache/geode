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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Test;

import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.metrics.CacheLifecycleMetricsSession.CacheLifecycle;
import org.apache.geode.internal.metrics.CacheLifecycleMetricsSession.ErrorLogger;
import org.apache.geode.metrics.MetricsPublishingService;

public class CacheLifecycleMetricsSessionTest {

  private final CompositeMeterRegistry compositeRegistry = new CompositeMeterRegistry();

  private CacheLifecycleMetricsSession metricsSession;

  @After
  public void tearDown() {
    if (metricsSession != null) {
      GemFireCacheImpl.removeCacheLifecycleListener(metricsSession);
    }
  }

  @Test
  public void startsWithNoDownstreamRegistries() {
    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        Collections.emptyList());

    Set<MeterRegistry> downstreamRegistries = compositeRegistry.getRegistries();

    assertThat(downstreamRegistries)
        .isEmpty();
  }

  @Test
  public void remembersConnectedDownstreamRegistries() {
    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        Collections.emptyList());

    MeterRegistry downstreamRegistry = new SimpleMeterRegistry();

    metricsSession.addSubregistry(downstreamRegistry);

    assertThat(compositeRegistry.getRegistries())
        .contains(downstreamRegistry);
  }

  @Test
  public void forgetsDisconnectedDownstreamRegistries() {
    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        Collections.emptyList());

    MeterRegistry downstreamRegistry = new SimpleMeterRegistry();
    metricsSession.addSubregistry(downstreamRegistry);

    metricsSession.removeSubregistry(downstreamRegistry);

    assertThat(compositeRegistry.getRegistries())
        .doesNotContain(downstreamRegistry);
  }

  @Test
  public void connectsExistingMetersToNewDownstreamRegistries() {
    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        Collections.emptyList());

    String counterName = "the.counter";
    Counter primaryCounter = compositeRegistry.counter(counterName);

    double amountIncrementedBeforeConnectingDownstreamRegistry = 3.0;
    primaryCounter.increment(amountIncrementedBeforeConnectingDownstreamRegistry);

    MeterRegistry downstreamRegistry = new SimpleMeterRegistry();
    metricsSession.addSubregistry(downstreamRegistry);

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
    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        Collections.emptyList());

    MeterRegistry downstreamRegistry = new SimpleMeterRegistry();
    metricsSession.addSubregistry(downstreamRegistry);

    String counterName = "the.counter";
    Counter newCounter = compositeRegistry.counter(counterName);

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

  @Test
  public void cacheCreatedStartsEachMetricsPublishingService() {
    List<MetricsPublishingService> metricsPublishingServices = Arrays.asList(
        metricsPublishingService("metricsPublishingService1"),
        metricsPublishingService("metricsPublishingService2"),
        metricsPublishingService("metricsPublishingService3"));

    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        metricsPublishingServices);

    metricsSession.cacheCreated(mock(InternalCache.class));

    for (MetricsPublishingService metricsPublishingService : metricsPublishingServices) {
      verify(metricsPublishingService).start(same(metricsSession));
    }
  }

  @Test
  public void cacheClosedStopsEachMetricsPublishingService() {
    List<MetricsPublishingService> metricsPublishingServices = Arrays.asList(
        metricsPublishingService("metricsPublishingService1"),
        metricsPublishingService("metricsPublishingService2"),
        metricsPublishingService("metricsPublishingService3"));

    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        metricsPublishingServices);

    metricsSession.cacheClosed(mock(InternalCache.class));

    for (MetricsPublishingService metricsPublishingService : metricsPublishingServices) {
      verify(metricsPublishingService).stop();
    }
  }

  @Test
  public void cacheClosedDisconnectsAllDownstreamRegistries() {
    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        Collections.emptyList());

    MeterRegistry downstreamMeterRegistry1 = new SimpleMeterRegistry();
    MeterRegistry downstreamMeterRegistry2 = new SimpleMeterRegistry();
    MeterRegistry downstreamMeterRegistry3 = new SimpleMeterRegistry();

    metricsSession.addSubregistry(downstreamMeterRegistry1);
    metricsSession.addSubregistry(downstreamMeterRegistry2);
    metricsSession.addSubregistry(downstreamMeterRegistry3);

    metricsSession.cacheClosed(mock(InternalCache.class));

    assertThat(compositeRegistry.getRegistries()).isEmpty();
  }

  @Test
  public void cacheClosedRemovesSessionAsCacheLifecycleListener() {
    CacheLifecycle theCacheLifecycle = mock(CacheLifecycle.class);
    metricsSession =
        new CacheLifecycleMetricsSession(theCacheLifecycle, compositeRegistry,
            Collections.emptyList());

    metricsSession.cacheClosed(mock(InternalCache.class));

    verify(theCacheLifecycle).removeListener(same(metricsSession));
  }

  @Test
  public void cacheClosedClosesCompositeMeterRegistry() {
    CacheLifecycle theCacheLifecycle = mock(CacheLifecycle.class);
    CompositeMeterRegistry theCompositeRegistry = spy(compositeRegistry);
    metricsSession =
        new CacheLifecycleMetricsSession(theCacheLifecycle, theCompositeRegistry,
            Collections.emptyList());

    metricsSession.cacheClosed(mock(InternalCache.class));

    verify(theCompositeRegistry).close();
  }

  @Test
  public void cacheClosedDoesNotCloseSubregistries() {
    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        Collections.emptyList());
    MeterRegistry theSubregistry = spy(new SimpleMeterRegistry());
    metricsSession.addSubregistry(theSubregistry);

    metricsSession.cacheClosed(mock(InternalCache.class));

    assertThat(theSubregistry.isClosed()).isFalse();
  }

  @Test
  public void cacheCreated_logsErrorMessage_ifMetricsPublishingServiceStartThrowsRuntimeException() {
    MetricsPublishingService metricsPublishingService =
        metricsPublishingService("metricsPublishingService");
    String theClassName = metricsPublishingService.getClass().getName();
    RuntimeException theException = new RuntimeException("theExceptionMessage");
    doThrow(theException).when(metricsPublishingService).start(any());
    List<MetricsPublishingService> metricsPublishingServices =
        Collections.singletonList(metricsPublishingService);
    ErrorLogger errorLogger = mock(ErrorLogger.class);
    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        metricsPublishingServices, errorLogger);

    metricsSession.cacheCreated(mock(InternalCache.class));

    verify(errorLogger).logError(anyString(), eq("start"), same(theClassName), same(theException));
  }

  @Test
  public void cacheCreated_logsErrorMessage_ifMetricsPublishingServiceStartThrowsError() {
    MetricsPublishingService metricsPublishingService =
        metricsPublishingService("metricsPublishingService");
    String theClassName = metricsPublishingService.getClass().getName();
    Error theError = new Error("theErrorMessage");
    doThrow(theError).when(metricsPublishingService).start(any());
    List<MetricsPublishingService> metricsPublishingServices =
        Collections.singletonList(metricsPublishingService);
    ErrorLogger errorLogger = mock(ErrorLogger.class);
    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        metricsPublishingServices, errorLogger);

    metricsSession.cacheCreated(mock(InternalCache.class));

    verify(errorLogger).logError(anyString(), eq("start"), same(theClassName), same(theError));
  }

  @Test
  public void cacheClosed_logsErrorMessage_ifMetricsPublishingServiceStopThrowsRuntimeException() {
    MetricsPublishingService metricsPublishingService =
        metricsPublishingService("metricsPublishingService");
    String theClassName = metricsPublishingService.getClass().getName();
    RuntimeException theException = new RuntimeException("theExceptionMessage");
    doThrow(theException).when(metricsPublishingService).stop();
    List<MetricsPublishingService> metricsPublishingServices =
        Collections.singletonList(metricsPublishingService);
    ErrorLogger errorLogger = mock(ErrorLogger.class);
    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        metricsPublishingServices, errorLogger);

    metricsSession.cacheClosed(mock(InternalCache.class));

    verify(errorLogger).logError(anyString(), eq("stop"), same(theClassName), same(theException));
  }

  @Test
  public void cacheClosed_logsErrorMessage_ifMetricsPublishingServiceStopThrowsError() {
    MetricsPublishingService metricsPublishingService =
        metricsPublishingService("metricsPublishingService");
    String theClassName = metricsPublishingService.getClass().getName();
    Error theError = new Error("theErrorMessage");
    doThrow(theError).when(metricsPublishingService).stop();
    List<MetricsPublishingService> metricsPublishingServices =
        Collections.singletonList(metricsPublishingService);
    ErrorLogger errorLogger = mock(ErrorLogger.class);
    metricsSession = new CacheLifecycleMetricsSession(mock(CacheLifecycle.class), compositeRegistry,
        metricsPublishingServices, errorLogger);

    metricsSession.cacheClosed(mock(InternalCache.class));

    verify(errorLogger).logError(anyString(), eq("stop"), same(theClassName), same(theError));
  }

  private MetricsPublishingService metricsPublishingService(String name) {
    return mock(MetricsPublishingService.class, name);
  }
}
