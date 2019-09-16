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

import java.util.Collection;
import java.util.HashSet;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.cache.CacheLifecycleListener;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.internal.util.ListCollectingServiceLoader;
import org.apache.geode.metrics.MetricsPublishingService;
import org.apache.geode.metrics.MetricsSession;

public class CacheLifecycleMetricsSession implements MetricsSession, CacheLifecycleListener {
  private static final Logger logger = LogService.getLogger();

  private final CacheLifecycle cacheLifecycle;
  private final CompositeMeterRegistry registry;
  private final Collection<MetricsPublishingService> metricsPublishingServices;
  private final ErrorLogger errorLogger;

  public static Builder builder() {
    return new Builder();
  }

  @VisibleForTesting
  CacheLifecycleMetricsSession(CacheLifecycle cacheLifecycle, CompositeMeterRegistry registry,
      Collection<MetricsPublishingService> metricsPublishingServices) {
    this(cacheLifecycle, registry, metricsPublishingServices, logger::error);
  }

  @VisibleForTesting
  CacheLifecycleMetricsSession(CacheLifecycle cacheLifecycle, CompositeMeterRegistry registry,
      Collection<MetricsPublishingService> metricsPublishingServices, ErrorLogger errorLogger) {
    this.cacheLifecycle = cacheLifecycle;
    this.registry = registry;
    this.metricsPublishingServices = metricsPublishingServices;
    this.errorLogger = errorLogger;
  }

  @Override
  public void addSubregistry(MeterRegistry subregistry) {
    registry.add(subregistry);
  }

  @Override
  public void removeSubregistry(MeterRegistry subregistry) {
    registry.remove(subregistry);
  }

  @Override
  public void cacheCreated(InternalCache cache) {
    for (MetricsPublishingService metricsPublishingService : metricsPublishingServices) {
      try {
        metricsPublishingService.start(this);
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Error | RuntimeException e) {
        logError(errorLogger, "start", metricsPublishingService.getClass().getName(), e);
      }
    }
  }

  @Override
  public void cacheClosed(InternalCache cache) {
    cacheLifecycle.removeListener(this);

    for (MetricsPublishingService metricsPublishingService : metricsPublishingServices) {
      try {
        metricsPublishingService.stop();
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Error | RuntimeException e) {
        logError(errorLogger, "stop", metricsPublishingService.getClass().getName(), e);
      }
    }

    for (MeterRegistry downstream : new HashSet<>(registry.getRegistries())) {
      removeSubregistry(downstream);
    }

    registry.close();
  }

  @VisibleForTesting
  CompositeMeterRegistry meterRegistry() {
    return registry;
  }

  @VisibleForTesting
  Collection<MetricsPublishingService> metricsPublishingServices() {
    return metricsPublishingServices;
  }

  private static void logError(ErrorLogger errorLogger, String methodName, String className,
      Throwable throwable) {
    errorLogger.logError("Error invoking {} for MetricsPublishingService implementation {}",
        methodName, className, throwable);
  }

  interface ErrorLogger {
    void logError(String message, String methodName, String className, Throwable throwable);
  }

  public static class Builder {

    private CollectingServiceLoader serviceLoader = new ListCollectingServiceLoader();
    private CacheLifecycle cacheLifecycle = new CacheLifecycle() {};

    private Builder() {
      // private to prevent instantiation
    }

    @VisibleForTesting
    Builder setCacheLifecycle(CacheLifecycle cacheLifecycle) {
      this.cacheLifecycle = cacheLifecycle;
      return this;
    }

    @VisibleForTesting
    Builder setServiceLoader(CollectingServiceLoader serviceLoader) {
      this.serviceLoader = serviceLoader;
      return this;
    }

    public CacheLifecycleMetricsSession build(CompositeMeterRegistry registry) {
      Collection<MetricsPublishingService> services =
          serviceLoader.loadServices(MetricsPublishingService.class);
      CacheLifecycleMetricsSession cacheLifecycleMetricsSession =
          new CacheLifecycleMetricsSession(cacheLifecycle, registry, services);
      cacheLifecycle.addListener(cacheLifecycleMetricsSession);
      return cacheLifecycleMetricsSession;
    }
  }

  @VisibleForTesting
  interface CacheLifecycle {

    default void addListener(CacheLifecycleListener listener) {
      GemFireCacheImpl.addCacheLifecycleListener(listener);
    }

    default void removeListener(CacheLifecycleListener listener) {
      GemFireCacheImpl.removeCacheLifecycleListener(listener);
    }
  }
}
