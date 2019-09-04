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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCacheBuilder;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.internal.util.ListCollectingServiceLoader;
import org.apache.geode.metrics.MetricsPublishingService;

// TODO DHE: Catch and log Exception thrown by additional calls?
// TODO DHE: Catch and log Error as well as Exception?
// TODO DHE: Which errors/exceptions should be logged as warnings vs errors?

/**
 * A cache metrics session that manages meters, user registries, and publishing services using
 * a composite meter registry.
 */
public class CompositeCacheMetricsSession implements CacheMetricsSession {
  private final Logger logger;
  private final Supplier<CompositeMeterRegistry> registrySupplier;
  private final CollectingServiceLoader<MetricsPublishingService> publishingServiceLoader;
  private final Collection<MetricsPublishingService> publishingServices = new ArrayList<>();
  private final Supplier<Set<MeterBinder>> binderSupplier;
  private final Set<AutoCloseable> bindersToClose = new HashSet<>();
  private final Set<MeterRegistry> userRegistries;
  private CompositeMeterRegistry compositeRegistry;

  @VisibleForTesting
  CompositeCacheMetricsSession(Logger logger,
      Supplier<CompositeMeterRegistry> registrySupplier,
      CollectingServiceLoader<MetricsPublishingService> publishingServiceLoader,
      Supplier<Set<MeterBinder>> binderSupplier, Set<MeterRegistry> userRegistries) {
    this.logger = logger;
    this.registrySupplier = registrySupplier;
    this.publishingServiceLoader = publishingServiceLoader;
    this.binderSupplier = binderSupplier;
    this.userRegistries = new HashSet<>(userRegistries);
  }

  /**
   * Create a cache metrics session with the given user registries.
   *
   * @param userRegistries the user registries to add the the cache's composite meter registry
   */
  public CompositeCacheMetricsSession(Set<MeterRegistry> userRegistries) {
    this(LogService.getLogger(), CompositeMeterRegistry::new, new ListCollectingServiceLoader<>(),
        () -> standardMeterBinders(), userRegistries);
  }

  /**
   * Starts this metrics session and adds it to the given system. The session remains active until
   * {@code stop()} is called.
   * <p>
   * This method creates fresh composite meter registry configured with:
   * <ul>
   * <li>a set of common tags describing the given system</li>
   * <li>a set of meters that measure JVM and process quantities</li>
   * <li>this metrics session's user registries as sub-registries</li>
   * </ul>
   *
   * This method also loads and starts each {@link MetricsPublishingService} found on the classpath.
   *
   * @param system the system that owns this metrics session's cache
   */
  @Override
  public void start(InternalDistributedSystem system) {
    compositeRegistry = registrySupplier.get();

    configureCommonTags(system);
    addStandardMeters();

    userRegistries.forEach(compositeRegistry::add);

    publishingServices.addAll(publishingServiceLoader.loadServices(MetricsPublishingService.class));

    publishingServices.forEach(this::startMetricsPublishingService);

    system.setMetricsSession(this);
  }

  /**
   * Returns this metrics session's meter registry if the session is active.
   *
   * @return this metrics session's meter registry, or {@code null} if the session is not active
   */
  @Override
  public MeterRegistry meterRegistry() {
    return compositeRegistry;
  }

  /**
   * Prepares the given cache builder to reconstruct this metrics session in the cache to be built.
   * This implementation adds each user registry to the cache builder.
   *
   * @param cacheBuilder the cache builder to prepare
   */
  @Override
  public void prepareToReconstruct(InternalCacheBuilder cacheBuilder) {
    userRegistries.forEach(cacheBuilder::addMeterSubregistry);
  }

  /**
   * Stops all publishing services and closes this metrics session's composite meter registry.
   * This method removes from the composite registry all user registries and registries added to the
   * session by publishing services. This method does not close any user registry or any registry
   * added to the session by publishing services.
   * <p>
   * After this method is called, this session is no longer active, and no longer has a meter
   * registry.
   */
  @Override
  public void stop() {
    bindersToClose.forEach(this::closeMeterBinder);
    bindersToClose.clear();

    if (compositeRegistry != null) {
      userRegistries.forEach(compositeRegistry::remove);

      publishingServices.forEach(this::stopMetricsPublishingService);
      publishingServices.clear();

      // TODO DHE: Publishing services should remove their registries. Should we log a warning if
      // any registries remain?
      Set<MeterRegistry> subRegistries = new HashSet<>(compositeRegistry.getRegistries());
      subRegistries.forEach(compositeRegistry::remove);

      compositeRegistry.close();
      compositeRegistry = null;
    }
  }

  private void addStandardMeters() {
    Collection<MeterBinder> binders = binderSupplier.get();
    binders.stream()
        .filter(AutoCloseable.class::isInstance)
        .map(AutoCloseable.class::cast)
        .forEach(bindersToClose::add);

    // TODO DHE: Log warning/error if binder throws?
    binders.forEach(binder -> binder.bindTo(compositeRegistry));
  }

  private void configureCommonTags(InternalDistributedSystem system) {
    MeterRegistry.Config registryConfig = compositeRegistry.config();

    String clusterId = String.valueOf(system.getConfig().getDistributedSystemId());
    registryConfig.commonTags("cluster", clusterId);

    String memberName = system.getName();
    registryConfig.commonTags("member", memberName == null ? "" : memberName);

    String hostName = system.getDistributedMember().getHost();
    registryConfig.commonTags("host", hostName == null ? "" : hostName);
  }

  private static Set<MeterBinder> standardMeterBinders() {
    Set<MeterBinder> binders = new HashSet<>();
    binders.add(new FileDescriptorMetrics());
    binders.add(new JvmGcMetrics());
    binders.add(new JvmMemoryMetrics());
    binders.add(new JvmThreadMetrics());
    binders.add(new ProcessorMetrics());
    binders.add(new UptimeMetrics());
    return binders;
  }

  @Override
  public void addSubregistry(MeterRegistry subregistry) {
    compositeRegistry.add(subregistry);
  }

  @Override
  public void removeSubregistry(MeterRegistry subregistry) {
    compositeRegistry.remove(subregistry);
  }

  private void closeMeterBinder(AutoCloseable binder) {
    try {
      binder.close();
    } catch (Exception thrown) {
      logger.warn("Exception while closing meter binder " + binder, thrown);
    }
  }

  private void startMetricsPublishingService(MetricsPublishingService service) {
    try {
      service.start(this);
    } catch (Exception thrown) {
      logger.error("Exception while starting metrics publishing service "
          + service.getClass().getName(), thrown);
    }
  }

  private void stopMetricsPublishingService(MetricsPublishingService service) {
    try {
      service.stop();
    } catch (Exception thrown) {
      logger.error("Exception while starting metrics publishing service "
          + service.getClass().getName(), thrown);
    }
  }
}
