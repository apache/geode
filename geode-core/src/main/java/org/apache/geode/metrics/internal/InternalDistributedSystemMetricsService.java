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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.internal.util.ListCollectingServiceLoader;
import org.apache.geode.metrics.MetricsPublishingService;

/**
 * Manages metrics on behalf of an {@code InternalDistributedSystem}.
 * This metrics service uses a composite meter registry to manage meters, client-supplied
 * meter registries, and metrics publishing services.
 * <p>
 * Each meter added to this metrics service's meter registry gains these common tags:
 * </p>
 * <ul>
 * <li><em>member</em>: The name of the system.</li>
 * <li><em>host</em>: The name of the host on which the system is running.</li>
 * <li><em>cluster</em>: The ID of the cluster that includes the system.
 * This tag is omitted if the system is a client.
 * </li>
 * </ul>
 */
public class InternalDistributedSystemMetricsService implements MetricsService {
  private final Logger logger;
  private final CompositeMeterRegistry meterRegistry;
  private final CollectingServiceLoader<MetricsPublishingService> publishingServiceLoader;
  private final Collection<MetricsPublishingService> publishingServices = new ArrayList<>();
  private final CloseableMeterBinder binder;
  private final Set<MeterRegistry> persistentMeterRegistries = new HashSet<>();
  private final MetricsService.Builder builder;

  @FunctionalInterface
  @VisibleForTesting
  interface Factory {
    MetricsService create(MetricsService.Builder builder, Logger logger,
        CollectingServiceLoader<MetricsPublishingService> publishingServiceLoader,
        CompositeMeterRegistry metricsServiceMeterRegistry,
        Collection<MeterRegistry> persistentMeterRegistries, CloseableMeterBinder binder,
        InternalDistributedSystem system,
        boolean isClient);
  }

  @VisibleForTesting
  InternalDistributedSystemMetricsService(MetricsService.Builder builder, Logger logger,
      CollectingServiceLoader<MetricsPublishingService> publishingServiceLoader,
      CompositeMeterRegistry metricsServiceMeterRegistry,
      Collection<MeterRegistry> persistentMeterRegistries, CloseableMeterBinder binder,
      InternalDistributedSystem system, boolean isClient) {
    this.builder = builder;
    this.logger = logger;
    this.meterRegistry = metricsServiceMeterRegistry;
    this.publishingServiceLoader = publishingServiceLoader;
    this.binder = binder;
    this.persistentMeterRegistries.addAll(persistentMeterRegistries);
    meterRegistry.config().commonTags(commonTags(system, isClient));
  }

  /**
   * Starts this metrics service. The service remains active until {@code stop()} is called.
   * <p>
   * This method configures the service's meter registry with:
   * <ul>
   * <li>a set of standard process, system, and JVM meters</li>
   * </ul>
   *
   * This method also loads and starts each {@link MetricsPublishingService} found on the classpath.
   */
  @Override
  public void start() {
    persistentMeterRegistries.forEach(meterRegistry::add);

    binder.bindTo(meterRegistry);

    publishingServices.addAll(publishingServiceLoader.loadServices(MetricsPublishingService.class));
    publishingServices.forEach(this::startMetricsPublishingService);
  }

  /**
   * Returns this metrics service's meter registry.
   *
   * @return this metrics service's meter registry
   */
  @Override
  public MeterRegistry getMeterRegistry() {
    return meterRegistry;
  }

  @Override
  public MetricsService.Builder getRebuilder() {
    return builder;
  }

  /**
   * Stops all publishing services and closes this metrics service's meter registry. This method
   * removes from the metrics service all client meter registries and all registries added by
   * publishing services. This method does not close any client meter registry or any registry
   * added to this metrics service by publishing services.
   * <p>
   * After this method returns, this metrics service is no longer active, and its meter registry is
   * closed.
   */
  @Override
  public void stop() {
    closeMeterBinder();
    clearAndCloseMeterRegistry();
  }

  @Override
  public void addSubregistry(MeterRegistry subregistry) {
    meterRegistry.add(subregistry);
  }

  @Override
  public void removeSubregistry(MeterRegistry subregistry) {
    meterRegistry.remove(subregistry);
  }

  private static Set<Tag> commonTags(InternalDistributedSystem system, boolean isClient) {
    Set<Tag> commonTags = new HashSet<>();

    if (!isClient) {
      String systemId = String.valueOf(system.getConfig().getDistributedSystemId());
      commonTags.add(Tag.of("cluster", systemId));
    }

    String memberName = system.getName();
    requireNonNull(memberName);
    if (!memberName.isEmpty()) {
      commonTags.add(Tag.of("member", memberName));
    }

    String hostName = system.getDistributedMember().getHost();
    requireNonNull(hostName);

    if (hostName.isEmpty()) {
      throw new IllegalArgumentException("Host name must not be empty");
    }
    commonTags.add(Tag.of("host", hostName));
    return commonTags;
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
      service.stop(this);
    } catch (Exception thrown) {
      logger.error("Exception while stopping metrics publishing service "
          + service.getClass().getName(), thrown);
    }
  }

  private void closeMeterBinder() {
    try {
      binder.close();
    } catch (Exception ignored) {
      // Nothing to do, because the standard meter binder catches and logs all exceptions
    }
  }

  private void clearAndCloseMeterRegistry() {
    if (meterRegistry != null) {
      publishingServices.forEach(this::stopMetricsPublishingService);
      publishingServices.clear();

      new HashSet<>(meterRegistry.getRegistries())
          .forEach(meterRegistry::remove);

      meterRegistry.close();
    }
  }

  public static class Builder implements MetricsService.Builder {
    private boolean isClient = false;
    private Supplier<Logger> loggerSupplier = LogService::getLogger;
    private Supplier<CloseableMeterBinder> meterBinderSupplier = StandardMeterBinder::new;
    private Factory metricsServiceFactory = InternalDistributedSystemMetricsService::new;
    private Supplier<CompositeMeterRegistry> compositeRegistrySupplier =
        CompositeMeterRegistry::new;
    private Supplier<CollectingServiceLoader<MetricsPublishingService>> serviceLoaderSupplier =
        ListCollectingServiceLoader::new;
    private Set<MeterRegistry> persistentMeterRegistries = new HashSet<>();

    @Override
    public MetricsService build(InternalDistributedSystem system) {
      return metricsServiceFactory.create(this, loggerSupplier.get(), serviceLoaderSupplier.get(),
          compositeRegistrySupplier.get(), persistentMeterRegistries, meterBinderSupplier.get(),
          system, isClient);
    }

    @Override
    public Builder addPersistentMeterRegistry(MeterRegistry registry) {
      persistentMeterRegistries.add(registry);
      return this;
    }

    @Override
    public Builder addPersistentMeterRegistries(Collection<MeterRegistry> registries) {
      this.persistentMeterRegistries.addAll(registries);
      return this;
    }

    @Override
    public MetricsService.Builder setIsClient(boolean isClient) {
      this.isClient = isClient;
      return this;
    }

    @VisibleForTesting
    Builder setBinder(CloseableMeterBinder binder) {
      meterBinderSupplier = () -> binder;
      return this;
    }

    @VisibleForTesting
    Builder setCompositeMeterRegistry(CompositeMeterRegistry registry) {
      compositeRegistrySupplier = () -> registry;
      return this;
    }

    @VisibleForTesting
    Builder setLogger(Logger logger) {
      loggerSupplier = () -> logger;
      return this;
    }

    @VisibleForTesting
    Builder setMetricsServiceFactory(Factory factory) {
      metricsServiceFactory = factory;
      return this;
    }

    @VisibleForTesting
    Builder setServiceLoader(CollectingServiceLoader<MetricsPublishingService> serviceLoader) {
      serviceLoaderSupplier = () -> serviceLoader;
      return this;
    }
  }
}
