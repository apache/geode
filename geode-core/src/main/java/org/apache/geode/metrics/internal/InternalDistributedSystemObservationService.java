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

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

import io.micrometer.observation.ObservationRegistry;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.util.CollectingServiceLoader;
import org.apache.geode.internal.util.ListCollectingServiceLoader;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.metrics.ObservationPublishingService;

/**
 * Manages Micrometer Observation on behalf of an {@code InternalDistributedSystem}.
 */
public class InternalDistributedSystemObservationService implements ObservationService {
  private final ObservationService.Builder builder;
  private final Logger logger;
  private final CollectingServiceLoader<ObservationPublishingService> publishingServiceLoader;
  private final ObservationRegistry observationRegistry;
  private final Collection<ObservationPublishingService> publishingServices = new ArrayList<>();

  @FunctionalInterface
  @VisibleForTesting
  interface Factory {
    ObservationService create(ObservationService.Builder builder, Logger logger,
        CollectingServiceLoader<ObservationPublishingService> publishingServiceLoader,
        ObservationRegistry observationRegistry);
  }

  @VisibleForTesting
  InternalDistributedSystemObservationService(ObservationService.Builder builder, Logger logger,
      CollectingServiceLoader<ObservationPublishingService> publishingServiceLoader,
      ObservationRegistry observationRegistry) {
    this.builder = builder;
    this.logger = logger;
    this.publishingServiceLoader = publishingServiceLoader;
    this.observationRegistry = observationRegistry;
  }

  @Override
  public void start() {
    publishingServices.addAll(
        publishingServiceLoader.loadServices(ObservationPublishingService.class));
    publishingServices.forEach(this::startObservationPublishingService);
  }

  @Override
  public void stop() {
    publishingServices.forEach(this::stopObservationPublishingService);
    publishingServices.clear();
  }

  @Override
  public ObservationRegistry getObservationRegistry() {
    return observationRegistry;
  }

  @Override
  public ObservationService.Builder getRebuilder() {
    return builder;
  }

  private void startObservationPublishingService(ObservationPublishingService service) {
    try {
      service.start(this);
    } catch (Exception thrown) {
      logger.error("Exception while starting observation publishing service "
          + service.getClass().getName(), thrown);
    }
  }

  private void stopObservationPublishingService(ObservationPublishingService service) {
    try {
      service.stop(this);
    } catch (Exception thrown) {
      logger.error("Exception while stopping observation publishing service "
          + service.getClass().getName(), thrown);
    }
  }

  public static class Builder implements ObservationService.Builder {
    private Supplier<Logger> loggerSupplier = LogService::getLogger;
    private Factory observationServiceFactory = InternalDistributedSystemObservationService::new;
    private Supplier<ObservationRegistry> observationRegistrySupplier = ObservationRegistry::create;
    private Supplier<CollectingServiceLoader<ObservationPublishingService>> serviceLoaderSupplier =
        ListCollectingServiceLoader::new;

    @Override
    public ObservationService build(InternalDistributedSystem system) {
      return observationServiceFactory.create(this, loggerSupplier.get(),
          serviceLoaderSupplier.get(),
          observationRegistrySupplier.get());
    }

    @Override
    public ObservationService.Builder setIsClient(boolean isClient) {
      return this;
    }

    @VisibleForTesting
    Builder setLogger(Logger logger) {
      loggerSupplier = () -> logger;
      return this;
    }

    @VisibleForTesting
    Builder setObservationRegistry(ObservationRegistry observationRegistry) {
      observationRegistrySupplier = () -> observationRegistry;
      return this;
    }

    @VisibleForTesting
    Builder setObservationServiceFactory(Factory factory) {
      observationServiceFactory = factory;
      return this;
    }

    @VisibleForTesting
    Builder setServiceLoader(CollectingServiceLoader<ObservationPublishingService> serviceLoader) {
      serviceLoaderSupplier = () -> serviceLoader;
      return this;
    }
  }
}
