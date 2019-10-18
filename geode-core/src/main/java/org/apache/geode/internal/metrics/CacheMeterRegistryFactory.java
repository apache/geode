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

import static java.util.Objects.requireNonNull;

import java.util.Properties;
import java.util.function.BooleanSupplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

public class CacheMeterRegistryFactory implements CompositeMeterRegistryFactory {

  private final BooleanSupplier hasLocators;
  private final BooleanSupplier hasCacheServer;

  public CacheMeterRegistryFactory() {
    this(Locator::hasLocator, () -> ServerLauncher.getInstance() != null);
  }

  @VisibleForTesting
  CacheMeterRegistryFactory(BooleanSupplier hasLocators, BooleanSupplier hasCacheServer) {
    this.hasLocators = hasLocators;
    this.hasCacheServer = hasCacheServer;
  }

  @Override
  public CompositeMeterRegistry create(int systemId, String memberName, String hostName,
      boolean isClient, String memberType) {
    requireNonNull(memberName);
    requireNonNull(hostName);
    requireNonNull(memberType);
    if (hostName.isEmpty()) {
      throw new IllegalArgumentException("Host name must not be empty");
    }


    JvmGcMetrics gcMetricsBinder = new JvmGcMetrics();
    GeodeCompositeMeterRegistry registry = new GeodeCompositeMeterRegistry(gcMetricsBinder);

    MeterRegistry.Config registryConfig = registry.config();
    if (!isClient) {
      registryConfig.commonTags("cluster", String.valueOf(systemId));
    }
    if (!memberName.isEmpty()) {
      registryConfig.commonTags("member", memberName);
    }
    registryConfig.commonTags("host", hostName);
    registryConfig.commonTags("member.type", memberType);

    gcMetricsBinder.bindTo(registry);
    new JvmMemoryMetrics().bindTo(registry);
    new JvmThreadMetrics().bindTo(registry);
    new ProcessorMetrics().bindTo(registry);
    new UptimeMetrics().bindTo(registry);
    new FileDescriptorMetrics().bindTo(registry);

    return registry;
  }

  @Override
  public CompositeMeterRegistry create(InternalDistributedSystem internalDistributedSystem,
      boolean isClient) {
    int clusterId = internalDistributedSystem.getConfig().getDistributedSystemId();

    String memberName = internalDistributedSystem.getName();
    String hostName = internalDistributedSystem.getDistributedMember().getHost();
    String memberType = resolveMemberType(hasLocators.getAsBoolean(),
        hasCacheServer.getAsBoolean());

    return create(clusterId, memberName, hostName, isClient, memberType);
  }

  /**
   * Will be true the configuration properties requested an embedded locator
   */
  public boolean startLocatorRequested(Properties configProperties) {
    // If the property exists that means a locator was requested.

    if (configProperties == null) {
      return false;
    }

    String property = configProperties.getProperty(DistributionConfig.START_LOCATOR_NAME);

    return !((property == null) || (property.isEmpty()));
  }

  private static final String LOCATOR = "locator";
  private static final String SERVER = "server";
  private static final String SERVER_LOCATOR = "server-locator";
  private static final String EMBEDDED_CACHE = "embedded-cache";

  /**
   * Guesses the member type based on passed in conditions primarily for metrics
   *
   * @param hasLocator will be true on a locator or on a server that has an embedded
   *        locator
   * @param hasCacheServer will be true if the cache was not created by ServerLauncher
   * @return member type as a String
   */
  private String resolveMemberType(boolean hasLocator,
      boolean hasCacheServer) {

    // logger.info("ALINDSEY: hasLocator=" + hasLocator + " hasCacheServer=" + hasCacheServer);

    if (hasCacheServer && hasLocator) {
      return SERVER_LOCATOR;
    }

    if (hasCacheServer) {
      return SERVER;
    }

    if (hasLocator) {
      return LOCATOR;
    }

    return EMBEDDED_CACHE;
  }

}
