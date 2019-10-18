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

import java.util.HashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
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
  public CompositeMeterRegistry create(InternalDistributedSystem system, boolean isClient) {

    JvmGcMetrics gcMetricsBinder = new JvmGcMetrics();
    GeodeCompositeMeterRegistry registry = new GeodeCompositeMeterRegistry(gcMetricsBinder);

    addCommonTags(registry, system, isClient);

    gcMetricsBinder.bindTo(registry);
    new JvmMemoryMetrics().bindTo(registry);
    new JvmThreadMetrics().bindTo(registry);
    new ProcessorMetrics().bindTo(registry);
    new UptimeMetrics().bindTo(registry);
    new FileDescriptorMetrics().bindTo(registry);

    return registry;
  }

  private void addCommonTags(GeodeCompositeMeterRegistry registry, InternalDistributedSystem system,
      boolean isClient) {
    Set<Tag> commonTags = getCommonTags(system, isClient);

    MeterRegistry.Config registryConfig = registry.config();
    registryConfig.commonTags(commonTags);
  }

  private Set<Tag> getCommonTags(InternalDistributedSystem system, boolean isClient) {
    int clusterId = system.getConfig().getDistributedSystemId();

    String memberName = system.getName();
    String hostName = system.getDistributedMember().getHost();
    String memberType = determineMemberType(hasLocators.getAsBoolean(),
        hasCacheServer.getAsBoolean());

    requireNonNull(memberName, "Member Name is null.");
    requireNonNull(hostName, "Host Name is null.");

    if (hostName.isEmpty()) {
      throw new IllegalArgumentException("Host name must not be empty");
    }
    Set<Tag> tags = new HashSet<>();

    if (!isClient) {
      tags.add(Tag.of("cluster", String.valueOf(clusterId)));

    }

    if (!memberName.isEmpty()) {
      tags.add(Tag.of("member", memberName));
    }

    tags.add(Tag.of("host", hostName));
    tags.add(Tag.of("member.type", memberType));
    return tags;
  }

  private String determineMemberType(boolean hasLocator, boolean hasCacheServer) {
    if (hasCacheServer && hasLocator) {
      return "server-locator";
    }

    if (hasCacheServer) {
      return "server";
    }

    if (hasLocator) {
      return "locator";
    }

    return "embedded-cache";
  }
}
