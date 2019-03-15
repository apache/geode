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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.function.Function;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import org.apache.geode.annotations.VisibleForTesting;

public class CacheMeterRegistryFactory implements CompositeMeterRegistryFactory {

  @VisibleForTesting
  static final String CLUSTER_ID_TAG = "cluster.id";
  @VisibleForTesting
  static final String MEMBER_NAME_TAG = "member.name";
  @VisibleForTesting
  static final String HOST_NAME_TAG = "host.name";
  @VisibleForTesting
  static final String HOST_JVM_MEMORY_USED = "host.jvm.memory.used";
  @VisibleForTesting
  static final String JVM_MEMORY_AREA_TAG = "area";

  @Override
  public CompositeMeterRegistry create(int systemId, String memberName, String hostName) {
    CompositeMeterRegistry registry = new CompositeMeterRegistry();

    MeterRegistry.Config registryConfig = registry.config();
    registryConfig.commonTags(CLUSTER_ID_TAG, String.valueOf(systemId));
    registryConfig.commonTags(MEMBER_NAME_TAG, memberName == null ? "" : memberName);
    registryConfig.commonTags(HOST_NAME_TAG, hostName == null ? "" : hostName);

    createMemoryMeter(registry);

    return registry;
  }

  private void createMemoryMeter(CompositeMeterRegistry registry) {
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    String meterName = HOST_JVM_MEMORY_USED;
    String description = "The amount of memory in bytes that is used by the Java virtual machine";
    String unit = "bytes";

    Gauge
        .builder(meterName, memoryMXBean, m -> getUsageValue(m, MemoryMXBean::getHeapMemoryUsage))
        .tags(JVM_MEMORY_AREA_TAG, "heap")
        .description(description)
        .baseUnit(unit)
        .register(registry);

    Gauge
        .builder(meterName, memoryMXBean,
            m -> getUsageValue(m, MemoryMXBean::getNonHeapMemoryUsage))
        .tags(JVM_MEMORY_AREA_TAG, "nonheap")
        .description(description)
        .baseUnit(unit)
        .register(registry);
  }

  private double getUsageValue(MemoryMXBean memoryMXBean,
      Function<MemoryMXBean, MemoryUsage> extractor) {
    MemoryUsage usage = null;
    try {
      usage = extractor.apply(memoryMXBean);
    } catch (InternalError e) {
      // Defensive for potential InternalError with some specific JVM options. Based on its Javadoc,
      // MemoryMXBean.getUsage() should return null, not throwing InternalError, so it seems to be a
      // JVM bug.
    }
    if (usage == null) {
      return Double.NaN;
    }
    return usage.getUsed();
  }
}
