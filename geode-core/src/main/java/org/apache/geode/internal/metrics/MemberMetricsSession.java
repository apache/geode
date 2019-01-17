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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.DistributionConfig;

/**
 * Creates, configures, and manages the meter registry for a member of a distributed system.
 */
public class MemberMetricsSession implements MetricsSession {
  private final CompositeMeterRegistry composite;

  /**
   * Constructs a metrics session to maintain the meter registry for a member of a distributed
   * system. The registry is configured to give each meter a set of common tags that identify the
   * member.
   *
   * @param memberConfig the configuration that describes this member
   */
  public MemberMetricsSession(DistributionConfig memberConfig) {
    this(new CompositeMeterRegistry(), memberConfig);
  }

  /**
   * Constructs a metrics session that maintains its meters using the given registry.
   *
   * @param registry the registry to use as the member's meter registry
   * @param memberConfig the configuration that describes this member
   */
  @VisibleForTesting
  public MemberMetricsSession(CompositeMeterRegistry registry, DistributionConfig memberConfig) {
    MeterRegistry.Config registryConfig = registry.config();

    int systemId = memberConfig.getDistributedSystemId();
    registryConfig.commonTags("DistributedSystemID", String.valueOf(systemId));

    String memberName = memberConfig.getName();
    registryConfig.commonTags("MemberName", memberName == null ? "" : memberName);

    composite = registry;
  }

  /**
   * Returns the member's meter registry.
   */
  public MeterRegistry meterRegistry() {
    return composite;
  }

  @Override
  public void connectDownstreamRegistry(MeterRegistry downstream) {
    composite.add(downstream);
  }

  @Override
  public void disconnectDownstreamRegistry(MeterRegistry downstream) {
    composite.remove(downstream);
  }
}
