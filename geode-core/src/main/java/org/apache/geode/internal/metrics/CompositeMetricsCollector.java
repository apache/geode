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

/**
 * Collects metrics using a {@link CompositeMeterRegistry}.
 */
public class CompositeMetricsCollector implements MetricsCollector {
  private final CompositeMeterRegistry primaryRegistry;

  /**
   * Constructs a metrics collector that uses a new {@link CompositeMeterRegistry} as its primary
   * registry.
   */
  public CompositeMetricsCollector() {
    this(new CompositeMeterRegistry());
  }

  /**
   * Constructs a metrics collector that uses the given registry as its primary registry.
   *
   * @param primaryRegistry the registry to use as the primary registry
   */
  public CompositeMetricsCollector(CompositeMeterRegistry primaryRegistry) {
    this.primaryRegistry = primaryRegistry;
  }

  @Override
  public MeterRegistry primaryRegistry() {
    return primaryRegistry;
  }

  /**
   * {@inheritDoc} In this implementation, the newly-created meter in the downstream registry starts
   * at an initial state defined by the downstream registry.
   *
   * @param downstream the downstream registry to add
   */
  @Override
  public void addDownstreamRegistry(MeterRegistry downstream) {
    primaryRegistry.add(downstream);
  }

  @Override
  public void removeDownstreamRegistry(MeterRegistry downstream) {
    primaryRegistry.remove(downstream);
  }
}
