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

/**
 * Manages the lifecycle of a meter registry and allows connecting "downstream" meter registries to
 * publish the managed registry's meters to external monitoring systems.
 */
public interface MetricsSession {
  /**
   * Connects the given "downstream" registry to the managed registry. For each meter in the
   * managed registry, a corresponding meter is created or discovered in the downstream registry.
   * Subsequent operations on the managed registry's meters are forwarded to the corresponding
   * meters in the downstream registry. When meters are subsequently added or removed in the
   * managed registry, corresponding meters are added or removed in the downstream registry.
   *
   * @param downstream the registry to connect to the managed registry
   */
  void connectDownstreamRegistry(MeterRegistry downstream);

  /**
   * Disconnects the given registry from the managed registry. For each meter in the managed
   * registry, the corresponding meter in the downstream registry is removed. Subsequent additions
   * and removals of meters in the managed registry have no effect on disconnected registries.
   *
   * @param downstream the registry to disconnect from the managed registry
   */
  void disconnectDownstreamRegistry(MeterRegistry downstream);
}
