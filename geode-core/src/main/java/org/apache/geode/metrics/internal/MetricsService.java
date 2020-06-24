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

import java.util.Collection;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.metrics.MetricsSession;
import org.apache.geode.services.module.ModuleService;

/**
 * A metrics session that can be started and stopped, and that manages a meter registry.
 */
public interface MetricsService extends MetricsSession {

  /**
   * Starts this metrics service and configures its meter registry. How this metrics service
   * configures its meter registry is defined by the implementation.
   */
  void start();

  /**
   * Stops this metrics service, freeing any resources.
   */
  void stop();

  /**
   * Returns this service's meter registry. The registry may be {@code null} before the service
   * is started or after it is stopped.
   *
   * @return the meter registry
   */
  MeterRegistry getMeterRegistry();

  /**
   * Returns the builder that built this metrics service. The builder can be used during reconnect
   * to create a metrics service configured similarly to this one.
   * <p>
   * Before calling this method, call {@link #stop()}.
   *
   * @return the builder that built this metrics service
   */
  Builder getRebuilder();

  interface Builder {
    /**
     * Adds the given meter registry to the eventually built metrics service. In the event of a
     * reconnect, this registry will be removed from the metrics service in the disconnected
     * system and added to the metrics service in the reconnected system.
     *
     * @return this builder
     */
    Builder addPersistentMeterRegistry(MeterRegistry registry);

    /**
     * Adds the given meter registries to the eventually built metrics service. In the event of a
     * reconnect, these registries will be removed from metrics service in the disconnected
     * system added to the metrics service in the reconnected system.
     *
     * @return this builder
     */
    Builder addPersistentMeterRegistries(Collection<MeterRegistry> registries);

    /**
     * Informs this builder whether it is building a metrics service on behalf of a client.
     *
     * @return this builder
     */
    Builder setIsClient(boolean isClient);

    /**
     * Builds a metrics service associated with the given system.
     */
    MetricsService build(InternalDistributedSystem system, ModuleService moduleService);
  }
}
