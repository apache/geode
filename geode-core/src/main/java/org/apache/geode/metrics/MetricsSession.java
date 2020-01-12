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
package org.apache.geode.metrics;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.annotations.Experimental;

/**
 * A session that manages Geode metrics. Users can implement and register
 * {@link MetricsPublishingService}s to publish the session's metrics to external monitoring
 * systems.
 *
 * <p>
 * Experimental: Micrometer metrics is a new addition to Geode and the API may change.
 *
 * @see <a href="https://micrometer.io/docs">Micrometer Documentation</a>
 * @see <a href="https://micrometer.io/docs/concepts">Micrometer Concepts</a>
 */
@Experimental("Micrometer metrics is a new addition to Geode and the API may change")
public interface MetricsSession {

  /**
   * Adds the given registry to this metrics session.
   *
   * @param registry the registry to add
   */
  void addSubregistry(MeterRegistry registry);

  /**
   * Removes the given registry from this metrics session.
   *
   * <p>
   * <strong>Caution:</strong> For each meter in this metrics session, this method deletes the
   * corresponding meter from the given registry.
   *
   * @param registry the registry to remove
   */
  void removeSubregistry(MeterRegistry registry);
}
