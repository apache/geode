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
 * Provides the ability to add and remove a meter registry for publishing cache metrics to external
 * monitoring systems.
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
   * Adds the given registry to the cache's composite registry.
   *
   * @param subregistry the registry to add
   */
  void addSubregistry(MeterRegistry subregistry);

  /**
   * Removes the given registry from the cache's composite registry.
   *
   * <p>
   * <strong>Caution:</strong> This method deletes from the sub-registry each meter that corresponds
   * to a meter in the cache's composite registry.
   *
   * @param subregistry the registry to remove
   */
  void removeSubregistry(MeterRegistry subregistry);
}
