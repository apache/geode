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

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCacheBuilder;
import org.apache.geode.metrics.MetricsSession;

/**
 * Manages a metrics session on behalf of a cache.
 */
public interface CacheMetricsSession extends MetricsSession {

  /**
   * Starts this metrics session and configures its meter registry. How this metrics session
   * configures its meter registry is defined by the implementation.
   *
   * @param system the system that owns this metrics session's cache
   */
  void start(InternalDistributedSystem system);

  /**
   * Closes the meter registry and frees any resources allocated by this metrics session.
   */
  void stop();

  /**
   * Returns this session's meter registry. The registry may be {@code null} before the session
   * is started or after it is stopped.
   *
   * @return the meter registry or {@code null} if the session is not active
   */
  MeterRegistry meterRegistry();

  /**
   * Prepares the given cache builder to reconstruct this metrics session in the cache to be built.
   * How this metrics session prepares the cache builder is defined by the implementation.
   *
   * @param cacheBuilder the cache builder to prepare
   */
  void prepareToReconstruct(InternalCacheBuilder cacheBuilder);
}
