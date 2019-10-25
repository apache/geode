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

import java.util.ServiceLoader;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.annotations.Experimental;

/**
 * Publishes metrics managed by a {@link MetricsSession}.
 *
 * <p>
 * Geode discovers {@code MetricsPublishingService}s during system creation, using the standard Java
 * {@link ServiceLoader} mechanism:
 *
 * <p>
 *
 * <pre>
 * package com.application;
 *
 * public class MyMetricsPublishingService implements MetricsPublishingService {
 *   private volatile MeterRegistry registry;
 *   private volatile MetricsSession session;
 *
 *  {@literal @}Override
 *   public void start(MetricsSession session) {
 *     this.session = session;
 *     registry = ... // configure your meter registry and start publishing
 *
 *     // add your registry as a sub-registry to the session
 *     session.addSubregistry(registry);
 *   }
 *
 *  {@literal @}Override
 *   public void stop() {
 *     ...
 *     // clean up any resources used by your meter registry
 *     ...
 *
 *     session.removeSubregistry(registry);
 *   }
 * }
 * </pre>
 *
 * <p>
 * To make your service available for loading, add the following provider-configuration file in the
 * resource directory of your application Jar:
 *
 * <p>
 * {@code META-INF/services/org.apache.geode.metrics.MetricsPublishingService}
 *
 * <p>
 * Add a line inside the file indicating the fully qualified class name of your implementation:
 *
 * <p>
 * {@code com.application.MyMetricsPublishingService}
 *
 * <p>
 * Experimental: Micrometer metrics is a new addition to Geode and the API may change.
 *
 * @see <a href="https://micrometer.io/docs">Micrometer Documentation</a>
 * @see <a href="https://micrometer.io/docs/concepts">Micrometer Concepts</a>
 */
@Experimental("Micrometer metrics is a new addition to Geode and the API may change")
public interface MetricsPublishingService {

  /**
   * Invoked when a metrics session starts. A typical implementation will create a meter registry
   * that can publish to an external monitoring system, and call
   * {@link MetricsSession#addSubregistry(MeterRegistry)} to add that registry to the session.
   *
   * @param session the metrics session whose metrics this publishing service will publish
   */
  void start(MetricsSession session);

  /**
   * Invoked when a metrics session stops. The implementation should call
   * {@link MetricsSession#removeSubregistry(MeterRegistry)} to remove each meter registry it added
   * to the session. The implementation should also clean up any other resources it allocated during
   *
   * @param session the metrics session whose metrics this publishing service was publishing
   *        {@link #start(MetricsSession)}.
   */
  void stop(MetricsSession session);
}
