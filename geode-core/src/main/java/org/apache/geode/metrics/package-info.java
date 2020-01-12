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
/**
 * Geode uses Micrometer for its metrics. Each Geode cache maintains a registry of all meters that
 * instrument Geode code. Users can add sub-registries for various purposes. A key purpose for a
 * sub-registry is to publish a cache's metrics to an external monitoring system.
 *
 * <p>
 * The {@link org.apache.geode.metrics.MetricsSession MetricsSession} provides the ability to add
 * and remove a meter registry for publishing metrics to external monitoring systems.
 *
 * <p>
 * The {@link org.apache.geode.metrics.MetricsPublishingService MetricsPublishingService} defines
 * a service that users can implement to be notified when a metrics session starts or stops. When
 * a session starts, the service can add a meter registry to the session. When the session stops,
 * the service should clean up any resources and remove its meter registry from the session.
 *
 * @see <a href="https://micrometer.io/docs">Micrometer Documentation</a>
 * @see <a href="https://micrometer.io/docs/concepts">Micrometer Concepts</a>
 */
@Experimental("Micrometer metrics is a new addition to Geode and the API may change")
package org.apache.geode.metrics;

import org.apache.geode.annotations.Experimental;
