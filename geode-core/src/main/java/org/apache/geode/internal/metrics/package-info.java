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
 * Geode's metrics system uses Micrometer meters and registries. The key components are:
 *
 * <ul>
 * <li>
 * <em>Meters</em> instrument code to define metrics.
 * </li>
 * <li>
 * Geode's <em>primary meter registry</em> maintains the set of meters registered with Geode.
 * Developers can use the primary registry to create meters to instrument their own code.
 * </li>
 * <li>
 * <em>Downstream meter registries</em> publish some or all of the registered meters to external
 * systems. Developers can configure the Micrometer meter registry implementation of their choice
 * and connect it as a downstream registry using the {@code MetricsCollector}.
 * </li>
 * <li>
 * The {@link org.apache.geode.internal.metrics.MetricsCollector MetricsCollector} coordinates
 * between the primary meter registry and the downstream meter registries. Developers can use the
 * metrics collector to connect a downstream registry and to obtain Geode's primary registry to
 * create and register meters.
 * </li>
 * <li>
 * The {@link org.apache.geode.distributed.internal.InternalDistributedSystem
 * InternalDistributedSystem} configures Geode's primary meter registry and the metrics collector on
 * startup.
 * </li>
 * </ul>
 */
package org.apache.geode.internal.metrics;
