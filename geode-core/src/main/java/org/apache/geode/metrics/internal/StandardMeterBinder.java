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

import static java.util.Arrays.asList;

import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;

/**
 * Binds a standard suite of meters to a meter registry. Meters include:
 *
 * <ul>
 * <li>File descriptor metrics</li>
 * <li>JVM garbage collection metrics</li>
 * <li>JVM memory metrics</li>
 * <li>JVM thread metrics</li>
 * <li>Process uptime metrics</li>
 * <li>System processor metrics</li>
 * </ul>
 */
public class StandardMeterBinder extends CompoundMeterBinder {
  public StandardMeterBinder() {
    super(asList(
        new FileDescriptorMetrics(),
        new JvmGcMetrics(),
        new JvmMemoryMetrics(),
        new JvmThreadMetrics(),
        new UptimeMetrics(),
        new ProcessorMetrics()));
  }
}
