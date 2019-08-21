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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.HashSet;

import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import org.junit.Test;

public class GeodeCompositeMeterRegistryTest {

  @Test
  public void registerBinders_AllBindersWereRegistered() {
    HashSet<MeterBinder> binders = new HashSet<>();

    JvmMemoryMetrics jvmMemoryMetrics = mock(JvmMemoryMetrics.class);
    JvmGcMetrics jvmGcMetrics = mock(JvmGcMetrics.class);
    binders.add(jvmMemoryMetrics);
    binders.add(jvmGcMetrics);

    GeodeCompositeMeterRegistry registry =
        new GeodeCompositeMeterRegistry(binders);

    registry.registerBinders();

    verify(jvmMemoryMetrics).bindTo(registry);
    verify(jvmGcMetrics).bindTo(registry);

  }

  @Test
  public void close_AllAutoClosablesHadCloseCalled() {
    HashSet<MeterBinder> binders = new HashSet<>();

    JvmMemoryMetrics jvmMemoryMetrics = mock(JvmMemoryMetrics.class);
    JvmGcMetrics jvmGcMetrics = mock(JvmGcMetrics.class);
    binders.add(jvmMemoryMetrics);
    binders.add(jvmGcMetrics);

    GeodeCompositeMeterRegistry registry =
        new GeodeCompositeMeterRegistry(binders);

    registry.close();
    verify(jvmGcMetrics).close();
  }
}
