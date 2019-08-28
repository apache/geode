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
package org.apache.geode.internal.cache.execute.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.Test;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;

public class MeterRegistrySupplierTest {

  @Test
  public void get_internalDistributedSystemIsNull_expectNull() {
    MeterRegistrySupplier meterRegistrySupplier = new MeterRegistrySupplier(() -> null);

    MeterRegistry value = meterRegistrySupplier.get();

    assertThat(value).isNull();
  }

  @Test
  public void get_internalCacheIsNull_expectNull() {
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    when(internalDistributedSystem.getCache()).thenReturn(null);
    MeterRegistrySupplier meterRegistrySupplier =
        new MeterRegistrySupplier(() -> internalDistributedSystem);

    MeterRegistry value = meterRegistrySupplier.get();

    assertThat(value).isNull();
  }

  @Test
  public void get_meterRegistryIsNull_expectNull() {
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    InternalCache internalCache = mock(InternalCache.class);
    when(internalDistributedSystem.getCache()).thenReturn(internalCache);
    when(internalCache.getMeterRegistry()).thenReturn(null);
    MeterRegistrySupplier meterRegistrySupplier =
        new MeterRegistrySupplier(() -> internalDistributedSystem);

    MeterRegistry value = meterRegistrySupplier.get();

    assertThat(value).isNull();
  }

  @Test
  public void get_meterRegistryExists_expectMeterRegistry() {
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    InternalCache internalCache = mock(InternalCache.class);
    MeterRegistry meterRegistry = mock(MeterRegistry.class);
    when(internalDistributedSystem.getCache()).thenReturn(internalCache);
    when(internalCache.getMeterRegistry()).thenReturn(meterRegistry);
    MeterRegistrySupplier meterRegistrySupplier =
        new MeterRegistrySupplier(() -> internalDistributedSystem);

    MeterRegistry value = meterRegistrySupplier.get();

    assertThat(value).isSameAs(meterRegistry);
  }
}
