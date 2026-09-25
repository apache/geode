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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.observation.ObservationRegistry;
import org.junit.Test;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;

public class ObservationRegistrySupplierTest {
  @Test
  public void get_internalDistributedSystemIsNull_expectNull() {
    ObservationRegistrySupplier observationRegistrySupplier = new ObservationRegistrySupplier(
        () -> null);

    ObservationRegistry value = observationRegistrySupplier.get();

    assertThat(value)
        .isNull();
  }

  @Test
  public void get_internalCacheIsNull_expectNull() {
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    when(internalDistributedSystem.getCache()).thenReturn(null);
    ObservationRegistrySupplier observationRegistrySupplier =
        new ObservationRegistrySupplier(() -> internalDistributedSystem);

    ObservationRegistry value = observationRegistrySupplier.get();

    assertThat(value)
        .isNull();
  }

  @Test
  public void get_observationRegistryIsNull_expectNull() {
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    InternalCache internalCache = mock(InternalCache.class);
    when(internalDistributedSystem.getCache()).thenReturn(internalCache);
    when(internalCache.getObservationRegistry()).thenReturn(null);
    ObservationRegistrySupplier observationRegistrySupplier =
        new ObservationRegistrySupplier(() -> internalDistributedSystem);

    ObservationRegistry value = observationRegistrySupplier.get();

    assertThat(value)
        .isNull();
  }

  @Test
  public void get_observationRegistryExists_expectActualObservationRegistry() {
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    InternalCache internalCache = mock(InternalCache.class);
    ObservationRegistry observationRegistry = mock(ObservationRegistry.class);
    when(internalDistributedSystem.getCache()).thenReturn(internalCache);
    when(internalCache.getObservationRegistry()).thenReturn(observationRegistry);
    ObservationRegistrySupplier observationRegistrySupplier =
        new ObservationRegistrySupplier(() -> internalDistributedSystem);

    ObservationRegistry value = observationRegistrySupplier.get();

    assertThat(value)
        .isSameAs(observationRegistry);
  }
}
