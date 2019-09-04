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

import static org.apache.geode.internal.util.ArrayUtils.asList;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.HashSet;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.Test;

import org.apache.geode.internal.cache.InternalCacheBuilder;

public class UserRegistryCacheMetricsSessionTest {
  @Test
  public void prepareBuilder_addsUserRegistriesToBuilder() {
    MeterRegistry userRegistry1 = mock(MeterRegistry.class, "user registry 1");
    MeterRegistry userRegistry2 = mock(MeterRegistry.class, "user registry 2");
    MeterRegistry userRegistry3 = mock(MeterRegistry.class, "user registry 3");

    Collection<MeterRegistry> userRegistries = asList(userRegistry1, userRegistry2, userRegistry3);

    CacheMetricsSession metricsSession =
        new UserRegistryCacheMetricsSession(new HashSet<>(userRegistries));

    InternalCacheBuilder cacheBuilder = mock(InternalCacheBuilder.class);

    metricsSession.prepareBuilder(cacheBuilder);

    verify(cacheBuilder, times(1))
        .addMeterSubregistry(same(userRegistry1));
    verify(cacheBuilder, times(1))
        .addMeterSubregistry(same(userRegistry2));
    verify(cacheBuilder, times(1))
        .addMeterSubregistry(same(userRegistry3));
  }
}
