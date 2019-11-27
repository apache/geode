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
package org.apache.geode.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Test;

import org.apache.geode.internal.cache.InternalCache;

public class CacheFactoryIntegrationTest {

  private InternalCache cache;

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void recreateDoesNotThrowDistributedSystemDisconnectedException() {
    cache = (InternalCache) new CacheFactory().set(LOCATORS, "").create();

    cache.close();

    cache = (InternalCache) new CacheFactory().set(LOCATORS, "").create();
  }

  @Test
  public void addMeterSubregistryAddsSubregistryToCacheMeterRegistry() {
    MeterRegistry theMeterRegistry = new SimpleMeterRegistry();

    cache = (InternalCache) new CacheFactory()
        .addMeterSubregistry(theMeterRegistry)
        .create();

    assertThat(getCacheMeterRegistry(cache).getRegistries())
        .containsExactly(theMeterRegistry);
  }

  @Test
  public void addMeterSubregistryAddsMultipleSubregistriesToCacheMeterRegistry() {
    MeterRegistry firstMeterRegistry = new SimpleMeterRegistry();
    MeterRegistry secondMeterRegistry = new SimpleMeterRegistry();
    MeterRegistry thirdMeterRegistry = new SimpleMeterRegistry();

    cache = (InternalCache) new CacheFactory()
        .addMeterSubregistry(firstMeterRegistry)
        .addMeterSubregistry(secondMeterRegistry)
        .addMeterSubregistry(thirdMeterRegistry)
        .create();

    assertThat(getCacheMeterRegistry(cache).getRegistries())
        .containsExactlyInAnyOrder(firstMeterRegistry, secondMeterRegistry, thirdMeterRegistry);
  }

  @Test
  public void addMeterSubregistryThrowsNullPointerExceptionIfTheGivenRegistryIsNull() {
    CacheFactory cacheFactory = new CacheFactory();

    assertThatThrownBy(() -> cacheFactory.addMeterSubregistry(null))
        .isInstanceOf(NullPointerException.class);
  }

  private CompositeMeterRegistry getCacheMeterRegistry(InternalCache cache) {
    return (CompositeMeterRegistry) cache.getMeterRegistry();
  }
}
