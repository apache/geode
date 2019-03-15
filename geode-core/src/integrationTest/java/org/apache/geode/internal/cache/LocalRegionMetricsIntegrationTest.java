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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;

public class LocalRegionMetricsIntegrationTest {

  private InternalCache cache;
  private CompositeMeterRegistry meterRegistry;
  private String regionName;

  @Before
  public void setUp() {
    cache = (InternalCache) new CacheFactory().set(ENABLE_TIME_STATISTICS, "true").create();
    meterRegistry = (CompositeMeterRegistry) cache.getMeterRegistry();
    meterRegistry.add(new SimpleMeterRegistry());
    regionName = "MyRegion";
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void putIncrementsPutTimer() {
    Region<String, String> region = cache.<String, String>createRegionFactory(REPLICATE)
        .create(regionName);

    Timer timer = meterRegistry.find("cache.region.operations")
        .tag("region.name", regionName)
        .tag("operation.type", "put")
        .timer();

    long count = timer.count();

    region.put("key", "value");

    assertThat(timer.count()).isEqualTo(count + 1);
  }

  @Test
  public void getIncrementsGetTimer() {
    Region<String, String> region = cache.<String, String>createRegionFactory(REPLICATE)
        .create(regionName);
    region.put("key", "value");

    Timer timer = meterRegistry.find("cache.region.operations")
        .tag("region.name", regionName)
        .tag("operation.type", "get")
        .timer();

    long count = timer.count();

    region.get("key");

    assertThat(timer.count()).isEqualTo(count + 1);
  }
}
