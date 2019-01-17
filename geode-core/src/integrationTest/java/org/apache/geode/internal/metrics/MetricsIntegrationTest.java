/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.metrics;

import static org.apache.geode.cache.RegionShortcut.LOCAL;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;

public class MetricsIntegrationTest {

  private InternalCache cache;
  private Region<String, String> region;

  @Rule
  public TestName testName = new TestName();
  private InternalDistributedSystem internalDistributedSystem;


  @Before
  public void setUp() {
    cache = (InternalCache) new CacheFactory().create();
    internalDistributedSystem = cache.getInternalDistributedSystem();

    String regionName = getClass().getSimpleName() + "_" + testName.getMethodName();
    region = cache.<String, String>createRegionFactory(LOCAL).create(regionName);
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void memberMeterRegistry_registerMeter_forwardsTheMeterToDownstreamRegistries() {
    MeterRegistry memberMeterRegistry = internalDistributedSystem.getMeterRegistry();
    MeterRegistry downstreamRegistry = new SimpleMeterRegistry();

    internalDistributedSystem.getMetricsSession().connectDownstreamRegistry(downstreamRegistry);

    String gaugeName = "test.region.size";

    Gauge.builder(gaugeName, region, Region::size)
        .tag("region.name", region.getName())
        .register(memberMeterRegistry);

    Gauge downstreamGauge = downstreamRegistry.find(gaugeName).gauge();
    assertThat(downstreamGauge)
        .as("downstream gauge")
        .isNotNull();

    assertThat(downstreamGauge.value())
        .as("value of downstream gauge before putting entries")
        .isEqualTo(region.size());

    int numberOfEntries = 100;
    for (int i = 1; i <= numberOfEntries; i++) {
      region.put("key-" + i, "value-" + i);
    }

    assertThat(downstreamGauge.value())
        .as("value of downstream gauge after putting entries")
        .isEqualTo(region.size());
  }
}
