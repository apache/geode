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

import java.util.Set;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

public class CacheMeterRegistryFactory implements CompositeMeterRegistryFactory {

  @Override
  public CompositeMeterRegistry create(int systemId, String memberName, String hostName,
      Set<String> meterWhitelist) {
    CompositeMeterRegistry registry = new CompositeMeterRegistry();
    MeterRegistry whitelistedRegistry = new SimpleMeterRegistry();

    // meter name
    // whether to wire it to a stat
    // how to wire it to a stat

    MeterFilter acceptOnlyStatMeters =
        MeterFilter.denyUnless(id -> meterWhitelist.contains(id.getName()));
    whitelistedRegistry.config().meterFilter(acceptOnlyStatMeters);

    registry.add(whitelistedRegistry);

    MeterRegistry.Config registryConfig = registry.config();
    registryConfig.commonTags("cluster.id", String.valueOf(systemId));
    registryConfig.commonTags("member.name", memberName == null ? "" : memberName);
    registryConfig.commonTags("host.name", hostName == null ? "" : hostName);

    new JvmMemoryMetrics().bindTo(registry);

    return registry;
  }
}
