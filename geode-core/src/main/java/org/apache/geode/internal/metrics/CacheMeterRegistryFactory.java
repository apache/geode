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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

public class CacheMeterRegistryFactory implements CompositeMeterRegistryFactory {

  @Override
  public CompositeMeterRegistry create(int systemId, String memberName, String hostName) {
    GeodeCompositeMeterRegistry registry = new GeodeCompositeMeterRegistry();

    MeterRegistry.Config registryConfig = registry.config();
    registryConfig.commonTags("cluster.id", String.valueOf(systemId));
    registryConfig.commonTags("member.name", memberName == null ? "" : memberName);
    registryConfig.commonTags("host.name", hostName == null ? "" : hostName);
    registry.registerBinders();

    return registry;
  }
}
