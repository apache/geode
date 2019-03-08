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
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import org.apache.geode.annotations.VisibleForTesting;

/**
 * Creates {@code CompositeMeterRegistry} and configures commonTags.
 */
public interface CompositeMeterRegistryFactory {

  @VisibleForTesting
  String CLUSTER_ID_TAG = "ClusterId";
  @VisibleForTesting
  String MEMBER_NAME_TAG = "MemberName";
  @VisibleForTesting
  String HOST_NAME_TAG = "HostName";

  default CompositeMeterRegistry create(int systemId, String memberName, String hostName) {
    CompositeMeterRegistry registry = new CompositeMeterRegistry();

    MeterRegistry.Config registryConfig = registry.config();
    registryConfig.commonTags(CLUSTER_ID_TAG, String.valueOf(systemId));
    registryConfig.commonTags(MEMBER_NAME_TAG, memberName == null ? "" : memberName);
    registryConfig.commonTags(HOST_NAME_TAG, hostName == null ? "" : hostName);

    return registry;
  }
}
