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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.internal.cache.InternalCacheServer.EndpointType;
import org.apache.geode.internal.cache.wan.GatewayReceiverEndpoint;
import org.apache.geode.internal.cache.wan.GatewayReceiverMetrics;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.WanTest;

@Category(WanTest.class)
public class GatewayReceiverEndpointTest {

  private InternalCache cache;
  private SecurityService securityService;
  private GatewayReceiver gatewayReceiver;
  private GatewayReceiverMetrics gatewayReceiverMetrics;

  @Before
  public void setUp() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    cache = mock(InternalCache.class);
    securityService = mock(SecurityService.class);
    gatewayReceiver = mock(GatewayReceiver.class);
    gatewayReceiverMetrics = new GatewayReceiverMetrics(meterRegistry);
  }

  @Test
  public void isGatewayEndpoint() {
    GatewayReceiverEndpoint server = new GatewayReceiverEndpoint(cache, securityService,
        gatewayReceiver, gatewayReceiverMetrics);

    assertThat(server.getEndpointType()).isSameAs(EndpointType.GATEWAY);
  }
}
