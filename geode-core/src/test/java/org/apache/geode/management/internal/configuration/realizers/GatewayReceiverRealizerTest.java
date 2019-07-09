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

package org.apache.geode.management.internal.configuration.realizers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.runtime.GatewayReceiverInfo;

public class GatewayReceiverRealizerTest {
  private GatewayReceiverRealizer gatewayReceiverRealizer;

  @Before
  public void before() throws Exception {
    gatewayReceiverRealizer = new GatewayReceiverRealizer();
  }

  @Test
  public void generatesGatewayReceiverInfo() {
    GatewayReceiver gatewayReceiver = mock(GatewayReceiver.class);
    when(gatewayReceiver.getBindAddress()).thenReturn("localhost");
    when(gatewayReceiver.getHostnameForSenders()).thenReturn("localhost");
    when(gatewayReceiver.getPort()).thenReturn(321);
    when(gatewayReceiver.isRunning()).thenReturn(true);

    GatewayReceiverInfo actual =
        gatewayReceiverRealizer.generateGatewayReceiverInfo(gatewayReceiver);
    assertThat(actual.getBindAddress()).isEqualTo("localhost");
    assertThat(actual.getHostnameForSenders()).isEqualTo("localhost");
    assertThat(actual.getPort()).isEqualTo(321);
    assertThat(actual.isRunning()).isEqualTo(true);
  }

  @Test
  public void getGatewayReceiverInfoReturnsNullWhenListIsNull() {
    InternalCache cache = mock(InternalCache.class);
    when(cache.getGatewayReceivers()).thenReturn(null);

    assertThat(gatewayReceiverRealizer.get(null, cache)).isNull();
  }

  @Test
  public void getGatewayReceiverInfoReturnsNullWhenListIsEmpty() {
    InternalCache cache = mock(InternalCache.class);
    Set<GatewayReceiver> emptySet = Collections.emptySet();
    when(cache.getGatewayReceivers()).thenReturn(emptySet);

    assertThat(gatewayReceiverRealizer.get(null, cache)).isNull();
  }
}
