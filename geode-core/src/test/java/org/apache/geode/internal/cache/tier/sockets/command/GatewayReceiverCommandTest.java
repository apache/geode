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
package org.apache.geode.internal.cache.tier.sockets.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.wan.GatewayReceiverStats;
import org.apache.geode.internal.security.SecurityService;

public class GatewayReceiverCommandTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private Message clientMessage;
  @Mock
  private ServerConnection serverConnection;
  @Mock
  private SecurityService securityService;
  @Mock
  private InternalCache cache;

  private long start;

  private MeterRegistry meterRegistry;

  @Before
  public void setUp() {
    start = 1;
    meterRegistry = new SimpleMeterRegistry();

    when(cache.getMeterRegistry()).thenReturn(meterRegistry);
    when(clientMessage.getPart(anyInt())).thenReturn(mock(Part.class));
    when(serverConnection.getCache()).thenReturn(cache);
    when(serverConnection.getCacheServerStats()).thenReturn(mock(GatewayReceiverStats.class));
    when(serverConnection.getResponseMessage()).thenReturn(mock(Message.class));
  }

  @Test
  public void cmdExecuteIncrementsEventsReceivedCounter() throws Exception {
    GatewayReceiverCommand command = new GatewayReceiverCommand();
    command.cmdExecute(clientMessage, serverConnection, securityService, start);

    Counter counter = meterRegistry.find("cache.gatewayreceiver.events.received")
        .counter();

    assertThat(counter).isNotNull();
    assertThat(counter.count()).isEqualTo(1L);
  }
}
