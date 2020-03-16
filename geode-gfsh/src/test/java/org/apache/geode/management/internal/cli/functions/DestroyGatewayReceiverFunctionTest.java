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
package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class DestroyGatewayReceiverFunctionTest {
  private DestroyGatewayReceiverFunction function;
  private FunctionContext<Void> context;
  private InternalCache cache;
  private ResultSender<Object> resultSender;
  private ArgumentCaptor<CliFunctionResult> resultCaptor;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    function = spy(DestroyGatewayReceiverFunction.class);
    context = mock(FunctionContext.class);
    cache = mock(InternalCache.class);
    resultSender = mock(ResultSender.class);
    resultCaptor = ArgumentCaptor.forClass(CliFunctionResult.class);
    when(context.getCache()).thenReturn(cache);
    when(context.getResultSender()).thenReturn(resultSender);
    when(cache.getDistributedSystem()).thenReturn(mock(DistributedSystem.class));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void getGatewayReceiversNull_doesNotThrowException() {
    when(cache.getGatewayReceivers()).thenReturn(null);
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).isEqualTo("Gateway receiver not found.");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void getGatewayReceiversNotFound_returnsStatusIgnored() {
    when(cache.getGatewayReceivers()).thenReturn(Collections.emptySet());
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.getStatus(true)).contains("IGNORED");
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).isEqualTo("Gateway receiver not found.");
  }

  @Test
  public void runningReceivers_stopCalledBeforeDestroying() {
    GatewayReceiver receiver = mock(GatewayReceiver.class);
    Set<GatewayReceiver> receivers = new HashSet<>();
    receivers.add(receiver);

    when(cache.getGatewayReceivers()).thenReturn(receivers);
    when(receiver.isRunning()).thenReturn(true);
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    verify(receiver).stop();
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.getStatus(true)).isEqualTo("OK");
  }

  @Test
  public void stoppedReceivers_stopNotCalledBeforeDestroying() {
    GatewayReceiver receiver = mock(GatewayReceiver.class);
    Set<GatewayReceiver> receivers = new HashSet<>();
    receivers.add(receiver);

    when(cache.getGatewayReceivers()).thenReturn(receivers);
    when(receiver.isRunning()).thenReturn(false);
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    verify(receiver, never()).stop();
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.getStatus(true)).isEqualTo("OK");
  }
}
