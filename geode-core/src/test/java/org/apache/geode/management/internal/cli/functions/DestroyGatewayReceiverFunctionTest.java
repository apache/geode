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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;

public class DestroyGatewayReceiverFunctionTest {
  private DestroyGatewayReceiverFunction function;
  private FunctionContext context;
  private InternalCache cache;
  private ResultSender resultSender;
  private ArgumentCaptor<CliFunctionResult> resultCaptor;

  @Before
  public void setUp() throws Exception {
    function = spy(DestroyGatewayReceiverFunction.class);
    context = mock(FunctionContext.class);
    cache = mock(InternalCache.class);
    resultSender = mock(ResultSender.class);
    resultCaptor = ArgumentCaptor.forClass(CliFunctionResult.class);
    when(context.getCache()).thenReturn(cache);
    when(context.getResultSender()).thenReturn(resultSender);
    when(cache.getDistributedSystem()).thenReturn(mock(DistributedSystem.class));
    when(context.getArguments()).thenReturn(false);
  }

  @Test
  public void getGatewayReceiversNull_doesNotThrowException() {
    when(cache.getGatewayReceivers()).thenReturn(null);
    when(context.getArguments()).thenReturn(false);
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).isEqualTo("Gateway receiver not found.");
  }

  @Test
  public void getGatewayReceiversNotFound_ifExists_false() {
    when(cache.getGatewayReceivers()).thenReturn(Collections.emptySet());
    when(context.getArguments()).thenReturn(false);
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).isEqualTo("Gateway receiver not found.");
  }

  @Test
  public void getGatewayReceiversNotFound_ifExists_true() {
    when(cache.getGatewayReceivers()).thenReturn(Collections.emptySet());
    when(context.getArguments()).thenReturn(true);
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).isEqualTo("Skipping: Gateway receiver not found.");
  }
}
