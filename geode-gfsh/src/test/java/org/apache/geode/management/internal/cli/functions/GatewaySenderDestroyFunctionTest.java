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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.functions.CliFunctionResult;


public class GatewaySenderDestroyFunctionTest {

  private GatewaySenderDestroyFunction function;
  private FunctionContext<GatewaySenderDestroyFunctionArgs> context;
  private InternalCache cache;
  private ResultSender<Object> resultSender;
  private ArgumentCaptor<CliFunctionResult> resultCaptor;
  private GatewaySenderDestroyFunctionArgs args;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    function = spy(GatewaySenderDestroyFunction.class);
    context = mock(FunctionContext.class);
    cache = mock(InternalCache.class);
    args = mock(GatewaySenderDestroyFunctionArgs.class);
    resultSender = mock(ResultSender.class);
    when(context.getCache()).thenReturn(cache);
    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getArguments()).thenReturn(args);
    when(args.getId()).thenReturn("id");
    resultCaptor = ArgumentCaptor.forClass(CliFunctionResult.class);
    when(cache.getDistributedSystem()).thenReturn(mock(DistributedSystem.class));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void gateWaySenderNotFound_ifExists_false() {
    when(cache.getGatewaySender(any())).thenReturn(null);
    when(args.isIfExists()).thenReturn(false);
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).isEqualTo("Gateway sender id not found.");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void gateWaySenderNotFound_ifExists_true() {
    when(cache.getGatewaySender(any())).thenReturn(null);
    when(args.isIfExists()).thenReturn(true);
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).isEqualTo("Skipping: Gateway sender id not found.");
  }
}
