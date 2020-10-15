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
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.functions.CliFunctionResult;


public class AlterGatewaySenderFunctionTest {

  private AlterGatewaySenderFunction function;
  private FunctionContext<GatewaySenderFunctionArgs> context;
  private InternalCache cache;
  private ResultSender<Object> resultSender;
  private ArgumentCaptor<CliFunctionResult> resultCaptor;
  private GatewaySenderFunctionArgs args;
  private GatewaySender sender;
  private ArgumentCaptor<GatewaySenderAttributes> updateCapture1;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    sender = mock(GatewaySender.class);
    function = spy(AlterGatewaySenderFunction.class);
    context = mock(FunctionContext.class);
    cache = mock(InternalCache.class);
    args = mock(GatewaySenderFunctionArgs.class);
    resultSender = mock(ResultSender.class);
    when(context.getCache()).thenReturn(cache);
    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getArguments()).thenReturn(args);
    when(context.getMemberName()).thenReturn("member");
    when(args.getId()).thenReturn("id");
    resultCaptor = ArgumentCaptor.forClass(CliFunctionResult.class);
    when(cache.getDistributedSystem()).thenReturn(mock(DistributedSystem.class));
    updateCapture1 = ArgumentCaptor.forClass(GatewaySenderAttributes.class);
    when(sender.getId()).thenReturn("id");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void gateWaySenderNotFound() {
    when(cache.getGatewaySender(any())).thenReturn(null);
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getThrowable()).isInstanceOf(EntityNotFoundException.class);
    assertThat(result.getMessage()).isNull();
  }

  @Test
  @SuppressWarnings({"unchecked", "deprecation"})
  public void gateWaySenderNoAttributesToUpdate() {
    when(cache.getGatewaySender("id")).thenReturn(sender);
    when(args.getAlertThreshold()).thenReturn(null);
    when(args.getBatchSize()).thenReturn(null);
    when(args.getBatchTimeInterval()).thenReturn(null);
    when(args.getGatewayEventFilter()).thenReturn(null);
    when(args.getGatewayTransportFilter()).thenReturn(null);
    when(args.mustGroupTransactionEvents()).thenReturn(null);

    function.execute(context);
    verify(sender).update(updateCapture1.capture());
    GatewaySenderAttributes attr = updateCapture1.getValue();
    assertThat(attr.modifyAlertThreshold()).isFalse();
    assertThat(attr.modifyBatchSize()).isFalse();
    assertThat(attr.modifyBatchTimeInterval()).isFalse();

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).isEqualTo("GatewaySender id is updated on member member");
  }

  @Test
  @SuppressWarnings({"unchecked", "deprecation"})
  public void gateWaySender3AttributesUpdated() {
    when(cache.getGatewaySender("id")).thenReturn(sender);
    when(args.getAlertThreshold()).thenReturn(200);
    when(args.getBatchSize()).thenReturn(50);
    when(args.getBatchTimeInterval()).thenReturn(150);
    when(args.getGatewayEventFilter()).thenReturn(null);
    when(args.getGatewayTransportFilter()).thenReturn(null);
    when(args.mustGroupTransactionEvents()).thenReturn(null);

    function.execute(context);
    verify(sender).update(updateCapture1.capture());
    GatewaySenderAttributes attr = updateCapture1.getValue();
    assertThat(attr.modifyAlertThreshold()).isTrue();
    assertThat(attr.getAlertThreshold()).isEqualTo(200);
    assertThat(attr.modifyBatchSize()).isTrue();
    assertThat(attr.getBatchSize()).isEqualTo(50);
    assertThat(attr.modifyBatchTimeInterval()).isTrue();
    assertThat(attr.getBatchTimeInterval()).isEqualTo(150);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).isEqualTo("GatewaySender id is updated on member member");
  }
}
