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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class DescribeConnectionFunctionTest {

  private static final String EXISTING_CONFIG = "existingConfig";

  private DescribeConnectionFunction function;
  private InternalJdbcConnectorService service;
  private FunctionContext<String> context;
  private ConnectionConfiguration connectionConfig;
  private ResultSender<Object> resultSender;

  @Before
  public void setUp() {
    function = new DescribeConnectionFunction();

    InternalCache cache = mock(InternalCache.class);
    context = mock(FunctionContext.class);
    service = mock(InternalJdbcConnectorService.class);
    connectionConfig = mock(ConnectionConfiguration.class);
    resultSender = mock(ResultSender.class);

    DistributedMember member = mock(DistributedMember.class);
    DistributedSystem system = mock(DistributedSystem.class);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getService(eq(InternalJdbcConnectorService.class))).thenReturn(service);
    when(service.getConnectionConfig(EXISTING_CONFIG)).thenReturn(connectionConfig);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(member);
  }

  @Test
  public void isHAReturnsFalse() {
    assertThat(function.isHA()).isFalse();
  }

  @Test
  public void getIdReturnsNameOfClass() {
    assertThat(function.getId()).isEqualTo(function.getClass().getName());
  }

  @Test
  public void serializes() {
    Serializable original = function;
    Object copy = SerializationUtils.clone(original);
    assertThat(copy).isNotSameAs(original).isInstanceOf(DescribeConnectionFunction.class);
  }

  @Test
  public void returnsResultWithCorrectConfig() {
    when(context.getArguments()).thenReturn(EXISTING_CONFIG);

    function.execute(context);

    ArgumentCaptor<Object> argument = ArgumentCaptor.forClass(Object.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue()).isSameAs(connectionConfig);
  }

  @Test
  public void returnNullWithNonExistingConfig() {
    when(context.getArguments()).thenReturn("non existing");

    function.execute(context);

    ArgumentCaptor<Object> argument = ArgumentCaptor.forClass(Object.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue()).isNull();

  }

  @Test
  public void executeReturnsResultForExceptionWithoutMessage() {
    when(service.getConnectionConfig(any())).thenThrow(new NullPointerException());

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getMessage()).contains(NullPointerException.class.getName());
  }

  @Test
  public void executeReturnsResultForExceptionWithMessage() {
    when(service.getConnectionConfig(any()))
        .thenThrow(new IllegalArgumentException("some message"));

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getMessage()).contains("some message")
        .doesNotContain(IllegalArgumentException.class.getName());
  }
}
