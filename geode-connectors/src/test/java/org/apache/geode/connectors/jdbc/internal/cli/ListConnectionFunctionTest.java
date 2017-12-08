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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;

public class ListConnectionFunctionTest {

  private FunctionContext<Void> context;
  private ResultSender<Object> resultSender;
  private InternalJdbcConnectorService service;

  private ConnectionConfiguration connectionConfig1;
  private ConnectionConfiguration connectionConfig2;
  private ConnectionConfiguration connectionConfig3;

  private Set<ConnectionConfiguration> expected;

  private ListConnectionFunction function;

  @Before
  public void setUp() {
    InternalCache cache = mock(InternalCache.class);
    context = mock(FunctionContext.class);
    DistributedMember member = mock(DistributedMember.class);
    resultSender = mock(ResultSender.class);
    service = mock(InternalJdbcConnectorService.class);
    DistributedSystem system = mock(DistributedSystem.class);

    connectionConfig1 = mock(ConnectionConfiguration.class);
    connectionConfig2 = mock(ConnectionConfiguration.class);
    connectionConfig3 = mock(ConnectionConfiguration.class);

    expected = new HashSet<>();

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(member);
    when(cache.getService(eq(InternalJdbcConnectorService.class))).thenReturn(service);
    when(service.getConnectionConfigs()).thenReturn(expected);

    function = new ListConnectionFunction();
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

    assertThat(copy).isNotSameAs(original).isInstanceOf(ListConnectionFunction.class);
  }

  @Test
  public void getConnectionConfigsReturnsMultiple() {
    expected.add(connectionConfig1);
    expected.add(connectionConfig2);
    expected.add(connectionConfig3);

    ConnectionConfiguration[] actual = function.getConnectionConfigAsArray(service);

    assertThat(actual).containsExactlyInAnyOrder(connectionConfig1, connectionConfig2,
        connectionConfig3);
  }

  @Test
  public void getConnectionConfigsReturnsEmpty() {
    ConnectionConfiguration[] actual = function.getConnectionConfigAsArray(service);

    assertThat(actual).isEmpty();
  }

  @Test
  public void executeReturnsResultWithAllConfigs() {
    expected.add(connectionConfig1);
    expected.add(connectionConfig2);
    expected.add(connectionConfig3);

    function.execute(context);

    ArgumentCaptor<Object[]> argument = ArgumentCaptor.forClass(Object[].class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue()).containsExactlyInAnyOrder(connectionConfig1, connectionConfig2,
        connectionConfig3);
  }

  @Test
  public void executeReturnsEmptyResultForNoConfigs() {
    function.execute(context);

    ArgumentCaptor<Object[]> argument = ArgumentCaptor.forClass(Object[].class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue()).isEmpty();
  }

  @Test
  public void executeReturnsResultForExceptionWithoutMessage() {
    when(service.getConnectionConfigs()).thenThrow(new NullPointerException());

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getMessage()).contains(NullPointerException.class.getName());
  }

  @Test
  public void executeReturnsResultForExceptionWithMessage() {
    when(service.getConnectionConfigs()).thenThrow(new IllegalArgumentException("some message"));

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getMessage()).contains("some message")
        .doesNotContain(IllegalArgumentException.class.getName());
  }
}
