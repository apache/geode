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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigExistsException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.categories.UnitTest;

public class CreateConnectionFunctionTest {

  private static final String CONNECTION_NAME = "theConnection";

  private ConnectorService.Connection connectionConfig;
  private FunctionContext<ConnectorService.Connection> context;
  private ResultSender<Object> resultSender;
  private JdbcConnectorService service;

  private CreateConnectionFunction function;

  @Before
  public void setUp() {
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    InternalCache cache = mock(InternalCache.class);
    DistributedSystem system = mock(DistributedSystem.class);
    DistributedMember distributedMember = mock(DistributedMember.class);
    service = mock(JdbcConnectorService.class);

    connectionConfig =
        new ConnectorService.Connection(CONNECTION_NAME, null, null, null, (String) null);;

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(distributedMember);
    when(context.getArguments()).thenReturn(connectionConfig);
    when(cache.getService(eq(JdbcConnectorService.class))).thenReturn(service);

    function = new CreateConnectionFunction();
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

    assertThat(copy).isNotSameAs(original).isInstanceOf(CreateConnectionFunction.class);
  }

  @Test
  public void createConnectionConfigReturnsConnectionName() throws Exception {
    function.createConnectionConfig(service, connectionConfig);

    verify(service, times(1)).createConnectionConfig(connectionConfig);
  }

  @Test
  public void createConnectionConfigThrowsIfConnectionExists() throws Exception {
    doThrow(ConnectionConfigExistsException.class).when(service)
        .createConnectionConfig(eq(connectionConfig));

    assertThatThrownBy(() -> function.createConnectionConfig(service, connectionConfig))
        .isInstanceOf(ConnectionConfigExistsException.class);

    verify(service, times(1)).createConnectionConfig(connectionConfig);
  }

  @Test
  public void executeCreatesConnection() throws Exception {
    function.execute(context);

    verify(service, times(1)).createConnectionConfig(connectionConfig);
  }

  @Test
  public void executeReportsErrorIfConnectionConfigExists() throws Exception {
    doThrow(ConnectionConfigExistsException.class).when(service)
        .createConnectionConfig(eq(connectionConfig));

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatusMessage())
        .contains(ConnectionConfigExistsException.class.getName());
  }

}
