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
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigNotFoundException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AlterConnectionFunctionTest {

  private static final String CONNECTION_NAME = "theConnection";

  private ConnectorService.Connection connectionConfig;
  private ConnectorService.Connection existingConfig;
  private ConnectorService.Connection configToAlter;
  private FunctionContext<ConnectorService.Connection> context;
  private ResultSender<Object> resultSender;
  private JdbcConnectorService service;

  private AlterConnectionFunction function;

  @Before
  public void setUp() {
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    InternalCache cache = mock(InternalCache.class);
    DistributedSystem system = mock(DistributedSystem.class);
    DistributedMember distributedMember = mock(DistributedMember.class);
    service = mock(JdbcConnectorService.class);

    connectionConfig =
        new ConnectorService.Connection(CONNECTION_NAME, null, null, null, (String) null);
    existingConfig =
        new ConnectorService.Connection(CONNECTION_NAME, null, null, null, (String) null);
    String[] parameters = {"key1:value1", "key2:value2"};
    configToAlter = new ConnectorService.Connection(CONNECTION_NAME, "originalUrl", "originalUser",
        "originalPassword", parameters);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(distributedMember);
    when(context.getArguments()).thenReturn(connectionConfig);
    when(cache.getService(eq(JdbcConnectorService.class))).thenReturn(service);

    function = new AlterConnectionFunction();
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

    assertThat(copy).isNotSameAs(original).isInstanceOf(AlterConnectionFunction.class);
  }

  @Test
  public void alterConnectionConfigThrowsConnectionNotFound() {
    AlterConnectionFunction alterFunction = mock(AlterConnectionFunction.class);
    doThrow(ConnectionConfigNotFoundException.class).when(alterFunction)
        .alterConnectionConfig(any(), any());

    assertThatThrownBy(() -> alterFunction.alterConnectionConfig(connectionConfig, existingConfig))
        .isInstanceOf(ConnectionConfigNotFoundException.class);
  }

  @Test
  public void executeInvokesReplaceOnService() throws Exception {
    when(service.getConnectionConfig(CONNECTION_NAME)).thenReturn(existingConfig);
    function.execute(context);

    verify(service, times(1)).replaceConnectionConfig(any());
  }

  @Test
  public void executeReportsErrorIfConnectionConfigNotFound() throws Exception {
    IgnoredException ignoredException =
        IgnoredException.addIgnoredException(ConnectionConfigNotFoundException.class.getName());

    doThrow(ConnectionConfigNotFoundException.class).when(service)
        .replaceConnectionConfig(eq(connectionConfig));

    try {
      function.execute(context);
    } finally {
      ignoredException.remove();
    }

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatus()).contains(CONNECTION_NAME);
  }

  @Test
  public void alterConnectionConfigUrl() {
    ConnectorService.Connection newConfigValues =
        new ConnectorService.Connection(CONNECTION_NAME, "newUrl", null, null, (String[]) null);

    ConnectorService.Connection alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);

    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("newUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("originalUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("originalPassword");
    Map<String, String> parameters = alteredConfig.getParameterMap();
    assertThat(parameters).containsOnly(entry("key1", "value1"), entry("key2", "value2"));
  }

  @Test
  public void alterConnectionConfigUser() {
    ConnectorService.Connection newConfigValues =
        new ConnectorService.Connection(CONNECTION_NAME, null, "newUser", null, (String[]) null);

    ConnectorService.Connection alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);

    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("originalUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("newUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("originalPassword");
    assertThat(alteredConfig.getParameterMap()).containsOnly(entry("key1", "value1"),
        entry("key2", "value2"));
  }

  @Test
  public void alterConnectionConfigPassword() {
    ConnectorService.Connection newConfigValues = new ConnectorService.Connection(CONNECTION_NAME,
        null, null, "newPassword", (String[]) null);

    ConnectorService.Connection alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);

    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("originalUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("originalUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("newPassword");
    assertThat(alteredConfig.getParameterMap()).containsOnly(entry("key1", "value1"),
        entry("key2", "value2"));
  }

  @Test
  public void alterConnectionConfigParameters() {
    String[] newParameters = new String[] {"key1:anotherValue1", "key8:value8"};
    ConnectorService.Connection newConfigValues =
        new ConnectorService.Connection(CONNECTION_NAME, null, null, null, newParameters);

    ConnectorService.Connection alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);

    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("originalUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("originalUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("originalPassword");
    assertThat(alteredConfig.getParameterMap()).containsOnly(entry("key1", "anotherValue1"),
        entry("key8", "value8"));
  }

  @Test
  public void alterConnectionConfigWithNothingToAlter() {
    ConnectorService.Connection newConfigValues =
        new ConnectorService.Connection(CONNECTION_NAME, null, null, null, (String[]) null);

    ConnectorService.Connection alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);

    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("originalUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("originalUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("originalPassword");
    assertThat(alteredConfig.getParameterMap()).containsOnly(entry("key1", "value1"),
        entry("key2", "value2"));
  }

  @Test
  public void alterConnectionRemoveParameters() {
    ConnectorService.Connection newConfigValues =
        new ConnectorService.Connection(CONNECTION_NAME, null, null, null, new String[0]);

    ConnectorService.Connection alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);
    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("originalUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("originalUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("originalPassword");
    assertThat(alteredConfig.getParameterMap()).isEmpty();
  }

  @Test
  public void alterConnectionRemoveParametersWithEmptyString() {
    ConnectorService.Connection newConfigValues =
        new ConnectorService.Connection(CONNECTION_NAME, null, null, null, new String[] {""});

    ConnectorService.Connection alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);
    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("originalUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("originalUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("originalPassword");
    assertThat(alteredConfig.getParameterMap()).isEmpty();
  }
}
