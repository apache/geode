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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigNotFoundException;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AlterConnectionFunctionTest {

  private static final String CONNECTION_NAME = "theConnection";

  private ConnectionConfiguration connectionConfig;
  private ConnectionConfiguration existingConfig;
  private ConnectionConfiguration configToAlter;
  private FunctionContext<ConnectionConfiguration> context;
  private ResultSender<Object> resultSender;
  private InternalJdbcConnectorService service;

  private AlterConnectionFunction function;

  @Before
  public void setUp() {
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    InternalCache cache = mock(InternalCache.class);
    DistributedSystem system = mock(DistributedSystem.class);
    DistributedMember distributedMember = mock(DistributedMember.class);
    service = mock(InternalJdbcConnectorService.class);

    connectionConfig = new ConnectionConfigBuilder().withName(CONNECTION_NAME).build();
    existingConfig = new ConnectionConfigBuilder().withName(CONNECTION_NAME).build();
    Map<String, String> parameters = new HashMap<>();
    parameters.put("key1", "value1");
    parameters.put("key2", "value2");
    configToAlter = new ConnectionConfiguration(CONNECTION_NAME, "originalUrl", "originalUser",
        "originalPassword", parameters);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(distributedMember);
    when(context.getArguments()).thenReturn(connectionConfig);
    when(cache.getService(eq(InternalJdbcConnectorService.class))).thenReturn(service);

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
    doThrow(ConnectionConfigNotFoundException.class).when(service)
        .replaceConnectionConfig(eq(connectionConfig));

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getErrorMessage()).contains(CONNECTION_NAME);
  }

  @Test
  public void alterConnectionConfigUrl() {
    ConnectionConfiguration newConfigValues =
        new ConnectionConfiguration(CONNECTION_NAME, "newUrl", null, null, null);

    ConnectionConfiguration alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);

    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("newUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("originalUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("originalPassword");
    Map<String, String> parameters = alteredConfig.getParameters();
    assertThat(parameters).containsOnly(entry("key1", "value1"), entry("key2", "value2"));
  }

  @Test
  public void alterConnectionConfigUser() {
    ConnectionConfiguration newConfigValues =
        new ConnectionConfiguration(CONNECTION_NAME, null, "newUser", null, null);

    ConnectionConfiguration alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);

    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("originalUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("newUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("originalPassword");
    assertThat(alteredConfig.getParameters()).containsOnly(entry("key1", "value1"),
        entry("key2", "value2"));
  }

  @Test
  public void alterConnectionConfigPassword() {
    ConnectionConfiguration newConfigValues =
        new ConnectionConfiguration(CONNECTION_NAME, null, null, "newPassword", null);

    ConnectionConfiguration alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);

    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("originalUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("originalUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("newPassword");
    assertThat(alteredConfig.getParameters()).containsOnly(entry("key1", "value1"),
        entry("key2", "value2"));
  }

  @Test
  public void alterConnectionConfigParameters() {
    Map<String, String> newParameters = new HashMap<>();
    newParameters.put("key1", "anotherValue1");
    newParameters.put("key8", "value8");
    ConnectionConfiguration newConfigValues =
        new ConnectionConfiguration(CONNECTION_NAME, null, null, null, newParameters);

    ConnectionConfiguration alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);

    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("originalUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("originalUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("originalPassword");
    assertThat(alteredConfig.getParameters()).containsOnly(entry("key1", "anotherValue1"),
        entry("key8", "value8"));
  }

  @Test
  public void alterConnectionConfigWithNothingToAlter() {
    ConnectionConfiguration newConfigValues =
        new ConnectionConfiguration(CONNECTION_NAME, null, null, null, null);

    ConnectionConfiguration alteredConfig =
        function.alterConnectionConfig(newConfigValues, configToAlter);

    assertThat(alteredConfig.getName()).isEqualTo(CONNECTION_NAME);
    assertThat(alteredConfig.getUrl()).isEqualTo("originalUrl");
    assertThat(alteredConfig.getUser()).isEqualTo("originalUser");
    assertThat(alteredConfig.getPassword()).isEqualTo("originalPassword");
    assertThat(alteredConfig.getParameters()).containsOnly(entry("key1", "value1"),
        entry("key2", "value2"));
  }
}
