/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.cli.functions;

import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY;
import static org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction.AUTHORIZER_PARAMETERS_MESSAGE;
import static org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction.AUTHORIZER_UPDATED_MESSAGE;
import static org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction.DEPRECATED_PROPERTY_ERROR;
import static org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction.EMPTY_AUTHORIZER_ERROR;
import static org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction.SECURITY_NOT_ENABLED_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.internal.QueryConfigurationService;
import org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class AlterQueryServiceFunctionTest {
  private final String PARAMETER1 = "paramValue1";
  private final String PARAMETER2 = "paramValue2";
  private final String AUTHORIZER_NAME = "authorizerName";
  private final Set<String> parameterSet = new HashSet<>(Arrays.asList(PARAMETER1, PARAMETER2));
  private InternalCache mockCache;
  private AlterQueryServiceFunction function;
  private FunctionContext<Object[]> mockContext;
  private QueryConfigurationServiceImpl mockQueryConfigService;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @SuppressWarnings("deprecation")
  private void setAllowUntrustedMethodInvocationSystemProperty() {
    System.setProperty(ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY, "true");
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    mockCache = mock(InternalCache.class);
    mockQueryConfigService = mock(QueryConfigurationServiceImpl.class);
    when(mockCache.getService(QueryConfigurationService.class)).thenReturn(mockQueryConfigService);

    mockContext = mock(FunctionContext.class);
    when(mockContext.getCache()).thenReturn(mockCache);
    when(mockContext.getArguments()).thenReturn(new Object[] {true, AUTHORIZER_NAME, parameterSet});

    function = spy(new AlterQueryServiceFunction());
    doReturn(true).when(function).isSecurityEnabled();
  }

  @Test
  public void executeFunctionUpdatesMethodAuthorizerAndReturnsMessageWhenAuthorizerNameIsNotNull() {
    CliFunctionResult result = function.executeFunction(mockContext);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .contains(AUTHORIZER_UPDATED_MESSAGE + AUTHORIZER_NAME + AUTHORIZER_PARAMETERS_MESSAGE)
        .contains(PARAMETER1).contains(PARAMETER2);
    verify(mockQueryConfigService).updateMethodAuthorizer(mockCache, true, AUTHORIZER_NAME,
        parameterSet);
  }

  @Test
  public void executeFunctionUpdatesMethodAuthorizerAndReturnsMessageWhenAuthorizerNameIsNotNullAndThereAreNoParameters() {
    when(mockContext.getArguments()).thenReturn(new Object[] {false, AUTHORIZER_NAME, null});

    CliFunctionResult result = function.executeFunction(mockContext);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage()).isEqualTo(AUTHORIZER_UPDATED_MESSAGE + AUTHORIZER_NAME);
    verify(mockQueryConfigService).updateMethodAuthorizer(mockCache, false, AUTHORIZER_NAME,
        Collections.emptySet());
  }

  @Test
  public void executeFunctionReturnsErrorWhenAuthorizerNameIsNull() {
    when(mockContext.getArguments()).thenReturn(new Object[] {false, null, Collections.emptySet()});

    CliFunctionResult result = function.executeFunction(mockContext);
    verifyNoInteractions(mockQueryConfigService);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo(EMPTY_AUTHORIZER_ERROR);
  }

  @Test
  public void executeFunctionReturnsErrorWhenSecurityIsNotEnabled() {
    doReturn(false).when(function).isSecurityEnabled();

    CliFunctionResult result = function.executeFunction(mockContext);
    verifyNoInteractions(mockQueryConfigService);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo(SECURITY_NOT_ENABLED_MESSAGE);
  }

  @Test
  public void executeFunctionReturnsErrorWhenDeprecatedSystemPropertyIsSet() {
    setAllowUntrustedMethodInvocationSystemProperty();

    CliFunctionResult result = function.executeFunction(mockContext);
    verifyNoInteractions(mockQueryConfigService);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo(DEPRECATED_PROPERTY_ERROR);
  }

  @Test
  public void executeFunctionReturnsErrorWhenCacheIsNull() {
    when(mockContext.getCache()).thenReturn(null);

    CliFunctionResult result = function.executeFunction(mockContext);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
  }

  @Test
  public void executeFunctionReturnsErrorWhenUpdateMethodAuthorizerThrowsException() {
    doThrow(new RuntimeException()).when(mockQueryConfigService).updateMethodAuthorizer(any(),
        anyBoolean(), any(), any());

    CliFunctionResult result = function.executeFunction(mockContext);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
  }
}
