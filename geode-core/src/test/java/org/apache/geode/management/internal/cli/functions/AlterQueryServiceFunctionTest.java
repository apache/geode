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

import static org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction.AUTHORIZER_PARAMETERS_MESSAGE;
import static org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction.AUTHORIZER_UPDATED_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl;
import org.apache.geode.internal.cache.InternalCache;

public class AlterQueryServiceFunctionTest {

  private AlterQueryServiceFunction function = spy(new AlterQueryServiceFunction());
  @SuppressWarnings("unchecked")
  private FunctionContext<Object[]> mockContext = mock(FunctionContext.class);
  private final String AUTHORIZER_NAME = "authorizerName";
  private final String PARAMETER_VALUE1 = "paramValue1";
  private final String PARAMETER_VALUE2 = "paramValue2";
  private QueryConfigurationServiceImpl mockQueryConfigService =
      mock(QueryConfigurationServiceImpl.class);
  private InternalCache mockCache = mock(InternalCache.class);

  @Before
  public void setUp() {
    Set<String> parameterSet = new HashSet<>();
    parameterSet.add(PARAMETER_VALUE1);
    parameterSet.add(PARAMETER_VALUE2);
    Object[] arguments = new Object[] {AUTHORIZER_NAME, parameterSet};
    when(mockContext.getArguments()).thenReturn(arguments);
    when(mockContext.getCache()).thenReturn(mockCache);
    when(mockCache.getService(any())).thenReturn(mockQueryConfigService);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void executeFunctionUpdatesMethodAuthorizerAndReturnsMessageWhenAuthorizerNameIsNotNull() {
    CliFunctionResult result = function.executeFunction(mockContext);

    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage()).contains(AUTHORIZER_UPDATED_MESSAGE + AUTHORIZER_NAME
        + AUTHORIZER_PARAMETERS_MESSAGE).contains(PARAMETER_VALUE1).contains(PARAMETER_VALUE2);

    verify(mockQueryConfigService).updateMethodAuthorizer(eq(mockCache), eq(AUTHORIZER_NAME),
        any(Set.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void executeFunctionUpdatesMethodAuthorizerAndReturnsMessageWhenAuthorizerNameIsNotNullAndThereAreNoParameters() {
    when(mockContext.getArguments()).thenReturn(new Object[] {AUTHORIZER_NAME, null});
    CliFunctionResult result = function.executeFunction(mockContext);

    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage()).isEqualTo(AUTHORIZER_UPDATED_MESSAGE + AUTHORIZER_NAME);

    verify(mockQueryConfigService).updateMethodAuthorizer(eq(mockCache), eq(AUTHORIZER_NAME),
        any(Set.class));
  }

  @Test
  public void executeFunctionDoesNotUpdateMethodAuthorizerWhenAuthorizerNameIsNull() {
    Set<String> parameterSet = new HashSet<>();
    parameterSet.add(PARAMETER_VALUE1);
    parameterSet.add(PARAMETER_VALUE2);
    when(mockContext.getArguments()).thenReturn(new Object[] {null, parameterSet});
    CliFunctionResult result = function.executeFunction(mockContext);

    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    verify(mockQueryConfigService, times(0)).updateMethodAuthorizer(any(), any(), any());
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
        any(), any());

    CliFunctionResult result = function.executeFunction(mockContext);

    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
  }
}
