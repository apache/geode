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

import static org.apache.geode.management.internal.cli.functions.DescribeQueryServiceFunction.QUERY_SERVICE_NOT_FOUND_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl;
import org.apache.geode.cache.query.management.configuration.QueryConfigService;
import org.apache.geode.cache.util.TestMethodAuthorizer;

public class DescribeQueryServiceFunctionTest {
  private DescribeQueryServiceFunction function;
  private FunctionContext mockContext;
  private QueryConfigurationServiceImpl mockQueryServiceObject;
  private TestMethodAuthorizer mockAuthorizer;
  private Set<String> parameterSet;

  @Before
  public void setUp() {
    function = spy(new DescribeQueryServiceFunction());
    mockContext = mock(FunctionContext.class);

    mockQueryServiceObject = mock(QueryConfigurationServiceImpl.class);
    mockAuthorizer = mock(TestMethodAuthorizer.class);
    when(mockQueryServiceObject.getMethodAuthorizer()).thenReturn(mockAuthorizer);
    parameterSet = new HashSet<>();
    parameterSet.add("param1");
    parameterSet.add("param2");
    when(mockAuthorizer.getParameters()).thenReturn(parameterSet);
  }

  @Test
  public void executeFunctionReturnsErrorWhenQueryServiceIsNull() {
    doReturn(null).when(function).getQueryConfigurationService();

    CliFunctionResult result = function.executeFunction(mockContext);

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage()).isEqualTo(QUERY_SERVICE_NOT_FOUND_MESSAGE);
  }

  @Test
  public void executeFunctionReturnsSuccessWhenQueryServiceIsNotNull() {
    doReturn(mockQueryServiceObject).when(function).getQueryConfigurationService();
    QueryConfigService mockQueryConfigService = mock(QueryConfigService.class);
    doReturn(mockQueryConfigService).when(function)
        .translateQueryServiceObjectIntoQueryConfigService(mockQueryServiceObject);

    CliFunctionResult result = function.executeFunction(mockContext);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getResultObject()).isSameAs(mockQueryConfigService);
  }

  @Test
  public void translateQueryServiceObjectIntoQueryConfigServiceReturnsCorrectlyPopulatedObject() {
    QueryConfigService queryConfigService =
        function.translateQueryServiceObjectIntoQueryConfigService(mockQueryServiceObject);

    assertThat(queryConfigService.getMethodAuthorizer().getClassName())
        .isEqualTo(mockAuthorizer.getClass().getName());
    List<QueryConfigService.MethodAuthorizer.Parameter> parameters =
        queryConfigService.getMethodAuthorizer().getParameters();
    assertThat(parameters.size()).isEqualTo(parameterSet.size());
    assertThat(parameters.stream().map(p -> p.getParameterValue()).collect(Collectors.toSet()))
        .isEqualTo(parameterSet);
  }
}
