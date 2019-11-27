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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.cli.commands.DescribeQueryServiceCommand.ALL_METHODS_ALLOWED;
import static org.apache.geode.management.internal.cli.commands.DescribeQueryServiceCommand.AUTHORIZER_CLASS_NAME;
import static org.apache.geode.management.internal.cli.commands.DescribeQueryServiceCommand.COMMAND_NAME;
import static org.apache.geode.management.internal.cli.commands.DescribeQueryServiceCommand.FUNCTION_FAILED_ON_ALL_MEMBERS;
import static org.apache.geode.management.internal.cli.commands.DescribeQueryServiceCommand.NO_CLUSTER_CONFIG_AND_NO_MEMBERS;
import static org.apache.geode.management.internal.cli.commands.DescribeQueryServiceCommand.QUERY_SERVICE_DATA_SECTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.cache.query.management.configuration.QueryConfigService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.functions.DescribeQueryServiceFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class DescribeQueryServiceCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private DescribeQueryServiceCommand command;
  private Set<DistributedMember> memberSet;
  private QueryConfigService queryConfigService;

  @Before
  public void setup() {
    command = Mockito.spy(new DescribeQueryServiceCommand());
    memberSet = new HashSet<>();
    queryConfigService = new QueryConfigService();
    doReturn(memberSet).when(command).findMembers(null, null);
    doReturn(queryConfigService).when(command).getQueryConfigService();
    doReturn(true).when(command).isSecurityEnabled();
  }

  @Test
  public void executeConstructsResultModelWhenClusterConfigContainsQueryConfigurationService() {
    doReturn(null).when(command).constructResultModelFromQueryService(queryConfigService);
    command.execute();

    verify(command).constructResultModelFromQueryService(queryConfigService);
  }

  @Test
  public void executeConstructsResultModelOnFirstSuccessfulFunctionExecutionWhenClusterConfigDoesNotContainQueryConfigurationService() {
    doReturn(null).when(command).getQueryConfigService();
    CliFunctionResult mockResult = mock(CliFunctionResult.class);
    doReturn(mockResult).when(command).executeFunctionAndGetFunctionResult(any(
        DescribeQueryServiceFunction.class), eq(null), any(DistributedMember.class));
    when(mockResult.getResultObject()).thenReturn(queryConfigService);

    when(mockResult.isSuccessful()).thenReturn(true);

    memberSet.add(mock(DistributedMember.class));
    memberSet.add(mock(DistributedMember.class));
    command.execute();

    verify(command, times(1)).executeFunctionAndGetFunctionResult(any(
        DescribeQueryServiceFunction.class), eq(null), any(DistributedMember.class));
    verify(command).constructResultModelFromQueryService(queryConfigService);
  }

  @Test
  public void executeConstructsResultModelWhenClusterConfigDoesNotContainQueryConfigurationServiceAndFirstMemberDoesNotReturnSuccess() {
    doReturn(null).when(command).getQueryConfigService();
    CliFunctionResult mockResult = mock(CliFunctionResult.class);
    doReturn(mockResult).when(command).executeFunctionAndGetFunctionResult(any(
        DescribeQueryServiceFunction.class), eq(null), any(DistributedMember.class));
    when(mockResult.getResultObject()).thenReturn(queryConfigService);

    when(mockResult.isSuccessful()).thenReturn(false).thenReturn(true);

    memberSet.add(mock(DistributedMember.class));
    memberSet.add(mock(DistributedMember.class));
    command.execute();

    verify(command, times(memberSet.size())).executeFunctionAndGetFunctionResult(any(
        DescribeQueryServiceFunction.class), eq(null), any(DistributedMember.class));
    verify(command).constructResultModelFromQueryService(queryConfigService);
  }

  @Test
  public void commandReturnsErrorWhenClusterConfigDoesNotContainQueryConfigurationServiceAndFunctionFailsOnAllMembers() {
    doReturn(null).when(command).getQueryConfigService();
    CliFunctionResult mockResult = mock(CliFunctionResult.class);
    doReturn(mockResult).when(command).executeFunctionAndGetFunctionResult(any(
        DescribeQueryServiceFunction.class), eq(null), any(DistributedMember.class));
    when(mockResult.isSuccessful()).thenReturn(false);
    when(mockResult.getResultObject()).thenReturn(queryConfigService);

    memberSet.add(mock(DistributedMember.class));
    memberSet.add(mock(DistributedMember.class));

    gfsh.executeAndAssertThat(command, COMMAND_NAME).statusIsError()
        .containsOutput(FUNCTION_FAILED_ON_ALL_MEMBERS);
    verify(command, times(2)).executeFunctionAndGetFunctionResult(any(
        DescribeQueryServiceFunction.class), eq(null), any(DistributedMember.class));
  }

  @Test
  public void commandReturnsErrorWhenClusterConfigDoesNotContainQueryConfigurationServiceAndNoMembersFound() {
    doReturn(null).when(command).getQueryConfigService();

    gfsh.executeAndAssertThat(command, COMMAND_NAME).statusIsError()
        .containsOutput(NO_CLUSTER_CONFIG_AND_NO_MEMBERS);
  }

  @Test
  public void addMethodAuthorizerToResultModelPopulatesResultWithCorrectValuesAndSectionsWhenAuthorizerIsFoundAndSecurityIsEnabled() {
    final int numberOfParameters = 2;
    String methodAuthClassName = "Class.Package.Name";
    QueryConfigService.MethodAuthorizer methodAuthorizer =
        new QueryConfigService.MethodAuthorizer();
    methodAuthorizer.setClassName(methodAuthClassName);

    List<QueryConfigService.MethodAuthorizer.Parameter> params = new ArrayList<>();
    for (int i = 0; i < numberOfParameters; i++) {
      QueryConfigService.MethodAuthorizer.Parameter param =
          new QueryConfigService.MethodAuthorizer.Parameter();
      param.setParameterValue("param" + i);
      params.add(param);
    }

    methodAuthorizer.setParameters(params);
    queryConfigService.setMethodAuthorizer(methodAuthorizer);

    ResultModel result = new ResultModel();
    command.addMethodAuthorizerToResultModel(queryConfigService, result);

    assertThat(result.getDataSection(QUERY_SERVICE_DATA_SECTION)).isNotNull();
    assertThat(result.getDataSection(QUERY_SERVICE_DATA_SECTION).getContent().size())
        .isEqualTo(1);
    assertThat(result.getDataSection(QUERY_SERVICE_DATA_SECTION).getContent())
        .containsValue(methodAuthClassName);
  }

  @Test
  public void addMethodAuthorizerToResultModelPopulatesResultWithCorrectSectionWhenSecurityIsDisabled() {
    doReturn(false).when(command).isSecurityEnabled();

    QueryConfigService.MethodAuthorizer methodAuthorizer =
        new QueryConfigService.MethodAuthorizer();
    queryConfigService.setMethodAuthorizer(methodAuthorizer);
    ResultModel result = new ResultModel();
    command.addMethodAuthorizerToResultModel(queryConfigService, result);

    Map<String, String> dataSection =
        result.getDataSection(QUERY_SERVICE_DATA_SECTION).getContent();

    assertThat(dataSection.get(AUTHORIZER_CLASS_NAME)).isEqualTo(ALL_METHODS_ALLOWED);
  }
}
