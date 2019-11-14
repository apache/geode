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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.AUTHORIZER_PARAMETERS;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.COMMAND_NAME;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.DEPRECATED_PROPERTY_ERROR;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.METHOD_AUTHORIZER_NAME;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.NO_ARGUMENTS_MESSAGE;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.NO_MEMBERS_FOUND_MESSAGE;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.PARAMETERS_WITHOUT_AUTHORIZER_MESSAGE;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.PARTIAL_FAILURE_MESSAGE;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.SECURITY_NOT_ENABLED_MESSAGE;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.SINGLE_AUTHORIZER_PARAMETER;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.SINGLE_PARAM_AND_PARAMETERS_SPECIFIED_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.query.management.configuration.QueryConfigService;
import org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class AlterQueryServiceCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private AlterQueryServiceCommand command;
  private Set<DistributedMember> mockMemberSet;
  private QueryConfigService mockQueryConfigService;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    command = spy(new AlterQueryServiceCommand());
    mockMemberSet = mock(HashSet.class);
    mockQueryConfigService = mock(QueryConfigService.class);
    doReturn(true).when(command).isSecurityEnabled();
  }

  @Test
  public void commandReturnsCorrectMessageWithNoOptions() {
    gfsh.executeAndAssertThat(command, COMMAND_NAME).statusIsError()
        .containsOutput(NO_ARGUMENTS_MESSAGE);
  }

  @Test
  public void commandReturnsErrorWhenAuthorizerParametersAreSpecifiedAndAuthorizerNameIsUnspecified() {
    gfsh.executeAndAssertThat(command,
        COMMAND_NAME + " --" + AUTHORIZER_PARAMETERS + "=param1,param2")
        .statusIsError().containsOutput(PARAMETERS_WITHOUT_AUTHORIZER_MESSAGE);
    gfsh.executeAndAssertThat(command,
        COMMAND_NAME + " --" + SINGLE_AUTHORIZER_PARAMETER + "=param1")
        .statusIsError().containsOutput(PARAMETERS_WITHOUT_AUTHORIZER_MESSAGE);
  }

  @Test
  public void commandReturnsErrorWhenAuthorizerParametersAndSingleParameterAreSpecifiedWithMethodAuthorizer() {
    gfsh.executeAndAssertThat(command,
        COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "=authorizerName" + " --"
            + AUTHORIZER_PARAMETERS + "=param1,param2" + " --" + SINGLE_AUTHORIZER_PARAMETER
            + "=param3")
        .statusIsError().containsOutput(SINGLE_PARAM_AND_PARAMETERS_SPECIFIED_MESSAGE);
  }

  @Test
  public void commandReturnsErrorWhenAuthorizerIsSpecifiedAndAllowUntrustedMethodInvocationIsTrue() {
    doReturn(mockMemberSet).when(command).findMembers(null, null);
    when(mockMemberSet.size()).thenReturn(1);
    try {
      System.setProperty(GEMFIRE_PREFIX + "QueryService.allowUntrustedMethodInvocation", "true");
      gfsh.executeAndAssertThat(command,
          COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "="
              + UnrestrictedMethodAuthorizer.class.getName())
          .statusIsError().containsOutput(DEPRECATED_PROPERTY_ERROR);
    } finally {
      System.clearProperty(GEMFIRE_PREFIX + "QueryService.allowUntrustedMethodInvocation");
    }
  }

  @Test
  public void commandReturnsErrorWhenAuthorizerSpecifiedAndSecurityNotEnabled() {
    doReturn(mockMemberSet).when(command).findMembers(null, null);
    when(mockMemberSet.size()).thenReturn(1);
    doReturn(false).when(command).isSecurityEnabled();

    gfsh.executeAndAssertThat(command,
        COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "="
            + UnrestrictedMethodAuthorizer.class.getName())
        .statusIsError().containsOutput(SECURITY_NOT_ENABLED_MESSAGE);
  }

  @Test
  public void commandReturnsCorrectMessageWhenMethodAuthorizerIsSpecifiedAndNoMembersAreFound() {
    doReturn(mockMemberSet).when(command).findMembers(null, null);
    when(mockMemberSet.size()).thenReturn(0);
    gfsh.executeAndAssertThat(command,
        COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "="
            + UnrestrictedMethodAuthorizer.class.getName())
        .statusIsSuccess().containsOutput(NO_MEMBERS_FOUND_MESSAGE);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void commandReturnsCorrectResultModelWhenMethodAuthorizerIsSpecified() {
    doReturn(mockMemberSet).when(command).findMembers(null, null);
    when(mockMemberSet.size()).thenReturn(1);

    doReturn(mockQueryConfigService).when(command).getQueryConfigService();
    doNothing().when(command).populateMethodAuthorizer(any(String.class), any(Set.class), any(
        QueryConfigService.class));

    List<CliFunctionResult> resultList = new ArrayList<>();
    String memberName = "memberName";
    resultList.add(new CliFunctionResult(memberName, CliFunctionResult.StatusState.OK, ""));
    doReturn(resultList).when(command).executeAndGetFunctionResult(any(
        AlterQueryServiceFunction.class), any(Object[].class), eq(mockMemberSet));

    gfsh.executeAndAssertThat(command,
        COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "="
            + UnrestrictedMethodAuthorizer.class.getName())
        .statusIsSuccess().containsOutput(memberName);
  }

  @Test
  public void commandReturnsCorrectResultModelWhenCliResultListIsPartialFailure() {
    doReturn(mockMemberSet).when(command).findMembers(null, null);
    when(mockMemberSet.size()).thenReturn(2);

    doReturn(mockQueryConfigService).when(command).getQueryConfigService();
    doNothing().when(command).populateMethodAuthorizer(any(String.class), any(Set.class), any(
        QueryConfigService.class));

    List<CliFunctionResult> resultList = new ArrayList<>();
    String memberName = "memberName";
    resultList.add(new CliFunctionResult(memberName + 1, CliFunctionResult.StatusState.OK, ""));
    resultList.add(new CliFunctionResult(memberName + 2, CliFunctionResult.StatusState.ERROR, ""));
    doReturn(resultList).when(command).executeAndGetFunctionResult(any(
        AlterQueryServiceFunction.class), any(Object[].class), eq(mockMemberSet));

    gfsh.executeAndAssertThat(command,
        COMMAND_NAME + " --" + METHOD_AUTHORIZER_NAME + "="
            + UnrestrictedMethodAuthorizer.class.getName())
        .statusIsSuccess().containsOutput(PARTIAL_FAILURE_MESSAGE);
  }

  @Test
  public void populateMethodAuthorizerSetsAuthorizerNameAndParameters() {
    String authorizerName = "authorizerName";
    Set<String> parameters = new HashSet<>();
    parameters.add("param1");
    parameters.add("param2");
    QueryConfigService queryConfigService = new QueryConfigService();

    command.populateMethodAuthorizer(authorizerName, parameters, queryConfigService);

    QueryConfigService.MethodAuthorizer methodAuthorizer =
        queryConfigService.getMethodAuthorizer();
    assertThat(methodAuthorizer.getClassName()).isEqualTo(authorizerName);
    assertThat(methodAuthorizer.getParameters().size()).isEqualTo(parameters.size());
    assertThat(methodAuthorizer.getParameters().stream().map(p -> p.getParameterValue())
        .collect(Collectors.toSet())).isEqualTo(parameters);
  }

  @Test
  public void getQueryConfigurationServiceReturnsExistingQueryConfigService() {
    ConfigurationPersistenceService mockConfigPersistenceService =
        mock(ConfigurationPersistenceService.class);
    CacheConfig mockCacheConfig = mock(CacheConfig.class);
    QueryConfigService mockQueryConfigService = mock(QueryConfigService.class);

    doReturn(mockConfigPersistenceService).when(command).getConfigurationPersistenceService();
    when(mockConfigPersistenceService.getCacheConfig(null)).thenReturn(mockCacheConfig);
    when(mockCacheConfig.findCustomCacheElement(
        QueryConfigService.ELEMENT_ID, QueryConfigService.class))
            .thenReturn(mockQueryConfigService);

    assertThat(command.getQueryConfigService()).isSameAs(mockQueryConfigService);
  }

  @Test
  public void getQueryConfigurationServiceReturnsNewQueryConfigServiceWhenNoExistingServiceIsFound() {
    ConfigurationPersistenceService mockConfigPersistenceService =
        mock(ConfigurationPersistenceService.class);
    CacheConfig mockCacheConfig = mock(CacheConfig.class);

    doReturn(mockConfigPersistenceService).when(command).getConfigurationPersistenceService();
    when(mockConfigPersistenceService.getCacheConfig(null)).thenReturn(mockCacheConfig);
    when(mockCacheConfig.findCustomCacheElement(
        QueryConfigService.ELEMENT_ID, QueryConfigService.class)).thenReturn(null);

    assertThat(command.getQueryConfigService())
        .isInstanceOf(QueryConfigService.class);
  }

  @Test
  public void updateConfigForGroupReplacesExistingElementWithNewElement() {
    CacheConfig mockCacheConfig = mock(CacheConfig.class);
    QueryConfigService existingConfigService = mock(QueryConfigService.class);
    QueryConfigService newConfigService = mock(QueryConfigService.class);

    List<CacheElement> elements = new ArrayList<>();
    elements.add(existingConfigService);

    when(mockCacheConfig.getCustomCacheElements()).thenReturn(elements);

    assertThat(elements.size()).isEqualTo(1);
    assertThat(command.updateConfigForGroup(null, mockCacheConfig, newConfigService)).isTrue();

    assertThat(elements.size()).isEqualTo(1);
    assertThat(elements.get(0)).isSameAs(newConfigService);
  }
}
