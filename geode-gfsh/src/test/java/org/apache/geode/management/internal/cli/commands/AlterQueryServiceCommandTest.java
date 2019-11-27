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

import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.AUTHORIZER_NAME;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.AUTHORIZER_PARAMETERS;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.COMMAND_NAME;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.FORCE_UPDATE;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.NO_MEMBERS_FOUND_MESSAGE;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.PARTIAL_FAILURE_MESSAGE;
import static org.apache.geode.management.internal.cli.commands.AlterQueryServiceCommand.SPLITTING_REGEX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.query.management.configuration.QueryConfigService;
import org.apache.geode.cache.query.security.RegExMethodAuthorizer;
import org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.AlterQueryServiceFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class AlterQueryServiceCommandTest {
  private Set<DistributedMember> members;
  private AlterQueryServiceCommand command;
  private QueryConfigService mockQueryConfigService;

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Before
  public void setup() {
    members = new HashSet<>();
    command = spy(new AlterQueryServiceCommand());
    mockQueryConfigService = mock(QueryConfigService.class);

    doReturn(members).when(command).findMembers(null, null);
  }

  private void mockMembers(int amount, String memberNamePreffix) {
    IntStream.range(0, amount).forEach(i -> {
      DistributedMember mockMember = mock(DistributedMember.class);
      when(mockMember.getName()).thenReturn(memberNamePreffix + i);
      members.add(mockMember);
    });
  }

  private String buildCommandString(String authorizerName, String authorizerParameters,
      Boolean forceUpdate) {
    StringBuilder commandBuilder = new StringBuilder(COMMAND_NAME);
    commandBuilder.append(" --").append(AUTHORIZER_NAME).append("=").append(authorizerName);

    if (authorizerParameters != null) {
      commandBuilder.append(" --").append(AUTHORIZER_PARAMETERS).append("=")
          .append(authorizerParameters);
    }

    if (forceUpdate != null) {
      commandBuilder.append(" --").append(FORCE_UPDATE).append("=").append(forceUpdate);
    }

    return commandBuilder.toString();
  }

  @Test
  public void commandParsesForceUpdateCorrectly() {
    GfshParseResult noForceUpdate =
        gfsh.parse(COMMAND_NAME + " --" + AUTHORIZER_NAME + "=MyAuthorizer");
    assertThat(noForceUpdate.getCommandName()).isEqualTo(COMMAND_NAME);
    assertThat(noForceUpdate.getParamValue(AUTHORIZER_NAME)).isEqualTo("MyAuthorizer");
    assertThat(noForceUpdate.getParamValue(FORCE_UPDATE)).isEqualTo(false);

    GfshParseResult forceUpdateWithoutValue =
        gfsh.parse(COMMAND_NAME + " --" + AUTHORIZER_NAME + "=MyAuthorizer --" + FORCE_UPDATE);
    assertThat(forceUpdateWithoutValue.getCommandName()).isEqualTo(COMMAND_NAME);
    assertThat(forceUpdateWithoutValue.getParamValue(AUTHORIZER_NAME)).isEqualTo("MyAuthorizer");
    assertThat(forceUpdateWithoutValue.getParamValue(FORCE_UPDATE)).isEqualTo(true);

    GfshParseResult forceUpdateAsFalse = gfsh.parse(
        COMMAND_NAME + " --" + AUTHORIZER_NAME + "=MyAuthorizer --" + FORCE_UPDATE + "=false");
    assertThat(forceUpdateAsFalse.getCommandName()).isEqualTo(COMMAND_NAME);
    assertThat(forceUpdateAsFalse.getParamValue(AUTHORIZER_NAME)).isEqualTo("MyAuthorizer");
    assertThat(forceUpdateAsFalse.getParamValue(FORCE_UPDATE)).isEqualTo(false);

    GfshParseResult forceUpdateAsTrue = gfsh.parse(
        COMMAND_NAME + " --" + AUTHORIZER_NAME + "=MyAuthorizer --" + FORCE_UPDATE + "=true");
    assertThat(forceUpdateAsTrue.getCommandName()).isEqualTo(COMMAND_NAME);
    assertThat(forceUpdateAsTrue.getParamValue(AUTHORIZER_NAME)).isEqualTo("MyAuthorizer");
    assertThat(forceUpdateAsTrue.getParamValue(FORCE_UPDATE)).isEqualTo(true);

  }

  @Test
  public void commandShouldReturnErrorWhenMandatoryParameterIsNotSet() {
    gfsh.executeAndAssertThat(command, COMMAND_NAME).statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void commandReturnsCorrectMessageWhenMethodAuthorizerIsSpecifiedAndNoMembersAreFound() {
    String commandString =
        buildCommandString(UnrestrictedMethodAuthorizer.class.getName(), null, null);

    gfsh.executeAndAssertThat(command, commandString).statusIsSuccess()
        .containsOutput(NO_MEMBERS_FOUND_MESSAGE);
  }

  @Test
  public void commandReturnsCorrectResultModelWhenMethodAuthorizerIsSpecified() {
    String memberName = "memberName";
    mockMembers(1, memberName);
    doReturn(mockQueryConfigService).when(command).getQueryConfigService();
    doNothing().when(command).populateMethodAuthorizer(any(), any(), any());
    List<CliFunctionResult> resultList = new ArrayList<>();
    resultList.add(new CliFunctionResult(memberName, CliFunctionResult.StatusState.OK, ""));
    doReturn(resultList).when(command).executeAndGetFunctionResult(any(), any(), any());
    String authorizerName = RegExMethodAuthorizer.class.getName();
    String parameterString = "^java.util.List..{4,8}$;^java.util.Set..{4,8}$";
    Set<String> expectedParameterSet =
        new HashSet<>(Arrays.asList(parameterString.split(SPLITTING_REGEX)));
    String commandString = buildCommandString(authorizerName, parameterString, null);

    gfsh.executeAndAssertThat(command, commandString).statusIsSuccess().containsOutput(memberName);
    verify(command).populateMethodAuthorizer(authorizerName, expectedParameterSet,
        mockQueryConfigService);
    verify(command).executeAndGetFunctionResult(any(AlterQueryServiceFunction.class),
        eq(new Object[] {false, authorizerName, expectedParameterSet}), eq(members));
  }

  @Test
  public void commandReturnsCorrectResultModelWhenCliResultListIsPartialFailure() {
    String memberName = "name";
    mockMembers(2, memberName);
    doReturn(mockQueryConfigService).when(command).getQueryConfigService();
    doNothing().when(command).populateMethodAuthorizer(any(), any(), any());
    List<CliFunctionResult> resultList = new ArrayList<>();
    resultList.add(new CliFunctionResult(memberName + 1, CliFunctionResult.StatusState.OK, ""));
    resultList.add(new CliFunctionResult(memberName + 2, CliFunctionResult.StatusState.ERROR, ""));
    doReturn(resultList).when(command).executeAndGetFunctionResult(any(), any(), any());
    String commandString =
        buildCommandString(UnrestrictedMethodAuthorizer.class.getName(), null, true);

    gfsh.executeAndAssertThat(command, commandString).statusIsSuccess()
        .containsOutput(PARTIAL_FAILURE_MESSAGE);
    verify(command).executeAndGetFunctionResult(any(AlterQueryServiceFunction.class),
        eq(new Object[] {true, UnrestrictedMethodAuthorizer.class.getName(), new HashSet<>()}),
        eq(members));
  }

  @Test
  public void populateMethodAuthorizerSetsAuthorizerNameAndParameters() {
    String authorizerName = "authorizerName";
    QueryConfigService queryConfigService = new QueryConfigService();
    Set<String> parameters = new HashSet<>(Arrays.asList("param1", "param2"));
    command.populateMethodAuthorizer(authorizerName, parameters, queryConfigService);

    QueryConfigService.MethodAuthorizer methodAuthorizer = queryConfigService.getMethodAuthorizer();
    assertThat(methodAuthorizer.getClassName()).isEqualTo(authorizerName);
    assertThat(methodAuthorizer.getParameters().size()).isEqualTo(parameters.size());
    assertThat(methodAuthorizer.getParameters().stream()
        .map(QueryConfigService.MethodAuthorizer.Parameter::getParameterValue)
        .collect(Collectors.toSet())).isEqualTo(parameters);
  }

  @Test
  public void getQueryConfigurationServiceReturnsExistingQueryConfigService() {
    CacheConfig mockCacheConfig = mock(CacheConfig.class);
    QueryConfigService mockQueryConfigService = mock(QueryConfigService.class);
    ConfigurationPersistenceService mockConfigPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(mockConfigPersistenceService).when(command).getConfigurationPersistenceService();
    when(mockConfigPersistenceService.getCacheConfig(null)).thenReturn(mockCacheConfig);
    when(mockCacheConfig.findCustomCacheElement(QueryConfigService.ELEMENT_ID,
        QueryConfigService.class)).thenReturn(mockQueryConfigService);

    assertThat(command.getQueryConfigService()).isSameAs(mockQueryConfigService);
  }

  @Test
  public void getQueryConfigurationServiceReturnsNewQueryConfigServiceWhenNoExistingServiceIsFound() {
    CacheConfig mockCacheConfig = mock(CacheConfig.class);
    ConfigurationPersistenceService mockConfigPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(mockConfigPersistenceService).when(command).getConfigurationPersistenceService();
    when(mockConfigPersistenceService.getCacheConfig(null)).thenReturn(mockCacheConfig);
    when(mockCacheConfig.findCustomCacheElement(QueryConfigService.ELEMENT_ID,
        QueryConfigService.class)).thenReturn(null);

    assertThat(command.getQueryConfigService()).isInstanceOf(QueryConfigService.class);
  }

  @Test
  public void updateConfigForGroupReplacesExistingElementWithNewElement() {
    CacheConfig mockCacheConfig = mock(CacheConfig.class);
    QueryConfigService newConfigService = mock(QueryConfigService.class);
    QueryConfigService existingConfigService = mock(QueryConfigService.class);
    List<CacheElement> elements = new ArrayList<>();
    elements.add(existingConfigService);
    when(mockCacheConfig.getCustomCacheElements()).thenReturn(elements);

    assertThat(elements.size()).isEqualTo(1);
    assertThat(command.updateConfigForGroup(null, mockCacheConfig, newConfigService)).isTrue();
    assertThat(elements.size()).isEqualTo(1);
    assertThat(elements.get(0)).isSameAs(newConfigService);
  }
}
