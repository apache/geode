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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.configuration.JndiBindingsType.JndiBinding;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CreateJndiBindingFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class CreateJndiBindingCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private CreateJndiBindingCommand command;
  JndiBindingsType.JndiBinding binding;
  List<JndiBindingsType.JndiBinding> bindings;

  private static String COMMAND = "create jndi-binding ";

  @Before
  public void setUp() throws Exception {
    InternalCache cache = mock(InternalCache.class);
    when(cache.getDistributionManager()).thenReturn(mock(DistributionManager.class));
    command = spy(CreateJndiBindingCommand.class);
    command.setCache(cache);

    binding = new JndiBindingsType.JndiBinding();
    binding.setJndiName("name");
    bindings = new ArrayList<>();
  }

  @Test
  public void missingMandatory() {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void wrongType() {
    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=NOTSOSIMPLE --name=name --jdbc-driver-class=driver --connection-url=url ")
        .statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void configPropertyIsProperlyParsed() {
    GfshParseResult result = gfsh.parse(COMMAND
        + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url "
        + "--datasource-config-properties={'name':'name1','type':'type1','value':'value1'},{'name':'name2','type':'type2','value':'value2'}");

    JndiBindingsType.JndiBinding.ConfigProperty[] configProperties =
        (JndiBindingsType.JndiBinding.ConfigProperty[]) result
            .getParamValue("datasource-config-properties");
    assertThat(configProperties).hasSize(2);
    assertThat(configProperties[0].getValue()).isEqualTo("value1");
    assertThat(configProperties[1].getValue()).isEqualTo("value2");
  }

  @Test
  public void verifyJdbcDriverClassNotRequired() {
    gfsh.executeAndAssertThat(command, COMMAND + " --type=SIMPLE --name=name --connection-url=url")
        .statusIsSuccess();
  }

  @Test
  public void verifyJdbcTypeDefaultsToSimple() {
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name --connection-url=url")
        .statusIsSuccess();
  }

  @Test
  public void verifyUrlAllowedAsAliasForConnectionUrl() {
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name --url=url").statusIsSuccess();
  }

  @Test
  public void returnsErrorIfBindingAlreadyExistsAndIfUnspecified() {
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);
    when(cacheConfig.getJndiBindings()).thenReturn(bindings);
    bindings.add(binding);

    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());

    gfsh.executeAndAssertThat(command,
        COMMAND + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url")
        .statusIsError().containsOutput("already exists.");
  }

  @Test
  public void skipsIfBindingAlreadyExistsAndIfSpecified() {
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);

    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());
    when(cacheConfig.getJndiBindings()).thenReturn(bindings);
    bindings.add(binding);

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --if-not-exists")
        .statusIsSuccess().containsOutput("Skipping");
  }

  @Test
  public void skipsIfBindingAlreadyExistsAndIfSpecifiedTrue() {
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);
    when(cacheConfig.getJndiBindings()).thenReturn(bindings);
    bindings.add(binding);

    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --if-not-exists=true")
        .statusIsSuccess().containsOutput("Skipping");
  }

  @Test
  public void returnsErrorIfBindingAlreadyExistsAndIfSpecifiedFalse() {
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);
    when(cacheConfig.getJndiBindings()).thenReturn(bindings);
    bindings.add(binding);

    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --if-not-exists=false")
        .statusIsError().containsOutput("already exists.");
  }

  @Test
  public void whenNoMembersFoundAndNoClusterConfigServiceRunningThenError() {

    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationPersistenceService();

    gfsh.executeAndAssertThat(command,
        COMMAND + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url")
        .statusIsSuccess().containsOutput("No members found").containsOutput(
            "Cluster configuration service is not running. Configuration change is not persisted.");
  }

  @Test
  public void whenNoMembersFoundAndClusterConfigRunningThenUpdateClusterConfig() {
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);

    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());

    doAnswer(invocation -> {
      UnaryOperator<CacheConfig> mutator = invocation.getArgument(1);
      mutator.apply(cacheConfig);
      return null;
    }).when(clusterConfigService).updateCacheConfig(any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url")
        .statusIsSuccess().containsOutput("No members found.")
        .containsOutput("Cluster configuration for group 'cluster' is updated");

    verify(clusterConfigService).updateCacheConfig(any(), any());
    verify(command).updateConfigForGroup(eq("cluster"), eq(cacheConfig), any());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void whenMembersFoundAndNoClusterConfigRunningThenOnlyInvokeFunction() {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result = new CliFunctionResult("server1", true,
        "Tried creating jndi binding \"name\" on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --datasource-config-properties={'name':'name1','type':'type1','value':'value1'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status", "OK").tableHasColumnOnlyWithValues("Message",
            "Tried creating jndi binding \"name\" on \"server1\"");

    ArgumentCaptor<CreateJndiBindingFunction> function =
        ArgumentCaptor.forClass(CreateJndiBindingFunction.class);
    ArgumentCaptor<Object[]> arguments =
        ArgumentCaptor.forClass(Object[].class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Set<DistributedMember>> targetMembers = ArgumentCaptor.forClass(Set.class);
    verify(command, times(1)).executeAndGetFunctionResult(function.capture(), arguments.capture(),
        targetMembers.capture());
    Object[] actualArguments = arguments.getValue();
    JndiBinding jndiConfig = (JndiBinding) actualArguments[0];
    boolean creatingDataSource = (Boolean) actualArguments[1];

    assertThat(function.getValue()).isInstanceOf(CreateJndiBindingFunction.class);
    assertThat(creatingDataSource).isFalse();
    assertThat(jndiConfig.getJndiName()).isEqualTo("name");
    assertThat(jndiConfig.getConfigProperties().get(0).getName()).isEqualTo("name1");
    assertThat(targetMembers.getValue()).isEqualTo(members);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void whenMembersFoundAndClusterConfigRunningThenUpdateClusterConfigAndInvokeFunction() {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result = new CliFunctionResult("server1", true,
        "Tried creating jndi binding \"name\" on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());
    doAnswer(invocation -> {
      UnaryOperator<CacheConfig> mutator = invocation.getArgument(1);
      mutator.apply(cacheConfig);
      return null;
    }).when(clusterConfigService).updateCacheConfig(any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --datasource-config-properties={'name':'name1','type':'type1','value':'value1'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status", "OK").tableHasColumnOnlyWithValues("Message",
            "Tried creating jndi binding \"name\" on \"server1\"");

    verify(clusterConfigService).updateCacheConfig(any(), any());
    verify(command).updateConfigForGroup(eq("cluster"), eq(cacheConfig), any());

    ArgumentCaptor<CreateJndiBindingFunction> function =
        ArgumentCaptor.forClass(CreateJndiBindingFunction.class);
    ArgumentCaptor<Object[]> arguments =
        ArgumentCaptor.forClass(Object[].class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Set<DistributedMember>> targetMembers = ArgumentCaptor.forClass(Set.class);
    verify(command, times(1)).executeAndGetFunctionResult(function.capture(), arguments.capture(),
        targetMembers.capture());
    Object[] actualArguments = arguments.getValue();
    JndiBinding jndiConfig = (JndiBinding) actualArguments[0];
    boolean creatingDataSource = (Boolean) actualArguments[1];

    assertThat(function.getValue()).isInstanceOf(CreateJndiBindingFunction.class);
    assertThat(creatingDataSource).isFalse();
    assertThat(jndiConfig.getJndiName()).isEqualTo("name");
    assertThat(jndiConfig.getConfigProperties().get(0).getName()).isEqualTo("name1");
    assertThat(targetMembers.getValue()).isEqualTo(members);
  }
}
