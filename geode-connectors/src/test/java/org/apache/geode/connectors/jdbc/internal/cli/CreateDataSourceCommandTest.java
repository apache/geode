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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
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
import org.mockito.Mockito;

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

public class CreateDataSourceCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private CreateDataSourceCommand command;
  JndiBindingsType.JndiBinding binding;
  List<JndiBindingsType.JndiBinding> bindings;

  private static String COMMAND = "create data-source ";

  @Before
  public void setUp() {
    InternalCache cache = mock(InternalCache.class);
    when(cache.getDistributionManager()).thenReturn(mock(DistributionManager.class));
    command = Mockito.spy(CreateDataSourceCommand.class);
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
  public void nonPooledWorks() {
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();

    gfsh.executeAndAssertThat(command, COMMAND + " --pooled=false --url=url --name=name")
        .statusIsSuccess();
  }

  @Test
  public void pooledWorks() {
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();

    gfsh.executeAndAssertThat(command, COMMAND + " --pooled=true --url=url --name=name")
        .statusIsSuccess();
  }

  @Test
  public void poolPropertiesIsProperlyParsed() {
    GfshParseResult result = gfsh.parse(COMMAND
        + " --pooled --name=name --url=url "
        + "--pool-properties={'name':'name1','value':'value1'},{'name':'name2','value':'value2'}");

    CreateDataSourceCommand.PoolProperty[] poolProperties =
        (CreateDataSourceCommand.PoolProperty[]) result
            .getParamValue("pool-properties");
    assertThat(poolProperties).hasSize(2);
    assertThat(poolProperties[0].getName()).isEqualTo("name1");
    assertThat(poolProperties[1].getName()).isEqualTo("name2");
    assertThat(poolProperties[0].getValue()).isEqualTo("value1");
    assertThat(poolProperties[1].getValue()).isEqualTo("value2");
  }

  @Test
  public void poolPropertiesRequiresPooled() {
    gfsh.executeAndAssertThat(command,
        COMMAND + " --pooled=false --name=name --url=url "
            + "--pool-properties={'name':'name1','value':'value1'}")
        .statusIsError().containsOutput("pool-properties option is only valid on --pooled");
  }

  @Test
  public void poolFactoryRequiresPooled() {
    gfsh.executeAndAssertThat(command,
        COMMAND + " --pooled=false --name=name --url=url "
            + "--pooled-data-source-factory-class=factoryClassValue")
        .statusIsError()
        .containsOutput("pooled-data-source-factory-class option is only valid on --pooled");
  }

  @Test
  public void returnsErrorIfDataSourceAlreadyExistsAndIfUnspecified() {
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);
    when(cacheConfig.getJndiBindings()).thenReturn(bindings);
    bindings.add(binding);

    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());

    gfsh.executeAndAssertThat(command,
        COMMAND + " --name=name  --url=url")
        .statusIsError().containsOutput("already exists.");
  }

  @Test
  public void skipsIfDataSourceAlreadyExistsAndIfSpecified() {
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);

    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());
    when(cacheConfig.getJndiBindings()).thenReturn(bindings);
    bindings.add(binding);

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --name=name  --url=url --if-not-exists")
        .statusIsSuccess().containsOutput("Skipping");
  }

  @Test
  public void skipsIfDataSourceAlreadyExistsAndIfSpecifiedTrue() {
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);
    when(cacheConfig.getJndiBindings()).thenReturn(bindings);
    bindings.add(binding);

    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --name=name  --url=url --if-not-exists=true")
        .statusIsSuccess().containsOutput("Skipping");
  }

  @Test
  public void returnsErrorIfDataSourceAlreadyExistsAndIfSpecifiedFalse() {
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);
    when(cacheConfig.getJndiBindings()).thenReturn(bindings);
    bindings.add(binding);

    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --name=name  --url=url --if-not-exists=false")
        .statusIsError().containsOutput("already exists.");
  }

  @Test
  public void whenNoMembersFoundAndNoClusterConfigServiceRunningThenError() {

    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationPersistenceService();

    gfsh.executeAndAssertThat(command,
        COMMAND + " --name=name  --url=url")
        .statusIsError().containsOutput("No members found and cluster configuration unavailable.");
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
        COMMAND + " --name=name  --url=url")
        .statusIsSuccess()
        .containsOutput("No members found, data source saved to cluster configuration.")
        .containsOutput("Cluster configuration for group 'cluster' is updated");

    verify(clusterConfigService).updateCacheConfig(any(), any());
    verify(command).updateConfigForGroup(eq("cluster"), eq(cacheConfig), isNotNull());
  }

  @SuppressWarnings("deprecation")
  @Test
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
            + " --pooled --name=name  --url=url --pool-properties={'name':'name1','value':'value1'}")
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

    assertThat(function.getValue()).isInstanceOf(CreateJndiBindingFunction.class);
    assertThat(arguments.getValue()).isNotNull();
    Object[] actualArguments = arguments.getValue();
    JndiBinding jndiConfig = (JndiBinding) actualArguments[0];
    boolean creatingDataSource = (Boolean) actualArguments[1];
    assertThat(creatingDataSource).isTrue();
    assertThat(jndiConfig.getJndiName()).isEqualTo("name");
    assertThat(jndiConfig.getConfigProperties().get(0).getName()).isEqualTo("name1");
    assertThat(targetMembers.getValue()).isEqualTo(members);
  }

  @SuppressWarnings("deprecation")
  @Test
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
            + " --pooled --name=name  --url=url --pool-properties={'name':'name1','value':'value1'}")
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

    assertThat(function.getValue()).isInstanceOf(CreateJndiBindingFunction.class);
    assertThat(arguments.getValue()).isNotNull();
    Object[] actualArguments = arguments.getValue();
    JndiBinding jndiConfig = (JndiBinding) actualArguments[0];
    boolean creatingDataSource = (Boolean) actualArguments[1];

    assertThat(function.getValue()).isInstanceOf(CreateJndiBindingFunction.class);
    assertThat(creatingDataSource).isTrue();
    assertThat(jndiConfig.getJndiName()).isEqualTo("name");
    assertThat(jndiConfig.getConfigProperties().get(0).getName()).isEqualTo("name1");
    assertThat(targetMembers.getValue()).isEqualTo(members);
  }
}
