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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.commands.CreateJndiBindingCommand;
import org.apache.geode.management.internal.cli.functions.DestroyJndiBindingFunction;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class DestroyDataSourceCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private DestroyDataSourceCommand command;
  private CacheConfig cacheConfig;
  private InternalConfigurationPersistenceService ccService;

  private static String COMMAND = "destroy data-source ";

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    InternalCache cache = mock(InternalCache.class);
    command = spy(DestroyDataSourceCommand.class);
    doReturn(cache).when(command).getCache();
    cacheConfig = mock(CacheConfig.class);
    ccService = mock(InternalConfigurationPersistenceService.class);

    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    when(ccService.getCacheConfig(any())).thenReturn(cacheConfig);
    doAnswer(invocation -> {
      UnaryOperator<CacheConfig> mutator = invocation.getArgument(1);
      mutator.apply(cacheConfig);
      return null;
    }).when(ccService).updateCacheConfig(any(), any());

    when(ccService.getConfigurationRegion()).thenReturn(mock(Region.class));
    when(ccService.getConfiguration(any())).thenReturn(mock(Configuration.class));
  }

  @Test
  public void missingMandatory() {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void returnsErrorIfBindingDoesNotExistAndIfExistsUnspecified() {
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsError()
        .containsOutput("does not exist.");
  }

  @Test
  public void skipsIfBindingDoesNotExistAndIfExistsSpecified() {
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name --if-exists").statusIsSuccess()
        .containsOutput("does not exist.");
  }

  @Test
  public void skipsIfBindingDoesNotExistAndIfExistsSpecifiedTrue() {
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name --if-exists=true").statusIsSuccess()
        .containsOutput("does not exist.");
  }

  @Test
  public void returnsErrorIfBindingDoesNotExistAndIfExistsSpecifiedFalse() {
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name --if-exists=false").statusIsError()
        .containsOutput("does not exist.");
  }

  @Test
  public void whenNoMembersFoundAndNoClusterConfigServiceRunningThenError() {
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationPersistenceService();

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsError()
        .containsOutput("No members found and cluster configuration disabled.");
  }

  @Test
  public void whenClusterConfigRunningAndJndiBindingFoundThenError() {
    List<JndiBindingsType.JndiBinding> bindings = new ArrayList<>();
    JndiBindingsType.JndiBinding jndiBinding = new JndiBindingsType.JndiBinding();
    jndiBinding.setJndiName("name");
    jndiBinding.setType(CreateJndiBindingCommand.DATASOURCE_TYPE.MANAGED.getType());
    bindings.add(jndiBinding);
    doReturn(bindings).when(cacheConfig).getJndiBindings();

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsError()
        .containsOutput(
            "Data source named \"name\" does not exist. A jndi-binding was found with that name.");
  }

  @Test
  public void whenClusterConfigRunningAndDataSourceInUseByRegionThenError() {
    String DATA_SOURCE_NAME = "myDataSourceName";
    List<JndiBindingsType.JndiBinding> bindings = new ArrayList<>();
    JndiBindingsType.JndiBinding jndiBinding = new JndiBindingsType.JndiBinding();
    jndiBinding.setJndiName(DATA_SOURCE_NAME);
    jndiBinding.setType(CreateJndiBindingCommand.DATASOURCE_TYPE.SIMPLE.getType());
    bindings.add(jndiBinding);
    doReturn(bindings).when(cacheConfig).getJndiBindings();
    setupRegionConfigToUseDataSource(DATA_SOURCE_NAME);

    gfsh.executeAndAssertThat(command, COMMAND + " --name=" + DATA_SOURCE_NAME).statusIsError()
        .containsOutput("Data source named \"" + DATA_SOURCE_NAME
            + "\" is still being used by region \"regionUsingDataSource\"."
            + " Use destroy jdbc-mapping --region=regionUsingDataSource and then try again.");
  }

  private void setupRegionConfigToUseDataSource(String dataSourceName) {
    RegionConfig regionConfig = mock(RegionConfig.class);
    when(regionConfig.getName()).thenReturn("regionUsingDataSource");
    List<RegionConfig> regionConfigList = new ArrayList<>();
    regionConfigList.add(regionConfig);
    when(cacheConfig.getRegions()).thenReturn(regionConfigList);
    RegionMapping regionMapping = mock(RegionMapping.class);
    when(regionMapping.getDataSourceName()).thenReturn(dataSourceName);
    List<CacheElement> cacheElementList = new ArrayList<>();
    cacheElementList.add(regionMapping);
    when(regionConfig.getCustomRegionElements()).thenReturn(cacheElementList);
  }

  @Test
  public void whenNoMembersFoundAndClusterConfigRunningAndRegionUsingOtherDataSourceThenUpdateClusterConfig() {
    List<JndiBindingsType.JndiBinding> bindings = new ArrayList<>();
    JndiBindingsType.JndiBinding jndiBinding = new JndiBindingsType.JndiBinding();
    jndiBinding.setJndiName("name");
    jndiBinding.setType(CreateJndiBindingCommand.DATASOURCE_TYPE.SIMPLE.getType());
    bindings.add(jndiBinding);
    doReturn(bindings).when(cacheConfig).getJndiBindings();
    setupRegionConfigToUseDataSource("otherDataSource");

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .containsOutput("No members found, data source removed from cluster configuration.")
        .containsOutput("Cluster configuration for group 'cluster' is updated");

    verify(ccService).updateCacheConfig(any(), any());
    verify(command).updateConfigForGroup(eq("cluster"), eq(cacheConfig), isNotNull());
  }

  @Test
  public void whenNoMembersFoundAndClusterConfigRunningThenUpdateClusterConfig() {
    List<JndiBindingsType.JndiBinding> bindings = new ArrayList<>();
    JndiBindingsType.JndiBinding jndiBinding = new JndiBindingsType.JndiBinding();
    jndiBinding.setJndiName("name");
    jndiBinding.setType(CreateJndiBindingCommand.DATASOURCE_TYPE.SIMPLE.getType());
    bindings.add(jndiBinding);
    doReturn(bindings).when(cacheConfig).getJndiBindings();

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .containsOutput("No members found, data source removed from cluster configuration.")
        .containsOutput("Cluster configuration for group 'cluster' is updated");

    verify(ccService).updateCacheConfig(any(), any());
    verify(command).updateConfigForGroup(eq("cluster"), eq(cacheConfig), isNotNull());
  }

  @Test
  public void whenNoMembersFoundAndClusterConfigRunningWithPooledTypeThenUpdateClusterConfig() {
    List<JndiBindingsType.JndiBinding> bindings = new ArrayList<>();
    JndiBindingsType.JndiBinding jndiBinding = new JndiBindingsType.JndiBinding();
    jndiBinding.setJndiName("name");
    jndiBinding.setType(CreateJndiBindingCommand.DATASOURCE_TYPE.POOLED.getType());
    bindings.add(jndiBinding);
    doReturn(bindings).when(cacheConfig).getJndiBindings();

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .containsOutput("No members found, data source removed from cluster configuration.")
        .containsOutput("Cluster configuration for group 'cluster' is updated");

    verify(ccService).updateCacheConfig(any(), any());
    verify(command).updateConfigForGroup(eq("cluster"), eq(cacheConfig), isNotNull());
  }

  @Test
  public void whenMembersFoundAllReturnErrorAndIfExistsFalseThenError() {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result =
        new CliFunctionResult("server1", true, "Data source \"name\" not found on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name --if-exists=false").statusIsError()
        .containsOutput("Data source named \"name\" does not exist.");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void whenMembersFoundAllReturnErrorAndIfExistsTrueThenSuccess() {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result =
        new CliFunctionResult("server1", true, "Data source \"name\" not found on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name --if-exists=true").statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status", "OK")
        .tableHasColumnOnlyWithValues("Message", "Data source \"name\" not found on \"server1\"");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void whenMembersFoundPartialReturnErrorAndIfExistsFalseThenSuccess() {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));
    members.add(mock(DistributedMember.class));

    CliFunctionResult result =
        new CliFunctionResult("server1", true, "Data source \"name\" not found on \"server1\"");
    CliFunctionResult result2 =
        new CliFunctionResult("server2", true, "Data source \"name\" destroyed on \"server2\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);
    results.add(result2);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name --if-exists=false").statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server1", "server2")
        .tableHasColumnOnlyWithValues("Status", "OK", "OK")
        .tableHasColumnOnlyWithValues("Message", "Data source \"name\" not found on \"server1\"",
            "Data source \"name\" destroyed on \"server2\"");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void whenMembersFoundAndNoClusterConfigRunningThenOnlyInvokeFunction() {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result =
        new CliFunctionResult("server1", true, "Data source \"name\" destroyed on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status", "OK")
        .tableHasColumnOnlyWithValues("Message", "Data source \"name\" destroyed on \"server1\"");

    verify(ccService, times(0)).updateCacheConfig(any(), any());

    ArgumentCaptor<DestroyJndiBindingFunction> function =
        ArgumentCaptor.forClass(DestroyJndiBindingFunction.class);
    ArgumentCaptor<Object[]> arguments = ArgumentCaptor.forClass(Object[].class);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Set<DistributedMember>> targetMembers = ArgumentCaptor.forClass(Set.class);
    verify(command, times(1)).executeAndGetFunctionResult(function.capture(), arguments.capture(),
        targetMembers.capture());

    String jndiName = (String) arguments.getValue()[0];
    boolean destroyingDataSource = (boolean) arguments.getValue()[1];

    assertThat(function.getValue()).isInstanceOf(DestroyJndiBindingFunction.class);
    assertThat(jndiName).isEqualTo("name");
    assertThat(destroyingDataSource).isEqualTo(true);
    assertThat(targetMembers.getValue()).isEqualTo(members);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void whenMembersFoundAndClusterConfigRunningThenUpdateClusterConfigAndInvokeFunction() {
    List<JndiBindingsType.JndiBinding> bindings = new ArrayList<>();
    JndiBindingsType.JndiBinding jndiBinding = new JndiBindingsType.JndiBinding();
    jndiBinding.setJndiName("name");
    jndiBinding.setType(CreateJndiBindingCommand.DATASOURCE_TYPE.SIMPLE.getType());
    bindings.add(jndiBinding);
    doReturn(bindings).when(cacheConfig).getJndiBindings();

    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result =
        new CliFunctionResult("server1", true, "Data source \"name\" destroyed on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status", "OK")
        .tableHasColumnOnlyWithValues("Message", "Data source \"name\" destroyed on \"server1\"");

    assertThat(cacheConfig.getJndiBindings().isEmpty()).isTrue();
    verify(command).updateConfigForGroup(eq("cluster"), eq(cacheConfig), any());

    ArgumentCaptor<DestroyJndiBindingFunction> function =
        ArgumentCaptor.forClass(DestroyJndiBindingFunction.class);
    ArgumentCaptor<Object[]> arguments = ArgumentCaptor.forClass(Object[].class);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Set<DistributedMember>> targetMembers = ArgumentCaptor.forClass(Set.class);
    verify(command, times(1)).executeAndGetFunctionResult(function.capture(), arguments.capture(),
        targetMembers.capture());

    String jndiName = (String) arguments.getValue()[0];
    boolean destroyingDataSource = (boolean) arguments.getValue()[1];

    assertThat(function.getValue()).isInstanceOf(DestroyJndiBindingFunction.class);
    assertThat(jndiName).isEqualTo("name");
    assertThat(destroyingDataSource).isEqualTo(true);
    assertThat(targetMembers.getValue()).isEqualTo(members);
  }
}
