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
import static org.mockito.Mockito.doCallRealMethod;
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

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateJndiBindingFunction;
import org.apache.geode.management.internal.cli.functions.DestroyJndiBindingFunction;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class DestroyJndiBindingCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private DestroyJndiBindingCommand command;
  private InternalCache cache;
  private CacheConfig cacheConfig;
  private InternalClusterConfigurationService ccService;

  private static String COMMAND = "destroy jndi-binding ";

  @Before
  public void setUp() throws Exception {
    cache = mock(InternalCache.class);
    command = spy(DestroyJndiBindingCommand.class);
    doReturn(cache).when(command).getCache();
    cacheConfig = mock(CacheConfig.class);
    ccService = mock(InternalClusterConfigurationService.class);
    doReturn(ccService).when(command).getConfigurationService();
    when(ccService.getCacheConfig(any())).thenReturn(cacheConfig);
    doCallRealMethod().when(ccService).updateCacheConfig(any(), any());
    when(ccService.getConfigurationRegion()).thenReturn(mock(Region.class));
    when(ccService.getConfiguration(any())).thenReturn(mock(Configuration.class));
  }

  @Test
  public void missingMandatory() {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void returnsErrorIfBindingDoesNotExistAndIfExistsUnspecified() {
    when(ccService.findIdentifiable(any(), any())).thenReturn(null);
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsError()
        .containsOutput("does not exist.");
  }

  @Test
  public void skipsIfBindingDoesNotExistAndIfExistsSpecified() {
    when(ccService.findIdentifiable(any(), any())).thenReturn(null);
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name --if-exists").statusIsSuccess()
        .containsOutput("does not exist.");
  }

  @Test
  public void skipsIfBindingDoesNotExistAndIfExistsSpecifiedTrue() {
    when(ccService.findIdentifiable(any(), any())).thenReturn(null);
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name --if-exists=true").statusIsSuccess()
        .containsOutput("does not exist.");
  }

  @Test
  public void returnsErrorIfBindingDoesNotExistAndIfExistsSpecifiedFalse() {
    when(ccService.findIdentifiable(any(), any())).thenReturn(null);
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name --if-exists=false").statusIsError()
        .containsOutput("does not exist.");
  }

  @Test
  public void whenNoMembersFoundAndNoClusterConfigServiceRunningThenError() {
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationService();

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .containsOutput("No members found").hasFailToPersistError();
  }

  @Test
  public void whenNoMembersFoundAndClusterConfigRunningThenUpdateClusterConfig() {
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    when(ccService.findIdentifiable(any(), any()))
        .thenReturn(mock(JndiBindingsType.JndiBinding.class));

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .containsOutput(
            "No members found. Jndi-binding \\\"name\\\" is removed from cluster configuration.")
        .hasNoFailToPersistError();

    verify(ccService).updateCacheConfig(any(), any());
  }

  @Test
  public void whenMembersFoundAndNoClusterConfigRunningThenOnlyInvokeFunction() {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result =
        new CliFunctionResult("server1", true, "Jndi binding \"name\" destroyed on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationService();
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status", "Jndi binding \"name\" destroyed on \"server1\"");

    verify(ccService, times(0)).updateCacheConfig(any(), any());

    ArgumentCaptor<DestroyJndiBindingFunction> function =
        ArgumentCaptor.forClass(DestroyJndiBindingFunction.class);
    ArgumentCaptor<String> jndiName = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Set<DistributedMember>> targetMembers = ArgumentCaptor.forClass(Set.class);
    verify(command, times(1)).executeAndGetFunctionResult(function.capture(), jndiName.capture(),
        targetMembers.capture());

    assertThat(function.getValue()).isInstanceOf(DestroyJndiBindingFunction.class);
    assertThat(jndiName.getValue()).isNotNull();
    assertThat(jndiName.getValue()).isEqualTo("name");
    assertThat(targetMembers.getValue()).isEqualTo(members);
  }

  @Test
  public void whenMembersFoundAndClusterConfigRunningThenUpdateClusterConfigAndInvokeFunction() {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result =
        new CliFunctionResult("server1", true, "Jndi binding \"name\" destroyed on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());
    when(ccService.findIdentifiable(any(), any()))
        .thenReturn(mock(JndiBindingsType.JndiBinding.class));

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status", "Jndi binding \"name\" destroyed on \"server1\"");

    ArgumentCaptor<CreateJndiBindingFunction> function =
        ArgumentCaptor.forClass(CreateJndiBindingFunction.class);
    ArgumentCaptor<String> jndiName = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Set<DistributedMember>> targetMembers = ArgumentCaptor.forClass(Set.class);
    verify(command, times(1)).executeAndGetFunctionResult(function.capture(), jndiName.capture(),
        targetMembers.capture());

    assertThat(function.getValue()).isInstanceOf(DestroyJndiBindingFunction.class);
    assertThat(jndiName.getValue()).isNotNull();
    assertThat(jndiName.getValue()).isEqualTo("name");
    assertThat(targetMembers.getValue()).isEqualTo(members);
  }
}
