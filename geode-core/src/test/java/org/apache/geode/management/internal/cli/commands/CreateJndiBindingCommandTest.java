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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.xml.sax.SAXException;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateJndiBindingFunction;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class CreateJndiBindingCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private CreateJndiBindingCommand command;
  private InternalCache cache;

  private static String COMMAND = "create jndi-binding ";

  @Before
  public void setUp() throws Exception {
    cache = mock(InternalCache.class);
    command = spy(CreateJndiBindingCommand.class);
    doReturn(cache).when(command).getCache();
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
  public void returnsErrorIfBindingAlreadyExistsAndIfUnspecified()
      throws ParserConfigurationException, SAXException, IOException {
    InternalClusterConfigurationService clusterConfigService =
        mock(InternalClusterConfigurationService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);
    JndiBindingsType.JndiBinding existingBinding = mock(JndiBindingsType.JndiBinding.class);

    doReturn(clusterConfigService).when(command).getConfigurationService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());
    doReturn(existingBinding).when(clusterConfigService).findIdentifiable(any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url")
        .statusIsError().containsOutput("already exists.");
  }

  @Test
  public void skipsIfBindingAlreadyExistsAndIfSpecified()
      throws ParserConfigurationException, SAXException, IOException {
    InternalClusterConfigurationService clusterConfigService =
        mock(InternalClusterConfigurationService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);
    JndiBindingsType.JndiBinding existingBinding = mock(JndiBindingsType.JndiBinding.class);

    doReturn(clusterConfigService).when(command).getConfigurationService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());
    doReturn(existingBinding).when(clusterConfigService).findIdentifiable(any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --if-not-exists")
        .statusIsSuccess().containsOutput("Skipping");
  }

  @Test
  public void skipsIfBindingAlreadyExistsAndIfSpecifiedTrue()
      throws ParserConfigurationException, SAXException, IOException {
    InternalClusterConfigurationService clusterConfigService =
        mock(InternalClusterConfigurationService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);
    JndiBindingsType.JndiBinding existingBinding = mock(JndiBindingsType.JndiBinding.class);

    doReturn(clusterConfigService).when(command).getConfigurationService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());
    doReturn(existingBinding).when(clusterConfigService).findIdentifiable(any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --if-not-exists=true")
        .statusIsSuccess().containsOutput("Skipping");
  }

  @Test
  public void returnsErrorIfBindingAlreadyExistsAndIfSpecifiedFalse() {
    InternalClusterConfigurationService clusterConfigService =
        mock(InternalClusterConfigurationService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);
    JndiBindingsType.JndiBinding existingBinding = mock(JndiBindingsType.JndiBinding.class);

    doReturn(clusterConfigService).when(command).getConfigurationService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());
    doReturn(existingBinding).when(clusterConfigService).findIdentifiable(any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --if-not-exists=false")
        .statusIsError().containsOutput("already exists.");
  }

  @Test
  public void whenNoMembersFoundAndNoClusterConfigServiceRunningThenError() {

    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationService();

    gfsh.executeAndAssertThat(command,
        COMMAND + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url")
        .statusIsSuccess().containsOutput("No members found").hasFailToPersistError();
  }

  @Test
  public void whenNoMembersFoundAndClusterConfigRunningThenUpdateClusterConfig() {
    InternalClusterConfigurationService clusterConfigService =
        mock(InternalClusterConfigurationService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);

    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(clusterConfigService).when(command).getConfigurationService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());
    doReturn(null).when(clusterConfigService).findIdentifiable(any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url")
        .statusIsSuccess()
        .containsOutput(
            "No members found. Cluster configuration is updated with jndi-binding \\\"name\\\".")
        .hasNoFailToPersistError();

    verify(clusterConfigService).updateCacheConfig(any(), any());
  }

  @Test
  public void whenMembersFoundAndNoClusterConfigRunningThenOnlyInvokeFunction() {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result = new CliFunctionResult("server1", true,
        "Tried creating jndi binding \"name\" on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(null).when(command).getConfigurationService();
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --datasource-config-properties={'name':'name1','type':'type1','value':'value1'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status",
            "Tried creating jndi binding \"name\" on \"server1\"");

    ArgumentCaptor<CreateJndiBindingFunction> function =
        ArgumentCaptor.forClass(CreateJndiBindingFunction.class);
    ArgumentCaptor<JndiBindingsType.JndiBinding> jndiConfig =
        ArgumentCaptor.forClass(JndiBindingsType.JndiBinding.class);
    ArgumentCaptor<Set<DistributedMember>> targetMembers = ArgumentCaptor.forClass(Set.class);
    verify(command, times(1)).executeAndGetFunctionResult(function.capture(), jndiConfig.capture(),
        targetMembers.capture());

    assertThat(function.getValue()).isInstanceOf(CreateJndiBindingFunction.class);
    assertThat(jndiConfig.getValue()).isNotNull();
    assertThat(jndiConfig.getValue().getJndiName()).isEqualTo("name");
    assertThat(jndiConfig.getValue().getConfigProperty().get(0).getName()).isEqualTo("name1");
    assertThat(targetMembers.getValue()).isEqualTo(members);
  }

  @Test
  public void whenMembersFoundAndClusterConfigRunningThenUpdateClusterConfigAndInvokeFunction()
      throws IOException, ParserConfigurationException, SAXException, TransformerException {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result = new CliFunctionResult("server1", true,
        "Tried creating jndi binding \"name\" on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);
    InternalClusterConfigurationService clusterConfigService =
        mock(InternalClusterConfigurationService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(clusterConfigService).when(command).getConfigurationService();
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());
    doReturn(null).when(clusterConfigService).findIdentifiable(any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --datasource-config-properties={'name':'name1','type':'type1','value':'value1'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status",
            "Tried creating jndi binding \"name\" on \"server1\"");

    verify(clusterConfigService).updateCacheConfig(any(), any());

    ArgumentCaptor<CreateJndiBindingFunction> function =
        ArgumentCaptor.forClass(CreateJndiBindingFunction.class);
    ArgumentCaptor<JndiBindingsType.JndiBinding> jndiConfig =
        ArgumentCaptor.forClass(JndiBindingsType.JndiBinding.class);
    ArgumentCaptor<Set<DistributedMember>> targetMembers = ArgumentCaptor.forClass(Set.class);
    verify(command, times(1)).executeAndGetFunctionResult(function.capture(), jndiConfig.capture(),
        targetMembers.capture());

    assertThat(function.getValue()).isInstanceOf(CreateJndiBindingFunction.class);
    assertThat(jndiConfig.getValue()).isNotNull();
    assertThat(jndiConfig.getValue().getJndiName()).isEqualTo("name");
    assertThat(jndiConfig.getValue().getConfigProperty().get(0).getName()).isEqualTo("name1");
    assertThat(targetMembers.getValue()).isEqualTo(members);
  }
}
