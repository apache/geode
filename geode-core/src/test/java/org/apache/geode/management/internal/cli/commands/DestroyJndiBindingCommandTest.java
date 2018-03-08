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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.datasource.ConfigProperty;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateJndiBindingFunction;
import org.apache.geode.management.internal.cli.functions.DestroyJndiBindingFunction;
import org.apache.geode.management.internal.cli.functions.JndiBindingConfiguration;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class DestroyJndiBindingCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private DestroyJndiBindingCommand command;
  private InternalCache cache;

  private static String COMMAND = "destroy jndi-binding ";

  @Before
  public void setUp() throws Exception {
    cache = mock(InternalCache.class);
    command = spy(DestroyJndiBindingCommand.class);
    doReturn(cache).when(command).getCache();
  }

  @Test
  public void missingMandatory() {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void returnsErrorIfNoBindingExistsWithGivenName()
      throws ParserConfigurationException, SAXException, IOException {
    ClusterConfigurationService clusterConfigService = mock(ClusterConfigurationService.class);
    doReturn(clusterConfigService).when(command).getSharedConfiguration();
    doReturn(null).when(clusterConfigService).getXmlElement(any(), any(), any(), any());
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsError()
        .containsOutput("does not exist.");
  }

  @Test
  public void removeJndiBindingFromXmlShouldRemoveBindingFromClusterConfiguration()
      throws IOException, ParserConfigurationException, SAXException, TransformerException {
    ClusterConfigurationService clusterConfigService = mock(ClusterConfigurationService.class);
    Region configRegion = mock(Region.class);

    Configuration clusterConfig = new Configuration("cluster");
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>"
        + "<cache xmlns=\"http://geode.apache.org/schema/cache\" "
        + "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
        + "copy-on-read=\"false\" is-server=\"false\" lock-lease=\"120\" "
        + "lock-timeout=\"60\" search-timeout=\"300\" version=\"1.0\" "
        + "xsi:schemaLocation=\"http://geode.apache.org/schema/cache "
        + "http://geode.apache.org/schema/cache/cache-1.0.xsd\">" + "<jndi-bindings>"
        + "<jndi-binding jndi-name=\"jndi1\" type=\"SimpleDataSource\"/>"
        + "<jndi-binding jndi-name=\"jndi2\" type=\"SimpleDataSource\"/>"
        + "</jndi-bindings></cache>";
    clusterConfig.setCacheXmlContent(xml);

    doReturn(configRegion).when(clusterConfigService).getConfigurationRegion();
    doReturn(null).when(configRegion).put(any(), any());
    doReturn(clusterConfig).when(clusterConfigService).getConfiguration("cluster");
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(clusterConfigService).when(command).getSharedConfiguration();

    command.removeJndiBindingFromXml("jndi1");
    assertThat(clusterConfig.getCacheXmlContent()).doesNotContain("jndi1");
    assertThat(clusterConfig.getCacheXmlContent()).contains("jndi2");
    verify(configRegion, times(1)).put("cluster", clusterConfig);
  }

  @Test
  public void removeJndiBindingFromXmlIsNoOpWhenNoBindingFoundInClusterConfiguration()
      throws IOException, ParserConfigurationException, SAXException, TransformerException {
    ClusterConfigurationService clusterConfigService = mock(ClusterConfigurationService.class);
    Region configRegion = mock(Region.class);
    Configuration clusterConfig = new Configuration("cluster");

    doReturn(configRegion).when(clusterConfigService).getConfigurationRegion();
    doReturn(null).when(configRegion).put(any(), any());
    doReturn(clusterConfig).when(clusterConfigService).getConfiguration("cluster");
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(clusterConfigService).when(command).getSharedConfiguration();

    command.removeJndiBindingFromXml("jndi1");
    verify(configRegion, times(0)).put("cluster", clusterConfig);
  }

  @Test
  public void whenNoMembersFoundAndNoClusterConfigServiceRunningThenError()
      throws IOException, ParserConfigurationException, SAXException, TransformerException {
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(null).when(command).getSharedConfiguration();
    doNothing().when(command).removeJndiBindingFromXml(any());

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .containsOutput("No members found").hasFailToPersistError();
  }

  @Test
  public void whenNoMembersFoundAndClusterConfigRunningThenUpdateClusterConfig()
      throws IOException, ParserConfigurationException, SAXException, TransformerException {
    ClusterConfigurationService clusterConfigService = mock(ClusterConfigurationService.class);
    Element existingBinding = mock(Element.class);

    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(existingBinding).when(clusterConfigService).getXmlElement(any(), any(), any(), any());
    doReturn(clusterConfigService).when(command).getSharedConfiguration();
    doNothing().when(command).removeJndiBindingFromXml(any());

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .containsOutput(
            "No members found. Jndi-binding \\\"name\\\" is removed from cluster configuration.")
        .hasNoFailToPersistError();

    verify(command, times(1)).removeJndiBindingFromXml(any());
  }

  @Test
  public void whenMembersFoundAndNoClusterConfigRunningThenOnlyInvokeFunction()
      throws IOException, ParserConfigurationException, SAXException, TransformerException {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result =
        new CliFunctionResult("server1", true, "Jndi binding \"name\" destroyed on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(null).when(command).getSharedConfiguration();
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status", "Jndi binding \"name\" destroyed on \"server1\"");

    verify(command, times(0)).removeJndiBindingFromXml(any());

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
  public void whenMembersFoundAndClusterConfigRunningThenUpdateClusterConfigAndInvokeFunction()
      throws IOException, ParserConfigurationException, SAXException, TransformerException {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result =
        new CliFunctionResult("server1", true, "Jndi binding \"name\" destroyed on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);
    ClusterConfigurationService clusterConfigService = mock(ClusterConfigurationService.class);
    Element existingBinding = mock(Element.class);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(existingBinding).when(clusterConfigService).getXmlElement(any(), any(), any(), any());
    doReturn(clusterConfigService).when(command).getSharedConfiguration();
    doNothing().when(command).removeJndiBindingFromXml(any());
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status", "Jndi binding \"name\" destroyed on \"server1\"");

    verify(command, times(1)).removeJndiBindingFromXml(any());

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
