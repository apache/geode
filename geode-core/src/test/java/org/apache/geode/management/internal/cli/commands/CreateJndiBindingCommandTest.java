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
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.datasource.ConfigProperty;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.CreateJndiBindingFunction;
import org.apache.geode.management.internal.cli.functions.JndiBindingConfiguration;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
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

    ConfigProperty[] configProperties =
        (ConfigProperty[]) result.getParamValue("datasource-config-properties");
    assertThat(configProperties).hasSize(2);
    assertThat(configProperties[0].getValue()).isEqualTo("value1");
    assertThat(configProperties[1].getValue()).isEqualTo("value2");
  }

  @Test
  public void returnsErrorIfBindingAlreadyExistsAndIfUnspecified()
      throws ParserConfigurationException, SAXException, IOException {
    InternalClusterConfigurationService clusterConfigService =
        mock(InternalClusterConfigurationService.class);
    Element existingBinding = mock(Element.class);

    doReturn(clusterConfigService).when(command).getSharedConfiguration();
    doReturn(existingBinding).when(clusterConfigService).getXmlElement(any(), any(), any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url")
        .statusIsError().containsOutput("already exists.");
  }

  @Test
  public void skipsIfBindingAlreadyExistsAndIfSpecified()
      throws ParserConfigurationException, SAXException, IOException {
    InternalClusterConfigurationService clusterConfigService =
        mock(InternalClusterConfigurationService.class);
    Element existingBinding = mock(Element.class);

    doReturn(clusterConfigService).when(command).getSharedConfiguration();
    doReturn(existingBinding).when(clusterConfigService).getXmlElement(any(), any(), any(), any());

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
    Element existingBinding = mock(Element.class);

    doReturn(clusterConfigService).when(command).getSharedConfiguration();
    doReturn(existingBinding).when(clusterConfigService).getXmlElement(any(), any(), any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --if-not-exists=true")
        .statusIsSuccess().containsOutput("Skipping");
  }

  @Test
  public void returnsErrorIfBindingAlreadyExistsAndIfSpecifiedFalse()
      throws ParserConfigurationException, SAXException, IOException {
    InternalClusterConfigurationService clusterConfigService =
        mock(InternalClusterConfigurationService.class);
    Element existingBinding = mock(Element.class);

    doReturn(clusterConfigService).when(command).getSharedConfiguration();
    doReturn(existingBinding).when(clusterConfigService).getXmlElement(any(), any(), any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --if-not-exists=false")
        .statusIsError().containsOutput("already exists.");
  }

  @Test
  public void updateXmlShouldClusterConfigurationWithJndiConfiguration()
      throws IOException, ParserConfigurationException, SAXException, TransformerException {
    Configuration clusterConfig = new Configuration("cluster");
    InternalClusterConfigurationService clusterConfigService =
        mock(InternalClusterConfigurationService.class);
    Region configRegion = mock(Region.class);

    doReturn(configRegion).when(clusterConfigService).getConfigurationRegion();
    doReturn(null).when(configRegion).put(any(), any());
    doReturn(clusterConfig).when(clusterConfigService).getConfiguration("cluster");
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(clusterConfigService).when(command).getSharedConfiguration();

    JndiBindingConfiguration jndi = new JndiBindingConfiguration();
    jndi.setBlockingTimeout(60);
    jndi.setConnectionUrl("URL");
    jndi.setConnectionPoolDatasource("org.datasource");
    jndi.setMaxPoolSize(10);
    jndi.setManagedConnFactory("connFactory");
    jndi.setLoginTimeout(100);
    jndi.setJndiName("jndi1");
    jndi.setJdbcDriver("driver");
    jndi.setUsername("user1");
    jndi.setIdleTimeout(50);
    jndi.setInitPoolSize(5);
    jndi.setPassword("p@ssw0rd");
    jndi.setTransactionType("txntype");
    jndi.setType(JndiBindingConfiguration.DATASOURCE_TYPE.SIMPLE);
    jndi.setXaDatasource("xaDS");
    ConfigProperty prop = new ConfigProperty("somename", "somevalue", "sometype");
    jndi.setDatasourceConfigurations(Arrays.asList(new ConfigProperty[] {prop}));
    command.updateXml(jndi);

    Document document = XmlUtils.createDocumentFromXml(clusterConfig.getCacheXmlContent());
    assertThat(document).isNotNull();
    assertThat(document.getElementsByTagName("jndi-bindings").item(0)).isNotNull();
    Element jndiElement = (Element) document.getElementsByTagName("jndi-binding").item(0);
    assertThat(jndiElement).isNotNull();
    assertThat(jndiElement.getParentNode().getNodeName()).isEqualTo("jndi-bindings");
    assertThat(jndiElement.getAttribute("blocking-timeout-seconds")).isEqualTo("60");
    assertThat(jndiElement.getAttribute("conn-pooled-datasource-class"))
        .isEqualTo("org.datasource");
    assertThat(jndiElement.getAttribute("connection-url")).isEqualTo("URL");
    assertThat(jndiElement.getAttribute("idle-timeout-seconds")).isEqualTo("50");
    assertThat(jndiElement.getAttribute("init-pool-size")).isEqualTo("5");
    assertThat(jndiElement.getAttribute("jdbc-driver-class")).isEqualTo("driver");
    assertThat(jndiElement.getAttribute("jndi-name")).isEqualTo("jndi1");
    assertThat(jndiElement.getAttribute("login-timeout-seconds")).isEqualTo("100");
    assertThat(jndiElement.getAttribute("managed-conn-factory-class")).isEqualTo("connFactory");
    assertThat(jndiElement.getAttribute("max-pool-size")).isEqualTo("10");
    assertThat(jndiElement.getAttribute("password")).isEqualTo("p@ssw0rd");
    assertThat(jndiElement.getAttribute("transaction-type")).isEqualTo("txntype");
    assertThat(jndiElement.getAttribute("type")).isEqualTo("SimpleDataSource");
    assertThat(jndiElement.getAttribute("user-name")).isEqualTo("user1");
    assertThat(jndiElement.getAttribute("xa-datasource-class")).isEqualTo("xaDS");

    Node configProperty = document.getElementsByTagName("config-property").item(0);
    assertThat(configProperty.getParentNode().getNodeName()).isEqualTo("jndi-binding");

    Node nameProperty = document.getElementsByTagName("config-property-name").item(0);
    assertThat(nameProperty).isNotNull();
    assertThat(nameProperty.getParentNode().getNodeName()).isEqualTo("config-property");
    assertThat(nameProperty.getTextContent()).isEqualTo("somename");
    Node typeProperty = document.getElementsByTagName("config-property-type").item(0);
    assertThat(typeProperty).isNotNull();
    assertThat(nameProperty.getParentNode().getNodeName()).isEqualTo("config-property");
    assertThat(typeProperty.getTextContent()).isEqualTo("sometype");
    Node valueProperty = document.getElementsByTagName("config-property-value").item(0);
    assertThat(valueProperty).isNotNull();
    assertThat(nameProperty.getParentNode().getNodeName()).isEqualTo("config-property");
    assertThat(valueProperty.getTextContent()).isEqualTo("somevalue");

    verify(configRegion, times(1)).put("cluster", clusterConfig);
  }

  @Test
  public void whenNoMembersFoundAndNoClusterConfigServiceRunningThenError()
      throws IOException, ParserConfigurationException, SAXException, TransformerException {

    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(null).when(command).getSharedConfiguration();
    doNothing().when(command).updateXml(any());

    gfsh.executeAndAssertThat(command,
        COMMAND + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url")
        .statusIsSuccess().containsOutput("No members found").hasFailToPersistError();
  }

  @Test
  public void whenNoMembersFoundAndClusterConfigRunningThenUpdateClusterConfig()
      throws IOException, ParserConfigurationException, SAXException, TransformerException {
    InternalClusterConfigurationService clusterConfigService =
        mock(InternalClusterConfigurationService.class);

    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(null).when(clusterConfigService).getXmlElement(any(), any(), any(), any());
    doReturn(clusterConfigService).when(command).getSharedConfiguration();
    doNothing().when(command).updateXml(any());

    gfsh.executeAndAssertThat(command,
        COMMAND + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url")
        .statusIsSuccess()
        .containsOutput(
            "No members found. Cluster configuration is updated with jndi-binding \\\"name\\\".")
        .hasNoFailToPersistError();

    verify(command, times(1)).updateXml(any());
  }

  @Test
  public void whenMembersFoundAndNoClusterConfigRunningThenOnlyInvokeFunction()
      throws IOException, ParserConfigurationException, SAXException, TransformerException {
    Set<DistributedMember> members = new HashSet<>();
    members.add(mock(DistributedMember.class));

    CliFunctionResult result = new CliFunctionResult("server1", true,
        "Tried creating jndi binding \"name\" on \"server1\"");
    List<CliFunctionResult> results = new ArrayList<>();
    results.add(result);

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(null).when(command).getSharedConfiguration();
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --datasource-config-properties={'name':'name1','type':'type1','value':'value1'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status",
            "Tried creating jndi binding \"name\" on \"server1\"");

    verify(command, times(0)).updateXml(any());

    ArgumentCaptor<CreateJndiBindingFunction> function =
        ArgumentCaptor.forClass(CreateJndiBindingFunction.class);
    ArgumentCaptor<JndiBindingConfiguration> jndiConfig =
        ArgumentCaptor.forClass(JndiBindingConfiguration.class);
    ArgumentCaptor<Set<DistributedMember>> targetMembers = ArgumentCaptor.forClass(Set.class);
    verify(command, times(1)).executeAndGetFunctionResult(function.capture(), jndiConfig.capture(),
        targetMembers.capture());

    assertThat(function.getValue()).isInstanceOf(CreateJndiBindingFunction.class);
    assertThat(jndiConfig.getValue()).isNotNull();
    assertThat(jndiConfig.getValue().getJndiName()).isEqualTo("name");
    assertThat(jndiConfig.getValue().getDatasourceConfigurations().get(0).getName())
        .isEqualTo("name1");
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

    doReturn(members).when(command).findMembers(any(), any());
    doReturn(null).when(clusterConfigService).getXmlElement(any(), any(), any(), any());
    doReturn(clusterConfigService).when(command).getSharedConfiguration();
    doNothing().when(command).updateXml(any());
    doReturn(results).when(command).executeAndGetFunctionResult(any(), any(), any());

    gfsh.executeAndAssertThat(command,
        COMMAND
            + " --type=SIMPLE --name=name --jdbc-driver-class=driver --connection-url=url --datasource-config-properties={'name':'name1','type':'type1','value':'value1'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server1")
        .tableHasColumnOnlyWithValues("Status",
            "Tried creating jndi binding \"name\" on \"server1\"");

    verify(command, times(1)).updateXml(any());

    ArgumentCaptor<CreateJndiBindingFunction> function =
        ArgumentCaptor.forClass(CreateJndiBindingFunction.class);
    ArgumentCaptor<JndiBindingConfiguration> jndiConfig =
        ArgumentCaptor.forClass(JndiBindingConfiguration.class);
    ArgumentCaptor<Set<DistributedMember>> targetMembers = ArgumentCaptor.forClass(Set.class);
    verify(command, times(1)).executeAndGetFunctionResult(function.capture(), jndiConfig.capture(),
        targetMembers.capture());

    assertThat(function.getValue()).isInstanceOf(CreateJndiBindingFunction.class);
    assertThat(jndiConfig.getValue()).isNotNull();
    assertThat(jndiConfig.getValue().getJndiName()).isEqualTo("name");
    assertThat(jndiConfig.getValue().getDatasourceConfigurations().get(0).getName())
        .isEqualTo("name1");
    assertThat(targetMembers.getValue()).isEqualTo(members);
  }
}
