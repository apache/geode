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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS;
import static org.apache.geode.distributed.ConfigurationProperties.LOAD_CLUSTER_CONFIGURATION_FROM_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.geode.internal.process.signal.AbstractSignalNotificationHandler;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(IntegrationTest.class)
public class StartLocatorCommandIntegrationTest {
  private static final String FAKE_HOSTNAME = "someFakeHostname";

  @Rule
  public GfshParserRule commandRule = new GfshParserRule();

  private StartLocatorCommand spy;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    spy = Mockito.spy(StartLocatorCommand.class);

    Gfsh gfsh = mock(Gfsh.class);
    doReturn(gfsh).when(spy).getGfsh();
    doReturn(mock(AbstractSignalNotificationHandler.class)).when(gfsh).getSignalHandler();
  }

  @Test
  public void startLocatorWorksWithNoOptions() throws Exception {
    doReturn(true).when(spy).isConfigurationExists(any());

    commandRule.executeAndAssertThat(spy, "start locator");

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartLocatorCommandLine(any(), any(), any(),
        gemfirePropertiesCaptor.capture(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(ENABLE_CLUSTER_CONFIGURATION);
    assertThat(gemfireProperties.get(ENABLE_CLUSTER_CONFIGURATION)).isEqualTo("true");
  }

  @Test
  public void startLocatorRespectsJmxManagerHostnameForClients() throws Exception {
    String startLocatorCommand = new CommandStringBuilder("start locator")
        .addOption(JMX_MANAGER_HOSTNAME_FOR_CLIENTS, FAKE_HOSTNAME).toString();

    doReturn(true).when(spy).isConfigurationExists(any());

    commandRule.executeAndAssertThat(spy, startLocatorCommand);

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartLocatorCommandLine(any(), any(), any(),
        gemfirePropertiesCaptor.capture(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(JMX_MANAGER_HOSTNAME_FOR_CLIENTS);
    assertThat(gemfireProperties.get(JMX_MANAGER_HOSTNAME_FOR_CLIENTS)).isEqualTo(FAKE_HOSTNAME);
  }

  @Test
  public void startLocatorWithClusterConfigDirMakesLoadOptional() throws Exception {
    String startLocatorCommand = new CommandStringBuilder("start locator")
        .addOption("cluster-config-dir", temporaryFolder.newFolder("tmp").getAbsolutePath())
        .toString();

    doReturn(true).when(spy).isConfigurationExists(any());

    commandRule.executeAndAssertThat(spy, startLocatorCommand);

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartLocatorCommandLine(any(), any(), any(),
        gemfirePropertiesCaptor.capture(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(LOAD_CLUSTER_CONFIGURATION_FROM_DIR);
    assertThat(gemfireProperties.get(LOAD_CLUSTER_CONFIGURATION_FROM_DIR)).isEqualTo("true");
  }

  @Test
  public void startLocatorShouldFailIfNoClusterConfigFound() throws Exception {
    String startLocatorCommand = new CommandStringBuilder("start locator")
        .addOption("cluster-config-dir", temporaryFolder.newFolder("tmp").getAbsolutePath())
        .toString();

    commandRule.executeAndAssertThat(spy, startLocatorCommand).statusIsError().containsOutput(
        "No cluster configuration is found or missing config for certain groups in");
  }

  @Test
  public void startLocatorShouldFailIfAnyGroupXmlIsMissing() throws Exception {
    File config = temporaryFolder.getRoot();
    File cluster = temporaryFolder.newFolder("cluster");
    File sg1 = temporaryFolder.newFolder("sg1");
    File sg2 = temporaryFolder.newFolder("sg2");
    File clusterXML = temporaryFolder.newFile("cluster/cluster.xml");
    File sg1Xml = temporaryFolder.newFile("sg1/sg1.xml");


    temporaryFolder.create();
    String startLocatorCommand = new CommandStringBuilder("start locator")
        .addOption("cluster-config-dir", config.getAbsolutePath()).toString();

    commandRule.executeAndAssertThat(spy, startLocatorCommand).statusIsError().containsOutput(
        "No cluster configuration is found or missing config for certain groups in");
  }
}
