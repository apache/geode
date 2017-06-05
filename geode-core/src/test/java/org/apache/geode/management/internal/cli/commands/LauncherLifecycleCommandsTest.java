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
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.GfshParserRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.util.Properties;

@Category(IntegrationTest.class)
public class LauncherLifecycleCommandsTest {
  private static final String FAKE_HOSTNAME = "someFakeHostname";

  @Rule
  public GfshParserRule commandRule = new GfshParserRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void startLocatorWorksWithNoOptions() throws Exception {
    LauncherLifecycleCommands spy = spyOnCommand("start locator");
    commandRule.executeLastCommandWithInstance(spy);

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartLocatorCommandLine(any(), any(), any(),
        gemfirePropertiesCaptor.capture(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(ENABLE_CLUSTER_CONFIGURATION);
    assertThat(gemfireProperties.get(ENABLE_CLUSTER_CONFIGURATION)).isEqualTo("true");
  }

  @Test
  public void startServerWorksWithNoOptions() throws Exception {
    LauncherLifecycleCommands spy = spyOnCommand("start server");
    commandRule.executeLastCommandWithInstance(spy);

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartServerCommandLine(any(), any(), any(), gemfirePropertiesCaptor.capture(),
        any(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(USE_CLUSTER_CONFIGURATION);
    assertThat(gemfireProperties.get(USE_CLUSTER_CONFIGURATION)).isEqualTo("true");
  }

  @Test
  public void startLocatorRespectsJmxManagerHostnameForClients() throws Exception {
    String startLocatorCommand = new CommandStringBuilder("start locator")
        .addOption(JMX_MANAGER_HOSTNAME_FOR_CLIENTS, FAKE_HOSTNAME).toString();

    LauncherLifecycleCommands spyCommand = spyOnCommand(startLocatorCommand);
    commandRule.executeLastCommandWithInstance(spyCommand);

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spyCommand).createStartLocatorCommandLine(any(), any(), any(),
        gemfirePropertiesCaptor.capture(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(JMX_MANAGER_HOSTNAME_FOR_CLIENTS);
    assertThat(gemfireProperties.get(JMX_MANAGER_HOSTNAME_FOR_CLIENTS)).isEqualTo(FAKE_HOSTNAME);
  }

  @Test
  public void startServerRespectsJmxManagerHostnameForClients() throws Exception {
    String startServerCommand = new CommandStringBuilder("start server")
        .addOption(JMX_MANAGER_HOSTNAME_FOR_CLIENTS, FAKE_HOSTNAME).toString();

    LauncherLifecycleCommands commandSpy = spyOnCommand(startServerCommand);

    commandRule.executeLastCommandWithInstance(commandSpy);

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(commandSpy).createStartServerCommandLine(any(), any(), any(),
        gemfirePropertiesCaptor.capture(), any(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(JMX_MANAGER_HOSTNAME_FOR_CLIENTS);
    assertThat(gemfireProperties.get(JMX_MANAGER_HOSTNAME_FOR_CLIENTS)).isEqualTo(FAKE_HOSTNAME);
  }

  private LauncherLifecycleCommands spyOnCommand(String command) {
    LauncherLifecycleCommands spy = commandRule.spyCommand(command);
    doReturn(mock(Gfsh.class)).when(spy).getGfsh();
    doReturn(temporaryFolder.getRoot().getAbsolutePath()).when(spy).resolveWorkingDir(any(), any());

    return spy;
  }
}
