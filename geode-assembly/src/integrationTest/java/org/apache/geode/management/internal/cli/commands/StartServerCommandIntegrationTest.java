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

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class StartServerCommandIntegrationTest {
  private static final String FAKE_HOSTNAME = "someFakeHostname";

  @Rule
  public GfshParserRule commandRule = new GfshParserRule();

  private StartServerCommand spy;

  @Before
  public void before() {
    spy = Mockito.spy(StartServerCommand.class);
    doReturn(mock(Gfsh.class)).when(spy).getGfsh();
  }

  @Test
  public void startServerWorksWithNoOptions() throws Exception {
    commandRule.executeAndAssertThat(spy, "start server");

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartServerCommandLine(any(), any(), any(), gemfirePropertiesCaptor.capture(),
        any(), any(), any(), any(), any(), any(), anyBoolean());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(USE_CLUSTER_CONFIGURATION);
    assertThat(gemfireProperties.get(USE_CLUSTER_CONFIGURATION)).isEqualTo("true");
  }

  @Test
  public void startServerRespectsJmxManagerHostnameForClients() throws Exception {
    String startServerCommand = new CommandStringBuilder("start server")
        .addOption(JMX_MANAGER_HOSTNAME_FOR_CLIENTS, FAKE_HOSTNAME).toString();

    commandRule.executeAndAssertThat(spy, startServerCommand);

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartServerCommandLine(any(), any(), any(), gemfirePropertiesCaptor.capture(),
        any(), any(), any(), any(), any(), any(), anyBoolean());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(JMX_MANAGER_HOSTNAME_FOR_CLIENTS);
    assertThat(gemfireProperties.get(JMX_MANAGER_HOSTNAME_FOR_CLIENTS)).isEqualTo(FAKE_HOSTNAME);
  }

  @Test
  public void startServerRespectsHostnameForClients() throws Exception {
    String startServerCommand = new CommandStringBuilder("start server")
        .addOption("hostname-for-clients", FAKE_HOSTNAME).toString();

    commandRule.executeAndAssertThat(spy, startServerCommand);
    ArgumentCaptor<String[]> commandLines = ArgumentCaptor.forClass(String[].class);
    verify(spy).getProcess(any(), commandLines.capture());
    String[] lines = commandLines.getValue();
    assertThat(lines).containsOnlyOnce("--hostname-for-clients=" + FAKE_HOSTNAME);
  }
}
