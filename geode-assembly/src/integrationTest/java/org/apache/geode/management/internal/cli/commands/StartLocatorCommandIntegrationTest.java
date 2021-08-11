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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class StartLocatorCommandIntegrationTest {
  private static final String FAKE_HOSTNAME = "someFakeHostname";

  @Rule
  public GfshParserRule commandRule = new GfshParserRule();

  private StartLocatorCommand spy;

  @Before
  public void before() throws IOException {
    final Process process = mock(Process.class);
    when(process.getInputStream()).thenReturn(mock(InputStream.class));
    when(process.getErrorStream()).thenReturn(mock(InputStream.class));
    when(process.getOutputStream()).thenReturn(mock(OutputStream.class));

    spy = Mockito.spy(StartLocatorCommand.class);
    doReturn(process).when(spy).getProcess(any(), any());
    doReturn(mock(Gfsh.class)).when(spy).getGfsh();
  }

  @Test
  public void startLocatorWorksWithNoOptions() throws Exception {
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

    commandRule.executeAndAssertThat(spy, startLocatorCommand);

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartLocatorCommandLine(any(), any(), any(),
        gemfirePropertiesCaptor.capture(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(JMX_MANAGER_HOSTNAME_FOR_CLIENTS);
    assertThat(gemfireProperties.get(JMX_MANAGER_HOSTNAME_FOR_CLIENTS)).isEqualTo(FAKE_HOSTNAME);
  }

  @Test
  public void startWithBindAddress() throws Exception {
    commandRule.executeAndAssertThat(spy, "start locator --bind-address=127.0.0.1");

    ArgumentCaptor<String[]> commandLines = ArgumentCaptor.forClass(String[].class);
    verify(spy).getProcess(any(), commandLines.capture());

    String[] lines = commandLines.getValue();
    assertThat(lines[12]).isEqualTo("--bind-address=127.0.0.1");
  }

  @Test
  public void startLocatorRespectsHostnameForClients() throws Exception {
    String startLocatorCommand = new CommandStringBuilder("start locator")
        .addOption("hostname-for-clients", FAKE_HOSTNAME).toString();

    commandRule.executeAndAssertThat(spy, startLocatorCommand);
    ArgumentCaptor<String[]> commandLines = ArgumentCaptor.forClass(String[].class);
    verify(spy).getProcess(any(), commandLines.capture());
    String[] lines = commandLines.getValue();
    assertThat(lines).containsOnlyOnce("--hostname-for-clients=" + FAKE_HOSTNAME);
  }
}
