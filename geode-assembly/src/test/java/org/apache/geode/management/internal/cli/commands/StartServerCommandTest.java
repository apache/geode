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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class StartServerCommandTest {
  private StartServerCommand serverCommands;

  @Before
  public void setup() {
    serverCommands = new StartServerCommand();
  }

  @After
  public void tearDown() {
    serverCommands = null;
  }

  @Test
  public void testServerClasspathOrder() {
    String userClasspath = "/path/to/user/lib/app.jar:/path/to/user/classes";
    String expectedClasspath =
        StartMemberUtils.getGemFireJarPath().concat(File.pathSeparator).concat(userClasspath)
            .concat(File.pathSeparator).concat(StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);
    String actualClasspath = serverCommands.getServerClasspath(false, userClasspath);
    assertEquals(expectedClasspath, actualClasspath);
  }

  @Test
  public void testCreateServerCommandLine() throws Exception {
    ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .setCommand(ServerLauncher.Command.START).setDisableDefaultServer(true)
        .setMemberName("testCreateServerCommandLine").setRebalance(true).setServerPort(41214)
        .setCriticalHeapPercentage(95.5f).setEvictionHeapPercentage(85.0f)
        .setSocketBufferSize(1024 * 1024).setMessageTimeToLive(93).build();

    String[] commandLineElements = serverCommands.createStartServerCommandLine(serverLauncher, null,
        null, new Properties(), null, false, new String[0], false, null, null);

    assertNotNull(commandLineElements);
    assertTrue(commandLineElements.length > 0);

    Set<String> expectedCommandLineElements = new HashSet<>(6);
    expectedCommandLineElements.add(serverLauncher.getCommand().getName());
    expectedCommandLineElements.add("--disable-default-server");
    expectedCommandLineElements.add(serverLauncher.getMemberName().toLowerCase());
    expectedCommandLineElements.add("--rebalance");
    expectedCommandLineElements
        .add(String.format("--server-port=%1$d", serverLauncher.getServerPort()));
    expectedCommandLineElements.add(String.format("--critical-heap-percentage=%1$s",
        serverLauncher.getCriticalHeapPercentage()));
    expectedCommandLineElements.add(String.format("--eviction-heap-percentage=%1$s",
        serverLauncher.getEvictionHeapPercentage()));
    expectedCommandLineElements
        .add(String.format("--socket-buffer-size=%1$d", serverLauncher.getSocketBufferSize()));
    expectedCommandLineElements
        .add(String.format("--message-time-to-live=%1$d", serverLauncher.getMessageTimeToLive()));

    for (String commandLineElement : commandLineElements) {
      expectedCommandLineElements.remove(commandLineElement.toLowerCase());
    }
    assertTrue(String.format("Expected ([]); but was (%1$s)", expectedCommandLineElements),
        expectedCommandLineElements.isEmpty());
  }

  @Test
  public void testCreateServerCommandLineWithRestAPI() throws Exception {
    ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .setCommand(ServerLauncher.Command.START).setDisableDefaultServer(true)
        .setMemberName("testCreateServerCommandLine").setRebalance(true).setServerPort(41214)
        .setCriticalHeapPercentage(95.5f).setEvictionHeapPercentage(85.0f).build();

    Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(START_DEV_REST_API, "true");
    gemfireProperties.setProperty(HTTP_SERVICE_PORT, "8080");
    gemfireProperties.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");

    String[] commandLineElements = serverCommands.createStartServerCommandLine(serverLauncher, null,
        null, gemfireProperties, null, false, new String[0], false, null, null);

    assertNotNull(commandLineElements);
    assertTrue(commandLineElements.length > 0);

    Set<String> expectedCommandLineElements = new HashSet<>(6);

    expectedCommandLineElements.add(serverLauncher.getCommand().getName());
    expectedCommandLineElements.add("--disable-default-server");
    expectedCommandLineElements.add(serverLauncher.getMemberName().toLowerCase());
    expectedCommandLineElements.add("--rebalance");
    expectedCommandLineElements
        .add(String.format("--server-port=%1$d", serverLauncher.getServerPort()));
    expectedCommandLineElements.add(String.format("--critical-heap-percentage=%1$s",
        serverLauncher.getCriticalHeapPercentage()));
    expectedCommandLineElements.add(String.format("--eviction-heap-percentage=%1$s",
        serverLauncher.getEvictionHeapPercentage()));

    expectedCommandLineElements
        .add("-d" + DistributionConfig.GEMFIRE_PREFIX + "" + START_DEV_REST_API + "=" + "true");
    expectedCommandLineElements
        .add("-d" + DistributionConfig.GEMFIRE_PREFIX + "" + HTTP_SERVICE_PORT + "=" + "8080");
    expectedCommandLineElements.add("-d" + DistributionConfig.GEMFIRE_PREFIX + ""
        + HTTP_SERVICE_BIND_ADDRESS + "=" + "localhost");

    for (String commandLineElement : commandLineElements) {
      expectedCommandLineElements.remove(commandLineElement.toLowerCase());
    }
    assertTrue(String.format("Expected ([]); but was (%1$s)", expectedCommandLineElements),
        expectedCommandLineElements.isEmpty());
  }

  private static final String FAKE_HOSTNAME = "someFakeHostname";

  @Rule
  public GfshParserRule commandRule = new GfshParserRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private StartServerCommand spy;

  @Before
  public void before() throws Exception {
    spy = Mockito.spy(StartServerCommand.class);
    doReturn(mock(Gfsh.class)).when(spy).getGfsh();
  }

  @Test
  public void startServerWorksWithNoOptions() throws Exception {
    thrown.expect(NullPointerException.class);
    commandRule.executeCommandWithInstance(spy, "start server");

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartServerCommandLine(any(), any(), any(), gemfirePropertiesCaptor.capture(),
        any(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(USE_CLUSTER_CONFIGURATION);
    assertThat(gemfireProperties.get(USE_CLUSTER_CONFIGURATION)).isEqualTo("true");
  }

  @Test
  public void startServerRespectsJmxManagerHostnameForClients() throws Exception {
    String startServerCommand = new CommandStringBuilder("start server")
        .addOption(JMX_MANAGER_HOSTNAME_FOR_CLIENTS, FAKE_HOSTNAME).toString();

    thrown.expect(NullPointerException.class);
    commandRule.executeCommandWithInstance(spy, startServerCommand);

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartServerCommandLine(any(), any(), any(), gemfirePropertiesCaptor.capture(),
        any(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(JMX_MANAGER_HOSTNAME_FOR_CLIENTS);
    assertThat(gemfireProperties.get(JMX_MANAGER_HOSTNAME_FOR_CLIENTS)).isEqualTo(FAKE_HOSTNAME);
  }
}
