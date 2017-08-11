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
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.test.junit.categories.UnitTest;

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
        serverCommands.getGemFireJarPath().concat(File.pathSeparator).concat(userClasspath)
            .concat(File.pathSeparator).concat(StartMemberCommand.CORE_DEPENDENCIES_JAR_PATHNAME);
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
}
