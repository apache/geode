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
package org.apache.geode.management.internal.cli.shell;

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.process.ProcessType.SERVER;
import static org.apache.geode.management.internal.cli.shell.DirectoryTree.printDirectoryTree;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.ExitCode;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

/**
 * See also org.apache.geode.management.internal.cli.shell.StatusLocatorExitCodeAcceptanceTest
 */
@Category(GfshTest.class)
public class StatusServerExitCodeAcceptanceTest {

  private static final String LOCATOR_NAME = "myLocator";
  private static final String SERVER_NAME = "myServer";

  private Path toolsJar;
  private int serverPid;
  private Path serverDir;
  private String connectCommand;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  @Before
  public void startCluster() {
    int locatorPort = getRandomAvailableTCPPort();

    GfshExecution execution = GfshScript
        .of("start locator --name=" + LOCATOR_NAME + " --port=" + locatorPort,
            "start server --disable-default-server --name=" + SERVER_NAME)
        .execute(gfshRule);

    assertThat(execution.getProcess().exitValue())
        .isZero();

    serverDir = execution.getWorkingDir().toPath().resolve(SERVER_NAME);
    serverPid = SERVER.readPid(serverDir);

    connectCommand = "connect --locator=[" + locatorPort + "]";
  }

  @Before
  public void setUpJavaTools() {
    String javaHome = System.getProperty("java.home");
    assertThat(javaHome)
        .as("System.getProperty(\"java.home\")")
        .isNotNull();

    Path javaHomeFile = new File(javaHome).toPath();
    assertThat(javaHomeFile)
        .as(javaHomeFile + ": " + printDirectoryTree(javaHomeFile.toFile()))
        .exists();

    String toolsPath = javaHomeFile.toFile().getName().equalsIgnoreCase("jre")
        ? ".." + File.separator + "lib" + File.separator + "tools.jar"
        : "lib" + File.separator + "tools.jar";
    toolsJar = javaHomeFile.resolve(toolsPath);
  }

  @Test
  public void statusCommandWithInvalidOptionValueShouldFail() {
    String commandWithBadPid = "status server --pid=-1";

    GfshScript
        .of(commandWithBadPid)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void statusCommandWithIncorrectDirShouldFail() {
    String commandWithWrongDir = "status server --dir=.";

    GfshScript
        .of(commandWithWrongDir)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void statusCommandWithIncorrectNameShouldFail() {
    String commandWithWrongName = "status server --name=some-server-name";

    GfshScript
        .of(commandWithWrongName)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void statusCommandWithIncorrectPidShouldFail() {
    String commandWithWrongPid = "status server --pid=100";

    GfshScript
        .of(commandWithWrongPid)
        .withName("test-frame")
        .addToClasspath(toolsJar.toFile().getAbsolutePath())
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void onlineStatusCommandShouldFailWhenNotConnected_server_name() {
    String statusCommand = "status server --name=" + SERVER_NAME;

    GfshScript
        .of(statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_server_name() {
    String statusCommand = "status server --name=" + SERVER_NAME;

    GfshScript
        .of(connectCommand, statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_server_dir() {
    String statusCommand = "status server --dir=" + serverDir;

    GfshScript
        .of(connectCommand, statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_server_pid() {
    String statusCommand = "status server --pid=" + serverPid;

    GfshScript
        .of(connectCommand, statusCommand)
        .withName("test-frame")
        .addToClasspath(toolsJar.toFile().getAbsolutePath())
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_server_dir() {
    String statusCommand = "status server --dir=" + serverDir;

    GfshScript
        .of(statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_server_pid() {
    String statusCommand = "status server --pid=" + serverPid;

    GfshScript
        .of(statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .addToClasspath(toolsJar.toFile().getAbsolutePath())
        .execute(gfshRule);
  }
}
