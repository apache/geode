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

import static java.util.Arrays.stream;
import static org.apache.geode.management.internal.cli.shell.DirectoryTree.printDirectoryTree;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.process.PidFile;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

/**
 * See also org.apache.geode.management.internal.cli.shell.StatusServerExitCodeAcceptanceTest
 */
@Category(GfshTest.class)
public class StatusLocatorExitCodeAcceptanceTest {

  private static final String LOCATOR_NAME = "myLocator";

  private static int locatorPort;
  private static Path toolsJar;
  private static int locatorPid;
  private static Path locatorDir;
  private static Path rootPath;
  private static String connectCommand;

  @ClassRule
  public static GfshRule gfshRule = new GfshRule();

  @BeforeClass
  public static void startLocator() throws IOException {
    rootPath = gfshRule.getTemporaryFolder().getRoot().toPath();
    locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();

    GfshExecution execution = GfshScript.of(
        "start locator --name=" + LOCATOR_NAME + " --port=" + locatorPort)
        .execute(gfshRule);

    assertThat(execution.getProcess().exitValue())
        .isZero();

    locatorPid = readPidFile(LOCATOR_NAME, "locator.pid");
    locatorDir = rootPath.resolve(LOCATOR_NAME).toAbsolutePath();

    connectCommand = "connect --locator=[" + locatorPort + "]";
  }

  @BeforeClass
  public static void setUpJavaTools() {
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
  public void statusCommandWithInvalidPortShouldFail() {
    String commandWithBadPort = "status locator --port=-10";

    GfshScript.of(commandWithBadPort)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void statusCommandWithInvalidOptionValueShouldFail() {
    String commandWithBadPid = "status locator --pid=-1";

    GfshScript.of(commandWithBadPid)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void statusCommandWithIncorrectHostShouldFail() {
    String commandWithWrongHostname = "status locator --host=someIncorrectHostname";

    GfshScript.of(commandWithWrongHostname)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void statusCommandWithIncorrectPortShouldFail() {
    int incorrectPort = AvailablePortHelper.getRandomAvailableTCPPort();
    String commandWithWrongPort = "status locator --port=" + incorrectPort;

    GfshScript.of(commandWithWrongPort)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void statusCommandWithIncorrectDirShouldFail() {
    String commandWithWrongDir = "status locator --dir=.";

    GfshScript.of(commandWithWrongDir)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void statusCommandWithIncorrectNameShouldFail() {
    String commandWithWrongName = "status locator --name=some-locator-name";

    GfshScript.of(commandWithWrongName)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_locator_name() {
    String statusCommand = "status locator --name=" + LOCATOR_NAME;

    GfshScript.of(connectCommand, statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_locator_port() {
    String statusCommand = "status locator --port=" + locatorPort;

    GfshScript.of(connectCommand, statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_locator_host_and_port() {
    String statusCommand = "status locator --host=localhost --port=" + locatorPort;

    GfshScript.of(connectCommand, statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void onlineStatusCommandShouldFailWhenConnectedNonDefaultPort_locator_host() {
    String statusCommand = "status locator --host=localhost";

    GfshScript.of(connectCommand, statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_locator_dir() {
    String statusCommand = "status locator --dir=" + locatorDir;

    GfshScript.of(connectCommand, statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void onlineStatusCommandShouldFailWhenNotConnected_locator_name() {
    String statusCommand = "status locator --name=" + LOCATOR_NAME;

    GfshScript.of(statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.FATAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenNotConnected_locator_port() {
    // --host defaults to localhost, so `status locator --port=xxx` should still succeed.
    String statusCommand = "status locator --port=" + locatorPort;

    GfshScript.of(statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_locator_pid() {
    String statusCommand = "status locator --pid=" + locatorPid;

    GfshScript.of(connectCommand, statusCommand)
        .withName("test-frame")
        .addToClasspath(toolsJar.toFile().getAbsolutePath())
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenNotConnected_locator_host_and_port() {
    // Since this is still local to the testing VM's machine, `status locator --host=localhost
    // --port=xxx` should succeed
    String statusCommand = "status locator --host=localhost --port=" + locatorPort;

    GfshScript.of(statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_locator_dir() {
    String statusCommand = "status locator --dir=" + locatorDir;

    GfshScript.of(statusCommand)
        .withName("test-frame")
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_locator_pid() {
    String statusCommand = "status locator --pid=" + locatorPid;

    GfshScript.of(statusCommand)
        .withName("test-frame")
        .addToClasspath(toolsJar.toFile().getAbsolutePath())
        .expectExitCode(ExitCode.NORMAL.getValue())
        .execute(gfshRule);
  }

  private static int readPidFile(String memberName, String pidFileEndsWith) throws IOException {
    File directory = rootPath.resolve(memberName).toFile();
    File[] files = directory.listFiles();

    assertThat(files)
        .as(String.format("Expected directory ('%s') for member '%s'.", directory, memberName))
        .isNotNull();

    File pidFile = stream(files)
        .filter(file -> file.getName().endsWith(pidFileEndsWith))
        .findFirst()
        .orElseThrow(() -> new RuntimeException(String
            .format("Expected member '%s' to have pid file but could not find it.", memberName)));

    return new PidFile(pidFile).readPid();
  }
}
