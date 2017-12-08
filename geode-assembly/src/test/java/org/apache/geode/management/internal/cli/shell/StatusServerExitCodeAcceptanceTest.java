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

import static java.util.concurrent.TimeUnit.MINUTES;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.process.PidFile;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.test.junit.categories.AcceptanceTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

// Originally created in response to GEODE-2971

/**
 * See also org.apache.geode.management.internal.cli.shell.StatusLocatorExitCodeAcceptanceTest
 */
@Category(AcceptanceTest.class)
public class StatusServerExitCodeAcceptanceTest {
  private static File toolsJar;
  private static final ThreePhraseGenerator nameGenerator = new ThreePhraseGenerator();
  private static final String memberControllerName = "member-controller";

  @ClassRule
  public static GfshRule gfsh = new GfshRule();
  private static String locatorName;
  private static String serverName;

  private static int locatorPort;

  @BeforeClass
  public static void classSetup() {
    File javaHome = new File(System.getProperty("java.home"));
    String toolsPath =
        javaHome.getName().equalsIgnoreCase("jre") ? "../lib/tools.jar" : "lib/tools.jar";
    toolsJar = new File(javaHome, toolsPath);

    locatorName = "locator-" + nameGenerator.generate('-');
    serverName = "server-" + nameGenerator.generate('-');
    locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    GfshExecution exec = GfshScript.of(startLocatorCommand(), startServerCommand())
        .withName(memberControllerName).awaitAtMost(2, MINUTES).execute(gfsh);
    if (exec.getProcess().exitValue() != 0) {
      throw new RuntimeException(
          "The locator and server launcher exited with non-zero exit code.  This failure is beyond the scope of this test.");
    }
  }

  @Test
  public void statusCommandWithInvalidOptionValueShouldFail() {
    String commandWithBadPid = "status server --pid=-1";
    GfshScript.of(commandWithBadPid).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }


  @Test
  public void statusCommandWithIncorrectDirShouldFail() {
    String commandWithWrongDir = "status server --dir=.";
    GfshScript.of(commandWithWrongDir).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }

  @Test
  public void statusCommandWithIncorrectNameShouldFail() {
    String commandWithWrongName = "status server --name=some-server-name";
    GfshScript.of(commandWithWrongName).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }

  @Test
  public void statusCommandWithIncorrectPidShouldFail() {
    String commandWithWrongPid = "status server --pid=100";
    GfshScript.of(commandWithWrongPid).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }

  @Test
  public void onlineStatusCommandShouldFailWhenNotConnected_server_name() {
    String statusCommand = statusServerCommandByName();
    executeScriptWithExpectedExitCode(false, statusCommand, ExitCode.FATAL);
  }

  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_server_name() {
    String statusCommand = statusServerCommandByName();
    executeScriptWithExpectedExitCode(true, statusCommand, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_server_dir() {
    String statusCommand = statusServerCommandByDir();
    executeScriptWithExpectedExitCode(true, statusCommand, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_server_pid() throws IOException {
    Assume.assumeTrue(toolsJar.exists());
    String statusCommand = statusServerCommandByPid();
    executeScriptWithExpectedExitCode(true, statusCommand, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_server_dir() {

    String statusCommand = statusServerCommandByDir();
    executeScriptWithExpectedExitCode(false, statusCommand, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_server_pid()
      throws IOException {
    Assume.assumeTrue(toolsJar.exists());
    String statusCommand = statusServerCommandByPid();
    executeScriptWithExpectedExitCode(false, statusCommand, ExitCode.NORMAL);
  }

  private static String startLocatorCommand() {
    return new CommandStringBuilder("start locator").addOption("name", locatorName)
        .addOption("port", String.valueOf(locatorPort)).toString();
  }

  private static String startServerCommand() {
    return new CommandStringBuilder("start server").addOption("name", serverName).toString();
  }

  private String statusServerCommandByName() {
    return new CommandStringBuilder("status server").addOption("name", serverName).toString();
  }

  private String statusServerCommandByDir() {
    String serverDir = gfsh.getTemporaryFolder().getRoot().toPath().resolve(memberControllerName)
        .resolve(serverName).toAbsolutePath().toString();
    return new CommandStringBuilder("status server").addOption("dir", serverDir).toString();
  }

  private String statusServerCommandByPid() throws IOException {
    int serverPid = snoopMemberFile(serverName, "server.pid");
    return new CommandStringBuilder("status server").addOption("pid", String.valueOf(serverPid))
        .toString();
  }

  private String connectCommand() {
    return new CommandStringBuilder("connect")
        .addOption("locator", String.format("localhost[%d]", locatorPort)).toString();
  }

  private int snoopMemberFile(String memberName, String pidFileEndsWith) throws IOException {
    File directory = gfsh.getTemporaryFolder().getRoot().toPath().resolve(memberControllerName)
        .resolve(memberName).toFile();
    File[] files = directory.listFiles();
    if (files == null) {
      throw new RuntimeException(String.format(
          "Expected directory ('%s') for member '%s' either does not denote a directory, or an I/O error occurred.",
          directory.toString(), memberName));
    }
    File pidFile = Arrays.stream(files).filter(file -> file.getName().endsWith(pidFileEndsWith))
        .findFirst().orElseThrow(() -> new RuntimeException(String
            .format("Expected member '%s' to have pid file but could not find it.", memberName)));
    return new PidFile(pidFile).readPid();
  }

  private void executeScriptWithExpectedExitCode(boolean connectToLocator, String statusCommand,
      ExitCode expectedExit) {

    String[] gfshScriptCommands = connectToLocator ? new String[] {connectCommand(), statusCommand}
        : new String[] {statusCommand};
    GfshScript gfshScript = GfshScript.of(gfshScriptCommands).withName("test-frame")
        .awaitAtMost(1, MINUTES).expectExitCode(expectedExit.getValue());
    if (toolsJar.exists()) {
      gfshScript.addToClasspath(toolsJar.getAbsolutePath());
    }
    gfshScript.execute(gfsh);
  }
}
