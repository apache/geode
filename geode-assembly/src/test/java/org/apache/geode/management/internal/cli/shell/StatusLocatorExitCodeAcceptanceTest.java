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
import org.junit.Rule;
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

/**
 * See also org.apache.geode.management.internal.cli.shell.StatusServerExitCodeAcceptanceTest
 */
@Category(AcceptanceTest.class)
public class StatusLocatorExitCodeAcceptanceTest {
  private static File toolsJar;
  private static final ThreePhraseGenerator nameGenerator = new ThreePhraseGenerator();
  private static final String memberControllerName = "member-controller";

  @ClassRule
  public static GfshRule gfsh = new GfshRule();
  private static String locatorName;

  private static int locatorPort;

  @BeforeClass
  public static void classSetup() {
    File javaHome = new File(System.getProperty("java.home"));
    String toolsPath =
        javaHome.getName().equalsIgnoreCase("jre") ? "../lib/tools.jar" : "lib/tools.jar";
    toolsJar = new File(javaHome, toolsPath);

    locatorName = "locator-" + nameGenerator.generate('-');
    locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    GfshExecution exec = GfshScript.of(startLocatorCommand()).withName(memberControllerName)
        .awaitAtMost(4, MINUTES).execute(gfsh);
    if (exec.getProcess().exitValue() != 0) {
      throw new RuntimeException(
          "The locator and server launcher exited with non-zero exit code.  This failure is beyond the scope of this test.");
    }
  }

  @Test
  public void statusCommandWithInvalidPortShouldFail() {
    String commandWithBadPort = "status locator --port=-10";
    GfshScript.of(commandWithBadPort).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }

  @Test
  public void statusCommandWithInvalidOptionValueShouldFail() {
    String commandWithBadPid = "status locator --pid=-1";
    GfshScript.of(commandWithBadPid).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }

  @Test
  public void statusCommandWithIncorrectHostShouldFail() {
    String commandWithWrongHostname = "status locator --host=someIncorrectHostname";
    GfshScript.of(commandWithWrongHostname).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }

  @Test
  public void statusCommandWithIncorrectPortShouldFail() {
    int incorrectPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    String cmd = "status locator --port=" + incorrectPort;
    GfshScript.of(cmd).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }

  @Test
  public void statusCommandWithIncorrectDirShouldFail() {
    String cmd = "status locator --dir=.";
    GfshScript.of(cmd).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }

  @Test
  public void statusCommandWithIncorrectNameShouldFail() {
    String cmd = "status locator --name=anInvalidMemberName";
    GfshScript.of(cmd).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }

  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_locator_name() {
    String statusCommand = statusLocatorCommandByName();
    executeScriptWithExpectedExitCode(true, statusCommand, ExitCode.NORMAL);
  }

  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_locator_port() {
    String statusCommand = statusLocatorCommandByPort();
    executeScriptWithExpectedExitCode(true, statusCommand, ExitCode.NORMAL);
  }

  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_locator_host_and_port() {
    String statusCommand = statusLocatorCommandByHostAndPort();
    executeScriptWithExpectedExitCode(true, statusCommand, ExitCode.NORMAL);
  }

  @Test
  public void onlineStatusCommandShouldFailWhenConnectedNonDefaultPort_locator_host() {
    String statusCommand = statusLocatorCommandByHost();
    executeScriptWithExpectedExitCode(true, statusCommand, ExitCode.FATAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_locator_dir() {
    String statusCommand = statusLocatorCommandByDir();
    executeScriptWithExpectedExitCode(true, statusCommand, ExitCode.NORMAL);
  }

  @Test
  public void onlineStatusCommandShouldFailWhenNotConnected_locator_name() {
    String statusCommand = statusLocatorCommandByName();
    executeScriptWithExpectedExitCode(false, statusCommand, ExitCode.FATAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenNotConnected_locator_port() {
    // --host defaults to localhost, so `status locator --port=xxx` should still succeed.
    String statusCommand = statusLocatorCommandByPort();
    executeScriptWithExpectedExitCode(false, statusCommand, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_locator_pid() throws IOException {
    Assume.assumeTrue(toolsJar.exists());
    String statusCommand = statusLocatorCommandByPid();
    executeScriptWithExpectedExitCode(true, statusCommand, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenNotConnected_locator_host_and_port() {
    // Since this is still local to the testing VM's machine, `status locator --host=localhost
    // --port=xxx` should succeed

    String statusCommand = statusLocatorCommandByHostAndPort();
    executeScriptWithExpectedExitCode(false, statusCommand, ExitCode.NORMAL);
  }


  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_locator_dir() {
    String statusCommand = statusLocatorCommandByDir();
    executeScriptWithExpectedExitCode(false, statusCommand, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_locator_pid()
      throws IOException {
    Assume.assumeTrue(toolsJar.exists());

    String statusCommand = statusLocatorCommandByPid();
    executeScriptWithExpectedExitCode(false, statusCommand, ExitCode.NORMAL);
  }


  private static String startLocatorCommand() {
    return new CommandStringBuilder("start locator").addOption("name", locatorName)
        .addOption("port", String.valueOf(locatorPort)).toString();
  }

  private String statusLocatorCommandByName() {
    return new CommandStringBuilder("status locator").addOption("name", locatorName).toString();
  }

  private String statusLocatorCommandByPort() {
    return new CommandStringBuilder("status locator").addOption("port", String.valueOf(locatorPort))
        .toString();
  }

  private String statusLocatorCommandByHostAndPort() {
    return new CommandStringBuilder("status locator").addOption("host", "localhost")
        .addOption("port", String.valueOf(locatorPort)).toString();
  }

  private String statusLocatorCommandByHost() {
    return new CommandStringBuilder("status locator").addOption("host", "localhost").toString();
  }

  private String statusLocatorCommandByDir() {
    String locatorDir = gfsh.getTemporaryFolder().getRoot().toPath().resolve(memberControllerName)
        .resolve(locatorName).toAbsolutePath().toString();
    return new CommandStringBuilder("status locator").addOption("dir", locatorDir).toString();
  }


  private String statusLocatorCommandByPid() throws IOException {
    int locatorPid = snoopMemberFile(locatorName, "locator.pid");
    return new CommandStringBuilder("status locator").addOption("pid", String.valueOf(locatorPid))
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
