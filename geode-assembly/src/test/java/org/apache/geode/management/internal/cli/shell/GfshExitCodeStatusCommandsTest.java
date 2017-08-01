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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.process.PidFile;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.test.dunit.rules.gfsh.GfshExecution;
import org.apache.geode.test.dunit.rules.gfsh.GfshRule;
import org.apache.geode.test.dunit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.categories.AcceptanceTest;

// Originally created in response to GEODE-2971

@Category(AcceptanceTest.class)
@RunWith(JUnitParamsRunner.class)
public class GfshExitCodeStatusCommandsTest {
  private static File toolsJar;
  private static final ThreePhraseGenerator nameGenerator = new ThreePhraseGenerator();
  private static final String memberControllerName = "member-controller";

  @Rule
  public GfshRule gfsh = new GfshRule();
  private String locatorName;
  private String serverName;

  private int locatorPort;

  // Some test configuration shorthands
  private static final TestConfiguration LOCATOR_ONLINE_BUT_NOT_CONNECTED =
      new TestConfiguration(true, false, false);
  private static final TestConfiguration LOCATOR_ONLINE_AND_CONNECTED =
      new TestConfiguration(true, false, true);
  private static final TestConfiguration BOTH_ONLINE_BUT_NOT_CONNECTED =
      new TestConfiguration(true, true, false);
  private static final TestConfiguration BOTH_ONLINE_AND_CONNECTED =
      new TestConfiguration(true, true, true);

  @BeforeClass
  public static void classSetup() {
    File javaHome = new File(System.getProperty("java.home"));
    String toolsPath =
        javaHome.getName().equalsIgnoreCase("jre") ? "../lib/tools.jar" : "lib/tools.jar";
    toolsJar = new File(javaHome, toolsPath);
  }

  @Before
  public void setup() {
    locatorName = "locator-" + nameGenerator.generate('-');
    serverName = "server-" + nameGenerator.generate('-');
    locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
  }

  @Test
  @Parameters(
      value = {"status locator --port=-10", "status locator --pid=-1", "status server --pid=-1"})
  public void statusCommandWithInvalidOptionValueShouldFail(String cmd) {
    GfshScript.of(cmd).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }


  @Test
  @Parameters(value = {"status locator --host=somehostname", "status locator --port=10334",
      "status locator --dir=.", "status server --dir=.", "status locator --name=some-locator-name",
      "status server --name=some-server-name", "status locator --pid=100",
      "status server --pid=100"})
  public void statusCommandWithValidOptionValueShouldFailWithNoMembers(String cmd) {
    GfshScript.of(cmd).withName("test-frame").awaitAtMost(1, MINUTES)
        .expectExitCode(ExitCode.FATAL.getValue()).execute(gfsh);
  }


  @Test
  public void onlineStatusCommandShouldFailWhenNotConnected_locator_name() {
    TestConfiguration config = LOCATOR_ONLINE_BUT_NOT_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusLocatorCommandByName();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.FATAL);
  }


  @Test
  public void onlineStatusCommandShouldFailWhenNotConnected_server_name() {
    TestConfiguration config = BOTH_ONLINE_BUT_NOT_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusServerCommandByName();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.FATAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenNotConnected_locator_port() {
    // --host defaults to localhost, so `status locator --port=xxx` should still succeed.
    TestConfiguration config = LOCATOR_ONLINE_BUT_NOT_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusLocatorCommandByPort();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenNotConnected_locator_host_and_port() {
    // Since this is still local to the testing VM's machine, `status locator --host=localhost
    // --port=xxx` should succeed
    TestConfiguration config = LOCATOR_ONLINE_BUT_NOT_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusLocatorCommandByHostAndPort();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }



  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_locator_name() {
    TestConfiguration config = LOCATOR_ONLINE_AND_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusLocatorCommandByName();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }


  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_server_name() {
    TestConfiguration config = BOTH_ONLINE_AND_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusServerCommandByName();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }

  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_locator_port() {
    TestConfiguration config = LOCATOR_ONLINE_AND_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusLocatorCommandByPort();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }

  @Test
  public void onlineStatusCommandShouldSucceedWhenConnected_locator_host_and_port() {
    TestConfiguration config = LOCATOR_ONLINE_AND_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusLocatorCommandByHostAndPort();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }



  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_locator_dir() {
    TestConfiguration config = LOCATOR_ONLINE_AND_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusLocatorCommandByDir();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_server_dir() {
    TestConfiguration config = BOTH_ONLINE_AND_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusServerCommandByDir();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_locator_pid() throws IOException {
    Assume.assumeTrue(toolsJar.exists());
    TestConfiguration config = LOCATOR_ONLINE_AND_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusLocatorCommandByPid();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedWhenConnected_server_pid() throws IOException {
    Assume.assumeTrue(toolsJar.exists());
    TestConfiguration config = BOTH_ONLINE_AND_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusServerCommandByPid();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }



  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_locator_dir() {
    TestConfiguration config = LOCATOR_ONLINE_BUT_NOT_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusLocatorCommandByDir();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_server_dir() {
    TestConfiguration config = BOTH_ONLINE_BUT_NOT_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusServerCommandByDir();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_locator_pid()
      throws IOException {
    Assume.assumeTrue(toolsJar.exists());
    TestConfiguration config = LOCATOR_ONLINE_BUT_NOT_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusLocatorCommandByPid();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }

  @Test
  public void offlineStatusCommandShouldSucceedEvenWhenNotConnected_server_pid()
      throws IOException {
    Assume.assumeTrue(toolsJar.exists());
    TestConfiguration config = BOTH_ONLINE_BUT_NOT_CONNECTED;
    config.startNecessaryMembers(startLocatorCommand(), startServerCommand(), gfsh);

    String statusCommand = statusServerCommandByPid();
    executeScriptWithExpectedExitCode(statusCommand, config, ExitCode.NORMAL);
  }



  private String startLocatorCommand() {
    return new CommandStringBuilder("start locator").addOption("name", locatorName)
        .addOption("port", String.valueOf(locatorPort)).toString();
  }


  private String startServerCommand() {
    return new CommandStringBuilder("start server").addOption("name", serverName).toString();
  }


  private String connectCommand() {
    return new CommandStringBuilder("connect")
        .addOption("locator", String.format("localhost[%d]", locatorPort)).toString();
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

  private void executeScriptWithExpectedExitCode(String statusCommand, TestConfiguration config,
      ExitCode expectedExit) {

    String[] gfshScriptCommands = config.connectedToLocator
        ? new String[] {connectCommand(), statusCommand} : new String[] {statusCommand};
    GfshScript gfshScript = GfshScript.of(gfshScriptCommands).withName("test-frame")
        .awaitAtMost(1, MINUTES).expectExitCode(expectedExit.getValue());
    if (toolsJar.exists()) {
      gfshScript.addToClasspath(toolsJar.getAbsolutePath());
    }
    gfshScript.execute(gfsh);
  }


  private static class TestConfiguration {
    TestConfiguration(boolean locatorStarted, boolean serverStarted, boolean connectedToLocator) {
      this.locatorStarted = locatorStarted;
      this.serverStarted = serverStarted;
      this.connectedToLocator = connectedToLocator;
    }

    private boolean locatorStarted;
    private boolean serverStarted;
    private boolean connectedToLocator;

    void startNecessaryMembers(String startLocator, String startServer, GfshRule gfsh) {
      if (!locatorStarted && !serverStarted) {
        return;
      }

      List<String> commands = new ArrayList<>();
      if (locatorStarted) {
        commands.add(startLocator);
      }
      if (serverStarted) {
        commands.add(startServer);
      }

      GfshExecution exec = GfshScript.of(commands.toArray(new String[] {}))
          .withName(memberControllerName).awaitAtMost(1, MINUTES).execute(gfsh);
      if (exec.getProcess().exitValue() != 0) {
        throw new RuntimeException(
            "The locator and server launcher exited with non-zero exit code.  This failure is beyond the scope of this test.");
      }
    }
  }
}
