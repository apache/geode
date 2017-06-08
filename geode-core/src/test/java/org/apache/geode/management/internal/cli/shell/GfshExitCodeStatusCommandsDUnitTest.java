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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.SoftAssertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.process.PidFile;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.test.dunit.rules.RealGfshRule;
import org.apache.geode.test.junit.categories.DistributedTest;


// Originally created in response to GEODE-2971
@Category(DistributedTest.class)
public class GfshExitCodeStatusCommandsDUnitTest {
  private static final File GEODE_HOME = new File(System.getenv("GEODE_HOME"));
  private static final String GFSH_PATH = GEODE_HOME.toPath().resolve("bin/gfsh").toString();
  private static final ThreePhraseGenerator nameGenerator = new ThreePhraseGenerator();

  @Rule
  public RealGfshRule realGfshRule = new RealGfshRule(GFSH_PATH);

  private int locatorPort;
  private String locatorName;
  private String serverName;

  @Test
  public void offlineStatusWithInvalidOptionsShouldFail() throws Exception {
    ExitCode expectedExitCode = ExitCode.FATAL;

    String statusLocatorByPID = "status locator --pid=10";
    String statusLocatorByDir = "status locator --dir=some-invalid-dir";
    String statusLocatorByPort = "status locator --port=123";
    String statusLocatorByHostAndPort = "status locator --host=invalid-host-name --port=123";
    String statusServerByPID = "status server --pid=11";
    String statusServerByDir = "status server --dir=some-invalid-dir";

    Process p;
    SoftAssertions softly = new SoftAssertions();
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusLocatorByPID);
    softly.assertThat(p.exitValue()).describedAs(statusLocatorByPID)
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusLocatorByDir);
    softly.assertThat(p.exitValue()).describedAs(statusLocatorByDir)
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusLocatorByPort);
    softly.assertThat(p.exitValue()).describedAs(statusLocatorByPort)
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusLocatorByHostAndPort);
    softly.assertThat(p.exitValue()).describedAs(statusLocatorByHostAndPort)
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusServerByPID);
    softly.assertThat(p.exitValue()).describedAs(statusServerByPID)
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusServerByDir);
    softly.assertThat(p.exitValue()).describedAs(statusServerByDir)
        .isEqualTo(expectedExitCode.getExitCode());

    softly.assertAll();
  }


  @Test
  public void onlineStatusWithInvalidNameShouldFail() throws Exception {
    ExitCode expectedExitCode = ExitCode.FATAL;

    String statusLocator = "status locator --name=invalid-locator-name";
    String statusServer = "status server --name=invalid-server-name";

    Process p;
    SoftAssertions softly = new SoftAssertions();
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusLocator);
    softly.assertThat(p.exitValue()).describedAs(statusLocator)
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusServer);
    softly.assertThat(p.exitValue()).describedAs(statusServer)
        .isEqualTo(expectedExitCode.getExitCode());

    softly.assertAll();
  }

  @Test
  public void offlineStatusWithValidOptionsShouldSucceedWhenNotConnected() throws Exception {
    ExitCode expectedExitCode = ExitCode.NORMAL;
    launchLocatorAndServer();

    Process p;
    SoftAssertions softly = new SoftAssertions();

    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusServerCommandByDir());
    softly.assertThat(p.exitValue()).describedAs(statusServerCommandByDir())
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusServerCommandByPid());
    softly.assertThat(p.exitValue()).describedAs(statusServerCommandByPid())
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusLocatorCommandByDir());
    softly.assertThat(p.exitValue()).describedAs(statusLocatorCommandByDir())
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusLocatorCommandByPid());
    softly.assertThat(p.exitValue()).describedAs(statusLocatorCommandByPid())
        .isEqualTo(expectedExitCode.getExitCode());

    softly.assertAll();

  }

  @Test
  public void offlineStatusWithValidOptionsShouldSucceedWhenConnected() throws Exception {
    ExitCode expectedExitCode = ExitCode.NORMAL;
    launchLocatorAndServer();

    Process p;
    SoftAssertions softly = new SoftAssertions();

    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, connectCommand(),
        statusServerCommandByDir());
    softly.assertThat(p.exitValue()).describedAs(statusServerCommandByDir())
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, connectCommand(),
        statusServerCommandByPid());
    softly.assertThat(p.exitValue()).describedAs(statusServerCommandByPid())
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, connectCommand(),
        statusLocatorCommandByDir());
    softly.assertThat(p.exitValue()).describedAs(statusLocatorCommandByDir())
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, connectCommand(),
        statusLocatorCommandByPid());
    softly.assertThat(p.exitValue()).describedAs(statusLocatorCommandByPid())
        .isEqualTo(expectedExitCode.getExitCode());

    softly.assertAll();
  }

  @Test
  public void onlineStatusWithValidOptionsShouldFailWhenNotConnected() throws Exception {
    ExitCode expectedExitCode = ExitCode.FATAL;
    launchLocatorAndServer();

    Process p;
    SoftAssertions softly = new SoftAssertions();
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, statusServerCommandByName());
    softly.assertThat(p.exitValue()).describedAs(statusServerCommandByName())
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES,
        statusLocatorCommandByName());
    softly.assertThat(p.exitValue()).describedAs(statusLocatorCommandByName())
        .isEqualTo(expectedExitCode.getExitCode());

    softly.assertAll();

  }

  @Test
  public void onlineStatusWithValidOptionsShouldSucceedWhenConnected() throws Exception {
    ExitCode expectedExitCode = ExitCode.NORMAL;
    launchLocatorAndServer();

    Process p;
    SoftAssertions softly = new SoftAssertions();
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, connectCommand(),
        statusServerCommandByName());
    softly.assertThat(p.exitValue()).describedAs(statusServerCommandByName())
        .isEqualTo(expectedExitCode.getExitCode());
    p = realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, connectCommand(),
        statusLocatorCommandByName());
    softly.assertThat(p.exitValue()).describedAs(statusLocatorCommandByName())
        .isEqualTo(expectedExitCode.getExitCode());

    softly.assertAll();

  }


  private String startLocatorCommand() {
    locatorName = nameGenerator.generate('-');
    locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    return new CommandStringBuilder("start locator").addOption("name", locatorName)
        .addOption("port", String.valueOf(locatorPort)).toString();
  }

  private String startServerCommand() {
    serverName = nameGenerator.generate('-');
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
    String serverDir = realGfshRule.getTemporaryFolder().getRoot().toPath().resolve(serverName)
        .toAbsolutePath().toString();
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

  private String statusLocatorCommandByDir() {
    String locatorDir = realGfshRule.getTemporaryFolder().getRoot().toPath().resolve(locatorName)
        .toAbsolutePath().toString();
    return new CommandStringBuilder("status locator").addOption("dir", locatorDir).toString();
  }

  private String statusLocatorCommandByPid() throws IOException {
    int locatorPid = snoopMemberFile(locatorName, "locator.pid");
    return new CommandStringBuilder("status locator").addOption("pid", String.valueOf(locatorPid))
        .toString();
  }


  private void launchLocatorAndServer() throws IOException, InterruptedException {
    // Bring up a locator, wait for it, and bring up a server connected to the locator
    Process p = realGfshRule.executeCommandsAndWaitAtMost(2, TimeUnit.MINUTES,
        startLocatorCommand(), startServerCommand());
    if (p.exitValue() != 0) {
      throw new RuntimeException(
          "The locator and server launch exited with non-zero exit code.  This failure is beyond the scope of this test.");
    }
  }


  private int snoopMemberFile(String memberName, String pidFileEndsWith) throws IOException {
    File directory =
        realGfshRule.getTemporaryFolder().getRoot().toPath().resolve(memberName).toFile();
    File pidFile = Arrays.stream(directory.listFiles())
        .filter(file -> file.getName().endsWith(pidFileEndsWith)).findFirst()
        .orElseThrow(() -> new RuntimeException(String
            .format("Expected member '%s' to have pid file but could not find it.", memberName)));
    return new PidFile(pidFile).readPid();
  }
}
