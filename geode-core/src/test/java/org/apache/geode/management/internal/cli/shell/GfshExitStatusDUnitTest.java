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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.internal.ShellExitCode;
import org.apache.geode.management.internal.cli.commands.LauncherLifecycleCommands;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.LocatorStarterRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

// Originally created in response to GEODE-2971

@Category(DistributedTest.class)
public class GfshExitStatusDUnitTest {

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfshConnection = new GfshShellConnectionRule();

  private MemberVM locator;
  private MemberVM server;

  // These initial values will be overwritten at VM launch, but are instantiated to test gfsh
  // commands that should fail.
  private static String locatorName = "not-actually-the-locator-name";
  private Properties locatorProperties;
  private File locatorDir = new File(System.getProperty("user.dir")).toPath().resolve("not")
      .resolve("the").resolve("locator").resolve("dir").toFile();
  private int locatorPID = 10;
  private int locatorPort = 0;

  private static String serverName = "not-actually-the-server-name";
  private Properties serverProperties;
  private File serverDir = new File(System.getProperty("user.dir")).toPath().resolve("not")
      .resolve("the").resolve("server").resolve("dir").toFile();
  private int serverPID = 10;


  private void connect() throws Exception {
    assertThat(locator).isNotNull();
    gfshConnection.connectAndVerify(locator);
  }


  private void launch(boolean connectToLocator, Status spoofedStatus) throws Exception {
    assertThat(locator).isNull();
    assertThat(server).isNull();
    ServerStarterRule.FakeLauncher.setStatus(spoofedStatus);
    LocatorStarterRule.FakeLauncher.setStatus(spoofedStatus);

    locator = lsRule.startLocatorVM(0, locatorProperties);
    locatorDir = locator.getWorkingDir();
    // TODO: these getPid calls need to be RMIs to the locator's xLauncher,
    // not the local one, to get the correct pid.
    locatorPID =
        locator.getVM().invoke((SerializableCallableIF<Integer>) LocatorStarterRule::getPid);
    locatorPort = locator.getPort();
    locatorName = locator.getName();

    server = lsRule.startServerVM(1, serverProperties, locatorPort);
    serverDir = server.getWorkingDir();
    serverPID = server.getVM().invoke((SerializableCallableIF<Integer>) ServerStarterRule::getPid);
    serverName = server.getName();

    if (connectToLocator) {
      connect();
      Awaitility.await().atMost(60, TimeUnit.SECONDS)
          .until((Gfsh::isCurrentInstanceConnectedAndReady));
      Awaitility.await().atMost(60, TimeUnit.SECONDS)
          .until(() -> LauncherLifecycleCommands.isMemberMXBeanAvailable(serverName));


    }

  }

  @Before
  public void setup() throws Exception {
    locatorProperties = new Properties();
    serverProperties = new Properties();
  }


  // These commands should resolve even when not connected to a locator
  private List<String> getOfflineStatusQueryStrings() {
    return Arrays.asList(String.format("status locator --dir=%s", locatorDir.getAbsolutePath()),
        String.format("status locator --pid=%s", String.valueOf(locatorPID)),
        String.format("status server --dir=%s", serverDir.getAbsolutePath()),
        String.format("status server --pid=%s", String.valueOf(serverPID)));
  }

  // These commands should resolve only when connected to the locator
  private List<String> getOnlineStatusQueryStrings() {
    return Arrays.asList(String.format("status locator --name=%s", locatorName),
        String.format("status server --name=%s", serverName));
  }

  private void makeQueries(int expectedExitValue, List<String> queries) throws Exception {
    SoftAssertions softly = new SoftAssertions();
    for (String q : queries) {
      softly.assertThat(executeGfshCommand(q)).describedAs(q).isEqualTo(expectedExitValue);
    }
    softly.assertAll();
  }

  // Bad queries should fail:
  @Test
  public void queryOfflineWithNoMembers() throws Exception {
    int ExpectedExitCode = ShellExitCode.FATAL_EXIT.getExitCode();
    makeQueries(ExpectedExitCode, getOfflineStatusQueryStrings());
  }

  @Test
  public void queryOnlineWithNoMembers() throws Exception {
    int ExpectedExitCode = ShellExitCode.FATAL_EXIT.getExitCode();
    makeQueries(ExpectedExitCode, getOnlineStatusQueryStrings());
  }

  // The LocatorStarterRule and ServerStarterRule will spoof member status in a --name query,
  // but the filesystem and OS handle --dir and --pid options, so these are not correctly spoofed.
  // --dir and --pid options should succeed even when not connected, --name will require connection.

  @Test
  public void queryOfflineWithMembersLaunched() throws Exception {
    int ExpectedExitCode = ShellExitCode.NORMAL_EXIT.getExitCode();
    launch(false, Status.ONLINE);
    makeQueries(ExpectedExitCode, getOfflineStatusQueryStrings());
  }


  @Test
  public void queryOnlineWithMembersOnline() throws Exception {
    int ExpectedExitCode = ShellExitCode.NORMAL_EXIT.getExitCode();
    launch(true, Status.ONLINE);
    makeQueries(ExpectedExitCode, getOnlineStatusQueryStrings());
  }

  private int executeGfshCommand(String cmd) throws Exception {
    gfshConnection.executeCommand(cmd);
    return gfshConnection.getShellExitcode();
  }

}
