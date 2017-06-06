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

import java.io.File;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.LocatorStarterRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


// Originally created in response to GEODE-2971
@Category(DistributedTest.class)
public class GfshExitStatusDUnitTest {
  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfshConnection = new GfshShellConnectionRule();

  private MemberVM locator;
  private MemberVM server;

  private File locatorDir;
  private int locatorPID;

  private File serverDir;
  private int serverPID;


  @Test
  public void offlineStatusWithInvalidOptionsShouldFail() throws Exception {
    ExitCode expectedExitCode = ExitCode.FATAL;

    // String statusLocatorByPID = "status locator --pid=10";
    // String statusServerByPID = "status server --pid=11";
    // String statusLocatorByDir = "status locator --dir=some-invalid-dir";
    // String statusServerByDir = "status server --dir=some-invalid-dir";
    String statusLocatorByHostAndPort = "status locator --host=invalid-host-name --port=123";
    String statusLocatorByPort = "status locator --port=123";

    executeAndVerifyStatusCommands(expectedExitCode,
        // statusLocatorByPID, statusServerByPID,
        // statusLocatorByDir, statusServerByDir,
        statusLocatorByHostAndPort, statusLocatorByPort);
  }

  @Test
  public void onlineStatusWithInvalidNameShouldFail() throws Exception {
    ExitCode expectedExitCode = ExitCode.FATAL;

    String statusLocator = "status locator --name=invalid-locator-name";
    String statusServer = "status server --name=invalid-server-name";

    executeAndVerifyStatusCommands(expectedExitCode, statusLocator, statusServer);
  }

  @Test
  public void offlineStatusWithValidOptionsShouldSucceedWhenNotConnected() throws Exception {
    ExitCode expectedExitCode = ExitCode.NORMAL;
    launch(false, Status.ONLINE);

    // String statusLocatorByPID = "status locator --pid=" + String.valueOf(locatorPID);
    // String statusServerByPID = "status server --pid=" + String.valueOf(serverPID);
    // String statusLocatorByDir = "status locator --dir=" + locatorDir.getAbsolutePath();
    // String statusServerByDir = "status server --dir=" + serverDir.getAbsolutePath();
    String statusLocatorByPort = "status locator --port=" + locator.getPort();
    String statusLocatorByHostAndPort =
        new CommandStringBuilder("status locator").addOption("host", localhost())
            .addOption("port", String.valueOf(locator.getPort())).getCommandString();

    executeAndVerifyStatusCommands(expectedExitCode,
        // statusLocatorByPID, statusServerByPID,
        // statusLocatorByDir, statusServerByDir,
        statusLocatorByHostAndPort, statusLocatorByPort);
  }

  @Test
  public void onlineStatusWithValidOptionsShouldFailWhenNotConnected() throws Exception {
    ExitCode expectedExitCode = ExitCode.FATAL;
    launch(false, Status.ONLINE);

    String statusLocator = "status locator --name=" + locator.getName();
    String statusServer = "status server --name=" + server.getName();

    executeAndVerifyStatusCommands(expectedExitCode, statusLocator, statusServer);
  }

  @Test
  public void offlineStatusWithValidOptionsShouldSucceedWhenConnected() throws Exception {
    ExitCode expectedExitCode = ExitCode.NORMAL;
    launch(true, Status.ONLINE);

    // String statusLocatorByPID = "status locator --pid=" + String.valueOf(locatorPID);
    // String statusServerByPID = "status server --pid=" + String.valueOf(serverPID);
    // String statusLocatorByDir = "status locator --dir=" + locatorDir.getAbsolutePath();
    // String statusServerByDir = "status server --dir=" + serverDir.getAbsolutePath();
    String statusLocatorByPort = "status locator --port=" + locator.getPort();
    String statusLocatorByHostAndPort =
        new CommandStringBuilder("status locator").addOption("host", localhost())
            .addOption("port", String.valueOf(locator.getPort())).getCommandString();

    executeAndVerifyStatusCommands(expectedExitCode,
        // statusLocatorByPID, statusServerByPID,
        // statusLocatorByDir, statusServerByDir,
        statusLocatorByHostAndPort, statusLocatorByPort);
  }

  @Test
  public void onlineStatusWithValidOptionsShouldSucceedWhenConnected() throws Exception {
    ExitCode expectedExitCode = ExitCode.NORMAL;
    launch(true, Status.ONLINE);

    String statusLocator = "status locator --name=" + locator.getName();
    String statusServer = "status server --name=" + server.getName();

    executeAndVerifyStatusCommands(expectedExitCode, statusLocator, statusServer);
  }

  private void executeAndVerifyStatusCommands(ExitCode expectedExitCode, String... queries)
      throws Exception {
    SoftAssertions softly = new SoftAssertions();
    for (String q : queries) {
      softly.assertThat(executeGfshCommand(q)).describedAs(q)
          .isEqualTo(expectedExitCode.getExitCode());
    }
    softly.assertAll();
  }

  private int executeGfshCommand(String cmd) throws Exception {
    gfshConnection.executeCommand(cmd);
    return gfshConnection.getShellExitcode();
  }

  private void connect() throws Exception {
    assertThat(locator).isNotNull();
    gfshConnection.connectAndVerify(locator);
  }

  private void launch(boolean connectToLocator, Status spoofedStatus) throws Exception {
    assertThat(locator).isNull();
    assertThat(server).isNull();
    ServerStarterRule.FakeLauncher.setStatus(spoofedStatus);
    LocatorStarterRule.FakeLauncher.setStatus(spoofedStatus);

    locator = lsRule.startLocatorVM(0);
    locatorDir = locator.getWorkingDir();
    // locatorPID =
    // locator.getVM().invoke((SerializableCallableIF<Integer>) LocatorStarterRule::getPid);
    int locatorPort = locator.getPort();

    server = lsRule.startServerVM(1, locatorPort);
    serverDir = server.getWorkingDir();
    // serverPID = server.getVM().invoke((SerializableCallableIF<Integer>)
    // ServerStarterRule::getPid);

    if (connectToLocator) {
      connect();
      Awaitility.await().atMost(60, TimeUnit.SECONDS)
          .until((Gfsh::isCurrentInstanceConnectedAndReady));
      Awaitility.await().atMost(60, TimeUnit.SECONDS)
          .until(() -> MXBeanProvider.isMemberMXBeanAvailable(server.getName()));
    }
  }

  private String localhost() throws Exception {
    return InetAddress.getLocalHost().getHostAddress();
  }
}
