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

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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

  public String startLocatorCommand() {
    locatorName = nameGenerator.generate('-');
    locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    return new CommandStringBuilder("start locator").addOption("name", locatorName)
        .addOption("port", String.valueOf(locatorPort)).toString();
  }

  public String startServerCommand() {
    serverName = nameGenerator.generate('-');
    return new CommandStringBuilder("start server").addOption("name", serverName).toString();
  }




  public String connectCommand() {
    return new CommandStringBuilder("connect").addOption("port", String.valueOf(locatorPort))
        .toString();
  }




  public String statusServerCommandByName() {
    return new CommandStringBuilder("status server").addOption("name", serverName).toString();
  }



  public String statusServerCommandByDir() {
    String serverDir = realGfshRule.getTemporaryFolder().getRoot()
        .toPath().resolve(serverName).toAbsolutePath().toString();
    return new CommandStringBuilder("status server").addOption("dir", serverDir).toString();
  }

  public String statusServerCommandByPid() throws IOException {
    int serverPid = snoopMemberFile(serverName, "server.pid");
    return new CommandStringBuilder("status server").addOption("name", String.valueOf(serverPid)).toString();
  }






  public String statusLocatorCommandByName() {
    return new CommandStringBuilder("status locator").addOption("name", locatorName).toString();
  }

  public String statusLocatorCommandByDir() {
    String locatorDir = realGfshRule.getTemporaryFolder().getRoot()
        .toPath().resolve(locatorName).toAbsolutePath().toString();
    return new CommandStringBuilder("status locator").addOption("dir", locatorDir).toString();
  }

  public String statusLocatorCommandByPid() throws IOException {
    int locatorPid = snoopMemberFile(locatorName, "locator.pid");
    return new CommandStringBuilder("status locator").addOption("name", String.valueOf(locatorPid)).toString();
  }




  public List<Process> executeStartLocatorAndServer() throws IOException, InterruptedException {
    return Stream.of(
        realGfshRule.executeCommands(startLocatorCommand()),
        realGfshRule.executeCommands(connectCommand(), startServerCommand())).collect(toList());
  }

  public void executeOfflineStatusCommands() throws IOException, InterruptedException {
    Process p = realGfshRule.executeCommands(statusServerCommandByDir());
    Process p2 = realGfshRule.executeCommands(statusServerCommandByPid());
    Process p3 = realGfshRule.executeCommands(statusLocatorCommandByDir());
    Process p4 = realGfshRule.executeCommands(statusLocatorCommandByPid());
  }

  public void executeOnlineStatusCommandsWhileDisconnected() throws IOException, InterruptedException {
    Process p = realGfshRule.executeCommands(statusServerCommandByName());
    Process p2 = realGfshRule.executeCommands(statusLocatorCommandByName());
  }

  public void executeOnlineStatusCommandsWhileConnected() throws IOException, InterruptedException {
    Process p = realGfshRule.executeCommands(connectCommand(), statusServerCommandByName());
    Process p2 = realGfshRule.executeCommands(connectCommand(), statusLocatorCommandByName());
  }





  public int snoopMemberFile(String memberName, String filename) throws IOException {
    File resolved_filename = realGfshRule.getTemporaryFolder().getRoot().toPath()
        .resolve(memberName).resolve(filename).toFile();
    PidFile pidFile = new PidFile(resolved_filename);
    return pidFile.readPid();
  }

  @Test
  public void offlineStatusWithInvalidOptionsShouldFail() throws Exception {
    ExitCode expectedExitCode = ExitCode.FATAL;

    String statusLocatorByPID = "status locator --pid=10";
    String statusServerByPID = "status server --pid=11";
    String statusLocatorByDir = "status locator --dir=some-invalid-dir";
    String statusServerByDir = "status server --dir=some-invalid-dir";
    String statusLocatorByHostAndPort = "status locator --host=invalid-host-name --port=123";
    String statusLocatorByPort = "status locator --port=123";

    executeAndVerifyStatusCommands(expectedExitCode,
        statusLocatorByPID, statusServerByPID,
        statusLocatorByDir, statusServerByDir,
        statusLocatorByHostAndPort, statusLocatorByPort);
  }

  private void executeAndVerifyStatusCommands(ExitCode expectedExitCode, String... queries)
      throws Exception {
    SoftAssertions softly = new SoftAssertions();
    realGfshRule.executeCommandsAndWaitAtMost(1, TimeUnit.MINUTES, queries);
    for (Process p : realGfshRule.getProcesses()) {
      softly.assertThat(p.exitValue()).isEqualTo(expectedExitCode.getExitCode());
    }
    softly.assertAll();
  }


  private String localhost() throws Exception {
    return InetAddress.getLocalHost().getHostAddress();
  }

  @Test
  public void onlineStatusWithInvalidNameShouldFail() throws Exception {
    ExitCode expectedExitCode = ExitCode.FATAL;

    String statusLocator = "status locator --name=invalid-locator-name";
    String statusServer = "status server --name=invalid-server-name";

    executeAndVerifyStatusCommands(expectedExitCode, statusLocator, statusServer);
  }

  // @Test
  // public void offlineStatusWithValidOptionsShouldSucceedWhenNotConnected() throws Exception {
  // ExitCode expectedExitCode = ExitCode.NORMAL;
  // launch(false, Status.ONLINE);
  //
  // // String statusLocatorByPID = "status locator --pid=" + String.valueOf(locatorPID);
  // // String statusServerByPID = "status server --pid=" + String.valueOf(serverPID);
  // // String statusLocatorByDir = "status locator --dir=" + locatorDir.getAbsolutePath();
  // // String statusServerByDir = "status server --dir=" + serverDir.getAbsolutePath();
  // String statusLocatorByPort = "status locator --port=" + locator.getPort();
  // String statusLocatorByHostAndPort =
  // new CommandStringBuilder("status locator").addOption("host", localhost())
  // .addOption("port", String.valueOf(locator.getPort())).getCommandString();
  //
  // executeAndVerifyStatusCommands(expectedExitCode,
  // // statusLocatorByPID, statusServerByPID,
  // // statusLocatorByDir, statusServerByDir,
  // statusLocatorByHostAndPort, statusLocatorByPort);
  // }
  //
  // @Test
  // public void onlineStatusWithValidOptionsShouldFailWhenNotConnected() throws Exception {
  // ExitCode expectedExitCode = ExitCode.FATAL;
  // launch(false, Status.ONLINE);
  //
  // String statusLocator = "status locator --name=" + locator.getName();
  // String statusServer = "status server --name=" + server.getName();
  //
  // executeAndVerifyStatusCommands(expectedExitCode, statusLocator, statusServer);
  // }
  //
  // @Test
  // public void offlineStatusWithValidOptionsShouldSucceedWhenConnected() throws Exception {
  // ExitCode expectedExitCode = ExitCode.NORMAL;
  // launch(true, Status.ONLINE);
  //
  // // String statusLocatorByPID = "status locator --pid=" + String.valueOf(locatorPID);
  // // String statusServerByPID = "status server --pid=" + String.valueOf(serverPID);
  // // String statusLocatorByDir = "status locator --dir=" + locatorDir.getAbsolutePath();
  // // String statusServerByDir = "status server --dir=" + serverDir.getAbsolutePath();
  // String statusLocatorByPort = "status locator --port=" + locator.getPort();
  // String statusLocatorByHostAndPort =
  // new CommandStringBuilder("status locator").addOption("host", localhost())
  // .addOption("port", String.valueOf(locator.getPort())).getCommandString();
  //
  // executeAndVerifyStatusCommands(expectedExitCode,
  // // statusLocatorByPID, statusServerByPID,
  // // statusLocatorByDir, statusServerByDir,
  // statusLocatorByHostAndPort, statusLocatorByPort);
  // }
  //
  // @Test
  // public void onlineStatusWithValidOptionsShouldSucceedWhenConnected() throws Exception {
  // ExitCode expectedExitCode = ExitCode.NORMAL;
  // launch(true, Status.ONLINE);
  //
  // String statusLocator = "status locator --name=" + locator.getName();
  // String statusServer = "status server --name=" + server.getName();

//  executeAndVerifyStatusCommands(expectedExitCode, statusLocator, statusServer);
//
//}

}
