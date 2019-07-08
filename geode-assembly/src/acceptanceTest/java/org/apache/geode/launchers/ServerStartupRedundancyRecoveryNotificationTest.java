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

package org.apache.geode.launchers;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class ServerStartupRedundancyRecoveryNotificationTest {
  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();
  private static final String SERVER_1_NAME = "server1";
  private static final String SERVER_2_NAME = "server2";
  private static final String LOCATOR_NAME = "locator";
  private Path locatorFolder;
  private Path server1Folder;
  private Path server2Folder;
  private int locatorPort;
  private String startServer1Command;

  @Before
  public void setup() throws IOException {
    locatorFolder = temporaryFolder.newFolder(LOCATOR_NAME).toPath().toAbsolutePath();
//    server1Folder = temporaryFolder.newFolder(SERVER_1_NAME).toPath().toAbsolutePath();
    server1Folder = Paths.get("/Users/demery/my-test-folder").toAbsolutePath();
    Files.createDirectories(server1Folder);
    server2Folder = temporaryFolder.newFolder(SERVER_2_NAME).toPath().toAbsolutePath();

    locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + LOCATOR_NAME,
        "--dir=" + locatorFolder,
        "--port=" + locatorPort,
        "--locators=localhost[" + locatorPort + "]"
    );

    startServer1Command = String.join(" ",
        "start server",
        "--name=" + SERVER_1_NAME,
        "--dir=" + server1Folder,
        "--locators=localhost[" + locatorPort + "]",
        "--disable-default-server");

    String startServer2Command = String.join(" ",
        "start server",
        "--name=" + SERVER_2_NAME,
        "--dir=" + server2Folder,
        "--locators=localhost[" + locatorPort + "]",
        "--disable-default-server");

    String regionName = "myRegion";
    String createRegionCommand = String.join(" ",
        "create region",
        "--name=" + regionName,
        "--type=PARTITION_REDUNDANT",
        "--redundant-copies=1"
    );

    String putCommand = String.join(" ",
        "put",
        "--region=" + regionName,
        "--key=James",
        "--value=Bond"
    );

    gfshRule.execute(startLocatorCommand, startServer1Command, startServer2Command,
        createRegionCommand, putCommand);

    String stopServer1Command = "stop server --dir=" + server1Folder;
    gfshRule.execute(stopServer1Command);
  }

  @After
  public void stopAllMembers() throws InterruptedException {
    Thread.sleep(10000);
    String stopServer1Command = "stop server --dir=" + server1Folder;
    String stopServer2Command = "stop server --dir=" + server2Folder;
    String stopLocatorCommand = "stop locator --dir=" + locatorFolder;
    gfshRule.execute(stopServer1Command, stopServer2Command, stopLocatorCommand);
  }

  /**
   * TODO: When we start up server 1 for the second time, we want to assert that "server is online"
   * appears only *after* redundancy has been restored.
   *
   * TODO: To detect when redundancy is recovered, look for a log line like this:
   * <pre>
   * [info 2019/07/08 16:14:39.998 PDT <Pooled Waiting Message Processor 1> tid=0x24] Configured
   * redundancy of 2 copies has been restored to /myRegion
   * </pre>
   *
   * TODO: Use a distinct log file for the each server 1 start, and assert only against the second
   * log file.
   */
  @Test
  public void startupReportsOnlineOnlyAfterRedundancyRecoveryCompletes() {
    String connectCommand = "connect --locator=localhost[" + locatorPort + "]";
    gfshRule.execute(connectCommand, startServer1Command);

    Pattern logLinePattern = Pattern.compile("^\\[info .*].*Server is online.*");
    Path logFile = server1Folder.resolve(SERVER_1_NAME + ".log");
    // TODO: Check that this log line appears *after* the "recovery restored" line.
    GeodeAwaitility.await()
        .untilAsserted(() ->
            assertThat(Files.lines(logFile))
                .as("Log file " + logFile + " includes line matching " + logLinePattern)
                .anyMatch(logLinePattern.asPredicate())
        );
  }
}
