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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class ServerStartupRedundancyRecoveryNotificationTest {

  private static final String SERVER_1_NAME = "server1";
  private static final String SERVER_2_NAME = "server2";
  private static final String LOCATOR_NAME = "locator";

  @Rule
  public GfshRule gfshRule = new GfshRule();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public TestName testName = new TestName();

  private Path locatorFolder;
  private Path server1Folder;
  private Path server2Folder;
  private int locatorPort;
  private String startServer1Command;
  private String regionName;
  private String regionNameTwo;

  @Before
  public void redundantRegionThatRequiresRedundancyRecovery() throws IOException {
    locatorFolder = temporaryFolder.newFolder(LOCATOR_NAME).toPath().toAbsolutePath();
    server1Folder = temporaryFolder.newFolder(SERVER_1_NAME + "_before").toPath()
        .toAbsolutePath();
    server2Folder = temporaryFolder.newFolder(SERVER_2_NAME).toPath().toAbsolutePath();

    locatorPort = getRandomAvailableTCPPort();

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + LOCATOR_NAME,
        "--dir=" + locatorFolder,
        "--port=" + locatorPort,
        "--locators=localhost[" + locatorPort + "]");

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

    regionName = "myRegion";
    String createRegionCommand = String.join(" ",
        "create region",
        "--name=" + regionName,
        "--type=PARTITION_REDUNDANT",
        "--redundant-copies=1");

    regionNameTwo = "mySecondRegion";
    String createRegionTwoCommand = String.join(" ",
        "create region",
        "--name=" + regionNameTwo,
        "--type=PARTITION_REDUNDANT",
        "--redundant-copies=1");

    String putCommand = String.join(" ",
        "put",
        "--region=" + regionName,
        "--key=James",
        "--value=Bond");

    String putCommandInRegionTwo = String.join(" ",
        "put",
        "--region=" + regionNameTwo,
        "--key=Derrick",
        "--value=Flint");

    gfshRule.execute(startLocatorCommand, startServer1Command, startServer2Command,
        createRegionCommand, createRegionTwoCommand, putCommand, putCommandInRegionTwo);

    String stopServer1Command = "stop server --dir=" + server1Folder;
    gfshRule.execute(stopServer1Command);
  }

  @After
  public void stopAllMembers() {
    String stopServer1Command = "stop server --dir=" + server1Folder;
    String stopServer2Command = "stop server --dir=" + server2Folder;
    String stopLocatorCommand = "stop locator --dir=" + locatorFolder;
    gfshRule.execute(stopServer1Command, stopServer2Command, stopLocatorCommand);
  }

  @Test
  public void startupReportsOnlineOnlyAfterRedundancyRestored() throws IOException {
    String connectCommand = "connect --locator=localhost[" + locatorPort + "]";
    server1Folder =
        temporaryFolder.newFolder(SERVER_1_NAME + "_test").toPath().toAbsolutePath();
    startServer1Command = String.join(" ",
        "start server",
        "--name=" + SERVER_1_NAME,
        "--dir=" + server1Folder,
        "--locators=localhost[" + locatorPort + "]",
        "--disable-default-server");

    gfshRule.execute(connectCommand, startServer1Command);

    Pattern serverOnlinePattern =
        Pattern.compile("^\\[info .*].*Server " + SERVER_1_NAME + " startup completed in \\d+ ms");
    Pattern redundancyRestoredPattern =
        Pattern.compile(
            "^\\[info .*].*Configured redundancy of 2 copies has been restored to " + SEPARATOR
                + regionName
                + ".*");
    Pattern redundancyRestoredSecondRegionPattern =
        Pattern.compile(
            "^\\[info .*].*Configured redundancy of 2 copies has been restored to " + SEPARATOR
                + regionNameTwo
                + ".*");

    Path logFile = server1Folder.resolve(SERVER_1_NAME + ".log");

    await()
        .untilAsserted(() -> {
          final Predicate<String> isRelevantLine = redundancyRestoredPattern.asPredicate()
              .or(redundancyRestoredSecondRegionPattern.asPredicate())
              .or(serverOnlinePattern.asPredicate());

          final List<String> foundPatterns =
              Files.lines(logFile).filter(isRelevantLine)
                  .collect(Collectors.toList());

          assertThat(foundPatterns)
              .as("Log file " + logFile + " includes one line matching each of "
                  + redundancyRestoredPattern + ", " + redundancyRestoredSecondRegionPattern
                  + ", and "
                  + serverOnlinePattern)
              .hasSize(3);

          assertThat(foundPatterns)
              .as("lines in the log file")
              .withFailMessage("%n Expect line matching %s %n but was %s",
                  redundancyRestoredPattern.pattern(), foundPatterns)
              .anyMatch(redundancyRestoredPattern.asPredicate())
              .withFailMessage("%n Expect line matching %s %n but was %s",
                  redundancyRestoredSecondRegionPattern.pattern(), foundPatterns)
              .anyMatch(redundancyRestoredSecondRegionPattern.asPredicate());

          assertThat(foundPatterns.get(2))
              .as("Third matching Log line of " + foundPatterns)
              .matches(serverOnlinePattern.asPredicate(), serverOnlinePattern.pattern());
        });
  }
}
