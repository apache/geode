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

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class ServerStartupValueRecoveryNotificationTest {


  private static final String SERVER_1_NAME = "server1";
  private static final String LOCATOR_NAME = "locator";
  private static final String DISKSTORE_1 = "diskstore1";
  private static final String DISKSTORE_2 = "diskstore2";

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  private Path locatorFolder;
  private Path server1Folder;
  private int locatorPort;
  private String startServer1Command;

  @Before
  public void persistentRegionThatRequiresValueRecovery() throws IOException {
    locatorFolder = temporaryFolder.newFolder(LOCATOR_NAME).toPath().toAbsolutePath();
    server1Folder = temporaryFolder.newFolder(SERVER_1_NAME).toPath().toAbsolutePath();
    Path diskStore1Folder = temporaryFolder.newFolder(DISKSTORE_1).toPath().toAbsolutePath();
    Path diskStore2Folder = temporaryFolder.newFolder(DISKSTORE_2).toPath().toAbsolutePath();
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    locatorPort = ports[0];

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

    String createDiskStore1 = String.join(" ",
        "create disk-store",
        "--name=" + DISKSTORE_1,
        "--dir=" + diskStore1Folder);

    String regionName = "myRegion";
    String createRegionCommand = String.join(" ",
        "create region",
        "--name=" + regionName,
        "--type=REPLICATE_PERSISTENT",
        "--disk-store=" + DISKSTORE_1);

    String createDiskStore2 = String.join(" ",
        "create disk-store",
        "--name=" + DISKSTORE_2,
        "--dir=" + diskStore2Folder);

    String regionNameTwo = "mySecondRegion";
    String createRegionTwoCommand = String.join(" ",
        "create region",
        "--name=" + regionNameTwo,
        "--type=REPLICATE_PERSISTENT",
        "--disk-store=" + DISKSTORE_2);

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

    gfshRule.execute(startLocatorCommand, startServer1Command, createDiskStore1,
        createRegionCommand, createDiskStore2, createRegionTwoCommand, putCommand,
        putCommandInRegionTwo);

    String stopServer1Command = "stop server --dir=" + server1Folder;
    gfshRule.execute(stopServer1Command);
  }

  @After
  public void stopAllMembers() {
    String stopServer1Command = "stop server --dir=" + server1Folder;
    String stopLocatorCommand = "stop locator --dir=" + locatorFolder;
    gfshRule.execute(stopServer1Command, stopLocatorCommand);
  }

  @Test
  public void startupReportsOnlineOnlyAfterRedundancyRestored() throws IOException {
    String connectCommand = "connect --locator=localhost[" + locatorPort + "]";
    server1Folder =
        temporaryFolder.newFolder(SERVER_1_NAME + "secondfolder").toPath().toAbsolutePath();
    startServer1Command = String.join(" ",
        "start server",
        "--name=" + SERVER_1_NAME,
        "--dir=" + server1Folder,
        "--locators=localhost[" + locatorPort + "]");

    gfshRule.execute(connectCommand, startServer1Command);

    Pattern serverOnlinePattern =
        Pattern.compile("^\\[info .*].*Server " + SERVER_1_NAME + " startup completed in \\d+ ms");
    Pattern valuesRecoveredPattern =
        Pattern.compile(
            "^\\[info .*].* Recovered values for disk store " + DISKSTORE_1 + " with unique id .*");
    Pattern valuesRecoveredSecondRegionPattern =
        Pattern.compile(
            "^\\[info .*].* Recovered values for disk store " + DISKSTORE_2 + " with unique id .*");

    Path logFile = server1Folder.resolve(SERVER_1_NAME + ".log");

    await()
        .untilAsserted(() -> {
          final Predicate<String> isRelevantLine = valuesRecoveredPattern.asPredicate()
              .or(valuesRecoveredSecondRegionPattern.asPredicate())
              .or(serverOnlinePattern.asPredicate());

          final List<String> foundPatterns =
              Files.lines(logFile).filter(isRelevantLine)
                  .collect(Collectors.toList());

          assertThat(foundPatterns)
              .as("Log file " + logFile + " includes one line matching each of "
                  + valuesRecoveredPattern + ", " + valuesRecoveredSecondRegionPattern
                  + ", and "
                  + serverOnlinePattern)
              .hasSize(3);

          assertThat(foundPatterns)
              .as("lines in the log file")
              .withFailMessage("%n Expect line matching %s %n but was %s",
                  valuesRecoveredPattern.pattern(), foundPatterns)
              .anyMatch(valuesRecoveredPattern.asPredicate())
              .withFailMessage("%n Expect line matching %s %n but was %s",
                  valuesRecoveredSecondRegionPattern.pattern(), foundPatterns)
              .anyMatch(valuesRecoveredSecondRegionPattern.asPredicate());

          assertThat(foundPatterns.get(2))
              .as("Third matching Log line of " + foundPatterns)
              .matches(serverOnlinePattern.asPredicate(), serverOnlinePattern.pattern());
        });
  }

}
