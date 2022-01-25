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
package org.apache.geode.cache.persistence;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class MissingDiskStoreAfterServerRestartAcceptanceTest {

  private static final String SERVER_1_NAME = "server1";
  private static final String SERVER_2_NAME = "server2";
  private static final String SERVER_3_NAME = "server3";
  private static final String SERVER_4_NAME = "server4";
  private static final String SERVER_5_NAME = "server5";
  private static final String LOCATOR_NAME = "locator";
  private static final String REGION_NAME_WITH_UNDERSCORE = "_myRegion";

  private Path server4Folder;
  private Path server5Folder;
  private TemporaryFolder temporaryFolder;

  private int locatorPort;

  private String startServer1Command;
  private String startServer2Command;
  private String startServer3Command;
  private String startServer4Command;
  private String startServer5Command;

  private String createRegionWithUnderscoreCommand;
  private String connectToLocatorCommand;
  private String queryCommand;

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Before
  public void setUp() throws Exception {
    temporaryFolder = gfshRule.getTemporaryFolder();
    server4Folder = temporaryFolder.newFolder(SERVER_4_NAME).toPath().toAbsolutePath();
    server5Folder = temporaryFolder.newFolder(SERVER_5_NAME).toPath().toAbsolutePath();

    int[] ports = getRandomAvailableTCPPorts(6);
    locatorPort = ports[0];
    int server1Port = ports[1];
    int server2Port = ports[2];
    int server3Port = ports[3];
    int server4Port = ports[4];
    int server5Port = ports[5];

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + LOCATOR_NAME,
        "--port=" + locatorPort,
        "--locators=localhost[" + locatorPort + "]");

    startServer1Command = String.join(" ",
        "start server",
        "--name=" + SERVER_1_NAME,
        "--locators=localhost[" + locatorPort + "]",
        "--server-port=" + server1Port);

    startServer2Command = String.join(" ",
        "start server",
        "--name=" + SERVER_2_NAME,
        "--locators=localhost[" + locatorPort + "]",
        "--server-port=" + server2Port);

    startServer3Command = String.join(" ",
        "start server",
        "--name=" + SERVER_3_NAME,
        "--locators=localhost[" + locatorPort + "]",
        "--server-port=" + server3Port);

    startServer4Command = String.join(" ",
        "start server",
        "--name=" + SERVER_4_NAME,
        "--dir=" + server4Folder,
        "--locators=localhost[" + locatorPort + "]",
        "--server-port=" + server4Port);

    startServer5Command = String.join(" ",
        "start server",
        "--name=" + SERVER_5_NAME,
        "--dir=" + server5Folder,
        "--locators=localhost[" + locatorPort + "]",
        "--server-port=" + server5Port);

    createRegionWithUnderscoreCommand = String.join(" ",
        "create region",
        "--name=" + REGION_NAME_WITH_UNDERSCORE,
        "--type=PARTITION_REDUNDANT_PERSISTENT",
        "--redundant-copies=1",
        "--enable-synchronous-disk=false");

    connectToLocatorCommand = "connect --locator=localhost[" + locatorPort + "]";

    queryCommand =
        "query --query=\"select * from " + SEPARATOR + REGION_NAME_WITH_UNDERSCORE + "\"";

    gfshRule.execute(startLocatorCommand, startServer1Command, startServer2Command,
        startServer3Command, startServer4Command,
        createRegionWithUnderscoreCommand);
  }

  @Test
  public void serverLauncherUnderscore() throws IOException {
    gfshRule.execute(connectToLocatorCommand, queryCommand);
    gfshRule.execute(connectToLocatorCommand, "stop server --name=" + SERVER_4_NAME);
    assertThat(
        gfshRule.execute(connectToLocatorCommand, "show missing-disk-stores").getOutputText())
            .contains("Missing Disk Stores");

    Path server4Path = Paths.get(String.valueOf(server4Folder));
    Path server5Path = Paths.get(String.valueOf(server5Folder));
    Files.move(server4Path, server5Path, StandardCopyOption.REPLACE_EXISTING);

    gfshRule.execute(startServer5Command);

    await().untilAsserted(() -> {
      String waitingForMembersMessage = String.format(
          "Server server5 startup completed in");

      LogFileAssert.assertThat(server5Folder.resolve(SERVER_5_NAME + ".log").toFile())
          .exists()
          .contains(waitingForMembersMessage);
    });

    String showDiskStoresOutput =
        gfshRule.execute(connectToLocatorCommand, "show missing-disk-stores").getOutputText();
    assertThat(showDiskStoresOutput).contains("No missing disk store found");
  }
}
