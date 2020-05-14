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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.cache.client.ClientRegionShortcut.PROXY;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;

import java.nio.file.Path;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class MissingDiskStoreAcceptanceTest {

  private static final String SERVER_1_NAME = "server1";
  private static final String SERVER_2_NAME = "server2";
  private static final String LOCATOR_NAME = "locator";
  private static final String REGION_NAME = "myRegion";

  private ClientCache clientCache;

  private Path locatorFolder;
  private Path server1Folder;
  private Path server2Folder;

  private int locatorPort;

  private String startServer1Command;
  private String startServer2Command;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    locatorFolder = temporaryFolder.newFolder(LOCATOR_NAME).toPath().toAbsolutePath();
    server1Folder = temporaryFolder.newFolder(SERVER_1_NAME).toPath().toAbsolutePath();
    server2Folder = temporaryFolder.newFolder(SERVER_2_NAME).toPath().toAbsolutePath();

    int[] ports = getRandomAvailableTCPPorts(3);
    locatorPort = ports[0];
    int server1Port = ports[1];
    int server2Port = ports[2];

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
        "--server-port=" + server1Port);

    startServer2Command = String.join(" ",
        "start server",
        "--name=" + SERVER_2_NAME,
        "--dir=" + server2Folder,
        "--locators=localhost[" + locatorPort + "]",
        "--server-port=" + server2Port);

    String createRegionCommand = String.join(" ",
        "create region",
        "--name=" + REGION_NAME,
        "--type=REPLICATE_PERSISTENT");

    gfshRule.execute(startLocatorCommand, startServer1Command, startServer2Command,
        createRegionCommand);

    clientCache = new ClientCacheFactory()
        .addPoolLocator("localhost", locatorPort)
        .create();
  }

  @After
  public void tearDown() {
    if (clientCache != null) {
      clientCache.close();
    }

    gfshRule.execute("stop server --dir=" + server1Folder);
    gfshRule.execute("stop server --dir=" + server2Folder);
    gfshRule.execute("stop locator --dir=" + locatorFolder);
  }

  @Test
  public void waitingForMembersMessageIsLogged() throws Exception {
    Region<Integer, Integer> region = clientCache.<Integer, Integer>createClientRegionFactory(PROXY)
        .create(REGION_NAME);

    region.put(1, 1);
    gfshRule.execute("stop server --dir=" + server1Folder);

    region.put(1, 2);
    gfshRule.execute("stop server --dir=" + server2Folder);

    String connectToLocatorCommand = "connect --locator=localhost[" + locatorPort + "]";

    Future<Void> startServer1 = executorServiceRule.submit(() -> {
      gfshRule.execute(connectToLocatorCommand, startServer1Command);
    });

    await().untilAsserted(() -> {
      String waitingForMembersMessage = String.format(
          "Region %s has potentially stale data. It is waiting for another member to recover the latest data.",
          Region.SEPARATOR + REGION_NAME);

      LogFileAssert.assertThat(server1Folder.resolve(SERVER_1_NAME + ".log").toFile())
          .exists()
          .contains(waitingForMembersMessage);
    });

    gfshRule.execute(connectToLocatorCommand, startServer2Command);

    startServer1.get(getTimeout().toMillis(), MILLISECONDS);
  }
}
