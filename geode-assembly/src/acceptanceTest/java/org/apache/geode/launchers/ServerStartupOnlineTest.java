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
import java.util.concurrent.CompletableFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.ServerLauncherCacheProvider;
import org.apache.geode.launchers.startuptasks.WaitForFileToExist;
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class ServerStartupOnlineTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  private Path serverFolder;
  private String serverName;
  private String startServerCommand;

  @Before
  public void setup() {
    String serviceJarPath = serviceJarRule.createJarFor("ServerLauncherCacheProvider.jar",
        ServerLauncherCacheProvider.class, WaitForFileToExist.class);

    serverFolder = temporaryFolder.getRoot().toPath().toAbsolutePath();
    serverName = testName.getMethodName();

    startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + serverFolder,
        "--classpath=" + serviceJarPath,
        "--disable-default-server");
  }

  @After
  public void stopServer() {
    String stopServerCommand = "stop server --dir=" + serverFolder;
    gfshRule.execute(stopServerCommand);
  }

  @Test
  public void startServerReturnsAfterStartupTaskCompletes() throws IOException,
      InterruptedException {
    CompletableFuture<Void> startServerTask =
        executorServiceRule.runAsync(() -> gfshRule.execute(startServerCommand));

    waitForStartServerCommandToHang();

    assertThat(startServerTask).isNotDone();

    completeRemoteStartupTask();

    await().untilAsserted(() -> assertThat(startServerTask).isDone());
  }

  @Test
  public void statusServerReportsStartingUntilStartupTaskCompletes() throws IOException,
      InterruptedException {
    CompletableFuture<Void> startServerTask =
        executorServiceRule.runAsync(() -> gfshRule.execute(startServerCommand));

    waitForStartServerCommandToHang();

    await().untilAsserted(() -> {
      String startingStatus = getServerStatus();
      assertThat(startingStatus)
          .as("Status server command output")
          .contains("Starting Server");
    });

    completeRemoteStartupTask();

    await().untilAsserted(() -> {
      assertThat(startServerTask).isDone();
      String onlineStatus = getServerStatus();
      assertThat(onlineStatus)
          .as("Status server command output")
          .contains("is currently online");
    });
  }

  // TODO: Aaron: test state of MemberMXBean in server JVM

  private String getServerStatus() {
    String statusServerCommand = "status server --dir=" + serverFolder;
    return gfshRule.execute(statusServerCommand).getOutputText();
  }

  private void waitForStartServerCommandToHang()
      throws InterruptedException {
    await().untilAsserted(() -> assertThat(serverFolder.resolve(serverName + ".log")).exists());
    // Without sleeping, this test can pass when it shouldn't.
    Thread.sleep(10_000);
  }

  private void completeRemoteStartupTask() throws IOException {
    Files.createFile(serverFolder.resolve(WaitForFileToExist.WAITING_FILE_NAME));
  }
}
