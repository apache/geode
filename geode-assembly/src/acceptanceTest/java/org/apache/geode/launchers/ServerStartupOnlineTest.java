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

  @Test
  public void startServerReturnsAfterStartupTaskCompletes() throws IOException,
      InterruptedException {

    String serviceJarPath = serviceJarRule.createJarFor("ServerLauncherCacheProvider.jar",
        ServerLauncherCacheProvider.class, WaitForFileToExist.class);

    Path serverFolder = temporaryFolder.getRoot().toPath().toAbsolutePath();
    String serverName = testName.getMethodName();

    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + serverFolder,
        "--classpath=" + serviceJarPath,
        "--disable-default-server");

    CompletableFuture<Void> startServerTask =
        executorServiceRule.runAsync(() -> gfshRule.execute(startServerCommand));

    waitForStartServerCommandToHang(serverFolder, serverName);

    assertThat(startServerTask).isNotDone();

    completeRemoteStartupTask(serverFolder);

    await().untilAsserted(() -> assertThat(startServerTask).isDone());
  }

  // TODO: Aaron: test output of GFSH "status server" command

  // TODO: Aaron: test state of MemberMXBean in server JVM

  private void waitForStartServerCommandToHang(Path serverFolder, String serverName)
      throws InterruptedException {
    await().untilAsserted(() -> assertThat(serverFolder.resolve(serverName + ".log")).exists());
    Thread.sleep(5000);
  }

  private void completeRemoteStartupTask(Path serverFolder) throws IOException {
    Files.createFile(serverFolder.resolve(WaitForFileToExist.WAITING_FILE_NAME));
  }
}
