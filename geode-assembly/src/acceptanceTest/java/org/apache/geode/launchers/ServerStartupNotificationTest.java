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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletionException;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.distributed.ServerLauncherCacheProvider;
import org.apache.geode.launchers.startuptasks.CompletingAndFailing;
import org.apache.geode.launchers.startuptasks.Failing;
import org.apache.geode.launchers.startuptasks.MultipleFailing;
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class ServerStartupNotificationTest {

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);
  @Rule(order = 2)
  public TestName testName = new TestName();
  @Rule(order = 3)
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  private Path serverFolder;
  private String serverName;

  @Before
  public void setup() {
    serverFolder = folderRule.getFolder().toPath().toAbsolutePath();
    serverName = testName.getMethodName();
  }

  @After
  public void stopServer() {
    String stopServerCommand = "stop server --dir=" + serverFolder.toFile();
    gfshRule.execute(stopServerCommand);
  }

  @Test
  public void startupWithNoAsyncTasks() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + serverFolder,
        "--disable-default-server");

    gfshRule.execute(startServerCommand);

    Path logFile = serverFolder.resolve(serverName + ".log");

    Pattern expectedLogLine =
        Pattern.compile("^\\[info .*].*Server " + serverName + " startup completed in \\d+ ms");
    await().untilAsserted(() -> assertThat(Files.lines(logFile))
        .as("Log file " + logFile + " includes line matching " + expectedLogLine)
        .anyMatch(expectedLogLine.asPredicate()));
  }

  @Test
  public void startupWithFailingAsyncTask() {
    Path serviceJarPath = serviceJarRule.createJarFor("ServerLauncherCacheProvider.jar",
        ServerLauncherCacheProvider.class, Failing.class);

    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + serverFolder,
        "--classpath=" + serviceJarPath,
        "--disable-default-server");

    gfshRule.execute(startServerCommand);

    Path logFile = serverFolder.resolve(serverName + ".log");

    Exception exception = Failing.EXCEPTION;
    String errorDetail = CompletionException.class.getName() + ": " +
        exception.getClass().getName() + ": " + exception.getMessage();

    Pattern expectedLogLine = Pattern.compile("^\\[error .*].*Server " + serverName +
        " startup completed in \\d+ ms with error: " + errorDetail);

    await().untilAsserted(() -> assertThat(Files.lines(logFile))
        .as("Log file " + logFile + " includes line matching " + expectedLogLine)
        .anyMatch(expectedLogLine.asPredicate()));
  }

  @Test
  public void startupWithMultipleFailingAsyncTasks() {
    Path serviceJarPath = serviceJarRule.createJarFor("ServerLauncherCacheProvider.jar",
        ServerLauncherCacheProvider.class, MultipleFailing.class);

    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + serverFolder,
        "--classpath=" + serviceJarPath,
        "--disable-default-server");

    gfshRule.execute(startServerCommand);

    Path logFile = serverFolder.resolve(serverName + ".log");

    Exception exception = MultipleFailing.EXCEPTION;
    String errorDetail = CompletionException.class.getName() + ": " +
        exception.getClass().getName() + ": " + exception.getMessage();

    Pattern expectedLogLine = Pattern.compile("^\\[error .*].*Server " + serverName +
        " startup completed in \\d+ ms with error: " + errorDetail);

    await().untilAsserted(() -> assertThat(Files.lines(logFile))
        .as("Log file " + logFile + " includes line matching " + expectedLogLine)
        .anyMatch(expectedLogLine.asPredicate()));
  }

  @Test
  public void startupWithCompletingAndFailingAsyncTasks() {
    Path serviceJarPath = serviceJarRule.createJarFor("ServerLauncherCacheProvider.jar",
        ServerLauncherCacheProvider.class, CompletingAndFailing.class);

    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + serverFolder,
        "--classpath=" + serviceJarPath,
        "--disable-default-server");

    gfshRule.execute(startServerCommand);

    Path logFile = serverFolder.resolve(serverName + ".log");

    Exception exception = CompletingAndFailing.EXCEPTION;
    String errorDetail = CompletionException.class.getName() + ": " +
        exception.getClass().getName() + ": " + exception.getMessage();

    Pattern expectedLogLine = Pattern.compile("^\\[error .*].*Server " + serverName +
        " startup completed in \\d+ ms with error: " + errorDetail);

    await().untilAsserted(() -> assertThat(Files.lines(logFile))
        .as("Log file " + logFile + " includes line matching " + expectedLogLine)
        .anyMatch(expectedLogLine.asPredicate()));
  }
}
