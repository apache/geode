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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class LocatorLauncherConfiguresGlobalGlobalSerialFilterAcceptanceTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  private File serverFolder;
  private String serverName;

  @Before
  public void setup() {
    serverFolder = temporaryFolder.getRoot();
    serverName = testName.getMethodName();
  }

  @After
  public void stopServer() {
    String stopServerCommand = "stop server --dir=" + serverFolder.getAbsolutePath();
    gfshRule.execute(stopServerCommand);
  }

  @Test
  public void startupWithNoAsyncTasks() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + serverFolder.getAbsolutePath(),
        "--disable-default-server");

    gfshRule.execute(startServerCommand);

    Path logFile = serverFolder.toPath().resolve(serverName + ".log");

    Pattern expectedLogLine =
        Pattern.compile("^\\[info .*].*Server " + serverName + " startup completed in \\d+ ms");
    await().untilAsserted(() -> assertThat(Files.lines(logFile))
        .as("Log file " + logFile + " includes line matching " + expectedLogLine)
        .anyMatch(expectedLogLine.asPredicate()));
  }
}
