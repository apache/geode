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
package org.apache.geode.logging;

import static org.apache.geode.test.assertj.LogFileAssert.assertThat;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

/**
 * Verify that --J=-Dlog4j2.formatMsgNoLookups=true works for Servers when started with GFSH.
 */
@Category(LoggingTest.class)
public class ServerWithLog4j2FormatMsgNoLookupsAcceptanceTest {

  private static final String LOCATOR_NAME = "locator";
  private static final String SERVER_NAME = "server";

  private Path workingDir;
  private Path serverLogFile;

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Before
  public void setUpFiles() {
    TemporaryFolder temporaryFolder = gfshRule.getTemporaryFolder();

    workingDir = temporaryFolder.getRoot().toPath().toAbsolutePath();
    serverLogFile = workingDir.resolve(SERVER_NAME + ".log");
  }

  @Test
  public void startServer_log4j2FormatMsgNoLookups() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + SERVER_NAME,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--J=-Dlog4j2.formatMsgNoLookups=true");

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("-Dlog4j2.formatMsgNoLookups=true")
          .contains("log4j2.formatMsgNoLookups = true")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class")
          .doesNotContain("[warn")
          .doesNotContain("[error")
          .doesNotContain("[fatal");
    });
  }
}
