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

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
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
 * Verify that --J=-Dlog4j2.formatMsgNoLookups=true works for Locators when started with GFSH.
 */
@Category(LoggingTest.class)
public class LocatorWithLog4j2FormatMsgNoLookupsAcceptanceTest {

  private static final String LOCATOR_NAME = "locator";

  private Path workingDir;
  private int locatorPort;
  private int httpPort;
  private int rmiPort;
  private Path locatorLogFile;
  private Path pulseLogFile;

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Before
  public void setUpFiles() {
    TemporaryFolder temporaryFolder = gfshRule.getTemporaryFolder();

    workingDir = temporaryFolder.getRoot().toPath().toAbsolutePath();
    locatorLogFile = workingDir.resolve(LOCATOR_NAME + ".log");
    pulseLogFile = workingDir.resolve("pulse.log");
  }

  @Before
  public void setUpPorts() {
    int[] ports = getRandomAvailableTCPPorts(3);

    locatorPort = ports[0];
    httpPort = ports[1];
    rmiPort = ports[2];
  }

  @Test
  public void startLocator_log4j2FormatMsgNoLookups() {
    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + LOCATOR_NAME,
        "--dir=" + workingDir,
        "--port=" + locatorPort,
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-http-port=" + httpPort,
        "--J=-Dgemfire.jmx-manager-port=" + rmiPort,
        "--J=-Dlog4j2.formatMsgNoLookups=true");

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("-Dlog4j2.formatMsgNoLookups=true")
          .contains("log4j2.formatMsgNoLookups = true")
          .contains("Located war: geode-pulse")
          .contains("Adding webapp /pulse")
          .contains("Starting server location for Distribution Locator")
          .doesNotContain("geode-pulse war file was not found")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class")
          .doesNotContain("[warn")
          .doesNotContain("[error")
          .doesNotContain("[fatal");

      assertThat(pulseLogFile.toFile())
          .as(pulseLogFile.toFile().getAbsolutePath())
          .exists()
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class")
          .doesNotContain("WARN")
          .doesNotContain("ERROR")
          .doesNotContain("FATAL");
    });
  }
}
