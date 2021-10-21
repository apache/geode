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

import static org.apache.commons.lang3.JavaVersion.JAVA_1_8;
import static org.apache.commons.lang3.JavaVersion.JAVA_9;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.nio.file.Path;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class LocatorLauncherConfiguresGlobalSerialFilterAcceptanceTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  private File locatorFolder;

  @Test
  public void gfshStartLocatorJava8() {
    assumeThat(isJavaVersionAtMost(JAVA_1_8)).isTrue();

    locatorFolder = temporaryFolder.getRoot();

    int[] ports = getRandomAvailableTCPPorts(2);

    int locatorPort = ports[0];
    int locatorJmxPort = ports[1];

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=locator",
        "--dir=" + locatorFolder.getAbsolutePath(),
        "--port=" + locatorPort,
        "--J=-Dgemfire.jmx-manager-port=" + locatorJmxPort);

    gfshRule.execute(startLocatorCommand);

    try {
      Path locatorLogFile = locatorFolder.toPath().resolve("locator.log");
      await().untilAsserted(() -> {
        LogFileAssert.assertThat(locatorLogFile.toFile()).exists()
            .contains("Global serial filter is now configured.")
            .doesNotContain("jdk.serialFilter");
      });
    } finally {
      String stopLocatorCommand = "stop locator --dir=" + locatorFolder.getAbsolutePath();
      gfshRule.execute(stopLocatorCommand);
    }
  }

  // another test for java 9 that does not create global serial filter
  @Test
  public void gfshStartLocatorJava9AndAbove() {
    assumeThat(isJavaVersionAtLeast(JAVA_9)).isTrue();

    locatorFolder = temporaryFolder.getRoot();

    int[] ports = getRandomAvailableTCPPorts(2);

    int locatorPort = ports[0];
    int locatorJmxPort = ports[1];

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=locator",
        "--dir=" + locatorFolder.getAbsolutePath(),
        "--port=" + locatorPort,
        "--J=-Dgemfire.jmx-manager-port=" + locatorJmxPort);

    gfshRule.execute(startLocatorCommand);

    try {
      Path locatorLogFile = locatorFolder.toPath().resolve("locator.log");
      await().untilAsserted(() -> {
        LogFileAssert.assertThat(locatorLogFile.toFile()).exists()
            .doesNotContain("Global serial filter is now configured.")
            .doesNotContain("jdk.serialFilter");
      });
    } finally {
      String stopLocatorCommand = "stop locator --dir=" + locatorFolder.getAbsolutePath();
      gfshRule.execute(stopLocatorCommand);
    }
  }
}
