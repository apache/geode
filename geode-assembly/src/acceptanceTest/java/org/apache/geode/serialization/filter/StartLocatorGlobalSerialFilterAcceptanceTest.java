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
package org.apache.geode.serialization.filter;

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.assertj.LogFileAssert.assertThat;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class StartLocatorGlobalSerialFilterAcceptanceTest {

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();
  @Rule
  public GfshRule gfshRule = new GfshRule();

  private Path locatorFolder;
  private int locatorPort;
  private int locatorJmxPort;
  private Path locatorLogFile;

  @Before
  public void setUpFiles() {
    locatorFolder = gfshRule.getTemporaryFolder().getRoot().toPath().toAbsolutePath();
    locatorLogFile = locatorFolder.resolve("locator.log");
  }

  @Before
  public void setUpPorts() {
    int[] ports = getRandomAvailableTCPPorts(2);
    locatorPort = ports[0];
    locatorJmxPort = ports[1];
  }

  @Test
  public void startDoesNotConfigureGlobalSerialFilter_byDefault() {
    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=locator",
        "--dir=" + locatorFolder,
        "--port=" + locatorPort,
        "--J=-Dgemfire.jmx-manager-port=" + locatorJmxPort);

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .exists()
          .doesNotContain("Global serial filter is now configured.")
          .doesNotContain("jdk.serialFilter");
    });
  }

  @Test
  public void startDoesNotConfigureGlobalSerialFilter_whenJdkSerialFilterIsNotBlank() {
    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=locator",
        "--dir=" + locatorFolder,
        "--port=" + locatorPort,
        "--J=-Dgemfire.jmx-manager-port=" + locatorJmxPort,
        "--J=-Djdk.serialFilter=*");

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .exists()
          .doesNotContain("Global serial filter is now configured.")
          .contains("jdk.serialFilter");
    });
  }

  @Test
  public void startConfiguresGlobalSerialFilter_whenEnableGlobalSerialFilterIsTrue() {
    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=locator",
        "--dir=" + locatorFolder,
        "--port=" + locatorPort,
        "--J=-Dgemfire.jmx-manager-port=" + locatorJmxPort,
        "--J=-Dgeode.enableGlobalSerialFilter=true");

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Global serial filter is now configured.")
          .doesNotContain("jdk.serialFilter");
    });
  }

  @Test
  public void startDoesNotConfigureGlobalSerialFilter_whenEnableGlobalSerialFilterIsTrue_andJdkSerialFilterIsNotBlank() {
    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=locator",
        "--dir=" + locatorFolder,
        "--port=" + locatorPort,
        "--J=-Dgemfire.jmx-manager-port=" + locatorJmxPort,
        "--J=-Dgeode.enableGlobalSerialFilter=true",
        "--J=-Djdk.serialFilter=*");

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .exists()
          .doesNotContain("Global serial filter is now configured.")
          .contains("jdk.serialFilter");
    });
  }
}
