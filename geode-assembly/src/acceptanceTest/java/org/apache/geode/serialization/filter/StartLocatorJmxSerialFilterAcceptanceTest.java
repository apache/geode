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

import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.nio.file.Path;

import org.apache.commons.lang3.JavaVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class StartLocatorJmxSerialFilterAcceptanceTest {

  private static final String PROPERTY_NAME = "jmx.remote.rmi.server.serial.filter.pattern";

  private File locatorFolder;
  private int locatorPort;
  private int jmxPort;
  private Path locatorLogFile;

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();
  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Before
  public void setLocatorFolder() {
    locatorFolder = gfshRule.getTemporaryFolder().getRoot();
    locatorLogFile = locatorFolder.toPath().resolve("locator" + ".log");
  }

  @Before
  public void setLocatorPorts() {
    int[] ports = getRandomAvailableTCPPorts(2);
    locatorPort = ports[0];
    jmxPort = ports[1];
  }

  @After
  public void stopLocator() {
    String stopLocatorCommand = "stop locator --dir=" + locatorFolder.getAbsolutePath();
    gfshRule.execute(stopLocatorCommand);
  }

  @Test
  public void startWithJmxManagerConfiguresJmxSerialFilter_onJava9orGreater() {
    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + "locator",
        "--dir=" + locatorFolder,
        "--port=" + locatorPort,
        "--J=-Dgemfire.enable-cluster-configuration=false",
        "--J=-Dgemfire.http-service-port=0",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxPort,
        "--J=-Dgemfire.jmx-manager-start=true");

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      LogFileAssert.assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("System property '" + PROPERTY_NAME + "' is now configured with");
    });
  }

  @Test
  public void startWithJmxManagerConfiguresJmxSerialFilter_onJava8() {
    assumeThat(isJavaVersionAtMost(JavaVersion.JAVA_1_8)).isTrue();

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + "locator",
        "--dir=" + locatorFolder,
        "--port=" + locatorPort,
        "--J=-Dgemfire.enable-cluster-configuration=false",
        "--J=-Dgemfire.http-service-port=0",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxPort,
        "--J=-Dgemfire.jmx-manager-start=true");

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      LogFileAssert.assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .exists()
          .doesNotContain("System property '" + PROPERTY_NAME + "' is now configured with");
    });
  }
}
