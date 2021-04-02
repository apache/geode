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
package org.apache.geode.management;

import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.management.internal.OpenJmxTypesSerialFilter.PROPERTY_NAME;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.nio.file.Path;

import org.apache.commons.lang3.JavaVersion;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class ServerManagerConfiguresJmxSerialFilterAcceptanceTest {

  private static final String NAME = "the-server";

  private Path workingDir;
  private int jmxPort;
  private Path serverLogFile;

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();
  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Before
  public void setUpOutputFiles() {
    TemporaryFolder temporaryFolder = gfshRule.getTemporaryFolder();

    workingDir = temporaryFolder.getRoot().toPath().toAbsolutePath();
    serverLogFile = workingDir.resolve(NAME + ".log");
  }

  @Before
  public void setUpRandomPorts() {
    jmxPort = getRandomAvailableTCPPort();
  }

  @Test
  public void startingServerWithJmxManager_configuresSerialFilter_atLeastJava9() {
    assumeThat(isJavaVersionAtLeast(JavaVersion.JAVA_9)).isTrue();

    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + NAME,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--J=-Dgemfire.enable-cluster-configuration=false",
        "--J=-Dgemfire.http-service-port=0",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxPort,
        "--J=-Dgemfire.jmx-manager-start=true");

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      LogFileAssert.assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("System property " + PROPERTY_NAME + " is now configured with");
    });
  }

  @Test
  public void startingServerWithJmxManager_configuresSerialFilter_atMostJava8() {
    assumeThat(isJavaVersionAtMost(JavaVersion.JAVA_1_8)).isTrue();

    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + NAME,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--J=-Dgemfire.enable-cluster-configuration=false",
        "--J=-Dgemfire.http-service-port=0",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxPort,
        "--J=-Dgemfire.jmx-manager-start=true");

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      LogFileAssert.assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .doesNotContain("System property " + PROPERTY_NAME + " is now configured with");
    });
  }
}
