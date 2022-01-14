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

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.io.File;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class StartServerGlobalSerialFilterAcceptanceTest {

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();
  @Rule
  public GfshRule gfshRule = new GfshRule();

  private File serverFolder;
  private int jmxPort;

  @Before
  public void setServerFolder() {
    serverFolder = gfshRule.getTemporaryFolder().getRoot();
  }

  @Before
  public void setJmxPort() {
    jmxPort = getRandomAvailableTCPPort();
  }

  @Test
  public void startDoesNotConfigureGlobalSerialFilter_byDefault() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=server",
        "--dir=" + serverFolder.getAbsolutePath(),
        "--disable-default-server",
        "--J=-Dgemfire.enable-cluster-configuration=false",
        "--J=-Dgemfire.http-service-port=0",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxPort,
        "--J=-Dgemfire.jmx-manager-start=true");

    gfshRule.execute(startServerCommand);

    Path serverLogFile = serverFolder.toPath().resolve("server.log");
    await().untilAsserted(() -> {
      LogFileAssert.assertThat(serverLogFile.toFile()).exists()
          .doesNotContain("Global serial filter is now configured.")
          .doesNotContain("jdk.serialFilter");
    });
  }

  @Test
  public void startDoesNotConfigureGlobalSerialFilter_whenJdkSerialFilterIsNotBlank() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=server",
        "--dir=" + serverFolder.getAbsolutePath(),
        "--disable-default-server",
        "--J=-Dgemfire.enable-cluster-configuration=false",
        "--J=-Dgemfire.http-service-port=0",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxPort,
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Djdk.serialFilter=*");

    gfshRule.execute(startServerCommand);

    Path serverLogFile = serverFolder.toPath().resolve("server.log");
    await().untilAsserted(() -> {
      LogFileAssert.assertThat(serverLogFile.toFile()).exists()
          .doesNotContain("Global serial filter is now configured.")
          .contains("jdk.serialFilter");
    });
  }

  /**
   * ServerLauncher is not currently wired up with EnabledGlobalSerialFilterConfigurationFactory
   * like LocatorLauncher is.
   */
  @Test
  public void startDoesNotConfigureGlobalSerialFilter_whenEnableGlobalSerialFilterIsTrue() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=server",
        "--dir=" + serverFolder.getAbsolutePath(),
        "--disable-default-server",
        "--J=-Dgemfire.enable-cluster-configuration=false",
        "--J=-Dgemfire.http-service-port=0",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxPort,
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgeode.enableGlobalSerialFilter=true");

    gfshRule.execute(startServerCommand);

    Path serverLogFile = serverFolder.toPath().resolve("server.log");
    await().untilAsserted(() -> {
      LogFileAssert.assertThat(serverLogFile.toFile()).exists()
          .contains("Global serial filter is now configured.")
          .doesNotContain("jdk.serialFilter");
    });
  }

  @Test
  public void startDoesNotConfigureGlobalSerialFilter_whenEnableGlobalSerialFilterIsTrue_andJdkSerialFilterIsNotBlank() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=server",
        "--dir=" + serverFolder.getAbsolutePath(),
        "--disable-default-server",
        "--J=-Dgemfire.enable-cluster-configuration=false",
        "--J=-Dgemfire.http-service-port=0",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxPort,
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgeode.enableGlobalSerialFilter=true",
        "--J=-Djdk.serialFilter=*");

    gfshRule.execute(startServerCommand);

    Path serverLogFile = serverFolder.toPath().resolve("server.log");
    await().untilAsserted(() -> {
      LogFileAssert.assertThat(serverLogFile.toFile()).exists()
          .doesNotContain("Global serial filter is now configured.")
          .contains("jdk.serialFilter");
    });
  }
}
