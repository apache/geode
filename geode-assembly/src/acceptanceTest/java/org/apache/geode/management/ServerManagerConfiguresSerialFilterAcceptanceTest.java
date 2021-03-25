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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.internal.lang.SystemUtils.isWindows;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.JavaVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.internal.OpenJmxTypesSerialFilter;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

public class ServerManagerConfiguresSerialFilterAcceptanceTest {

  private static final String NAME = "the-server";

  private int jmxPort;
  private Process server;
  private Path geodeDependencies;
  private Path stdoutFile;
  private Path serverLogFile;
  private Path javaBin;

  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUpJava() {
    String javaHome = System.getProperty("java.home");
    assertThat(javaHome)
        .as("java.home is not null")
        .isNotNull();

    String java = isWindows() ? "java.exe" : "java";
    javaBin = Paths.get(javaHome, "bin", java);
    assertThat(javaBin)
        .as("JAVA_HOME/bin/" + java + " exists")
        .exists();
  }

  @Before
  public void setUpGeodeDependencies() {
    Path geodeHome = requiresGeodeHome.getGeodeHome().toPath();
    geodeDependencies = geodeHome.resolve("lib/geode-dependencies.jar");

    assertThat(geodeDependencies)
        .as("GEODE_HOME/lib/geode-dependencies.jar exists")
        .exists();
  }

  @Before
  public void setUpOutputFiles() {
    stdoutFile = temporaryFolder.getRoot().toPath().resolve("stdout.txt");
    serverLogFile = temporaryFolder.getRoot().toPath().resolve(NAME + ".log");
  }

  @Before
  public void setUpRandomPorts() {
    jmxPort = getRandomAvailableTCPPort();
  }

  @After
  public void stopServer() throws Exception {
    if (server != null) {
      server.destroyForcibly().waitFor(getTimeout().toMillis(), MILLISECONDS);
    }
  }

  @Test
  public void startingServerWithJmxManager_configuresSerialFilter_atLeastJava9() throws Exception {
    assumeThat(isJavaVersionAtLeast(JavaVersion.JAVA_9)).isTrue();

    ProcessBuilder processBuilder = new ProcessBuilder()
        .redirectErrorStream(true)
        .redirectOutput(stdoutFile.toFile())
        .directory(temporaryFolder.getRoot())
        .command(javaBin.toFile().getAbsolutePath(),
            "-Dgemfire.enable-cluster-configuration=false",
            "-Dgemfire.http-service-port=" + 0,
            "-Dgemfire.jmx-manager=true",
            "-Dgemfire.jmx-manager-start=true",
            "-Dgemfire.jmx-manager-port=" + jmxPort,
            "-Djava.awt.headless=true",
            "-cp", geodeDependencies.toFile().getAbsolutePath(),
            "org.apache.geode.distributed.ServerLauncher", "start", NAME,
            "--disable-default-server");

    System.out.println("Environment: " + System.getenv());
    System.out.println("Launching command: " + processBuilder.command());

    server = processBuilder
        .start();

    assertThat(server.isAlive()).isTrue();

    await().untilAsserted(() -> {
      LogFileAssert.assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("System property " + OpenJmxTypesSerialFilter.PROPERTY_NAME
              + " is now configured with");
    });
  }

  @Test
  public void startingServerWithJmxManager_configuresSerialFilter_atMostJava8() throws Exception {
    assumeThat(isJavaVersionAtMost(JavaVersion.JAVA_1_8)).isTrue();

    ProcessBuilder processBuilder = new ProcessBuilder()
        .redirectErrorStream(true)
        .redirectOutput(stdoutFile.toFile())
        .directory(temporaryFolder.getRoot())
        .command(javaBin.toFile().getAbsolutePath(),
            "-Dgemfire.enable-cluster-configuration=false",
            "-Djava.awt.headless=true",
            "-Dgemfire.jmx-manager=true",
            "-Dgemfire.jmx-manager-start=true",
            "-Dgemfire.jmx-manager-port=" + jmxPort,
            "-Dgemfire.http-service-port=" + 0,
            "-cp", geodeDependencies.toFile().getAbsolutePath(),
            "org.apache.geode.distributed.ServerLauncher", "start", NAME,
            "--disable-default-server");

    System.out.println("Environment: " + System.getenv());
    System.out.println("Launching command: " + processBuilder.command());

    server = processBuilder
        .start();

    assertThat(server.isAlive()).isTrue();

    await().untilAsserted(() -> {
      LogFileAssert.assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .doesNotContain("System property " + OpenJmxTypesSerialFilter.PROPERTY_NAME
              + " is now configured with");
    });
  }
}
