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

import static java.nio.file.Files.copy;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.internal.lang.SystemUtils.isWindows;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

public class ServerLauncherWithCustomLogConfigAcceptanceTest {

  private static final String CONFIG_FILE_NAME =
      "ServerLauncherWithCustomLogConfigAcceptanceTest.xml";
  private static final String SERVER_NAME = "the-server";

  private int serverPort;
  private Path configFile;
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
  public void setUpLogConfigFile() {
    configFile = createFileFromResource(getResource(CONFIG_FILE_NAME), temporaryFolder.getRoot(),
        CONFIG_FILE_NAME)
            .toPath();
  }

  @Before
  public void setUpOutputFiles() {
    stdoutFile = temporaryFolder.getRoot().toPath().resolve("stdout.txt");
    serverLogFile = temporaryFolder.getRoot().toPath().resolve(SERVER_NAME + ".log");
  }

  @Before
  public void setUpRandomPorts() {
    serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
  }

  @After
  public void stopServer() throws Exception {
    if (server != null) {
      server.destroyForcibly().waitFor(getTimeout().toMillis(), MILLISECONDS);
    }
  }

  @Test
  public void serverLauncherUsesSpecifiedConfigFile() throws Exception {
    ProcessBuilder processBuilder = new ProcessBuilder()
        .redirectErrorStream(true)
        .redirectOutput(stdoutFile.toFile())
        .directory(temporaryFolder.getRoot())
        .command(javaBin.toFile().getAbsolutePath(),
            "-Djava.awt.headless=true",
            "-Dlog4j.configurationFile=" + configFile.toAbsolutePath(),
            "-cp", geodeDependencies.toFile().getAbsolutePath(),
            "org.apache.geode.distributed.ServerLauncher", "start", SERVER_NAME,
            "--server-port", String.valueOf(serverPort));

    System.out.println("Environment: " + System.getenv());
    System.out.println("Launching command: " + processBuilder.command());

    server = processBuilder
        .start();

    assertThat(server.isAlive()).isTrue();

    await().untilAsserted(() -> {
      assertThat(serverLogFile)
          .as(serverLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      LogFileAssert.assertThat(stdoutFile.toFile())
          .as(stdoutFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + SERVER_NAME + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }

  @Test
  public void serverLauncherUsesConfigFileInClasspath() throws Exception {
    copy(configFile, temporaryFolder.getRoot().toPath().resolve("log4j2.xml"));

    String classpath = temporaryFolder.getRoot().getAbsolutePath() + File.pathSeparator +
        geodeDependencies.toFile().getAbsolutePath();

    ProcessBuilder processBuilder = new ProcessBuilder()
        .redirectErrorStream(true)
        .redirectOutput(stdoutFile.toFile())
        .directory(temporaryFolder.getRoot())
        .command(javaBin.toFile().getAbsolutePath(),
            "-Djava.awt.headless=true",
            "-cp", classpath,
            "org.apache.geode.distributed.ServerLauncher", "start", SERVER_NAME,
            "--server-port", String.valueOf(serverPort));

    System.out.println("Environment: " + System.getenv());
    System.out.println("Launching command: " + processBuilder.command());

    server = processBuilder
        .start();

    assertThat(server.isAlive()).isTrue();

    await().untilAsserted(() -> {
      assertThat(serverLogFile)
          .as(serverLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      LogFileAssert.assertThat(stdoutFile.toFile())
          .as(stdoutFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + SERVER_NAME + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }
}
