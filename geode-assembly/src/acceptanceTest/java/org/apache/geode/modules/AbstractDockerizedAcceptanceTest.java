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
package org.apache.geode.modules;

import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import org.apache.geode.test.junit.rules.gfsh.GfshRule;

@RunWith(Parameterized.class)
public abstract class AbstractDockerizedAcceptanceTest {
  @ClassRule
  public static GfshRule gfshRule = new GfshRule();
  @ClassRule
  public static TemporaryFolder stagingTempDir = new TemporaryFolder();

  private static final String LOCATOR_START_COMMAND =
      "start locator --name=locator1 --port=10334 --J=-Dgemfire.enable-network-partition-detection=false";
  private static final String SERVER1_START_COMMAND =
      "start server --name=server1 --locators=localhost[10334] --server-port=40404 --http-service-port=9090 --start-rest-api ";
  private static final String SERVER2_START_COMMAND =
      "start server --name=server2 --locators=localhost[10334] --server-port=40405 --http-service-port=9091 --start-rest-api ";
  private static final String GFSH_PATH = "/geode/bin/gfsh";

  private static final String NONMODULAR_LAUNCH_COMMAND = "";
  private static final String MODULAR_LAUNCH_COMMAND = "--experimental";

  private static final String EMPTY_STRING = "";

  protected static GenericContainer<?> geodeContainer = setupDockerContainer();

  private String locatorGFSHConnectionString;

  private static String currentLaunchCommand;
  private static String previousLocatorGFSHConnectionString;

  static int locatorPort;
  static int serverPort;
  static int httpPort;
  static int redisPort;
  static int memcachePort;
  static int jmxHttpPort;
  static String host;

  protected String getLocatorGFSHConnectionString() {
    return locatorGFSHConnectionString == null ? previousLocatorGFSHConnectionString
        : locatorGFSHConnectionString;
  }

  protected String getCurrentLaunchCommand() {
    return currentLaunchCommand;
  }

  protected String getLocatorStartCommand() {
    return LOCATOR_START_COMMAND;
  }

  protected String getServer1StartCommand() {
    return SERVER1_START_COMMAND;
  }

  protected String getServer2StartCommand() {
    return SERVER2_START_COMMAND;
  }

  protected boolean isModular() {
    return getCurrentLaunchCommand().equals(MODULAR_LAUNCH_COMMAND);
  }

  protected String runGfshCommandInContainer(String... commands)
      throws IOException, InterruptedException {
    List<String> gfshCommandList = new LinkedList<>();
    gfshCommandList.add(GFSH_PATH);
    for (String command : commands) {
      gfshCommandList.add("-e");
      gfshCommandList.add(command);
    }
    Container.ExecResult execResult =
        geodeContainer.execInContainer(gfshCommandList.toArray(new String[] {}));
    System.out.println(execResult.getStdout());
    System.err.println(execResult.getStderr());
    // assertThat(execResult.getStderr()).isEmpty();
    return execResult.getStdout();
  }

  protected void launch(String launchCommand) throws IOException, InterruptedException {
    if (!geodeContainer.isRunning()) {
      startDockerContainer(launchCommand);
      launchServicesInContainer(launchCommand);
      mapPorts();
    } else if (!currentLaunchCommand.equals(launchCommand)) {
      geodeContainer.stop();
      startDockerContainer(launchCommand);
      launchServicesInContainer(launchCommand);
      mapPorts();
    }
  }

  protected void startDockerContainer(String launchCommand) {
    geodeContainer.withCommand("./launch.sh");
    geodeContainer.start();
    currentLaunchCommand = launchCommand;
  }

  private void mapPorts() {
    host = geodeContainer.getHost();
    locatorPort = getMappedPort(10334);
    serverPort = getMappedPort(40404);
    jmxHttpPort = getMappedPort(7070);
    httpPort = getMappedPort(9090);
    redisPort = getMappedPort(6379);
    memcachePort = getMappedPort(5678);

    previousLocatorGFSHConnectionString = locatorGFSHConnectionString =
        "connect --locator=" + host + "[" + locatorPort + "] --use-http --url=http://localhost:"
            + jmxHttpPort + "/gemfire/v1";
  }

  protected int getMappedPort(int port) {
    return geodeContainer.getMappedPort(port);
  }

  protected void launchServicesInContainer(String launchCommand)
      throws IOException, InterruptedException {
    runGfshCommandInContainer(LOCATOR_START_COMMAND);
    runGfshCommandInContainer("connect", "configure pdx --read-serialized=true");

    runGfshCommandInContainer(
        SERVER1_START_COMMAND + " " + getServer1SpecificGfshCommands() + " " + launchCommand);
    runGfshCommandInContainer(
        SERVER2_START_COMMAND + " " + getServer2SpecificGfshCommands() + " " + launchCommand);
  }

  protected String getServer1SpecificGfshCommands() {
    return EMPTY_STRING;
  }

  protected String getServer2SpecificGfshCommands() {
    return EMPTY_STRING;
  }

  private static GenericContainer<?> setupDockerContainer() {
    String currentDirectory = System.getProperty("user.dir");
    geodeContainer = new GenericContainer<>(
        new ImageFromDockerfile()
            .withDockerfile(new File(
                currentDirectory.substring(0, currentDirectory.indexOf("build"))
                    .concat("build/modularDocker/Dockerfile"))
                        .toPath()));
    geodeContainer.withExposedPorts(9090, 10334, 40404, 1099, 7070, 6379, 5678);
    geodeContainer.withReuse(true);
    geodeContainer.waitingFor(Wait.forHealthcheck());
    geodeContainer.withStartupTimeout(Duration.ofSeconds(120));
    return geodeContainer;
  }

  protected static File loadTestResource(String fileName) {
    String filePath =
        createTempFileFromResource(DeployJarAcceptanceTest.class, fileName).getAbsolutePath();
    assertThat(filePath).isNotNull();

    return new File(filePath);
  }

  @Parameterized.Parameters
  public static List<String> getStartServerCommand() {
    return Arrays.asList(NONMODULAR_LAUNCH_COMMAND, MODULAR_LAUNCH_COMMAND);
  }
}
