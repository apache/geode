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

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

import org.apache.geode.test.junit.rules.gfsh.GfshRule;

@RunWith(Parameterized.class)
public abstract class AbstractDockerizedAcceptanceTest {
  @ClassRule
  public static GfshRule gfshRule = new GfshRule();
  @ClassRule
  public static TemporaryFolder stagingTempDir = new TemporaryFolder();

  protected static GenericContainer<?> geodeContainer;
  private String locatorGFSHConnectionString;

  private static String currentLaunchCommand;
  private static String previousLocatorGFSHConnectionString;

  static int locatorPort;
  static int serverPort;
  static int httpPort;
  static int redisPort;
  static int memcachePort;

  public String getLocatorGFSHConnectionString() {
    return locatorGFSHConnectionString == null ? previousLocatorGFSHConnectionString
        : locatorGFSHConnectionString;
  }

  protected void launch(String launchCommand) throws IOException {
    setupDockerContainer();
    if (!geodeContainer.isRunning()) {
      startDockerContainer(launchCommand);
    } else {
      if (!currentLaunchCommand.equals(launchCommand)) {
        geodeContainer.stop();
        startDockerContainer(launchCommand);
      }
    }
  }

  private void startDockerContainer(String launchCommand) {
    geodeContainer.withCommand("./" + launchCommand);
    try {
      geodeContainer.start();
    } catch (Exception e) {
      e.printStackTrace();
    }
    currentLaunchCommand = launchCommand;


    String host = geodeContainer.getHost();
    locatorPort = geodeContainer.getMappedPort(10334);
    serverPort = geodeContainer.getMappedPort(40404);
    int jmxHttpPort = geodeContainer.getMappedPort(7070);
    httpPort = geodeContainer.getMappedPort(9090);
    redisPort = geodeContainer.getMappedPort(6379);
    memcachePort = geodeContainer.getMappedPort(5678);

    previousLocatorGFSHConnectionString = locatorGFSHConnectionString =
        "connect --locator=" + host + "[" + locatorPort + "] --use-http --url=http://localhost:"
            + jmxHttpPort + "/gemfire/v1";
  }

  private static void setupDockerContainer() {
    if (geodeContainer == null) {
      String currentDirectory = System.getProperty("user.dir");
      geodeContainer = new GenericContainer<>(
          new ImageFromDockerfile()
              .withDockerfile(new File(
                  currentDirectory.substring(0, currentDirectory.indexOf("build"))
                      .concat("build/docker/Dockerfile"))
                          .toPath()));
      geodeContainer.withExposedPorts(10334, 40404, 7070, 9090, 5678, 6379);
      geodeContainer.withCreateContainerCmdModifier(cmd -> {
        long availableProcessors = Runtime.getRuntime().availableProcessors();
        cmd.getHostConfig()
            .withCpuCount(availableProcessors);
      });
      geodeContainer.waitingFor(new HttpWaitStrategy().forPort(9090).forPath("/geode/v1"));
      geodeContainer.withStartupTimeout(Duration.ofSeconds(120));
    }
  }

  @Parameterized.Parameters
  public static List<String> getStartServerCommand() {
    return Arrays.asList("launch.sh", "launchExperimental.sh");
  }
}
