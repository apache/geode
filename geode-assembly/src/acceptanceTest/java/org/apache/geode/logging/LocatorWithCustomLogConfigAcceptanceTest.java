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

import static java.nio.file.Files.copy;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.assertj.LogFileAssert.assertThat;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

@Category(LoggingTest.class)
public class LocatorWithCustomLogConfigAcceptanceTest {

  private static final String CONFIG_WITH_GEODE_PLUGINS_FILE_NAME =
      "LocatorWithCustomLogConfigAcceptanceTestWithGeodePlugins.xml";
  private static final String CONFIG_WITHOUT_GEODE_PLUGINS_FILE_NAME =
      "LocatorWithCustomLogConfigAcceptanceTestWithoutGeodePlugins.xml";

  private String locatorName;
  private Path workingDir;
  private int locatorPort;
  private int httpPort;
  private int rmiPort;
  private Path configWithGeodePluginsFile;
  private Path configWithoutGeodePluginsFile;
  private Path locatorLogFile;
  private Path pulseLogFile;
  private Path customLogFile;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);
  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    // set up log config files
    workingDir = folderRule.getFolder().toPath().toAbsolutePath();

    configWithGeodePluginsFile = createFileFromResource(
        getResource(CONFIG_WITH_GEODE_PLUGINS_FILE_NAME), workingDir.toFile(),
        CONFIG_WITH_GEODE_PLUGINS_FILE_NAME)
            .toPath();

    configWithoutGeodePluginsFile = createFileFromResource(
        getResource(CONFIG_WITHOUT_GEODE_PLUGINS_FILE_NAME), workingDir.toFile(),
        CONFIG_WITHOUT_GEODE_PLUGINS_FILE_NAME)
            .toPath();

    // set up output files
    locatorName = testName.getMethodName();

    locatorLogFile = workingDir.resolve(locatorName + ".log");
    pulseLogFile = workingDir.resolve("pulse.log");
    customLogFile = workingDir.resolve("custom.log");

    // set up random ports
    int[] ports = getRandomAvailableTCPPorts(3);

    locatorPort = ports[0];
    httpPort = ports[1];
    rmiPort = ports[2];
  }

  @After
  public void stopLocator() {
    String stopLocatorCommand = "stop locator --dir=" + workingDir;
    GfshExecution execution = gfshRule.execute(stopLocatorCommand);
    execution.locatorStopper().awaitStop(workingDir);
  }

  @Test
  public void locatorLauncherUsesDefaultLoggingConfig() {
    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + locatorName,
        "--dir=" + workingDir,
        "--port=" + locatorPort,
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-http-port=" + httpPort,
        "--J=-Dgemfire.jmx-manager-port=" + rmiPort);

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Located war: geode-pulse")
          .contains("Adding webapp /pulse")
          .contains("Starting server location for Distribution Locator")
          .doesNotContain("geode-pulse war file was not found")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");

      assertThat(pulseLogFile.toFile())
          .as(pulseLogFile.toFile().getAbsolutePath())
          .exists();

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .doesNotExist();
    });
  }

  @Test
  public void locatorLauncherUsesSpecifiedConfigFileWithoutGeodePlugins() {
    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + locatorName,
        "--dir=" + workingDir,
        "--port=" + locatorPort,
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-http-port=" + httpPort,
        "--J=-Dgemfire.jmx-manager-port=" + rmiPort,
        "--J=-Dlog4j.configurationFile=" + configWithoutGeodePluginsFile.toAbsolutePath());

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(pulseLogFile.toFile())
          .as(pulseLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Located war: geode-pulse")
          .contains("Adding webapp /pulse")
          .contains("Starting server location for Distribution Locator")
          .doesNotContain("geode-pulse war file was not found")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }

  @Test
  public void locatorLauncherUsesConfigFileInClasspathWithoutGeodePlugins() throws IOException {
    copy(configWithoutGeodePluginsFile, workingDir.resolve("log4j2.xml"));

    String classpath = workingDir.toFile().getAbsolutePath();

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + locatorName,
        "--dir=" + workingDir,
        "--port=" + locatorPort,
        "--classpath", classpath,
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-http-port=" + httpPort,
        "--J=-Dgemfire.jmx-manager-port=" + rmiPort);

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(pulseLogFile.toFile())
          .as(pulseLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Located war: geode-pulse")
          .contains("Adding webapp /pulse")
          .contains("Starting server location for Distribution Locator")
          .doesNotContain("geode-pulse war file was not found")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }

  @Test
  public void locatorLauncherUsesSpecifiedConfigFileWithGeodePlugins() {
    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + locatorName,
        "--dir=" + workingDir,
        "--port=" + locatorPort,
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-http-port=" + httpPort,
        "--J=-Dgemfire.jmx-manager-port=" + rmiPort,
        "--J=-Dlog4j.configurationFile=" + configWithGeodePluginsFile.toAbsolutePath());

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Located war: geode-pulse")
          .contains("Adding webapp /pulse")
          .contains("Starting server location for Distribution Locator")
          .doesNotContain("geode-pulse war file was not found")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");

      assertThat(pulseLogFile.toFile())
          .as(pulseLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Located war: geode-pulse")
          .contains("Adding webapp /pulse")
          .contains("Starting server location for Distribution Locator")
          .doesNotContain("geode-pulse war file was not found")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }

  @Test
  public void locatorLauncherUsesConfigFileInClasspathWithGeodePlugins() throws IOException {
    copy(configWithGeodePluginsFile, workingDir.resolve("log4j2.xml"));

    String classpath = workingDir.toFile().getAbsolutePath();

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + locatorName,
        "--dir=" + workingDir,
        "--port=" + locatorPort,
        "--classpath", classpath,
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-http-port=" + httpPort,
        "--J=-Dgemfire.jmx-manager-port=" + rmiPort);

    gfshRule.execute(startLocatorCommand);

    await().untilAsserted(() -> {
      assertThat(locatorLogFile.toFile())
          .as(locatorLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Located war: geode-pulse")
          .contains("Adding webapp /pulse")
          .contains("Starting server location for Distribution Locator")
          .doesNotContain("geode-pulse war file was not found")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");

      assertThat(pulseLogFile.toFile())
          .as(pulseLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Located war: geode-pulse")
          .contains("Adding webapp /pulse")
          .contains("Starting server location for Distribution Locator")
          .doesNotContain("geode-pulse war file was not found")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }
}
