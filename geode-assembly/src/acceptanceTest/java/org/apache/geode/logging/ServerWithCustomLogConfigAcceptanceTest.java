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
import static org.apache.geode.test.assertj.LogFileAssert.assertThat;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

@Category(LoggingTest.class)
public class ServerWithCustomLogConfigAcceptanceTest {

  private static final String CONFIG_WITH_GEODE_PLUGINS_FILE_NAME =
      "ServerWithCustomLogConfigAcceptanceTestWithGeodePlugins.xml";
  private static final String CONFIG_WITHOUT_GEODE_PLUGINS_FILE_NAME =
      "ServerWithCustomLogConfigAcceptanceTestWithoutGeodePlugins.xml";

  private String serverName;
  private Path workingDir;
  private Path configWithGeodePluginsFile;
  private Path configWithoutGeodePluginsFile;
  private Path serverLogFile;
  private Path customLogFile;

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);
  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUpLogConfigFiles() {
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
    serverName = testName.getMethodName();

    serverLogFile = workingDir.resolve(serverName + ".log");
    customLogFile = workingDir.resolve("custom.log");
  }

  @Test
  public void serverLauncherUsesDefaultLoggingConfig() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + workingDir,
        "--disable-default-server");

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .doesNotExist();
    });
  }

  @Test
  public void serverLauncherUsesSpecifiedConfigFileWithoutGeodePlugins() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--J=-Dlog4j.configurationFile=" + configWithoutGeodePluginsFile.toAbsolutePath());

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }

  @Test
  public void serverLauncherUsesConfigFileInClasspathWithoutGeodePlugins() throws IOException {
    copy(configWithoutGeodePluginsFile, workingDir.resolve("log4j2.xml"));

    String classpath = workingDir.toFile().getAbsolutePath();

    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--classpath", classpath);

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }

  @Test
  public void serverLauncherUsesSpecifiedConfigFileWithGeodePlugins() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--J=-Dlog4j.configurationFile=" + configWithGeodePluginsFile.toAbsolutePath());

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }

  @Test
  public void serverLauncherUsesConfigFileInClasspathWithGeodePlugins() throws IOException {
    copy(configWithGeodePluginsFile, workingDir.resolve("log4j2.xml"));

    String classpath = workingDir.toFile().getAbsolutePath();

    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--classpath", classpath);

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("Server " + serverName + " startup completed in ")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class");
    });
  }
}
