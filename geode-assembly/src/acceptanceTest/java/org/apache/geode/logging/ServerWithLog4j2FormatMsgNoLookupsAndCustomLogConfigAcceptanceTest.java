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
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

/**
 * Verify that --J=-Dlog4j2.formatMsgNoLookups=true works for Servers when started with GFSH and
 * a custom log config.
 */
@Category(LoggingTest.class)
public class ServerWithLog4j2FormatMsgNoLookupsAndCustomLogConfigAcceptanceTest {

  private static final String SERVER_NAME = "server";

  private static final String CONFIG_WITH_GEODE_PLUGINS_FILE_NAME =
      "ServerWithLog4j2FormatMsgNoLookupsAndCustomLogConfigAcceptanceTestWithGeodePlugins.xml";
  private static final String CONFIG_WITHOUT_GEODE_PLUGINS_FILE_NAME =
      "ServerWithLog4j2FormatMsgNoLookupsAndCustomLogConfigAcceptanceTestWithoutGeodePlugins.xml";

  private Path workingDir;
  private Path configWithGeodePluginsFile;
  private Path configWithoutGeodePluginsFile;
  private Path serverLogFile;
  private Path customLogFile;

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Before
  public void setUpFiles() {
    TemporaryFolder temporaryFolder = gfshRule.getTemporaryFolder();

    configWithGeodePluginsFile = createFileFromResource(
        getResource(CONFIG_WITH_GEODE_PLUGINS_FILE_NAME), temporaryFolder.getRoot(),
        CONFIG_WITH_GEODE_PLUGINS_FILE_NAME)
            .toPath();

    configWithoutGeodePluginsFile = createFileFromResource(
        getResource(CONFIG_WITHOUT_GEODE_PLUGINS_FILE_NAME), temporaryFolder.getRoot(),
        CONFIG_WITHOUT_GEODE_PLUGINS_FILE_NAME)
            .toPath();

    workingDir = temporaryFolder.getRoot().toPath().toAbsolutePath();
    serverLogFile = workingDir.resolve(SERVER_NAME + ".log");
    customLogFile = workingDir.resolve("custom.log");
  }

  @Test
  public void startServer_usesSpecifiedConfigFile_withoutGeodePlugins_withLog4j2FormatMsgNoLookups() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + SERVER_NAME,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--J=-Dlog4j2.configurationFile=" + configWithoutGeodePluginsFile.toAbsolutePath(),
        "--J=-Dlog4j2.formatMsgNoLookups=true");

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("-Dlog4j2.formatMsgNoLookups=true")
          .contains("log4j2.formatMsgNoLookups = true")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class")
          .doesNotContain("[error")
          .doesNotContain("[fatal");
    });
  }

  @Test
  public void startServer_usesConfigFileInClasspath_withoutGeodePlugins_withLog4j2FormatMsgNoLookups()
      throws IOException {
    copy(configWithoutGeodePluginsFile, workingDir.resolve("log4j2.xml"));

    String classpath = workingDir.toFile().getAbsolutePath();

    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + SERVER_NAME,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--classpath", classpath,
        "--J=-Dlog4j2.formatMsgNoLookups=true");

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .doesNotExist();

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("-Dlog4j2.formatMsgNoLookups=true")
          .contains("log4j2.formatMsgNoLookups = true")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class")
          .doesNotContain("[error")
          .doesNotContain("[fatal");
    });
  }

  @Test
  public void startServer_usesSpecifiedConfigFile_withGeodePlugins_withLog4j2FormatMsgNoLookups() {
    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + SERVER_NAME,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--J=-Dlog4j2.configurationFile=" + configWithGeodePluginsFile.toAbsolutePath(),
        "--J=-Dlog4j2.formatMsgNoLookups=true");

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("-Dlog4j2.formatMsgNoLookups=true")
          .contains("log4j2.formatMsgNoLookups = true")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class")
          .doesNotContain("[error")
          .doesNotContain("[fatal");

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("-Dlog4j2.formatMsgNoLookups=true")
          .contains("log4j2.formatMsgNoLookups = true")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class")
          .doesNotContain("[error")
          .doesNotContain("[fatal");
    });
  }

  @Test
  public void startServer_usesConfigFileInClasspath_withGeodePlugins_withLog4j2FormatMsgNoLookups()
      throws IOException {
    copy(configWithGeodePluginsFile, workingDir.resolve("log4j2.xml"));

    String classpath = workingDir.toFile().getAbsolutePath();

    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + SERVER_NAME,
        "--dir=" + workingDir,
        "--disable-default-server",
        "--classpath", classpath,
        "--J=-Dlog4j2.formatMsgNoLookups=true");

    gfshRule.execute(startServerCommand);

    await().untilAsserted(() -> {
      assertThat(serverLogFile.toFile())
          .as(serverLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("-Dlog4j2.formatMsgNoLookups=true")
          .contains("log4j2.formatMsgNoLookups = true")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class")
          .doesNotContain("[error")
          .doesNotContain("[fatal");

      assertThat(customLogFile.toFile())
          .as(customLogFile.toFile().getAbsolutePath())
          .exists()
          .contains("-Dlog4j2.formatMsgNoLookups=true")
          .contains("log4j2.formatMsgNoLookups = true")
          .doesNotContain("java.lang.IllegalStateException: No factory method found for class")
          .doesNotContain("[error")
          .doesNotContain("[fatal");
    });
  }
}
