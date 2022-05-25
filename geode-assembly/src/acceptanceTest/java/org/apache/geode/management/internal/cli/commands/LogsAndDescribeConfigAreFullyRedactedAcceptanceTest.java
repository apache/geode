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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.examples.security.ExampleSecurityManager.SECURITY_JSON;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.security.ExampleSecurityManager;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

/**
 * This test class expects a "security.json" file to be visible on the member classpath. The
 * security.json is consumed by {@link ExampleSecurityManager} and should contain each
 * username/password combination present (even though not all will actually be consumed by the
 * Security Manager).
 *
 * <p>
 * Each password shares the string below for easier log scanning.
 */
@Category({SecurityTest.class, LoggingTest.class})
public class LogsAndDescribeConfigAreFullyRedactedAcceptanceTest {

  private static final String PASSWORD = "abcdefg";

  private int locatorPort;

  @Rule(order = 0)
  public RequiresGeodeHome geodeHome = new RequiresGeodeHome();
  @Rule(order = 1)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 2)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);

  @Before
  public void createDirectoriesAndFiles() throws IOException {
    Path rootFolder = folderRule.getFolder().toPath().toAbsolutePath();

    Path geodePropertiesFile = rootFolder.resolve("geode.properties");
    Path securityPropertiesFile = rootFolder.resolve("security.properties");

    Properties geodeProperties = new Properties();
    geodeProperties.setProperty(LOG_LEVEL, "debug");
    geodeProperties.setProperty("security-username", "propertyFileUser");
    geodeProperties.setProperty("security-password", PASSWORD + "-propertyFile");

    try (FileOutputStream fos = new FileOutputStream(geodePropertiesFile.toFile())) {
      geodeProperties.store(fos, null);
    }

    Properties securityProperties = new Properties();
    securityProperties.setProperty(SECURITY_MANAGER, ExampleSecurityManager.class.getName());
    securityProperties.setProperty(SECURITY_JSON, "security.json");
    securityProperties.setProperty("security-file-username", "securityPropertyFileUser");
    securityProperties.setProperty("security-file-password", PASSWORD + "-securityPropertyFile");

    try (FileOutputStream fos = new FileOutputStream(securityPropertiesFile.toFile())) {
      securityProperties.store(fos, null);
    }

    // The json is in the root resource directory.
    createFileFromResource(getResource("/security.json"), rootFolder.toFile(),
        "security.json");

    locatorPort = getRandomAvailableTCPPort();

    String startLocatorCmd = new CommandStringBuilder("start locator")
        .addOption("name", "test-locator")
        .addOption("port", String.valueOf(locatorPort))
        .addOption("properties-file", geodePropertiesFile.toString())
        .addOption("security-properties-file", securityPropertiesFile.toString())
        .addOption("J", "-Dsecure-username-jd=user-jd")
        .addOption("J", "-Dsecure-password-jd=password-jd")
        .addOption("classpath", rootFolder.toString())
        .getCommandString();

    String startServerCmd = new CommandStringBuilder("start server")
        .addOption("name", "test-server")
        .addOption("locators", "localhost[" + locatorPort + "]")
        .addOption("disable-default-server", "true")
        .addOption("user", "viaStartMemberOptions")
        .addOption("password", PASSWORD + "-viaStartMemberOptions")
        .addOption("properties-file", geodePropertiesFile.toString())
        .addOption("security-properties-file", securityPropertiesFile.toString())
        .addOption("J", "-Dsecure-username-jd=user-jd")
        .addOption("J", "-Dsecure-password-jd=" + PASSWORD + "-password-jd")
        .addOption("classpath", rootFolder.toString())
        .getCommandString();

    gfshRule.execute(startLocatorCmd, startServerCmd);
  }

  @Test
  public void logsDoNotContainStringThatShouldBeRedacted() {
    Path rootFolder = folderRule.getFolder().toPath();
    File dir = rootFolder.toFile();
    File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));

    for (File logFile : logFiles) {
      LogFileAssert.assertThat(logFile).doesNotContain(PASSWORD);
    }
  }

  @Test
  public void describeConfigRedactsJvmArguments() {
    String connectCommand = new CommandStringBuilder("connect")
        .addOption("locator", "localhost[" + locatorPort + "]")
        .addOption("user", "viaStartMemberOptions")
        .addOption("password", PASSWORD + "-viaStartMemberOptions")
        .getCommandString();

    String describeLocatorConfigCommand = new CommandStringBuilder("describe config")
        .addOption("hide-defaults", "false")
        .addOption("member", "test-locator")
        .getCommandString();

    String describeServerConfigCommand = new CommandStringBuilder("describe config")
        .addOption("hide-defaults", "false")
        .addOption("member", "test-server")
        .getCommandString();

    GfshExecution execution =
        gfshRule.execute(connectCommand, describeLocatorConfigCommand,
            describeServerConfigCommand);
    assertThat(execution.getOutputText()).doesNotContain(PASSWORD);
  }
}
