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
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.examples.security.ExampleSecurityManager;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.categories.SecurityTest;
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

  @Rule
  public RequiresGeodeHome geodeHome = new RequiresGeodeHome();
  @Rule
  public GfshRule gfsh = new GfshRule();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void createDirectoriesAndFiles() throws Exception {
    File geodePropertiesFile = temporaryFolder.newFile("geode.properties");
    File securityPropertiesFile = temporaryFolder.newFile("security.properties");

    Properties geodeProperties = new Properties();
    geodeProperties.setProperty(LOG_LEVEL, "debug");
    geodeProperties.setProperty("security-username", "propertyFileUser");
    geodeProperties.setProperty("security-password", PASSWORD + "-propertyFile");

    try (FileOutputStream fileOutputStream = new FileOutputStream(geodePropertiesFile)) {
      geodeProperties.store(fileOutputStream, null);
    }

    Properties securityProperties = new Properties();
    securityProperties.setProperty(SECURITY_MANAGER, ExampleSecurityManager.class.getName());
    securityProperties.setProperty(SECURITY_JSON, "security.json");
    securityProperties.setProperty("security-file-username", "securityPropertyFileUser");
    securityProperties.setProperty("security-file-password", PASSWORD + "-securityPropertyFile");

    try (FileOutputStream fileOutputStream = new FileOutputStream(securityPropertiesFile)) {
      securityProperties.store(fileOutputStream, null);
    }

    // The json is in the root resource directory.
    createFileFromResource(getResource("/security.json"), temporaryFolder.getRoot(),
        "security.json");

    String startLocatorCmd = new CommandStringBuilder("start locator")
        .addOption("name", "test-locator")
        .addOption("properties-file", geodePropertiesFile.getAbsolutePath())
        .addOption("security-properties-file", securityPropertiesFile.getAbsolutePath())
        .addOption("J", "-Dsecure-username-jd=user-jd")
        .addOption("J", "-Dsecure-password-jd=password-jd")
        .addOption("classpath", temporaryFolder.getRoot().getAbsolutePath())
        .getCommandString();

    String startServerCmd = new CommandStringBuilder("start server")
        .addOption("name", "test-server")
        .addOption("user", "viaStartMemberOptions")
        .addOption("password", PASSWORD + "-viaStartMemberOptions")
        .addOption("properties-file", geodePropertiesFile.getAbsolutePath())
        .addOption("security-properties-file", securityPropertiesFile.getAbsolutePath())
        .addOption("J", "-Dsecure-username-jd=user-jd")
        .addOption("J", "-Dsecure-password-jd=" + PASSWORD + "-password-jd")
        .addOption("server-port", "0")
        .addOption("classpath", temporaryFolder.getRoot().getAbsolutePath())
        .getCommandString();

    gfsh.execute(startLocatorCmd, startServerCmd);
  }

  @Test
  public void logsDoNotContainStringThatShouldBeRedacted() {
    File dir = gfsh.getTemporaryFolder().getRoot();
    File[] logFiles = dir.listFiles((d, name) -> name.endsWith(".log"));

    for (File logFile : logFiles) {
      LogFileAssert.assertThat(logFile).doesNotContain(PASSWORD);
    }
  }

  @Test
  public void describeConfigRedactsJvmArguments() {
    String connectCommand = new CommandStringBuilder("connect")
        .addOption("user", "viaStartMemberOptions")
        .addOption("password", PASSWORD + "-viaStartMemberOptions").getCommandString();

    String describeLocatorConfigCommand = new CommandStringBuilder("describe config")
        .addOption("hide-defaults", "false").addOption("member", "test-locator").getCommandString();

    String describeServerConfigCommand = new CommandStringBuilder("describe config")
        .addOption("hide-defaults", "false").addOption("member", "test-server").getCommandString();

    GfshExecution execution =
        gfsh.execute(connectCommand, describeLocatorConfigCommand, describeServerConfigCommand);
    assertThat(execution.getOutputText()).doesNotContain(PASSWORD);
  }
}
