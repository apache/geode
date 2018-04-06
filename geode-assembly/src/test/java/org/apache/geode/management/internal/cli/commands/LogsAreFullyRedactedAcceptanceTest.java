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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.security.ExampleSecurityManager;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.categories.AcceptanceTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.util.test.TestUtil;

/**
 * This test class expects a "security.json" file to be visible on the member classpath. The
 * security.json is consumed by {@link org.apache.geode.examples.security.ExampleSecurityManager}
 * and should contain each username/password combination present (even though not all will actually
 * be consumed by the Security Manager).
 *
 * Each password shares the string below for easier log scanning.
 */
@Category(AcceptanceTest.class)
public class LogsAreFullyRedactedAcceptanceTest {
  private static String sharedPasswordString = "abcdefg";

  private File propertyFile;
  private File securityPropertyFile;

  @Rule
  public GfshRule gfsh = new GfshRule();

  @Before
  public void createDirectoriesAndFiles() throws IOException {
    propertyFile = gfsh.getTemporaryFolder().newFile("geode.properties");
    securityPropertyFile = gfsh.getTemporaryFolder().newFile("security.properties");

    Properties properties = new Properties();
    properties.setProperty(LOG_LEVEL, "debug");
    properties.setProperty("security-username", "propertyFileUser");
    properties.setProperty("security-password", sharedPasswordString + "-propertyFile");
    try (FileOutputStream fileOutputStream = new FileOutputStream(propertyFile)) {
      properties.store(fileOutputStream, null);
    }

    Properties securityProperties = new Properties();
    securityProperties.setProperty(SECURITY_MANAGER, ExampleSecurityManager.class.getName());
    securityProperties.setProperty(ExampleSecurityManager.SECURITY_JSON, "security.json");
    securityProperties.setProperty("security-file-username", "securityPropertyFileUser");
    securityProperties.setProperty("security-file-password",
        sharedPasswordString + "-securityPropertyFile");
    try (FileOutputStream fileOutputStream = new FileOutputStream(securityPropertyFile)) {
      securityProperties.store(fileOutputStream, null);
    }
  }

  @Test
  public void logsDoNotContainStringThatShouldBeRedacted() throws FileNotFoundException {
    // The json is in the root resource directory.
    String securityJson =
        TestUtil.getResourcePath(LogsAreFullyRedactedAcceptanceTest.class, "/security.json");
    // We want to add the folder to the classpath, so we strip off the filename.
    securityJson = securityJson.substring(0, securityJson.length() - "security.json".length());
    String startLocatorCmd =
        new CommandStringBuilder("start locator").addOption("name", "test-locator")
            .addOption("properties-file", propertyFile.getAbsolutePath())
            .addOption("security-properties-file", securityPropertyFile.getAbsolutePath())
            .addOption("J", "-Dsecure-username-jd=user-jd")
            .addOption("J", "-Dsecure-password-jd=password-jd").addOption("classpath", securityJson)
            .getCommandString();

    String startServerCmd = new CommandStringBuilder("start server")
        .addOption("name", "test-server").addOption("user", "viaStartMemberOptions")
        .addOption("password", sharedPasswordString + "-viaStartMemberOptions")
        .addOption("properties-file", propertyFile.getAbsolutePath())
        .addOption("security-properties-file", securityPropertyFile.getAbsolutePath())
        .addOption("J", "-Dsecure-username-jd=user-jd")
        .addOption("J", "-Dsecure-password-jd=" + sharedPasswordString + "-password-jd")
        .addOption("classpath", securityJson).getCommandString();


    gfsh.execute(startLocatorCmd, startServerCmd);

    Collection<File> logs =
        FileUtils.listFiles(gfsh.getTemporaryFolder().getRoot(), new String[] {"log"}, true);

    // Use soft assertions to report all redaction failures, not just the first one.
    SoftAssertions softly = new SoftAssertions();
    for (File logFile : logs) {
      Scanner scanner = new Scanner(logFile);
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        softly.assertThat(line).describedAs("File: %s, Line: %s", logFile.getAbsolutePath(), line)
            .doesNotContain(sharedPasswordString);
      }
    }
    softly.assertAll();
  }
}
