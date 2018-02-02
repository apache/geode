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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.categories.AcceptanceTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

@Category(AcceptanceTest.class)
public class LogsAreFullyRedactedAcceptanceTest {

  private static String PASSWORD = "abcdefghijklmn";

  // Don't put "password" in all the key names, since that is one of our taboo words.
  private static final String gfshSysEnvUsername = "log-no-test.security-username";
  private static final String gfshSysEnvUsernameValue = "test.sysenv-username";
  private static final String gfshSysEnvPassword = "log-no-test.security-password";
  private static final String gfshSysEnvPasswordValue = PASSWORD + "-sysenv";

  private static final String gfshSysEnvUsernamePost = "security-username.log-no-test";
  private static final String gfshSysEnvUsernameValuePost = "sysenv-username.test";
  private static final String gfshSysEnvPasswordPost = "security-password.log-no-test";
  private static final String gfshSysEnvPasswordValuePost = PASSWORD + "-post";



  private File propertyFile;
  private File securityPropertyFile;

  @Rule
  public GfshRule gfsh = new GfshRule();

  @BeforeClass
  public static void addSecuritySystemProperties() {
    System.setProperty(gfshSysEnvUsername, gfshSysEnvUsernameValue);
    System.setProperty(gfshSysEnvPassword, gfshSysEnvPasswordValue);
    System.setProperty(gfshSysEnvUsernamePost, gfshSysEnvUsernameValuePost);
    System.setProperty(gfshSysEnvPasswordPost, gfshSysEnvPasswordValuePost);
  }

  @Before
  public void createPropertyFiles() throws IOException {
    propertyFile = gfsh.getTemporaryFolder().newFile("geode.properties");
    securityPropertyFile = gfsh.getTemporaryFolder().newFile("security.properties");

    Properties properties = new Properties();
    properties.setProperty(LOG_LEVEL, "debug");
    try (FileOutputStream fileOutputStream = new FileOutputStream(propertyFile)) {
      properties.store(fileOutputStream, null);
    }

    Properties securityProperties = new Properties();
    securityProperties.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    try (FileOutputStream fileOutputStream = new FileOutputStream(securityPropertyFile)) {
      securityProperties.store(fileOutputStream, null);
    }
  }

  @AfterClass
  public static void removeSecuritySystemProperties() {
    System.clearProperty(gfshSysEnvUsername);
    System.clearProperty(gfshSysEnvPassword);
    System.clearProperty(gfshSysEnvUsernamePost);
    System.clearProperty(gfshSysEnvPasswordPost);
  }

  @Test
  public void logsDoNotContainStringThatShouldBeRedacted() throws FileNotFoundException {
    String startLocatorCmd = new CommandStringBuilder("start locator")
        .addOption("properties-file", propertyFile.getAbsolutePath())
        .addOption("security-properties-file", securityPropertyFile.getAbsolutePath())
        .addOption("name", "test-locator").getCommandString();

    // Since we're in geode-assembly and rely on the SimpleSecurityManager, we'll need to also
    // make sure no --password=cluster winds up in our logs, since that's what we need to use here.
    String startServerCmd = new CommandStringBuilder("start server")
        .addOption("properties-file", propertyFile.getAbsolutePath())
        .addOption("security-properties-file", securityPropertyFile.getAbsolutePath())
        .addOption("user", "cluster").addOption("password", "cluster")
        .addOption("name", "test-server").getCommandString();


    gfsh.execute(startLocatorCmd, startServerCmd);

    Collection<File> logs =
        FileUtils.listFiles(gfsh.getTemporaryFolder().getRoot(), new String[] {"log"}, true);

    // Use soft assertions to report all redaction failures, not just the first one.
    SoftAssertions softly = new SoftAssertions();
    for (File logFile : logs) {
      Scanner scanner = new Scanner(logFile);
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        System.out.println(line);
        softly.assertThat(line).describedAs("File: %s, Line: %s", logFile.getAbsolutePath(), line)
            .doesNotContain(PASSWORD).doesNotContainPattern("password.*cluster");
      }
    }
    softly.assertAll();
  }
}
