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

import static org.apache.geode.management.internal.cli.i18n.CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR__DIR;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR__MEMBER_NAME;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR__PORT;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR__PROPERTIES;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR__SECURITY_PROPERTIES;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.MessageFormat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class StartLocatorCommandDUnitTest {
  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testWithConflictingPIDFile() throws Exception {
    final String fileName = ProcessType.LOCATOR.getPidFileName();

    // create dir for pid file
    File dir = tempFolder.newFolder();

    // create pid file
    File pidFile = new File(dir.getAbsolutePath(), fileName);
    assertThat(pidFile.createNewFile()).isTrue();

    // write pid to pid file
    try (FileWriter fileWriter = new FileWriter(pidFile, false)) {
      fileWriter.write(getPidOrOne().toString() + "\n");
      fileWriter.flush();
    }

    assertThat(pidFile.isFile()).isTrue();

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, "locator")
        .addOption(START_LOCATOR__DIR, pidFile.getParentFile().getCanonicalPath())
        .addOption(START_LOCATOR__PORT, "0");

    CommandResult result = gfsh.executeCommand(command.getCommandString());

    if (result.getStatus() == Result.Status.OK) {
      gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
    }

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(
        "Exception in thread \"main\" java.lang.RuntimeException: A PID file already exists and a Locator may be running in "
            + IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(pidFile.getParentFile()));
    assertThat(result.getMessageFromContent()).contains(
        "Caused by: org.apache.geode.internal.process.FileAlreadyExistsException: Pid file already exists: "
            + IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(pidFile));
  }

  @Test
  public void testWithMissingGemFirePropertiesFile() {
    final String missingPropertiesPath = "/path/to/missing/gemfire.properties";
    final String expectedError =
        MessageFormat.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, "", missingPropertiesPath);

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, "locator")
        .addOption(START_LOCATOR__PROPERTIES, missingPropertiesPath);

    CommandResult result = gfsh.executeCommand(command.getCommandString());

    if (result.getStatus() == Result.Status.OK) {
      gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
    }

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithMissingGemFireSecurityPropertiesFile() {
    final String missingSecurityPropertiesPath = "/path/to/missing/gemfire-security.properties";
    final String expectedError = MessageFormat.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE,
        "Security ", missingSecurityPropertiesPath);

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, "locator")
        .addOption(START_LOCATOR__SECURITY_PROPERTIES, missingSecurityPropertiesPath);

    CommandResult result = gfsh.executeCommand(command.getCommandString());

    if (result.getStatus() == Result.Status.OK) {
      gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
    }

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithUnavailablePort() throws IOException {
    final Integer locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String unexpectedMessage = "[" + locatorPort + "] as locator is currently online.";
    final String expectedMessage = "java.net.BindException: Network is unreachable; port ("
        + locatorPort + ") is not available on localhost.";

    Socket interferingProcess = new Socket();
    interferingProcess.bind(new InetSocketAddress(locatorPort));

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, "locator")
        .addOption(START_LOCATOR__PORT, locatorPort.toString());

    CommandResult result = gfsh.executeCommand(command.getCommandString());

    interferingProcess.close();

    if (result.getStatus() == Result.Status.OK) {
      gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
    }

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).doesNotContain(unexpectedMessage);
    assertThat(result.getMessageFromContent()).contains(expectedMessage);
  }

  @Test
  public void testWithAvailablePort() {
    final Integer locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String expectedMessage = "[" + locatorPort + "] as locator is currently online.";

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, "locator")
        .addOption(START_LOCATOR__PORT, locatorPort.toString());

    CommandResult result = gfsh.executeCommand(command.getCommandString());

    if (result.getStatus() == Result.Status.OK) {
      gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
    }

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getMessageFromContent()).contains(expectedMessage);
  }

  @Test
  public void testWithDefaultLocatorPort() {
    final String unexpectedMessage = "[0] as locator is currently online.";
    final String expectedMessage = "\\[\\d+\\] as locator is currently online.";

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, "locator")
        .addOption(START_LOCATOR__PORT, "0");

    CommandResult result = gfsh.executeCommand(command.getCommandString());

    if (result.getStatus() == Result.Status.OK) {
      gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
    }

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getMessageFromContent()).doesNotContain(unexpectedMessage);
    assertThat(result.getMessageFromContent()).containsPattern(expectedMessage);
  }

  @Test
  public void testInMissingRelativeDirectory() {
    final String missingDirPath = "/missing/path/to/start/in";
    final String expectedMessage = "Could not create directory " + missingDirPath
        + ". Please verify directory path or user permissions.";

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, "locator")
        .addOption(START_LOCATOR__DIR, missingDirPath);

    CommandResult result = gfsh.executeCommand(command.getCommandString());

    if (result.getStatus() == Result.Status.OK) {
      gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
    }

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedMessage);
  }

  @Test
  public void testInRelativeDirectory() throws IOException {
    final Integer locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final File dir = tempFolder.newFolder();
    final String expectedMessage =
        "Locator in " + IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(dir);

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, "locator")
        .addOption(START_LOCATOR__DIR, dir.getAbsolutePath())
        .addOption(START_LOCATOR__PORT, locatorPort.toString());

    CommandResult result = gfsh.executeCommand(command.getCommandString());

    if (result.getStatus() == Result.Status.OK) {
      gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
    }

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getMessageFromContent()).contains(expectedMessage);
  }

  @Test
  public void testInCurrentDirectory() {
    final Integer locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String expectedMessage = "Locator in " + System.getProperty("user.dir");
    final String expectedVersionPattern = "Geode Version: \\d+\\.\\d+\\.\\d+";

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, "locator")
        .addOption(START_LOCATOR__PORT, locatorPort.toString());

    CommandResult result = gfsh.executeCommand(command.getCommandString());

    if (result.getStatus() == Result.Status.OK) {
      gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
    }

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getMessageFromContent()).contains(expectedMessage);
    // Verify GEODE-2138
    assertThat(result.getMessageFromContent()).doesNotContain("Gemfire").doesNotContain("GemFire");
    assertThat(result.getMessageFromContent()).containsPattern(expectedVersionPattern);
  }

  /**
   * Attempts to determine the PID of the running process from the ManagementFactory's runtime MBean
   *
   * @return 1 if unable to determine the pid
   * @return the PID if possible
   */
  private Integer getPidOrOne() {
    Integer pid = 1;
    String[] name = ManagementFactory.getRuntimeMXBean().getName().split("@");
    if (name.length > 1) {
      try {
        pid = Integer.parseInt(name[0]);
      } catch (NumberFormatException nex) {
        // Ignored
      }
    }

    return pid;
  }
}
