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

import static org.apache.geode.internal.i18n.LocalizedStrings.Launcher_Command_START_PID_FILE_ALREADY_EXISTS_ERROR_MESSAGE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.LOCATOR_TERM_NAME;
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.MessageFormat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class StartLocatorCommandDUnitTest {
  @Rule
  public GfshRule gfsh = new GfshRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testWithConflictingPIDFile() throws Exception {
    final String fileName = ProcessType.LOCATOR.getPidFileName();
    final String memberName = "testWithConflictingPIDFile-locator";

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
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__DIR, pidFile.getParentFile().getCanonicalPath())
        .addOption(START_LOCATOR__PORT, "0");

    String result = GfshScript.of(command.getCommandString()).execute(gfsh).getOutputText();

    final String expectedError = Launcher_Command_START_PID_FILE_ALREADY_EXISTS_ERROR_MESSAGE
        .toString(LOCATOR_TERM_NAME, pidFile.getParentFile().getCanonicalPath(),
            InetAddress.getLocalHost().getHostAddress() + "[0]");
    final String expectedCause = "Caused by: "
        + "org.apache.geode.internal.process.FileAlreadyExistsException: Pid file already exists: "
        + pidFile.getCanonicalPath();

    assertThat(result).contains(expectedError);
    assertThat(result).contains(expectedCause);
  }

  @Test
  public void testWithMissingGemFirePropertiesFile() throws IOException {
    final String missingPropertiesPath = "/path/to/missing/gemfire.properties";
    final String memberName = "testWithMissingGemFirePropertiesFile-locator";
    final String expectedError =
        MessageFormat.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, "", missingPropertiesPath);

    File workingDir = tempFolder.newFolder();

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__DIR, workingDir.getAbsolutePath())
        .addOption(START_LOCATOR__PROPERTIES, missingPropertiesPath);

    GfshExecution execution = GfshScript.of(command.getCommandString()).execute(gfsh);
    String result = execution.getOutputText();

    assertThat(result).contains(expectedError);
  }

  @Test
  public void testWithMissingGemFireSecurityPropertiesFile() throws IOException {
    final String missingSecurityPropertiesPath = "/path/to/missing/gemfire-security.properties";
    final String memberName = "testWithMissingGemFireSecurityPropertiesFile-locator";
    final String expectedError = MessageFormat.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE,
        "Security ", missingSecurityPropertiesPath);

    File workingDir = tempFolder.newFolder();

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__DIR, workingDir.getAbsolutePath())
        .addOption(START_LOCATOR__SECURITY_PROPERTIES, missingSecurityPropertiesPath);

    GfshExecution execution = GfshScript.of(command.getCommandString()).execute(gfsh);
    String result = execution.getOutputText();

    assertThat(result).contains(expectedError);
  }

  @Test
  public void testWithUnavailablePort() throws IOException {
    final Integer locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testWithUnavailablePort-locator";
    final String unexpectedMessage = "[" + locatorPort + "] as locator is currently online.";
    final String expectedMessage = "java.net.BindException: Network is unreachable; port ("
        + locatorPort + ") is not available on localhost.";

    Socket interferingProcess = new Socket();
    interferingProcess.bind(new InetSocketAddress(locatorPort));

    File workingDir = tempFolder.newFolder();

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__DIR, workingDir.getAbsolutePath())
        .addOption(START_LOCATOR__PORT, locatorPort.toString());

    GfshExecution execution = GfshScript.of(command.getCommandString()).execute(gfsh);
    String result = execution.getOutputText();

    interferingProcess.close();

    assertThat(result).doesNotContain(unexpectedMessage);
    assertThat(result).contains(expectedMessage);
  }

  @Test
  public void testWithAvailablePort() throws IOException {
    final Integer locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testWithAvailablePort-locator";
    final String expectedMessage =
        "[" + locatorPort + "] as " + memberName + " is currently online.";

    File workingDir = tempFolder.newFolder();

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__DIR, workingDir.getAbsolutePath())
        .addOption(START_LOCATOR__PORT, locatorPort.toString());

    GfshExecution execution = GfshScript.of(command.getCommandString()).execute(gfsh);
    String result = execution.getOutputText();

    assertThat(result).contains(expectedMessage);
  }

  @Test
  public void testWithDefaultLocatorPort() throws IOException {
    final String memberName = "testWithDefaultLocatorPort-locator";
    final String unexpectedMessage = "[0] as " + memberName + " is currently online.";
    final String expectedMessage = "\\[\\d+\\] as " + memberName + " is currently online.";

    File workingDir = tempFolder.newFolder();

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__DIR, workingDir.getAbsolutePath())
        .addOption(START_LOCATOR__PORT, "0");

    GfshExecution execution = GfshScript.of(command.getCommandString()).execute(gfsh);
    String result = execution.getOutputText();

    assertThat(result).doesNotContain(unexpectedMessage);
    assertThat(result).containsPattern(expectedMessage);
  }

  @Test
  public void testInMissingRelativeDirectory() throws IOException {
    final String missingDirPath = "/missing/path/to/start/in";
    final String expectedMessage = "Could not create directory " + missingDirPath
        + ". Please verify directory path or user permissions.";
    final String memberName = "testWithMissingRelativeDirectory-locator";

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__DIR, missingDirPath);

    GfshExecution execution = GfshScript.of(command.getCommandString()).execute(gfsh);
    String result = execution.getOutputText();

    assertThat(result).contains(expectedMessage);
  }

  @Test
  public void testWithRelativeDirectory() throws IOException {
    final Integer locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final File dir = tempFolder.newFolder();
    final String memberName = "testWithRelativeDirectory-locator";
    final String expectedMessage =
        "Locator in " + IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(dir);

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__DIR, dir.getAbsolutePath())
        .addOption(START_LOCATOR__PORT, locatorPort.toString());

    GfshExecution execution = GfshScript.of(command.getCommandString()).execute(gfsh);
    String result = execution.getOutputText();

    assertThat(result).contains(expectedMessage);
  }

  @Test
  public void testWithCurrentDirectory() throws IOException {
    final Integer locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String expectedMessage = "Locator in " + System.getProperty("user.dir");
    final String expectedVersionPattern = "Geode Version: \\d+\\.\\d+\\.\\d+";
    final String memberName = "testWithCurrentDirectory-locator";

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__PORT, locatorPort.toString());

    try {
      GfshExecution execution = GfshScript.of(command.getCommandString()).execute(gfsh);
    String result = execution.getOutputText();

      assertThat(result).contains(expectedMessage);

      // Verify GEODE-2138
      assertThat(result).doesNotContain("Gemfire").doesNotContain("GemFire");
      assertThat(result).containsPattern(expectedVersionPattern);
    } finally {
      String pathToFile = System.getProperty("user.dir") + "/" + memberName;
      File toDelete = new File(pathToFile);
      deleteLocatorFiles(toDelete);
    }
  }

  private void deleteLocatorFiles(File toDelete) {
    File[] nestedToDelete = toDelete.listFiles();

    if (nestedToDelete != null && nestedToDelete.length > 0) {
      for (File file : nestedToDelete) {
        deleteLocatorFiles(file);
      }
    }

    toDelete.delete();
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
