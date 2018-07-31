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

import static org.apache.geode.management.internal.cli.i18n.CliStrings.CACHE_XML_NOT_FOUND_MESSAGE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_REGION;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_REGION__REGIONSHORTCUT;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__CACHE_XML_FILE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__DIR;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__FORCE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__FORCE__HELP;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__LOCATORS;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__NAME;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__PROPERTIES;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__SECURITY_PROPERTIES;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__SERVER_PORT;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.Socket;

import okhttp3.internal.Internal;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.ServerLauncher.ServerState;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class StartServerCommandDUnitTest {
  private static MemberVM locator;
  private static String locatorConnectionString;

  @ClassRule
  public static final ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static final GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void before() throws Exception {
    // locator used to clean up members started during tests
    locator = cluster.startLocatorVM(0, 0);

    locatorConnectionString = "localhost[" + locator.getPort() + "]";

    gfsh.connectAndVerify(locator);
  }

  @AfterClass
  public static void after() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.execute("shutdown --include-locators");
  }

  @Test
  public void testWithMissingCacheXml() {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String missingCacheXmlPath = "/missing/cache.xml";
    final String memberName = "testWithMissingCacheXml-server";
    final String expectedError = CliStrings.format(CACHE_XML_NOT_FOUND_MESSAGE, missingCacheXmlPath);

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__CACHE_XML_FILE, missingCacheXmlPath)
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithMissingGemFirePropertiesFile() {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String missingGemfirePropertiesPath = "/missing/gemfire.properties";
    final String memberName = "testWithMissingGemFirePropertiesFile-server";
    final String expectedError = CliStrings.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, "", missingGemfirePropertiesPath);

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__PROPERTIES, missingGemfirePropertiesPath)
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithMissingPassword() {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testWithMissingPassword-server";
    final String expectedError = "password must be specified.";

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__USERNAME, "usernameValue")
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithMissingSecurityPropertiesFile() {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String missingSecurityPropertiesPath = "/missing/security.properties";
    final String memberName = "testWithMissingSecurityPropertiesFile-server";
    final String expectedError = CliStrings.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, "Security ", missingSecurityPropertiesPath);

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__SECURITY_PROPERTIES, missingSecurityPropertiesPath)
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithUnavailablePort() throws IOException {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testWithUnavailablePort-server";
    final String expectedError = "java.lang.RuntimeException: An IO error occurred while starting a Server";
    final String expectedCause = "Caused by: java.net.BindException: "
        + "Network is unreachable; port (" + serverPort + ") is not available on localhost.";

    try(Socket interferingProcess = new Socket()) {
      interferingProcess.bind(new InetSocketAddress(serverPort)); // make the target port unavailable

      String command = new CommandStringBuilder(START_SERVER)
          .addOption(START_SERVER__NAME, memberName)
          .addOption(START_SERVER__LOCATORS, locatorConnectionString)
          .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
          .getCommandString();

      CommandResult result = gfsh.executeCommand(command);

      assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
      assertThat(result.getMessageFromContent()).contains(expectedError).contains(expectedCause);
    }
  }

  @Test
  public void testWithAvailablePort() {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testWithAvailablePort-server";
    final String expectedMessage = "Server in " + System.getProperty("user.dir") + "/" + memberName;
    final String expectedMessage2 = "as " + memberName + " is currently online.";

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getMessageFromContent()).contains(expectedMessage).contains(expectedMessage2);
  }

  @Test
  public void testWithMissingStartDirectoryThatCannotBeCreated() {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String missingDirPath = "/missing/dir/to/start/in";
    final String memberName = "testWithMissingStartDirectoryThatCannotBeCreated-server";
    final String expectedError = "Could not create directory " + missingDirPath
        + ". Please verify directory path or user permissions.";

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__DIR, missingDirPath)
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithMissingStartDirectoryThatCanBeCreated() {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String missingDirPath = System.getProperty("user.dir") + "/missing/dir/to/start/in";
    final String memberName = "testWithMissingStartDirectoryThatCanBeCreated-server";
    final String expectedMessage = "Server in " + missingDirPath;

    try {
      String command = new CommandStringBuilder(START_SERVER)
          .addOption(START_SERVER__NAME, memberName)
          .addOption(START_SERVER__LOCATORS, locatorConnectionString)
          .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
          .addOption(START_SERVER__DIR, missingDirPath)
          .getCommandString();

      CommandResult result = gfsh.executeCommand(command);

      assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
      assertThat(result.getMessageFromContent()).contains(expectedMessage);
    } finally {
      File toDelete = new File(missingDirPath);
      deleteServerFiles(toDelete);
    }
  }

  @Test
  public void testWithConflictingPIDFile() throws IOException {
    final String fileName = ProcessType.SERVER.getPidFileName();
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testWithConflictingPIDFile-server";

    // create PID file
    File dir = temporaryFolder.newFolder();
    File pidFile = new File(dir.getAbsolutePath(), fileName);
    assertThat(pidFile.createNewFile()).isTrue();

    // write PID to PID file
    try (FileWriter fileWriter = new FileWriter(pidFile, false)) {
      fileWriter.write(getPidOrOne().toString() + "\n");
      fileWriter.flush();
    }
    assertThat(pidFile.isFile()).isTrue();

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__DIR, pidFile.getParentFile().getCanonicalPath())
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    final String expectedError = "A PID file already exists and a Server may be running in "
        + pidFile.getParentFile().getCanonicalPath();
    final String expectedCause = "Caused by: "
        + "org.apache.geode.internal.process.FileAlreadyExistsException: Pid file already exists: "
        + pidFile.getCanonicalPath();

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError).contains(expectedCause);

  }

  @Test
  public void testWithForceOverwriteConflictingPIDFile() throws IOException {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testWithForceOverwriteConflictingPIDFile-server";
    final String fileName = ProcessType.SERVER.getPidFileName();

    // create PID file
    File dir = temporaryFolder.newFolder();
    File pidFile = new File(dir.getAbsolutePath(), fileName);
    assertThat(pidFile.createNewFile()).isTrue();

    // write PID to PID file
    try (FileWriter fileWriter = new FileWriter(pidFile, false)) {
      fileWriter.write(getPidOrOne().toString() + "\n");
      fileWriter.flush();
    }
    assertThat(pidFile.isFile()).isTrue();

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__DIR, pidFile.getParentFile().getCanonicalPath())
        .addOption(START_SERVER__FORCE, "true")
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    final String expectedMessage = "Server in " + pidFile.getParentFile().getCanonicalPath();

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getMessageFromContent()).contains(expectedMessage);
  }

  @Test
  public void testWithConnectionToLocator() {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testWithConnectionToLocator-server";
    final String expectedVersionPattern = "Geode Version: \\d+\\.\\d+\\.\\d+";
    final String expectedMessage = "Server in " + System.getProperty("user.dir") + "/" + memberName;

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(result.getMessageFromContent()).contains(expectedMessage);

    // verify GEODE-2138 (Geode commands do not contain GemFire in output)
    assertThat(result.getMessageFromContent()).doesNotContain("Gemfire")
        .doesNotContain("GemFire");
    assertThat(result.getMessageFromContent()).containsPattern(expectedVersionPattern);
  }

  @Test
  public void testServerJVMTerminatesOnOutOfMemoryError() {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testServerJVMTerminatesOnOutOfMemoryError-server";
    final String serverDir = System.getProperty("user.dir") + "/" + memberName;
    final String regionName = "serverRegion";
    final String regionPath = "/" + regionName;

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__DIR, serverDir)
        .getCommandString();
    CommandResult result = gfsh.executeCommand(command);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .setCommand(ServerLauncher.Command.STATUS)
        .setWorkingDirectory(serverDir)
        .build();
    assertThat(serverLauncher).isNotNull();

    ServerState serverState = serverLauncher.status();
    assertThat(serverState.getStatus()).isEqualTo(Status.ONLINE);
    assertThat(serverState.isVmWithProcessIdRunning()).isTrue();

    command = new CommandStringBuilder(CREATE_REGION)
        .addOption("name", regionName)
        .addOption(CREATE_REGION__REGIONSHORTCUT, "partition")
        .getCommandString();
    result = gfsh.executeCommand(command);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(regionPath, 1);

    // put too much data
    locator.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion(regionPath);
      System.out.println("HERE: " + region);
      System.out.println(region.getName());
    });

    serverState = serverLauncher.status();
    assertThat(serverState.isVmWithProcessIdRunning()).isFalse();
    assertThat(serverState.getStatus()).isEqualTo(Status.NOT_RESPONDING);
  }

  @Test
  public void testOutOfMemoryErrorWithDisableExitOption() {}

  private void deleteServerFiles(File toDelete) {
    File[] nestedToDelete = toDelete.listFiles();

    if (nestedToDelete != null && nestedToDelete.length > 0) {
      for (File file : nestedToDelete) {
        deleteServerFiles(file);
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
