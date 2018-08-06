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
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_REGION__REGION;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.CREATE_REGION__REGIONSHORTCUT;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.GROUP;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__CACHE_XML_FILE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__DIR;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__FORCE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__LOCATORS;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__NAME;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__OFF_HEAP_MEMORY_SIZE;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__PROPERTIES;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__SECURITY_PROPERTIES;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__SERVER_PORT;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class StartServerCommandDUnitTest {
  private static MemberVM locator;
  private static String locatorConnectionString;
  private File workingDir;

  @ClassRule
  public static final ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static final GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    workingDir = temporaryFolder.newFolder();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    // locator used to clean up members started during tests
    locator = cluster.startLocatorVM(0, 0);

    locatorConnectionString = "localhost[" + locator.getPort() + "]";

    gfsh.connectAndVerify(locator);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.execute("shutdown --include-locators");
  }

  @Test
  public void testWithMissingCacheXml() throws IOException {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String missingCacheXmlPath = "/missing/cache.xml";
    final String memberName = "testWithMissingCacheXml-server";
    final String expectedError =
        CliStrings.format(CACHE_XML_NOT_FOUND_MESSAGE, missingCacheXmlPath);

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__CACHE_XML_FILE, missingCacheXmlPath)
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithMissingGemFirePropertiesFile() throws IOException {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String missingGemfirePropertiesPath = "/missing/gemfire.properties";
    final String memberName = "testWithMissingGemFirePropertiesFile-server";
    final String expectedError =
        CliStrings.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, "", missingGemfirePropertiesPath);

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__PROPERTIES, missingGemfirePropertiesPath)
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithMissingPassword() throws IOException {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testWithMissingPassword-server";
    final String expectedError = "password must be specified.";

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__USERNAME, "usernameValue")
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithMissingSecurityPropertiesFile() throws IOException {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String missingSecurityPropertiesPath = "/missing/security.properties";
    final String memberName = "testWithMissingSecurityPropertiesFile-server";
    final String expectedError = CliStrings.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE,
        "Security ", missingSecurityPropertiesPath);

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
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
    final String expectedError =
        "java.lang.RuntimeException: An IO error occurred while starting a Server";
    final String expectedCause = "Caused by: java.net.BindException: "
        + "Network is unreachable; port (" + serverPort + ") is not available on localhost.";

    try (Socket interferingProcess = new Socket()) {
      interferingProcess.bind(new InetSocketAddress(serverPort)); // make the target port
                                                                  // unavailable

      String command = new CommandStringBuilder(START_SERVER)
          .addOption(START_SERVER__NAME, memberName)
          .addOption(START_SERVER__LOCATORS, locatorConnectionString)
          .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
          .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
          .getCommandString();

      CommandResult result = gfsh.executeCommand(command);

      assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
      assertThat(result.getMessageFromContent()).contains(expectedError).contains(expectedCause);
    }
  }

  @Test
  public void testWithAvailablePort() throws IOException {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testWithAvailablePort-server";
    final String expectedMessage = "Server in " + workingDir.getCanonicalPath();
    final String expectedMessage2 = "as " + memberName + " is currently online.";

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
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
  public void testWithMissingStartDirectoryThatCanBeCreated() throws IOException {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String missingDirPath = workingDir.getCanonicalPath() + "/missing/dir/to/start/in";
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
    File pidFile = new File(workingDir.getAbsolutePath(), fileName);
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
    File pidFile = new File(workingDir.getAbsolutePath(), fileName);
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
  public void testWithConnectionToLocator() throws IOException {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testWithConnectionToLocator-server";
    final String expectedVersionPattern = "Geode Version: \\d+\\.\\d+\\.\\d+";
    final String expectedMessage = "Server in " + workingDir.getCanonicalPath();

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
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
  public void testServerJVMTerminatesOnOutOfMemoryError() throws IOException {
    final Integer serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String memberName = "testServerJVMTerminatesOnOutOfMemoryError-server";
    final String groupName = "serverGroup";
    final String regionName = "testRegion";

    MemberVM server = cluster.startServerVM(1, locator.getPort());

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption("group", groupName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__OFF_HEAP_MEMORY_SIZE, "0M")
        .addOption(START_SERVER__SERVER_PORT, serverPort.toString())
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);

    command = new CommandStringBuilder(CREATE_REGION)
        .addOption(CREATE_REGION__REGIONSHORTCUT, "PARTITION")
        .addOption(GROUP, groupName)
        .addOption(CREATE_REGION__REGION, regionName)
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess();

    ServerLauncher serverLauncher = new ServerLauncher.Builder()
        .setCommand(ServerLauncher.Command.STATUS)
        .setWorkingDirectory(workingDir.getCanonicalPath())
        .build();
    assertThat(serverLauncher).isNotNull();

    ServerLauncher.ServerState serverState = serverLauncher.status();
    assertThat(serverState.getStatus()).isEqualTo(AbstractLauncher.Status.ONLINE);
    assertThat(serverState.isVmWithProcessIdRunning()).isTrue();

    Integer serverPid = serverState.getPid();
    assertThat(ProcessUtils.isProcessAlive(serverPid)).isTrue();

    final String jarName = "RunOutOfMemory.jar";
    File jar = temporaryFolder.newFile(jarName);
    new ClassBuilder().writeJarFromClass(new RunOutOfMemoryFunction().getClass(), jar);
    gfsh.executeAndAssertThat("deploy --groups=" + groupName + " --jar=" + jar).statusIsSuccess();

    locator.invoke((() -> {
      try {
        FunctionService.onMember(groupName).execute(new RunOutOfMemoryFunction()).getResult();
      } catch (FunctionException e) {
        e.printStackTrace();
      }
    }));

    gfsh.executeAndAssertThat("list members").statusIsSuccess().doesNotContainOutput(memberName)
        .containsOutput(server.getName());

    server.stop(Boolean.TRUE);

    assertThat(ProcessUtils.isProcessAlive(serverPid)).isFalse();
  }

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
