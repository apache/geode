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

import static org.apache.geode.management.internal.i18n.CliStrings.CACHE_XML_NOT_FOUND_MESSAGE;
import static org.apache.geode.management.internal.i18n.CliStrings.CREATE_REGION;
import static org.apache.geode.management.internal.i18n.CliStrings.CREATE_REGION__REGION;
import static org.apache.geode.management.internal.i18n.CliStrings.CREATE_REGION__REGIONSHORTCUT;
import static org.apache.geode.management.internal.i18n.CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE;
import static org.apache.geode.management.internal.i18n.CliStrings.GROUP;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__CACHE_XML_FILE;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__DIR;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__FORCE;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__LOCATORS;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__MAXHEAP;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__NAME;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__OFF_HEAP_MEMORY_SIZE;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__PROPERTIES;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__SECURITY_PROPERTIES;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__SERVER_PORT;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__USERNAME;
import static org.apache.geode.management.internal.i18n.CliStrings.START_SERVER__USERNAME_LONGFORM;
import static org.apache.geode.test.compiler.ClassBuilder.writeJarFromClasses;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.isA;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.client.internal.locator.GetAllServersRequest;
import org.apache.geode.cache.client.internal.locator.GetAllServersResponse;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.ServerLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedErrorCollector;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class StartServerCommandDUnitTest implements Serializable {

  private static MemberVM locator;
  private static String locatorConnectionString;

  private File workingDir;
  private String memberName;
  private int serverPort;

  @ClassRule
  public static RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    // locator used to clean up members started during tests
    locator = cluster.startLocatorVM(0, 0);

    locatorConnectionString = "localhost[" + locator.getPort() + "]";

    gfsh.connectAndVerify(locator);
  }

  @Before
  public void before() throws IOException {
    workingDir = temporaryFolder.newFolder();
    memberName = testName.getMethodName();
    serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.execute("shutdown --include-locators");
  }

  @Test
  public void testWithMissingCacheXml() throws IOException {
    String missingCacheXmlPath =
        Paths.get("missing", "cache.xml").toAbsolutePath().toString();
    String expectedError =
        CliStrings.format(CACHE_XML_NOT_FOUND_MESSAGE, missingCacheXmlPath);

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
        .addOption(START_SERVER__CACHE_XML_FILE, missingCacheXmlPath)
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(expectedError);
  }

  @Test
  public void testWithMissingGemFirePropertiesFile() throws IOException {
    String missingGemfirePropertiesPath =
        Paths.get("missing", "gemfire.properties").toAbsolutePath().toString();
    String expectedError =
        CliStrings.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, "", missingGemfirePropertiesPath);

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
        .addOption(START_SERVER__PROPERTIES, missingGemfirePropertiesPath)
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(expectedError);
  }

  @Test
  public void testWithUsernameAndMissingPassword() throws IOException {
    String expectedError = "password must be specified.";

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
        .addOption(START_SERVER__USERNAME, "usernameValue")
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(expectedError);
  }

  @Test
  public void testWithUsernameLongFormAndMissingPassword() throws IOException {
    String expectedError = "password must be specified.";

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
        .addOption(START_SERVER__USERNAME_LONGFORM, "usernameValue")
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(expectedError);
  }

  @Test
  public void testWithMissingSecurityPropertiesFile() throws IOException {
    String missingSecurityPropertiesPath =
        Paths.get("missing", "security.properties").toAbsolutePath().toString();
    String expectedError = CliStrings.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE,
        "Security ", missingSecurityPropertiesPath);

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .addOption(START_SERVER__SECURITY_PROPERTIES, missingSecurityPropertiesPath)
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(expectedError);
  }

  @Test
  public void testWithUnavailablePort() throws IOException {
    String expectedError =
        "java.lang.RuntimeException: An IO error occurred while starting a Server";
    String expectedCause = "Caused by: java.net.BindException: "
        + "Network is unreachable; port (" + serverPort + ") is not available on localhost.";

    try (Socket interferingProcess = new Socket()) {
      // make the target port unavailable
      interferingProcess.bind(new InetSocketAddress(serverPort));

      String command = new CommandStringBuilder(START_SERVER)
          .addOption(START_SERVER__NAME, memberName)
          .addOption(START_SERVER__LOCATORS, locatorConnectionString)
          .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
          .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
          .getCommandString();

      gfsh.executeAndAssertThat(command).statusIsError()
          .hasOutput().contains(expectedError, expectedCause);
    }
  }

  @Test
  public void testWithAvailablePort() throws IOException {
    String expectedMessage = "Server in " + workingDir.getCanonicalPath();
    String expectedMessage2 = "as " + memberName + " is currently online.";

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .hasOutput().contains(expectedMessage, expectedMessage2);
  }

  @Test
  public void testWithConflictingPIDFile() throws Exception {
    String fileName = ProcessType.SERVER.getPidFileName();

    // create PID file
    File pidFile = new File(workingDir.getAbsolutePath(), fileName);
    assertThat(pidFile.createNewFile()).isTrue();

    // write PID to PID file
    try (FileWriter fileWriter = new FileWriter(pidFile, false)) {
      fileWriter.write(ProcessUtils.identifyPid() + "\n");
      fileWriter.flush();
    }
    assertThat(pidFile.isFile()).isTrue();

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
        .addOption(START_SERVER__DIR, pidFile.getParentFile().getCanonicalPath())
        .getCommandString();

    String expectedError = "A PID file already exists and a Server may be running in "
        + pidFile.getParentFile().getCanonicalPath();
    String expectedCause = "Caused by: "
        + "org.apache.geode.internal.process.FileAlreadyExistsException: Pid file already exists: "
        + pidFile.getCanonicalPath();

    gfsh.executeAndAssertThat(command).statusIsError()
        .hasOutput().contains(expectedError, expectedCause);
  }

  @Test
  public void testWithForceOverwriteConflictingPIDFile() throws Exception {
    String fileName = ProcessType.SERVER.getPidFileName();

    // create PID file
    File pidFile = new File(workingDir.getAbsolutePath(), fileName);
    assertThat(pidFile.createNewFile()).isTrue();

    // write PID to PID file
    try (FileWriter fileWriter = new FileWriter(pidFile, false)) {
      fileWriter.write(ProcessUtils.identifyPid() + "\n");
      fileWriter.flush();
    }
    assertThat(pidFile.isFile()).isTrue();

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
        .addOption(START_SERVER__DIR, pidFile.getParentFile().getCanonicalPath())
        .addOption(START_SERVER__FORCE, "true")
        .getCommandString();

    String expectedMessage = "Server in " + pidFile.getParentFile().getCanonicalPath();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput(expectedMessage);
  }

  @Test
  public void testWithConnectionToLocator() throws IOException {
    String expectedVersionPattern = "Geode Version: \\d+\\.\\d+\\.\\d+";
    String expectedMessage = "Server in " + workingDir.getCanonicalPath();

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .hasOutput()
        .contains(expectedMessage)
        .doesNotContainPattern("Gem[Ff]ire Version")
        .containsPattern(expectedVersionPattern);
  }

  @Test
  public void testServerJVMTerminatesOnOutOfMemoryError() throws IOException {
    String groupName = "serverGroup";
    String regionName = "testRegion";

    MemberVM server = cluster.startServerVM(1, locator.getPort());

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption("group", groupName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__OFF_HEAP_MEMORY_SIZE, "0M")
        .addOption(START_SERVER__MAXHEAP, "300M")
        .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess();

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

    String jarName = "RunOutOfMemory.jar";
    File jar = temporaryFolder.newFile(jarName);
    writeJarFromClasses(jar, RunOutOfMemoryFunction.class);
    gfsh.executeAndAssertThat("deploy --groups=" + groupName + " --jar=" + jar).statusIsSuccess();

    locator.invoke((() -> {
      try {
        FunctionService.onMember(groupName).execute(new RunOutOfMemoryFunction()).getResult();
      } catch (FunctionException e) {
        errorCollector.checkThat(e, isA(FunctionException.class));
      }
    }));

    gfsh.executeAndAssertThat("list members").statusIsSuccess().doesNotContainOutput(memberName)
        .containsOutput(server.getName());

    server.stop(Boolean.TRUE);

    assertThat(ProcessUtils.isProcessAlive(serverPid)).isFalse();
  }


  @Test
  public void startServerRespectsHostnameForClients() throws IOException {
    String expectedMessagePattern = "Server (.*) is currently online";

    String command = new CommandStringBuilder(START_SERVER)
        .addOption(START_SERVER__NAME, memberName)
        .addOption(START_SERVER__LOCATORS, locatorConnectionString)
        .addOption(START_SERVER__DIR, workingDir.getCanonicalPath())
        .addOption(START_SERVER__SERVER_PORT, String.valueOf(serverPort))
        .addOption(START_SERVER__HOSTNAME__FOR__CLIENTS, "fakeServerName")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .hasOutput().containsPattern(expectedMessagePattern);

    // Verify that the ServerLocation contains the specified hostname-for-clients.
    locator.invoke(() -> {
      InternalLocator internalLocator = InternalLocator.getLocator();
      final ServerLocator serverLocator = internalLocator.getServerLocatorAdvisee();
      GetAllServersResponse serversResponse =
          (GetAllServersResponse) serverLocator.processRequest(new GetAllServersRequest());
      List<ServerLocation> locators = serversResponse.getServers();

      assertThat(locators.stream().filter(
          serverLocation -> "fakeServerName".equals(serverLocation.getHostName())))
              .isNotEmpty().hasSize(1);
    });
  }
}
