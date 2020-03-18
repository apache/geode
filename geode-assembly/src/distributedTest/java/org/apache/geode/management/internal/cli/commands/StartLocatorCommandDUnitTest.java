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

import static org.apache.geode.management.internal.i18n.CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__CONNECT;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__DIR;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__HOSTNAME_FOR_CLIENTS;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__LOCATORS;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__MEMBER_NAME;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__PORT;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__PROPERTIES;
import static org.apache.geode.management.internal.i18n.CliStrings.START_LOCATOR__SECURITY_PROPERTIES;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.client.internal.locator.LocatorListRequest;
import org.apache.geode.cache.client.internal.locator.LocatorListResponse;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.ServerLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

public class StartLocatorCommandDUnitTest {

  private static MemberVM locator;
  private static String locatorConnectionString;

  private String memberName;

  @ClassRule
  public static RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void before() {
    // locator used to clean up JVMs after test
    locator = cluster.startLocatorVM(0, 0);

    locatorConnectionString = "localhost[" + locator.getPort() + "]";
  }

  @Before
  public void setUp() {
    memberName = testName.getMethodName();
  }

  @AfterClass
  public static void after() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.execute("shutdown --include-locators");
  }

  @Test
  public void testWithConflictingPIDFile() throws Exception {
    String fileName = ProcessType.LOCATOR.getPidFileName();

    // create dir for pid file
    File dir = temporaryFolder.newFolder();

    // create pid file
    File pidFile = new File(dir.getAbsolutePath(), fileName);
    assertThat(pidFile.createNewFile()).isTrue();

    // write pid to pid file
    try (FileWriter fileWriter = new FileWriter(pidFile, false)) {
      fileWriter.write(ProcessUtils.identifyPid() + "\n");
      fileWriter.flush();
    }

    assertThat(pidFile.isFile()).isTrue();

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__LOCATORS, locatorConnectionString)
        .addOption(START_LOCATOR__DIR, pidFile.getParentFile().getCanonicalPath())
        .addOption(START_LOCATOR__PORT, "0")
        .addOption("J", "-Dgemfire.enable-management-rest-service=false");



    String expectedError = "A PID file already exists and a Locator may be running in "
        + pidFile.getParentFile().getCanonicalPath();
    String expectedCause = "Caused by: "
        + "org.apache.geode.internal.process.FileAlreadyExistsException: Pid file already exists: "
        + pidFile.getCanonicalPath();

    gfsh.executeAndAssertThat(command.getCommandString()).statusIsError()
        .containsOutput(expectedError, expectedCause);
  }

  @Test
  public void testWithMissingGemFirePropertiesFile() throws IOException {
    String missingPropertiesPath =
        Paths.get("missing", "gemfire.properties").toAbsolutePath().toString();
    String expectedError =
        MessageFormat.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, "", missingPropertiesPath);

    File workingDir = temporaryFolder.newFolder();

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__DIR, workingDir.getAbsolutePath())
        .addOption(START_LOCATOR__PROPERTIES, missingPropertiesPath)
        .addOption("J", "-Dgemfire.enable-management-rest-service=false");


    gfsh.executeAndAssertThat(command.getCommandString()).statusIsError()
        .containsOutput(expectedError);
  }

  @Test
  public void testWithMissingGemFireSecurityPropertiesFile() throws IOException {
    String missingSecurityPropertiesPath = Paths
        .get("missing", "gemfire-security.properties").toAbsolutePath().toString();
    String expectedError = MessageFormat.format(GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE,
        "Security ", missingSecurityPropertiesPath);

    File workingDir = temporaryFolder.newFolder();

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__LOCATORS, locatorConnectionString)
        .addOption(START_LOCATOR__DIR, workingDir.getAbsolutePath())
        .addOption(START_LOCATOR__SECURITY_PROPERTIES, missingSecurityPropertiesPath)
        .addOption("J", "-Dgemfire.enable-management-rest-service=false");

    gfsh.executeAndAssertThat(command.getCommandString()).statusIsError()
        .containsOutput(expectedError);
  }

  @Test
  public void testWithUnavailablePort() throws IOException {
    int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    String unexpectedMessage = "[" + locatorPort + "] as locator is currently online.";
    String expectedMessage = "java.net.BindException: Network is unreachable; port ("
        + locatorPort + ") is not available on localhost.";

    try (Socket interferingProcess = new Socket()) {
      interferingProcess.bind(new InetSocketAddress(locatorPort));

      File workingDir = temporaryFolder.newFolder();

      CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
          .addOption(START_LOCATOR__MEMBER_NAME, memberName)
          .addOption(START_LOCATOR__LOCATORS, locatorConnectionString)
          .addOption(START_LOCATOR__DIR, workingDir.getAbsolutePath())
          .addOption(START_LOCATOR__PORT, Integer.toString(locatorPort))
          .addOption("J", "-Dgemfire.enable-management-rest-service=false");

      gfsh.executeAndAssertThat(command.getCommandString()).statusIsError()
          .containsOutput(expectedMessage).doesNotContainOutput(unexpectedMessage);
    }
  }

  @Test
  public void testWithAvailablePort() throws IOException {
    int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    String expectedMessage =
        "[" + locatorPort + "] as " + memberName + " is currently online.";

    File workingDir = temporaryFolder.newFolder();

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__LOCATORS, locatorConnectionString)
        .addOption(START_LOCATOR__DIR, workingDir.getAbsolutePath())
        .addOption(START_LOCATOR__PORT, String.valueOf(locatorPort))
        .addOption("J", "-Dgemfire.enable-management-rest-service=false");

    gfsh.executeAndAssertThat(command.getCommandString()).statusIsSuccess()
        .containsOutput(expectedMessage);
  }

  @Test
  public void testWithDefaultLocatorPort() throws IOException {
    String unexpectedMessage = "[0] as " + memberName + " is currently online.";
    String expectedMessage = "\\[\\d+\\] as " + memberName + " is currently online.";

    File workingDir = temporaryFolder.newFolder();

    CommandStringBuilder command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__LOCATORS, locatorConnectionString)
        .addOption(START_LOCATOR__DIR, workingDir.getAbsolutePath())
        .addOption(START_LOCATOR__PORT, "0")
        .addOption("J", "-Dgemfire.enable-management-rest-service=false");

    gfsh.executeAndAssertThat(command.getCommandString()).statusIsSuccess()
        .hasOutput()
        .doesNotContain(unexpectedMessage).containsPattern(expectedMessage);
  }

  @Test
  public void startLocatorRespectsHostnameForClients() throws IOException {
    File workingDir = temporaryFolder.newFolder();
    String expectedMessagePattern = "Locator (.*) is currently online";

    String command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__LOCATORS, locatorConnectionString)
        .addOption(START_LOCATOR__DIR, workingDir.getAbsolutePath())
        .addOption(START_LOCATOR__PORT, "10339")
        .addOption(START_LOCATOR__HOSTNAME_FOR_CLIENTS, "fakeLocatorName")
        .addOption("J", "-Dgemfire.enable-management-rest-service=false")
        .addOption(START_LOCATOR__CONNECT, "true").getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .hasOutput().containsPattern(expectedMessagePattern);

    // Verify that the ServerLocation contains the specified hostname-for-clients.
    locator.invoke(() -> {
      InternalLocator internalLocator = InternalLocator.getLocator();
      final ServerLocator serverLocator = internalLocator.getServerLocatorAdvisee();
      LocatorListResponse locatorListResponse =
          (LocatorListResponse) serverLocator.processRequest(new LocatorListRequest());

      List<ServerLocation> locators = locatorListResponse.getLocators();
      assertThat(locators.stream().filter(
          serverLocation -> "fakeLocatorName".equals(serverLocation.getHostName())))
              .isNotEmpty().hasSize(1);
    });
  }
}
