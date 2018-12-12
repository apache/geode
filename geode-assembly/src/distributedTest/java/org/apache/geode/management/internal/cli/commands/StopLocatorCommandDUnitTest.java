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

import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.management.cli.Result.Status.ERROR;
import static org.apache.geode.management.cli.Result.Status.OK;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.GROUP;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR__DIR;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR__J;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR__LOCATORS;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR__MEMBER_NAME;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_LOCATOR__PORT;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.STOP_LOCATOR;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.STOP_LOCATOR__DIR;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.STOP_LOCATOR__MEMBER;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class StopLocatorCommandDUnitTest {
  private static MemberVM locator;
  private static final String memberName = "locatorToStop";
  private static final String groupName = "locatorGroup";
  private static String locatorConnectionString;
  private File workingDir;
  private static Integer locatorPort, jmxPort, toStopJmxPort;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static final GfshCommandRule gfsh = new GfshCommandRule();

  @ClassRule
  public static final ClusterStartupRule cluster = new ClusterStartupRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();

    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, jmxPort.toString());
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");

    locator = cluster.startLocatorVM(0, props);

    locatorConnectionString = "localhost[" + locator.getPort() + "]";

    gfsh.connectAndVerify(locator);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("shutdown --include-locators").statusIsSuccess();
  }

  @Before
  public void before() throws Exception {
    workingDir = temporaryFolder.newFolder();
    int[] availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    locatorPort = availablePorts[0];
    toStopJmxPort = availablePorts[1];

    final String command = new CommandStringBuilder(START_LOCATOR)
        .addOption(START_LOCATOR__MEMBER_NAME, memberName)
        .addOption(START_LOCATOR__LOCATORS, locatorConnectionString)
        .addOption(START_LOCATOR__PORT, locatorPort.toString())
        .addOption(GROUP, groupName)
        .addOption(START_LOCATOR__J,
            "-D" + GEMFIRE_PREFIX + "jmx-manager-port=" + toStopJmxPort.toString())
        .addOption(START_LOCATOR__J,
            "-D" + GEMFIRE_PREFIX + "jmx-manager-hostname-for-clients=localhost")
        .addOption(START_LOCATOR__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    gfsh.connectAndVerify(locator);
    gfsh.executeCommand(command);
  }

  @After
  public void after() throws IOException {
    gfsh.executeCommand("stop locator --dir=" + workingDir.getCanonicalPath());
  }

  @Test
  public void testWithDisconnectedGfsh() throws Exception {
    final String expectedError = CliStrings.format(STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE,
        CliStrings.LOCATOR_TERM_NAME);
    final String command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__MEMBER, memberName)
        .getCommandString();

    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }

    CommandResult result = gfsh.executeCommand(command);

    gfsh.connectAndVerify(locator);

    assertThat(result.getStatus()).isEqualTo(ERROR);
    assertThat(result.getMessageFromContent()).contains(expectedError);
  }

  @Test
  public void testWithMemberName() {
    final String command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__MEMBER, memberName)
        .getCommandString();


    // The new locator is not immediately available to be stopped because its mbean
    // has to be propagated to the existing locator that gfsh is connected to. Wait
    // for the stop to work
    waitForCommandToSucceed(command);
    gfsh.executeAndAssertThat("list members").doesNotContainOutput(memberName);
  }

  @Test
  public void testWithMemberID() {
    int port = jmxPort; // this assignment is needed to pass a local var into the invocation below

    final String memberID = locator.invoke(() -> getMemberId(port));

    final String command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__MEMBER, memberID)
        .getCommandString();

    // The new locator is not immediately available to be stopped because its mbean
    // has to be propagated to the existing locator that gfsh is connected to. Wait
    // for the stop to work
    waitForCommandToSucceed(command);

    gfsh.executeAndAssertThat("list members").doesNotContainOutput(memberName);
  }

  private void waitForCommandToSucceed(String command) {
    GeodeAwaitility.await().untilAsserted(() -> {
      CommandResult result = gfsh.executeCommand(command);
      assertThat(result.getStatus()).isEqualTo(OK);
    });
  }

  @Test
  public void testWithDirOnline() throws IOException {
    final String command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(OK);
    gfsh.executeAndAssertThat("list members").doesNotContainOutput(memberName);
  }

  @Test
  public void testWithDirOffline() throws Exception {
    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }

    final String command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__DIR, workingDir.getCanonicalPath())
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    gfsh.connect(locator);

    assertThat(result.getStatus()).isEqualTo(OK);
    gfsh.executeAndAssertThat("list members").doesNotContainOutput(memberName);
  }

  @Test
  public void testWithInvalidMemberName() {
    final String command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__MEMBER, "invalidMemberName")
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(ERROR);
    gfsh.executeAndAssertThat("list members").containsOutput(memberName);
  }

  @Test
  public void testWithInvalidMemberID() {
    final String command = new CommandStringBuilder(STOP_LOCATOR)
        .addOption(STOP_LOCATOR__MEMBER, "42")
        .getCommandString();

    CommandResult result = gfsh.executeCommand(command);

    assertThat(result.getStatus()).isEqualTo(ERROR);
    gfsh.executeAndAssertThat("list members").containsOutput(memberName);
  }

  protected static String getMemberId(int port) throws Exception {
    JMXConnector conn = null;

    try {
      final ObjectName objectNamePattern = ObjectName.getInstance("GemFire:type=Member,*");
      final QueryExp query = Query.eq(Query.attr("Name"), Query.value(memberName));
      final JMXServiceURL url =
          new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:" + port + "/jmxrmi");
      System.out.println(url.toString());

      assertThat(url).isNotNull();
      conn = JMXConnectorFactory.connect(url);
      assertThat(conn).isInstanceOf(JMXConnector.class);

      final MBeanServerConnection connection = conn.getMBeanServerConnection();
      assertThat(connection).isInstanceOf(MBeanServerConnection.class);

      GeodeAwaitility.await().untilAsserted(() -> {
        final Set<ObjectName> objectNames = connection.queryNames(objectNamePattern, query);
        assertThat(objectNames).isNotNull().isNotEmpty().hasSize(1);
      });

      final Set<ObjectName> objectNames = connection.queryNames(objectNamePattern, query);
      final ObjectName objectName = objectNames.iterator().next();
      final Object memberId = connection.getAttribute(objectName, "Id");

      return ObjectUtils.toString(memberId);
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }
}
