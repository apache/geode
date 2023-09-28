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
package org.apache.geode.internal.cache.wan.wancommand;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMember;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.startSender;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewaySenderMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderState;

import java.io.Serializable;
import java.util.Properties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
@RunWith(JUnitParamsRunner.class)
public class ClusterConfigStartStopPauseAndResumeGatewaySenderCommandDUnitTest
    implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(8);

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locatorSite1;
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;
  private MemberVM server4;
  private MemberVM server5;

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = clusterStartupRule.startLocatorVM(1, props);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorSite1.getPort() + "]");
    clusterStartupRule.startLocatorVM(2, props);

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locatorSite1);
  }

  /**
   * Test to validate that parallel and serial gateway-sender startup action is persisted within
   * cluster configuration after "start, stop and pause gateway-sender" commands are executed.
   */
  @Test
  @Parameters({CliStrings.START_GATEWAYSENDER + ", true",
      CliStrings.START_GATEWAYSENDER + ", false",
      CliStrings.STOP_GATEWAYSENDER + ", true", CliStrings.STOP_GATEWAYSENDER + ", false",
      CliStrings.PAUSE_GATEWAYSENDER + ", true", CliStrings.PAUSE_GATEWAYSENDER + ", false"})
  public void testStartStopPauseGatewaySenderWithClusterConfiguration(String cmd,
      String isParallel) {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);

    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, isParallel)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, "" + getManualStartParameter(cmd))
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> verifySenderState("ln", !getManualStartParameter(cmd), false));
    server2.invoke(() -> verifySenderState("ln", !getManualStartParameter(cmd), false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln",
            !getManualStartParameter(cmd), false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln",
            !getManualStartParameter(cmd), false));

    command = new CommandStringBuilder(cmd)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln",
            shouldBeRunning(cmd), shouldBePaused(cmd)));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln",
            shouldBeRunning(cmd), shouldBePaused(cmd)));

    server1.invoke(() -> verifySenderState("ln", shouldBeRunning(cmd), shouldBePaused(cmd)));
    server2.invoke(() -> verifySenderState("ln", shouldBeRunning(cmd), shouldBePaused(cmd)));

    server1.stop(true);
    server2.stop(true);

    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);

    server1.invoke(() -> verifySenderState("ln", shouldBeRunning(cmd), shouldBePaused(cmd)));
    server2.invoke(() -> verifySenderState("ln", shouldBeRunning(cmd), shouldBePaused(cmd)));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln",
            shouldBeRunning(cmd), shouldBePaused(cmd)));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln",
            shouldBeRunning(cmd), shouldBePaused(cmd)));
  }

  /**
   * Test to validate that gateway-sender startup action is persisted withing cluster configuration
   * after start, stop and pause gateway-sender command is executed for particular group.
   */
  @Test
  @Parameters({CliStrings.PAUSE_GATEWAYSENDER, CliStrings.START_GATEWAYSENDER,
      CliStrings.STOP_GATEWAYSENDER})
  public void testStartStopGatewaySenderMultipleGroupClusterConfiguration(String cmd) {
    int locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server2 = startServerWithGroups(4, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1, SenderGroup2", locator1Port);
    server4 = startServerWithGroups(6, "SenderGroup1, SenderGroup2", locator1Port);
    server5 = startServerWithGroups(7, "SenderGroup3", locator1Port);

    // setup servers in Site #1
    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, "" + getManualStartParameter(cmd))
        .addOption(CliStrings.GROUPS, "SenderGroup1,SenderGroup2,SenderGroup3")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> verifySenderState("ln", !getManualStartParameter(cmd), false));
    server2.invoke(() -> verifySenderState("ln", !getManualStartParameter(cmd), false));
    server3.invoke(() -> verifySenderState("ln", !getManualStartParameter(cmd), false));
    server4.invoke(() -> verifySenderState("ln", !getManualStartParameter(cmd), false));
    server5.invoke(() -> verifySenderState("ln", !getManualStartParameter(cmd), false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln",
            !getManualStartParameter(cmd), false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln",
            !getManualStartParameter(cmd), false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln",
            !getManualStartParameter(cmd), false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server4.getVM()), "ln",
            !getManualStartParameter(cmd), false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln",
            !getManualStartParameter(cmd), false));

    command = new CommandStringBuilder(cmd)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.GROUP, "SenderGroup1,SenderGroup2")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'SenderGroup1' is updated.")
        .containsOutput("Cluster configuration for group 'SenderGroup2' is updated.")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK", "OK");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln",
            shouldBeRunning(cmd), shouldBePaused(cmd)));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln",
            shouldBeRunning(cmd), shouldBePaused(cmd)));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln",
            shouldBeRunning(cmd), shouldBePaused(cmd)));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server4.getVM()), "ln",
            shouldBeRunning(cmd), shouldBePaused(cmd)));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln",
            !getManualStartParameter(cmd), false));

    server1.invoke(() -> verifySenderState("ln", shouldBeRunning(cmd), shouldBePaused(cmd)));
    server2.invoke(() -> verifySenderState("ln", shouldBeRunning(cmd), shouldBePaused(cmd)));
    server3.invoke(() -> verifySenderState("ln", shouldBeRunning(cmd), shouldBePaused(cmd)));
    server4.invoke(() -> verifySenderState("ln", shouldBeRunning(cmd), shouldBePaused(cmd)));
    server5.invoke(() -> verifySenderState("ln", !getManualStartParameter(cmd), false));

    server1.stop(false);
    server3.stop(false);
    server5.stop(false);

    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1, SenderGroup2", locator1Port);
    server5 = startServerWithGroups(7, "SenderGroup3", locator1Port);

    server1.invoke(() -> verifySenderState("ln", shouldBeRunning(cmd), shouldBePaused(cmd)));
    server3.invoke(() -> verifySenderState("ln", shouldBeRunning(cmd), shouldBePaused(cmd)));
    server5.invoke(() -> verifySenderState("ln", !getManualStartParameter(cmd), false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln",
            shouldBeRunning(cmd), shouldBePaused(cmd)));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln",
            shouldBeRunning(cmd), shouldBePaused(cmd)));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln",
            !getManualStartParameter(cmd), false));
  }

  /**
   * Test to validate that gateway-sender startup action is persisted withing cluster configuration
   * after "resume gateway-sender" command is executed.
   * This behavior is tested in a following way:
   *
   * 1. Create gateway-sender on all servers with manual-start set to true (gws will not be started
   * automatically).
   * 2. Start gateway-sender using "start gateway-sender command".
   * 2. Pause gateway-sender using "pause gateway-sender command".
   * 2. Resume gateway-senders using "resume gateway-sender command".
   * 3. Stop and then start again all servers and verify that gateway-sender will reach paused
   * state.
   */
  @Test
  public void testResumeGatewaySenderWithClusterConfigurationService() {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);

    // Create gateway-sender on all members
    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, "true")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    // Start gateway-sender on all members
    command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK");

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));

    // Pause gateway-sender on all members
    command = new CommandStringBuilder(CliStrings.PAUSE_GATEWAYSENDER)
        .addOption(CliStrings.PAUSE_GATEWAYSENDER__ID, "ln")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK");

    server1.invoke(() -> verifySenderState("ln", true, true));
    server2.invoke(() -> verifySenderState("ln", true, true));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, true));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, true));

    // Resume gateway-sender on all members
    command = new CommandStringBuilder(CliStrings.RESUME_GATEWAYSENDER)
        .addOption(CliStrings.RESUME_GATEWAYSENDER__ID, "ln")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK");

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));

    server1.stop(false);
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server1.invoke(() -> verifySenderState("ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
  }

  /**
   * Test to validate that gateway-sender startup action is persisted withing cluster configuration
   * after resume gateway-sender command is executed for particular group.
   * This behavior is tested in a following way:
   *
   * 1. Create gateway-sender on all servers with manual-start set to true (gws will be started
   * automatically).
   * 2. Pause gateway-sender using "pause gateway-sender" command for wanted group of servers.
   * 2. Resume gateway-sender using "resume gateway-sender" command for wanted group of servers.
   * 3. Stop and then start again servers and verify that gateway-sender in wanted
   * group of servers is started.
   */
  @Test
  public void testResumeGatewaySenderMultipleGroupClusterConfiguration() {
    int locator1Port = locatorSite1.getPort();

    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server2 = startServerWithGroups(4, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1, SenderGroup2", locator1Port);
    server4 = startServerWithGroups(6, "SenderGroup2", locator1Port);
    server5 = startServerWithGroups(7, "SenderGroup3", locator1Port);

    // setup servers in Site #1
    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, "false")
        .addOption(CliStrings.GROUPS, "SenderGroup1,SenderGroup2,SenderGroup3")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
    server4.invoke(() -> verifySenderState("ln", true, false));
    server5.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server4.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln", true, false));


    command = new CommandStringBuilder(CliStrings.PAUSE_GATEWAYSENDER)
        .addOption(CliStrings.PAUSE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.GROUP, "SenderGroup1,SenderGroup2")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'SenderGroup1' is updated.")
        .containsOutput("Cluster configuration for group 'SenderGroup2' is updated.")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK", "OK");

    server1.invoke(() -> verifySenderState("ln", true, true));
    server2.invoke(() -> verifySenderState("ln", true, true));
    server3.invoke(() -> verifySenderState("ln", true, true));
    server4.invoke(() -> verifySenderState("ln", true, true));
    server5.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, true));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, true));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, true));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server4.getVM()), "ln", true, true));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln", true, false));


    command = new CommandStringBuilder(CliStrings.RESUME_GATEWAYSENDER)
        .addOption(CliStrings.RESUME_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.GROUP, "SenderGroup1,SenderGroup2")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'SenderGroup1' is updated.")
        .containsOutput("Cluster configuration for group 'SenderGroup2' is updated.")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK", "OK");

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
    server4.invoke(() -> verifySenderState("ln", true, false));
    server5.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server4.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln", true, false));

    server1.stop(false);
    server3.stop(false);
    server5.stop(false);

    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1, SenderGroup2", locator1Port);
    server5 = startServerWithGroups(7, "SenderGroup3", locator1Port);

    server1.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
    server5.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln", true, false));
  }


  /**
   * Test to validate that gateway-sender is persisted in cluster configuration after executing
   * all actions. This behavior is tested in a following way:
   *
   * 1. Create gateway-sender on all servers with manual-start set to false (gws will be started
   * automatically).
   * 2. Pause gateway-sender using "pause gateway-sender" command.
   * 3. Stop and then start again all servers and verify that gateway-sender remain paused.
   * 4. Resume gateway-sender and verify it's state
   * 5. Stop gateway-sender and verify it's state
   * 6. Start gateway-sender and verify it's state
   */
  @Test
  public void testPauseGatewaySenderWithClusterConfiguration() {
    Integer locator1Port = locatorSite1.getPort();
    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "false")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, "false")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> startSender("ln"));
    server2.invoke(() -> startSender("ln"));
    server3.invoke(() -> startSender("ln"));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));

    command = new CommandStringBuilder(CliStrings.PAUSE_GATEWAYSENDER)
        .addOption(CliStrings.PAUSE_GATEWAYSENDER__ID, "ln")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, true));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, true));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, true));

    server1.invoke(() -> verifySenderState("ln", true, true));
    server2.invoke(() -> verifySenderState("ln", true, true));
    server3.invoke(() -> verifySenderState("ln", true, true));

    server1.stop(true);
    server2.stop(true);

    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);

    server1.invoke(() -> verifySenderState("ln", true, true));
    server2.invoke(() -> verifySenderState("ln", true, true));
    server3.invoke(() -> verifySenderState("ln", true, true));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, true));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, true));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, true));

    // Resume gateway-sender on all members
    command = new CommandStringBuilder(CliStrings.RESUME_GATEWAYSENDER)
        .addOption(CliStrings.RESUME_GATEWAYSENDER__ID, "ln")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK");

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));

    // Stop gateway-sender on all members
    command = new CommandStringBuilder(CliStrings.STOP_GATEWAYSENDER)
        .addOption(CliStrings.STOP_GATEWAYSENDER__ID, "ln")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK");

    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));
    server3.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", false, false));

    // Resume gateway-sender on all members
    command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK");

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));
  }

  private static boolean shouldBeRunning(String cmd) {
    switch (cmd) {
      case CliStrings.START_GATEWAYSENDER:
      case CliStrings.PAUSE_GATEWAYSENDER:
      case CliStrings.RESUME_GATEWAYSENDER:
        return true;
      default:
        // CliStrings.STOP_GATEWAYSENDER
        return false;
    }
  }

  private static boolean shouldBePaused(String cmd) {
    switch (cmd) {
      case CliStrings.START_GATEWAYSENDER:
      case CliStrings.STOP_GATEWAYSENDER:
      case CliStrings.RESUME_GATEWAYSENDER:
        return false;
      default:
        // CliStrings.STOP_PAUSE_GATEWAYSENDER
        return true;
    }
  }

  private static boolean getManualStartParameter(String cmd) {
    switch (cmd) {
      case CliStrings.START_GATEWAYSENDER:
        return true;
      default:
        // CliStrings.PAUSE_GATEWAYSENDER
        // CliStrings.STOP_GATEWAYSENDER
        return false;
    }
  }

  private MemberVM startServerWithGroups(int index, String groups, int locPort) {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return clusterStartupRule.startServerVM(index, props, locPort);
  }
}
