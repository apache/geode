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
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.createSender;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMember;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewaySenderMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderState;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
public class StartGatewaySenderCommandDUnitTest implements Serializable {

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
   * Test wan commands for error in input 1> start gateway-sender command needs only one of member
   * or group.
   */
  @Test
  public void testStartGatewaySender_ErrorConditions() {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    DistributedMember vm1Member = getMember(server1.getVM());
    String command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.MEMBER, vm1Member.getId())
        .addOption(CliStrings.GROUP, "SenderGroup1")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE);
  }

  @Test
  public void testStartGatewaySender() {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));
    server3.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", false, false));

    String command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is not updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
  }

  /**
   * Test to validate that gateway-sender state is persisted withing cluster configuration
   * after "stop gateway-sender" command is executed.
   * This behavior is tested in a following way:
   *
   * 1. Create gateway-sender on all servers with manual-start set to true (gws will not be started
   * automatically).
   * 2. Start gateway-senders using start gateway-sender command.
   * 3. Stop and then start again all servers and verify that gateway-sender is started.
   */
  @Test
  public void testStartGatewaySenderWithClusterConfiguration() {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);

    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "false")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, "true")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", false, false));

    command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));

    server1.stop(true);
    server2.stop(true);

    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
  }

  /**
   * Test validates that cluster configuration is not updated when stop gateway-sender is executed
   * per member.
   */
  @Test
  public void testStartGatewaySenderOnMemberWithClusterConfigurationService() {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "false")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, "true")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));

    DistributedMember vm1Member = getMember(server1.getVM());
    command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.MEMBER, vm1Member.getId())
        .getCommandString();

    CommandResultAssert resultAssert = gfsh.executeAndAssertThat(command).statusIsSuccess();
    resultAssert.containsOutput(
        "Configuration change is not persisted because the command is executed on specific member.");
    resultAssert.hasTableSection().hasColumn("Message").asList().element(0).asString()
        .contains("is started on member");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server1.stop(true);
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server1.invoke(() -> verifySenderState("ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on a member
   */
  @Test
  public void testStartGatewaySender_onMember() {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server1.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));

    DistributedMember vm1Member = getMember(server1.getVM());
    String command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.MEMBER, vm1Member.getId())
        .getCommandString();

    CommandResultAssert resultAssert = gfsh.executeAndAssertThat(command).statusIsSuccess();
    resultAssert.containsOutput(
        "Configuration change is not persisted because the command is executed on specific member.");
    resultAssert.hasTableSection().hasColumn("Message").asList().element(0).asString()
        .contains("is started on member");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));

    server1.invoke(() -> verifySenderState("ln", true, false));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on a group of members
   */
  @Test
  public void testStartGatewaySender_Group() {
    int locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server2 = startServerWithGroups(4, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1", locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));
    server3.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", false, false));

    String command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.GROUP, "SenderGroup1")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'SenderGroup1' is not updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more sender members belongs
   * to multiple groups
   */
  @Test
  public void testStartGatewaySender_MultipleGroup() {
    int locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server2 = startServerWithGroups(4, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1, SenderGroup2", locator1Port);
    server4 = startServerWithGroups(6, "SenderGroup1, SenderGroup2", locator1Port);
    server5 = startServerWithGroups(7, "SenderGroup3", locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server4.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server5.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));
    server3.invoke(() -> verifySenderState("ln", false, false));
    server4.invoke(() -> verifySenderState("ln", false, false));
    server5.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server4.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln", false, false));

    String command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.GROUP, "SenderGroup1,SenderGroup2")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'SenderGroup1' is not updated.")
        .containsOutput("Cluster configuration for group 'SenderGroup2' is not updated.")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK", "OK");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server4.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln", false, false));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
    server4.invoke(() -> verifySenderState("ln", true, false));
    server5.invoke(() -> verifySenderState("ln", false, false));
  }

  /**
   * Test to validate that gateway-sender state is persisted withing cluster configuration
   * after start gateway-sender command is executed for particular group.
   * This behavior is tested in a following way:
   *
   * 1. Create gateway-sender on all servers with manual-start set to true (gws won't be started
   * automatically).
   * 2. Stop gateway-senders using "start gateway-sender" command for wanted group of servers.
   * 3. Stop and then start again servers and verify that gateway-sender in wanted
   * group of servers is started.
   */
  @Test
  public void testStartGatewaySenderMultipleGroupClusterConfiguration() {
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
        .addOption(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, "true")
        .addOption(CliStrings.GROUPS, "SenderGroup1,SenderGroup2,SenderGroup3")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));
    server3.invoke(() -> verifySenderState("ln", false, false));
    server4.invoke(() -> verifySenderState("ln", false, false));
    server5.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server4.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln", false, false));

    command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.GROUP, "SenderGroup1,SenderGroup2")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'SenderGroup1' is updated.")
        .containsOutput("Cluster configuration for group 'SenderGroup2' is updated.")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK", "OK");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server4.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln", false, false));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
    server4.invoke(() -> verifySenderState("ln", true, false));
    server5.invoke(() -> verifySenderState("ln", false, false));

    server1.stop(false);
    server3.stop(false);
    server5.stop(false);

    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1, SenderGroup2", locator1Port);
    server5 = startServerWithGroups(7, "SenderGroup3", locator1Port);

    server1.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
    server5.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln", false, false));

  }

  /**
   * Test to validate the test scenario when one of the member ion group does not have the sender.
   */
  @Test
  public void testStartGatewaySender_Group_MissingSenderFromGroup() {
    int locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server2 = startServerWithGroups(4, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1", locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    server1.invoke(() -> verifySenderState("ln", false, false));
    server3.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", false, false));

    String command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.GROUP, "SenderGroup1")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'SenderGroup1' is not updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("Error", "OK", "OK");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
  }

  @Test
  public void testStartGatewaySender_clean_queues() {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));
    server3.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", false, false));

    String command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.START_GATEWAYSENDER__CLEAN_QUEUE, "true")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is not updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
  }

  @Test
  public void testStartGatewaySender_clean_queues_false() {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));
    server3.invoke(() -> verifySenderState("ln", false, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", false, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", false, false));

    String command = new CommandStringBuilder(CliStrings.START_GATEWAYSENDER)
        .addOption(CliStrings.START_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.START_GATEWAYSENDER__CLEAN_QUEUE, "false")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is not updated")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()), "ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, false));

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));
  }

  private MemberVM startServerWithGroups(int index, String groups, int locPort) {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return clusterStartupRule.startServerVM(index, props, locPort);
  }
}
