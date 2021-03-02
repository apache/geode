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
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.startSender;
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
public class PauseGatewaySenderCommandDUnitTest implements Serializable {

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

  @Test
  public void testPauseGatewaySender_ErrorConditions() {
    server1 = clusterStartupRule.startServerVM(3, locatorSite1.getPort());
    DistributedMember vm1Member = getMember(server1.getVM());
    String command = CliStrings.PAUSE_GATEWAYSENDER + " --" + CliStrings.PAUSE_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.MEMBER + "=" + vm1Member.getId() + " --" + CliStrings.GROUP
        + "=SenderGroup1";
    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE);
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on a member
   */
  @Test
  public void testPauseGatewaySender_onMember() {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server1.invoke(() -> startSender("ln"));
    server1.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));

    DistributedMember vm1Member = getMember(server1.getVM());
    String command = new CommandStringBuilder(CliStrings.PAUSE_GATEWAYSENDER)
        .addOption(CliStrings.PAUSE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.MEMBER, vm1Member.getId())
        .getCommandString();

    CommandResultAssert resultAssert = gfsh.executeAndAssertThat(command).statusIsSuccess();
    resultAssert.containsOutput(
        "Configuration change is not persisted because the command is executed on specific member.");
    resultAssert.hasTableSection().hasColumn("Message").asList().element(0).asString()
        .contains("is paused on member");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, true));

    server1.invoke(() -> verifySenderState("ln", true, true));
  }

  /**
   * Test validates that cluster configuration is not updated when pause gateway-sender is executed
   * per member.
   */
  @Test
  public void testPauseGatewaySenderOnMemberWithClusterConfiguration() {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "false")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__MANUALSTART, "false")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));

    DistributedMember vm1Member = getMember(server1.getVM());
    command = new CommandStringBuilder(CliStrings.PAUSE_GATEWAYSENDER)
        .addOption(CliStrings.PAUSE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.MEMBER, vm1Member.getId())
        .getCommandString();

    CommandResultAssert resultAssert = gfsh.executeAndAssertThat(command).statusIsSuccess();
    resultAssert.containsOutput(
        "Configuration change is not persisted because the command is executed on specific member.");
    resultAssert.hasTableSection().hasColumn("Message").asList().element(0).asString()
        .contains("is paused on member");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, true));
    server1.invoke(() -> verifySenderState("ln", true, true));

    server1.stop(true);
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server1.invoke(() -> verifySenderState("ln", true, false));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, false));
  }

  @Test
  public void testPauseGatewaySender() {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

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

    String command = new CommandStringBuilder(CliStrings.PAUSE_GATEWAYSENDER)
        .addOption(CliStrings.PAUSE_GATEWAYSENDER__ID, "ln")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'cluster' is not updated")
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
  }

  /**
   * Test to validate that gateway-sender state is persisted withing cluster configuration
   * after "pause gateway-sender" command is executed. Additionally, check that resume,
   * stop and start gateway-sender commands work afterwards as expected.
   * This behavior is tested in a following way:
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

    // Stop gateway-sender on all members
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

  /**
   * test to validate that the start gateway sender starts the gateway sender on a group of members
   */
  @Test
  public void testPauseGatewaySender_Group() {
    int locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    String groups = "SenderGroup1";
    server1 = startServerWithGroups(3, groups, locator1Port);
    server2 = startServerWithGroups(4, groups, locator1Port);
    server3 = startServerWithGroups(5, groups, locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

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

    String command = new CommandStringBuilder(CliStrings.PAUSE_GATEWAYSENDER)
        .addOption(CliStrings.PAUSE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.GROUP, "SenderGroup1")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'SenderGroup1' is not updated")
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
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more sender members belongs
   * to multiple groups
   */
  @Test
  public void testPauseGatewaySender_MultipleGroup() {
    int locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server2 = startServerWithGroups(4, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1", locator1Port);
    server4 = startServerWithGroups(6, "SenderGroup2", locator1Port);
    server5 = startServerWithGroups(7, "SenderGroup3", locator1Port);

    server1.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server4.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    server5.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    server1.invoke(() -> startSender("ln"));
    server2.invoke(() -> startSender("ln"));
    server3.invoke(() -> startSender("ln"));
    server4.invoke(() -> startSender("ln"));
    server5.invoke(() -> startSender("ln"));

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

    String command = new CommandStringBuilder(CliStrings.PAUSE_GATEWAYSENDER)
        .addOption(CliStrings.PAUSE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.GROUP, "SenderGroup1,SenderGroup2")
        .getCommandString();

    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .containsOutput("Cluster configuration for group 'SenderGroup1' is not updated.")
        .containsOutput("Cluster configuration for group 'SenderGroup2' is not updated.")
        .hasTableSection().hasColumn("Result")
        .containsExactlyInAnyOrder("OK", "OK", "OK", "OK");

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

    server1.invoke(() -> verifySenderState("ln", true, true));
    server2.invoke(() -> verifySenderState("ln", true, true));
    server3.invoke(() -> verifySenderState("ln", true, true));
    server4.invoke(() -> verifySenderState("ln", true, true));
    server5.invoke(() -> verifySenderState("ln", true, false));
  }

  /**
   * Test to validate that gateway-sender state is persisted withing cluster configuration
   * after pause gateway-sender command is executed for particular group.
   * This behavior is tested in a following way:
   *
   * 1. Create gateway-sender on all servers with manual-start set to true (gws won't be started
   * automatically).
   * 2. Stop gateway-senders using "pause gateway-sender" command for wanted group of servers.
   * 3. Stop and then start again servers and verify that gateway-sender in wanted
   * group of servers remain paused.
   */
  @Test
  public void testPauseGatewaySenderMultipleGroupClusterConfiguration() {
    int locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server2 = startServerWithGroups(4, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1", locator1Port);
    server4 = startServerWithGroups(6, "SenderGroup2", locator1Port);
    server5 = startServerWithGroups(7, "SenderGroup3", locator1Port);

    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "2")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "false")
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

    server1.invoke(() -> verifySenderState("ln", true, true));
    server2.invoke(() -> verifySenderState("ln", true, true));
    server3.invoke(() -> verifySenderState("ln", true, true));
    server4.invoke(() -> verifySenderState("ln", true, true));
    server5.invoke(() -> verifySenderState("ln", true, false));

    server1.stop(false);
    server3.stop(false);
    server5.stop(false);

    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1, SenderGroup2", locator1Port);
    server5 = startServerWithGroups(7, "SenderGroup3", locator1Port);

    server1.invoke(() -> verifySenderState("ln", true, true));
    server3.invoke(() -> verifySenderState("ln", true, true));
    server5.invoke(() -> verifySenderState("ln", true, false));

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, true));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()), "ln", true, true));
    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()), "ln", true, false));

  }

  private MemberVM startServerWithGroups(int index, String groups, int locPort) {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return clusterStartupRule.startServerVM(index, props, locPort);
  }
}
