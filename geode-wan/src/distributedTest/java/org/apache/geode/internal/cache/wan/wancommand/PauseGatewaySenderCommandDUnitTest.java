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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
@SuppressWarnings("serial")
public class PauseGatewaySenderCommandDUnitTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(8);

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locatorSite1;
  private MemberVM locatorSite2;
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
    locatorSite2 = clusterStartupRule.startLocatorVM(2, props);

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locatorSite1);
  }

  @Test
  public void testPauseGatewaySender_ErrorConditions() throws Exception {
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
  public void testPauseGatewaySender_onMember() throws Exception {
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
    String command = CliStrings.PAUSE_GATEWAYSENDER + " --" + CliStrings.PAUSE_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.MEMBER + "=" + vm1Member.getId();
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();

    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);
    TabularResultModel resultData = ((ResultModel) cmdResult.getResultData())
        .getTableSection(CliStrings.PAUSE_GATEWAYSENDER);
    List<String> messages = resultData.getValuesInColumn("Message");
    assertThat(messages.get(0)).contains("is paused on member");

    locatorSite1.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()), "ln", true, true));

    server1.invoke(() -> verifySenderState("ln", true, true));
  }

  @Test
  public void testPauseGatewaySender() throws Exception {
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

    String command =
        CliStrings.PAUSE_GATEWAYSENDER + " --" + CliStrings.PAUSE_GATEWAYSENDER__ID + "=ln";
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    TabularResultModel resultData = ((ResultModel) cmdResult.getResultData())
        .getTableSection(CliStrings.PAUSE_GATEWAYSENDER);
    List<String> status = resultData.getValuesInColumn("Result");
    assertThat(status).containsExactlyInAnyOrder("OK", "OK", "OK");

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
   * test to validate that the start gateway sender starts the gateway sender on a group of members
   */
  @Test
  public void testPauseGatewaySender_Group() throws Exception {
    Integer locator1Port = locatorSite1.getPort();

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

    String command = CliStrings.PAUSE_GATEWAYSENDER + " --" + CliStrings.PAUSE_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.GROUP + "=SenderGroup1";
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    TabularResultModel resultData = ((ResultModel) cmdResult.getResultData())
        .getTableSection(CliStrings.PAUSE_GATEWAYSENDER);
    List<String> status = resultData.getValuesInColumn("Result");
    assertThat(status).containsExactlyInAnyOrder("OK", "OK", "OK");

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
  public void testPauseGatewaySender_MultipleGroup() throws Exception {
    Integer locator1Port = locatorSite1.getPort();

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

    String command = CliStrings.PAUSE_GATEWAYSENDER + " --" + CliStrings.PAUSE_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.GROUP + "=SenderGroup1,SenderGroup2";
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    TabularResultModel resultData = ((ResultModel) cmdResult.getResultData())
        .getTableSection(CliStrings.PAUSE_GATEWAYSENDER);
    List<String> status = resultData.getValuesInColumn("Result");
    assertThat(status).containsExactlyInAnyOrder("OK", "OK", "OK", "OK");

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

  private MemberVM startServerWithGroups(int index, String groups, int locPort) throws Exception {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return clusterStartupRule.startServerVM(index, props, locPort);
  }
}
