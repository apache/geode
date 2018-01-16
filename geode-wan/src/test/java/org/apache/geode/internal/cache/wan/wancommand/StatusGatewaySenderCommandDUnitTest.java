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
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.createAndStartReceiver;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.createSender;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMemberIdCallable;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.startSender;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.pause;

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
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class StatusGatewaySenderCommandDUnitTest {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

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
  public void testGatewaySenderStatus() throws Exception {

    Integer lnPort = locatorSite1.getPort();
    Integer nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = clusterStartupRule.startServerVM(3, lnPort);
    server2 = clusterStartupRule.startServerVM(4, lnPort);
    server3 = clusterStartupRule.startServerVM(5, lnPort);

    // server in Site 2 (New York)
    server4 = clusterStartupRule.startServerVM(6, nyPort);

    server4.invoke(() -> createAndStartReceiver(nyPort));

    server1.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    server1.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));

    server2.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));

    server3.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));

    pause(10000);
    String command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial";
    CommandResult cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testGatewaySenderStatus : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }

    server1.invoke(() -> startSender("ln_Serial"));
    server1.invoke(() -> startSender("ln_Parallel"));

    server2.invoke(() -> startSender("ln_Serial"));
    server2.invoke(() -> startSender("ln_Parallel"));

    server3.invoke(() -> startSender("ln_Serial"));
    server3.invoke(() -> startSender("ln_Parallel"));

    pause(10000);
    command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial";
    cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      /*
       * tableResultData = ((CompositeResultData) cmdResult.getResultData())
       * .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE)
       * .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER); List<String> result_hosts =
       * tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER); assertEquals(2,
       * result_hosts.size()); String strCmdResult = cmdResult.toString();
       * getLogWriter().info("testGatewaySenderStatus : " + strCmdResult + ">>>>> ");
       * assertEquals(Result.Status.OK, cmdResult.getStatus());
       */
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }

  @Test
  public void testGatewaySenderStatus_OnMember() throws Exception {

    Integer lnPort = locatorSite1.getPort();
    Integer nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = clusterStartupRule.startServerVM(3, lnPort);
    server2 = clusterStartupRule.startServerVM(4, lnPort);
    server3 = clusterStartupRule.startServerVM(5, lnPort);

    // server in Site 2 (New York)
    server4 = clusterStartupRule.startServerVM(6, nyPort);

    server4.invoke(() -> createAndStartReceiver(nyPort));

    server1.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    server1.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));

    server2.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));

    server3.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));

    final DistributedMember server1DM = (DistributedMember) server1.invoke(getMemberIdCallable());
    pause(10000);
    String command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial --" + CliStrings.MEMBER + "=" + server1DM.getId();
    CommandResult cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testGatewaySenderStatus_OnMember : " + strCmdResult + ">>>>> ");
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(1, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }

    server1.invoke(() -> startSender("ln_Serial"));
    server1.invoke(() -> startSender("ln_Parallel"));

    server2.invoke(() -> startSender("ln_Serial"));
    server2.invoke(() -> startSender("ln_Parallel"));

    pause(10000);
    command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial --" + CliStrings.MEMBER + "=" + server1DM.getId();
    cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testGatewaySenderStatus_OnMember : " + strCmdResult + ">>>>> ");
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(1, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }

    final DistributedMember server3DM = (DistributedMember) server3.invoke(getMemberIdCallable());
    command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial --" + CliStrings.MEMBER + "=" + server3DM.getId();
    cmdResult = gfsh.executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testGatewaySenderStatus_OnMember : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }

  @Test
  public void testGatewaySenderStatus_OnGroups() throws Exception {

    Integer lnPort = locatorSite1.getPort();
    Integer nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = startServerWithGroups(3, "Serial_Sender, Parallel_Sender", lnPort);
    server2 = startServerWithGroups(4, "Serial_Sender, Parallel_Sender", lnPort);
    server3 = startServerWithGroups(5, "Parallel_Sender", lnPort);
    server4 = startServerWithGroups(6, "Serial_Sender", lnPort);

    // server in Site 2 (New York)
    server5 = clusterStartupRule.startServerVM(7, nyPort);

    server5.invoke(() -> createAndStartReceiver(nyPort));

    server1.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    server1.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));

    server2.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    server2.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));

    server3.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    server3.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));

    pause(10000);
    String command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial --" + CliStrings.GROUP + "=Serial_Sender";
    CommandResult cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(2, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(1, result_hosts.size());
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testGatewaySenderStatus_OnGroups : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }

    server1.invoke(() -> startSender("ln_Serial"));
    server1.invoke(() -> startSender("ln_Parallel"));

    server2.invoke(() -> startSender("ln_Serial"));
    server2.invoke(() -> startSender("ln_Parallel"));

    pause(10000);
    command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial --" + CliStrings.GROUP + "=Serial_Sender";
    cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(2, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(1, result_hosts.size());
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testGatewaySenderStatus_OnGroups : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }

  private MemberVM startServerWithGroups(int index, String groups, int locPort) throws Exception {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return clusterStartupRule.startServerVM(index, props, locPort);
  }
}
