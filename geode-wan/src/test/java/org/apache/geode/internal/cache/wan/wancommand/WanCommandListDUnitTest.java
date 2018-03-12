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
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.pause;

import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class, WanTest.class})
public class WanCommandListDUnitTest {

  private static final long serialVersionUID = 1L;

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

    gfsh.connectAndVerify(locatorSite1);

  }

  @Test
  public void testListGatewayWithNoSenderReceiver() throws Exception {
    Integer lnPort = locatorSite1.getPort();

    // setup servers in Site #1 (London)
    server1 = clusterStartupRule.startServerVM(3, lnPort);
    server2 = clusterStartupRule.startServerVM(4, lnPort);
    server3 = clusterStartupRule.startServerVM(5, lnPort);

    pause(10000);
    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = gfsh.executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testListGatewaySender : : " + strCmdResult);
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }

  @Test
  public void testListGatewaySender() throws Exception {
    Integer lnPort = locatorSite1.getPort();
    Integer nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = clusterStartupRule.startServerVM(3, lnPort);
    server2 = clusterStartupRule.startServerVM(4, lnPort);
    server3 = clusterStartupRule.startServerVM(5, lnPort);

    // servers in Site 2 (New York)
    server4 = clusterStartupRule.startServerVM(6, nyPort);
    server5 = clusterStartupRule.startServerVM(7, nyPort);

    // Site 2 Receivers
    server4.invoke(() -> createAndStartReceiver(nyPort));
    server5.invoke(() -> createAndStartReceiver(nyPort));

    // Site 1 Senders
    server1.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, false));
    server1.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, false));

    server2.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, false));
    server2.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, false));

    server3.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, false));

    pause(10000);
    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = gfsh.executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testListGatewaySender" + strCmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_senderIds =
          tableResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertTrue(result_senderIds.contains("ln_Serial"));
      assertTrue(result_senderIds.contains("ln_Parallel"));
      assertEquals(5, result_senderIds.size());

      assertEquals(null, ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER));
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }

  @Test
  public void testListGatewayReceiver() throws Exception {
    Integer lnPort = locatorSite1.getPort();
    Integer nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = clusterStartupRule.startServerVM(3, lnPort);
    server2 = clusterStartupRule.startServerVM(4, lnPort);

    // servers in Site 2 (New York)
    server3 = clusterStartupRule.startServerVM(5, nyPort);
    server4 = clusterStartupRule.startServerVM(6, nyPort);

    server1.invoke(() -> createAndStartReceiver(lnPort));
    server2.invoke(() -> createAndStartReceiver(lnPort));

    server3.invoke(() -> createSender("ln_Serial", 1, false, 100, 400, false, false, null, false));
    server4.invoke(() -> createSender("ln_Serial", 1, false, 100, 400, false, false, null, false));
    server4.invoke(() -> createSender("ln_Parallel", 1, true, 100, 400, false, false, null, false));

    pause(10000);
    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = gfsh.executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testListGatewayReceiver" + strCmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(2, ports.size());
      List<String> hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(2, hosts.size());

      assertEquals(null, ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER));
    } else {
      fail("testListGatewayReceiver failed as did not get CommandResult");
    }
  }

  @Test
  public void testListGatewaySenderGatewayReceiver() throws Exception {
    Integer lnPort = locatorSite1.getPort();
    Integer nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = clusterStartupRule.startServerVM(3, lnPort);
    server2 = clusterStartupRule.startServerVM(4, lnPort);
    server3 = clusterStartupRule.startServerVM(5, lnPort);

    // servers in Site 2 (New York)
    server4 = clusterStartupRule.startServerVM(6, nyPort);
    server5 = clusterStartupRule.startServerVM(7, nyPort);

    server4.invoke(() -> createAndStartReceiver(nyPort));

    server1.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, false));
    server1.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, false));

    server2.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, false));
    server2.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, false));

    server3.invoke(() -> createAndStartReceiver(lnPort));

    server5.invoke(() -> createSender("ln_Serial", 1, false, 100, 400, false, false, null, false));
    server5.invoke(() -> createSender("ln_Parallel", 1, true, 100, 400, false, false, null, false));

    pause(10000);
    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testListGatewaySenderGatewayReceiver : " + strCmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders =
          tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(4, senders.size());
      List<String> hosts = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(4, hosts.size());


      TabularResultData tableReceiverResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(1, ports.size());
      hosts = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(1, hosts.size());
    } else {
      fail("testListGatewaySenderGatewayReceiver failed as did not get CommandResult");
    }
  }

  @Test
  public void testListGatewaySenderGatewayReceiver_group() throws Exception {

    Integer lnPort = locatorSite1.getPort();
    Integer nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = startServerWithGroups(3, "Serial_Sender, Parallel_Sender", lnPort);
    server2 = startServerWithGroups(4, "Serial_Sender, Parallel_Sender", lnPort);
    server3 = startServerWithGroups(5, "Parallel_Sender, Receiver_Group", lnPort);

    // server in Site 2 (New York)
    server4 = clusterStartupRule.startServerVM(6, nyPort);
    server5 = clusterStartupRule.startServerVM(7, nyPort);

    server4.invoke(() -> createAndStartReceiver(nyPort));

    server1.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, false));
    server1.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, false));

    server2.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, false));
    server2.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, false));

    server3.invoke(() -> createAndStartReceiver(lnPort));
    server3.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, false));

    server5.invoke(() -> createSender("ln_Serial", 1, false, 100, 400, false, false, null, false));
    server5.invoke(() -> createSender("ln_Parallel", 1, true, 100, 400, false, false, null, false));

    pause(10000);
    String command = CliStrings.LIST_GATEWAY + " --" + CliStrings.GROUP + "=Serial_Sender";
    CommandResult cmdResult = gfsh.executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testListGatewaySenderGatewayReceiver_group : " + strCmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders =
          tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(4, senders.size());
      List<String> hosts = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(4, hosts.size());

    } else {
      fail("testListGatewaySenderGatewayReceiver_group failed as did not get CommandResult");
    }

    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.GROUP + "=Parallel_Sender";
    cmdResult = gfsh.executeCommand(command);
    if (cmdResult != null) {
      TabularResultData tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders =
          tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(5, senders.size());

      TabularResultData tableReceiverResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(1, ports.size());

      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testListGatewaySenderGatewayReceiver_group : " + strCmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySenderGatewayReceiver_group failed as did not get CommandResult");
    }

    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.GROUP + "=Receiver_Group";
    cmdResult = gfsh.executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testListGatewaySenderGatewayReceiver_group : " + strCmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders =
          tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(1, senders.size());

      TabularResultData tableReceiverResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(1, ports.size());

    } else {
      fail("testListGatewaySenderGatewayReceiver_group failed as did not get CommandResult");
    }

    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.GROUP + "=Serial_Sender,Parallel_Sender";
    cmdResult = gfsh.executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testListGatewaySenderGatewayReceiver_group : " + strCmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders =
          tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(5, senders.size());

      TabularResultData tableReceiverResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(1, ports.size());
    } else {
      fail("testListGatewaySenderGatewayReceiver_group failed as did not get CommandResult");
    }

    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.GROUP
        + "=Serial_Sender,Parallel_Sender,Receiver_Group";
    cmdResult = gfsh.executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testListGatewaySenderGatewayReceiver_group : " + strCmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders =
          tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(5, senders.size());

      TabularResultData tableReceiverResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(1, ports.size());

    } else {
      fail("testListGatewaySenderGatewayReceiver_group failed as did not get CommandResult");
    }
  }

  private MemberVM startServerWithGroups(int index, String groups, int locPort) throws Exception {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return clusterStartupRule.startServerVM(index, props, locPort);
  }
}
