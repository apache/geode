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
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMember;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewayReceiverMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewaySenderMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateMemberMXBeanProxy;
import static org.apache.geode.management.MXBeanAwaitility.awaitGatewayReceiverMXBeanProxy;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
@SuppressWarnings("serial")
public class ListGatewaysCommandDUnitTest implements Serializable {

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

    gfsh.connectAndVerify(locatorSite1);
  }

  @Test
  public void testListGatewayWithNoSenderReceiver() {
    Integer lnPort = locatorSite1.getPort();

    // setup servers in Site #1 (London)
    server1 = clusterStartupRule.startServerVM(3, lnPort);
    server2 = clusterStartupRule.startServerVM(4, lnPort);
    server3 = clusterStartupRule.startServerVM(5, lnPort);

    locatorSite1.invoke(() -> validateMemberMXBeanProxy(getMember(server1.getVM())));
    locatorSite1.invoke(() -> validateMemberMXBeanProxy(getMember(server2.getVM())));
    locatorSite1.invoke(() -> validateMemberMXBeanProxy(getMember(server3.getVM())));

    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.ERROR);
  }

  @Test
  public void testListGatewaySender() {
    int lnPort = locatorSite1.getPort();
    int nyPort = locatorSite2.getPort();

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

    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()),
        "ln_Serial", true, false));
    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()),
        "ln_Parallel", true, false));

    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()),
        "ln_Serial", true, false));
    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()),
        "ln_Parallel", true, false));

    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()),
        "ln_Serial", true, false));

    locatorSite2.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server4.getVM()), true));
    locatorSite2.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server5.getVM()), true));

    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
    List<String> result_senderIds =
        tableResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
    assertThat(result_senderIds).contains("ln_Serial");
    assertThat(result_senderIds).contains("ln_Parallel");
    assertThat(result_senderIds).hasSize(5);

    assertThat(((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)).isNull();
  }

  @Test
  public void testListGatewayReceiver() {
    int lnPort = locatorSite1.getPort();
    int nyPort = locatorSite2.getPort();

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

    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server1.getVM()), true));
    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server2.getVM()), true));

    locatorSite2.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server3.getVM()),
        "ln_Serial", true, false));
    locatorSite2.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server4.getVM()),
        "ln_Serial", true, false));
    locatorSite2.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server4.getVM()),
        "ln_Parallel", true, false));

    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);

    List<String> ports = tableResultData.retrieveAllValues(CliStrings.RESULT_PORT);
    assertThat(ports).hasSize(2);

    List<String> hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
    assertThat(hosts).hasSize(2);

    assertThat(((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)).isNull();
  }

  @Test
  public void testListGatewaySenderGatewayReceiver() {
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

    locatorSite2.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server4.getVM()), true));

    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()),
        "ln_Serial", true, false));
    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()),
        "ln_Parallel", true, false));

    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()),
        "ln_Serial", true, false));
    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()),
        "ln_Parallel", true, false));

    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server3.getVM()), true));

    locatorSite2.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()),
        "ln_Serial", true, false));
    locatorSite2.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()),
        "ln_Parallel", true, false));

    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    TabularResultData tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
    List<String> senders =
        tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
    assertThat(senders).hasSize(4);

    List<String> hosts = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
    assertThat(hosts).hasSize(4);

    TabularResultData tableReceiverResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
    List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
    assertThat(ports).hasSize(1);

    hosts = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
    assertThat(hosts).hasSize(1);
  }

  @Test
  public void testListGatewaySenderGatewayReceiver_group() {
    int lnPort = locatorSite1.getPort();
    int nyPort = locatorSite2.getPort();

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

    locatorSite2.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server4.getVM()), true));

    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()),
        "ln_Serial", true, false));
    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server1.getVM()),
        "ln_Parallel", true, false));

    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()),
        "ln_Parallel", true, false));
    locatorSite1.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()),
        "ln_Serial", true, false));

    locatorSite1.invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server3.getVM()), true));

    locatorSite2.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()),
        "ln_Serial", true, false));
    locatorSite2.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server5.getVM()),
        "ln_Parallel", true, false));

    String command = CliStrings.LIST_GATEWAY + " --" + CliStrings.GROUP + "=Serial_Sender";
    CommandResult cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    TabularResultData tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
    List<String> senders =
        tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
    assertThat(senders).hasSize(4);
    List<String> hosts = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
    assertThat(hosts).hasSize(4);

    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.GROUP + "=Parallel_Sender";
    cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();

    tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
    senders = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
    assertThat(senders).hasSize(5);

    TabularResultData tableReceiverResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
    List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
    assertThat(ports).hasSize(1);
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.GROUP + "=Receiver_Group";
    cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
    senders = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
    assertThat(senders).hasSize(1);

    tableReceiverResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
    ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
    assertThat(ports).hasSize(1);

    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.GROUP + "=Serial_Sender,Parallel_Sender";
    cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
    senders = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
    assertThat(senders).hasSize(5);

    tableReceiverResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
    ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
    assertThat(ports).hasSize(1);

    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.GROUP
        + "=Serial_Sender,Parallel_Sender,Receiver_Group";
    cmdResult = gfsh.executeCommand(command);
    assertThat(cmdResult).isNotNull();
    assertThat(cmdResult.getStatus()).isSameAs(Result.Status.OK);

    tableSenderResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
    senders = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
    assertThat(senders).hasSize(5);

    tableReceiverResultData = ((CompositeResultData) cmdResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
    ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
    assertThat(ports).hasSize(1);
  }

  private MemberVM startServerWithGroups(int index, String groups, int locPort) {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return clusterStartupRule.startServerVM(index, props, locPort);
  }

  @Test
  public void listGatewaysShouldCorrectlyUpdateSendersConnectedCountWhenReceiverStops()
      throws Exception {
    int site1Port = locatorSite1.getPort();
    int site2Port = locatorSite2.getPort();

    // Setup servers in Site #1
    server1 = clusterStartupRule.startServerVM(3, site1Port);
    server1.invoke(() -> createAndStartReceiver(site1Port));

    // Setup servers in Site #2
    server2 = clusterStartupRule.startServerVM(4, site2Port);
    server2.invoke(() -> createSender("ln_Serial", 1, false, 100, 400, false, false, null, false));

    // Check Gateways
    locatorSite2.invoke(() -> validateGatewaySenderMXBeanProxy(getMember(server2.getVM()),
        "ln_Serial", true, false));
    locatorSite1.invoke(() -> {
      GatewayReceiverMXBean gatewayReceiverMXBean =
          awaitGatewayReceiverMXBeanProxy(getMember(server1.getVM()));
      assertThat(gatewayReceiverMXBean).isNotNull();
      GeodeAwaitility.await("Awaiting GatewayReceiverMXBean.isRunning(true)")
          .untilAsserted(() -> assertThat(gatewayReceiverMXBean.isRunning()).isTrue());
      GeodeAwaitility.await("Awaiting GatewayReceiverMXBean.getClientConnectionCount() > 0")
          .untilAsserted(
              () -> assertThat(gatewayReceiverMXBean.getClientConnectionCount()).isPositive());
    });

    // Verify Results
    gfsh.connect(locatorSite1);
    CommandResult listGatewaysCommandResult = gfsh.executeCommand(CliStrings.LIST_GATEWAY);
    assertThat(listGatewaysCommandResult).isNotNull();
    assertThat(listGatewaysCommandResult.getStatus()).isSameAs(Result.Status.OK);
    TabularResultData gatewayReceiversResultData =
        ((CompositeResultData) listGatewaysCommandResult.getResultData())
            .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
            .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
    assertThat(gatewayReceiversResultData.retrieveAllValues(CliStrings.RESULT_PORT)).hasSize(1);
    assertThat(gatewayReceiversResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER))
        .hasSize(1);
    List<String> sendersCount =
        gatewayReceiversResultData.retrieveAllValues(CliStrings.RESULT_SENDERS_COUNT);
    assertThat(sendersCount).hasSize(1).doesNotContain("0");
    assertThat(((CompositeResultData) listGatewaysCommandResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)).isNull();

    // Stop receivers in Site #1 and Verify Sender Count
    server1.invoke(WANCommandUtils::stopReceivers);

    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server1.getVM()), false));
    gfsh.connect(locatorSite1);
    listGatewaysCommandResult = gfsh.executeCommand(CliStrings.LIST_GATEWAY);
    assertThat(listGatewaysCommandResult).isNotNull();
    assertThat(listGatewaysCommandResult.getStatus()).isSameAs(Result.Status.OK);
    gatewayReceiversResultData = ((CompositeResultData) listGatewaysCommandResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER)
        .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
    assertThat(gatewayReceiversResultData.retrieveAllValues(CliStrings.RESULT_PORT)).hasSize(1);
    assertThat(gatewayReceiversResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER))
        .hasSize(1);
    sendersCount = gatewayReceiversResultData.retrieveAllValues(CliStrings.RESULT_SENDERS_COUNT);
    assertThat(sendersCount).hasSize(1).containsExactly("0");
    assertThat(((CompositeResultData) listGatewaysCommandResult.getResultData())
        .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER)).isNull();
  }
}
