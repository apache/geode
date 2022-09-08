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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.management.MXBeanAwaitility.awaitGatewaySenderMXBeanProxy;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
public class WanConnectionsLoadBalanceDistributedTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(9);

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locator1Site2;
  private MemberVM locator1Site1;
  private MemberVM locator2Site1;

  private MemberVM server1Site1;
  private MemberVM server2Site1;
  private MemberVM server3Site1;

  private MemberVM server1Site2;
  private MemberVM server2Site2;

  private ClientVM clientSite2;

  private static final String DISTRIBUTED_SYSTEM_ID_SITE1 = "1";
  private static final String DISTRIBUTED_SYSTEM_ID_SITE2 = "2";
  private static final String REGION_NAME = "test1";
  private static final int NUMBER_OF_DISPATCHER_THREADS = 21;

  @Before
  public void setupMultiSite() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, DISTRIBUTED_SYSTEM_ID_SITE1);
    locator1Site1 = clusterStartupRule.startLocatorVM(0, props);
    locator2Site1 = clusterStartupRule.startLocatorVM(1, props, locator1Site1.getPort());

    // start servers for site #1
    server1Site1 =
        clusterStartupRule.startServerVM(2, locator1Site1.getPort(), locator2Site1.getPort());
    server2Site1 =
        clusterStartupRule.startServerVM(3, locator1Site1.getPort(), locator2Site1.getPort());
    server3Site1 =
        clusterStartupRule.startServerVM(4, locator1Site1.getPort(), locator2Site1.getPort());

    connectGfshToSite(locator1Site1);

    // create partition region on site #1
    CommandStringBuilder regionCmd = new CommandStringBuilder(CliStrings.CREATE_REGION);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGION, REGION_NAME);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION");
    regionCmd.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, "1");

    gfsh.executeAndAssertThat(regionCmd.toString()).statusIsSuccess();

    String csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYRECEIVER)
        .addOption(CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS, "localhost")
        .getCommandString();

    gfsh.executeAndAssertThat(csb).statusIsSuccess();

    server1Site1.invoke(() -> verifyReceiverState(true));
    server2Site1.invoke(() -> verifyReceiverState(true));
    server3Site1.invoke(() -> verifyReceiverState(true));

    props.setProperty(DISTRIBUTED_SYSTEM_ID, DISTRIBUTED_SYSTEM_ID_SITE2);
    props.setProperty(REMOTE_LOCATORS,
        "localhost[" + locator1Site1.getPort() + "],localhost[" + locator2Site1.getPort() + "]");
    locator1Site2 = clusterStartupRule.startLocatorVM(5, props);

    // start servers for site #2
    server1Site2 = clusterStartupRule.startServerVM(6, locator1Site2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(7, locator1Site2.getPort());

    // create parallel gateway-sender on site #2
    connectGfshToSite(locator1Site2);
    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "1")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS,
            "" + NUMBER_OF_DISPATCHER_THREADS)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY, "key")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    verifyGatewaySenderState(server1Site2, true);
    verifyGatewaySenderState(server2Site2, true);

    // create partition region on site #2
    regionCmd = new CommandStringBuilder(CliStrings.CREATE_REGION);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGION, REGION_NAME);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION");
    regionCmd.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, "ln");
    regionCmd.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, "1");
    gfsh.executeAndAssertThat(regionCmd.toString()).statusIsSuccess();
  }

  @Test
  public void testGatewayConnectionLoadUpdatedOnBothLocators() throws Exception {
    // Do put operations to initialize gateway-sender connections
    startClientSite2(locator1Site2.getPort());
    doPutsClientSite2(0, 500);

    checkGatewayReceiverLoadUpdatedOnLocators(BigDecimal.valueOf(0.06));
  }

  @Test
  public void testGatewayConnectionLoadUpdatedLocatorsStopAndStartGatewaySenders()
      throws Exception {
    // Do put operations to initialize gateway-sender connections
    startClientSite2(locator1Site2.getPort());
    doPutsClientSite2(0, 400);
    checkGatewayReceiverLoadUpdatedOnLocators(BigDecimal.valueOf(0.06));

    executeGatewaySenderActionCommandAndValidateStateSite2(CliStrings.STOP_GATEWAYSENDER, null);
    checkGatewayReceiverLoadUpdatedOnLocators(BigDecimal.valueOf(0));

    executeGatewaySenderActionCommandAndValidateStateSite2(CliStrings.START_GATEWAYSENDER, null);

    doPutsClientSite2(400, 800);
    checkGatewayReceiverLoadUpdatedOnLocators(BigDecimal.valueOf(0.06));
  }

  @Test
  public void testGatewayConnectionLoadUpdatedLocatorsStopAndStartOneGatewaySender()
      throws Exception {
    // Do put operations to initialize gateway-sender connections
    startClientSite2(locator1Site2.getPort());
    doPutsClientSite2(0, 400);
    checkGatewayReceiverLoadUpdatedOnLocators(BigDecimal.valueOf(0.06));

    executeGatewaySenderActionCommandAndValidateStateSite2(CliStrings.STOP_GATEWAYSENDER,
        server1Site2);
    checkGatewayReceiverLoadUpdatedOnLocators(BigDecimal.valueOf(0.03));

    executeGatewaySenderActionCommandAndValidateStateSite2(CliStrings.START_GATEWAYSENDER,
        server1Site2);

    doPutsClientSite2(400, 800);
    checkGatewayReceiverLoadUpdatedOnLocators(BigDecimal.valueOf(0.06));
  }

  void checkGatewayReceiverLoadUpdatedOnLocators(BigDecimal expectedConnectionLoad) {
    locator1Site1.invoke(
        () -> await().untilAsserted(() -> assertThat(
            getTotalGatewayReceiverConnectionLoad().compareTo(expectedConnectionLoad))
                .isEqualTo(0)));
    locator2Site1.invoke(
        () -> await().untilAsserted(() -> assertThat(
            getTotalGatewayReceiverConnectionLoad().compareTo(expectedConnectionLoad))
                .isEqualTo(0)));
  }

  BigDecimal getTotalGatewayReceiverConnectionLoad() {
    Map<ServerLocationAndMemberId, ServerLoad> servers =
        ClusterStartupRule.getLocator().getServerLocatorAdvisee().getLoadSnapshot()
            .getGatewayReceiverLoadMap();
    BigDecimal totalLoadOnLocator = new BigDecimal(0);
    for (ServerLoad load : servers.values()) {
      BigDecimal serverLoad = new BigDecimal(String.valueOf(load.getConnectionLoad()));
      totalLoadOnLocator = totalLoadOnLocator.add(serverLoad);
    }
    return totalLoadOnLocator;
  }

  static void verifyReceiverState(boolean isRunning) {
    Set<GatewayReceiver> receivers = ClusterStartupRule.getCache().getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      assertThat(receiver.isRunning()).isEqualTo(isRunning);
    }
  }

  static void validateGatewaySenderMXBeanProxy(final InternalDistributedMember member,
      final String senderId, final boolean isRunning, final boolean isPaused) {
    GatewaySenderMXBean gatewaySenderMXBean = awaitGatewaySenderMXBeanProxy(member, senderId);
    GeodeAwaitility.await(
        "Awaiting GatewaySenderMXBean.isRunning(" + isRunning + ").isPaused(" + isPaused + ")")
        .untilAsserted(() -> {
          assertThat(gatewaySenderMXBean.isRunning()).isEqualTo(isRunning);
          assertThat(gatewaySenderMXBean.isPaused()).isEqualTo(isPaused);
        });
    assertThat(gatewaySenderMXBean).isNotNull();
  }

  static void verifySenderState(String senderId, boolean isRunning, boolean isPaused) {
    GatewaySender sender = ClusterStartupRule.getCache().getGatewaySenders().stream()
        .filter(x -> senderId.equals(x.getId())).findFirst().orElse(null);
    assertThat(sender.isRunning()).isEqualTo(isRunning);
    assertThat(sender.isPaused()).isEqualTo(isPaused);
  }

  void verifyGatewaySenderState(MemberVM memberVM, boolean isRunning) {
    memberVM.invoke(() -> verifySenderState("ln", isRunning, false));
    locator1Site2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(memberVM.getVM()), "ln", isRunning,
            false));
  }

  void executeGatewaySenderActionCommandAndValidateStateSite2(String cliString, MemberVM memberVM)
      throws Exception {
    connectGfshToSite(locator1Site2);
    String command;
    if (memberVM == null) {
      command = new CommandStringBuilder(cliString)
          .addOption(CliStrings.STOP_GATEWAYSENDER__ID, "ln")
          .getCommandString();
      gfsh.executeAndAssertThat(command).statusIsSuccess();
      verifyGatewaySenderState(server1Site2, isRunning(cliString));
      verifyGatewaySenderState(server2Site2, isRunning(cliString));
    } else {
      command = new CommandStringBuilder(cliString)
          .addOption(CliStrings.STOP_GATEWAYSENDER__ID, "ln")
          .addOption(CliStrings.MEMBERS, getMember(memberVM.getVM()).toString())
          .getCommandString();
      gfsh.executeAndAssertThat(command).statusIsSuccess();
      verifyGatewaySenderState(memberVM, isRunning(cliString));
    }
  }

  boolean isRunning(String cliString) {
    return CliStrings.START_GATEWAYSENDER.equals(cliString);
  }

  public static InternalDistributedMember getMember(final VM vm) {
    return vm.invoke(() -> ClusterStartupRule.getCache().getMyId());
  }

  void startClientSite2(int locatorPort) throws Exception {
    clientSite2 =
        clusterStartupRule.startClientVM(8, c -> c.withLocatorConnection(locatorPort));
    clientSite2.invoke(() -> {
      ClusterStartupRule.clientCacheRule.createProxyRegion(REGION_NAME);
    });
  }

  void doPutsClientSite2(int startRange, int stopRange) {
    clientSite2.invoke(() -> {
      Region<Integer, Integer> region =
          ClusterStartupRule.clientCacheRule.getCache().getRegion(REGION_NAME);
      for (int i = startRange; i < stopRange; i++) {
        region.put(i, i);
      }
    });
  }

  void connectGfshToSite(MemberVM locator) throws Exception {
    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }
    gfsh.connectAndVerify(locator);
  }
}
