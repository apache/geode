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
package org.apache.geode.internal.cache.wan.parallel;

import static org.apache.geode.cache.server.CacheServer.DEFAULT_LOAD_POLL_INTERVAL;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMember;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewayReceiverMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewaySenderMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifyReceiverState;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderState;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
public class ParallelGatewaySenderConnectionLoadBalanceDistributedTest implements Serializable {

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
  private static final int NUM_CONNECTION_PER_SERVER_OFFSET = 4;
  private static final int LOAD_POLL_INTERVAL_OFFSET = 2000;

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
    locator2Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server1Site1.getVM()), true));
    locator1Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server1Site1.getVM()), true));

    server2Site1.invoke(() -> verifyReceiverState(true));
    locator2Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server2Site1.getVM()), true));
    locator1Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server2Site1.getVM()), true));

    server3Site1.invoke(() -> verifyReceiverState(true));
    locator2Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server3Site1.getVM()), true));
    locator1Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server3Site1.getVM()), true));

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

    server1Site2.invoke(() -> verifySenderState("ln", true, false));
    server2Site2.invoke(() -> verifySenderState("ln", true, false));
    locator1Site2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1Site2.getVM()), "ln", true, false));
    locator1Site2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2Site2.getVM()), "ln", true, false));

    // create partition region on site #2
    regionCmd = new CommandStringBuilder(CliStrings.CREATE_REGION);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGION, REGION_NAME);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION");
    regionCmd.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, "ln");
    regionCmd.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, "1");
    gfsh.executeAndAssertThat(regionCmd.toString()).statusIsSuccess();
  }

  @Test
  public void testGatewayConnectionCorrectlyLoadBalancedAtStartup() throws Exception {
    // Do put operations to initialize gateway-sender connections
    startClientSite2(locator1Site2.getPort());
    doPutsClientSite2(0, 500);

    checkConnectionLoadBalancedOnServers(server1Site1, server2Site1, server3Site1);
  }

  @Test
  public void testGatewayConnLoadBalancedAfterCoordinatorLocatorShutdown() throws Exception {
    // Do put operations to initialize gateway-sender connections
    startClientSite2(locator1Site2.getPort());
    doPutsClientSite2(0, 400);
    checkConnectionLoadBalancedOnServers(server1Site1, server2Site1, server3Site1);

    executeGatewaySenderActionCommandAndValidateStateSite2(CliStrings.STOP_GATEWAYSENDER, null);

    locator1Site1.stop(true);

    // Wait for default load-poll-interval plus offset to expire, so that all servers send load
    // to locator before continuing with the test
    GeodeAwaitility.await().atLeast(DEFAULT_LOAD_POLL_INTERVAL + LOAD_POLL_INTERVAL_OFFSET,
        TimeUnit.MILLISECONDS);

    executeGatewaySenderActionCommandAndValidateStateSite2(CliStrings.START_GATEWAYSENDER, null);

    doPutsClientSite2(400, 800);

    checkConnectionLoadBalancedOnServers(server1Site1, server2Site1, server3Site1);
  }

  @Test
  public void testGatewayConnLoadBalancedAfterCoordinatorLocatorShutdownAndGatewayReceiverStopped()
      throws Exception {
    // Do put operations to initialize gateway-sender connections
    startClientSite2(locator1Site2.getPort());
    doPutsClientSite2(0, 400);
    checkConnectionLoadBalancedOnServers(server1Site1, server2Site1, server3Site1);

    executeGatewayReceiverActionCommandAndValidateStateSite1(CliStrings.STOP_GATEWAYRECEIVER,
        server1Site1);
    executeGatewaySenderActionCommandAndValidateStateSite2(CliStrings.STOP_GATEWAYSENDER,
        server1Site2);
    locator1Site1.stop(true);

    // Wait for default load-poll-interval plus offset to expire, so that all servers send load
    // to locator before continuing with the test
    GeodeAwaitility.await().atLeast(DEFAULT_LOAD_POLL_INTERVAL + LOAD_POLL_INTERVAL_OFFSET,
        TimeUnit.MILLISECONDS);

    executeGatewayReceiverActionCommandAndValidateStateSite1(CliStrings.START_GATEWAYRECEIVER,
        server1Site1);
    executeGatewaySenderActionCommandAndValidateStateSite2(CliStrings.START_GATEWAYSENDER,
        server1Site2);
    doPutsClientSite2(400, 800);

    checkConnectionLoadBalancedOnServers(server1Site1, server2Site1, server3Site1);
  }

  void executeGatewayReceiverActionCommandAndValidateStateSite1(String cliString, MemberVM memberVM)
      throws Exception {
    connectGfshToSite(locator2Site1);
    String command = new CommandStringBuilder(cliString)
        .addOption(CliStrings.MEMBERS, getMember(memberVM.getVM()).toString())
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    if (cliString.equals(CliStrings.STOP_GATEWAYRECEIVER)) {
      memberVM.invoke(() -> verifyReceiverState(false));
      locator2Site1.invoke(
          () -> validateGatewayReceiverMXBeanProxy(getMember(memberVM.getVM()), false));
    } else if (cliString.equals(CliStrings.START_GATEWAYRECEIVER)) {
      memberVM.invoke(() -> verifyReceiverState(true));
      locator2Site1.invoke(
          () -> validateGatewayReceiverMXBeanProxy(getMember(memberVM.getVM()), true));
    }
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

  void verifyGatewaySenderState(MemberVM memberVM, boolean isRunning) {
    memberVM.invoke(() -> verifySenderState("ln", isRunning, false));
    locator1Site2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(memberVM.getVM()), "ln", isRunning,
            false));
  }

  int getGatewayReceiverStats() {
    Set<GatewayReceiver> gatewayReceivers = ClusterStartupRule.getCache().getGatewayReceivers();
    GatewayReceiver receiver = gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();
    return stats.getCurrentClientConnections();
  }

  void checkConnectionLoadBalancedOnServers(MemberVM... members) {
    int numberOfConnections = members[0].invoke(this::getGatewayReceiverStats);

    for (MemberVM memberVM : members) {
      await().untilAsserted(() -> assertThat(memberVM.invoke(this::getGatewayReceiverStats))
          .isLessThan(numberOfConnections + NUM_CONNECTION_PER_SERVER_OFFSET));
      await().untilAsserted(() -> assertThat(memberVM.invoke(this::getGatewayReceiverStats))
          .isGreaterThan(numberOfConnections - NUM_CONNECTION_PER_SERVER_OFFSET));
    }
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
