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
package org.apache.geode.internal.cache.wan.serial;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewaySenderMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderState;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
public class InternalConflictResolutionReplicateRegionWithSerialGwsDistributedTest
    implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(9);

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locator1Site2;

  private MemberVM server1Site1;
  private MemberVM server2Site1;

  private MemberVM server1Site2;
  private MemberVM server2Site2;

  private int server1Site2Port;
  private int server2Site2Port;

  private ClientVM clientConnectedToServer1Site2;
  private ClientVM clientConnectedToServer2Site2;

  private static final String DISTRIBUTED_SYSTEM_ID_SITE1 = "1";
  private static final String DISTRIBUTED_SYSTEM_ID_SITE2 = "2";
  private static final String REGION_NAME = "test1";

  private static final String GATEWAY_SENDER_ID = "ln";

  private final Map.Entry<Integer, Integer> ENTRY_INITIAL = new AbstractMap.SimpleEntry<>(1, 0);
  private final Map.Entry<Integer, Integer> ENTRY_CONFLICT_RESOLUTION_WINNER =
      new AbstractMap.SimpleEntry<>(1, 1);
  private final Map.Entry<Integer, Integer> ENTRY_CONFLICT_RESOLUTION_LOSER =
      new AbstractMap.SimpleEntry<>(1, 2);

  @Before
  public void setupMultiSite() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, DISTRIBUTED_SYSTEM_ID_SITE1);
    MemberVM locator1Site1 = clusterStartupRule.startLocatorVM(0, props);
    MemberVM locator2Site1 = clusterStartupRule.startLocatorVM(1, props, locator1Site1.getPort());

    // start servers for site #1
    server1Site1 =
        clusterStartupRule.startServerVM(2, locator1Site1.getPort(), locator2Site1.getPort());
    server2Site1 =
        clusterStartupRule.startServerVM(3, locator1Site1.getPort(), locator2Site1.getPort());
    connectGfshToSite(locator1Site1);

    // create partition region on site #1
    CommandStringBuilder regionCmd = new CommandStringBuilder(CliStrings.CREATE_REGION);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGION, REGION_NAME);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");

    gfsh.executeAndAssertThat(regionCmd.toString()).statusIsSuccess();

    String csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYRECEIVER)
        .addOption(CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS, "localhost")
        .getCommandString();

    gfsh.executeAndAssertThat(csb).statusIsSuccess();

    server1Site1.invoke(
        InternalConflictResolutionReplicateRegionWithSerialGwsDistributedTest::verifyReceiverState);
    server2Site1.invoke(
        InternalConflictResolutionReplicateRegionWithSerialGwsDistributedTest::verifyReceiverState);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, DISTRIBUTED_SYSTEM_ID_SITE2);
    props.setProperty(REMOTE_LOCATORS,
        "localhost[" + locator1Site1.getPort() + "],localhost[" + locator2Site1.getPort() + "]");
    locator1Site2 = clusterStartupRule.startLocatorVM(5, props);

    // start servers for site #2
    server1Site2 = clusterStartupRule.startServerVM(6, locator1Site2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(7, locator1Site2.getPort());

    server2Site2Port = server2Site2.getPort();
    server1Site2Port = server1Site2.getPort();

    // create gateway-sender on site #2
    connectGfshToSite(locator1Site2);
    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.MEMBERS, server2Site2.getName())
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, GATEWAY_SENDER_ID)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "1")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "false")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION, "true")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    verifyGatewaySenderState(server2Site2, false);

    executeGatewaySenderActionCommandSite2(CliStrings.PAUSE_GATEWAYSENDER);

    // create partition region on site #2
    regionCmd = new CommandStringBuilder(CliStrings.CREATE_REGION);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGION, REGION_NAME);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    regionCmd.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, GATEWAY_SENDER_ID);
    gfsh.executeAndAssertThat(regionCmd.toString()).statusIsSuccess();
  }

  @Test
  public void testEventIsNotConflatedWhenConcurrentModificationIsDetected() throws Exception {
    startClientToServer1Site2(server1Site2Port);
    startClientToServer2Site2(server2Site2Port);

    clientConnectedToServer2Site2.invoke(() -> executePutOperation(ENTRY_INITIAL));
    checkEventIsConsistentlyReplicatedAcrossServers(ENTRY_INITIAL, server1Site2, server2Site2);

    // Configure cache writer on server to delay writing of entry in order to provoke
    // the internal conflict
    server1Site2.invoke(() -> {
      InternalRegion region =
          ClusterStartupRule.getCache().getInternalRegionByPath("/" + REGION_NAME);
      region.getAttributesMutator().setCacheWriter(new TestCacheWriterDelayWritingOfEntry(
          ENTRY_CONFLICT_RESOLUTION_WINNER, ENTRY_CONFLICT_RESOLUTION_LOSER));
    });

    clientConnectedToServer2Site2.invokeAsync(() -> executePutOperation(
        ENTRY_CONFLICT_RESOLUTION_WINNER));

    server1Site2.invoke(() -> await().untilAsserted(() -> assertThat(
        TestCacheWriterDelayWritingOfEntry.ENTRY_CONFLICT_WINNER_HAS_REACHED_THE_REDUNDANT_SERVER)
            .isTrue()));

    clientConnectedToServer1Site2.invokeAsync(() -> executePutOperation(
        ENTRY_CONFLICT_RESOLUTION_LOSER));

    // Check that expected entry has won the internal conflict resolution
    checkEventIsConsistentlyReplicatedAcrossServers(ENTRY_CONFLICT_RESOLUTION_WINNER, server1Site2,
        server2Site2);

    server2Site2.invoke(() -> checkQueueSize(GATEWAY_SENDER_ID, 3));
    executeGatewaySenderActionCommandSite2(CliStrings.RESUME_GATEWAYSENDER);

    // check that expected event is replicated to the remote cluster
    checkEventIsConsistentlyReplicatedAcrossServers(ENTRY_CONFLICT_RESOLUTION_WINNER, server1Site1,
        server2Site1);
  }

  void checkEventIsConsistentlyReplicatedAcrossServers(final Map.Entry<Integer, Integer> entry,
      MemberVM... servers) {
    for (MemberVM server : servers) {
      server.invoke(() -> {
        Region<Integer, Integer> region =
            ClusterStartupRule.getCache().getRegion("/" + REGION_NAME);
        await().untilAsserted(
            () -> assertThat(region.get(entry.getKey())).isEqualTo(entry.getValue()));
      });
    }
  }

  void executeGatewaySenderActionCommandSite2(final String action) throws Exception {
    connectGfshToSite(locator1Site2);
    CommandStringBuilder regionCmd = new CommandStringBuilder(action);
    regionCmd.addOption(CliStrings.MEMBERS, server2Site2.getName());
    regionCmd.addOption(CliStrings.PAUSE_GATEWAYSENDER__ID, GATEWAY_SENDER_ID);
    gfsh.executeAndAssertThat(regionCmd.toString()).statusIsSuccess();

    verifyGatewaySenderState(server2Site2, CliStrings.PAUSE_GATEWAYSENDER.equals(action));
  }

  private void executePutOperation(Map.Entry<Integer, Integer> entry) {
    Region<Integer, Integer> region =
        ClusterStartupRule.clientCacheRule.getCache().getRegion(REGION_NAME);
    region.put(entry.getKey(), entry.getValue());
  }

  public static void checkQueueSize(String senderId, int numQueueEntries) {
    await()
        .untilAsserted(() -> testQueueSize(senderId, numQueueEntries));
  }

  public static void testQueueSize(String senderId, int numQueueEntries) {
    GatewaySender sender = ClusterStartupRule.getCache().getGatewaySender(senderId);
    Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
    int size = 0;
    for (RegionQueue q : queues) {
      size += q.size();
    }
    assertEquals(numQueueEntries, size);
  }

  static void verifyReceiverState() {
    Set<GatewayReceiver> receivers = ClusterStartupRule.getCache().getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      assertThat(receiver.isRunning()).isEqualTo(true);
    }
  }

  void verifyGatewaySenderState(MemberVM memberVM, boolean isPaused) {
    memberVM.invoke(() -> verifySenderState(GATEWAY_SENDER_ID, true, isPaused));
    locator1Site2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(memberVM.getVM()), GATEWAY_SENDER_ID, true,
            isPaused));
  }

  public static InternalDistributedMember getMember(final VM vm) {
    return vm.invoke(() -> ClusterStartupRule.getCache().getMyId());
  }

  void startClientToServer1Site2(final int serverPort) throws Exception {
    clientConnectedToServer1Site2 =
        clusterStartupRule.startClientVM(8, c -> c.withServerConnection(serverPort));
    clientConnectedToServer1Site2.invoke(() -> {
      ClusterStartupRule.clientCacheRule.createProxyRegion(REGION_NAME);
    });
  }

  void startClientToServer2Site2(final int serverPort) throws Exception {
    clientConnectedToServer2Site2 =
        clusterStartupRule.startClientVM(4, c -> c.withServerConnection(serverPort));
    clientConnectedToServer2Site2.invoke(() -> {
      ClusterStartupRule.clientCacheRule.createProxyRegion(REGION_NAME);
    });
  }

  void connectGfshToSite(MemberVM locator) throws Exception {
    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }
    gfsh.connectAndVerify(locator);
  }
}
