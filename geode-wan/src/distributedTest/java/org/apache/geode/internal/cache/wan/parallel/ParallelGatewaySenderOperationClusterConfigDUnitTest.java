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

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMember;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewayReceiverMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewaySenderMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifyReceiverState;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderState;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
public class ParallelGatewaySenderOperationClusterConfigDUnitTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(8);

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locatorSite1;
  private MemberVM locatorSite2;
  private MemberVM server1Site1;
  private MemberVM server2Site1;
  private MemberVM server1Site2;
  private MemberVM server2Site2;

  private ClientVM clientSite1;
  private ClientVM clientSite2;

  // Initialize put operations
  private static final Map<String, String> putData;
  static {
    putData = new HashMap<>();
    putData.put("1", "data1");
    putData.put("2", "data2");
    putData.put("3", "data3");
    putData.put("4", "data3");
  }

  @Before
  public void before() throws Exception {
    // Start locators for site #1
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = clusterStartupRule.startLocatorVM(1, props);

    // Start locators for site #2
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorSite1.getPort() + "]");
    locatorSite2 = clusterStartupRule.startLocatorVM(2, props);

    // Start servers and gateway-receiver for site #1
    Properties properties = new Properties();
    server1Site1 = clusterStartupRule.startServerVM(3, properties, locatorSite1.getPort());
    server2Site1 = clusterStartupRule.startServerVM(4, properties, locatorSite1.getPort());

    connectGfshToSite(locatorSite1);
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYRECEIVER);
    csb.addOptionWithValueCheck(CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART, "false");
    csb.addOption(CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT,
        "" + AvailablePort.AVAILABLE_PORTS_LOWER_BOUND);
    csb.addOption(CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT,
        "" + AvailablePort.AVAILABLE_PORTS_UPPER_BOUND);
    gfsh.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    // verify that gateway-receiver has been successfully created on site #1
    server1Site1.invoke(() -> verifyReceiverState(true));
    server2Site1.invoke(() -> verifyReceiverState(true));
    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server1Site1.getVM()), true));
    locatorSite1
        .invoke(() -> validateGatewayReceiverMXBeanProxy(getMember(server2Site1.getVM()), true));

    // create partition region on site #2
    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // start servers for site #2
    server1Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(6, locatorSite2.getPort());

    // create parallel gateway-sender on site #2
    connectGfshToSite(locatorSite2);
    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "1")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1Site2.invoke(() -> verifySenderState("ln", true, false));
    server2Site2.invoke(() -> verifySenderState("ln", true, false));

    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1Site2.getVM()), "ln", true, false));
    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2Site2.getVM()), "ln", true, false));

    // create partition region on site #2
    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_REDUNDANT");
    csb.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, "ln");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // Start clients
    clientSite2 =
        clusterStartupRule.startClientVM(7, c -> c.withLocatorConnection(locatorSite2.getPort()));
    clientSite2.invoke(() -> {
      ClusterStartupRule.clientCacheRule.createProxyRegion("test1");
    });
    clientSite1 =
        clusterStartupRule.startClientVM(8, c -> c.withLocatorConnection(locatorSite1.getPort()));
    clientSite1.invoke(() -> {
      ClusterStartupRule.clientCacheRule.createProxyRegion("test1");
    });
  }

  /**
   * Verify that after gateway-sender is started in paused state all events are queued, and
   * eventually sent over the gateway sender after resume command has been executed.
   */
  @Test
  public void testThatPauseStateRemainAfterTheRestartOfMember() {

    executeGfshCommand(CliStrings.PAUSE_GATEWAYSENDER);
    verifyGatewaySenderState(true, true);

    // stop servers on site #2
    server1Site2.stop(true);
    server2Site2.stop(true);

    // start again servers in Site #2
    server1Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(6, locatorSite2.getPort());

    verifyGatewaySenderState(true, true);

    // Do some puts
    clientSite2.invoke(() -> doPuts(putData));
    // Check that data has been enqueued
    server1Site2.invoke(() -> checkQueueSize("ln", putData.size()));

    executeGfshCommand(CliStrings.RESUME_GATEWAYSENDER);
    verifyGatewaySenderState(true, false);

    // Check that data is sent over the gateway-sender
    clientSite1.invoke(() -> checkPuts(putData.size()));
  }

  @Test
  public void testStopAndStartCommands() {

    executeGfshCommand(CliStrings.STOP_GATEWAYSENDER);
    verifyGatewaySenderState(false, false);

    // stop servers on site #2
    server1Site2.stop(true);
    server2Site2.stop(true);

    // start again servers in Site #2
    server1Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(6, locatorSite2.getPort());

    verifyGatewaySenderState(false, false);

    // Do some puts
    clientSite2.invoke(() -> doPuts(putData));
    clientSite2.invoke(() -> checkPuts(putData.size()));

    // Check that data has not been enqueued
    server1Site2.invoke(() -> checkQueueSize("ln", 0));

    executeGfshCommand(CliStrings.START_GATEWAYSENDER);
    verifyGatewaySenderState(true, false);

    // Check that data is not sent over the gateway-sender
    clientSite1.invoke(() -> checkPuts(0));

    // stop servers on site #2
    server1Site2.stop(true);
    server2Site2.stop(true);

    // start again servers in Site #2
    server1Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(6, locatorSite2.getPort());
    verifyGatewaySenderState(true, false);

    // Do some puts
    clientSite2.invoke(() -> doPuts(putData));
    // Check that data is sent over the gateway-sender
    clientSite1.invoke(() -> checkPuts(putData.size()));
  }

  void connectGfshToSite(MemberVM locator) throws Exception {
    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }
    gfsh.connectAndVerify(locator);
  }

  void verifyGatewaySenderState(boolean isRunning, boolean isPaused) {
    server1Site2.invoke(() -> verifySenderState("ln", isRunning, isPaused));
    server2Site2.invoke(() -> verifySenderState("ln", isRunning, isPaused));
    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1Site2.getVM()), "ln", isRunning,
            isPaused));
    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2Site2.getVM()), "ln", isRunning,
            isPaused));
  }

  private void executeGfshCommand(String cliCommand) {
    String command = new CommandStringBuilder(cliCommand)
        .addOption("id", "ln")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  private static void doPuts(Map<String, String> puts) {
    Region<String, String> region =
        ClusterStartupRule.clientCacheRule.getCache().getRegion("test1");
    region.putAll(puts);
  }

  private void checkPuts(int expectedSize) {
    await()
        .untilAsserted(() -> assertEquals(expectedSize, getDataSize()));
  }

  private int getDataSize() {
    Region<String, String> region =
        ClusterStartupRule.clientCacheRule.getCache().getRegion("test1");
    Map<String, String> data = region.getAll(putData.keySet());
    int size = 0;
    for (String dat : data.values()) {
      if (dat != null) {
        size++;
      }
    }
    return size;
  }

  public static void checkQueueSize(String senderId, int numQueueEntries) {
    await()
        .untilAsserted(() -> testQueueSize(senderId, numQueueEntries));
  }

  public static void testQueueSize(String senderId, int numQueueEntries) {
    assertThat(ClusterStartupRule.getCache()).isNotNull();
    InternalCache internalCache = ClusterStartupRule.getCache();
    GatewaySender sender = internalCache.getGatewaySender(senderId);
    if (sender.isParallel()) {
      int totalSize = 0;
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      if (queues != null) {
        for (RegionQueue q : queues) {
          ConcurrentParallelGatewaySenderQueue prQ = (ConcurrentParallelGatewaySenderQueue) q;
          totalSize += prQ.size();
        }
      }
      assertEquals(numQueueEntries, totalSize);
    } else {
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
      int size = 0;
      for (RegionQueue q : queues) {
        size += q.size();
      }
      assertEquals(numQueueEntries, size);
    }
  }
}
