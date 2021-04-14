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
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMember;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewayReceiverMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewaySenderMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifyReceiverState;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderState;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
public class SerialGatewaySenderOperationClusterConfigDUnitTest implements Serializable {

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

  /**
   * Verify that serial gateway-sender state is persisted after pause and resume gateway-sender
   * commands are executed, and that gateway-sender works as expected after member restart:
   *
   * 1. Pause gateway-sender
   * 2. Restart servers that host gateway-sender
   * 3. Run some traffic and verify that data is enqueued in serial gateway-sender queues
   * 4. Resume gateway-sender
   * 5. Verify that traffic is sent over the gateway-sender to remote site
   * 6. Restart servers that host gateway-sender
   * 7. Run some traffic and verify that traffic is sent over the gateway-sender to remote site
   */
  @Test
  public void testThatPauseResumeStateRemainAfterTheRestartOfMembers() throws Exception {
    configureSites(false, "PARTITION", "0");

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
    Set<String> keys = clientSite2.invoke(() -> doPutsInRange(0, 15));
    // Check that data has been enqueued
    server1Site2.invoke(() -> checkQueueSize("ln", keys.size()));

    executeGfshCommand(CliStrings.RESUME_GATEWAYSENDER);
    verifyGatewaySenderState(true, false);

    // Check that data is sent over the gateway-sender
    clientSite1.invoke(() -> checkDataAvailable(keys));

    // stop servers on site #2
    server1Site2.stop(true);
    server2Site2.stop(true);

    // start again servers in Site #2
    server1Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(6, locatorSite2.getPort());
    verifyGatewaySenderState(true, false);

    // Do some puts
    Set<String> key1 = clientSite2.invoke(() -> doPutsInRange(20, 35));
    // Check that data is sent over the gateway-sender
    clientSite1.invoke(() -> checkDataAvailable(key1));
  }

  /**
   * Verify that gateway-sender queue is persisted while in paused state and it is recovered after
   * the restart of members.
   *
   * - Region type: PARTITION_PERSISTENT and non-redundant
   * - Gateway sender configured with queue persistence
   *
   * 1. Pause gateway-sender
   * 2. Run some traffic and verify that data is enqueued in serial gateway-sender queues
   * 3. Restart servers that host gateway-sender
   * 4. Check that queues are recovered from persistent storage, and that gateway-sender
   * remained in paused state
   * 5. Run some traffic and verify that new data is enqueued in serial gateway-sender queues
   * 6. Resume gateway-sender
   * 7. Verify that complete traffic is sent over the gateway-sender to remote site
   */
  @Test
  public void testThatPauseStateAndQueuesRemainAfterRestart() throws Exception {
    configureSites(true, "PARTITION_PERSISTENT", "0");

    executeGfshCommand(CliStrings.PAUSE_GATEWAYSENDER);
    verifyGatewaySenderState(true, true);

    Set<String> keysQueued = clientSite2.invoke(() -> doPutsInRange(70, 85));
    clientSite2.invoke(() -> checkDataAvailable(keysQueued));
    server1Site2.invoke(() -> checkQueueSize("ln", keysQueued.size()));

    // stop servers on site #2
    server1Site2.stop(false);
    server2Site2.stop(false);

    // start again servers in Site #2
    Thread thread = new Thread(
        () -> server1Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort()));
    Thread thread1 = new Thread(
        () -> server2Site2 = clusterStartupRule.startServerVM(6, locatorSite2.getPort()));
    // start threads
    thread.start();
    thread1.start();
    thread.join();
    thread1.join();

    verifyGatewaySenderState(true, true);

    // queue recovered from persistent storage
    server1Site2.invoke(() -> checkQueueSize("ln", keysQueued.size()));

    // Do some puts
    Set<String> keysQueued1 = clientSite2.invoke(() -> doPutsInRange(0, 15));
    // Check that data has been enqueued
    server1Site2.invoke(() -> checkQueueSize("ln", keysQueued1.size() + keysQueued.size()));

    executeGfshCommand(CliStrings.RESUME_GATEWAYSENDER);
    verifyGatewaySenderState(true, false);

    // Check that data is sent over the gateway-sender
    server1Site2.invoke(() -> checkQueueSize("ln", 0));
    clientSite1.invoke(() -> checkDataAvailable(keysQueued1));
    clientSite1.invoke(() -> checkDataAvailable(keysQueued));
  }

  /**
   * Verify that gateway-sender is recovered from redundant server after the
   * restart of member.
   *
   * - Region type: PARTITION_PERSISTENT and redundant
   * - Gateway sender configured with queue persistence
   *
   * 1. Pause gateway-sender
   * 2. Run some traffic and verify that data is enqueued in serial gateway-sender queues
   * 3. Stop one server that host gateway-sender
   * 4. Resume gateway-sender on running server, and check that data is replicated
   * 5. Pause gateway-sender again
   * 5. Run some traffic and verify that new data is enqueued in serial gateway-sender queues
   * 6. Start server that has been previously stopped, and check that already replicated data is not
   * inserted into queue
   * 7. Resume gateway-sender
   * 8. Verify that complete traffic is sent over the gateway-sender to remote site
   */
  @Test
  public void testThatPauseStateRemainAfterTheRestartOfMembersRedundantPersistent()
      throws Exception {
    configureSites(true, "PARTITION_PERSISTENT", "1");

    executeGfshCommand(CliStrings.PAUSE_GATEWAYSENDER);
    verifyGatewaySenderState(true, true);

    Set<String> keysQueued = clientSite2.invoke(() -> doPutsInRange(70, 85));
    server1Site2.invoke(() -> checkQueueSize("ln", keysQueued.size()));

    // stop servers on site #2
    server1Site2.stop(false);

    executeGfshCommand(CliStrings.RESUME_GATEWAYSENDER);
    verifyGatewaySenderStateOnMember(server2Site2, true, false);
    server2Site2.invoke(() -> checkQueueSize("ln", 0));
    clientSite1.invoke(() -> checkDataAvailable(keysQueued));

    executeGfshCommand(CliStrings.PAUSE_GATEWAYSENDER);
    verifyGatewaySenderStateOnMember(server2Site2, true, true);
    /*
     * The batch dispatcher thread blocks and waits for configured time (batch-time-interval) to
     * read new events. The batch-time-interval default value is 1000 milliseconds. So even if
     * gateway-sender is paused it will still collect all events(for batch) received within these
     * 1000 milliseconds and dispatch them. After 1000 milliseconds expire dispatcher thread will be
     * actually paused. So it is necessary to wait for batch-time-interval + offset,
     * before putting new data.
     */
    GeodeAwaitility.await()
        .atLeast(Duration.ofMillis(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL + 500));

    Set<String> keysQueued1 = clientSite2.invoke(() -> doPutsInRange(100, 200));
    clientSite2.invoke(() -> checkDataAvailable(keysQueued1));
    server2Site2.invoke(() -> checkQueueSize("ln", keysQueued1.size()));

    // start again servers in Site #2
    Thread thread = new Thread(
        () -> server1Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort()));
    // start threads
    thread.start();
    thread.join();

    verifyGatewaySenderStateOnMember(server1Site2, true, true);
    server1Site2.invoke(() -> checkQueueSize("ln", keysQueued1.size()));

    executeGfshCommand(CliStrings.RESUME_GATEWAYSENDER);
    verifyGatewaySenderStateOnMember(server2Site2, true, false);
    server2Site2.invoke(() -> checkQueueSize("ln", 0));
    clientSite1.invoke(() -> checkDataAvailable(keysQueued1));
    clientSite1.invoke(() -> checkDataAvailable(keysQueued));
  }

  /**
   * Verify that serial gateway-sender state is persisted after stop and start gateway-sender
   * commands are executed, and that gateway-sender works as expected after member restart:
   *
   * 1. Stop gateway-sender
   * 2. Restart servers that host gateway-sender
   * 3. Run some traffic and verify that data is just stored in regions, but not enqueued and sent
   * to the remote site
   * 4. Start gateway-sender
   * 5. Verify that new traffic is sent over the gateway-sender to remote site
   * 6. Restart servers that host gateway-sender
   * 7. Run some traffic and verify that traffic is sent over the gateway-sender to remote site
   */
  @Test
  public void testThatStopStateRemainAfterTheRestartOfMembers() throws Exception {
    configureSites(false, "PARTITION", "0");

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
    Set<String> keys = clientSite2.invoke(() -> doPutsInRange(0, 15));
    clientSite2.invoke(() -> checkDataAvailable(keys));

    // Check that data is not enqueued
    server1Site2.invoke(() -> checkQueueSize("ln", 0));

    executeGfshCommand(CliStrings.START_GATEWAYSENDER);
    verifyGatewaySenderState(true, false);

    // Do some puts
    Set<String> keys1 = clientSite2.invoke(() -> doPutsInRange(20, 35));
    clientSite2.invoke(() -> checkDataAvailable(keys1));
    clientSite1.invoke(() -> checkDataAvailable(keys1));

    // stop servers on site #2
    server1Site2.stop(true);
    server2Site2.stop(true);

    // start again servers in Site #2
    server1Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(6, locatorSite2.getPort());
    verifyGatewaySenderState(true, false);

    // Do some puts
    Set<String> key1 = clientSite2.invoke(() -> doPutsInRange(40, 55));
    // Check that data is sent over the gateway-sender
    clientSite1.invoke(() -> checkDataAvailable(key1));
    clientSite1.invoke(() -> checkDataNotAvailable(keys));
  }

  /**
   * Verify that gateway-sender queue is not filled in stopped state and it is not recovered
   * after the restart of member.
   *
   * - Region type: PARTITION_PERSISTENT and non-redundant
   * - Gateway sender configured with queue persistence
   *
   * 1. Stop gateway-sender
   * 2. Run some traffic and verify that data is not enqueued in serial gateway-sender queues
   * 3. Restart servers that host gateway-sender
   * 4. Check that queues are recovered empty, and that gateway-sender
   * remained in stopped state
   * 5. Run some traffic and verify that data is not enqueued in serial gateway-sender queues
   * 6. Start gateway-sender
   * 7. Verify that no traffic is sent over the gateway-sender to remote site
   */
  @Test
  public void testThatStopStateRemainAfterTheRestartOfMembersPersistent() throws Exception {
    configureSites(true, "PARTITION_PERSISTENT", "0");

    executeGfshCommand(CliStrings.STOP_GATEWAYSENDER);
    verifyGatewaySenderState(false, false);

    Set<String> keysNotQueued = clientSite2.invoke(() -> doPutsInRange(70, 85));
    clientSite2.invoke(() -> checkDataAvailable(keysNotQueued));
    server1Site2.invoke(() -> checkQueueSize("ln", 0));

    // stop servers on site #2
    server1Site2.stop(false);
    server2Site2.stop(false);

    // start again servers in Site #2
    Thread thread = new Thread(
        () -> server1Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort()));
    Thread thread1 = new Thread(
        () -> server2Site2 = clusterStartupRule.startServerVM(6, locatorSite2.getPort()));
    // start threads
    thread.start();
    thread1.start();
    thread.join();
    thread1.join();

    verifyGatewaySenderState(false, false);
    // Do some puts
    Set<String> keysNotQueued1 = clientSite2.invoke(() -> doPutsInRange(0, 15));
    // Check that data has been enqueued
    clientSite2.invoke(() -> checkDataAvailable(keysNotQueued1));
    server1Site2.invoke(() -> checkQueueSize("ln", 0));

    executeGfshCommand(CliStrings.START_GATEWAYSENDER);
    verifyGatewaySenderState(true, false);

    server1Site2.invoke(() -> checkQueueSize("ln", 0));
    clientSite1.invoke(() -> checkDataNotAvailable(keysNotQueued));
    clientSite1.invoke(() -> checkDataNotAvailable(keysNotQueued1));
  }

  /**
   * Verify that parallel gateway-sender queue recovers only data enqueued prior to stop command.
   *
   * - Region type: PARTITION_PERSISTENT and non-redundant
   * - Gateway sender configured with queue persistence
   *
   * 1. Pause gateway-sender
   * 2. Run some traffic and verify that data is enqueued in serial gateway-sender queues
   * 3. Stop gateway-sender
   * 4. Restart all servers
   * 5. Check that queues are not recovered from persistent storage, and that gateway-sender
   * remained in stopped state
   * 6. Start gateway-senders
   */
  @Test
  public void testThatStopStateRemainAfterTheRestartOfMembersNotRecover() throws Exception {
    configureSites(true, "PARTITION_PERSISTENT", "0");

    executeGfshCommand(CliStrings.PAUSE_GATEWAYSENDER);
    verifyGatewaySenderState(true, true);

    Set<String> keysQueued = clientSite2.invoke(() -> doPutsInRange(70, 85));
    clientSite2.invoke(() -> checkDataAvailable(keysQueued));
    server1Site2.invoke(() -> checkQueueSize("ln", keysQueued.size()));

    executeGfshCommand(CliStrings.STOP_GATEWAYSENDER);
    verifyGatewaySenderState(false, false);

    Set<String> keysNotQueued = clientSite2.invoke(() -> doPutsInRange(100, 105));
    clientSite2.invoke(() -> checkDataAvailable(keysNotQueued));
    server1Site2.invoke(() -> checkQueueSize("ln", 0));

    // stop servers on site #2
    server1Site2.stop(false);
    server2Site2.stop(false);

    // start again servers in Site #2
    Thread thread = new Thread(
        () -> server1Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort()));
    Thread thread1 = new Thread(
        () -> server2Site2 = clusterStartupRule.startServerVM(6, locatorSite2.getPort()));
    // start threads
    thread.start();
    thread1.start();
    thread.join();
    thread1.join();

    verifyGatewaySenderState(false, false);

    executeGfshCommand(CliStrings.START_GATEWAYSENDER);
    verifyGatewaySenderState(true, false);
    // Check that data is sent over the gateway-sender
    server1Site2.invoke(() -> checkQueueSize("ln", 0));
    clientSite1.invoke(() -> checkDataAvailable(keysQueued));
    clientSite1.invoke(() -> checkDataNotAvailable(keysNotQueued));
  }

  void configureSites(boolean enableGWSPersistence, String regionShortcut, String redundancy)
      throws Exception {
    String enablePersistenceParameter = "false";
    if (enableGWSPersistence) {
      enablePersistenceParameter = "true";
    }

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

    // create partition region on site #1
    csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortcut);
    csb.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, redundancy);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // Start client for site #1
    clientSite1 =
        clusterStartupRule.startClientVM(8, c -> c.withLocatorConnection(locatorSite1.getPort()));
    clientSite1.invoke(() -> {
      ClusterStartupRule.clientCacheRule.createProxyRegion("test1");
    });

    // start servers for site #2
    server1Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(6, locatorSite2.getPort());

    // create serial gateway-sender on site #2
    connectGfshToSite(locatorSite2);
    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "1")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "false")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE, enablePersistenceParameter)
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
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortcut);
    csb.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, "ln");
    csb.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, redundancy);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // Start client
    clientSite2 =
        clusterStartupRule.startClientVM(7, c -> c.withLocatorConnection(locatorSite2.getPort()));
    clientSite2.invoke(() -> {
      ClusterStartupRule.clientCacheRule.createProxyRegion("test1");
    });
  }

  void connectGfshToSite(MemberVM locator) throws Exception {
    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }
    gfsh.connectAndVerify(locator);
  }

  void verifyGatewaySenderState(boolean isRunning, boolean isPaused) {
    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1Site2.getVM()), "ln", isRunning,
            isPaused));
    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2Site2.getVM()), "ln", isRunning,
            isPaused));
    server1Site2.invoke(() -> verifySenderState("ln", isRunning, isPaused));
    server2Site2.invoke(() -> verifySenderState("ln", isRunning, isPaused));
  }

  void verifyGatewaySenderStateOnMember(MemberVM member, boolean isRunning, boolean isPaused) {
    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(member.getVM()), "ln", isRunning,
            isPaused));
    member.invoke(() -> verifySenderState("ln", isRunning, isPaused));
  }

  private void executeGfshCommand(String cliCommand) {
    String command = new CommandStringBuilder(cliCommand)
        .addOption("id", "ln")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  Set<String> doPutsInRange(int start, int stop) {
    Region<String, String> region =
        ClusterStartupRule.clientCacheRule.getCache().getRegion("test1");
    Set<String> keys = new HashSet<>();
    for (int i = start; i < stop; i++) {
      region.put(i + "key", i + "value");
      keys.add(i + "key");
    }
    return keys;
  }

  private void checkDataAvailable(Set<String> keys) {
    await()
        .untilAsserted(() -> assertEquals(keys.size(), isPutAvail(keys)));
  }

  private void checkDataNotAvailable(Set<String> keys) {
    assertNotEquals(keys.size(), isPutAvail(keys));
  }

  private int isPutAvail(Set<String> keys) {
    Region<String, String> region =
        ClusterStartupRule.clientCacheRule.getCache().getRegion("test1");
    Map<String, String> data = region.getAll(keys);
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
    Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
    assertFalse(sender.isParallel());
    int size = 0;
    if (queues != null) {
      for (RegionQueue q : queues) {
        size += q.size();
      }
    }
    assertEquals(numQueueEntries, size);
  }
}
