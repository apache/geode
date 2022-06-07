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
package org.apache.geode.cache.wan;

import static java.lang.System.lineSeparator;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.internal.EndpointManager;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.rules.DockerComposeRule;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.util.internal.GeodeGlossary;


/**
 * These tests use two Geode sites:
 *
 * - One site (the remote one) consisting of a 2-server, 1-locator Geode cluster. The servers host a
 * partition region (region-wan) and have gateway senders to receive events from the other site with
 * the same value for hostname-for-senders and listening on the same port (2324). The servers and
 * locator run each inside a Docker container and are not route-able from the host (where this JUnit
 * test is running). Another Docker container is running the HAProxy image and it's set up as a TCP
 * load balancer. The other site connects to the locator and to the gateway receivers via the TCP
 * load balancer that forwards traffic directed to the 20334 port to the locator and traffic
 * directed to the 2324 port to the receivers in a round robin fashion.
 *
 * - Another site consisting of a 1-server, 1-locator Geode cluster. The server hosts a partition
 * region (region-wan) and has a gateway receiver to send events to the remote site.
 */
@Category({WanTest.class})
public class SeveralGatewayReceiversWithSamePortAndHostnameForSendersTest {

  private static final int NUM_SERVERS = 2;

  private static Cache cache;

  private static final URL DOCKER_COMPOSE_PATH =
      SeveralGatewayReceiversWithSamePortAndHostnameForSendersTest.class
          .getResource("docker-compose.yml");

  @ClassRule
  public static DockerComposeRule docker = new DockerComposeRule.Builder()
      .file(DOCKER_COMPOSE_PATH.getPath())
      .service("haproxy", 20334)
      .service("haproxy", 2324)
      .build();

  @Rule
  public DistributedRule distributedRule =
      DistributedRule.builder().withVMCount(NUM_SERVERS + 1).build();

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Start locator
    docker.execForService("locator", "gfsh", "-e",
        startLocatorCommand());
    // Start server1
    docker.execForService("server1", "gfsh", "-e",
        "start server --name=server1 --locators=locator[20334]");
    docker.execForService("server2", "gfsh", "-e",
        "start server --name=server2 --locators=locator[20334]");

    docker.execForService("locator", "gfsh",
        "-e", "connect --locator=locator[20334]",
        "-e", "create region --name=region-wan --type=PARTITION");

    // Create gateway receiver
    String createGatewayReceiverCommand = createGatewayReceiverCommand();
    docker.execForService("locator", "gfsh", "-e",
        "connect --locator=locator[20334]", "-e", createGatewayReceiverCommand);
  }

  public SeveralGatewayReceiversWithSamePortAndHostnameForSendersTest() {
    super();
  }

  /**
   * The aim of this test is verify that when several gateway receivers in a remote site share the
   * same port and hostname-for-senders, the pings sent from the gateway senders reach the right
   * gateway receiver and not just any of the receivers. Failure to do this may result in the
   * closing of connections by a gateway receiver for not having received the ping in time.
   */
  @Test
  public void testPingsToReceiversWithSamePortAndHostnameForSendersReachTheRightReceivers()
      throws InterruptedException {
    String senderId = "ln";
    String regionName = "region-wan";
    final int remoteLocPort = docker.getExternalPortForService("haproxy", 20334);

    int locPort = createLocator(VM.getVM(0), 1, remoteLocPort);

    VM vm1 = VM.getVM(1);
    createCache(vm1, locPort);

    // We must use more than one dispatcher thread. With just one dispatcher thread, only one
    // connection will be created by the sender towards one of the receivers and it will be
    // monitored by the one ping thread for that remote receiver.
    // With more than one thread, several connections will be opened and there should be one ping
    // thread per remote receiver.
    createGatewaySender(vm1, senderId, 2, true, 5,
        5, GatewaySender.DEFAULT_ORDER_POLICY);

    createPartitionedRegion(vm1, regionName, senderId, 0, 10);

    int NUM_PUTS = 10;

    putKeyValues(vm1, NUM_PUTS, regionName);

    await()
        .untilAsserted(() -> assertThat(getQueuedEvents(vm1, senderId)).isEqualTo(0));


    // Wait longer than the value set in the receivers for
    // maximum-time-between-pings: 10000 (see geode-starter-create.gfsh)
    // to verify that connections are not closed
    // by the receivers because each has received the pings timely.
    int maxTimeBetweenPingsInReceiver = 15000;
    Thread.sleep(maxTimeBetweenPingsInReceiver);

    int senderPoolDisconnects = getSenderPoolDisconnects(vm1, senderId);
    assertEquals(0, senderPoolDisconnects);
  }

  @Test
  public void testSerialGatewaySenderThreadsConnectToSameReceiver() {
    String senderId = "ln";
    String regionName = "region-wan";
    final int remoteLocPort = docker.getExternalPortForService("haproxy", 20334);

    int locPort = createLocator(VM.getVM(0), 1, remoteLocPort);

    VM vm1 = VM.getVM(1);
    createCache(vm1, locPort);

    createGatewaySender(vm1, senderId, 2, false, 5,
        3, GatewaySender.DEFAULT_ORDER_POLICY);

    createPartitionedRegion(vm1, regionName, senderId, 0, 10);

    assertTrue(allDispatchersConnectedToSameReceiver(1));
    assertTrue(allDispatchersConnectedToSameReceiver(2));

  }

  @Test
  public void testTwoSendersWithSameIdShouldUseSameValueForEnforceThreadsConnectToSameServer() {
    String senderId = "ln";
    final int remoteLocPort = docker.getExternalPortForService("haproxy", 20334);

    int locPort = createLocator(VM.getVM(0), 1, remoteLocPort);

    VM vm1 = VM.getVM(1);
    createCache(vm1, locPort);

    VM vm2 = VM.getVM(2);
    createCache(vm2, locPort);

    createGatewaySender(vm1, senderId, 2, false, 5,
        3, GatewaySender.DEFAULT_ORDER_POLICY);

    Exception exception =
        assertThrows(Exception.class, () -> createGatewaySender(vm2, senderId, 2, false, 5,
            3, GatewaySender.DEFAULT_ORDER_POLICY, false));
    assertEquals(exception.getCause().getMessage(), "Cannot create Gateway Sender " + senderId
        + " with enforceThreadsConnectSameReceiver false because another cache has the same Gateway Sender defined with enforceThreadsConnectSameReceiver true");

  }

  /**
   * The aim of this test is verify that when several gateway receivers in a remote site share the
   * same port and hostname-for-senders, the pings sent from the gateway senders reach the right
   * gateway receiver and not just any of the receivers. Check that only one additional connection
   * is used.
   */
  @Test
  public void testPingsToReceiversWithSamePortAndHostnameForSendersUseOnlyOneMoreConnection()
      throws InterruptedException {
    String senderId = "ln";
    String regionName = "region-wan";
    final int remoteLocPort = docker.getExternalPortForService("haproxy", 20334);

    int locPort = createLocator(VM.getVM(0), 1, remoteLocPort);

    VM vm1 = VM.getVM(1);

    vm1.invoke(() -> {
      System.setProperty(
          GeodeGlossary.GEMFIRE_PREFIX
              + "LiveServerPinger.INITIAL_DELAY_MULTIPLIER_IN_MILLISECONDS",
          "500");

      Properties props = new Properties();
      props.setProperty(LOCATORS, "localhost[" + locPort + "]");
      CacheFactory cacheFactory = new CacheFactory(props);
      cache = cacheFactory.create();
    });

    createGatewaySender(vm1, senderId, 2, true, 5,
        2, GatewaySender.DEFAULT_ORDER_POLICY);

    createPartitionedRegion(vm1, regionName, senderId, 0, 10);

    int NUM_PUTS = 10;

    putKeyValues(vm1, NUM_PUTS, regionName);

    await().untilAsserted(() -> assertThat(getQueuedEvents(vm1, senderId)).isEqualTo(0));

    await().untilAsserted(() -> assertThat(getSenderPoolDisconnects(vm1, senderId)).isEqualTo(0));

    await().untilAsserted(
        () -> assertThat(getSenderPoolConnects(vm1, senderId)).isIn(3, 4));
  }


  /**
   * The aim of this test is verify that when several gateway receivers in a remote site share the
   * same port and hostname-for-senders, the pings sent from the gateway senders reach the right
   * gateway receiver and not just any of the receivers. Check that only one destination will be
   * pinged.
   */
  @Test
  public void testPingsToReceiversWithSamePortAndHostnameForSendersReachTheRightReceiver()
      throws InterruptedException {
    String senderId = "ln";
    String regionName = "region-wan";
    final int remoteLocPort = docker.getExternalPortForService("haproxy", 20334);

    int locPort = createLocator(VM.getVM(0), 1, remoteLocPort);

    VM vm1 = VM.getVM(1);
    createCache(vm1, locPort);

    // We use one dispatcher thread. With just one dispatcher thread, only one
    // connection will be created by the sender towards one of the receivers and it will be
    // monitored by the one ping thread for that remote receiver.
    createGatewaySender(vm1, senderId, 2, true, 5,
        1, GatewaySender.DEFAULT_ORDER_POLICY);

    createPartitionedRegion(vm1, regionName, senderId, 0, 10);

    int NUM_PUTS = 1;

    putKeyValues(vm1, NUM_PUTS, regionName);

    await().untilAsserted(() -> {
      assertThat(getQueuedEvents(vm1, senderId)).isEqualTo(0);
      assertThat(getSenderPoolDisconnects(vm1, senderId)).isEqualTo(0);
      assertThat(getPoolEndPointSize(vm1, senderId)).isEqualTo(1);
    });

  }

  private boolean allDispatchersConnectedToSameReceiver(int server) {

    String gfshOutput = runListGatewayReceiversCommandInServer(server);
    Vector<String> sendersConnectedToServer = parseSendersConnectedFromGfshOutput(gfshOutput);
    String firstSenderId = "";
    for (String senderId : sendersConnectedToServer) {
      if (firstSenderId.equals("")) {
        firstSenderId = senderId;
      } else {
        assertEquals("Found two different senders (" + firstSenderId + " and " + senderId
            + ") connected to same receiver in server " + server, firstSenderId, senderId);
      }
    }
    return true;
  }

  private String runListGatewayReceiversCommandInServer(int serverN) {
    return docker.execForService("locator", "gfsh",
        "-e", "set variable --name=APP_RESULT_VIEWER --value=200",
        "-e", "connect --locator=locator[20334]",
        "-e", "list gateways --receivers-only --member=server" + serverN);
  }

  private Vector<String> parseSendersConnectedFromGfshOutput(String gfshOutput) {
    String[] lines = gfshOutput.split(lineSeparator());
    final String sendersConnectedColumnHeader = "Senders Connected";
    String receiverInfo = null;
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].contains(sendersConnectedColumnHeader)) {
        receiverInfo = lines[i + 2];
        break;
      }
    }
    assertNotNull(
        "Error parsing gfsh output. '" + sendersConnectedColumnHeader + "' column header not found",
        receiverInfo);
    String[] tableRow = receiverInfo.split("\\|");
    String sendersConnectedColumnValue = tableRow[3].trim();
    Vector<String> senders = new Vector<>();
    for (String sender : sendersConnectedColumnValue.split(",")) {
      senders.add(sender.trim());
    }
    return senders;
  }

  private int createLocator(VM memberVM, int dsId, int remoteLocPort) {
    return memberVM.invoke("create locator", () -> {
      Properties props = new Properties();
      props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + dsId);
      props.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocPort + "]");
      return Locator.startLocatorAndDS(0, new File(""), props)
          .getPort();
    });
  }

  private static void createCache(VM vm, Integer locPort) {
    vm.invoke(() -> {
      Properties props = new Properties();
      props.setProperty(LOCATORS, "localhost[" + locPort + "]");
      CacheFactory cacheFactory = new CacheFactory(props);
      cache = cacheFactory.create();
    });
  }

  public static void createGatewaySender(VM vm, String dsName, int remoteDsId,
      boolean isParallel, Integer batchSize,
      int numDispatchers,
      GatewaySender.OrderPolicy orderPolicy) {
    createGatewaySender(vm, dsName, remoteDsId, isParallel, batchSize, numDispatchers, orderPolicy,
        true);
  }

  public static void createGatewaySender(VM vm, String dsName, int remoteDsId,
      boolean isParallel, Integer batchSize,
      int numDispatchers,
      GatewaySender.OrderPolicy orderPolicy,
      boolean enforceThreadsConnectToSameReceiver) {
    vm.invoke(() -> {
      final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
      try {
        InternalGatewaySenderFactory gateway =
            (InternalGatewaySenderFactory) cache.createGatewaySenderFactory();
        gateway.setParallel(isParallel);
        gateway.setBatchSize(batchSize);
        gateway.setDispatcherThreads(numDispatchers);
        gateway.setOrderPolicy(orderPolicy);
        gateway.setEnforceThreadsConnectSameReceiver(enforceThreadsConnectToSameReceiver);
        gateway.create(dsName, remoteDsId);

      } finally {
        exln.remove();
      }
    });
  }

  private static void createPartitionedRegion(VM vm, String regionName, String senderIds,
      Integer redundantCopies, Integer totalNumBuckets) {
    vm.invoke(() -> {
      IgnoredException exp =
          IgnoredException.addIgnoredException(ForceReattemptException.class.getName());
      IgnoredException exp1 =
          IgnoredException.addIgnoredException(PartitionOfflineException.class.getName());
      try {
        RegionFactory fact = cache.createRegionFactory(RegionShortcut.PARTITION);
        if (senderIds != null) {
          StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
          while (tokenizer.hasMoreTokens()) {
            String senderId = tokenizer.nextToken();
            fact.addGatewaySenderId(senderId);
          }
        }
        PartitionAttributesFactory pfact = new PartitionAttributesFactory();
        pfact.setTotalNumBuckets(totalNumBuckets);
        pfact.setRedundantCopies(redundantCopies);
        pfact.setRecoveryDelay(0);
        fact.setPartitionAttributes(pfact.create());
        Region r = fact.create(regionName);
        assertNotNull(r);
      } finally {
        exp.remove();
        exp1.remove();
      }
    });
  }

  private static int getQueuedEvents(VM vm, String senderId) {
    return vm.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
      GatewaySenderStats statistics = sender.getStatistics();
      return statistics.getEventQueueSize();
    });
  }

  private static int getPoolEndPointSize(VM vm, String senderId) {
    return vm.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderId);
      EndpointManager manager = sender.getProxy().getEndpointManager();
      return manager.getEndpointMap().size();
    });
  }

  private static int getSenderPoolDisconnects(VM vm, String senderId) {
    return vm.invoke(() -> {
      AbstractGatewaySender sender =
          (AbstractGatewaySender) CacheFactory.getAnyInstance().getGatewaySender(senderId);
      assertThat(sender).isNotNull();
      PoolStats poolStats = sender.getProxy().getStats();
      return poolStats.getDisConnects();
    });
  }

  private static int getSenderPoolConnects(VM vm, String senderId) {
    return vm.invoke(() -> {
      AbstractGatewaySender sender =
          (AbstractGatewaySender) CacheFactory.getAnyInstance().getGatewaySender(senderId);
      assertThat(sender).isNotNull();
      PoolStats poolStats = sender.getProxy().getStats();
      return poolStats.getConnects();
    });
  }

  private static void putKeyValues(VM vm, int numPuts, String region) {
    final HashMap<Integer, Integer> keyValues = new HashMap<>();
    for (int i = 0; i < numPuts; i++) {
      keyValues.put(i, i);
    }
    vm.invoke(() -> putGivenKeyValue(region, keyValues));
  }

  private static void putGivenKeyValue(String regionName, Map<Integer, Integer> keyValues) {
    Region<Integer, Integer> r = cache.getRegion(SEPARATOR + regionName);
    assertNotNull(r);
    for (Object key : keyValues.keySet()) {
      r.put((Integer) key, keyValues.get(key));
    }
  }

  private static String createGatewayReceiverCommand() {
    String ipAddress = docker.getIpAddressForService("haproxy", "geode-wan-test");
    return "create gateway-receiver --hostname-for-senders=" + ipAddress
        + " --start-port=2324 --end-port=2324 --maximum-time-between-pings=10000";
  }

  private static String startLocatorCommand() {
    String ipAddress = docker.getIpAddressForService("haproxy", "geode-wan-test");
    return "start locator --name=locator --port=20334 --connect=false --redirect-output --enable-cluster-configuration=true --hostname-for-clients="
        + ipAddress + " --J=-Dgemfire.distributed-system-id=2";

  }

}
