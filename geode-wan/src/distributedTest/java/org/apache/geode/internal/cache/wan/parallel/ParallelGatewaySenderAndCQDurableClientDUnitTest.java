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
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMember;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewaySenderMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderState;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
public class ParallelGatewaySenderAndCQDurableClientDUnitTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(10);

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  private static CqListenerTestReceivedEvents cqListener;

  private MemberVM locatorSite1;
  private MemberVM locatorSite2;
  private MemberVM server1Site2;
  private MemberVM server2Site2;
  private MemberVM server3Site2;

  private ClientVM clientSite2;
  private ClientVM clientSite3SubscriptionQueue;

  /**
   * Issue reproduces when following conditions are fulfilled:
   * - Redundant partition region must configured
   * - Number of servers must be greater than number of redundant copies of partition region
   * - Parallel gateway sender must be configured on partition region
   * - Client must register CQs for the region
   * - Transactions must be used with put operations
   * - Events must enqueued in parallel gateway senders (remote site is unavailable)
   *
   * Server that is hosting primary bucket will send TXCommitMessage to the server that
   * is hosting secondary bucket, and also to the server that is hosting CQ subscription
   * queue (if CQ condition is fulfilled). The problem is that server which is hosting CQ
   * subscription queue is not hosting the bucket for which event it actually intended.
   * In this case the server will store this event in bucketToTempQueueMap because it assumes
   * that the bucket is in the process of the creation, which is not correct.
   */
  @Test
  public void testSubscriptionQueueWan() throws Exception {
    configureSites("PARTITION", "1");
    startDurableClient();
    createDurableCQs("SELECT * FROM /test1");

    verifyGatewaySenderState(true, false);

    // Do some puts and check that data has been enqueued
    Set<String> keysQueue = clientSite2.invoke(() -> doPutsInRangeTransaction(0, 50));
    server1Site2.invoke(() -> checkQueueSize("ln", keysQueue.size()));

    server1Site2.invoke(() -> validateEmptyBucketToTempQueueMap("ln"));
    server2Site2.invoke(() -> validateEmptyBucketToTempQueueMap("ln"));
    server3Site2.invoke(() -> validateEmptyBucketToTempQueueMap("ln"));

    // Check that durable client has received all events
    checkCqEvents(keysQueue.size());
  }

  private void startDurableClient()
      throws Exception {
    int locatorPort = locatorSite2.getPort();
    clientSite3SubscriptionQueue = clusterStartupRule.startClientVM(7, ccf -> ccf
        .withPoolSubscription(true).withLocatorConnection(locatorPort).withCacheSetup(c -> c
            .set("durable-client-id", DURABLE_CLIENT_ID)));
  }

  private void createDurableCQs(String... queries) {
    clientSite3SubscriptionQueue.invoke(() -> {
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getClientCache().getQueryService();
      CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
      ParallelGatewaySenderAndCQDurableClientDUnitTest.cqListener =
          new CqListenerTestReceivedEvents();
      cqAttributesFactory.addCqListener(cqListener);

      for (String query : queries) {
        CqQuery cq = queryService.newCq(query, cqAttributesFactory.create(), true);
        cq.execute();
      }
      ClusterStartupRule.getClientCache().readyForEvents();
    });
  }

  /**
   * Check that all events are received on durable client that registered cq's
   */
  private void checkCqEvents(int expectedNumberOfEvents) {
    // Check if number of events is correct
    clientSite3SubscriptionQueue.invoke(() -> {
      await().untilAsserted(() -> assertThat(
          ParallelGatewaySenderAndCQDurableClientDUnitTest.cqListener.getNumEvents())
              .isEqualTo(expectedNumberOfEvents));
    });
  }

  /**
   * Checks that the bucketToTempQueueMap for a partitioned region
   * that holds events for buckets that are not available locally, is empty.
   */
  public static void validateEmptyBucketToTempQueueMap(String senderId) {
    GatewaySender sender = getGatewaySender(senderId);
    int size = 0;
    Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
    for (Object queue : queues) {
      PartitionedRegion region =
          (PartitionedRegion) ((ConcurrentParallelGatewaySenderQueue) queue).getRegion();
      int buckets = region.getTotalNumberOfBuckets();
      for (int bucket = 0; bucket < buckets; bucket++) {
        BlockingQueue<GatewaySenderEventImpl> newQueue =
            ((ConcurrentParallelGatewaySenderQueue) queue).getBucketTmpQueue(bucket);
        if (newQueue != null) {
          size += newQueue.size();
        }
      }
    }

    final int finalSize = size;
    assertEquals("Expected elements in TempQueueMap: " + 0
        + " but actual size: " + finalSize, 0, finalSize);
  }

  private static GatewaySender getGatewaySender(String senderId) {
    Set<GatewaySender> senders = ClusterStartupRule.getCache().getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }
    return sender;
  }

  void configureSites(String regionShortcut, String redundancy)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = clusterStartupRule.startLocatorVM(1, props);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorSite1.getPort() + "]");
    locatorSite2 = clusterStartupRule.startLocatorVM(2, props);

    // start servers for site #2
    Properties serverProps = new Properties();
    serverProps.setProperty("log-level", "debug");
    server1Site2 = clusterStartupRule.startServerVM(3, serverProps, locatorSite2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(4, serverProps, locatorSite2.getPort());
    server3Site2 = clusterStartupRule.startServerVM(5, serverProps, locatorSite2.getPort());

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
    server3Site2.invoke(() -> verifySenderState("ln", true, false));

    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1Site2.getVM()), "ln", true, false));
    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2Site2.getVM()), "ln", true, false));
    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3Site2.getVM()), "ln", true, false));

    // create partition region on site #2
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
    csb.addOption(CliStrings.CREATE_REGION__REGION, "test1");
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, regionShortcut);
    csb.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, "ln");
    csb.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, redundancy);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    // Start client
    clientSite2 =
        clusterStartupRule.startClientVM(6, c -> c.withLocatorConnection(locatorSite2.getPort()));
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
    server3Site2.invoke(() -> verifySenderState("ln", isRunning, isPaused));
  }

  Set<String> doPutsInRangeTransaction(int start, int stop) {
    Region<String, String> region =
        ClusterStartupRule.clientCacheRule.getCache().getRegion("test1");
    Set<String> keys = new HashSet<>();

    CacheTransactionManager transactionManager =
        ClusterStartupRule.getClientCache().getCacheTransactionManager();
    for (int i = start; i < stop; i++) {
      transactionManager.begin();
      region.put(i + "key", i + "value");
      transactionManager.commit();
      keys.add(i + "key");
    }
    return keys;
  }

  private void checkDataAvailable(Set<String> keys) {
    await()
        .untilAsserted(() -> assertEquals(keys.size(), isPutAvail(keys)));
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
    assertTrue(sender.isParallel());
    int totalSize = 0;
    Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
    if (queues != null) {
      for (RegionQueue q : queues) {
        ConcurrentParallelGatewaySenderQueue prQ = (ConcurrentParallelGatewaySenderQueue) q;
        totalSize += prQ.size();
      }
    }
    assertEquals(numQueueEntries, totalSize);
  }

  public class CqListenerTestReceivedEvents implements CqListener {
    private int numEvents = 0;
    private int numErrors = 0;

    public int getNumEvents() {
      return numEvents;
    }

    public int getNumErrors() {
      return numErrors;
    }

    @Override
    public void onEvent(CqEvent aCqEvent) {
      numEvents++;
    }

    @Override
    public void onError(CqEvent aCqEvent) {
      numErrors++;
    }

    @Override
    public void close() {}
  }



}
