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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.InitialImageOperation;
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
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(7);

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  private static CqListenerTestReceivedEvents cqListener;

  private MemberVM locatorSite2;
  private MemberVM server1Site2;
  private MemberVM server2Site2;
  private MemberVM server3Site2;

  private ClientVM clientSite2;
  private ClientVM clientSite2DurableSubscription;

  public static boolean IS_TEMP_QUEUE_USED = false;
  public static boolean IS_HOOK_TRIGGERED = false;

  private static final String DISTRIBUTED_SYSTEM_ID_SITE1 = "1";
  private static final String DISTRIBUTED_SYSTEM_ID_SITE2 = "2";
  private static final String REGION_NAME = "test1";
  private static final String REGION_COLOCATED = "colocatedRegion";
  private static final String REGION_CQ = "SELECT * FROM " + Region.SEPARATOR + REGION_NAME;
  private static final String COLO_REGION_CQ =
      "SELECT * FROM " + Region.SEPARATOR + REGION_COLOCATED;

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
    configureSites("113", false);
    startDurableClient();
    createDurableCQs(REGION_CQ);

    verifyGatewaySenderIsRunningAndNotPaused();

    // Do some puts and check that data has been enqueued
    Set<String> keysQueue =
        clientSite2.invoke(() -> doPutsInRangeTransactionSingleKey(0, 30, REGION_NAME));
    server1Site2.invoke(() -> checkQueueSize("ln", keysQueue.size()));

    server1Site2.invoke(() -> validateBucketToTempQueueMapIsEmpty("ln"));
    server2Site2.invoke(() -> validateBucketToTempQueueMapIsEmpty("ln"));
    server3Site2.invoke(() -> validateBucketToTempQueueMapIsEmpty("ln"));

    // Check that durable client has received all events
    checkCqEvents(keysQueue.size());
  }

  @Test
  public void testSubscriptionQueueWanColocatedRegionsMultipleOperations() throws Exception {
    configureSites("113", true);
    startDurableClient();
    createDurableCQs(REGION_CQ, COLO_REGION_CQ);

    verifyGatewaySenderIsRunningAndNotPaused();

    // Do some puts and check that data has been enqueued
    Set<String> keysQueue =
        clientSite2.invoke(() -> doPutsInRangeTransactionWithTwoPutOperation(0, 30));
    server1Site2.invoke(() -> checkQueueSize("ln", keysQueue.size() * 2));

    server1Site2.invoke(() -> validateBucketToTempQueueMapIsEmpty("ln"));
    server2Site2.invoke(() -> validateBucketToTempQueueMapIsEmpty("ln"));
    server3Site2.invoke(() -> validateBucketToTempQueueMapIsEmpty("ln"));

    // Check that durable client has received all events
    checkCqEvents(keysQueue.size() * 2);
  }

  /**
   * This test case verifies that the server during bucket recovery enqueues all events intended for
   * that bucket in temporary queue, and that after bucket redundancy is restored events are
   * transferred from temporary queue to real bucket queue.
   */
  @Test
  public void testSubscriptionQueueWanTrafficWhileRebalanced() throws Exception {
    configureSites("3", false);
    verifyGatewaySenderIsRunningAndNotPaused();

    List<MemberVM> allMembers = new ArrayList<>();
    allMembers.add(server1Site2);
    allMembers.add(server2Site2);
    allMembers.add(server3Site2);

    server1Site2.invoke(() -> PartitionRegionHelper.assignBucketsToPartitions(
        Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(REGION_NAME)));

    // check that bucketToTempQueueMap is empty on all members
    for (MemberVM member : allMembers) {
      member.invoke(() -> validateBucketToTempQueueMapIsEmpty("ln"));
    }

    // Choose server with bucket to be stopped
    MemberVM serverToStop = getServerToStop(allMembers);
    int bucketId = serverToStop.invoke(this::getPrimaryBucketList).iterator().next();
    serverToStop.stop(true);

    // remove from list the member that has been previously stopped
    allMembers.remove(serverToStop);

    startDurableClient();
    createDurableCQs(REGION_CQ);

    // configure hook on running members
    for (MemberVM member : allMembers) {
      configureHooksOnRunningMember(member, bucketId);
    }

    // perform rebalance operation to trigger bucket redundancy recovery on running servers
    String command = new CommandStringBuilder(CliStrings.REBALANCE)
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    for (MemberVM member : allMembers) {
      // All members after redundancy is recovered should have empty temporary queue
      member.invoke(() -> validateBucketToTempQueueMapIsEmpty("ln"));
      // If hook has been triggered on member, then check if member temporarily queued
      // events while getting initial image from primary server
      if (member.invoke(ParallelGatewaySenderAndCQDurableClientDUnitTest::isHookTriggered)) {
        assertThat(member.invoke(ParallelGatewaySenderAndCQDurableClientDUnitTest::isTempQueueUsed))
            .isTrue();
      }
    }

    // reset all hooks
    for (MemberVM member : allMembers) {
      resetHooks(member);
    }
  }

  /**
   * This hook will run some transaction traffic after server requested initial image from
   * primary bucket server, and then it will check if events are enqueued in temporary queue.
   * It is expected that events are enqueued in this case because bucket is not yet available.
   */
  void configureHooksOnRunningMember(MemberVM server, int bucketId) {
    server.invoke(() -> InitialImageOperation.setGIITestHook(new InitialImageOperation.GIITestHook(
        InitialImageOperation.GIITestHookType.BeforeRequestRVV,
        "_B__" + REGION_NAME + "_" + bucketId) {
      private static final long serialVersionUID = -3790198435185240444L;

      @Override
      public void reset() {}

      @Override
      public void run() {
        doPutsInServerTransaction();
        ParallelGatewaySenderAndCQDurableClientDUnitTest.IS_HOOK_TRIGGERED = true;
        if (sizeOfBucketToTempQueueMap("ln") != 0) {
          ParallelGatewaySenderAndCQDurableClientDUnitTest.IS_TEMP_QUEUE_USED = true;
        }
      }
    }));
  }

  void resetHooks(MemberVM server) {
    server.invoke(() -> InitialImageOperation.resetAllGIITestHooks());
  }

  public static boolean isHookTriggered() {
    return ParallelGatewaySenderAndCQDurableClientDUnitTest.IS_HOOK_TRIGGERED;
  }

  public static boolean isTempQueueUsed() {
    return ParallelGatewaySenderAndCQDurableClientDUnitTest.IS_TEMP_QUEUE_USED;
  }

  MemberVM getServerToStop(List<MemberVM> list) {
    for (MemberVM member : list) {
      if (!member.invoke(this::getPrimaryBucketList).isEmpty()) {
        return member;
      }
    }
    return null;
  }

  private void startDurableClient()
      throws Exception {
    int locatorPort = locatorSite2.getPort();
    clientSite2DurableSubscription = clusterStartupRule.startClientVM(7, ccf -> ccf
        .withPoolSubscription(true).withLocatorConnection(locatorPort).withCacheSetup(c -> c
            .set(ConfigurationProperties.DURABLE_CLIENT_ID, DURABLE_CLIENT_ID)));
  }

  private void createDurableCQs(String... queries) {
    clientSite2DurableSubscription.invoke(() -> {
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

  private Set<Integer> getPrimaryBucketList() {
    PartitionedRegion region = (PartitionedRegion) Objects
        .requireNonNull(ClusterStartupRule.getCache()).getRegion(REGION_NAME);
    return new HashSet<>(region.getDataStore().getAllLocalPrimaryBucketIds());
  }

  /**
   * Check that all events are received on durable client that registered cq's
   */
  private void checkCqEvents(int expectedNumberOfEvents) {
    // Check if number of events is correct
    clientSite2DurableSubscription.invoke(() -> await().untilAsserted(() -> assertThat(
        ParallelGatewaySenderAndCQDurableClientDUnitTest.cqListener.getNumEvents())
            .isEqualTo(expectedNumberOfEvents)));
  }

  /**
   * Checks that the bucketToTempQueueMap for a partitioned region
   * that holds events for buckets that are not available locally, is empty.
   */
  public static void validateBucketToTempQueueMapIsEmpty(String senderId) {
    final int finalSize = sizeOfBucketToTempQueueMap(senderId);
    assertThat(finalSize).isEqualTo(0);
  }

  public static int sizeOfBucketToTempQueueMap(String senderId) {
    GatewaySender sender =
        Objects.requireNonNull(ClusterStartupRule.getCache()).getGatewaySender(senderId);
    int size = 0;
    Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
    for (RegionQueue queue : queues) {
      PartitionedRegion region =
          (PartitionedRegion) queue.getRegion();
      int buckets = region.getTotalNumberOfBuckets();
      for (int bucket = 0; bucket < buckets; bucket++) {
        BlockingQueue<GatewaySenderEventImpl> newQueue =
            ((ConcurrentParallelGatewaySenderQueue) queue).getBucketTmpQueue(bucket);
        if (newQueue != null) {
          size += newQueue.size();
        }
      }
    }
    return size;
  }

  void configureSites(String totalBucketNum, boolean addColocatedRegion)
      throws Exception {

    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, DISTRIBUTED_SYSTEM_ID_SITE1);
    MemberVM locatorSite1 = clusterStartupRule.startLocatorVM(1, props);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, DISTRIBUTED_SYSTEM_ID_SITE2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorSite1.getPort() + "]");
    locatorSite2 = clusterStartupRule.startLocatorVM(2, props);

    // start servers for site #2
    server1Site2 = clusterStartupRule.startServerVM(3, locatorSite2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(4, locatorSite2.getPort());
    server3Site2 = clusterStartupRule.startServerVM(5, locatorSite2.getPort());

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
    csb.addOption(CliStrings.CREATE_REGION__REGION, REGION_NAME);
    csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION");
    csb.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, "ln");
    csb.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, "1");
    csb.addOption(CliStrings.CREATE_REGION__TOTALNUMBUCKETS, totalBucketNum);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();

    if (addColocatedRegion) {
      csb = new CommandStringBuilder(CliStrings.CREATE_REGION);
      csb.addOption(CliStrings.CREATE_REGION__REGION, REGION_COLOCATED);
      csb.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION");
      csb.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, "ln");
      csb.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, "1");
      csb.addOption(CliStrings.CREATE_REGION__TOTALNUMBUCKETS, totalBucketNum);
      csb.addOption(CliStrings.CREATE_REGION__COLOCATEDWITH, REGION_NAME);
      gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
    }

    // Start client
    clientSite2 =
        clusterStartupRule.startClientVM(6, c -> c.withLocatorConnection(locatorSite2.getPort()));
    clientSite2.invoke(() -> {
      ClusterStartupRule.clientCacheRule.createProxyRegion(REGION_NAME);
      if (addColocatedRegion) {
        ClusterStartupRule.clientCacheRule.createProxyRegion(REGION_COLOCATED);
      }
    });
  }

  void connectGfshToSite(MemberVM locator) throws Exception {
    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }
    gfsh.connectAndVerify(locator);
  }

  void verifyGatewaySenderIsRunningAndNotPaused() {
    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1Site2.getVM()), "ln", true,
            false));
    locatorSite2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2Site2.getVM()), "ln", true,
            false));
    server1Site2.invoke(() -> verifySenderState("ln", true, false));
    server2Site2.invoke(() -> verifySenderState("ln", true, false));
    server3Site2.invoke(() -> verifySenderState("ln", true, false));
  }

  Set<String> doPutsInRangeTransactionSingleKey(int start, int stop, String regionName) {
    Region<String, String> region =
        ClusterStartupRule.clientCacheRule.getCache().getRegion(regionName);
    Set<String> keys = new HashSet<>();

    CacheTransactionManager transactionManager =
        Objects.requireNonNull(ClusterStartupRule.getClientCache()).getCacheTransactionManager();
    for (int i = start; i < stop; i++) {
      transactionManager.begin();
      region.put(i + "key", i + "value");
      transactionManager.commit();
      keys.add(i + "key");
    }
    return keys;
  }

  Set<String> doPutsInRangeTransactionWithTwoPutOperation(int start, int stop) {
    Region<String, String> region =
        ClusterStartupRule.clientCacheRule.getCache().getRegion(REGION_NAME);

    Region<String, String> regionC =
        ClusterStartupRule.clientCacheRule.getCache().getRegion(REGION_COLOCATED);

    Set<String> keys = new HashSet<>();
    CacheTransactionManager transactionManager =
        Objects.requireNonNull(ClusterStartupRule.getClientCache()).getCacheTransactionManager();

    for (int i = start; i < stop; i++) {
      transactionManager.begin();
      region.put(i + "key", i + "value");
      regionC.put(i + "key", i + "value");
      transactionManager.commit();
      keys.add(i + "key");
    }
    return keys;
  }

  void doPutsInServerTransaction() {
    Region<String, String> region =
        Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(REGION_NAME);

    CacheTransactionManager transactionManager =
        ClusterStartupRule.getCache().getCacheTransactionManager();
    for (int i = 0; i < 50; i++) {
      transactionManager.begin();
      region.put(i + "key", i + "value");
      transactionManager.commit();
    }
  }

  public static void checkQueueSize(String senderId, int numQueueEntries) {
    await()
        .untilAsserted(() -> testQueueSize(senderId, numQueueEntries));
  }

  public static void testQueueSize(String senderId, int numQueueEntries) {
    assertThat(ClusterStartupRule.getCache()).isNotNull();
    InternalCache internalCache = ClusterStartupRule.getCache();
    GatewaySender sender = internalCache.getGatewaySender(senderId);
    assertThat(sender.isParallel()).isTrue();
    int totalSize = 0;
    Set<RegionQueue> queues = ((AbstractGatewaySender) sender).getQueues();
    if (queues != null) {
      for (RegionQueue q : queues) {
        ConcurrentParallelGatewaySenderQueue prQ = (ConcurrentParallelGatewaySenderQueue) q;
        totalSize += prQ.size();
      }
    }
    assertThat(numQueueEntries).isEqualTo(totalSize);
  }

  public static class CqListenerTestReceivedEvents implements CqListener {
    private int numEvents = 0;

    public int getNumEvents() {
      return numEvents;
    }

    @Override
    public void onEvent(CqEvent aCqEvent) {
      numEvents++;
    }

    @Override
    public void onError(CqEvent aCqEvent) {}

    @Override
    public void close() {}
  }
}
