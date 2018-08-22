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
package org.apache.geode.internal.cache.partitioned;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.ha.HARegionQueueStats;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * The test creates two datastores with a partitioned region, and also running a cache server each.
 * A publisher client is connected to one server while a subscriber client is connected to both the
 * servers. The partitioned region has entry expiry set with ttl of 3 seconds and action as DESTROY.
 * The test ensures that the EXPIRE_DESTROY events are propagated to the subscriber client and the
 * secondary server does process the QRMs for the EXPIRE_DESTROY events.
 */
@Category({RegionsTest.class})
@SuppressWarnings("serial")
public class QueueRemovalMessageProcessingDistributedTest implements Serializable {

  private static final int TOTAL_NUM_BUCKETS = 4;

  private static final AtomicInteger destroyedCount = new AtomicInteger();

  private String regionName;
  private String hostName;

  private VM primary;
  private VM secondary;

  private VM server1;
  private VM server2;
  private VM client1;
  private VM client2;

  @Rule
  public DistributedRule distributedTestRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server1 = getVM(0); // datastore and server
    server2 = getVM(1); // datastore and server
    client1 = getVM(2); // durable client with subscription
    client2 = getVM(3); // durable client without subscription

    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();
    hostName = getHostName();

    int port1 = server1.invoke(() -> createCacheServerWithPRDatastore());
    int port2 = server2.invoke(() -> createCacheServerWithPRDatastore());

    client1.invoke(() -> createClientCacheWithRI(port1, port2, "client1"));
    client2.invoke(() -> createClientCache(port1, "client2"));
  }

  @After
  public void tearDown() throws Exception {
    destroyedCount.set(0);
    invokeInEveryVM(() -> {
      destroyedCount.set(0);
    });
  }

  @Test
  public void testQRMOfExpiredEventsProcessedSuccessfully() throws Exception {
    int putCount = 10;

    // totalEvents = putCount * 2 [eviction-destroys] + 1 [marker message]
    int totalEvents = putCount * 2 + 1;

    client2.invoke(() -> doPuts(putCount));

    identifyPrimaryServer();

    client1.invoke(() -> await().atMost(1, MINUTES).until(() -> destroyedCount.get() == 10));

    primary.invoke(() -> verifyClientSubscriptionStatsOnPrimary(totalEvents));
    secondary.invoke(() -> verifyClientSubscriptionStatsOnSecondary(totalEvents));
  }

  private int createCacheServerWithPRDatastore() throws IOException {
    cacheRule.createCache();

    PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(1);
    paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    RegionFactory<String, String> rf =
        cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION);
    rf.setConcurrencyChecksEnabled(false);
    rf.setEntryTimeToLive(new ExpirationAttributes(3, ExpirationAction.DESTROY));
    rf.setPartitionAttributes(paf.create());

    rf.create(regionName);

    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientCacheWithRI(int port1, int port2, String durableClientId) {
    Properties config = new Properties();
    config.setProperty(DURABLE_CLIENT_ID, durableClientId);
    config.setProperty(DURABLE_CLIENT_TIMEOUT, "300000");

    ClientCacheFactory ccf = new ClientCacheFactory(config);
    ccf.addPoolServer(hostName, port1);
    ccf.addPoolServer(hostName, port2);
    ccf.setPoolSubscriptionAckInterval(50);
    ccf.setPoolSubscriptionEnabled(true);
    ccf.setPoolSubscriptionRedundancy(1);

    clientCacheRule.createClientCache(ccf);

    ClientRegionFactory<String, String> crf = clientCacheRule.getClientCache()
        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    crf.addCacheListener(new CacheListenerAdapter<String, String>() {
      @Override
      public void afterDestroy(EntryEvent<String, String> event) {
        destroyedCount.incrementAndGet();
      }
    });

    Region<String, String> region = crf.create(regionName);

    region.registerInterest("ALL_KEYS", true);
    clientCacheRule.getClientCache().readyForEvents();
  }

  private void createClientCache(int port, String durableClientId) {
    Properties config = new Properties();
    config.setProperty(DURABLE_CLIENT_ID, durableClientId);
    config.setProperty(DURABLE_CLIENT_TIMEOUT, "300000");

    ClientCacheFactory ccf = new ClientCacheFactory(config);
    ccf.setPoolSubscriptionAckInterval(50);
    ccf.setPoolSubscriptionRedundancy(1);
    ccf.addPoolServer(hostName, port);

    clientCacheRule.createClientCache(ccf);

    ClientRegionFactory<String, String> crf = clientCacheRule.getClientCache()
        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    crf.create(regionName);
  }

  private void doPuts(int putCount) {
    Region<String, String> region = clientCacheRule.getClientCache().getRegion(regionName);

    for (int i = 1; i <= putCount; i++) {
      region.put("KEY-" + i, "VALUE-" + i);
    }
  }

  private void verifyClientSubscriptionStatsOnPrimary(final int eventsCount) {
    await().atMost(1, MINUTES).until(() -> allEventsHaveBeenDispatched(eventsCount));
  }

  private boolean allEventsHaveBeenDispatched(final int eventsCount) {
    HARegionQueueStats stats = getCacheClientProxy().getHARegionQueue().getStatistics();

    int numOfEvents = eventsCount;
    long dispatched = stats.getEventsDispatched();

    return numOfEvents == dispatched;
  }

  private void verifyClientSubscriptionStatsOnSecondary(final int eventsCount) {
    await().atMost(1, MINUTES).until(() -> {
      HARegionQueueStats stats = getCacheClientProxy().getHARegionQueue().getStatistics();

      int numOfEvents = eventsCount - 1; // No marker

      long qrmed = stats.getEventsRemovedByQrm();

      return qrmed == numOfEvents || (qrmed + 1) == numOfEvents;
      // Why +1 above? Because sometimes(TODO: explain further) there may
      // not be any QRM sent to the secondary for the last event dispatched
      // at primary.
    });
  }

  private void identifyPrimaryServer() {
    boolean primaryIsServer1 = server1.invoke(() -> isPrimaryServerForClient());

    assertThat(primaryIsServer1).isNotEqualTo(server2.invoke(() -> isPrimaryServerForClient()));

    if (primaryIsServer1) {
      primary = server1;
      secondary = server2;
    } else {
      primary = server2;
      secondary = server1;
    }

    assertThat(primary).isNotNull();
    assertThat(secondary).isNotNull();
    assertThat(primary).isNotEqualTo(secondary);
  }

  private static CacheClientProxy getCacheClientProxy() {
    return (CacheClientProxy) CacheClientNotifier.getInstance().getClientProxies().toArray()[0];
  }

  private static boolean isPrimaryServerForClient() {
    return getCacheClientProxy().isPrimary();
  }
}
