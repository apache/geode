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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.apache.geode.internal.lang.SystemPropertyHelper.HA_REGION_QUEUE_EXPIRY_TIME_PROPERTY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.THREAD_ID_EXPIRY_TIME_PROPERTY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * HARegionQueueThreadIdExpiryRegressionTest
 * <p>
 * TRAC #48879: Leaking ThreadIdentifiers and DispatchedAndCurrentEvents objects when client uses
 * many short lived threads
 */
@Category(ClientSubscriptionTest.class)
@SuppressWarnings("serial")
public class HARegionQueueThreadIdExpiryRegressionTest implements Serializable {

  private static final int MESSAGE_SYNC_INTERVAL_SECONDS = 60;
  private static final String EXPIRY_TIME_SECONDS = "30";

  private VM server1;
  private VM server2;

  private String regionName;
  private String hostName;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server1 = getVM(0);
    server2 = getVM(1);

    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();
    hostName = getHostName();

    int port1 = server1.invoke(() -> createCacheServer());
    int port2 = server2.invoke(() -> createCacheServer());

    createClientCache(port1, port2);
  }

  @Test
  public void testThreadIdentifiersExpiry() {
    int puts = 10;

    server1.invoke(() -> doPuts(puts));

    server1.invoke(() -> verifyThreadsBeforeExpiration(1));
    server2.invoke(() -> verifyThreadsBeforeExpiration(1));

    server1.invoke(() -> awaitToVerifyStatsAfterExpiration(puts));
    server2.invoke(() -> awaitToVerifyStatsAfterExpiration(puts));
  }

  private int createCacheServer() throws IOException {
    System.setProperty(GEODE_PREFIX + HA_REGION_QUEUE_EXPIRY_TIME_PROPERTY, EXPIRY_TIME_SECONDS);
    System.setProperty(GEODE_PREFIX + THREAD_ID_EXPIRY_TIME_PROPERTY, EXPIRY_TIME_SECONDS);

    cacheRule.createCache();
    cacheRule.getCache().setMessageSyncInterval(MESSAGE_SYNC_INTERVAL_SECONDS);

    RegionFactory<String, String> rf = cacheRule.getCache().createRegionFactory(REPLICATE);
    rf.create(regionName);

    CacheServer server = cacheRule.getCache().addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createClientCache(int port1, int port2) {
    ClientCacheFactory ccf = new ClientCacheFactory();
    ccf.setPoolSubscriptionEnabled(true);
    ccf.setPoolSubscriptionAckInterval(50);
    ccf.setPoolSubscriptionRedundancy(1);
    ccf.addPoolServer(hostName, port1);
    ccf.addPoolServer(hostName, port2);

    clientCacheRule.createClientCache(ccf);

    ClientRegionFactory<String, String> crf =
        clientCacheRule.getClientCache().createClientRegionFactory(CACHING_PROXY);

    Region<String, String> region = crf.create(regionName);
    region.registerInterest("ALL_KEYS");
  }

  private void doPuts(int puts) {
    Region<String, String> region = cacheRule.getCache().getRegion(regionName);
    for (int i = 1; i <= puts; i++) {
      region.put("KEY-" + i, "VALUE-" + i);
    }
  }

  private CacheClientProxy getCacheClientProxy() {
    return CacheClientNotifier.getInstance().getClientProxies().iterator().next();
  }

  private boolean isPrimaryServer() {
    return getCacheClientProxy().isPrimary();
  }

  private void verifyThreadsBeforeExpiration(int expectedThreadIds) {
    HARegionQueueStats stats = getCacheClientProxy().getHARegionQueue().getStatistics();

    int actualThreadIds = stats.getThreadIdentiferCount();

    assertThat(actualThreadIds)
        .as("Expected ThreadIdentifier count >= " + expectedThreadIds + " but actual: "
            + actualThreadIds + (isPrimaryServer() ? " at primary." : " at secondary."))
        .isGreaterThanOrEqualTo(expectedThreadIds);
  }

  private void awaitToVerifyStatsAfterExpiration(int numOfEvents) {
    await().untilAsserted(() -> {
      verifyStatsAfterExpiration(numOfEvents);
    });
  }

  private void verifyStatsAfterExpiration(int numOfEvents) {
    HARegionQueueStats stats = getCacheClientProxy().getHARegionQueue().getStatistics();

    long actualEventsExpired = stats.getEventsExpired();
    long expectedEventsExpired = isPrimaryServer() ? 0 : numOfEvents;

    assertThat(actualEventsExpired)
        .as("Expected eventsExpired: " + expectedEventsExpired + " but actual eventsExpired: "
            + actualEventsExpired + (isPrimaryServer() ? " at primary." : " at secondary."))
        .isEqualTo(expectedEventsExpired);

    int actualThreadIds = stats.getThreadIdentiferCount();

    // Sometimes we may see 1 threadIdentifier due to slow machines, but never equal to
    // expectedThreadIds
    assertThat(actualThreadIds).as("Expected ThreadIdentifier count <= 1 but actual: "
        + actualThreadIds + (isPrimaryServer() ? " at primary." : " at secondary.")).isEqualTo(0);
  }
}
