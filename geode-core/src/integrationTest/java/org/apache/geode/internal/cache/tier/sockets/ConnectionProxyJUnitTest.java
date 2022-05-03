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
/*
 * Created on Feb 3, 2006
 *
 */
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.cache.client.PoolManager.createFactory;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.QueueStateImpl.SequenceIdAndExpirationObject;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * Tests the functionality of operations of AbstractConnectionProxy & its derived classes.
 */
@Category({ClientSubscriptionTest.class})
public class ConnectionProxyJUnitTest {
  DistributedSystem system;
  Cache cache;
  PoolImpl proxy = null;
  SequenceIdAndExpirationObject seo = null;
  CacheServer server = null;

  final Duration timeoutToVerifyExpiry = Duration.ofSeconds(30);
  final Duration timeoutToVerifyAckSend = Duration.ofSeconds(30);

  @Rule
  public ServerStarterRule serverStarter =
      new ServerStarterRule().withNoCacheServer().withAutoStart();

  @Before
  public void setUp() throws Exception {
    cache = serverStarter.getCache();
    system = cache.getDistributedSystem();
  }

  @After
  public void after() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testConnectedServerCount() throws Exception {
    int port3 = getRandomAvailableTCPPort();

    PoolFactory pf = PoolManager.createFactory();
    pf.addServer("localhost", port3);
    pf.setSubscriptionEnabled(false);
    pf.setReadTimeout(2000);
    pf.setMinConnections(1);
    pf.setSocketBufferSize(32768);
    pf.setRetryAttempts(1);
    pf.setPingInterval(500);
    proxy = (PoolImpl) pf.create("clientPool");

    assertThatThrownBy(() -> proxy.acquireConnection())
        .isInstanceOf(NoAvailableServersException.class);

    assertThat(proxy.getConnectedServerCount()).isEqualTo(0);
    addCacheServer(port3, 15000);
    await().untilAsserted(() -> {
      assertThat(proxy.getConnectedServerCount()).isEqualTo(1);
    });

  }

  @Test
  public void testThreadIdToSequenceIdMapCreation() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory pf = PoolManager.createFactory();
    pf.addServer("localhost", port3);
    pf.setSubscriptionEnabled(true);
    pf.setSubscriptionRedundancy(-1);
    proxy = (PoolImpl) pf.create("clientPool");
    assertThat(proxy.getThreadIdToSequenceIdMap()).isNotNull();
  }

  @Test
  public void testThreadIdToSequenceIdMapExpiryPositive() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory pf = PoolManager.createFactory();
    pf.addServer("localhost", port3);
    pf.setSubscriptionEnabled(true);
    pf.setSubscriptionRedundancy(-1);
    pf.setSubscriptionMessageTrackingTimeout(4000);
    pf.setSubscriptionAckInterval(2000);
    proxy = (PoolImpl) pf.create("clientPool");

    EventID eid = new EventID(new byte[0], 1, 1);
    assertThat(proxy.verifyIfDuplicate(eid))
        .describedAs(" eid should not be duplicate as it is a new entry")
        .isFalse();

    verifyExpiry();

    assertThat(proxy.verifyIfDuplicate(eid))
        .describedAs(" eid should not be duplicate as it is a new entry")
        .isFalse();
  }


  @Test
  public void testThreadIdToSequenceIdMapExpiryNegative() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory pf = createFactory();
    pf.addServer("localhost", port3);
    pf.setSubscriptionEnabled(true);
    pf.setSubscriptionRedundancy(-1);
    pf.setSubscriptionMessageTrackingTimeout(10000);

    proxy = (PoolImpl) pf.create("clientPool");

    final EventID eid = new EventID(new byte[0], 1, 1);
    assertThat(proxy.verifyIfDuplicate(eid))
        .describedAs(" eid should not be duplicate as it is a new entry")
        .isFalse();

    await().untilAsserted(() -> assertThat(proxy.verifyIfDuplicate(eid)).isTrue());
  }

  @Test
  public void testThreadIdToSequenceIdMapConcurrency() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory pf = PoolManager.createFactory();
    pf.addServer("localhost", port3);
    pf.setSubscriptionEnabled(true);
    pf.setSubscriptionRedundancy(-1);
    pf.setSubscriptionMessageTrackingTimeout(5000);
    pf.setSubscriptionAckInterval(2000);
    proxy = (PoolImpl) pf.create("clientPool");

    final int EVENT_ID_COUNT = 10000; // why 10,000?
    EventID[] eid = new EventID[EVENT_ID_COUNT];
    for (int i = 0; i < EVENT_ID_COUNT; i++) {
      eid[i] = new EventID(new byte[0], i, i);
      assertThat(proxy.verifyIfDuplicate(eid[i]))
          .describedAs("eid can never be duplicate, it is being created for the first time!")
          .isFalse();
    }
    verifyExpiry();

    for (int i = 0; i < EVENT_ID_COUNT; i++) {
      assertThat(proxy.verifyIfDuplicate(eid[i]))
          .describedAs(
              "eid can not be found to be duplicate since the entry should have expired! " + i)
          .isFalse();
    }
  }


  @Test
  public void testDuplicateSeqIdLesserThanCurrentSeqIdBeingIgnored() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory pf = PoolManager.createFactory();
    pf.addServer("localhost", port3);
    pf.setSubscriptionEnabled(true);
    pf.setSubscriptionRedundancy(-1);
    pf.setSubscriptionMessageTrackingTimeout(100000);
    proxy = (PoolImpl) pf.create("clientPool");

    EventID eid1 = new EventID(new byte[0], 1, 5);
    assertThat(proxy.verifyIfDuplicate(eid1))
        .describedAs("eid1 can never be duplicate, it is being created for the first time!")
        .isFalse();

    EventID eid2 = new EventID(new byte[0], 1, 2);

    assertThat(proxy.verifyIfDuplicate(eid2))
        .describedAs("eid2 should be duplicate, seqId is less than highest (5)")
        .isTrue();

    EventID eid3 = new EventID(new byte[0], 1, 3);

    assertThat(proxy.verifyIfDuplicate(eid3))
        .describedAs("eid3 should be duplicate, seqId is less than highest (5)")
        .isTrue();

    assertThat(!proxy.getThreadIdToSequenceIdMap().isEmpty()).isTrue();
    proxy.destroy();
  }


  @Test
  public void testCleanCloseOfThreadIdToSeqId() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory pf = PoolManager.createFactory();
    pf.addServer("localhost", port3);
    pf.setSubscriptionEnabled(true);
    pf.setSubscriptionRedundancy(-1);
    pf.setSubscriptionMessageTrackingTimeout(100000);
    proxy = (PoolImpl) pf.create("clientPool");

    EventID eid1 = new EventID(new byte[0], 1, 2);

    assertThat(proxy.verifyIfDuplicate(eid1))
        .describedAs("eid can never be duplicate, it is being created for the first time!")
        .isFalse();

    EventID eid2 = new EventID(new byte[0], 1, 3);
    assertThat(proxy.verifyIfDuplicate(eid2))
        .describedAs("eid can never be duplicate, since sequenceId is greater ")
        .isFalse();

    assertThat(proxy.verifyIfDuplicate(eid2))
        .describedAs("eid had to be a duplicate, since sequenceId is equal ")
        .isTrue();
    EventID eid3 = new EventID(new byte[0], 1, 1);
    if (!proxy.verifyIfDuplicate(eid3)) {
      fail(" eid had to be a duplicate, since sequenceId is lesser ");
    }
  }

  @Test
  public void testTwoClientsHavingDifferentThreadIdMaps() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory pf = PoolManager.createFactory();
    pf.addServer("localhost", port3);
    pf.setSubscriptionEnabled(true);
    pf.setSubscriptionRedundancy(-1);
    pf.setSubscriptionMessageTrackingTimeout(100000);

    PoolImpl proxy1 = (PoolImpl) pf.create("clientPool1");
    try {
      PoolImpl proxy2 = (PoolImpl) pf.create("clientPool2");
      try {
        Map map1 = proxy1.getThreadIdToSequenceIdMap();
        Map map2 = proxy2.getThreadIdToSequenceIdMap();
        assertThat(map1 == map2).isFalse();
      } finally {
        proxy2.destroy();
      }
    } finally {
      proxy1.destroy();
    }
  }

  @Test
  public void testPeriodicAckSendByClient() throws Exception {
    int port = getRandomAvailableTCPPort();
    addCacheServer(port, null);

    PoolFactory pf = PoolManager.createFactory();
    pf.addServer("localhost", port);
    pf.setSubscriptionEnabled(true);
    pf.setSubscriptionRedundancy(1);
    pf.setReadTimeout(20000);
    pf.setSubscriptionMessageTrackingTimeout(15000);
    pf.setSubscriptionAckInterval(5000);

    proxy = (PoolImpl) pf.create("clientPool");

    EventID eid = new EventID(new byte[0], 1, 1);

    assertThat(proxy.verifyIfDuplicate(eid))
        .describedAs("eid should not be duplicate as it is a new entry")
        .isFalse();

    seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
        .get(new ThreadIdentifier(new byte[0], 1));
    assertThat(seo.getAckSend()).isFalse();

    // should send the ack to server
    seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
        .get(new ThreadIdentifier(new byte[0], 1));
    verifyAckSend(true);

    // New update on same threadId
    eid = new EventID(new byte[0], 1, 2);
    assertThat(proxy.verifyIfDuplicate(eid))
        .describedAs("eid should not be duplicate as it is a new entry")
        .isFalse();

    seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
        .get(new ThreadIdentifier(new byte[0], 1));
    assertThat(seo.getAckSend()).isFalse();

    // should send another ack to server
    seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
        .get(new ThreadIdentifier(new byte[0], 1));
    verifyAckSend(true);

    // should expire with the this mentioned.
    verifyExpiry();
  }

  // No ack will be send if Redundancy level = 0
  @Test
  public void testNoAckSendByClient() throws Exception {
    int port = getRandomAvailableTCPPort();
    addCacheServer(port, null);

    PoolFactory pf = PoolManager.createFactory();
    pf.addServer("localhost", port);
    pf.setSubscriptionEnabled(true);
    pf.setSubscriptionRedundancy(1);
    pf.setReadTimeout(20000);
    pf.setSubscriptionMessageTrackingTimeout(8000);
    pf.setSubscriptionAckInterval(2000);

    proxy = (PoolImpl) pf.create("clientPool");

    EventID eid = new EventID(new byte[0], 1, 1);
    assertThat(proxy.verifyIfDuplicate(eid))
        .describedAs("eid should not be duplicate as it is a new entry")
        .isFalse();

    seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
        .get(new ThreadIdentifier(new byte[0], 1));
    assertThat(seo.getAckSend()).isFalse();

    // should not send an ack as redundancy level = 0;
    seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
        .get(new ThreadIdentifier(new byte[0], 1));
    verifyAckSend(false);

    // should expire without sending an ack as redundancy level = 0.
    verifyExpiry();
  }

  private void verifyAckSend(final boolean expectedAckSend) {
    await().timeout(timeoutToVerifyAckSend).untilAsserted(() -> {
      assertThat(seo.getAckSend()).isEqualTo(expectedAckSend);
    });
  }

  private void verifyExpiry() {
    await().timeout(timeoutToVerifyExpiry).untilAsserted(() -> {
      assertThat(proxy.getThreadIdToSequenceIdMap().size()).isEqualTo(0);
    });
  }

  // start the server
  private void addCacheServer(int serverPort, Integer maxBetweenPings) throws IOException {
    server = cache.addCacheServer();
    if (maxBetweenPings != null) {
      server.setMaximumTimeBetweenPings(maxBetweenPings);
    }
    server.setPort(serverPort);
    server.start();
  }
}
