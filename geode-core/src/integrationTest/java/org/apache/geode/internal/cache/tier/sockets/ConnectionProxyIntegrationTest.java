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
public class ConnectionProxyIntegrationTest {
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
  public void connectedServerCount() throws Exception {
    int port3 = getRandomAvailableTCPPort();

    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.addServer("localhost", port3);
    poolFactory.setSubscriptionEnabled(false);
    poolFactory.setReadTimeout(2000);
    poolFactory.setMinConnections(1);
    poolFactory.setSocketBufferSize(32768);
    poolFactory.setRetryAttempts(1);
    poolFactory.setPingInterval(500);
    proxy = (PoolImpl) poolFactory.create("clientPool");

    assertThatThrownBy(() -> proxy.acquireConnection())
        .isInstanceOf(NoAvailableServersException.class);

    assertThat(proxy.getConnectedServerCount()).isEqualTo(0);
    addCacheServer(port3, 15000);
    await().untilAsserted(() -> {
      assertThat(proxy.getConnectedServerCount()).isEqualTo(1);
    });

  }

  @Test
  public void threadIdToSequenceIdMapCreation() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.addServer("localhost", port3);
    poolFactory.setSubscriptionEnabled(true);
    poolFactory.setSubscriptionRedundancy(-1);
    proxy = (PoolImpl) poolFactory.create("clientPool");
    assertThat(proxy.getThreadIdToSequenceIdMap()).isNotNull();
  }

  @Test
  public void threadIdToSequenceIdMapExpiryPositive() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.addServer("localhost", port3);
    poolFactory.setSubscriptionEnabled(true);
    poolFactory.setSubscriptionRedundancy(-1);
    poolFactory.setSubscriptionMessageTrackingTimeout(4000);
    poolFactory.setSubscriptionAckInterval(2000);
    proxy = (PoolImpl) poolFactory.create("clientPool");

    EventID eventID = new EventID(new byte[0], 1, 1);
    assertThat(proxy.verifyIfDuplicate(eventID))
        .describedAs(" eventID should not be duplicate as it is a new entry")
        .isFalse();

    verifyExpiry();

    assertThat(proxy.verifyIfDuplicate(eventID))
        .describedAs(" eventID should not be duplicate as it is a new entry")
        .isFalse();
  }


  @Test
  public void threadIdToSequenceIdMapExpiryNegative() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory poolFactory = createFactory();
    poolFactory.addServer("localhost", port3);
    poolFactory.setSubscriptionEnabled(true);
    poolFactory.setSubscriptionRedundancy(-1);
    poolFactory.setSubscriptionMessageTrackingTimeout(10000);

    proxy = (PoolImpl) poolFactory.create("clientPool");

    final EventID eventID = new EventID(new byte[0], 1, 1);
    assertThat(proxy.verifyIfDuplicate(eventID))
        .describedAs(" eventID should not be duplicate as it is a new entry")
        .isFalse();

    await().untilAsserted(() -> assertThat(proxy.verifyIfDuplicate(eventID)).isTrue());
  }

  @Test
  public void threadIdToSequenceIdMapConcurrency() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.addServer("localhost", port3);
    poolFactory.setSubscriptionEnabled(true);
    poolFactory.setSubscriptionRedundancy(-1);
    poolFactory.setSubscriptionMessageTrackingTimeout(5000);
    poolFactory.setSubscriptionAckInterval(2000);
    proxy = (PoolImpl) poolFactory.create("clientPool");

    final int EVENT_ID_COUNT = 10000; // why 10,000?
    EventID[] eventIds = new EventID[EVENT_ID_COUNT];
    for (int i = 0; i < EVENT_ID_COUNT; i++) {
      eventIds[i] = new EventID(new byte[0], i, i);
      assertThat(proxy.verifyIfDuplicate(eventIds[i]))
          .describedAs("eventIds can never be duplicate, it is being created for the first time!")
          .isFalse();
    }
    verifyExpiry();

    for (int i = 0; i < EVENT_ID_COUNT; i++) {
      assertThat(proxy.verifyIfDuplicate(eventIds[i]))
          .describedAs(
              "eventIds can not be found to be duplicate since the entry should have expired! " + i)
          .isFalse();
    }
  }


  @Test
  public void duplicateSeqIdLesserThanCurrentSeqIdBeingIgnored() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.addServer("localhost", port3);
    poolFactory.setSubscriptionEnabled(true);
    poolFactory.setSubscriptionRedundancy(-1);
    poolFactory.setSubscriptionMessageTrackingTimeout(100000);
    proxy = (PoolImpl) poolFactory.create("clientPool");

    EventID eventId1 = new EventID(new byte[0], 1, 5);
    assertThat(proxy.verifyIfDuplicate(eventId1))
        .describedAs("eventId1 can never be duplicate, it is being created for the first time!")
        .isFalse();

    EventID eventId2 = new EventID(new byte[0], 1, 2);

    assertThat(proxy.verifyIfDuplicate(eventId2))
        .describedAs("eventId2 should be duplicate, seqId is less than highest (5)")
        .isTrue();

    EventID eventId3 = new EventID(new byte[0], 1, 3);

    assertThat(proxy.verifyIfDuplicate(eventId3))
        .describedAs("eventId3 should be duplicate, seqId is less than highest (5)")
        .isTrue();

    assertThat(!proxy.getThreadIdToSequenceIdMap().isEmpty()).isTrue();
    proxy.destroy();
  }


  @Test
  public void cleanCloseOfThreadIdToSeqId() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.addServer("localhost", port3);
    poolFactory.setSubscriptionEnabled(true);
    poolFactory.setSubscriptionRedundancy(-1);
    poolFactory.setSubscriptionMessageTrackingTimeout(100000);
    proxy = (PoolImpl) poolFactory.create("clientPool");

    EventID eventID1 = new EventID(new byte[0], 1, 2);

    assertThat(proxy.verifyIfDuplicate(eventID1))
        .describedAs("eventID1 can never be duplicate, it is being created for the first time!")
        .isFalse();

    EventID eventID2 = new EventID(new byte[0], 1, 3);
    assertThat(proxy.verifyIfDuplicate(eventID2))
        .describedAs("eventID2 can never be duplicate, since sequenceId is greater ")
        .isFalse();

    assertThat(proxy.verifyIfDuplicate(eventID2))
        .describedAs("eventID2 had to be a duplicate, since sequenceId is equal ")
        .isTrue();

    EventID eventID3 = new EventID(new byte[0], 1, 1);
    assertThat(proxy.verifyIfDuplicate(eventID3))
        .describedAs("eventId3 had to be a duplicate, since sequenceId is lesser")
        .isTrue();
  }

  @Test
  public void twoClientsHavingDifferentThreadIdMaps() throws Exception {
    int port3 = getRandomAvailableTCPPort();
    addCacheServer(port3, 10000);

    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.addServer("localhost", port3);
    poolFactory.setSubscriptionEnabled(true);
    poolFactory.setSubscriptionRedundancy(-1);
    poolFactory.setSubscriptionMessageTrackingTimeout(100000);

    PoolImpl proxy1 = (PoolImpl) poolFactory.create("clientPool1");
    try {
      PoolImpl proxy2 = (PoolImpl) poolFactory.create("clientPool2");
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
  public void periodicAckSendByClient() throws Exception {
    int port = getRandomAvailableTCPPort();
    addCacheServer(port, null);

    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.addServer("localhost", port);
    poolFactory.setSubscriptionEnabled(true);
    poolFactory.setSubscriptionRedundancy(1);
    poolFactory.setReadTimeout(20000);
    poolFactory.setSubscriptionMessageTrackingTimeout(15000);
    poolFactory.setSubscriptionAckInterval(5000);

    proxy = (PoolImpl) poolFactory.create("clientPool");

    EventID eventID = new EventID(new byte[0], 1, 1);

    assertThat(proxy.verifyIfDuplicate(eventID))
        .describedAs("eventID should not be duplicate as it is a new entry")
        .isFalse();

    seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
        .get(new ThreadIdentifier(new byte[0], 1));
    assertThat(seo.getAckSend()).isFalse();

    // should send the ack to server
    seo = (SequenceIdAndExpirationObject) proxy.getThreadIdToSequenceIdMap()
        .get(new ThreadIdentifier(new byte[0], 1));
    verifyAckSend(true);

    // New update on same threadId
    eventID = new EventID(new byte[0], 1, 2);
    assertThat(proxy.verifyIfDuplicate(eventID))
        .describedAs("eventID should not be duplicate as it is a new entry")
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
  public void noAckSendByClient() throws Exception {
    int port = getRandomAvailableTCPPort();
    addCacheServer(port, null);

    PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.addServer("localhost", port);
    poolFactory.setSubscriptionEnabled(true);
    poolFactory.setSubscriptionRedundancy(1);
    poolFactory.setReadTimeout(20000);
    poolFactory.setSubscriptionMessageTrackingTimeout(8000);
    poolFactory.setSubscriptionAckInterval(2000);

    proxy = (PoolImpl) poolFactory.create("clientPool");

    EventID eventID = new EventID(new byte[0], 1, 1);
    assertThat(proxy.verifyIfDuplicate(eventID))
        .describedAs("eventID should not be duplicate as it is a new entry")
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
