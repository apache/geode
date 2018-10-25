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

import static org.apache.geode.cache30.ClientServerTestCase.configureConnectionPool;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.cache.tier.sockets.ConflationDUnitTestHelper.setIsSlowStart;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.apache.geode.internal.lang.SystemPropertyHelper.HA_REGION_QUEUE_EXPIRY_TIME_PROPERTY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * This is a bug test for 36853 (Expiry logic in HA is used to expire early data that a secondary
 * picks up that is not in the primary. But it is also possible that it would cause data that is in
 * the primary queue to be expired. And this can cause a data loss. This issue is mostly related to
 * Expiry mechanism and not HA, but it affects HA functionality).
 *
 * <p>
 * This test has a cache-client connected to one cache-server. The expiry-time of events in the
 * queue for the client at the server is set low and dispatcher is set for delayed start. This will
 * make some of the events in the queue expire before dispatcher can start picking them up for
 * delivery to the client.
 *
 * <p>
 * TRAC #36853: HA events can expire on primary server and this can cause data loss.
 */
@Category({ClientSubscriptionTest.class})
public class HARegionQueueExpiryRegressionTest extends CacheTestCase {

  /** The time in milliseconds by which the start of dispatcher will be delayed */
  private static final int DISPATCHER_SLOWSTART_TIME = 10_000;
  private static final int PUT_COUNT = 5;

  private static CacheListener<String, String> spyCacheListener;

  private String uniqueName;
  private String hostName;
  private int serverPort;

  private VM server;
  private VM client;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server = getHost(0).getVM(0);
    client = getHost(0).getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    hostName = getServerHostName(getHost(0));

    server.invoke(() -> setIsSlowStart());
    serverPort = server.invoke(() -> createServerCache());

    client.invoke(() -> createClientCache());

    addIgnoredException("Unexpected IOException");
    addIgnoredException("Connection reset");
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * First generate some events, then wait for some time to let the initial events expire before
   * the dispatcher sends them to the client. Then do one final put so that the client knows when
   * to being validation.
   *
   * <p>
   * Client is waiting for afterCreate to be invoked number of PUT_COUNT times before proceeding
   * with validation.
   *
   * <p>
   * If the bug exists or is reintroduced, then the events will expire without reaching the client.
   */
  @Test
  public void allEventsShouldReachClientWithoutExpiring() throws Exception {
    server.invoke(() -> generateEvents());
    client.invoke(() -> validateEventCountAtClient());
  }

  private int createServerCache() throws IOException {
    System.setProperty(GEODE_PREFIX + HA_REGION_QUEUE_EXPIRY_TIME_PROPERTY, String.valueOf(1));
    System.setProperty("slowStartTimeForTesting", String.valueOf(DISPATCHER_SLOWSTART_TIME));

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    getCache().createRegion(uniqueName, factory.create());

    CacheServer server = getCache().addCacheServer();
    server.setPort(0);
    server.setNotifyBySubscription(true);
    server.start();
    return server.getPort();
  }

  private void createClientCache() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");

    getCache(config);

    spyCacheListener = spy(CacheListener.class);

    AttributesFactory factory = new AttributesFactory();
    factory.addCacheListener(spyCacheListener);
    factory.setScope(Scope.DISTRIBUTED_ACK);

    configureConnectionPool(factory, hostName, serverPort, -1, true, -1, 2, null);

    Region region = getCache().createRegion(uniqueName, factory.create());

    region.registerInterest("ALL_KEYS");
  }

  /**
   * First generate some events, then wait for some time to let the initial events expire before
   * the dispatcher sends them to the client. Then do one final put so that the client knows when
   * to being validation.
   *
   * <p>
   * Client is waiting for afterCreate to be invoked number of PUT_COUNT times before proceeding
   * with validation.
   */
  private void generateEvents() throws InterruptedException {
    Region<String, String> region = getCache().getRegion(uniqueName);
    for (int i = 0; i < PUT_COUNT - 1; i++) {
      region.put("key" + i, "val-" + i);
    }

    Thread.sleep(DISPATCHER_SLOWSTART_TIME + 1000);

    region.put("key" + PUT_COUNT, "LAST_VALUE");
  }

  /**
   * Waits for the listener to receive all events
   */
  private void validateEventCountAtClient() {
    await()
        .untilAsserted(() -> verify(spyCacheListener, times(PUT_COUNT)).afterCreate(any()));
  }
}
