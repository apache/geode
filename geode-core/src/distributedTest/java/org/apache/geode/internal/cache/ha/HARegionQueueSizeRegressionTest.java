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
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheListener;
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
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Client queue size statistic should not go negative when client goes offline.
 *
 * <p>
 * TRAC #48571: CacheClientProxy.getQueueSizeStat() gives negative numbers when client goes down.
 */
@Category(ClientSubscriptionTest.class)
public class HARegionQueueSizeRegressionTest implements Serializable {

  private static final AtomicInteger numOfPuts = new AtomicInteger();

  private static volatile CacheListener<String, String> spyCacheListener;

  private String regionName;
  private String hostName;

  private VM server;
  private VM client;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    server = getVM(0);
    client = getVM(1);

    hostName = getHostName();
    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();

    addIgnoredException("Unexpected IOException||Connection reset");
  }

  @After
  public void tearDown() throws Exception {
    invokeInEveryVM(() -> {
      spyCacheListener = null;
    });
  }

  @Test
  public void testStatsMatchWithSize() throws Exception {
    // start a server
    int port = server.invoke(() -> createServerCache());

    // create durable client, with durable RI
    client.invoke(() -> createClientCache(hostName, port));

    // do puts on server
    server.invoke(() -> doPuts(10));

    client.invoke(() -> awaitCreates(10));

    // close durable client
    client.invoke(() -> closeClientCacheWithKeepAlive());

    server.invoke(() -> awaitProxyIsPaused());

    // resume puts on server
    server.invoke(() -> resumePuts(10));

    // start durable client
    client.invoke(() -> createClientCache(hostName, port));

    // wait for full queue dispatch
    client.invoke(() -> awaitCreates(10));

    // verify the stats
    server.invoke(() -> verifyStats());
  }

  private int createServerCache() throws IOException {
    cacheRule.createCache();

    RegionFactory<String, String> rf = cacheRule.getCache().createRegionFactory(REPLICATE);
    rf.setConcurrencyChecksEnabled(false);
    rf.create(regionName);

    CacheServer server1 = cacheRule.getCache().addCacheServer();
    server1.setPort(0);
    server1.start();
    return server1.getPort();
  }

  private void createClientCache(String hostName, Integer port) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.setProperty(DURABLE_CLIENT_ID, "durable-48571");
    config.setProperty(DURABLE_CLIENT_TIMEOUT, "300000");

    ClientCacheFactory ccf = new ClientCacheFactory(config);
    ccf.setPoolSubscriptionEnabled(true);
    ccf.setPoolSubscriptionAckInterval(50);
    ccf.setPoolSubscriptionRedundancy(0);
    ccf.addPoolServer(hostName, port);

    clientCacheRule.createClientCache(ccf);

    ClientRegionFactory<String, String> crf =
        clientCacheRule.getClientCache().createClientRegionFactory(CACHING_PROXY);
    crf.setConcurrencyChecksEnabled(false);

    spyCacheListener = spy(CacheListener.class);
    crf.addCacheListener(spyCacheListener);

    Region<String, String> region = crf.create(regionName);
    region.registerInterest("ALL_KEYS", true);

    clientCacheRule.getClientCache().readyForEvents();
  }

  private void closeClientCacheWithKeepAlive() {
    clientCacheRule.getClientCache().close(true);
  }

  private void doPuts(int creates) {
    Region<String, String> region = cacheRule.getCache().getRegion(regionName);

    for (int i = 1; i <= creates; i++) {
      put(region, "KEY-" + i, "VALUE-" + i);
    }
  }

  private void awaitProxyIsPaused() {
    await().untilAsserted(() -> {
      CacheClientNotifier ccn = CacheClientNotifier.getInstance();
      Collection<CacheClientProxy> ccProxies = ccn.getClientProxies();

      boolean pausedFlag = false;
      for (CacheClientProxy ccp : ccProxies) {
        if (ccp.isPaused()) {
          pausedFlag = true;
          break;
        }
      }
      assertThat(pausedFlag).as("Proxy has not been paused in 1 minute").isTrue();
    });
  }

  private void resumePuts(int creates) {
    Region<String, String> region = cacheRule.getCache().getRegion(regionName);
    for (int i = 1; i <= creates; i++) {
      put(region, "NEWKEY-" + i, "VALUE_" + i);
    }
  }

  private void put(Region<String, String> region, String key, String value) {
    region.put(key, value);
    numOfPuts.incrementAndGet();
  }

  private void awaitCreates(int expectedCreates) {
    await().untilAsserted(() -> {
      verify(spyCacheListener, times(expectedCreates)).afterCreate(any());
    });
  }

  private void verifyStats() {
    await().untilAsserted(() -> {
      CacheClientNotifier ccn = CacheClientNotifier.getInstance();
      CacheClientProxy ccp = ccn.getClientProxies().iterator().next();
      // TODO: consider verifying ccp.getQueueSize()
      // TODO: consider verifying ccp.getQueueSizeStat()
      // TODO: consider verifying ccp.getHARegionQueue().getStatistics().getEventsEnqued()
      // TODO: consider verifying ccp.getHARegionQueue().getStatistics().getEventsDispatched()
      // TODO: consider verifying ccp.getHARegionQueue().getStatistics().getEventsRemoved()
      // TODO: consider verifying ccp.getHARegionQueue().getStatistics().getNumVoidRemovals()
      assertThat(ccp.getQueueSizeStat()).as("The queue size did not match the stat value")
          .isEqualTo(ccp.getQueueSize());
    });
  }
}
