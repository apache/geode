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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.test.dunit.DUnitEnv;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class PreferSerializedHARegionQueueTest extends JUnit4CacheTestCase {

  private static final long serialVersionUID = 1L;

  @Test
  public void copyingHARegionQueueShouldNotThrowException() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    VM vm4 = host.getVM(4);
    VM vm5 = host.getVM(5);
    VM vm6 = host.getVM(6);

    // Set prefer serialized
    vm1.invoke(() -> setPreferSerialized());
    vm2.invoke(() -> setPreferSerialized());
    vm3.invoke(() -> setPreferSerialized());
    vm4.invoke(() -> setPreferSerialized());

    String regionName = getTestMethodName() + "_PR";
    try {
      // Initialize initial cache servers
      vm1.invoke(() -> initializeServer(regionName));
      vm2.invoke(() -> initializeServer(regionName));

      // Create register interest client
      vm5.invoke(() -> createClient(regionName, true, 1, Integer.MAX_VALUE));

      // Wait for both primary and secondary servers to establish proxies
      vm1.invoke(() -> waitForCacheClientProxies(1));
      vm2.invoke(() -> waitForCacheClientProxies(1));

      // Create client loader and load entries
      int numPuts = 10;
      vm6.invoke(
          () -> createClient(regionName, false, 0, PoolFactory.DEFAULT_SUBSCRIPTION_ACK_INTERVAL));
      vm6.invoke(() -> {
        Region region = getCache().getRegion(regionName);
        IntStream.range(0, numPuts).forEach(i -> region.put(i, i));
      });

      // Verify HARegion sizes
      vm1.invoke(() -> waitForHARegionSize(numPuts));
      vm2.invoke(() -> waitForHARegionSize(numPuts));

      // Initialize next cache server
      vm3.invoke(() -> initializeServer(regionName));

      // Stop one of the original cache servers
      vm1.invoke(() -> closeCache());

      // Wait for new cache server to establish proxies
      vm3.invoke(() -> waitForCacheClientProxies(1));

      // Verify HARegion size
      vm3.invoke(() -> waitForHARegionSize(numPuts));

      // Initialize final cache server
      vm4.invoke(() -> initializeServer(regionName));

      // Stop other original cache server
      vm2.invoke(() -> closeCache());

      // Wait for new cache server to establish proxies
      vm4.invoke(() -> waitForCacheClientProxies(1));

      // Verify HARegion size
      vm4.invoke(() -> waitForHARegionSize(numPuts));

      // Stop the clients to prevent suspect strings when the servers are stopped
      vm5.invoke(() -> closeCache());
      vm6.invoke(() -> closeCache());
    } finally {
      // Clear prefer serialized
      vm1.invoke(() -> clearPreferSerialized());
      vm2.invoke(() -> clearPreferSerialized());
      vm3.invoke(() -> clearPreferSerialized());
      vm4.invoke(() -> clearPreferSerialized());
    }
  }

  public void initializeServer(String regionName) throws IOException {
    getCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);

    final CacheServer cacheServer = getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
  }

  public void createClient(String regionName, boolean subscriptionEnabled,
      int subscriptionRedundancy, int subscriptionAckInterval) {

    ClientCacheFactory clientCacheFactory =
        new ClientCacheFactory().setPoolSubscriptionAckInterval(subscriptionAckInterval)
            .setPoolSubscriptionEnabled(subscriptionEnabled)
            .setPoolSubscriptionRedundancy(subscriptionRedundancy)
            .addPoolLocator("localhost", DUnitEnv.get().getLocatorPort());

    ClientCache cache = getClientCache(clientCacheFactory);

    Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
    if (subscriptionEnabled) {
      region.registerInterest("ALL_KEYS");
    }
  }

  public static void setPreferSerialized() {
    System.setProperty("gemfire.PREFER_SERIALIZED", "true");
  }

  public static void clearPreferSerialized() {
    System.clearProperty("gemfire.PREFER_SERIALIZED");
  }

  public void waitForCacheClientProxies(final int expectedSize) {
    final CacheServer cs = getCache().getCacheServers().iterator().next();
    await()
        .untilAsserted(() -> assertEquals(expectedSize, cs.getAllClientSessions().size()));
  }

  public void waitForHARegionSize(final int expectedSize) {
    final CacheServer cs = getCache().getCacheServers().iterator().next();
    final CacheClientProxy ccp = (CacheClientProxy) cs.getAllClientSessions().iterator().next();
    await()
        .untilAsserted(() -> assertEquals(expectedSize, getHAEventsCount(ccp)));
  }

  private static int getHAEventsCount(CacheClientProxy ccp) {
    Region haRegion = ccp.getHARegion();
    if (haRegion == null) {
      return 0;
    }
    int count = 0;
    for (Object value : haRegion.values()) {
      if (value instanceof HAEventWrapper) {
        count += 1;
      }
    }
    return count;
  }
}
