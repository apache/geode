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
package org.apache.geode.internal.jta;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.transaction.Status;

import org.apache.geode.GemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.cache.tx.ClientTXStateStub;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({DistributedTest.class})
public class ClientServerJTADUnitTest extends JUnit4CacheTestCase {
  private String key = "key";
  private String value = "value";
  private String newValue = "newValue";
  final Host host = Host.getHost(0);
  final VM server = host.getVM(0);
  final VM client = host.getVM(1);

  @Test
  public void testClientTXStateStubBeforeCompletion() throws Exception {
    final String regionName = getUniqueName();
    getBlackboard().initBlackboard();
    final Properties properties = getDistributedSystemProperties();

    final int port = server.invoke("create cache", () -> {
      Cache cache = getCache(properties);
      CacheServer cacheServer = createCacheServer(cache, 0);
      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
      region.put(key, value);

      return cacheServer.getPort();
    });

    client.invoke(() -> createClientRegion(host, port, regionName));

    createClientRegion(host, port, regionName);

    Region region = getCache().getRegion(regionName);
    assertTrue(region.get(key).equals(value));

    String first = "one";
    String second = "two";

    client.invokeAsync(() -> commitTxWithBeforeCompletion(regionName, true, first, second));

    getBlackboard().waitForGate(first, 30, TimeUnit.SECONDS);
    TXManagerImpl mgr = (TXManagerImpl) getCache().getCacheTransactionManager();
    mgr.begin();
    region.put(key, newValue);
    TXStateProxyImpl tx = (TXStateProxyImpl) mgr.internalSuspend();
    ClientTXStateStub txStub = (ClientTXStateStub) tx.getRealDeal(null, null);
    mgr.internalResume(tx);
    try {
      txStub.beforeCompletion();
      fail("expected to get CommitConflictException");
    } catch (GemFireException e) {
      // expected commit conflict exception thrown from server
      mgr.setTXState(null);
      getBlackboard().signalGate(second);
    }

    // GEODE commit apply the tx change to cache before releasing the locks held, so
    // the region could have the new value but still hold the locks.
    // Add the wait to check new JTA tx can be committed.
    Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(10, TimeUnit.MILLISECONDS)
        .atMost(30, TimeUnit.SECONDS).until(() -> ableToCommitNewTx(regionName, mgr));
  }

  private boolean expectionLogged = false;

  private boolean ableToCommitNewTx(final String regionName, TXManagerImpl mgr) {
    try {
      commitTxWithBeforeCompletion(regionName, false, null, null);
    } catch (Exception e) {
      if (!expectionLogged) {
        LogService.getLogger().info("got exception stack trace", e);
        expectionLogged = true;
      }
      mgr.setTXState(null);
      return false;
    }
    return true;
  }

  private CacheServer createCacheServer(Cache cache, int maxThreads) {
    CacheServer server = cache.addCacheServer();
    server.setMaxThreads(maxThreads);
    server.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
    try {
      server.start();
    } catch (IOException e) {
      Assert.fail("got exception", e);
    }
    return server;
  }

  private void createClientRegion(final Host host, final int port0, String regionName) {
    ClientCacheFactory cf = new ClientCacheFactory();
    cf.addPoolServer(host.getHostName(), port0);
    ClientCache cache = getClientCache(cf);
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
  }

  private void commitTxWithBeforeCompletion(String regionName, boolean withWait, String first,
      String second) throws TimeoutException, InterruptedException {
    Region region = getCache().getRegion(regionName);
    TXManagerImpl mgr = (TXManagerImpl) getCache().getCacheTransactionManager();
    mgr.begin();
    region.put(key, newValue);
    TXStateProxyImpl tx = (TXStateProxyImpl) mgr.internalSuspend();
    ClientTXStateStub txStub = (ClientTXStateStub) tx.getRealDeal(null, null);
    mgr.internalResume(tx);
    txStub.beforeCompletion();
    if (withWait) {
      getBlackboard().signalGate(first);
      getBlackboard().waitForGate(second, 30, TimeUnit.SECONDS);
    }
    txStub.afterCompletion(Status.STATUS_COMMITTED);
  }

  @Test
  public void testJTAMaxThreads() throws TimeoutException, InterruptedException {
    testJTAWithMaxThreads(1);
  }

  @Test
  public void testJTANoMaxThreadsSetting() throws TimeoutException, InterruptedException {
    testJTAWithMaxThreads(0);
  }

  private void testJTAWithMaxThreads(int maxThreads) {
    final String regionName = getUniqueName();
    getBlackboard().initBlackboard();
    final Properties properties = getDistributedSystemProperties();

    final int port = server.invoke("create cache", () -> {
      Cache cache = getCache(properties);
      CacheServer cacheServer = createCacheServer(cache, maxThreads);
      Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
      region.put(key, value);

      return cacheServer.getPort();
    });

    createClientRegion(host, port, regionName);

    Region region = getCache().getRegion(regionName);
    assertTrue(region.get(key).equals(value));

    try {
      commitTxWithBeforeCompletion(regionName, false, null, null);
    } catch (Exception e) {
      Assert.fail("got unexpected exception", e);
    }
    assertTrue(region.get(key).equals(newValue));
  }
}
