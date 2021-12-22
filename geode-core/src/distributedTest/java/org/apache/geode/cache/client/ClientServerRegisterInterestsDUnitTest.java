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
package org.apache.geode.cache.client;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * The ClientServerRegisterInterestsDUnitTest class is a test suite of test cases testing the
 * interaction between a client and a server in a Register Interests scenario.
 *
 * @since GemFire 8.0
 */
@Category({ClientSubscriptionTest.class})
public class ClientServerRegisterInterestsDUnitTest extends JUnit4DistributedTestCase {

  protected static final long WAIT_TIME_MILLISECONDS = TimeUnit.SECONDS.toMillis(5);

  protected static final String LOCALHOST = "localhost";

  private final AtomicInteger serverPort = new AtomicInteger(0);

  private final Stack entryEvents = new Stack();

  private VM gemfireServerVm;

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    setupGemFireCacheServer();
    IgnoredException.addIgnoredException("java.net.ConnectException");
  }

  @Override
  public final void preTearDown() throws Exception {
    serverPort.set(0);
    entryEvents.clear();
    gemfireServerVm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        CacheFactory.getAnyInstance().close();
      }
    });
    gemfireServerVm = null;
  }

  private void setupGemFireCacheServer() {
    Host localhost = Host.getHost(0);

    gemfireServerVm = localhost.getVM(0);
    serverPort.set(AvailablePortHelper.getRandomAvailableTCPPort());

    gemfireServerVm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        try {
          Cache cache = new CacheFactory()
              .set("name", "ClientServerRegisterInterestsTestGemFireServer").set(MCAST_PORT, "0")
              .set(LOG_FILE, "clientServerRegisterInterestsTest.log").set(LOG_LEVEL, "config")
              // .set("jmx-manager", "true")
              // .set("jmx-manager-http-port", "0")
              // .set("jmx-manager-port", "1199")
              // .set("jmx-manager-start", "true")
              .create();

          RegionFactory<String, String> regionFactory = cache.createRegionFactory();

          regionFactory.setDataPolicy(DataPolicy.REPLICATE);
          regionFactory.setKeyConstraint(String.class);
          regionFactory.setValueConstraint(String.class);

          Region<String, String> example = regionFactory.create("Example");

          assertNotNull("The 'Example' Region was not properly configured and initialized!",
              example);
          assertEquals(SEPARATOR + "Example", example.getFullPath());
          assertEquals("Example", example.getName());
          assertTrue(example.isEmpty());

          example.put("1", "ONE");

          assertFalse(example.isEmpty());
          assertEquals(1, example.size());

          CacheServer cacheServer = cache.addCacheServer();

          cacheServer.setPort(serverPort.get());
          cacheServer.setMaxConnections(10);

          ClientSubscriptionConfig clientSubscriptionConfig =
              cacheServer.getClientSubscriptionConfig();

          clientSubscriptionConfig.setCapacity(100);
          clientSubscriptionConfig.setEvictionPolicy("entry");

          cacheServer.start();

          assertTrue("Cache Server is not running!", cacheServer.isRunning());
        } catch (UnknownHostException e) {
          throw new RuntimeException(e);
        } catch (IOException e) {
          throw new RuntimeException(String.format(
              "Failed to start the GemFire Cache Server listening on port (%1$d) due to IO error!",
              serverPort.get()), e);
        }
      }
    });
  }

  private ClientCache setupGemFireClientCache() {
    ClientCache clientCache =
        new ClientCacheFactory().set(DURABLE_CLIENT_ID, "TestDurableClientId").create();

    PoolFactory poolFactory = PoolManager.createFactory();

    poolFactory.setMaxConnections(10);
    poolFactory.setMinConnections(1);
    poolFactory.setReadTimeout(5000);
    poolFactory.setSubscriptionEnabled(true);
    poolFactory.addServer(LOCALHOST, serverPort.get());

    Pool pool = poolFactory.create("serverConnectionPool");

    assertNotNull("The 'serverConnectionPool' was not properly configured and initialized!", pool);

    ClientRegionFactory<String, String> regionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    regionFactory.addCacheListener(new TestEntryCacheListener());
    regionFactory.setPoolName(pool.getName());
    regionFactory.setKeyConstraint(String.class);
    regionFactory.setValueConstraint(String.class);

    Region<String, String> exampleCachingProxy = regionFactory.create("Example");

    assertNotNull("The 'Example' Client Region was not properly configured and initialized",
        exampleCachingProxy);

    clientCache.readyForEvents();

    exampleCachingProxy.registerInterest("ALL_KEYS", InterestResultPolicy.DEFAULT, false, true);

    return clientCache;
  }

  @SuppressWarnings("unchecked")
  protected <K, V> V put(final String regionName, final K key, final V value) {
    return (V) gemfireServerVm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Cache cache = CacheFactory.getAnyInstance();
        cache.getRegion(regionName).put(key, value);
        return cache.getRegion(regionName).get(key);
      }
    });
  }

  protected void waitOnEvent(final long waitTimeMilliseconds) {
    final long timeout = (System.currentTimeMillis() + waitTimeMilliseconds);

    while (entryEvents.empty() && (System.currentTimeMillis() < timeout)) {
      synchronized (this) {
        try {
          TimeUnit.MILLISECONDS.timedWait(this, Math.min(500, waitTimeMilliseconds));
        } catch (InterruptedException ignore) {
        }
      }
    }
  }

  @Test
  public void testClientRegisterInterests() {
    ClientCache clientCache = setupGemFireClientCache();

    try {
      Region<String, String> example = clientCache.getRegion(SEPARATOR + "Example");

      assertNotNull("'Example' Region in Client Cache was not found!", example);
      assertEquals(1, example.size());
      assertTrue(example.containsKey("1"));
      assertEquals("ONE", example.get("1"));
      assertTrue(entryEvents.empty());

      String value = put(SEPARATOR + "Example", "2", "TWO");

      assertEquals("TWO", value);

      waitOnEvent(WAIT_TIME_MILLISECONDS);

      assertFalse(entryEvents.empty());

      EntryEvent entryEvent = (EntryEvent) entryEvents.pop();

      assertEquals("2", entryEvent.getKey());
      assertEquals("TWO", entryEvent.getNewValue());
      assertNull(entryEvent.getOldValue());
      assertEquals(2, example.size());
      assertTrue(example.containsKey("2"));
      assertEquals("TWO", example.get("2"));
    } finally {
      clientCache.close();
    }
  }

  protected class TestEntryCacheListener extends CacheListenerAdapter<String, String> {

    @Override
    public void afterCreate(final EntryEvent<String, String> event) {
      super.afterCreate(event);
      System.out.printf("Created entry with key (%1$s) and value (%2$s) in Region (%3$s)!",
          event.getKey(), event.getNewValue(), event.getRegion().getName());
      entryEvents.push(event);
    }
  }

}
