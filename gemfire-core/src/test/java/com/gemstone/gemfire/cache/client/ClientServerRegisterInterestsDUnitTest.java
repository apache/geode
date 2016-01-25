/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ClientSubscriptionConfig;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * The ClientServerRegisterInterestsDUnitTest class is a test suite of test cases testing the interaction between a
 * client and a server in a Register Interests scenario.
 *
 * @author John Blum
 * @see com.gemstone.gemfire.test.dunit.DistributedTestCase
 * @since 8.0
 */
public class ClientServerRegisterInterestsDUnitTest extends DistributedTestCase {

  protected static final long WAIT_TIME_MILLISECONDS = TimeUnit.SECONDS.toMillis(5);

  protected static final String LOCALHOST = "localhost";

  private final AtomicInteger serverPort = new AtomicInteger(0);

  private final Stack entryEvents = new Stack();

  private VM gemfireServerVm;

  public ClientServerRegisterInterestsDUnitTest(final String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    setupGemFireCacheServer();
    addExpectedException("java.net.ConnectException");
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    serverPort.set(0);
    entryEvents.clear();
    gemfireServerVm.invoke(new SerializableRunnable() {
      @Override public void run() {
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
      @Override public void run() {
        try {
          Cache cache = new CacheFactory()
            .set("name", "ClientServerRegisterInterestsTestGemFireServer")
            .set("mcast-port", "0")
            .set("log-file", "clientServerRegisterInterestsTest.log")
            .set("log-level", "config")
            //.set("jmx-manager", "true")
            //.set("jmx-manager-http-port", "0")
            //.set("jmx-manager-port", "1199")
            //.set("jmx-manager-start", "true")
            .create();

          RegionFactory<String, String> regionFactory = cache.createRegionFactory();

          regionFactory.setDataPolicy(DataPolicy.REPLICATE);
          regionFactory.setKeyConstraint(String.class);
          regionFactory.setValueConstraint(String.class);

          Region<String, String> example = regionFactory.create("Example");

          assertNotNull("The 'Example' Region was not properly configured and initialized!", example);
          assertEquals("/Example", example.getFullPath());
          assertEquals("Example", example.getName());
          assertTrue(example.isEmpty());

          example.put("1", "ONE");

          assertFalse(example.isEmpty());
          assertEquals(1, example.size());

          CacheServer cacheServer = cache.addCacheServer();

          cacheServer.setPort(serverPort.get());
          cacheServer.setMaxConnections(10);

          ClientSubscriptionConfig clientSubscriptionConfig = cacheServer.getClientSubscriptionConfig();

          clientSubscriptionConfig.setCapacity(100);
          clientSubscriptionConfig.setEvictionPolicy("entry");

          cacheServer.start();

          assertTrue("Cache Server is not running!", cacheServer.isRunning());
        }
        catch (UnknownHostException ignore) {
          throw new RuntimeException(ignore);
        }
        catch (IOException e) {
          throw new RuntimeException(String.format(
            "Failed to start the GemFire Cache Server listening on port (%1$d) due to IO error!",
              serverPort.get()), e);
        }
      }
    });
  }

  private ClientCache setupGemFireClientCache() {
    ClientCache clientCache = new ClientCacheFactory()
      .set("durable-client-id", "TestDurableClientId")
      .create();

    PoolFactory poolFactory = PoolManager.createFactory();

    poolFactory.setMaxConnections(10);
    poolFactory.setMinConnections(1);
    poolFactory.setReadTimeout(5000);
    poolFactory.setSubscriptionEnabled(true);
    poolFactory.addServer(LOCALHOST, serverPort.get());

    Pool pool = poolFactory.create("serverConnectionPool");

    assertNotNull("The 'serverConnectionPool' was not properly configured and initialized!", pool);

    ClientRegionFactory<String, String> regionFactory = clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    regionFactory.addCacheListener(new TestEntryCacheListener());
    regionFactory.setPoolName(pool.getName());
    regionFactory.setKeyConstraint(String.class);
    regionFactory.setValueConstraint(String.class);

    Region<String, String> exampleCachingProxy = regionFactory.create("Example");

    assertNotNull("The 'Example' Client Region was not properly configured and initialized", exampleCachingProxy);

    clientCache.readyForEvents();

    exampleCachingProxy.registerInterest("ALL_KEYS", InterestResultPolicy.DEFAULT, false, true);

    return clientCache;
  }

  @SuppressWarnings("unchecked")
  protected <K, V> V put(final String regionName, final K key, final V value) {
    return (V) gemfireServerVm.invoke(new SerializableCallable() {
      @Override public Object call() throws Exception {
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
        }
        catch (InterruptedException ignore) {
        }
      }
    }
  }

  public void testClientRegisterInterests() {
    ClientCache clientCache = setupGemFireClientCache();

    try {
      Region<String, String> example = clientCache.getRegion("/Example");

      assertNotNull("'Example' Region in Client Cache was not found!", example);
      assertEquals(1, example.size());
      assertTrue(example.containsKey("1"));
      assertEquals("ONE", example.get("1"));
      assertTrue(entryEvents.empty());

      String value = put("/Example", "2", "TWO");

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
    }
    finally {
      clientCache.close();
    }
  }

  protected final class TestEntryCacheListener extends CacheListenerAdapter<String, String> {

    @Override
    public void afterCreate(final EntryEvent<String, String> event) {
      super.afterCreate(event);
      System.out.printf("Created entry with key (%1$s) and value (%2$s) in Region (%3$s)!", event.getKey(),
        event.getNewValue(), event.getRegion().getName());
      entryEvents.push(event);
    }
  }

}
