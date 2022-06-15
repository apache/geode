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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.api.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.IgnoredException;
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

  private final Stack<EntryEvent<String, String>> entryEvents = new Stack<>();

  private VM gemfireServerVm;

  @Override
  public final void postSetUp() {
    disconnectAllFromDS();
    setupGemFireCacheServer();
    IgnoredException.addIgnoredException("java.net.ConnectException");
  }

  @Override
  public final void preTearDown() {
    serverPort.set(0);
    entryEvents.clear();
    gemfireServerVm.invoke(() -> CacheFactory.getAnyInstance().close());
    gemfireServerVm = null;
  }

  private void setupGemFireCacheServer() {


    gemfireServerVm = VM.getVM(0);
    serverPort.set(AvailablePortHelper.getRandomAvailableTCPPort());

    gemfireServerVm.invoke(() -> {
      try {
        Cache cache = new CacheFactory()
            .set("name", "ClientServerRegisterInterestsTestGemFireServer").set(MCAST_PORT, "0")
            .set(LOG_FILE, "clientServerRegisterInterestsTest.log").set(LOG_LEVEL, "config")
            .create();

        RegionFactory<String, String> regionFactory = cache.createRegionFactory();

        regionFactory.setDataPolicy(DataPolicy.REPLICATE);
        regionFactory.setKeyConstraint(String.class);
        regionFactory.setValueConstraint(String.class);

        Region<String, String> example = regionFactory.create("Example");

        assertThat(example)
            .describedAs("The 'Example' Region was not properly configured and initialized!")
            .isNotNull();
        assertThat(example.getFullPath()).isEqualTo(SEPARATOR + "Example");
        assertThat(example.getName()).isEqualTo("Example");
        assertThat(example).isEmpty();

        example.put("1", "ONE");

        assertThat(example).isNotEmpty();
        assertThat(example).hasSize(1);

        CacheServer cacheServer = cache.addCacheServer();

        cacheServer.setPort(serverPort.get());
        cacheServer.setMaxConnections(10);

        ClientSubscriptionConfig clientSubscriptionConfig =
            cacheServer.getClientSubscriptionConfig();

        clientSubscriptionConfig.setCapacity(100);
        clientSubscriptionConfig.setEvictionPolicy("entry");

        cacheServer.start();

        assertThat(cacheServer.isRunning()).describedAs("Cache Server is not running!").isTrue();
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(String.format(
            "Failed to start the GemFire Cache Server listening on port (%1$d) due to IO error!",
            serverPort.get()), e);
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

    assertThat(pool)
        .describedAs("The 'serverConnectionPool' was not properly configured and initialized!")
        .isNotNull();

    ClientRegionFactory<String, String> regionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    regionFactory.addCacheListener(new TestEntryCacheListener());
    regionFactory.setPoolName(pool.getName());
    regionFactory.setKeyConstraint(String.class);
    regionFactory.setValueConstraint(String.class);

    Region<String, String> exampleCachingProxy = regionFactory.create("Example");

    assertThat(exampleCachingProxy)
        .describedAs("The 'Example' Client Region was not properly configured and initialized")
        .isNotNull();

    clientCache.readyForEvents();

    exampleCachingProxy.registerInterest("ALL_KEYS", InterestResultPolicy.DEFAULT, false, true);

    return clientCache;
  }

  protected <V> V put() {
    return (V) gemfireServerVm.invoke(() -> {
      Cache cache = CacheFactory.getAnyInstance();
      cache.getRegion("/Example").put("2", "TWO");
      return cache.getRegion("/Example").get("2");
    });
  }

  protected void waitOnEvent() {
    final long timeout = (System.currentTimeMillis()
        + ClientServerRegisterInterestsDUnitTest.WAIT_TIME_MILLISECONDS);

    while (entryEvents.empty() && (System.currentTimeMillis() < timeout)) {
      synchronized (this) {
        try {
          TimeUnit.MILLISECONDS.timedWait(this, Math.min(500,
              ClientServerRegisterInterestsDUnitTest.WAIT_TIME_MILLISECONDS));
        } catch (InterruptedException ignore) {
        }
      }
    }
  }

  @Test
  public void testClientRegisterInterests() {

    try (ClientCache clientCache = setupGemFireClientCache()) {
      Region<String, String> example = clientCache.getRegion(SEPARATOR + "Example");

      assertThat(example).describedAs("'Example' Region in Client Cache was not found!")
          .isNotNull();
      assertThat(example).containsOnly(entry("1", "ONE"));
      assertThat(entryEvents).isEmpty();

      String value = put();

      assertThat(value).isEqualTo("TWO");

      waitOnEvent();

      assertThat(entryEvents).isNotEmpty();

      EntryEvent<String, String> entryEvent = entryEvents.pop();

      assertThat(entryEvent.getKey()).isEqualTo("2");
      assertThat(entryEvent.getNewValue()).isEqualTo("TWO");
      assertThat(entryEvent.getOldValue()).isNull();
      assertThat(example).hasSize(2);
      assertThat(example).contains(entry("2", "TWO"));
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
