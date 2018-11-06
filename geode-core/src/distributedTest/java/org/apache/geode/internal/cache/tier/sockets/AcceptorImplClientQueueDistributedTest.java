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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(ClientServerTest.class)
@SuppressWarnings("serial")
public class AcceptorImplClientQueueDistributedTest implements Serializable {

  private static final int NUMBER_OF_ENTRIES = 200;

  private String hostName;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = CacheRule.builder()
      .addSystemProperty("BridgeServer.HANDSHAKE_POOL_SIZE", "1").build();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTestName name = new SerializableTestName();

  @Rule
  public SerializableTemporaryFolder tempDir = new SerializableTemporaryFolder();

  @Before
  public void setUp() throws Exception {
    hostName = getHostName();

    getVM(0).invoke(() -> cacheRule.createCache());
    getVM(1).invoke(() -> cacheRule.createCache());
  }

  @After
  public void tearDown() {
    getAllVMs().forEach(vm -> vm.invoke(() -> {
      InitialImageOperation.slowImageProcessing = 0;
    }));
  }

  @Test
  public void clientSubscriptionQueueInitializationShouldNotBlockNewConnections() throws Exception {
    VM vm0 = getVM(0);
    VM vm1 = getVM(1);
    VM vm2 = getVM(2);
    VM vm3 = getVM(3);

    // Start one server
    int vm0_port = vm0.invoke("Start server with subscription turned on",
        () -> createSubscriptionServer(cacheRule.getCache()));


    // Create a durable queue and shutdown the client
    vm2.invoke("Start Client1 with durable interest registration turned on", () -> {
      ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
      clientCacheFactory.setPoolSubscriptionEnabled(true);
      clientCacheFactory.setPoolSubscriptionRedundancy(1);
      clientCacheFactory.setPoolReadTimeout(200);
      clientCacheFactory.addPoolServer(hostName, vm0_port);
      ClientCache cache = clientCacheFactory.set("durable-client-id", "1")
          .set("durable-client-timeout", "300").set("mcast-port", "0").create();
      ClientRegionFactory<Object, Object> clientRegionFactory =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region region = clientRegionFactory.create("subscriptionRegion");

      region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      cache.readyForEvents();
      cache.close(true);
    });

    // Add some entries Which will end up the in the queue
    vm3.invoke("Start Client2 to add entries to region", () -> {
      ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
      clientCacheFactory.addPoolServer(hostName, vm0_port);
      ClientCache cache = clientCacheFactory.set("mcast-port", "0").create();
      ClientRegionFactory<Object, Object> clientRegionFactory =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region region = clientRegionFactory.create("subscriptionRegion");

      for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
        region.put(i, i);
      }
      cache.close();
    });

    // Start a second server
    int vm1_port = vm1.invoke("Start server2 in with subscriptions turned on", () -> {
      try {
        int serverPort = createSubscriptionServer(cacheRule.getCache());
        InitialImageOperation.slowImageProcessing = 500;
        return serverPort;
      } catch (IOException e) {
        return 0;
      }
    });

    // Make copying the queue slow
    vm0.invoke("Turn on slow image processing", () -> {
      InitialImageOperation.slowImageProcessing = 500;
    });

    // Restart the durable client, which will try to make a copy of the queue, which will
    // take a long time because we made it slow
    AsyncInvocation<Boolean> completedClient1 =
        vm2.invokeAsync("Start Client1, expecting durable messages to be delivered", () -> {

          ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
          clientCacheFactory.setPoolSubscriptionEnabled(true);
          clientCacheFactory.setPoolSubscriptionRedundancy(1);
          clientCacheFactory.setPoolMinConnections(1);
          clientCacheFactory.setPoolMaxConnections(1);
          clientCacheFactory.addPoolServer(hostName, vm1_port);
          ClientCacheFactory cacheFactory = clientCacheFactory.set("durable-client-id", "1")
              .set("durable-client-timeout", "300").set("mcast-port", "0");
          ClientCache cache = cacheFactory.create();

          ClientRegionFactory<Object, Object> clientRegionFactory =
              cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
          AtomicInteger eventCount = new AtomicInteger(0);

          Region<?, ?> region =
              clientRegionFactory.addCacheListener(new CacheListenerAdapter<Object, Object>() {
                @Override
                public void afterCreate(EntryEvent event) {
                  eventCount.incrementAndGet();
                }

                @Override
                public void afterUpdate(EntryEvent event) {
                  eventCount.incrementAndGet();
                }
              }).create("subscriptionRegion");

          region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
          cache.readyForEvents();
          await()
              .until(() -> eventCount.get() == NUMBER_OF_ENTRIES);
          cache.close();
          return eventCount.get() == NUMBER_OF_ENTRIES;
        });

    // TODO: replace sleep with Awaitility
    Thread.sleep(500);

    // Start a second client, which should not be blocked by the queue copying
    vm3.invoke("Start Client2 to add entries to region", () -> {
      ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
      clientCacheFactory.setPoolRetryAttempts(0);
      clientCacheFactory.setPoolMinConnections(1);
      clientCacheFactory.setPoolMaxConnections(1);
      clientCacheFactory.setPoolSocketConnectTimeout(5000);
      clientCacheFactory.addPoolServer(hostName, vm1_port);
      ClientCache cache = clientCacheFactory.set("mcast-port", "0").create();
      ClientRegionFactory<Integer, Integer> clientRegionFactory =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<Integer, Integer> region = clientRegionFactory.create("subscriptionRegion");

      for (int i = 0; i < 100; i++) {
        assertThat(region.get(i)).isGreaterThanOrEqualTo(0);
      }
      cache.close();
    });

    // Make copying the queue slow
    turnOffSlowImageProcessing(vm0);
    turnOffSlowImageProcessing(vm1);

    assertThat(completedClient1.get()).isTrue();
  }

  private void turnOffSlowImageProcessing(VM vm0) {
    vm0.invoke("Turn off slow image processing", () -> {
      InitialImageOperation.slowImageProcessing = 0;
    });
  }

  private int createSubscriptionServer(InternalCache cache) throws IOException {
    initializeDiskStore(cache);
    initializeReplicateRegion(cache);
    return initializeCacheServerWithSubscription(cache);
  }

  private void initializeDiskStore(InternalCache cache) throws IOException {
    DiskStoreAttributes diskStoreAttributes = new DiskStoreAttributes();
    diskStoreAttributes.name = "clientQueueDS";
    diskStoreAttributes.diskDirs = new File[] {tempDir.newFolder(name + "_dir")};
    cache.createDiskStoreFactory(diskStoreAttributes).create("clientQueueDS");
  }

  private void initializeReplicateRegion(InternalCache cache) {
    cache.createRegionFactory(RegionShortcut.REPLICATE).setStatisticsEnabled(true)
        .setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL))
        .create("subscriptionRegion");
  }

  private int initializeCacheServerWithSubscription(InternalCache cache) throws IOException {
    CacheServer cacheServer1 = cache.addCacheServer(false);
    ClientSubscriptionConfig clientSubscriptionConfig = cacheServer1.getClientSubscriptionConfig();
    clientSubscriptionConfig.setEvictionPolicy("entry");
    clientSubscriptionConfig.setCapacity(5);
    clientSubscriptionConfig.setDiskStoreName("clientQueueDS");
    cacheServer1.setPort(0);
    cacheServer1.setHostnameForClients(hostName);
    cacheServer1.start();
    return cacheServer1.getPort();
  }
}
