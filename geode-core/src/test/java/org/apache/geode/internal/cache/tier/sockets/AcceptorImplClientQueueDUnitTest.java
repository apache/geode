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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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
import org.apache.geode.distributed.DistributedLockBlackboard;
import org.apache.geode.distributed.DistributedLockBlackboardImpl;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.dunit.rules.SharedCountersRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(DistributedTest.class)
public class AcceptorImplClientQueueDUnitTest implements Serializable {
  private final Host host = Host.getHost(0);
  private static final int numberOfEntries = 200;
  private static final AtomicInteger eventCount = new AtomicInteger(0);
  private static final AtomicBoolean completedClient2 = new AtomicBoolean(false);

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public CacheRule cacheRule =
      CacheRule.builder().createCacheIn(host.getVM(0)).createCacheIn(host.getVM(1))
          .addSystemProperty("BridgeServer.HANDSHAKE_POOL_SIZE", "1").build();

  @Rule
  public SerializableTestName name = new SerializableTestName();

  @Rule
  public SerializableTemporaryFolder tempDir = new SerializableTemporaryFolder();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  private DistributedLockBlackboard blackboard = null;

  @Before
  public void setup() throws Exception {
    blackboard = DistributedLockBlackboardImpl.getInstance();
  }

  @After
  public void tearDown() throws RemoteException {
    blackboard.initCount();
    host.getAllVMs().forEach((vm) -> vm.invoke(() -> {
      InitialImageOperation.slowImageProcessing = 0;
      System.getProperties().remove("BridgeServer.HANDSHAKE_POOL_SIZE");
    }));
  }

  @Test
  public void testClientSubscriptionQueueBlockingConnectionInitialization() throws Exception {
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    int vm0_port = vm0.invoke("Start server with subscription turned on", () -> {
      try {
        return createSubscriptionServer(cacheRule.getCache());
      } catch (IOException e) {
        return 0;
      }
    });

    vm2.invoke("Start Client1 with durable interest registration turned on", () -> {
      ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
      clientCacheFactory.setPoolSubscriptionEnabled(true);
      clientCacheFactory.setPoolSubscriptionRedundancy(1);
      clientCacheFactory.setPoolReadTimeout(200);
      clientCacheFactory.addPoolServer(host.getHostName(), vm0_port);
      ClientCache cache = clientCacheFactory.set("durable-client-id", "1")
          .set("durable-client-timeout", "300").set("mcast-port", "0").create();
      ClientRegionFactory<Object, Object> clientRegionFactory =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region region = clientRegionFactory.create("subscriptionRegion");

      region.registerInterestRegex(".*", InterestResultPolicy.NONE, true);
      cache.readyForEvents();
      cache.close(true);
    });
    vm3.invoke("Start Client2 to add entries to region", () -> {
      ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
      clientCacheFactory.addPoolServer(host.getHostName(), vm0_port);
      ClientCache cache = clientCacheFactory.set("mcast-port", "0").create();
      ClientRegionFactory<Object, Object> clientRegionFactory =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region region = clientRegionFactory.create("subscriptionRegion");

      for (int i = 0; i < numberOfEntries; i++) {
        region.put(i, i);
      }
      cache.close();
    });

    int vm1_port = vm1.invoke("Start server2 in with subscriptions turned on", () -> {
      try {
        int serverPort = createSubscriptionServer(cacheRule.getCache());
        InitialImageOperation.slowImageProcessing = 30;
        return serverPort;
      } catch (IOException e) {
        return 0;
      }
    });

    vm0.invoke("Turn on slow image processsing", () -> {
      InitialImageOperation.slowImageProcessing = 30;
    });

    AsyncInvocation<Boolean> completedClient1 =
        vm2.invokeAsync("Start Client1, expecting durable messages to be delivered", () -> {

          ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
          clientCacheFactory.setPoolSubscriptionEnabled(true);
          clientCacheFactory.setPoolSubscriptionRedundancy(1);
          clientCacheFactory.setPoolMinConnections(1);
          clientCacheFactory.setPoolMaxConnections(1);
          clientCacheFactory.setPoolReadTimeout(200);
          clientCacheFactory.addPoolServer(host.getHostName(), vm1_port);
          ClientCacheFactory cacheFactory = clientCacheFactory.set("durable-client-id", "1")
              .set("durable-client-timeout", "300").set("mcast-port", "0");
          blackboard.incCount();
          ClientCache cache = cacheFactory.create();

          ClientRegionFactory<Object, Object> clientRegionFactory =
              cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
          Region region = clientRegionFactory.addCacheListener(new CacheListenerAdapter() {
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
          Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
              .until(() -> eventCount.get() == numberOfEntries);
          cache.close();
          return eventCount.get() == numberOfEntries;
        });

    vm3.invokeAsync("Start Client2 to add entries to region", () -> {
      while (true) {
        Thread.sleep(100);
        if (blackboard.getCount() == 1) {
          break;
        }
      }
      ClientCache cache = null;
      ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
      clientCacheFactory.setPoolRetryAttempts(0);
      clientCacheFactory.setPoolMinConnections(1);
      clientCacheFactory.setPoolMaxConnections(1);
      clientCacheFactory.setPoolReadTimeout(200);
      clientCacheFactory.setPoolSocketConnectTimeout(500);
      clientCacheFactory.addPoolServer(host.getHostName(), vm1_port);
      cache = clientCacheFactory.set("mcast-port", "0").create();
      ClientRegionFactory<Object, Object> clientRegionFactory =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region region = clientRegionFactory.create("subscriptionRegion");

      int returnValue = 0;
      for (int i = 0; i < 100; i++) {
        returnValue = (int) region.get(i);
      }
      cache.close();
      completedClient2.set(returnValue == 99);
    });
    assertTrue(completedClient1.get());
    assertTrue(vm3.invoke(() -> completedClient2.get()));
  }

  private int createSubscriptionServer(InternalCache cache) throws IOException {
    initializeDiskStore(cache);
    initializeReplicateRegion(cache);
    return initializeCacheServerWithSubscription(host, cache);
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

  private int initializeCacheServerWithSubscription(Host host, InternalCache cache)
      throws IOException {
    CacheServer cacheServer1 = cache.addCacheServer(false);
    ClientSubscriptionConfig clientSubscriptionConfig = cacheServer1.getClientSubscriptionConfig();
    clientSubscriptionConfig.setEvictionPolicy("entry");
    clientSubscriptionConfig.setCapacity(5);
    clientSubscriptionConfig.setDiskStoreName("clientQueueDS");
    cacheServer1.setPort(0);
    cacheServer1.setHostnameForClients(host.getHostName());
    cacheServer1.start();
    return cacheServer1.getPort();
  }
}
