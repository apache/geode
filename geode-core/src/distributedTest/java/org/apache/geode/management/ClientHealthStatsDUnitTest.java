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

package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.VMProvider;

public class ClientHealthStatsDUnitTest implements Serializable {
  private static final int NUMBER_PUTS = 100;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private MemberVM locator;
  private MemberVM server;
  private ClientVM client1;
  private ClientVM client2;

  private static volatile boolean lastKeyReceived;

  @Before
  public void before() {
    locator =
        cluster.startLocatorVM(0, r -> r.withoutClusterConfigurationService());
    server = cluster.startServerVM(1, s -> s.withRegion(RegionShortcut.REPLICATE, "regionA")
        .withConnectionToLocator(locator.getPort()));
  }

  @Test
  public void testClientHealthStats_SubscriptionEnabled() throws Exception {
    client1 = cluster.startClientVM(2, true, server.getPort());
    client2 = cluster.startClientVM(3, true, server.getPort());

    VMProvider.invokeInEveryMember(() -> {
      ClientRegionFactory<String, String> regionFactory =
          ClusterStartupRule.getClientCache()
              .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
      Region<String, String> region = regionFactory.create("regionA");
      // need to do some operation in order for the clients to be registered in mBean
      region.put("1", "1");
    }, client1, client2);

    locator.waitTillClientsAreReadyOnServers(server.getName(), server.getPort(), 2);
    verifyClientsAndSubscription(2);
  }

  @Test
  public void testClientHealthStats_SubscriptionDisabled() throws Exception {
    client1 = cluster.startClientVM(2, false, server.getPort());
    client2 = cluster.startClientVM(3, false, server.getPort());
    VMProvider.invokeInEveryMember(() -> {
      ClientRegionFactory<String, String> regionFactory =
          ClusterStartupRule.getClientCache()
              .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
      regionFactory.create("regionA");
    }, client1, client2);

    locator.waitTillClientsAreReadyOnServers(server.getName(), server.getPort(), 2);
    verifyClientsAndSubscription(0);
  }

  @Test
  public void testClientHealthStats_DurableClient() throws Exception {
    client1 = createDurableClient(2);
    client2 = createDurableClient(3);

    locator.waitTillClientsAreReadyOnServers(server.getName(), server.getPort(), 2);
    VMProvider.invokeInEveryMember(() -> ClusterStartupRule.getClientCache().close(true), client1,
        client2);
    verifyClientsAndSubscription(2);
  }

  @Test
  public void testStatsMatchWithSize() throws Exception {
    // create durable client, with durable RI
    client1 = createDurableClient(2);

    // do puts in server
    server.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getCache().getRegion("/regionA");

      Thread thread1 = new Thread(() -> {
        for (int i = 0; i < NUMBER_PUTS; i++) {
          region.put("T1_KEY_" + i, "VALUE_" + i);
        }
      });
      Thread thread2 = new Thread(() -> {
        for (int i = 0; i < NUMBER_PUTS; i++) {
          region.put("T2_KEY_" + i, "VALUE_" + i);
        }
      });
      Thread thread3 = new Thread(() -> {
        for (int i = 0; i < NUMBER_PUTS; i++) {
          region.put("T3_KEY_" + i, "VALUE_" + i);
        }
      });

      thread1.start();
      thread2.start();
      thread3.start();

      thread1.join();
      thread2.join();
      thread3.join();
    });

    // close durable client1
    client1.invoke(() -> ClusterStartupRule.getClientCache().close(true));
    server.waitTillCacheClientProxyHasBeenPaused();

    // resume puts on serverVM, add another 100.
    server.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getCache().getRegion("/regionA");
      for (int i = 0; i < NUMBER_PUTS; i++) {
        region.put("NEWKEY_" + i, "NEWVALUE_" + i);
      }
      region.put("last_key", "last_value");
    });

    // start durable client1 again
    client1 = createDurableClient(2);
    // wait for full queue dispatch
    client1.invoke(() -> await().until(() -> lastKeyReceived));

    // verify the stats
    server.invoke(() -> {
      ManagementService service = ClusterStartupRule.memberStarter.getManagementService();
      CacheServerMXBean cacheServerMXBean = service.getLocalCacheServerMXBean(server.getPort());

      CacheClientNotifier clientNotifier = CacheClientNotifier.getInstance();
      CacheClientProxy clientProxy = clientNotifier.getClientProxies().iterator().next();
      assertThat(clientProxy.getQueueSizeStat()).isEqualTo(clientProxy.getQueueSize());

      ClientQueueDetail queueDetails = cacheServerMXBean.showClientQueueDetails()[0];
      assertThat((int) queueDetails.getQueueSize()).isEqualTo(clientProxy.getQueueSizeStat());
    });
  }

  private ClientVM createDurableClient(int index) throws Exception {
    ClientVM client = cluster.startClientVM(index, ccf -> {
      ccf.setPoolSubscriptionEnabled(true);
      ccf.addPoolServer("localhost", server.getPort());
      ccf.set(DURABLE_CLIENT_ID, "client" + index);
      ccf.set(DURABLE_CLIENT_TIMEOUT, "" + 30000);
    });

    client.invoke(() -> {
      ClientRegionFactory<String, String> regionFactory = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
      regionFactory.setConcurrencyChecksEnabled(false);

      regionFactory.addCacheListener(new CacheListenerAdapter<String, String>() {
        @Override
        public void afterCreate(final EntryEvent<String, String> event) {
          if ("last_key".equals(event.getKey())) {
            lastKeyReceived = true;
          }
        }
      });
      Region<String, String> region = regionFactory.create("regionA");
      region.registerInterest("ALL_KEYS", true);
      ClusterStartupRule.getClientCache().readyForEvents();
    });
    return client;
  }

  private void verifyClientsAndSubscription(int subscriptionCount) {
    locator.invoke(() -> {
      CacheServerMXBean bean =
          ClusterStartupRule.memberStarter.getCacheServerMXBean(server.getName(), server.getPort());
      String[] clientIds = bean.getClientIds();

      ClientHealthStatus[] clientStatuses = bean.showAllClientStats();
      assertThat(clientStatuses).isNotNull().hasSize(2);

      ClientHealthStatus clientStatus1 = bean.showClientStats(clientIds[0]);
      ClientHealthStatus clientStatus2 = bean.showClientStats(clientIds[1]);
      assertThat(clientStatus1).isNotNull();
      assertThat(clientStatus2).isNotNull();

      DistributedSystemMXBean dsBean =
          ClusterStartupRule.memberStarter.getManagementService().getDistributedSystemMXBean();
      assertThat(dsBean.getNumClients()).isEqualTo(2);
      assertThat(dsBean.getNumSubscriptions()).isEqualTo(subscriptionCount);
    });
  }
}
