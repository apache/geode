/*
 *
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
 *
 */
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.Endpoint;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.UpdatableUserAuthInitialize;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.SerializableFunction;
import org.apache.geode.test.junit.rules.ClientCacheRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class SlowDispatcherDUnitTest {
  public static final String PARTITION_REGION = "partitionRegion";
  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Before
  public void setup() {
    locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    SerializableFunction<ServerStarterRule> serverStartRule =
        s -> s.withRegion(RegionShortcut.PARTITION, PARTITION_REGION)
            .withSystemProperty("slowStartTimeForTesting", "10000")
            .withConnectionToLocator(locatorPort);
    server1 = cluster.startServerVM(1, serverStartRule);
    server2 = cluster.startServerVM(2, serverStartRule);

    MemberVM.invokeInEveryMember(() -> {
      CacheClientProxy.isSlowStartForTesting = true;
    }, server1, server2);
  }

  private static KeyValueCacheListener myListener;

  @Test
  public void registeredInterest_durableClient_kill_primarySever_receives_marker()
      throws Exception {
    int locatorPort = locator.getPort();
    int server1Port = server1.getPort();
    int server2Port = server2.getPort();
    ClientVM clientVM = cluster.startClientVM(3,
        c -> c.withProperty(DURABLE_CLIENT_ID, "123456")
            .withCacheSetup(
                a -> a.setPoolSubscriptionRedundancy(2).setPoolSubscriptionEnabled(true)
                    .setPoolMinConnections(2))
            .withServerConnection(server1Port, server2Port));

    clientVM.invoke(() -> {
      myListener = new KeyValueCacheListener();
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> clientRegion =
          clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
              .addCacheListener(myListener)
              .create(PARTITION_REGION);
      clientRegion.registerInterestForAllKeys(InterestResultPolicy.KEYS, true);
      clientCache.readyForEvents();
    });

    // create another client to do puts
    ClientVM putClient = cluster.startClientVM(4,
        c -> c.withProperty(SECURITY_CLIENT_AUTH_INIT, UpdatableUserAuthInitialize.class.getName())
            .withPoolSubscription(false)
            .withLocatorConnection(locatorPort));
    AsyncInvocation putAsync = putClient.invokeAsync(() -> {
      UpdatableUserAuthInitialize.setUser("putter");
      Region<Object, Object> proxyRegion =
          ClusterStartupRule.clientCacheRule.createProxyRegion(PARTITION_REGION);
      IntStream.range(0, 50).forEach(i -> proxyRegion.put("key", i));
    });

    Boolean server1IsPrimary = clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Endpoint endpoint =
          ((PoolImpl) clientCache.getDefaultPool()).getPrimaryConnection().getEndpoint();
      return endpoint.toString().contains("server-1");
    });

    MemberVM primary;
    if (server1IsPrimary) {
      primary = server1;
    } else {
      primary = server2;
    }

    primary.stop();
    putAsync.await();

    // make sure the client still gets all the events
    clientVM.invoke(() -> {
      Region clientRegion = ClusterStartupRule.getClientCache().getRegion(PARTITION_REGION);
      await().untilAsserted(() -> assertThat(myListener.updateEventCount).isEqualTo(49));
      assertThat(myListener.markerEventCount).isEqualTo(1);
      assertThat(clientRegion.getEntry("key").getValue()).isEqualTo(49);
    });

  }

  public static class KeyValueCacheListener extends CacheListenerAdapter<Object, Object> {
    private static Logger logger = LogService.getLogger();
    public int updateEventCount;
    public int markerEventCount;

    @Override
    public void afterUpdate(EntryEvent event) {
      logger.info("Jinmei: got event {}", event.getOperation().toString());
      updateEventCount++;
    }

    public void afterRegionLive(RegionEvent event) {
      markerEventCount++;
      logger.info("Jinmei: got event {}", event.getOperation().toString());
    }
  }
}
