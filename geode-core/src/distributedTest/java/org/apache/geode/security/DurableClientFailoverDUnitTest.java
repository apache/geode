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

package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.QueueConnectionImpl;
import org.apache.geode.cache.query.dunit.SecurityTestUtils.KeysCacheListener;
import org.apache.geode.test.concurrent.FileBasedCountDownLatch;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ClientCacheRule;

public class DurableClientFailoverDUnitTest {
  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Before
  public void setup() {
    locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    server1 =
        cluster.startServerVM(1, s -> s.withRegion(RegionShortcut.REPLICATE_PERSISTENT, "region")
            .withConnectionToLocator(locatorPort));
    server2 =
        cluster.startServerVM(2, s -> s.withRegion(RegionShortcut.REPLICATE_PERSISTENT, "region")
            .withConnectionToLocator(locatorPort));
  }

  @Test
  public void durableClientGetAllEvents() throws Exception {
    String clientId = "client0";
    clientCacheRule
        .withCacheSetup(cf -> cf.setPoolSubscriptionEnabled(true).setPoolSubscriptionRedundancy(2))
        .withProperty(DURABLE_CLIENT_ID, clientId)
        .withLocatorConnection(locator.getPort());
    ClientCache clientCache = clientCacheRule.createCache();

    KeysCacheListener mylistener = new KeysCacheListener();
    Region<Object, Object> clientRegion =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY)
            .addCacheListener(mylistener).create("region");
    clientRegion.registerInterestForAllKeys(InterestResultPolicy.NONE, true);
    clientCache.readyForEvents();

    // find out which server is the primary subscription end point, server1 or server2
    boolean server1IsPrimary = false;
    PoolImpl pool = (PoolImpl) PoolManager.find(clientRegion.getAttributes().getPoolName());
    QueueConnectionImpl connection = (QueueConnectionImpl) pool.getPrimaryConnection();
    if (connection.toString().contains("server-1")) {
      server1IsPrimary = true;
    }

    // use server3 to input data constantly
    int locatorPort = locator.getPort();
    server3 =
        cluster.startServerVM(3, s -> s.withRegion(RegionShortcut.REPLICATE_PERSISTENT, "region")
            .withConnectionToLocator(locatorPort));

    FileBasedCountDownLatch latch = new FileBasedCountDownLatch(1);
    int size = 100;
    AsyncInvocation<Void> putAsync = server3.invokeAsync(() -> {
      Region<Object, Object> region = ClusterStartupRule.getCache().getRegion("region");
      for (int i = 0; i < size; i++) {
        if (i == size / 2) {
          latch.countDown();
        }
        region.put("key" + i, "value" + i);
      }
    });

    // wait till puts halfway to stop the primary server
    latch.await();
    if (server1IsPrimary) {
      server1.stop();
    } else {
      server2.stop();
    }
    putAsync.await();

    // make sure client still gets all the events
    await().atMost(20, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(mylistener.keys).hasSize(size));
  }
}
