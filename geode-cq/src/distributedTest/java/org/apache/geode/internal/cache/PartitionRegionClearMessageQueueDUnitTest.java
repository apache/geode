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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.ClientCacheRule;

public class PartitionRegionClearMessageQueueDUnitTest {
  public static final String NAME = "testRegion";
  protected static MemberVM locator;
  protected static MemberVM server1, server2, server3, accessor;
  protected static List<MemberVM> servers;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(5);

  @ClassRule
  public static ClientCacheRule client = new ClientCacheRule();

  @BeforeClass
  public static void setUp() throws Exception {
    locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    server1 = cluster.startServerVM(1,
        s -> s.withConnectionToLocator(locatorPort).withRegion(RegionShortcut.PARTITION, NAME));
    server2 = cluster.startServerVM(2,
        s -> s.withConnectionToLocator(locatorPort).withRegion(RegionShortcut.PARTITION, NAME));
    server3 = cluster.startServerVM(3,
        s -> s.withConnectionToLocator(locatorPort).withRegion(RegionShortcut.PARTITION, NAME));
    accessor = cluster.startServerVM(4, s -> s.withConnectionToLocator(locatorPort)
        .withRegion(RegionShortcut.PARTITION_PROXY, NAME));

    servers = Arrays.asList(server1, server2, server3, accessor);

    client.withLocatorConnection(locatorPort).withPoolSubscription(true).createCache();
    client.getCache()
        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
        .create(NAME);
  }

  @Test
  public void clearMessageSentToClientWithRegisteredInterest() throws Exception {
    Region<Object, Object> testRegion = client.getCache().getRegion(NAME);

    assertThat(testRegion).isEmpty();
    server1.invoke(() -> addRecord(10));
    // verify that before register for interest, client region has no data
    assertThat(testRegion).hasSize(0);

    // verify that after register for interest, client region has data now.
    testRegion.registerInterestForAllKeys();
    assertThat(testRegion).hasSize(10);

    // do PR clear on a server that has no client proxy, hence no HARegionQueue
    Boolean cleared = false;
    for (int i = 0; i < servers.size(); i++) {
      cleared = servers.get(i).invoke(
          PartitionRegionClearMessageQueueDUnitTest::clearRegionOnNoClientProxyMember);
      if (cleared) {
        break;
      }
    }

    // verify that clear is called on a server
    assertThat(cleared).isTrue();
    // verify that the PR clear message is still delivered to the client
    assertThat(testRegion).hasSize(0);
  }

  private static void addRecord(int size) {
    Region<Object, Object> region = getTestRegion();
    for (int i = 0; i < size; i++) {
      region.put(i, "value" + i);
    }
  }

  @Test
  public void clearMessageSentToClientWithCQ() throws Exception {
    QueryService queryService = client.getCache().getDefaultPool().getQueryService();
    CqAttributesFactory cqaFactory = new CqAttributesFactory();
    AtomicBoolean clearEventReceived = new AtomicBoolean(false);
    cqaFactory.addCqListener(new CqListener() {
      @Override
      public void onEvent(CqEvent aCqEvent) {
        Operation baseOperation = aCqEvent.getBaseOperation();
        if (baseOperation.isClear()) {
          clearEventReceived.set(true);
        }
      }

      @Override
      public void onError(CqEvent aCqEvent) {}
    });

    CqQuery cqQuery =
        queryService.newCq("select * from /" + NAME, cqaFactory.create());
    cqQuery.execute();

    server1.invoke(() -> addRecord(10));

    Boolean cleared = false;
    for (int i = 0; i < servers.size(); i++) {
      cleared = servers.get(i).invoke(
          PartitionRegionClearMessageQueueDUnitTest::clearRegionOnNoClientProxyMember);
      if (cleared) {
        break;
      }
    }

    // verify that clear is called on a server
    assertThat(cleared).isTrue();
    // verify that the PR clear message is still delivered to the client
    assertThat(clearEventReceived.get()).isTrue();
  }

  private static boolean clearRegionOnNoClientProxyMember() {
    Collection<CacheClientProxy> clientProxies =
        CacheClientNotifier.getInstance().getClientProxies();
    if (clientProxies.isEmpty()) {
      getTestRegion().clear();
      return true;
    }
    return false;
  }

  private static Region<Object, Object> getTestRegion() {
    return getCache().getRegion("/" + NAME);
  }
}
