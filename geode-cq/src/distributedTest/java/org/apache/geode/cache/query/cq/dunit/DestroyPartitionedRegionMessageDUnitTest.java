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
package org.apache.geode.cache.query.cq.dunit;


import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.rules.VMProvider;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
@Category(ClientSubscriptionTest.class)
public class DestroyPartitionedRegionMessageDUnitTest implements Serializable {
  private MemberVM server1, server2;
  private TestCqListener testListener;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Before
  public void before() throws Exception {
    MemberVM locator = clusterStartupRule.startLocatorVM(0, new Properties());
    Integer locator1Port = locator.getPort();
    server1 = clusterStartupRule.startServerVM(1, locator1Port);
    server2 = clusterStartupRule.startServerVM(2, locator1Port);

    VMProvider.invokeInEveryMember(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      cache.createRegionFactory(RegionShortcut.PARTITION).create("region");
    }, server1, server2);

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    clientCacheFactory.addPoolServer("localhost", server1.getPort());
    clientCacheFactory.setPoolSubscriptionEnabled(true);
    ClientCache clientCache = clientCacheFactory.create();
    clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("region");

    QueryService queryService = clientCache.getQueryService();
    CqAttributesFactory cqaf = new CqAttributesFactory();
    testListener = new TestCqListener();
    cqaf.addCqListener(testListener);
    CqAttributes cqAttributes = cqaf.create();

    queryService.newCq("Select * from " + SEPARATOR + "region r where r.ID + 3 > 4", cqAttributes)
        .execute();
  }

  @Test
  @Parameters({"1", "2"})
  @TestCaseName("[{index}] {method}: server{params}")
  public void closeMethodShouldBeCalledWhenRegionIsDestroyed(int serverIndex) {
    // The test is run twice with destroy being invoked in each server
    clusterStartupRule.getMember(serverIndex).invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      Region<Integer, Portfolio> regionOnServer = cache.getRegion("region");
      regionOnServer.destroyRegion();
    });

    // Wait until region destroy operation has been distributed.
    VMProvider.invokeInEveryMember(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      await().until(() -> cache.getRegion("region") == null);
    }, server1, server2);

    assertThat(testListener.closeInvoked.get()).isTrue();
  }

  private class TestCqListener implements CqListener, Serializable {
    AtomicBoolean closeInvoked = new AtomicBoolean();

    @Override
    public void onEvent(CqEvent aCqEvent) {}

    @Override
    public void onError(CqEvent aCqEvent) {}

    @Override
    public void close() {
      closeInvoked.set(true);
    }
  }
}
