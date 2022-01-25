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
package org.apache.geode.cache30;

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ClientServerCacheOperationDUnitTest implements Serializable {

  private final String regionName = "CsTestRegion";

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Test
  public void largeObjectPutWithReadTimeoutThrowsException() throws Exception {
    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 =
        clusterStartupRule.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));
    MemberVM server2 =
        clusterStartupRule.startServerVM(2, s -> s.withConnectionToLocator(locatorPort));
    ClientVM client1 =
        clusterStartupRule.startClientVM(3,
            c -> c.withLocatorConnection(locatorPort).withPoolSubscription(true));

    ClientVM client2 =
        clusterStartupRule.startClientVM(4,
            c -> c.withLocatorConnection(locatorPort).withPoolSubscription(true));
    final int byteSize = 40 * 1000 * 1000;
    final int listSize = 2;

    server1.invoke(() -> {
      RegionFactory<?, ?> regionFactory =
          ClusterStartupRule.getCache().createRegionFactory(REPLICATE);
      regionFactory.create(regionName);
    });

    server2.invoke(() -> {
      RegionFactory<?, ?> regionFactory =
          ClusterStartupRule.getCache().createRegionFactory(REPLICATE);
      regionFactory.create(regionName);
    });

    List<byte[]> list = new ArrayList<byte[]>(listSize);

    for (int i = 0; i < listSize; i++) {
      list.add(new byte[byteSize]);
    }

    client1.invoke(() -> {
      Pool pool = PoolManager.createFactory()
          .addLocator("localhost", locatorPort)
          .setSocketBufferSize(50)
          .setReadTimeout(40)
          .setPingInterval(200)
          .setSocketConnectTimeout(50)
          .setServerConnectionTimeout(50)
          .create("testPool");

      Region<Object, Object> region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .setPoolName(pool.getName())
          .create(regionName);
      assertThatThrownBy(() -> region.put("key", list))
          .isInstanceOf(ServerConnectivityException.class);

    });

    server1.invoke(() -> {
      Region<?, ?> region = ClusterStartupRule.getCache().getRegion(regionName);
      List<?> value = (List<?>) region.get("key");
      if (value != null) {
        assertThat(value.size()).isEqualTo(listSize);
        list.forEach((b) -> assertThat(b.length).isEqualTo(byteSize));
      }
    });

    client2.invoke(() -> {
      Region<Object, Object> region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(regionName);
      assertThat(region.size()).isEqualTo(0);
      List<?> value = (List<?>) region.get("key");
      if (value != null) {
        assertThat(value.size()).isEqualTo(listSize);
        list.forEach((b) -> assertThat(b.length).isEqualTo(byteSize));
      }
    });
  }

  @Test
  public void largeObjectGetWithReadTimeout() throws Exception {
    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();
    MemberVM server1 =
        clusterStartupRule.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));
    MemberVM server2 =
        clusterStartupRule.startServerVM(2, s -> s.withConnectionToLocator(locatorPort));
    MemberVM server3 =
        clusterStartupRule.startServerVM(3, s -> s.withConnectionToLocator(locatorPort));
    ClientVM client =
        clusterStartupRule.startClientVM(4,
            c -> c.withLocatorConnection(locatorPort).withPoolSubscription(true));

    server1.invoke(() -> {
      RegionFactory<?, ?> regionFactory =
          ClusterStartupRule.getCache().createRegionFactory(REPLICATE);
      regionFactory.create(regionName);
    });

    server2.invoke(() -> {
      RegionFactory<?, ?> regionFactory =
          ClusterStartupRule.getCache().createRegionFactory(REPLICATE);
      regionFactory.create(regionName);
    });

    server3.invoke(() -> {
      RegionFactory<?, ?> regionFactory =
          ClusterStartupRule.getCache().createRegionFactory(REPLICATE);
      Region region = regionFactory.create(regionName);

      int listSize = 2;
      List list = new ArrayList(listSize);

      for (int i = 0; i < listSize; i++) {
        list.add(new byte[75 * 1000 * 1000]);
      }

      region.put("key", list);
    });

    server1.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion(regionName);

      assertThat(region.size()).isEqualTo(1);
    });

    client.invoke(() -> {

      Pool pool = PoolManager.createFactory()
          .addLocator("localhost", locatorPort)
          .setSocketBufferSize(100)
          .setReadTimeout(50)
          .setRetryAttempts(4)
          .create("testPool");

      Region region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .setPoolName(pool.getName())
          .create(regionName);

      region.get("key");
      assertThat(region.size()).isEqualTo(0);

      Object value = region.get("key");

      assertThat(value).isInstanceOf(List.class);
    });

  }
}
