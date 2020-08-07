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

import java.io.IOException;
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
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ClientServerCacheOperationDUnitTest implements Serializable {

  private String regionName = "CsTestRegion";

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Test
  public void largeObjectPutWithReadTimeoutThrowsException() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM client = VM.getVM(2);

    final int byteSize = 40 * 1000 * 1000;
    final int listSize = 2;
    final int locatorPort = DistributedTestUtils.getLocatorPort();

    server1.invoke(() -> createServerCache());
    server2.invoke(() -> createServerCache());

    server1.invoke(() -> {
      RegionFactory<?, ?> regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE);
      regionFactory.create(regionName);
    });

    server2.invoke(() -> {
      RegionFactory<?, ?> regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE);
      regionFactory.create(regionName);
    });

    List<byte[]> list = new ArrayList(listSize);

    for (int i = 0; i < listSize; i++) {
      list.add(new byte[byteSize]);
    }

    client.invoke(() -> {
      clientCacheRule.createClientCache();

      Pool pool = PoolManager.createFactory()
          .addLocator("localhost", locatorPort)
          .setSocketBufferSize(50)
          .setReadTimeout(40)
          .setPingInterval(200)
          .setSocketConnectTimeout(50)
          .create("testPool");

      Region region = clientCacheRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .setPoolName(pool.getName())
          .create(regionName);

      assertThatThrownBy(() -> region.put("key", list))
          .isInstanceOf(ServerConnectivityException.class);

    });

    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      List value = (List) region.get("key");
      if (value != null) {
        assertThat(value.size()).isEqualTo(listSize);
        list.forEach((b) -> assertThat(b.length).isEqualTo(byteSize));
      }
    });

    client.invoke(() -> {
      Region region = clientCacheRule.getClientCache().getRegion(regionName);
      assertThat(region.size()).isEqualTo(0);
      List value = (List) region.get("key");
      if (value != null) {
        assertThat(value.size()).isEqualTo(listSize);
        list.forEach((b) -> assertThat(b.length).isEqualTo(byteSize));
      }
    });

  }

  @Test
  public void largeObjectGetWithReadTimeout() {
    VM server1 = VM.getVM(0);
    VM server2 = VM.getVM(1);
    VM server3 = VM.getVM(2);
    VM client = VM.getVM(3);

    final int locatorPort = DistributedTestUtils.getLocatorPort();

    server1.invoke(() -> createServerCache());
    server2.invoke(() -> createServerCache());
    server3.invoke(() -> createServerCache());

    server1.invoke(() -> {
      RegionFactory<?, ?> regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE);
      regionFactory.create(regionName);
    });

    server2.invoke(() -> {
      RegionFactory<?, ?> regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE);
      regionFactory.create(regionName);
    });

    server3.invoke(() -> {
      RegionFactory<?, ?> regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE);
      Region region = regionFactory.create(regionName);

      int listSize = 2;
      List list = new ArrayList(listSize);

      for (int i = 0; i < listSize; i++) {
        list.add(new byte[75 * 1000 * 1000]);
      }

      region.put("key", list);
    });

    server1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);

      assertThat(region.size()).isEqualTo(1);
    });

    client.invoke(() -> {
      clientCacheRule.createClientCache();

      Pool pool = PoolManager.createFactory()
          .addLocator("localhost", locatorPort)
          .setSocketBufferSize(100)
          .setReadTimeout(50)
          .create("testPool");

      Region region = clientCacheRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .setPoolName(pool.getName())
          .create(regionName);

      region.get("key");
      assertThat(region.size()).isEqualTo(0);

      Object value = region.get("key");

      assertThat(value).isInstanceOf(List.class);
    });

  }

  private void createServerCache() throws IOException {
    cacheRule.createCache();
    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
  }

}
