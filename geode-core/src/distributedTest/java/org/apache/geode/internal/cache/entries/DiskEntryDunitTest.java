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
 *
 */

package org.apache.geode.internal.cache.entries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.DiskStoreFactoryImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableConsumerIF;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class DiskEntryDunitTest {
  private static final String LOG_PREFIX = "XXX: ";
  private static final String REGION_NAME = "data";
  private static final String DISK_STORE_NAME = "data_store";
  @Rule
  public final ClusterStartupRule clusterStartupRule = new ClusterStartupRule(7);

  private List<MemberVM> servers;
  private List<ClientVM> clients;
  private int locatorPort;

  @Before
  public void init() {
    MemberVM locator0 = clusterStartupRule.startLocatorVM(0);
    locatorPort = locator0.getPort();
    servers = new ArrayList<>();
    clients = new ArrayList<>();

    Properties serverProp = new Properties();
    serverProp.setProperty("off-heap-memory-size", "500m");
    IntStream.range(1, 5).forEach(serverNum -> servers
        .add(clusterStartupRule.startServerVM(serverNum, serverProp, locatorPort)));
    servers.forEach(server -> server.invoke(() -> {
      DiskStoreFactoryImpl diskStoreFactory =
          new DiskStoreFactoryImpl(ClusterStartupRule.getCache());
      diskStoreFactory.create(DISK_STORE_NAME);
      ClusterStartupRule.getCache()
          .createRegionFactory()
          .setDataPolicy(DataPolicy.PERSISTENT_REPLICATE)
          .setDiskStoreName(DISK_STORE_NAME)
          .setDiskSynchronous(false)
          .setOffHeap(true)
          .create(REGION_NAME);
    }));
    System.out.println(LOG_PREFIX + "servers: " + servers);

    int port = locatorPort;
    SerializableConsumerIF<ClientCacheFactory> cacheSetup = cf -> {
      cf.addPoolLocator("localhost", port);
      // cf.setPoolReadTimeout(READ_TIMEOUT);
    };

    Properties clientProps = new Properties();

    IntStream.range(5, 7).forEach(clientNum -> {
      System.out.println(LOG_PREFIX + "clientNum: " + clientNum);
      try {
        clients.add(clusterStartupRule.startClientVM(clientNum, clientProps, cacheSetup));
      } catch (Exception e) {
        System.out.println(LOG_PREFIX + "bad client: " + e);
      }
      System.out.println(LOG_PREFIX + "good client: " + clientNum);
    });

    clients.forEach(client -> client.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
    }));
  }

  @Test
  public void serverShutdownDoesNotTriggerInternalGemfireError() {
    List<AsyncInvocation<Void>> asyncInvocations = startPuts();

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // disconnect server
    String serverName = servers.get(1).invoke(() -> {
      InternalDistributedSystem internalDistributedSystem =
          ClusterStartupRule.getCache().getInternalDistributedSystem();
      String sName = internalDistributedSystem.getName();
      internalDistributedSystem.getCache().close();
      internalDistributedSystem.disconnect();
      return sName;
    });

    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    int lPort = locatorPort;

    // re-create the cache on the server
    servers.get(1).invoke(() -> {
      Properties properties = new Properties();
      properties.put("name", serverName);
      properties.put("locators", "localhost[" + lPort + "]");
      new CacheFactory(properties).create();
    });

    try {
      Thread.sleep(8000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    GeodeAwaitility.await()
        .until(() -> asyncInvocations.stream().allMatch(AsyncInvocation::isDone));

    for (AsyncInvocation<Void> asyncInvocation : asyncInvocations) {
      Assertions.assertThatNoException().isThrownBy(asyncInvocation::get);
    }

    System.out.println(LOG_PREFIX + "asyncInvocations complete: " + asyncInvocations);
  }

  private List<AsyncInvocation<Void>> startPuts() {
    System.out.println(LOG_PREFIX + "clients: " + clients);

    return clients.stream().map(client -> client.invokeAsync(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      int processId = clientCache.getDistributedSystem().getDistributedMember().getProcessId();
      System.out
          .println(LOG_PREFIX + " readTimeout: " + clientCache.getDefaultPool().getReadTimeout());
      Region<Object, Object> region = clientCache.getRegion(DiskEntryDunitTest.REGION_NAME);
      int keyNum = 0;
      long run_until = System.currentTimeMillis() + 20000;

      System.out.println(LOG_PREFIX + ": start " + "[" + processId + "]");
      do {
        keyNum += 10;
        int mapKeyNum = keyNum;
        Map<Object, Object> putMap = new HashMap<>();
        IntStream.range(0, 9).forEach(num -> {
          String mapKey = "_key_" + (mapKeyNum + num);
          putMap.put(mapKey, (mapKeyNum + num));
        });
        try {
          region.putAll(putMap);
        } catch (Throwable unexpected) {
          // Report the unexpected exception and stop doing operations.
          System.out.println(
              LOG_PREFIX + ": exception keyNum: " + keyNum + " [" + processId + "] " + unexpected);
          throw unexpected;
        }
      } while (System.currentTimeMillis() < run_until);
      System.out.println(LOG_PREFIX + ": finished keyNum: " + keyNum + " [" + processId + "]"
          + " [region: " + DiskEntryDunitTest.REGION_NAME + "]");
    })).collect(Collectors.toList());
  }
}
