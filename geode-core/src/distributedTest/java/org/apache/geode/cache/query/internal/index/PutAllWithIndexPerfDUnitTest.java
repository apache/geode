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
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class PutAllWithIndexPerfDUnitTest extends JUnit4CacheTestCase {

  /** The port on which the cache server was started in this VM */
  private static int bridgeServerPort;
  static long timeWithoutStructTypeIndex = 0;
  static long timeWithStructTypeIndex = 0;

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  @Ignore("TODO: test is disabled")
  @Test
  public void testPutAllWithIndexes() {
    final String name = "testRegion";
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 10000;

    // Start server
    vm0.invoke(new CacheSerializableRunnable("Create cache server") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.put(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
        Cache cache = new CacheFactory(config).create();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        cache.createRegionFactory(factory.create()).create(name);
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
        // Create Index on empty region
        try {
          cache.getQueryService().createIndex("idIndex", "ID", "/" + name);
        } catch (Exception e) {
          Assert.fail("index creation failed", e);
        }
      }
    });

    // Create client region
    final int port = vm0.invoke(() -> PutAllWithIndexPerfDUnitTest.getCacheServerPort());
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());
    vm1.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        Properties config = new Properties();
        config.setProperty(MCAST_PORT, "0");
        ClientCache cache = new ClientCacheFactory().addPoolServer(host0, port).create();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(name);
      }
    });

    vm1.invoke(new CacheSerializableRunnable("putAll() test") {

      @Override
      public void run2() throws CacheException {
        Region exampleRegion = ClientCacheFactory.getAnyInstance().getRegion(name);

        Map warmupMap = new HashMap();
        Map data = new HashMap();
        for (int i = 0; i < 10000; i++) {
          Object p = new PortfolioPdx(i);
          if (i < 1000)
            warmupMap.put(i, p);
          data.put(i, p);
        }

        for (int i = 0; i < 10; i++) {
          exampleRegion.putAll(warmupMap);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
          exampleRegion.putAll(data);
        }
        long end = System.currentTimeMillis();
        timeWithoutStructTypeIndex = ((end - start) / 10);
        System.out
            .println("Total putall time for 10000 objects is: " + ((end - start) / 10) + "ms");

      }
    });

    vm0.invoke(new CacheSerializableRunnable("Remove Index and create new one") {

      @Override
      public void run2() throws CacheException {
        try {
          Cache cache = CacheFactory.getAnyInstance();
          cache.getQueryService().removeIndexes();
          cache.getRegion(name).clear();
          cache.getQueryService().createIndex("idIndex", "p.ID", "/" + name + " p");
        } catch (Exception e) {
          Assert.fail("index creation failed", e);
        }
      }
    });

    vm1.invoke(new CacheSerializableRunnable("putAll() test") {

      @Override
      public void run2() throws CacheException {
        Region exampleRegion = ClientCacheFactory.getAnyInstance().getRegion(name);
        exampleRegion.clear();
        Map warmupMap = new HashMap();
        Map data = new HashMap();
        for (int i = 0; i < 10000; i++) {
          Object p = new PortfolioPdx(i);
          if (i < 1000)
            warmupMap.put(i, p);
          data.put(i, p);
        }

        for (int i = 0; i < 10; i++) {
          exampleRegion.putAll(warmupMap);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
          exampleRegion.putAll(data);
        }
        long end = System.currentTimeMillis();
        timeWithStructTypeIndex = ((end - start) / 10);
        System.out
            .println("Total putall time for 10000 objects is: " + ((end - start) / 10) + "ms");

      }
    });

    if (timeWithoutStructTypeIndex > timeWithStructTypeIndex) {
      fail("putAll took more time without struct type index than simple index");
    }
  }

  /**
   * Starts a cache server on the given port, using the given deserializeValues and
   * notifyBySubscription to serve up the given region.
   */
  protected void startBridgeServer(int port, boolean notifyBySubscription) throws IOException {

    Cache cache = CacheFactory.getAnyInstance();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  private static int getCacheServerPort() {
    return bridgeServerPort;
  }
}
