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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;
import java.util.Properties;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.QueryTestUtils;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.internal.index.CompactRangeIndex;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public abstract class HelperTestCase extends JUnit4CacheTestCase {

  protected void createPartitionRegion(VM vm, final String regionName,
      final Class valueConstraint) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        RegionFactory factory = getCache().createRegionFactory(RegionShortcut.PARTITION)
            .setPartitionAttributes(paf.create());
        if (valueConstraint != null) {
          factory.setValueConstraint(valueConstraint);
        }
        factory.create(regionName);
        return true;
      }
    });
  }

  protected void createReplicatedRegion(VM vm, final String regionName,
      final Class valueConstraint) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        RegionFactory factory = getCache().createRegionFactory(RegionShortcut.REPLICATE);
        if (valueConstraint != null) {
          factory.setValueConstraint(valueConstraint);
        }
        factory.create(regionName);
        return true;
      }
    });
  }

  protected void createCachingProxyRegion(VM vm, final String regionName,
      final Class valueConstraint) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        ClientRegionFactory factory = ((ClientCache) getCache())
            .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
        if (valueConstraint != null) {
          factory.setValueConstraint(valueConstraint);
        }
        factory.create(regionName);
        return true;
      }
    });
  }

  protected void createCQ(VM vm, final String cqName, final String query,
      final CqAttributes cqAttr) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws CqException, RegionNotFoundException, CqExistsException {
        CqAttributes attrs = cqAttr;
        if (attrs == null) {
          attrs = createDummyCqAttributes();
        }
        getCache().getQueryService().newCq(cqName, query, attrs);
        return true;
      }
    });
  }

  protected void registerInterest(VM vm, final String regionName) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws CqException, RegionNotFoundException, CqExistsException {
        getCache().getRegion("/" + regionName).registerInterestRegex(".*");
        return true;
      }
    });
  }

  protected void createIndex(VM vm, final String indexName, final String indexedExpression,
      final String regionPath) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws RegionNotFoundException, CqExistsException, IndexExistsException,
          IndexNameConflictException {
        getCache().getQueryService().createIndex(indexName, indexedExpression, regionPath);
        return true;
      }
    });
  }

  protected void printIndexMapping(VM vm, final String regionName, final String indexName) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws RegionNotFoundException, CqExistsException, IndexExistsException,
          IndexNameConflictException {
        Region region = getCache().getRegion("/" + regionName);
        CompactRangeIndex index =
            (CompactRangeIndex) getCache().getQueryService().getIndex(region, indexName);
        System.out.println(index.dump());
        return true;
      }
    });
  }

  protected void put(VM vm, final String regionName, final Object key, final Object value) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws RegionNotFoundException, CqExistsException, IndexExistsException,
          IndexNameConflictException {
        getCache().getRegion("/" + regionName).put(key, value);
        return true;
      }
    });
  }

  private CqAttributes createDummyCqAttributes() {
    // Create CQ Attributes.
    CqAttributesFactory cqAf = new CqAttributesFactory();

    // Initialize and set CqListener.
    CqListener[] cqListeners = {new CqListener() {

      @Override
      public void close() {}

      @Override
      public void onEvent(CqEvent aCqEvent) {}

      @Override
      public void onError(CqEvent aCqEvent) {}

    }};
    cqAf.initCqListeners(cqListeners);
    CqAttributes cqa = cqAf.create();
    return cqa;
  }

  protected AsyncInvocation executeCQ(VM vm, final String cqName) {
    return vm.invokeAsync(new SerializableCallable() {
      public Object call() throws CqException, RegionNotFoundException {
        getCache().getQueryService().getCq(cqName).executeWithInitialResults();
        return true;
      }
    });
  }

  public void stopCacheServer(VM server) {
    server.invoke(new SerializableRunnable("Close CacheServer") {
      public void run() {
        CacheServer cs = getCache().getCacheServers().iterator().next();
        cs.stop();
        assertFalse(cs.isRunning());
      }
    });
  }

  protected void startCacheServer(VM server, final int port, final Properties properties)
      throws Exception {
    createCacheServer(server, port, properties);
    startCacheServers(server);
  }

  /*
   * creates but does not start the cache server. the start() method needs to be invoked explicitly;
   */
  protected void createCacheServer(VM server, final int port, final Properties properties)
      throws Exception {
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getSystem(properties);

        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        cache.setCopyOnRead(true);

        CacheServer cacheServer = getCache().addCacheServer();
        cacheServer.setPort(port);

        QueryTestUtils.setCache(cache);
        return true;
      }
    });
  }

  protected void startCacheServers(VM server) {
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Iterator<CacheServer> iterator = getCache().getCacheServers().iterator();
        while (iterator.hasNext()) {
          iterator.next().start();
        }
        return true;
      }
    });
  }

  protected void startClient(VM client, final VM[] servers, final int[] ports,
      final int redundancyLevel, final Properties properties) {
    client.invoke(new CacheSerializableRunnable("Start client") {
      public void run2() throws CacheException {
        getSystem(properties);

        final ClientCacheFactory ccf = new ClientCacheFactory(properties);
        for (int i = 0; i < servers.length; i++) {
          ccf.addPoolServer(NetworkUtils.getServerHostName(servers[i].getHost()), ports[i]);
        }
        ccf.setPoolSubscriptionEnabled(true);
        ccf.setPoolSubscriptionRedundancy(redundancyLevel);

        ClientCache cache = (ClientCache) getClientCache(ccf);
      }
    });
  }

  protected Properties getClientProperties() {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(LOCATORS, "");
    return p;
  }

  protected Properties getServerProperties(int mcastPort) {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, mcastPort + "");
    p.setProperty(LOCATORS, "");
    return p;
  }
}
