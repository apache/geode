/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.query.dunit;

import java.util.Iterator;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.QueryTestUtils;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.internal.index.CompactRangeIndex;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class HelperTestCase extends CacheTestCase {

  public HelperTestCase(String name) {
    super(name);
  }
  
  protected void createPartitionRegion(VM vm, final String regionName, final Class valueConstraint) {
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
  
  protected void createReplicatedRegion(VM vm, final String regionName, final Class valueConstraint) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        RegionFactory factory = getCache().createRegionFactory(
            RegionShortcut.REPLICATE);
        if (valueConstraint != null) {
          factory.setValueConstraint(valueConstraint);
        }
        factory.create(regionName);
        return true;
      }
    });
  }
  
  protected void createCachingProxyRegion(VM vm, final String regionName, final Class valueConstraint) {
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
  
  protected void createCQ(VM vm, final String cqName, final String query, final CqAttributes cqAttr) {
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
  
  protected void createIndex(VM vm, final String indexName, final String indexedExpression, final String regionPath) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws RegionNotFoundException, CqExistsException, IndexExistsException, IndexNameConflictException {
        getCache().getQueryService().createIndex(indexName, indexedExpression, regionPath);
        return true;
      }
    });
  }
  
  protected void printIndexMapping(VM vm, final String regionName, final String indexName) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws RegionNotFoundException, CqExistsException, IndexExistsException, IndexNameConflictException {
        Region region = getCache().getRegion("/" + regionName);
        CompactRangeIndex index = (CompactRangeIndex)getCache().getQueryService().getIndex(region, indexName);
        System.out.println(index.dump());
        return true;
      }
    });
  }
  
  protected void put(VM vm, final String regionName, final Object key, final Object value) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws RegionNotFoundException, CqExistsException, IndexExistsException, IndexNameConflictException {
        getCache().getRegion("/" + regionName).put(key, value);
        return true;
      }
    });
  }
  
  private CqAttributes createDummyCqAttributes() {
    // Create CQ Attributes.
    CqAttributesFactory cqAf = new CqAttributesFactory();
    
    // Initialize and set CqListener.
    CqListener[] cqListeners = { new CqListener() {

      @Override
      public void close() {
      }

      @Override
      public void onEvent(CqEvent aCqEvent) {
      }

      @Override
      public void onError(CqEvent aCqEvent) {
      }
      
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
  
  protected void startCacheServer(VM server, final int port, final Properties properties) throws Exception {
    createCacheServer(server, port, properties);
    startCacheServers(server);
  }

  /*
   * creates but does not start the cache server.
   * the start() method needs to be invoked explicitly;
   */
  protected void createCacheServer(VM server, final int port, final Properties properties) throws Exception {
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getSystem(properties);
        
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
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
  
  protected void startClient(VM client, final VM[] servers, final int[] ports, final int redundancyLevel, final Properties properties) {
    client.invoke(new CacheSerializableRunnable("Start client") {
      public void run2() throws CacheException {
        getSystem(properties);
        
        final ClientCacheFactory ccf = new ClientCacheFactory(properties);
        for (int i = 0; i < servers.length; i++) {
          ccf.addPoolServer(NetworkUtils.getServerHostName(servers[i].getHost()), ports[i]);
        }
        ccf.setPoolSubscriptionEnabled(true);
        ccf.setPoolSubscriptionRedundancy(redundancyLevel);
        
        ClientCache cache = (ClientCache)getClientCache(ccf);
      }
    });
  }
  
  protected Properties getClientProperties() {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    return p;
  }

  protected Properties getServerProperties(int mcastPort) {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, mcastPort+"");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    return p;
  }
}

