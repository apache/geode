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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.Properties;

import com.gemstone.gemfire.DeltaTestImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CqListenerAdapter;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author ashetkar
 *
 */
public class DeltaPropagationWithCQDUnitTest extends DistributedTestCase {
  
  private static GemFireCache cache = null;

  private static Pool pool = null;

  private static String regionName = "CQWithInterestDUnitTest_region";

  protected VM server1 = null;

  protected VM server2 = null;

  protected VM client1 = null;

  protected VM client2 = null;

  private static final String CQ1 = "SELECT * FROM " + Region.SEPARATOR
      + regionName;

  private static long totalEvents = 0;

  private static long cqEvents = 0;

  private static long cqErrors = 0;

  /**
   * @param name
   */
  public DeltaPropagationWithCQDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);
  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    server1.invoke(DeltaPropagationWithCQDUnitTest.class, "close");
    server2.invoke(DeltaPropagationWithCQDUnitTest.class, "close");
    client1.invoke(DeltaPropagationWithCQDUnitTest.class, "close");
    client2.invoke(DeltaPropagationWithCQDUnitTest.class, "close");
    close();
  }
  
  public static void close() throws Exception {
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
    totalEvents = 0;
    cqEvents = 0;
    cqErrors = 0;
  }

  public void testCqWithRI() throws Exception {
    // 1. setup a cache server
    int port = (Integer)server1.invoke(DeltaPropagationWithCQDUnitTest.class,
        "createCacheServer");
    // 2. setup a client
    client1
        .invoke(DeltaPropagationWithCQDUnitTest.class, "createClientCache",
            new Object[] {getServerHostName(server1.getHost()), port,
                Boolean.TRUE});
    // 3. setup another client with cqs and interest in all keys.
    createClientCache(getServerHostName(server1.getHost()), port, true);
    registerCQs(1, "CQWithInterestDUnitTest_cq");
    // 4. put a key on client1
    client1.invoke(DeltaPropagationWithCQDUnitTest.class, "doPut", new Object[] {
        "SAMPLE_KEY", "SAMPLE_VALUE"});
    // 5. update the key with new value, on client1
    client1.invoke(DeltaPropagationWithCQDUnitTest.class, "doPut", new Object[] {
        "SAMPLE_KEY", "NEW_VALUE"});
    // 6. Wait for some time
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return cqEvents == 2 && cqErrors == 0;
      }
      public String description() {
        return "Expected 2 cqEvents and 0 cqErrors, but found " + cqEvents
            + " cqEvents and " + cqErrors + " cqErrors";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 30 * 1000, 100, true);

    // 7. validate that client2 has the new value
    assertEquals("Latest value: ", "NEW_VALUE", cache.getRegion(regionName)
        .get("SAMPLE_KEY"));
  }

  public void testFullValueRequestsWithCqWithoutRI() throws Exception {
    int numOfListeners = 5;
    int numOfKeys = 10;
    int numOfCQs = 3;
    // 1. setup a cache server
    int port = (Integer)server1.invoke(DeltaPropagationWithCQDUnitTest.class,
        "createCacheServer");
    // 2. setup a client with register interest
    client1
        .invoke(DeltaPropagationWithCQDUnitTest.class, "createClientCache",
            new Object[] {getServerHostName(server1.getHost()), port,
                Boolean.TRUE});
    // 3. setup another client with cqs but without interest.
    createClientCache(getServerHostName(server1.getHost()), port, false/*RI*/);
    for (int i = 0; i < numOfCQs; i++) {
      registerCQs(numOfListeners, "Query_"+i);
    }
    // 4. do delta creates
    doPuts(numOfKeys, true);
    // verify client2's CQ listeners see above puts
    verifyCqListeners(numOfListeners * numOfKeys * numOfCQs);
    // verify full value requests at server are zero
    server1.invoke(DeltaPropagationWithCQDUnitTest.class,
        "verifyFullValueRequestsFromClients", new Object[] {0L});

    // 4. do delta updates on client1
    client1.invoke(DeltaPropagationWithCQDUnitTest.class, "doPuts",
        new Object[] {numOfKeys, true});
    // verify client2's CQ listeners see above puts
    verifyCqListeners(numOfListeners * numOfKeys * numOfCQs * 2);
    // verify full value requests at server 
    server1.invoke(DeltaPropagationWithCQDUnitTest.class,
        "verifyFullValueRequestsFromClients", new Object[] {10L});
  }

  public static void verifyCqListeners(final Integer events) throws Exception {
    WaitCriterion wc = new WaitCriterion() {
      public String description() {
        return "Expected " + events + " listener invocations but found "
            + (cqEvents + cqErrors);
      }

      public boolean done() {
        return (cqEvents + cqErrors) == events;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 10000, 100, true);
  }

  public static void verifyFullValueRequestsFromClients(Long expected)
      throws Exception {
    Object[] proxies = ((CacheServerImpl)((GemFireCacheImpl)cache)
        .getCacheServers().get(0)).getAcceptor().getCacheClientNotifier()
        .getClientProxies().toArray();
    long fullValueRequests = ((CacheClientProxy)proxies[0]).getStatistics()
        .getDeltaFullMessagesSent();
    if (fullValueRequests == 0) {
      assertEquals("Full value requests, ", expected.longValue(),
          ((CacheClientProxy)proxies[1]).getStatistics()
          .getDeltaFullMessagesSent());
    } else {
      assertEquals("Full value requests, ", expected.longValue(),
          fullValueRequests);
    }
  }

  public static void doPut(Object key, Object value) throws Exception {
    Region<Object, Object> region = cache.getRegion(regionName);
    region.put(key, value);
  }

  public static void doPuts(Integer num, Boolean useDelta) throws Exception {
    Region<Object, Object> region = cache.getRegion(regionName);
    for (int i = 0; i < num; i++) {
      if (useDelta) {
        DeltaTestImpl delta = new DeltaTestImpl(i, "VALUE_"+i);
        delta.setIntVar(i);
        region.put("KEY_" + i, delta);
      } else {
        region.put("KEY_" + i, i);
      }
    }
  }

  public static Integer createCacheServer() throws Exception {
    DeltaPropagationWithCQDUnitTest instance = new DeltaPropagationWithCQDUnitTest("temp");
    Properties props = new Properties();
    DistributedSystem ds = instance.getSystem(props);
    ds.disconnect();
    ds = instance.getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
//    Properties props = new Properties();
//    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "10333");
//    cache = new CacheFactory(props).create();
    RegionFactory<Object, Object> rf = ((Cache)cache)
        .createRegionFactory(RegionShortcut.REPLICATE);
    rf.create(regionName);
    CacheServer server = ((Cache)cache).addCacheServer();
    server.setPort(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    server.start();
    return server.getPort();
  }

  public static void createClientCache(String host, Integer port, Boolean doRI)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    DeltaPropagationWithCQDUnitTest instance = new DeltaPropagationWithCQDUnitTest("temp");
    DistributedSystem ds = instance.getSystem(props);
    ds.disconnect();
    ds = instance.getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
    AttributesFactory factory = new AttributesFactory();
    pool = ClientServerTestCase.configureConnectionPool(factory, "localhost", new int[]{port},
        true, 1, 2, null, 1000, 250, false, -2);

    factory.setScope(Scope.LOCAL);
//    String poolName = "CQWithInterestDUnitTest_pool";
//    cache = new ClientCacheFactory(new Properties()).create();
//    ClientRegionFactory<Object, Object> rf = ((ClientCache)cache)
//        .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);
//    PoolFactory pf = PoolManager.createFactory().addServer(host, port)
//        .setSubscriptionEnabled(true);
//    pool = pf.create(poolName);
//    rf.setPoolName(poolName);
//    rf.addCacheListener(new CacheListenerAdapter<Object, Object>() {
    factory.addCacheListener(new CacheListenerAdapter<Object, Object>() {
      public void afterCreate(EntryEvent<Object, Object> event) {
        totalEvents++;
      }

      public void afterUpdate(EntryEvent<Object, Object> event) {
        totalEvents++;
      }

      public void afterDestroy(EntryEvent<Object, Object> event) {
        totalEvents++;
      }

      public void afterInvalidate(EntryEvent<Object, Object> event) {
        totalEvents++;
      }
    });
    RegionAttributes attr = factory.create();
    Region region = ((Cache)cache).createRegion(regionName, attr);
    if (doRI) {
      region.registerInterest("ALL_KEYS");
    }
  }

  public static void registerCQs(Integer numOfListeners, String name) throws Exception {
    QueryService qs = pool.getQueryService();

    CqAttributesFactory caf = new CqAttributesFactory();

    CqListenerAdapter[] cqListeners = new CqListenerAdapter[numOfListeners];
    for (int i = 0; i < numOfListeners; i++) {
      cqListeners[i] = new CqListenerAdapter() {
        public void onEvent(CqEvent event) {
          event.getNewValue();
          cqEvents++;
        }

        public void onError(CqEvent event) {
          event.getNewValue();
          cqErrors++;
        }
      };
      caf.addCqListener(cqListeners[i]);
    }

    CqQuery cQuery = qs.newCq(name, CQ1, caf.create());
    cQuery.execute();

    if (qs.getCq(name) == null) {
      fail("Failed to get CQ " + name);
    }
  }
}

