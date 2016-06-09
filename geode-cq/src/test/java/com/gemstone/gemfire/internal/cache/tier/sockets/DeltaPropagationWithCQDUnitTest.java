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

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.DeltaTestImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CqListenerAdapter;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class DeltaPropagationWithCQDUnitTest extends JUnit4DistributedTestCase {
  
  private static GemFireCache cache = null;

  private static Pool pool = null;

  private static String regionName = DeltaPropagationWithCQDUnitTest.class.getSimpleName() + "_region";

  protected VM server1 = null;

  protected VM server2 = null;

  protected VM client1 = null;

  protected VM client2 = null;

  private static final String CQ1 = "SELECT * FROM " + Region.SEPARATOR
      + regionName;

  private static long totalEvents = 0;

  private static long cqEvents = 0;

  private static long cqErrors = 0;
  
  private static long deltasFound = 0;

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);
  }

  @Override
  public final void preTearDown() throws Exception {
    server1.invoke(() -> DeltaPropagationWithCQDUnitTest.close());
    server2.invoke(() -> DeltaPropagationWithCQDUnitTest.close());
    client1.invoke(() -> DeltaPropagationWithCQDUnitTest.close());
    client2.invoke(() -> DeltaPropagationWithCQDUnitTest.close());
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

  @Test
  public void testCqWithRI() throws Exception {
    // 1. setup a cache server
    int port = (Integer)server1.invoke(() -> DeltaPropagationWithCQDUnitTest.createCacheServer());
    // 2. setup a client
    client1
        .invoke(() -> DeltaPropagationWithCQDUnitTest.createClientCache(NetworkUtils.getServerHostName(server1.getHost()), port,
                Boolean.TRUE));
    // 3. setup another client with cqs and interest in all keys.
    createClientCache(NetworkUtils.getServerHostName(server1.getHost()), port, true);
    registerCQs(1, "CQWithInterestDUnitTest_cq");
    // 4. put a key on client1
    client1.invoke(() -> DeltaPropagationWithCQDUnitTest.doPut(
        "SAMPLE_KEY", "SAMPLE_VALUE"));
    // 5. update the key with new value, on client1
    client1.invoke(() -> DeltaPropagationWithCQDUnitTest.doPut(
        "SAMPLE_KEY", "NEW_VALUE"));
    // 6. Wait for some time
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return cqEvents == 2 && cqErrors == 0;
      }
      @Override
      public String description() {
        return "Expected 2 cqEvents and 0 cqErrors, but found " + cqEvents
            + " cqEvents and " + cqErrors + " cqErrors";
      }
    };
    Wait.waitForCriterion(wc, 30 * 1000, 100, true);

    // 7. validate that client2 has the new value
    assertEquals("Latest value: ", "NEW_VALUE", cache.getRegion(regionName)
        .get("SAMPLE_KEY"));
  }

  @Test
  public void testFullValueRequestsWithCqWithoutRI() throws Exception {
    int numOfListeners = 5;
    int numOfKeys = 10;
    int numOfCQs = 3;
    // 1. setup a cache server
    int port = (Integer)server1.invoke(() -> DeltaPropagationWithCQDUnitTest.createCacheServer());
    // 2. setup a client with register interest
    client1
        .invoke(() -> DeltaPropagationWithCQDUnitTest.createClientCache(NetworkUtils.getServerHostName(server1.getHost()), port,
                Boolean.TRUE));
    // 3. setup another client with cqs but without interest.
    createClientCache(NetworkUtils.getServerHostName(server1.getHost()), port, false/*RI*/);
    for (int i = 0; i < numOfCQs; i++) {
      registerCQs(numOfListeners, "Query_"+i);
    }
    // 4. do delta creates
    doPuts(numOfKeys, true);
    // verify client2's CQ listeners see above puts
    verifyCqListeners(numOfListeners * numOfKeys * numOfCQs);
    // verify full value requests at server are zero
    server1.invoke(() -> DeltaPropagationWithCQDUnitTest.verifyFullValueRequestsFromClients(0L));

    // 4. do delta updates on client1
    client1.invoke(() -> DeltaPropagationWithCQDUnitTest.doPuts(numOfKeys, true));
    // verify client2's CQ listeners see above puts
    verifyCqListeners(numOfListeners * numOfKeys * numOfCQs * 2);
    // verify number of deltas encountered in this client
    assertEquals(numOfKeys, deltasFound);
    // verify full value requests at server 
    server1.invoke(() -> DeltaPropagationWithCQDUnitTest.verifyFullValueRequestsFromClients(numOfKeys*1l));
  }

  public static void verifyCqListeners(final Integer events) throws Exception {
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public String description() {
        return "Expected " + events + " listener invocations but found "
            + (cqEvents + cqErrors);
      }

      @Override
      public boolean done() {
        System.out.println("verifyCqListeners: expected total="+events+"; cqEvents="+cqEvents+"; cqErrors="+cqErrors);
        return (cqEvents + cqErrors) == events;
      }
    };
    Wait.waitForCriterion(wc, 10000, 100, true);
  }

  public static void verifyFullValueRequestsFromClients(Long expected)
      throws Exception {
    List<CacheServerImpl> servers = ((GemFireCacheImpl)cache).getCacheServers();
    assertEquals("expected one server but found these: " + servers, 1, servers.size());

    CacheClientProxy[] proxies = servers.get(0).getAcceptor().getCacheClientNotifier()
        .getClientProxies().toArray(new CacheClientProxy[0]);
    
    // find the proxy for the client that processed the CQs - it will have
    // incremented its deltaFullMessagesSent statistic when the listener invoked
    // getValue() on the event and caused a RequestEventValue command to be
    // invoked on the server
    long fullValueRequests = 0;
    for (int i=0; (i < proxies.length) && (fullValueRequests <= 0l); i++) {
      CacheClientProxy proxy = proxies[i];
      fullValueRequests = proxy.getStatistics().getDeltaFullMessagesSent();
    }
    
    assertEquals("Full value requests, ", expected.longValue(), fullValueRequests);
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
    DeltaPropagationWithCQDUnitTest instance = new DeltaPropagationWithCQDUnitTest();
    Properties props = new Properties();
    DistributedSystem ds = instance.getSystem(props);
    ds.disconnect();
    ds = instance.getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
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
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    DeltaPropagationWithCQDUnitTest instance = new DeltaPropagationWithCQDUnitTest();
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
    factory.addCacheListener(new CacheListenerAdapter<Object, Object>() {
      @Override
      public void afterCreate(EntryEvent<Object, Object> event) {
        totalEvents++;
      }

      @Override
      public void afterUpdate(EntryEvent<Object, Object> event) {
        totalEvents++;
      }

      @Override
      public void afterDestroy(EntryEvent<Object, Object> event) {
        totalEvents++;
      }

      @Override
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
        @Override
        public void onEvent(CqEvent event) {
          System.out.println("CqListener.onEvent invoked.  Event="+event);
          if (event.getDeltaValue() != null) {
            deltasFound++;
          }
          // The first CQ event dispatched with a delta will not have a newValue.
          // Attempting to access the newValue will cause an exception to be
          // thrown, exiting this listener and causing the full value to be
          // read from the server.  The listener is then invoked a second time
          // and getNewValue will succeed
          event.getNewValue();
          if (event.getDeltaValue() != null) {
            // if there's a newValue we should ignore the delta bytes
            deltasFound--;
          }
          System.out.println("deltasFound="+deltasFound);
          cqEvents++;
          System.out.println("cqEvents is now " + cqEvents);
        }

        @Override
        public void onError(CqEvent event) {
          System.out.println("CqListener.onError invoked.  Event="+event);
          if (event.getDeltaValue() != null) {
            deltasFound++;
          }
          event.getNewValue();
          if (event.getDeltaValue() != null) {
            deltasFound--;
          }
          System.out.println("deltasFound="+deltasFound);
          cqErrors++;
          System.out.println("cqErrors is now " + cqErrors);
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

