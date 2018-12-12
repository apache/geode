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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.cq.dunit.CqQueryTestListener;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.internal.cache.tier.sockets.ConflationDUnitTestHelper;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This Dunit test is to verify that when the dispatcher of CS dispatches the Event , the peer's
 * HARegionQueue should get the events removed from the HA RegionQueues assuming the QRM thread has
 * acted upon by that time This is done in the following steps 1. start server1 and server2 2. start
 * client1 and client2 3. perform put operation from client1 4. check the entry in the regionque of
 * client2 on server2.It should be present. 5. Wait till client2 receives event 6. Make sure that
 * QRM is envoked 7. Again the entry in the regionque of client2 on server2.It should not be
 * present. 8. close client1 and client2 9. close server1 and server2
 */
@Category({ClientSubscriptionTest.class})
public class HADispatcherDUnitTest extends JUnit4DistributedTestCase {

  private static final String REGION_NAME = HADispatcherDUnitTest.class.getSimpleName() + "_region";
  private static final Object dummyObj = "dummyObject";
  private static final String KEY1 = "KEY1";
  private static final String VALUE1 = "VALUE1";
  private static final String KEY2 = "KEY2";
  private static final String VALUE2 = "VALUE2";

  private static Cache cache = null;
  private static volatile boolean isObjectPresent = false;
  private static volatile boolean waitFlag = true;

  private VM server1 = null;
  private VM server2 = null;
  private VM client1 = null;
  private VM client2 = null;
  private int PORT1;
  private int PORT2;

  @Override
  public final void postSetUp() throws Exception {
    String serverHostName = getServerHostName(getHost(0));

    // Server1 VM
    server1 = getHost(0).getVM(0);

    // Server2 VM
    server2 = getHost(0).getVM(1);

    // Client 1 VM
    client1 = getHost(0).getVM(2);

    // client 2 VM
    client2 = getHost(0).getVM(3);

    PORT1 = ((Integer) server1.invoke(() -> createServerCache(new Boolean(false)))).intValue();

    server1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart());
    server1.invoke(() -> makeDispatcherSlow());
    server1.invoke(() -> setQRMslow());

    PORT2 = ((Integer) server2.invoke(() -> createServerCache(new Boolean(true)))).intValue();

    client1.invoke(() -> CacheServerTestUtil.disableShufflingOfEndpoints());
    client2.invoke(() -> CacheServerTestUtil.disableShufflingOfEndpoints());
    client1.invoke(() -> createClientCache(serverHostName, new Integer(PORT1), new Integer(PORT2),
        new Boolean(false)));
    client2.invoke(() -> createClientCache(serverHostName, new Integer(PORT1), new Integer(PORT2),
        new Boolean(true)));
  }

  @Override
  public final void preTearDown() throws Exception {
    client1.invoke(() -> closeCache());
    client2.invoke(() -> closeCache());
    // close server
    server1.invoke(() -> resetQRMslow());
    server1.invoke(() -> closeCache());
    server2.invoke(() -> closeCache());
  }

  @Test
  public void testDispatcher() throws Exception {
    clientPut(client1, KEY1, VALUE1);
    // Waiting in the client2 till it receives the event for the key.
    checkFromClient(client2);

    // performing check in the regionqueue of the server2
    checkFromServer(server2, KEY1);

    // For CQ Only.
    // performing put from the client1
    clientPut(client1, KEY2, VALUE2);
    checkFromClient(client2);
    checkFromServer(server2, KEY2);

    getLogWriter().info("testDispatcher() completed successfully");
  }

  /**
   * This is to test the serialization mechanism of ClientUpdateMessage. Added after CQ support.
   * This could be done in different way, by overflowing the HARegion queue.
   */
  @Test
  public void testClientUpdateMessageSerialization() throws Exception {
    // Update Value.
    clientPut(client1, KEY1, VALUE1);
    getLogWriter().fine(">>>>>>>> after clientPut(c1, k1, v1)");
    // Waiting in the client2 till it receives the event for the key.
    checkFromClient(client2);
    getLogWriter().fine("after checkFromClient(c2)");

    // performing check in the regionqueue of the server2
    checkFromServer(server2, KEY1);
    getLogWriter().fine("after checkFromServer(s2, k1)");

    // UPDATE.
    clientPut(client1, KEY1, VALUE1);
    getLogWriter().fine("after clientPut 2 (c1, k1, v1)");
    // Waiting in the client2 till it receives the event for the key.
    checkFromClient(client2);
    getLogWriter().fine("after checkFromClient 2 (c2)");

    // performing check in the regionqueue of the server2
    checkFromServer(server2, KEY1);
    getLogWriter().fine("after checkFromServer 2 (s2, k1)");

    getLogWriter().info("testClientUpdateMessageSerialization() completed successfully");
  }

  private void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  private void setQRMslow() throws InterruptedException {
    int oldMessageSyncInterval = cache.getMessageSyncInterval();
    cache.setMessageSyncInterval(6);
    Thread.sleep((oldMessageSyncInterval + 1) * 1000);
  }

  private void resetQRMslow() {
    cache.setMessageSyncInterval(HARegionQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
  }

  private void makeDispatcherSlow() {
    System.setProperty("slowStartTimeForTesting", "5000");
  }

  private void clientPut(VM vm, final Object key, final Object value) {
    // performing put from the client1
    vm.invoke(new CacheSerializableRunnable("putFromClient") {
      @Override
      public void run2() throws CacheException {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        assertNotNull(region);
        region.put(key, value);
      }
    });
  }

  private void checkFromClient(VM vm) {
    // Waiting in the client till it receives the event for the key.
    vm.invoke(new CacheSerializableRunnable("checkFromClient") {
      @Override
      public void run2() throws CacheException {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        assertNotNull(region);
        cache.getLogger().fine("starting the wait");
        synchronized (dummyObj) {
          while (waitFlag) {
            try {
              dummyObj.wait(30000);
            } catch (InterruptedException e) {
              fail("interrupted", e);
            }
          }
        }
        cache.getLogger().fine("wait over...waitFlag=" + waitFlag);
        if (waitFlag)
          fail("test failed");
      }
    });
  }

  private void checkFromServer(VM vm, final Object key) {
    vm.invoke(new CacheSerializableRunnable("checkFromServer") {
      @Override
      public void run2() throws CacheException {
        Iterator iter = cache.getCacheServers().iterator();
        CacheServerImpl server = (CacheServerImpl) iter.next();
        Iterator iter_prox =
            server.getAcceptor().getCacheClientNotifier().getClientProxies().iterator();
        isObjectPresent = false;

        while (iter_prox.hasNext()) {
          final CacheClientProxy proxy = (CacheClientProxy) iter_prox.next();
          HARegion region = (HARegion) proxy.getHARegion();
          assertNotNull(region);
          final HARegionQueue regionQueue = region.getOwner();

          WaitCriterion wc = new WaitCriterion() {
            @Override
            public boolean done() {
              int sz = regionQueue.size();
              cache.getLogger().fine("regionQueue.size()::" + sz);
              return sz == 0 || !proxy.isConnected();
            }

            @Override
            public String description() {
              return "regionQueue not empty with size " + regionQueue.size() + " for proxy "
                  + proxy;
            }
          };
          GeodeAwaitility.await().untilAsserted(wc);

          cache.getLogger().fine("processed a proxy");
        }
      }
    });
  }

  private void createCache(Properties props) {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  private Integer createServerCache(Boolean isListenerPresent) throws IOException {
    createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    if (isListenerPresent.booleanValue() == true) {
      CacheListener serverListener = new HAServerListener();
      factory.setCacheListener(serverListener);
    }
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    CacheServerImpl server = (CacheServerImpl) cache.addCacheServer();
    assertNotNull(server);
    int port = getRandomAvailablePort(SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  private void createClientCache(String hostName, Integer port1, Integer port2,
      Boolean isListenerPresent) throws CqException, CqExistsException, RegionNotFoundException {
    int PORT1 = port1.intValue();
    int PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ClientServerTestCase.configureConnectionPool(factory, hostName, new int[] {PORT1, PORT2}, true,
        -1, 2, null);
    if (isListenerPresent.booleanValue() == true) {
      CacheListener clientListener = new HAClientListener();
      factory.setCacheListener(clientListener);
    }
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);

    {
      LocalRegion lr = (LocalRegion) region;
      final PoolImpl pool = (PoolImpl) (lr.getServerProxy().getPool());
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return pool.getPrimary() != null;
        }

        public String description() {
          return null;
        }
      };
      GeodeAwaitility.await().untilAsserted(ev);
      ev = new WaitCriterion() {
        public boolean done() {
          return pool.getRedundants().size() >= 1;
        }

        public String description() {
          return null;
        }
      };
      GeodeAwaitility.await().untilAsserted(ev);

      assertNotNull(pool.getPrimary());
      assertTrue("backups=" + pool.getRedundants() + " expected=" + 1,
          pool.getRedundants().size() >= 1);
      assertEquals(PORT1, pool.getPrimaryPort());
    }

    region.registerInterest(KEY1);

    // Register CQ.
    createCQ();
  }

  private void createCQ() throws CqException, CqExistsException, RegionNotFoundException {
    QueryService cqService = null;
    try {
      cqService = cache.getQueryService();
    } catch (Exception cqe) {
      cqe.printStackTrace();
      fail("Failed to getCQService.");
    }

    // Create CQ Attributes.
    CqAttributesFactory cqf = new CqAttributesFactory();
    CqListener[] cqListeners = {new CqQueryTestListener(getLogWriter())};
    cqf.initCqListeners(cqListeners);
    CqAttributes cqa = cqf.create();

    String cqName = "CQForHARegionQueueTest";
    String queryStr = "Select * from " + Region.SEPARATOR + REGION_NAME;

    // Create CQ.
    CqQuery cq1 = cqService.newCq(cqName, queryStr, cqa);
    cq1.execute();
  }

  /**
   * This is the client listener which notifies the waiting thread when it receives the event.
   */
  private static class HAClientListener extends CacheListenerAdapter implements Declarable {

    @Override
    public void afterCreate(EntryEvent event) {
      synchronized (dummyObj) {
        try {
          Object value = event.getNewValue();
          if (value.equals(VALUE1)) {
            waitFlag = false;
            dummyObj.notifyAll();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    @Override
    public void init(Properties props) {}
  }

  /**
   * This is the server listener which ensures that regionqueue is properly populated
   */
  private static class HAServerListener extends CacheListenerAdapter {
    @Override
    public void afterCreate(EntryEvent event) {
      Cache cache = event.getRegion().getCache();
      Iterator iter = cache.getCacheServers().iterator();
      CacheServerImpl server = (CacheServerImpl) iter.next();
      isObjectPresent = false;

      // The event not be there in the region first time; try couple of time.
      // This should have been replaced by listener on the HARegion and doing wait for event arrival
      // in that.
      while (true) {
        for (Iterator iter_prox =
            server.getAcceptor().getCacheClientNotifier().getClientProxies().iterator(); iter_prox
                .hasNext();) {
          CacheClientProxy proxy = (CacheClientProxy) iter_prox.next();
          HARegion regionForQueue = (HARegion) proxy.getHARegion();

          for (Iterator itr = regionForQueue.values().iterator(); itr.hasNext();) {
            Object obj = itr.next();
            if (obj instanceof HAEventWrapper) {
              Conflatable confObj = (Conflatable) obj;
              if (KEY1.equals(confObj.getKeyToConflate())
                  || KEY2.equals(confObj.getKeyToConflate())) {
                isObjectPresent = true;
              }
            }
          }
        }

        if (isObjectPresent) {
          break; // From while.
        }

        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          fail("interrupted", e);
        }
      }
    }
  }
}
