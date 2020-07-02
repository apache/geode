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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientCacheConnection;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheObserverAdapter;
import org.apache.geode.internal.cache.CacheObserverHolder;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * This is the DUnit Test to verify clear and DestroyRegion operation in Client-Server
 * Configuration.
 */
@Category({ClientServerTest.class})
public class ClearPropagationDUnitTest extends JUnit4DistributedTestCase {

  VM server1 = null;

  VM server2 = null;

  VM client1 = null;

  VM client2 = null;

  private static int PORT1;

  private static int PORT2;

  private static final String REGION_NAME = "ClearPropagationDUnitTest_region";

  protected static Cache cache = null;

  protected static boolean gotClear = false;

  protected static boolean gotDestroyed = false;

  /** constructor */
  public ClearPropagationDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    // Server1 VM
    server1 = host.getVM(0);

    // Server2 VM
    server2 = host.getVM(1);

    // Client 1 VM
    client1 = host.getVM(2);

    // client 2 VM
    client2 = host.getVM(3);

    int PORT1 =
        ((Integer) server1.invoke(() -> ClearPropagationDUnitTest.createServerCache())).intValue();
    int PORT2 =
        ((Integer) server2.invoke(() -> ClearPropagationDUnitTest.createServerCache())).intValue();

    client1.invoke(() -> ClearPropagationDUnitTest.createClientCache(
        NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1), new Integer(PORT2)));
    client2.invoke(() -> ClearPropagationDUnitTest.createClientCache(
        NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1), new Integer(PORT2)));

    CacheObserverHolder.setInstance(new CacheObserverAdapter());
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  /**
   * This is the DUnit Test to verify clear operation in Client-Server Configuration. Start client 1
   * Start client 2 Start Server 1 Start Server 2 Identify the server which is not primary and
   * perform put and then clear operation from client1 against that server. Verify that Client 1
   * does not get the update Verify that Client 2 does get the update
   *
   */
  @Test
  public void testVerifyClearNotReceivedBySenderReceivedByOthers() {
    CacheSerializableRunnable resetFlags = new CacheSerializableRunnable("resetFlags") {
      @Override
      public void run2() throws CacheException {
        gotClear = false;
        gotDestroyed = false;
      }
    };
    server1.invoke(resetFlags);
    server2.invoke(resetFlags);
    client1.invoke(resetFlags);
    client2.invoke(resetFlags);


    // First create entries on both servers via the two client
    client1.invoke(() -> ClearPropagationDUnitTest.createEntriesK1andK2());
    client2.invoke(() -> ClearPropagationDUnitTest.createEntriesK1andK2());
    client1.invoke(() -> ClearPropagationDUnitTest.registerKeysK1andK2());
    client2.invoke(() -> ClearPropagationDUnitTest.registerKeysK1andK2());

    server1.invoke(checkSizeRegion(2, false/* Do not Block */));
    server2.invoke(checkSizeRegion(2, false/* Do not Block */));
    client1.invoke(checkSizeRegion(2, false/* Do not Block */));
    client2.invoke(checkSizeRegion(2, false/* Do not Block */));

    client1.invoke(() -> ClearPropagationDUnitTest
        .acquireConnectionsAndClear(NetworkUtils.getServerHostName(client1.getHost())));

    client1.invoke(checkSizeRegion(2, false/* Do not Block */));
    client2.invoke(checkSizeRegion(0, true /* block */));
    server1.invoke(checkSizeRegion(0, false/* Do not Block */));
    server2.invoke(checkSizeRegion(0, false/* Do not Block */));

    client1.invoke(() -> ClearPropagationDUnitTest.verifyNoUpdates());

  }

  /**
   * This is the DUnit Test to verify destroyRegion operation in Client-Server Configuration. Start
   * client 1 Start client 2 Start Server 1 Start Server 2 Identify the server which is not primary
   * and perform destroyRegion operation from client1 against that server. Verify that Client 1 does
   * not get the update Verify that Client 2 does get the update
   *
   */
  @Test
  public void testEventIdGeneratedInDestroyRegionOperation() throws Exception {
    CacheSerializableRunnable resetFlags = new CacheSerializableRunnable("resetFlags") {
      @Override
      public void run2() throws CacheException {
        gotClear = false;
        gotDestroyed = false;
      }
    };
    server1.invoke(resetFlags);
    server2.invoke(resetFlags);
    client1.invoke(resetFlags);
    client2.invoke(resetFlags);

    client1.invoke(() -> ClearPropagationDUnitTest.createEntriesK1andK2());
    client2.invoke(() -> ClearPropagationDUnitTest.createEntriesK1andK2());
    client1.invoke(() -> ClearPropagationDUnitTest.registerKeysK1andK2());
    client2.invoke(() -> ClearPropagationDUnitTest.registerKeysK1andK2());

    server1.invoke(checkSizeRegion(2, false/* Do not Block */));
    server2.invoke(checkSizeRegion(2, false/* Do not Block */));
    client1.invoke(checkSizeRegion(2, false/* Do not Block */));
    client2.invoke(checkSizeRegion(2, false/* Do not Block */));

    client1.invoke(() -> ClearPropagationDUnitTest
        .acquireConnectionsAndDestroyRegion(NetworkUtils.getServerHostName(client1.getHost())));

    client1.invoke(checkSizeRegion(2, false/* Do not Block */));
    client2.invoke(checkDestroyRegion(true /* block */));
    server1.invoke(checkDestroyRegion(false/* Do not Block */));
    server2.invoke(checkDestroyRegion(false/* Do not Block */));

    client1.invoke(() -> ClearPropagationDUnitTest.verifyNoUpdates());

  }


  private CacheSerializableRunnable checkDestroyRegion(final boolean toBlock) {
    CacheSerializableRunnable checkRegion = new CacheSerializableRunnable("checkDestroyRegion") {
      @Override
      public void run2() throws CacheException {
        if (toBlock) {
          synchronized (ClearPropagationDUnitTest.class) {
            if (!gotDestroyed) {
              try {
                ClearPropagationDUnitTest.class.wait();
              } catch (InterruptedException e) {
                throw new CacheException(e) {

                };
              }
            }
          }
        }

        Region region = cache.getRegion(SEPARATOR + REGION_NAME);
        assertNull(region);
      }
    };

    return checkRegion;
  }

  private CacheSerializableRunnable checkSizeRegion(final int size, final boolean toBlock) {

    CacheSerializableRunnable clearRegion = new CacheSerializableRunnable("checkSize") {

      @Override
      public void run2() throws CacheException {
        Region region = cache.getRegion(SEPARATOR + REGION_NAME);
        assertNotNull(region);
        LogWriterUtils.getLogWriter().info("Size of the region " + region.size());

        if (toBlock) {
          synchronized (ClearPropagationDUnitTest.class) {
            if (!gotClear) {
              try {
                ClearPropagationDUnitTest.class.wait();
              } catch (InterruptedException e) {
                throw new CacheException(e) {

                };
              }
            }
          }
        }
        assertEquals(size, region.size());
      }
    };
    return clearRegion;
  }

  public static void acquireConnectionsAndClear(String host) {
    try {
      Region r1 = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r1);
      String poolName = r1.getAttributes().getPoolName();
      assertNotNull(poolName);
      PoolImpl pool = (PoolImpl) PoolManager.find(poolName);
      assertNotNull(pool);
      ClientCacheConnection conn1 = pool.acquireConnection(new ServerLocation(host, PORT2));
      assertNotNull(conn1);
      assertEquals(PORT2, conn1.getServer().getPort());
      ServerRegionProxy srp = new ServerRegionProxy(SEPARATOR + REGION_NAME, pool);
      srp.clearOnForTestsOnly(conn1, new EventID(new byte[] {1}, 1, 1), null);
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("while setting acquireConnections  " + ex);
    }
  }


  public static void acquireConnectionsAndDestroyRegion(String host) {
    try {
      Region r1 = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r1);
      String poolName = r1.getAttributes().getPoolName();
      assertNotNull(poolName);
      PoolImpl pool = (PoolImpl) PoolManager.find(poolName);
      assertNotNull(pool);
      ClientCacheConnection conn1 = pool.acquireConnection(new ServerLocation(host, PORT2));
      assertNotNull(conn1);
      assertEquals(PORT2, conn1.getServer().getPort());
      ServerRegionProxy srp = new ServerRegionProxy(SEPARATOR + REGION_NAME, pool);
      srp.destroyRegionOnForTestsOnly(conn1, new EventID(new byte[] {1}, 1, 1), null);
    } catch (Exception ex) {
      ex.printStackTrace();
      fail("while setting acquireConnections  " + ex);
    }
  }


  /**
   * Creates entries on the server
   *
   */
  public static void createEntriesK1andK2() {
    try {
      Region r1 = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r1);
      if (!r1.containsKey("key1")) {
        r1.create("key1", "key-1");
      }
      if (!r1.containsKey("key2")) {
        r1.create("key2", "key-2");
      }
      assertEquals(r1.getEntry("key1").getValue(), "key-1");
      assertEquals(r1.getEntry("key2").getValue(), "key-2");
    } catch (Exception ex) {
      Assert.fail("failed while createEntriesK1andK2()", ex);
    }
  }

  public static void createClientCache(String host, Integer port1, Integer port2) throws Exception {
    PORT1 = port1.intValue();
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new ClearPropagationDUnitTest().createCache(props);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory().addServer(host, PORT1).addServer(host, PORT2)
          .setSubscriptionEnabled(true).setReadTimeout(2000).setSocketBufferSize(1000)
          .setMinConnections(4)
          // .setRetryInterval(250)
          // .setRetryAttempts(2)
          .create("ClearPropagationDUnitTestPool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    factory.setCacheListener(new CacheListenerAdapter() {
      @Override
      public void afterRegionClear(RegionEvent re) {

        synchronized (ClearPropagationDUnitTest.class) {
          gotClear = true;
          ClearPropagationDUnitTest.class.notify();
        }
      }

      @Override
      public void afterRegionDestroy(RegionEvent re) {
        synchronized (ClearPropagationDUnitTest.class) {
          gotDestroyed = true;
          ClearPropagationDUnitTest.class.notify();
        }
      }
    });
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
  }

  public static Integer createServerCache() throws Exception {
    new ClearPropagationDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    CacheServer server = cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  public static void registerKeysK1andK2() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      List list = new ArrayList();
      list.add("key1");
      list.add("key2");
      r.registerInterest(list);

    } catch (Exception ex) {
      Assert.fail("failed while registering interest", ex);
    }
  }

  public static void verifyNoUpdates() {
    try {
      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      // verify no updates
      assertEquals("key-1", r.getEntry("key1").getValue());
      assertEquals("key-2", r.getEntry("key2").getValue());
    } catch (Exception ex) {
      Assert.fail("failed while verifyNoUpdates()", ex);
    }
  }

  public static void verifyUpdates() {
    try {

      Region r = cache.getRegion(SEPARATOR + REGION_NAME);
      assertNotNull(r);
      // verify updates
      assertNull(r.getEntry("key2").getValue());
      assertNull(r.getEntry("key1").getValue());

    } catch (Exception ex) {
      Assert.fail("failed while region", ex);
    }
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  @Override
  public final void preTearDown() throws Exception {
    // close client
    client1.invoke(() -> ClearPropagationDUnitTest.closeCache());
    client2.invoke(() -> ClearPropagationDUnitTest.closeCache());
    // close server
    server1.invoke(() -> ClearPropagationDUnitTest.closeCache());
    server2.invoke(() -> ClearPropagationDUnitTest.closeCache());
  }
}
