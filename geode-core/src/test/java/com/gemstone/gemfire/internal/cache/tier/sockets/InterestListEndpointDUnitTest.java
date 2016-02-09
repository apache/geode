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


import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.Assert;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.QueueConnectionImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 *
 * @author Yogesh Mahajan
 * @author Suyog Bhokare
 *
 */
public class InterestListEndpointDUnitTest extends DistributedTestCase
{

  static VM server1 = null;

  static VM server2 = null;

  static VM client1 = null;

  protected static Cache cache = null;
  private static int PORT1;
  private static int PORT2;

  private static Connection conn1 ;
  private static PoolImpl pool;
  private static final String REGION_NAME = "InterestListEndpointDUnitTest_region";

  private static final String k1 = "k1";
  private static final String k2 = "k2";
  private static final String client_k1 = "client-k1";
  private static final String client_k2 = "client-k2";
  private static final String server_k1 = "server-k1";
  private static final String server_k2 = "server-k2";

  static InterestListEndpointDUnitTest impl;

  /** constructor */
  public InterestListEndpointDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    Wait.pause(5000);
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);

    createImpl();
    for (int i=0; i<4; i++) {
      host.getVM(i).invoke(getClass(), "createImpl", null);
    }

    // create servers first
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    // then create client
    Wait.pause(5000);  // [bruce] avoid ConnectException
    client1.invoke(impl.getClass(), "createClientCache", new Object[] {
      NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1),new Integer(PORT2)});

  }

  /** subclass support */
  public static void createImpl() {
    impl = new InterestListEndpointDUnitTest("temp");
  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);

  }
  /**
   * tests wheteher upadets are sent to clients if put on server directly
   *
   */
  public void testDirectPutOnServer()
  {
    client1.invoke(impl.getClass(), "createEntriesK1andK2");
    server1.invoke(impl.getClass(), "createEntriesK1andK2");
    server2.invoke(impl.getClass(), "createEntriesK1andK2");

    client1.invoke(impl.getClass(), "registerKey1");
    //directly put on server
    server1.invoke(impl.getClass(), "put");
    client1.invoke(impl.getClass(), "verifyPut");
  }
 /**
  * put on non interest list ep and verify updates
  *
  */
  public void testInterestListEndpoint()
  {
    client1.invoke(impl.getClass(), "createEntriesK1andK2");
    server2.invoke(impl.getClass(), "createEntriesK1andK2"); // server
    server1.invoke(impl.getClass(), "createEntriesK1andK2"); // server

    client1.invoke(impl.getClass(), "registerKey1");

    server1.invoke(impl.getClass(), "verifyIfNotInterestListEndpointAndThenPut");
    server2.invoke(impl.getClass(), "verifyIfNotInterestListEndpointAndThenPut");
    client1.invoke(impl.getClass(), "verifyPut");
  }

  public void testInterestListEndpointAfterFailover() throws Exception
  {
    final long maxWaitTime = 20000;
    client1.invoke(impl.getClass(), "createEntriesK1andK2");
    server2.invoke(impl.getClass(), "createEntriesK1andK2");
    server1.invoke(impl.getClass(), "createEntriesK1andK2");

    client1.invoke(impl.getClass(), "registerKey1");

    boolean firstIsPrimary = isVm0Primary();
    VM primary = firstIsPrimary? server1 : server2;

    primary.invoke(impl.getClass(), "stopILEndpointServer");
    Wait.pause(5000);

    //Since the loadbalancing policy is roundrobin & there are two servers so
    // do two dumb puts, which will ensure that fail over happens from the
    // interest list end point in case Live ServerMonitor is not working
    client1.invoke(new CacheSerializableRunnable("Ensure that the failover from ILEP occurs") {
      public void run2() throws CacheException
      {
        Region r = cache.getRegion("/"+REGION_NAME);

        String poolName = r.getAttributes().getPoolName();
        assertNotNull(poolName);
        final PoolImpl pool = (PoolImpl)PoolManager.find(poolName);
        assertNotNull(pool);
        pool.acquireConnection();
        try {
          r.put("ping", "pong1"); // Used in the case where we don't have a LiveServerMonitorThread

        } catch (CacheWriterException itsOK) {}

        try {
          r.put("ping", "pong2"); // Used in the case where we don't have a LiveServerMonitorThread

        } catch (CacheWriterException itsOK) {}

        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return pool.getConnectedServerCount() != 2;
          }
          public String description() {
            return null;
          }
        };
        Wait.waitForCriterion(ev, maxWaitTime, 200, true);
      }
    });

    //put on stopped server
    primary.invoke(impl.getClass(), "put");
    client1.invoke(impl.getClass(), "verifyPut");
  }


  public static boolean isVm0Primary() throws Exception {
    int port = ((Integer)client1.invoke(impl.getClass(), "getPrimaryPort")).intValue();
    return port == PORT1;
  }

  public static int getPrimaryPort() throws Exception {
    Region r1 = cache.getRegion("/" + REGION_NAME);
    String poolName = r1.getAttributes().getPoolName();
    assertNotNull(poolName);
    pool = (PoolImpl)PoolManager.find(poolName);
    assertNotNull(pool);
    assertTrue(pool.getPrimaryName() != null);
    return pool.getPrimaryPort();
  }


 public void testUpdaterThreadIsAliveForFailedEndPoint(){
      client1.invoke(impl.getClass(), "acquirePoolConnection");
      client1.invoke(impl.getClass(), "processException");
      client1.invoke(impl.getClass(), "verifyUpdaterThreadIsAlive");
 }

 public static void acquirePoolConnection()
 {
   try {
     Region r1 = cache.getRegion("/"+REGION_NAME);
     assertNotNull(r1);
     String poolName = r1.getAttributes().getPoolName();
     assertNotNull(poolName);
     pool = (PoolImpl)PoolManager.find(poolName);
     assertNotNull(pool);
     conn1 = pool.getPrimaryConnection();
     assertNotNull(conn1);
   }
   catch (Exception ex) {
     throw new RuntimeException("Exception while setting acquireConnections  ", ex);
   }
 }
 public static void processException()
 {
   try {
     pool.processException(new IOException(),conn1);
   }
   catch (Exception ex) {
     throw new RuntimeException("Exception while setting processException  ", ex);
   }
 }
 public static void verifyUpdaterThreadIsAlive() throws InterruptedException
 {
   QueueConnectionImpl conn2 = (QueueConnectionImpl) pool.getPrimaryConnection();
   Assert.assertNotSame(conn1, conn2);
   Assert.assertFalse(conn1.getServer().equals(conn2.getServer()));
   assertNull(((QueueConnectionImpl)conn1).getUpdater());
   assertTrue((conn2).getUpdater().isAlive());
 }

  public static void stopILEndpointServer()
  {
  try {
        Cache c = CacheFactory.getAnyInstance();
        assertEquals("More than one BridgeServer", 1, c.getCacheServers().size());
        CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
        assertNotNull(bs);
        assertNotNull(bs.getAcceptor());
        assertNotNull(bs.getAcceptor().getCacheClientNotifier());
  Iterator iter_prox = bs.getAcceptor().getCacheClientNotifier().getClientProxies().iterator();
  if (iter_prox.hasNext()) {
       CacheClientProxy proxy = (CacheClientProxy)iter_prox.next();
       //if (proxy._interestList._keysOfInterest.get("/"+REGION_NAME) != null) {
       if(proxy.isPrimary()){
          Iterator iter = cache.getCacheServers().iterator();
          if (iter.hasNext()) {
            CacheServer server = (CacheServer)iter.next();
                  cache.getLogger().fine("stopping server " + server);
            server.stop();
          }
       }
    }
  }
    catch (Exception ex) {
      throw new RuntimeException("Exception while setting stopServer  ", ex);
    }
  }

  public static void createEntriesK1andK2()
  {
    try {
      Region r1 = cache.getRegion("/"+ REGION_NAME);
      assertNotNull(r1);
      if (!r1.containsKey(k1)) {
        r1.create(k1, client_k1);
      }
      if (!r1.containsKey(k2)) {
        r1.create(k2, client_k2);
      }
      if (r1.getAttributes().getPartitionAttributes() == null) {
        assertEquals(r1.getEntry(k1).getValue(), client_k1);
        assertEquals(r1.getEntry(k2).getValue(), client_k2);
      }
    }
    catch (Exception ex) {
      throw new RuntimeException("failed while createEntries()", ex);
    }
  }

  public static void createClientCache(String host, Integer port1, Integer port2) throws Exception
  {
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new InterestListEndpointDUnitTest("temp").createCache(props);
    Pool p;
    try {
      p = PoolManager.createFactory()
        .addServer(host, port1.intValue())
        .addServer(host, port2.intValue())
        .setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1)
        .setMinConnections(6)
        .setSocketBufferSize(32768)
        .setReadTimeout(2000)
        // .setRetryInterval(1000)
        // .setRetryAttempts(5)
        .create("InterestListEndpointDUnitTestPool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

  }

  protected int getMaxThreads() {
    return 0;
  }

  private int initServerCache(VM server) {
    Object[] args = new Object[] {new Integer(getMaxThreads())};
    return ((Integer)server.invoke(InterestListEndpointDUnitTest.class,
                                   "createServerCache",
                                   args)).intValue();
  }

  public static Integer createServerCache(Integer maxThreads) throws Exception
  {
    new InterestListEndpointDUnitTest("temp").createCache(new Properties());
    RegionAttributes attrs = impl.createServerCacheAttributes();
    cache.createRegion(REGION_NAME, attrs);
    CacheServer server1 = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    server1.setPort(port);
    server1.setMaxThreads(maxThreads.intValue());
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(server1.getPort());
  }

  protected RegionAttributes createServerCacheAttributes()
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    return factory.create();
  }

  public static void put()
  {
    try {
      Region r = cache.getRegion("/"+REGION_NAME);
      assertNotNull(r);
      r.put(k1, server_k1);
      r.put(k2, server_k2);
    }
    catch (Exception ex) {
      throw new RuntimeException("failed while region.put()", ex);
    }
  }

  public static void verifyIfNotInterestListEndpointAndThenPut()
  {
    try {
      Cache c = CacheFactory.getAnyInstance();
      assertEquals("More than one CacheServer", 1, c.getCacheServers().size());
      CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      Iterator iter = bs.getAcceptor().getCacheClientNotifier().getClientProxies().iterator();
      //only one server thats why if and not while
      if (iter.hasNext()) {
        CacheClientProxy proxy = (CacheClientProxy)iter.next();
        //if (proxy._interestList._keysOfInterest.get("/"+ REGION_NAME) == null) {
        if(! proxy.isPrimary()){
          Region r = cache.getRegion("/"+REGION_NAME);
          r.put(k1, server_k1);
          r.put(k2, server_k2);
        }
      }
    }
    catch (Exception ex) {
      com.gemstone.gemfire.test.dunit.Assert.fail("failed while verifyIfNotInterestListEndpointAndThenPut()", ex);
    }
  }

  public static void registerKey1()
  {
    try {
      Region r = cache.getRegion("/"+REGION_NAME);
      assertNotNull(r);
      r.registerInterest(k1, InterestResultPolicy.KEYS);
    }
    catch (Exception ex) {
      com.gemstone.gemfire.test.dunit.Assert.fail("failed while region.registerInterest()", ex);
    }
  }

  public static void verifyPut()
  {
    try {
      final Region r = cache.getRegion("/"+ REGION_NAME);
      assertNotNull(r);
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          Region.Entry e1 = r.getEntry(k1);
          if (e1 == null) return false;
          Region.Entry e2 = r.getEntry(k2);
          if (e2 == null) return false;
          Object v1 = e1.getValue();
          if (!server_k1.equals(v1)) return false;
          Object v2 = e2.getValue();
          if (!client_k2.equals(v2)) return false;
          // our state is ready for the assertions
          return true;
        }
        public String description() {
          Region.Entry e1 = r.getEntry(k1);
          if (e1 == null) return "Entry for " + k1 + " is null";
          Region.Entry e2 = r.getEntry(k2);
          if (e2 == null) return "Entry for " + k2 + " is null";
          Object v1 = e1.getValue();
          if (!server_k1.equals(v1)) {
            return "v1 supposed to be " + server_k1 + " but is " + v1;
          }
          Object v2 = e2.getValue();
          if (!client_k2.equals(v2)) {
            return "v2 supposed to be " + client_k2 + " but is " + v2;
          }
          return "Test missed a success";
        }
      };
      Wait.waitForCriterion(ev, 20 * 1000, 200, true);
      
      //yes update
      assertEquals(server_k1, r.getEntry(k1).getValue());
      //no update
      assertEquals(client_k2, r.getEntry(k2).getValue());
    }
    catch (Exception ex) {
      throw new RuntimeException("failed while region.verifyPut()", ex);
    }
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  @Override
  protected final void preTearDown() throws Exception {
    // Close client cache first, then server caches
    client1.invoke(impl.getClass(), "closeCache");
    server2.invoke(impl.getClass(), "closeCache");
    server1.invoke(impl.getClass(), "closeCache");
    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
  }
}
