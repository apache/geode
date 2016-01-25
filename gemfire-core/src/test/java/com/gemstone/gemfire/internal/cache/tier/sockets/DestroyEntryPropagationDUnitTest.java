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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import util.TestException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CertifiableTestCacheListener;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.ServerRegionProxy;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.server.CacheServer;

/**
 * Tests propagation of destroy entry operation across the vms
 *
 * @author Yogesh Mahajan
 *
 */

public class DestroyEntryPropagationDUnitTest extends DistributedTestCase
{

  VM vm0 = null;

  VM vm1 = null;

  VM vm2 = null;

  VM vm3 = null;

  private static  int PORT1 ;
  private static  int PORT2;
  protected static Cache cache = null;

  private static final String REGION_NAME = "DestroyEntryPropagationDUnitTest_region";

  /** constructor */
  public DestroyEntryPropagationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
   // pause(5000);

    final Host host = Host.getHost(0);
    //Server1 VM
    vm0 = host.getVM(0);

    //Server2 VM
    vm1 = host.getVM(1);

    //Client 1 VM
    vm2 = host.getVM(2);

    //client 2 VM
    vm3 = host.getVM(3);

    PORT1 =  ((Integer)vm0.invoke(DestroyEntryPropagationDUnitTest.class, "createServerCache" )).intValue();
    PORT2 =  ((Integer)vm1.invoke(DestroyEntryPropagationDUnitTest.class, "createServerCache" )).intValue();

    vm2.invoke(DestroyEntryPropagationDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(Host.getHost(0)), new Integer(PORT1),new Integer(PORT2)});
    vm3.invoke(DestroyEntryPropagationDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(Host.getHost(0)), new Integer(PORT1),new Integer(PORT2)});

  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  /**
   * This tests whether the destroy are propagted or not according to interest registration.
   *
   */
  public void testDestroyPropagation()
  {
    //First create entries on both servers via the two clients
    vm2.invoke(DestroyEntryPropagationDUnitTest.class, "createEntriesK1andK2");
    vm3.invoke(DestroyEntryPropagationDUnitTest.class, "createEntriesK1andK2");

    // register interest for key-1
    vm2.invoke(DestroyEntryPropagationDUnitTest.class, "registerKey1");
    // register interest for key-1
    vm3.invoke(DestroyEntryPropagationDUnitTest.class, "registerKey1");

    //destroy entry key-1 , key-2
    vm2.invoke(DestroyEntryPropagationDUnitTest.class, "destroyEntriesK1andK2");
    // verify destroy entry on first server
    vm0.invoke(DestroyEntryPropagationDUnitTest.class,
        "verifyEntriesAreDestroyed");
    // verify destroy entry on second server
    vm1.invoke(DestroyEntryPropagationDUnitTest.class,
        "verifyEntriesAreDestroyed");
    //  verify destroy entry in originator vm
    vm2.invoke(DestroyEntryPropagationDUnitTest.class,
        "verifyEntriesAreDestroyed");
    // verify only key-1 is destroyed
    vm3.invoke(DestroyEntryPropagationDUnitTest.class,
        "verifyOnlyRegisteredEntriesAreDestroyed");

  }

  /**
   * This tests whether the destroy happened directly on server are propagted or
   * not.
   *
   */
  public void testDestroyOnServerPropagation()
  {
    //First create entries on both servers via the two client
    vm2.invoke(DestroyEntryPropagationDUnitTest.class, "createEntriesK1andK2");
    vm3.invoke(DestroyEntryPropagationDUnitTest.class, "createEntriesK1andK2");

    vm2.invoke(DestroyEntryPropagationDUnitTest.class, "registerKey1");
    vm3.invoke(DestroyEntryPropagationDUnitTest.class, "registerKey1");

    //destroy entry on server directly
    vm0.invoke(DestroyEntryPropagationDUnitTest.class, "destroyEntriesK1andK2");
    // verify destroy entry on server 1
    vm0.invoke(DestroyEntryPropagationDUnitTest.class,
        "verifyEntriesAreDestroyed");
    //  verify destroy entry on second server
    vm1.invoke(DestroyEntryPropagationDUnitTest.class,
        "verifyEntriesAreDestroyed");
    //  verify destroy entry only for registered keys in client1
    vm2.invoke(DestroyEntryPropagationDUnitTest.class,
        "verifyOnlyRegisteredEntriesAreDestroyed");
    //  verify destroy entry only for registered keys in client 2
    vm3.invoke(DestroyEntryPropagationDUnitTest.class,
        "verifyOnlyRegisteredEntriesAreDestroyed");

  }

  static private final String WAIT_PROPERTY =
    "DestroyEntryPropagationDUnitTest.maxWaitTime";
  static private final int WAIT_DEFAULT = 120000;

  /**
   * This tests whether the destroy are received by the sender or not if there
   * are situation of Interest List fail over
   *
   */
  public void testVerifyDestroyNotReceivedBySender()
  {
    final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT).intValue();
    //First create entries on both servers via the two client
    vm2.invoke(DestroyEntryPropagationDUnitTest.class, "createEntriesK1andK2");
    vm3.invoke(DestroyEntryPropagationDUnitTest.class, "createEntriesK1andK2");
    vm2.invoke(DestroyEntryPropagationDUnitTest.class, "registerKey1");
    vm3.invoke(DestroyEntryPropagationDUnitTest.class, "registerKey1");
    //Induce fail over of InterestList Endpoint to Server 2 by killing server1
    vm0.invoke(DestroyEntryPropagationDUnitTest.class, "killServer", new Object[]{new Integer(PORT1) });
    //Wait for 10 seconds to allow fail over. This would mean that Interest
    // has failed over to Server2.
    vm2.invoke(new CacheSerializableRunnable("Wait for server on port1 to be dead") {
      public void run2() throws CacheException
      {
        Region r = cache.getRegion(REGION_NAME);

        try {
          r.put("ping", "pong1"); // Used in the case where we don't have a LiveServerMonitorThread
        } catch (CacheWriterException itsOK) {}
        try {
          r.put("ping", "pong1"); // Used in the case where we don't have a LiveServerMonitorThread
        } catch (CacheWriterException itsOK) {}

        String poolName = r.getAttributes().getPoolName();
        assertNotNull(poolName);
        final PoolImpl pool = (PoolImpl)PoolManager.find(poolName);
        assertNotNull(pool);
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return pool.getConnectedServerCount() != 2;
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, maxWaitTime, 200, true);
      }
    });

    //Start Server1 again so that both clients1 & Client 2 will establish
    // connection to server1 too.
    vm0.invoke(DestroyEntryPropagationDUnitTest.class, "startServer", new Object[]{new Integer(PORT1) });

    vm2.invoke(new CacheSerializableRunnable("Wait for server on port1 to spring to life") {
      public void run2() throws CacheException
      {
        Region r = cache.getRegion(REGION_NAME);
        String poolName = r.getAttributes().getPoolName();
        assertNotNull(poolName);
        final PoolImpl pool = (PoolImpl)PoolManager.find(poolName);
        assertNotNull(pool);
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return pool.getConnectedServerCount() == 2;
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, maxWaitTime, 200, true);
      }
    });

    //Do a destroy on Server1 via Connection object from client1.
    // Client1 should not receive updated value while client2 should receive
    vm2.invoke(DestroyEntryPropagationDUnitTest.class,
        "acquireConnectionsAndDestroyEntriesK1andK2");
   // pause(10000);
    //  Check if both the puts ( on key1 & key2 ) have reached the servers
    vm0.invoke(DestroyEntryPropagationDUnitTest.class,
        "verifyEntriesAreDestroyed");
    vm1.invoke(DestroyEntryPropagationDUnitTest.class,
        "verifyEntriesAreDestroyed");

    vm2.invoke(DestroyEntryPropagationDUnitTest.class,
        "verifyNoDestroyEntryInSender");

  }

  public static void acquireConnectionsAndDestroyEntriesK1andK2()
  {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR+REGION_NAME);
      assertNotNull(r1);
      String poolName = r1.getAttributes().getPoolName();
      assertNotNull(poolName);
      PoolImpl pool = (PoolImpl)PoolManager.find(poolName);
      assertNotNull(pool);
      Connection conn = pool.acquireConnection();
      final Connection conn1;
      if (conn.getServer().getPort() != PORT2) {
        conn1 = pool.acquireConnection(); // Ensure we have a server with the proper port
      } else {
        conn1 = conn;
      }
      assertNotNull(conn1);
      assertEquals(PORT2, conn1.getServer().getPort());
      ServerRegionProxy srp = new ServerRegionProxy(Region.SEPARATOR+REGION_NAME, pool);
      srp.destroyOnForTestsOnly(conn1, "key1", null, Operation.DESTROY, new EntryEventImpl(new EventID(new byte[] {1},100000,1)), null);
      srp.destroyOnForTestsOnly(conn1, "key2", null, Operation.DESTROY, new EntryEventImpl(new EventID(new byte[] {1},100000,2)), null);
    }
    catch (Exception ex) {
      throw new TestException("Failed while setting acquireConnectionsAndDestroyEntry  ", ex);
    }
  }

  public static void killServer(Integer port)
  {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      getLogWriter().fine ("Asif: servers running = "+cache.getCacheServers().size());
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        getLogWriter().fine("asif : server running on port="+server.getPort()+ " asked to kill serevre onport="+port);
         if(port.intValue() == server.getPort()){
         server.stop();
        }
      }
    }
    catch (Exception ex) {
      fail("while killing Server  " + ex);
    }
  }

  public static void startServer(Integer port)
  {
    try {
      CacheServer server1 = cache.addCacheServer();
      server1.setPort(port.intValue());
      server1.setNotifyBySubscription(true);
      server1.start();
    }
    catch (Exception ex) {
      fail("while killServer  " + ex);
    }
  }

  /**
   * Creates entries on the server
   *
   */
  public static void createEntriesK1andK2()
  {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR+REGION_NAME);
      assertNotNull(r1);
      if (!r1.containsKey("key1")) {
        r1.create("key1", "key-1");
      }
      if (!r1.containsKey("key2")) {
        r1.create("key2", "key-2");
      }
      assertEquals(r1.getEntry("key1").getValue(), "key-1");
      assertEquals(r1.getEntry("key2").getValue(), "key-2");
    }
    catch (Exception ex) {
      fail("failed while createEntries()", ex);
    }
  }

  /**
   * destroy entry
   *
   */
  public static void destroyEntriesK1andK2()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
      assertNotNull(r);
      r.destroy("key1");
      r.destroy("key2");
    }
    catch (Exception ex) {
      fail("failed while destroyEntry()", ex);
    }
  }

  public static void verifyNoDestroyEntryInSender()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
      assertNotNull(r);
      assertNotNull(r.getEntry("key1"));
      assertNotNull(r.getEntry("key2"));
    }
    catch (Exception ex) {
      fail("failed while verifyDestroyEntry in C1", ex);
    }
  }

  public static void verifyEntriesAreDestroyed()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
      assertNotNull(r);
      waitForDestroyEvent(r, "key1");
      assertNull(r.getEntry("key1"));
      assertNull(r.getEntry("key2"));
    }
    catch (Exception ex) {
      fail("failed while verifyDestroyEntry in C1", ex);
    }
  }

  public static void verifyOnlyRegisteredEntriesAreDestroyed()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
      assertNotNull(r);
      waitForDestroyEvent(r, "key1");
      assertNull(r.getEntry("key1"));
      assertNotNull(r.getEntry("key2"));
    }
    catch (Exception ex) {
      fail("failed while verifyDestroyEntry for key1", ex);
    }
  }

  public static void waitForDestroyEvent(Region r, final Object key) {
    final CertifiableTestCacheListener ccl = (CertifiableTestCacheListener) r.getAttributes().getCacheListener();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return ccl.destroys.contains(key);
      }
      public String description() {
        return "waiting for destroy event for " + key;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
    ccl.destroys.remove(key);
  }

  public static void createClientCache(String host, Integer port1, Integer port2) throws Exception
  {
    PORT1 = port1.intValue();
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new DestroyEntryPropagationDUnitTest("temp").createCache(props);
    CacheServerTestUtil.disableShufflingOfEndpoints();
    Pool p;
    try {
      p = PoolManager.createFactory()
        .addServer(host, PORT1)
        .addServer(host, PORT2)
        .setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1)
        .setReadTimeout(2000)
        .setSocketBufferSize(1000)
        .setMinConnections(4)
        // .setRetryAttempts(2)
        // .setRetryInterval(250)
        .create("EntryPropagationDUnitTestPool");
    } finally {
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    factory.setCacheListener(new CertifiableTestCacheListener(getLogWriter()));
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

  }

  public static Integer createServerCache() throws Exception
  {
    new DestroyEntryPropagationDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setCacheListener(new CertifiableTestCacheListener(getLogWriter()));
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  /**
   *
   * @param key
   *          Key in which client is interested
   */
  public static void registerKey1()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR+REGION_NAME);
      assertNotNull(r);
      List list = new ArrayList();
      list.add("key1");
      r.registerInterest(list);

    }
    catch (Exception ex) {
      fail("failed while registering interest", ex);
    }
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public void tearDown2() throws Exception
  {
    //close client
    vm2.invoke(DestroyEntryPropagationDUnitTest.class, "closeCache");
    vm3.invoke(DestroyEntryPropagationDUnitTest.class, "closeCache");
    //close server
    vm0.invoke(DestroyEntryPropagationDUnitTest.class, "closeCache");
    vm1.invoke(DestroyEntryPropagationDUnitTest.class, "closeCache");

  }

}
