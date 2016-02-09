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

import org.junit.Ignore;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.ServerRegionProxy;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 *
 * Start client 1
 * Start client 2
 * Start Server 1
 * Start Server 2
 * Register interest for client 1 on Server 1/2
 * Kill Server 1
 * Verify that interest fails over to Server 2
 * Restart Server 1
 * Do a put which goes against Server 1
 * Verify that Client 1 does not get the update
 * Verify that Client 2 does get the update
 *
 * The key is to verify that the memberid being used by the client
 * to register with the server is the same across servers
 *
 *
 * @author Yogesh Mahajan
 * @author Asif
 *
 */

public class UpdatePropagationDUnitTest extends DistributedTestCase
{

  VM server1 = null;

  VM server2 = null;

  VM client1 = null;

  VM client2 = null;

  private static  int PORT1 ;

  private static  int PORT2 ;

  private static final String REGION_NAME = "UpdatePropagationDUnitTest_region";

  protected static Cache cache = null;
  
  static UpdatePropagationDUnitTest impl;

  /** constructor */
  public UpdatePropagationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();

    final Host host = Host.getHost(0);
    //Server1 VM
    server1 = host.getVM(0);

    //Server2 VM
    server2 = host.getVM(1);

    //Client 1 VM
    client1 = host.getVM(2);

    //client 2 VM
    client2 = host.getVM(3);
    
    createImpl();
    for (int i=0; i<4; i++) {
      host.getVM(i).invoke(getClass(), "createImpl", null);
    }

    PORT1 =  ((Integer)server1.invoke(getClass(), "createServerCache" )).intValue();
    PORT2 =  ((Integer)server2.invoke(getClass(), "createServerCache" )).intValue();

    client1.invoke(getClass(), "createClientCache", new Object[] {
      NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1),new Integer(PORT2)});
    client2.invoke(getClass(), "createClientCache", new Object[] {
      NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1),new Integer(PORT2)});
    
    IgnoredException.addIgnoredException("java.net.SocketException");
    IgnoredException.addIgnoredException("Unexpected IOException");

  }
  
  /** subclass support */
  public static void createImpl() {
    impl = new UpdatePropagationDUnitTest("temp");
  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  static private final String WAIT_PROPERTY =
    "UpdatePropagationDUnitTest.maxWaitTime";
  static private final int WAIT_DEFAULT = 60000;


  /**
   * This tests whether the updates are received by the sender or not if
   * there is an Interest List failover
   *
   */
  @Ignore("Bug 50405")
  public void DISABLED_testVerifyUpdatesNotReceivedBySender()
  {
    final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT).intValue();

    //First create entries on both servers via the two client
    client1.invoke(impl.getClass(), "createEntriesK1andK2");
    client2.invoke(impl.getClass(), "createEntriesK1andK2");
    client1.invoke(impl.getClass(), "registerKeysK1andK2");
    client2.invoke(impl.getClass(), "registerKeysK1andK2");
    //Induce fail over of InteretsList Endpoint to Server 2 by killing server1
    
    server1.invoke(UpdatePropagationDUnitTest.class, "killServer",
        new Object[] {new Integer(PORT1)});
    //Wait for 10 seconds to allow fail over. This would mean that Interest
    // has failed over to Server2.
    client1.invoke(new CacheSerializableRunnable("Wait for server on port1 to be dead") {
      public void run2() throws CacheException
      {
        Region r = cache.getRegion(REGION_NAME);

        try {
          r.put("ping", "pong1"); // in the event there is no live server monitor thread
        } catch (CacheWriterException itsOk) {}

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
        Wait.waitForCriterion(ev, maxWaitTime, 200, true);
      }
    });

    client2.invoke(new CacheSerializableRunnable("Wait for server on port1 to be dead") {
      public void run2() throws CacheException
      {
        Region r = cache.getRegion(REGION_NAME);

        try {
          r.put("ping", "pong3"); // in the event there is no live server monitor thread
        } catch (CacheWriterException itsOk) {}

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
        Wait.waitForCriterion(ev, maxWaitTime, 200, true);
      }
    });

    //Start Server1 again so that both clients1 & Client 2 will establish connection to server1 too.
    server1.invoke(UpdatePropagationDUnitTest.class, "startServer", new Object[] {new Integer(PORT1)});

    client1.invoke(new CacheSerializableRunnable("Wait for server on port1 to be dead") {
      public void run2() throws CacheException
      {
        Region r = cache.getRegion(REGION_NAME);

        try {
          r.put("ping", "pong2"); // in the event there is no live server monitor thread
        } catch (CacheWriterException itsOk) {}

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
        Wait.waitForCriterion(ev, maxWaitTime, 200, true);
      }
    });

    //Do a put on Server1 via Connection object from client1.
    // Client1 should not receive updated value while client2 should receive
    client1.invoke(impl.getClass(),
        "acquireConnectionsAndPutonK1andK2",
        new Object[] { NetworkUtils.getServerHostName(client1.getHost())});
    //pause(5000);
    //Check if both the puts ( on key1 & key2 ) have reached the servers
    server1.invoke(impl.getClass(), "verifyUpdates");
    server2.invoke(impl.getClass(), "verifyUpdates");
    // verify no updates for update originator
    client1.invoke(impl.getClass(), "verifyNoUpdates");

  }


  /**
   * This tests whether the updates are received by other clients or not , if there are
   * situation of Interest List fail over
   *
   */
  public void testVerifyUpdatesReceivedByOtherClients()
  {
    final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT).intValue();
    //  First create entries on both servers via the two client
    client1.invoke(impl.getClass(), "createEntriesK1andK2");
    client2.invoke(impl.getClass(), "createEntriesK1andK2");
    client1.invoke(impl.getClass(), "registerKeysK1andK2");
    client2.invoke(impl.getClass(), "registerKeysK1andK2");
    //Induce fail over of InteretsList Endpoint to Server 2 by killing server1
    server1.invoke(UpdatePropagationDUnitTest.class, "killServer", new Object[] {new Integer(PORT1)});
    //Wait for 10 seconds to allow fail over. This would mean that Interstist has failed
    // over to Server2.
    client1.invoke(new CacheSerializableRunnable("Wait for server on port1 to be dead") {
      public void run2() throws CacheException
      {
        Region r = cache.getRegion(REGION_NAME);

        try {
          r.put("ping", "pong3"); // in the event there is no live server monitor thread
        } catch (CacheWriterException itsOk) {}

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
        Wait.waitForCriterion(ev, maxWaitTime, 200, true);
      }
    });
    client2.invoke(new CacheSerializableRunnable("Wait for server on port1 to be dead") {
      public void run2() throws CacheException
      {
        Region r = cache.getRegion(REGION_NAME);

        try {
          r.put("ping", "pong3"); // in the event there is no live server monitor thread
        } catch (CacheWriterException itsOk) {}

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
        Wait.waitForCriterion(ev, maxWaitTime, 200, true);
      }
    });

    //Start Server1 again so that both clients1 & Client 2 will establish connection to server1 too.
    server1.invoke(UpdatePropagationDUnitTest.class, "startServer", new Object[] {new Integer(PORT1)});

    client1.invoke(new CacheSerializableRunnable("Wait for servers to be alive") {
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
        Wait.waitForCriterion(ev, maxWaitTime, 200, true);
      }
    });

    client2.invoke(new CacheSerializableRunnable("Wait for servers to be alive") {
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
        Wait.waitForCriterion(ev, maxWaitTime, 200, true);
      }
    });
    
    Wait.pause(5000);

    //Do a put on Server1 via Connection object from client1.
    // Client1 should not receive updated value while client2 should receive
    client1.invoke(impl.getClass(),
        "acquireConnectionsAndPutonK1andK2",
        new Object[] { NetworkUtils.getServerHostName(client1.getHost())});
    Wait.pause(5000);
    //Check if both the puts ( on key1 & key2 ) have reached the servers
    server1.invoke(impl.getClass(), "verifyUpdates");
    server2.invoke(impl.getClass(), "verifyUpdates");
    // verify updates to other client
    client2.invoke(impl.getClass(), "verifyUpdates");
  }

  public static void acquireConnectionsAndPutonK1andK2(String host)
  {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r1);
      String poolName = r1.getAttributes().getPoolName();
      assertNotNull(poolName);
      PoolImpl pool = (PoolImpl)PoolManager.find(poolName);
      assertNotNull(pool);

      Connection conn = pool.acquireConnection(new ServerLocation(host,PORT1));
      assertNotNull(conn);
      assertEquals(PORT1, conn.getServer().getPort());
      ServerRegionProxy srp = new ServerRegionProxy(Region.SEPARATOR+ REGION_NAME, pool);
      srp.putOnForTestsOnly(conn, "key1", "server-value1", new EventID(new byte[] {1},159632,1), null);
      srp.putOnForTestsOnly(conn, "key2", "server-value2", new EventID(new byte[] {1},159632,2), null);
    }
    catch (Exception ex) {
      fail("while setting acquireConnections  " + ex);
    }
  }

  public static void killServer(Integer port )
  {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        if(server.getPort() == port.intValue()){
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
        r1.put("key1", "key-1");
      }
      if (!r1.containsKey("key2")) {
        r1.put("key2", "key-2");
      }
      assertEquals(r1.get("key1"), "key-1");
      if (r1.getAttributes().getPartitionAttributes() == null) {
        assertEquals(r1.getEntry("key1").getValue(), "key-1");
        assertEquals(r1.getEntry("key2").getValue(), "key-2");
      }
      else {
        assertEquals(r1.get("key1"), "key-1");
        assertEquals(r1.get("key2"), "key-2");
      }
    }
    catch (Exception ex) {
      Assert.fail("failed while createEntriesK1andK2()", ex);
    }
  }

  public static void createClientCache(String host, Integer port1 , Integer port2 ) throws Exception
  {
    PORT1 = port1.intValue() ;
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new UpdatePropagationDUnitTest("temp").createCache(props);
    Pool p;
    try {
      System.setProperty("gemfire.PoolImpl.DISABLE_RANDOM", "true");
      p = PoolManager.createFactory()
        .addServer(host, PORT1)
        .addServer(host, PORT2)
        .setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(-1)
        .setMinConnections(4)
        .setSocketBufferSize(1000)
        .setReadTimeout(2000)
        // .setRetryInterval(250)
        // .setRetryAttempts(2)
        .create("UpdatePropagationDUnitTestPool");
    } finally {
      System.setProperty("gemfire.PoolImpl.DISABLE_RANDOM", "false");
      CacheServerTestUtil.enableShufflingOfEndpoints();
    }
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

  }

  public static Integer createServerCache() throws Exception
  {
    new UpdatePropagationDUnitTest("temp").createCache(new Properties());
    RegionAttributes attrs = impl.createCacheServerAttributes(); 
    cache.createRegion(REGION_NAME, attrs);
    CacheServer server = cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }
  
  protected RegionAttributes createCacheServerAttributes()
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    return factory.create();
  }

  public static void registerKeysK1andK2()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
      assertNotNull(r);
      List list = new ArrayList();
      list.add("key1");
      list.add("key2");
      r.registerInterest(list);

    }
    catch (Exception ex) {
      Assert.fail("failed while registering interest", ex);
    }
  }

  public static void verifyNoUpdates()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR+ REGION_NAME);
      assertNotNull(r);
      // verify no updates
      assertEquals("key-1", r.getEntry("key1").getValue());
      assertEquals("key-2", r.getEntry("key2").getValue());
    }
    catch (Exception ex) {
      Assert.fail("failed while verifyNoUpdates()", ex);
    }
  }

  public static void verifyUpdates()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      // verify updates
      if (r.getAttributes().getPartitionAttributes() == null) {
        assertEquals("server-value2", r.getEntry("key2").getValue());
        assertEquals("server-value1", r.getEntry("key1").getValue());
      }
      else {
        assertEquals("server-value2", r.get("key2"));
        assertEquals("server-value1", r.get("key1"));
      }
    }
    catch (Exception ex) {
      Assert.fail("failed while region", ex);
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
    //close client
    client1.invoke(getClass(), "closeCache");
    client2.invoke(getClass(), "closeCache");
    //close server
    server1.invoke(getClass(), "closeCache");
    server2.invoke(getClass(), "closeCache");
  }
}



