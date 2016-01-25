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
package com.gemstone.gemfire.internal.cache;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.ServerRegionProxy;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This is the Bug test for the bug 36773. Skipped sequence id causes missing
 * entry in client. The test perform following operations in sequence. 
 * 1. Create Server1 & Server2 
 * 2. Create Client1 & Client2 
 * 3. Perform put operation from the client1 on key1 & key2 such a way that 
 * put with lower sequence id reaches primary after the put with greater sequence id.
 * 4. Check for the keys on Client2
 * 
 * @author Girish Thombare
 * 
 */

public class HABug36773DUnitTest extends DistributedTestCase
{

  VM server1 = null;

  VM server2 = null;

  VM client1 = null;

  VM client2 = null;

  private static int PORT1;

  private static int PORT2;

  private static final String REGION_NAME = "HABug36773DUnitTest_region";
  
  static final String KEY1 = "key1";
  
  static final String KEY2 = "key2";
  
  static final String VALUE1 = "newVal1";
  
  static final String VALUE2 = "newVal2";
  
  static volatile boolean waitFlag = true;
 
  protected static Cache cache = null;

  /** constructor */
  public HABug36773DUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
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

    PORT1 = ((Integer)server1.invoke(HABug36773DUnitTest.class,
        "createServerCache")).intValue();
    PORT2 = ((Integer)server2.invoke(HABug36773DUnitTest.class,
        "createServerCache")).intValue();
    client1.invoke(HABug36773DUnitTest.class, "disableShufflingOfEndpoints");
    client2.invoke(HABug36773DUnitTest.class, "disableShufflingOfEndpoints");
    client1.invoke(HABug36773DUnitTest.class, "createClientCache",
        new Object[] { new Integer(PORT1), new Integer(PORT2) });
    client2.invoke(HABug36773DUnitTest.class, "createClientCache",
        new Object[] { new Integer(PORT1), new Integer(PORT2) });

  }

  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void disableShufflingOfEndpoints()
  {
    System.setProperty("gemfire.bridge.disableShufflingOfEndpoints", "true");
  }

  public void _testBug36773() throws Exception
  {
    //First create entries on both servers via the two client
    client1.invoke(HABug36773DUnitTest.class, "createEntriesK1andK2");
    client2.invoke(HABug36773DUnitTest.class, "createEntriesK1andK2");
    client1.invoke(HABug36773DUnitTest.class, "registerKeysK1andK2");
    client2.invoke(HABug36773DUnitTest.class, "registerKeysK1andK2");

    server1.invoke(checkSizeRegion(2));
    server2.invoke(checkSizeRegion(2));
    client1.invoke(checkSizeRegion(2));
    client2.invoke(checkSizeRegion(2));

    server1.invoke(HABug36773DUnitTest.class, "waitOnTheKeyEntry");
    client1.invoke(HABug36773DUnitTest.class,
        "acquireConnectionsAndPut", new Object[] {new Integer(PORT2)});
    client1.invoke(HABug36773DUnitTest.class,
        "acquireConnectionsAndPut", new Object[] {new Integer(PORT1)});
      client2.invoke(HABug36773DUnitTest.class, "verifyEntries", new Object[] {new String(KEY2), new String (VALUE2)});
    server1.invoke(HABug36773DUnitTest.class, "notfiyThread");
    client2.invoke(HABug36773DUnitTest.class, "verifyEntries", new Object[] {new String(KEY1), new String (VALUE1)});
  
  }

  public void testDummyForBug36773()
  {
    getLogWriter().info(" This is the dummy test for the Bug 36773");
    
  }
  

  private CacheSerializableRunnable checkSizeRegion(final int size)
  {

    CacheSerializableRunnable checkRegion = new CacheSerializableRunnable(
        "checkSize") {

      public void run2() throws CacheException
      {
        Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
        assertNotNull(region);
        getLogWriter().info("Size of the region " + region.size());
        assertEquals(size, region.size());
      }
    };
    return checkRegion;
  }

  public static void acquireConnectionsAndPut(Integer portNumber)
  {
    try {
      int port = portNumber.intValue();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r1);
      PoolImpl pool = (PoolImpl)PoolManager.find(r1.getAttributes().getPoolName());
      assertNotNull(pool);
      Connection conn = pool.acquireConnection();
      final Connection conn1;
      ServerRegionProxy srp = new ServerRegionProxy(Region.SEPARATOR + REGION_NAME, pool);

      if (conn.getServer().getPort() != port) {
        conn1 = pool.acquireConnection(); // Ensure we have a server
        // with the
        // proper port
      }
      else {
        conn1 = conn;
      }
      assertNotNull(conn1);
      if (port == PORT2) {
        assertEquals(PORT2, conn1.getServer().getPort());
        srp.putOnForTestsOnly(conn1, KEY1, VALUE1, new EventID(new byte[] { 1 }, 2, 1), null);
      }
      else if (port == PORT1) {
        assertEquals(PORT1, conn1.getServer().getPort());
        srp.putOnForTestsOnly(conn1, KEY2, VALUE2, new EventID(new byte[] { 1 }, 2, 2), null);
      }
      else {
        fail("Invalid ports ");
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("while setting acquireConnections  " + ex);
    }
  }
  
  

  public static void verifyEntries(String KEY, String VALUE)
  {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    long maxWaitTime = 120000;
    try {
      long start = System.currentTimeMillis();
      while (!r1.getEntry(KEY).getValue().equals(VALUE)) {
        assertTrue("Waited over " + maxWaitTime + "entry to get updated",
            (System.currentTimeMillis() - start) < maxWaitTime);
        try {
          Thread.yield();
          Thread.sleep(700);
        }
        catch (InterruptedException ie) {
          fail("Interrupted while waiting ", ie);
        }
      }
    }
    catch (Exception e) {
      fail("Exception in trying to get due to " + e);
    }
  }
  


  public static void createEntriesK1andK2()
  {
    try {
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r1);
      if (!r1.containsKey(KEY1)) {
        r1.create(KEY1, "key-1");
      }
      if (!r1.containsKey(KEY2)) {
        r1.create(KEY2, "key-2");
      }
      assertEquals(r1.getEntry(KEY1).getValue(), "key-1");
      assertEquals(r1.getEntry(KEY2).getValue(), "key-2");
    }
    catch (Exception ex) {
      fail("failed while createEntriesK1andK2()", ex);
    }
  }
  
  public static void notfiyThread()
  {
    waitFlag=false; 
  }

  public static void createClientCache(Integer port1, Integer port2)
      throws Exception
  {
    PORT1 = port1.intValue();
    PORT2 = port2.intValue();
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    new HABug36773DUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ClientServerTestCase.configureConnectionPool(factory, DistributedTestCase.getIPLiteral(), new int[] {PORT1,PORT2}, true, -1, 2, null);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
  }

  public static Integer createServerCache()
      throws Exception
  {
    new HABug36773DUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setEarlyAck(true);
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

  /**
   *
   * @param key Key in which client is interested
   */
  public static void registerKeysK1andK2()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      List list = new ArrayList();
      list.add(KEY1);
      list.add(KEY2);
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

  public static void waitOnTheKeyEntry()
  {
    Thread thrd = new Thread() {
      public void run()
      {
        LocalRegion region = (LocalRegion)cache.getRegion(Region.SEPARATOR
            + REGION_NAME);
        RegionEntry regionEntry = region.basicGetEntry(KEY1);
        synchronized (regionEntry) {
          while (waitFlag) {
            try {
              Thread.sleep(500);
            }
            catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }
      }
    };

    thrd.start();

  }
  
  public void tearDown2() throws Exception
  {
    //close client
    client1.invoke(HABug36773DUnitTest.class, "closeCache");
    client2.invoke(HABug36773DUnitTest.class, "closeCache");
    //close server
    server1.invoke(HABug36773DUnitTest.class, "closeCache");
    server2.invoke(HABug36773DUnitTest.class, "closeCache");

  }
  
}

