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
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * Tests behaviour of transactions in client server model
 */
public class CacheServerTransactionsDUnitTest extends DistributedTestCase
{
  /** constructor */
  public CacheServerTransactionsDUnitTest(String name) {
    super(name);
  }

  private static Cache cache = null;

  private static PoolImpl pool = null;

  private static final String k1 = "k1";

  private static final String k2 = "k2";

  private static final String k3 = "k3";

  private static final String server1_k1 = "server1-k1";

  private static final String server1_k2 = "server1-k2";

  private static final String server2_k3 = "server2-k3";

  private static final String server2_k2 = "server2-k2";
  
  private static final String client_k2 = "client-k2";
  
  private static final String client_k1 = "client-k1";

  private static final String REGION_NAME = "CacheServerTransactionsDUnitTest_region";
  
  private static Host host = null;

  private static VM server1 = null;

  private static VM server2 = null;

  private static VM client1 = null;

  private static VM client2 = null;

//  private static RegionAttributes attrs = null;

  protected static boolean destroyed = false;
  
  protected static boolean invalidated = false;

  public void setUp() throws Exception
  {
    super.setUp();
    host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);
  }

  private static final int PAUSE = 5 * 1000;

  /**
   * Test for update propagation to the clients when there is one server and two
   * clients connected to the server.
   * 
   */
  public void testOneServerToClientTransactionsPropagation()
  {
    Integer port1 = initServerCache(server1);
    client1.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1 });
    client2.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1 });
    Wait.pause(PAUSE);

    server1.invoke(resetFlags());
    client1.invoke(resetFlags());
    client2.invoke(resetFlags());

    server1.invoke(CacheServerTransactionsDUnitTest.class, "putInTransaction",
        new Object[] { "server1" });
    Wait.pause(PAUSE);
    
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyNotUpdated");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyNotUpdated");

    server1.invoke(CacheServerTransactionsDUnitTest.class,
        "commitTransactionOnServer1");
    Wait.pause(PAUSE);

    server1.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdates");
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdates");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdates");
  }

  

  /**
   * Test for update propagation to the clients when there are  2 servers and two
   * clients connected to both the servers.
   * 
   */
  public void testServerToClientTransactionsPropagation()
  {
    Integer port1 = initServerCache(server1);
    Integer port2 = initServerCache(server2);
    client1.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1, port2 });
    client2.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1, port2 });
    Wait.pause(PAUSE);

    server1.invoke(resetFlags());
    server2.invoke(resetFlags());
    client1.invoke(resetFlags());
    client2.invoke(resetFlags());

    server1.invoke(CacheServerTransactionsDUnitTest.class, "putInTransaction",
        new Object[] { "server1" });
    Wait.pause(PAUSE);

    server2.invoke(CacheServerTransactionsDUnitTest.class, "verifyNotUpdated");
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyNotUpdated");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyNotUpdated");

    server1.invoke(CacheServerTransactionsDUnitTest.class,
        "commitTransactionOnServer1");
    Wait.pause(PAUSE);

    server1.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdates");
    server2.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdates");
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdates");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdates");
  }
  
  /**
   * Test for update propagation to the clients when there are  2 servers and two
   * clients connected to separate server.
   * 
   */
  public void testServerToClientTransactionsPropagationWithOneClientConnectedToOneServer()
  {
    Integer port1 = initServerCache(server1);
    Integer port2 = initServerCache(server2);
    client1.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1 });
    client2.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port2 });
    Wait.pause(PAUSE);

    server1.invoke(resetFlags());
    server2.invoke(resetFlags());
    client1.invoke(resetFlags());
    client2.invoke(resetFlags());

    server1.invoke(CacheServerTransactionsDUnitTest.class, "putInTransaction",
        new Object[] { "server1" });
    Wait.pause(PAUSE);

    server2.invoke(CacheServerTransactionsDUnitTest.class, "verifyNotUpdated");
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyNotUpdated");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyNotUpdated");

    server1.invoke(CacheServerTransactionsDUnitTest.class,
        "commitTransactionOnServer1");
    Wait.pause(PAUSE);

    server1.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdates");
    server2.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdates");
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdates");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdates");
  }
  
  /**
   * Test for invalidate propagation to the clients when there is one server and two
   * clients connected to the server.
   */
  public void testInvalidatesOneServerToClientTransactionsPropagation()
  {
    Integer port1 = initServerCache(server1);
    client1.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1 });
    client2.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1 });
    Wait.pause(PAUSE);

    server1.invoke(resetFlags());
    client1.invoke(resetFlags());
    client2.invoke(resetFlags());

    server1.invoke(CacheServerTransactionsDUnitTest.class, "invalidateInTransaction",
        new Object[] { "server1" });
    server1.invoke(CacheServerTransactionsDUnitTest.class,
        "commitTransactionOnServer1");
    Wait.pause(PAUSE);

    server1.invoke(CacheServerTransactionsDUnitTest.class, "verifyInvalidates");
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyInvalidates");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyInvalidates");
  }
  
  /**
   * Test for invalidate propagation to the clients when there are  2 servers and two
   * clients connected to both servers.
   */
  public void testInvalidatesServerToClientTransactionsPropagation()
  {
    Integer port1 = initServerCache(server1);
    Integer port2 = initServerCache(server2);
    client1.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1, port2 });
    client2.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1, port2 });
    Wait.pause(PAUSE);

    server1.invoke(resetFlags());
    server2.invoke(resetFlags());
    client1.invoke(resetFlags());
    client2.invoke(resetFlags());

    server1.invoke(CacheServerTransactionsDUnitTest.class, "invalidateInTransaction",
        new Object[] { "server1" });
    server1.invoke(CacheServerTransactionsDUnitTest.class,
        "commitTransactionOnServer1");
    Wait.pause(PAUSE);

    server1.invoke(CacheServerTransactionsDUnitTest.class, "verifyInvalidates");
    server2.invoke(CacheServerTransactionsDUnitTest.class, "verifyInvalidates");
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyInvalidates");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyInvalidates");
  }

  /**
   * Test for invalidate propagation to the clients when there are  2 servers and two
   * clients connected to separate servers.
   * 
   */
  public void testInvalidatesServerToClientTransactionsPropagationWithOneConnection()
  {
    Integer port1 = initServerCache(server1);
    Integer port2 = initServerCache(server2);
    client1.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1 });
    client2.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port2 });
    Wait.pause(PAUSE);

    server1.invoke(resetFlags());
    server2.invoke(resetFlags());
    client1.invoke(resetFlags());
    client2.invoke(resetFlags());

    server1.invoke(CacheServerTransactionsDUnitTest.class, "invalidateInTransaction",
        new Object[] { "server1" });
    server1.invoke(CacheServerTransactionsDUnitTest.class,
        "commitTransactionOnServer1");
    Wait.pause(PAUSE);

    server1.invoke(CacheServerTransactionsDUnitTest.class, "verifyInvalidates");
    server2.invoke(CacheServerTransactionsDUnitTest.class, "verifyInvalidates");
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyInvalidates");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyInvalidates");
  }
  
  
  /**
   * Test for destroy propagation to the clients when there is one server and two
   * clients connected to the server.
   * 
   */
  public void testDestroysOneServerToClientTransactionsPropagation()
  {
    Integer port1 = initServerCache(server1);
    client1.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1 });
    client2.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1 });
    Wait.pause(PAUSE);

    server1.invoke(resetFlags());
    client1.invoke(resetFlags());
    client2.invoke(resetFlags());

    server1.invoke(CacheServerTransactionsDUnitTest.class, "destroyInTransaction",
        new Object[] { "server1" });
    server1.invoke(CacheServerTransactionsDUnitTest.class,
        "commitTransactionOnServer1");
    Wait.pause(PAUSE);

    server1.invoke(CacheServerTransactionsDUnitTest.class, "verifyDestroys");
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyDestroys");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyDestroys");
  }
  
  /**
   * Test for destroy propagation to the clients when there are  2 servers and two
   * clients connected to both servers.
   */
  public void testDestroysServerToClientTransactionsPropagation()
  {
    Integer port1 = initServerCache(server1);
    Integer port2 = initServerCache(server2);
    client1.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1, port2 });
    client2.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1, port2 });
    Wait.pause(PAUSE);
    
    server1.invoke(resetFlags());
    server2.invoke(resetFlags());
    client1.invoke(resetFlags());
    client2.invoke(resetFlags());

    server1.invoke(CacheServerTransactionsDUnitTest.class, "destroyInTransaction",
        new Object[] { "server1" });
    server1.invoke(CacheServerTransactionsDUnitTest.class,
        "commitTransactionOnServer1");
    Wait.pause(PAUSE);

    server1.invoke(CacheServerTransactionsDUnitTest.class, "verifyDestroys");
    server2.invoke(CacheServerTransactionsDUnitTest.class, "verifyDestroys");
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyDestroys");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyDestroys");
  }

  
  /**
   * Test for destroy propagation to the clients when there are  2 servers and two
   * clients connected to sepatate servers.
   */
  public void testDestroysServerToClientTransactionsPropagationWithOneConnection()
  {
    Integer port1 = initServerCache(server1);
    Integer port2 = initServerCache(server2);
    client1.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1 });
    client2.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port2 });
    Wait.pause(PAUSE);
    
    server1.invoke(resetFlags());
    server2.invoke(resetFlags());
    client1.invoke(resetFlags());
    client2.invoke(resetFlags());

    server1.invoke(CacheServerTransactionsDUnitTest.class, "destroyInTransaction",
        new Object[] { "server1" });
    server1.invoke(CacheServerTransactionsDUnitTest.class,
        "commitTransactionOnServer1");
    Wait.pause(PAUSE);

    server1.invoke(CacheServerTransactionsDUnitTest.class, "verifyDestroys");
    server2.invoke(CacheServerTransactionsDUnitTest.class, "verifyDestroys");
    client1.invoke(CacheServerTransactionsDUnitTest.class, "verifyDestroys");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyDestroys");
  }
  
  /**
   * Tests if client commits are propagated to servers or not
   * Currently it is UnsupportedOperationException hence the test is commented 
   *
   */
  public void YOGESH_testClientToServerCommits(){
    Integer port1 = ((Integer)server1.invoke(
        CacheServerTransactionsDUnitTest.class, "createServerCache"));
    Integer port2 = ((Integer)server2.invoke(
        CacheServerTransactionsDUnitTest.class, "createServerCache"));
    client1.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port1 });
    client2.invoke(CacheServerTransactionsDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), port2 });
    client1.invoke(CacheServerTransactionsDUnitTest.class, "commitTransactionOnClient");
    Wait.pause(PAUSE);
    
    server1.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdatesOnServer");
    server2.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdatesOnServer");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "verifyUpdatesOnServer");    
  }

  private CacheSerializableRunnable resetFlags()
  {
    CacheSerializableRunnable resetFlags = new CacheSerializableRunnable(
        "resetFlags") {
      public void run2() throws CacheException
      {
	destroyed = false;
        invalidated = false;
      }
    };
    return resetFlags;
  }      

  public static void commitTransactionOnClient()
  {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    try {
      cache.getCacheTransactionManager().begin();
      r1.put(k1, client_k1);
      r1.put(k2, client_k2);
      cache.getCacheTransactionManager().commit();
    }
    catch (CommitConflictException e) {
      fail("Test failed due to CommitConflictException on client , which is not expected");
    }
    assertEquals(r1.getEntry(k1).getValue(), client_k1);
    assertEquals(r1.getEntry(k2).getValue(), client_k2);
  }
  
  public static void verifyUpdatesOnServer()
  {
    final Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    try {
      LogWriterUtils.getLogWriter().info(
          "vlaue for the key k1" + r1.getEntry(k1).getValue());
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          Thread.yield(); // TODO is this necessary?
          return r1.getEntry(k1).getValue().equals(client_k1);
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 120 * 1000, 200, true);
      
      ev = new WaitCriterion() {
        public boolean done() {
          Thread.yield(); // TODO is this necessary?
          return r1.getEntry(k2).getValue().equals(client_k2);
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 120 * 1000, 200, true);
    }
    catch (Exception e) {
      fail("Exception in trying to get due to " + e);
    }
  }

  
  public static void putInTransaction(String server)
  {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    cache.getCacheTransactionManager().begin();
    if (server.equals("server1")) {
      r1.put(k1, server1_k1);
      r1.put(k2, server1_k2);
      assertEquals(r1.getEntry(k1).getValue(), server1_k1);
      assertEquals(r1.getEntry(k2).getValue(), server1_k2);
    }
    else if (server.equals("server2")) {
      r1.put(k1, server2_k2);
      r1.put(k2, server2_k3);
      assertEquals(r1.getEntry(k1).getValue(), server2_k2);
      assertEquals(r1.getEntry(k2).getValue(), server2_k3);
    }
  }

  public static void invalidateInTransaction(String server) throws Exception
  {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    cache.getCacheTransactionManager().begin();
    if (server.equals("server1")) {
      r1.invalidate(k1);
      assertNull(r1.getEntry(k1).getValue());
      //assertEquals(r1.getEntry(k2).getValue(), server1_k2);
    }
    else if (server.equals("server2")) {
      r1.invalidate(k1);
      assertNull(r1.getEntry(k1).getValue());
      //assertEquals(r1.getEntry(k2).getValue(), server2_k3);
    }
  }

  public static void destroyInTransaction(String server) throws Exception
  {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    cache.getCacheTransactionManager().begin();
    if (server.equals("server1")) {
      r1.destroy(k1);
      assertNull(r1.getEntry(k1));
      //assertEquals(r1.getEntry(k2).getValue(), server1_k2);
    }
    else if (server.equals("server2")) {
      r1.destroy(k1);
      assertNull(r1.getEntry(k1));
      //assertEquals(r1.getEntry(k2).getValue(), server2_k3);
    }
  }

  
  public static void commitTransactionOnServer2()
  {
    try {
      cache.getCacheTransactionManager().commit();
      fail("CommitConflictException is expected on server2 , as server1 has not commited the transaction yet");
    }
    catch (CommitConflictException cce) {

    }
  }

  public static void commitTransactionOnServer1()
  {
    try {
      cache.getCacheTransactionManager().commit();
    }
    catch (CommitConflictException cce) {
      fail("Test failed due to a CommitConflictException on server1 , which is not expected");
    }
  }

  public static void verifyNotUpdated()
  {
    final Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    try {
      LogWriterUtils.getLogWriter().info(
          "vlaue for the key k1" + r1.getEntry(k1).getValue());
      // wait until
      // condition is
      // met
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          Thread.yield(); // TODO is this necessary?
          return r1.getEntry(k1).getValue().equals(k1);
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 120 * 1000, 200, true);

      ev = new WaitCriterion() {
        public boolean done() {
          Thread.yield(); // TODO is this necessary?
          return r1.getEntry(k2).getValue().equals(k2);
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 120 * 1000, 200, true);
    }
    catch (Exception e) {
      fail("Exception in trying to get due to " + e);
    }
  }

  public static void verifyUpdates()
  {
    final Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r1);
    
    try {
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          Thread.yield(); // TODO is this necessary?
          return r1.getEntry(k1).getValue().equals(server1_k1);
        }
        public String description() {
          return "Value for entry " + r1 + " never became " + server1_k1 
              + "; it is still " + r1.getEntry(k1).getValue();
        }
      };
      Wait.waitForCriterion(ev, 120 * 1000, 200, true);
      
      ev = new WaitCriterion() {
        public boolean done() {
          Thread.yield(); // TODO is this necessary?
          return r1.getEntry(k2).getValue().equals(server1_k2);
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 120 * 1000, 200, true);
    }
    catch (Exception e) {
      fail("Exception in trying to get due to " + e);
    }
  }

  public static void verifyInvalidates()
  {
    synchronized (CacheServerTransactionsDUnitTest.class) {
      if (!invalidated) {
        try {
          CacheServerTransactionsDUnitTest.class.wait(60000);
        }
        catch (InterruptedException e) {
          fail("interrupted");
        }
        if (!invalidated) {
          fail("failed to receive invalidation notification");
        }
      }
    }
  }
  
  
  public static void verifyDestroys()
  {
    synchronized (CacheServerTransactionsDUnitTest.class) {
      if (!destroyed) {
        try {
          CacheServerTransactionsDUnitTest.class.wait(60000);
        }
        catch (InterruptedException e) {
          fail("interrupted");
        }
      }
      if (!destroyed) {
        fail("failed to receive destroy notification");
      }
    }
  }
  
  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String host, Integer port) throws Exception
  {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    new CacheServerTransactionsDUnitTest("temp").createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory()
      .addServer(host, port.intValue())
      .setSubscriptionEnabled(true)
      // .setRetryInterval(2000)
      .create("CacheServerTransctionDUnitTestPool2");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());
    factory.setCacheListener(new CacheListenerAdapter() {
      public void afterDestroy (EntryEvent event) {
        synchronized (CacheServerTransactionsDUnitTest.class) {
          destroyed = true;
          CacheServerTransactionsDUnitTest.class.notify();
        }
      }
      public void afterInvalidate (EntryEvent event) {
        synchronized (CacheServerTransactionsDUnitTest.class) {
          invalidated = true;
          CacheServerTransactionsDUnitTest.class.notifyAll();
        }
      }
    });
    Region region1 = cache.createRegion(REGION_NAME, factory.create());
    assertNotNull(region1);
    pool = p;
    registerKeys();
  }

  public static void createClientCache(String host, Integer port1, Integer port2)
      throws Exception
  {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    new CacheServerTransactionsDUnitTest("temp").createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory()
      .addServer(host, port1.intValue())
      .addServer(host, port2.intValue())
      .setSubscriptionEnabled(true)
      // .setRetryInterval(2000)
      .create("CacheServerTransctionDUnitTestPool2");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());
    factory.setCacheListener(new CacheListenerAdapter() {
      public void afterDestroy (EntryEvent event) {
        synchronized (CacheServerTransactionsDUnitTest.class) {
          destroyed = true;
          CacheServerTransactionsDUnitTest.class.notify();
        }
      }
      public void afterInvalidate (EntryEvent event) {
        synchronized (CacheServerTransactionsDUnitTest.class) {
          invalidated = true;
          CacheServerTransactionsDUnitTest.class.notifyAll();
        }
      }
    });
    Region region1 = cache.createRegion(REGION_NAME, factory.create());
    assertNotNull(region1);
    pool = p;
    registerKeys();
  }

  protected int getMaxThreads() {
    return 0;
  }

  private Integer initServerCache(VM server) {
    Object[] args = new Object[] {new Integer(getMaxThreads())};
    return (Integer)server.invoke(CacheServerTransactionsDUnitTest.class,
                                  "createServerCache",
                                  args);
  }

  public static Integer createServerCache(Integer maxThreads) throws Exception
  {
    new CacheServerTransactionsDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setCacheListener(new CacheListenerAdapter() {
      public void afterDestroy (EntryEvent event) {
        synchronized (CacheServerTransactionsDUnitTest.class) {
          destroyed = true;
          CacheServerTransactionsDUnitTest.class.notify();
        }
      }
      public void afterInvalidate (EntryEvent event) {
        synchronized (CacheServerTransactionsDUnitTest.class) {
          invalidated = true;
          CacheServerTransactionsDUnitTest.class.notifyAll();
        }
      }
    });
    Region r1 = cache.createRegion(REGION_NAME, factory.create());
    assertNotNull(r1);
    CacheServer server1 = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    server1.setMaxThreads(maxThreads.intValue());
    server1.setNotifyBySubscription(true);
    server1.start();
    createEntries();
    return new Integer(server1.getPort());
  }

  public static void createEntries()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      if (!r.containsKey(k1)) {
        r.create(k1, k1);
      }
      if (!r.containsKey(k2)) {
        r.create(k2, k2);
      }
      if (!r.containsKey(k3)) {
        r.create(k3, k3);
      }
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry(k1).getValue(), k1);
      assertEquals(r.getEntry(k2).getValue(), k2);
      assertEquals(r.getEntry(k3).getValue(), k3);
    }
    catch (Exception ex) {
      Assert.fail("failed while createEntries()", ex);
    }
  }

  public static void registerKeys()
  {
    List keys = new ArrayList();
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      keys.add(k1);
      keys.add(k2);
      keys.add(k3);
      r.registerInterest(keys);
    }
    catch (Exception ex) {
      Assert.fail("failed while registering keys(" + keys + ")", ex);
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
    // close the clients first
    client1.invoke(CacheServerTransactionsDUnitTest.class, "closeCache");
    client2.invoke(CacheServerTransactionsDUnitTest.class, "closeCache");
    // then close the servers
    server1.invoke(CacheServerTransactionsDUnitTest.class, "closeCache");
    server2.invoke(CacheServerTransactionsDUnitTest.class, "closeCache");
  }
}
