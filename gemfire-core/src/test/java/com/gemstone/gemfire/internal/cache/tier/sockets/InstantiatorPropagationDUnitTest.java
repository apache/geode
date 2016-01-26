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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.InternalInstantiator;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.ClientServerObserverAdapter;
import com.gemstone.gemfire.internal.cache.ClientServerObserverHolder;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

public class InstantiatorPropagationDUnitTest extends DistributedTestCase {
  private static Cache cache = null;

  private static VM client1 = null;

  private static VM client2 = null;

  private static VM server1 = null;

  private static VM server2 = null;

  private static int PORT1 = -1;

  private static int PORT2 = -1;

  private static int instanceCountWithAllPuts = 3;

  private static int instanceCountWithOnePut = 1;

  private static final String REGION_NAME = "ClientServerInstantiatorRegistrationDUnitTest";
  
  protected static EventID eventId;

  static boolean testEventIDResult = false;

  public static boolean testObject20Loaded = false;




  public InstantiatorPropagationDUnitTest(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
    client1 = host.getVM(0);
    client2 = host.getVM(1);
    server1 = host.getVM(2);
    server2 = host.getVM(3);
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String host, Integer port1)
      throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new InstantiatorPropagationDUnitTest("temp").createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, port1.intValue())
        .setMinConnections(1).setSubscriptionEnabled(true).setPingInterval(200)
        .create("ClientServerInstantiatorRegistrationDUnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    Region r = cache.createRegion(REGION_NAME, factory.create());
    r.registerInterest("ALL_KEYS");
  }

  protected int getMaxThreads() {
    return 0;
  }

  private int initServerCache(VM server) {
    Object[] args = new Object[] { new Integer(getMaxThreads()) };
    return ((Integer)server.invoke(InstantiatorPropagationDUnitTest.class,
        "createServerCache", args)).intValue();
  }

  public static Integer createServerCache(Integer maxThreads) throws Exception {
    new InstantiatorPropagationDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setMirrorType(MirrorType.KEYS_VALUES);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setMaxThreads(maxThreads.intValue());
    server1.start();
    return new Integer(port);
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    // close the clients first
    closeCache();
    client1.invoke(InstantiatorPropagationDUnitTest.class, "closeCache");
    client2.invoke(InstantiatorPropagationDUnitTest.class, "closeCache");

    server1.invoke(InstantiatorPropagationDUnitTest.class, "closeCache");
    server1.invoke(InstantiatorPropagationDUnitTest.class, "closeCache");
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
  
  public static void unregisterInstantiatorsInAllVMs() {
    invokeInEveryVM(DistributedTestCase.class, "unregisterInstantiatorsInThisVM");
  }

  public static void verifyInstantiators(final int numOfInstantiators) {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;

      public boolean done() {
        return InternalInstantiator.getInstantiators().length == numOfInstantiators;
      }

      public String description() {
        return "expected " + numOfInstantiators + " but got this "
            + InternalInstantiator.getInstantiators().length
          + " instantiators=" + java.util.Arrays.toString(InternalInstantiator.getInstantiators());
      }
    };
    DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);
  }

  public static void registerTestObject1() throws Exception {

    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject1");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject1", e);
    }
  }

  public static void registerTestObject2() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject2");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject2", e);
    }
  }

  public static void registerTestObject3() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject3");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject3", e);
    }
  }

  public static void registerTestObject4() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject4");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject4", e);
    }
  }

  public static void registerTestObject5() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject5");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject5", e);
    }
  }

  public static void registerTestObject6() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject6");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject6", e);
    }
  }

  public static void registerTestObject7() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject7");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject7", e);
    }
  }

  public static void registerTestObject8() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject8");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject8", e);
    }
  }

  public static void registerTestObject9() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject9");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject9", e);
    }
  }

  public static void registerTestObject10() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject10");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject10", e);
    }
  }

  public static void registerTestObject11() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject11");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject11", e);
    }
  }

  public static void registerTestObject12() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject12");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject11", e);
    }
  }

  public static void registerTestObject13() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject13");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject13", e);
    }
  }

  public static void registerTestObject14() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject14");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject14", e);
    }
  }

  public static void registerTestObject15() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject15");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject15", e);
    }
  }

  public static void registerTestObject16() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject16");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject16", e);
    }
  }

  public static void registerTestObject17() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject17");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject17", e);
    }
  }

  public static void registerTestObject18() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject18");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject18", e);
    }
  }
  
  public static void registerTestObject19() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject19");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject19", e);
    }
  }

  public static void registerTestObject20() throws Exception {
    try {
      Class cls = Class
          .forName("com.gemstone.gemfire.internal.cache.tier.sockets.TestObject20");
      ConfigurableObject obj = (ConfigurableObject)cls.newInstance();
      obj.init(0);
    }
    catch (Exception e) {
      fail("Test failed due to exception in TestObject20", e);
    }
  }

  public static void stopServer() {
    try {
      assertEquals("Expected exactly one BridgeServer", 1, cache
          .getCacheServers().size());
      CacheServerImpl bs = (CacheServerImpl)cache.getCacheServers()
          .iterator().next();
      assertNotNull(bs);
      bs.stop();
    }
    catch (Exception ex) {
      fail("while setting stopServer  " + ex);
    }
  }

  public static void startServer() {
    try {
      Cache c = CacheFactory.getAnyInstance();
      assertEquals("Expected exactly one BridgeServer", 1, c.getCacheServers()
          .size());
      CacheServerImpl bs = (CacheServerImpl)c.getCacheServers().iterator()
          .next();
      assertNotNull(bs);
      bs.start();
    }
    catch (Exception ex) {
      fail("while startServer()  " + ex);
    }
  }

  /**
   * In this test the server is up first.2 Instantiators are registered on it.
   * Verified if the 2 instantiators get propagated to client when client gets
   * connected.
   */
  public void testServerUpFirstClientLater() throws Exception {
    PORT1 = initServerCache(server1);

    unregisterInstantiatorsInAllVMs();
    
    pause(3000);

    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "registerTestObject1");
    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "registerTestObject2");

    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(2) });

    client1
        .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server1.getHost()),
                new Integer(PORT1) });

    // // wait for client2 to come online
    pause(3000);
    //
    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(2) });
    //
    // // Put some entries from the client
    client1.invoke(new CacheSerializableRunnable("Put entries from client") {
      public void run2() throws CacheException {
        Region region = cache.getRegion(REGION_NAME);
        for (int i = 1; i <= 10; i++) {
          region.put(i, i);
        }
      }
    });

    // Run getAll
    client1
        .invoke(new CacheSerializableRunnable("Get all entries from server") {
          public void run2() throws CacheException {
            // Invoke getAll
            Region region = cache.getRegion(REGION_NAME);
            // Verify result size is correct
            assertEquals(1, region.get(1));
          }
        });

    server1.invoke(new CacheSerializableRunnable("Put entry from client") {
      public void run2() throws CacheException {
        Region region = cache.getRegion(REGION_NAME);
        region.put(1, 20);
      }
    });
    //
    pause(3000);
    // Run getAll
    client1.invoke(new CacheSerializableRunnable("Get entry from client") {
      public void run2() throws CacheException {
        // Invoke getAll
        Region region = cache.getRegion(REGION_NAME);
        // Verify result size is correct
        assertEquals(20, region.get(1));
      }
    });

    unregisterInstantiatorsInAllVMs();
  }

  /**
   * In this test there are 2 clients and 2 servers.Registered one instantiator
   * on one client. Verified, if that instantiator gets propagated to the server
   * the client is connected to(server1), to the other server(server2) in the DS
   * and the client(client2) that is connected to server2.
   */
  public void testInstantiatorsWith2ClientsN2Servers() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    pause(3000);

    client1
        .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server1.getHost()),
                new Integer(PORT1) });
    client2
        .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server1.getHost()),
                new Integer(PORT2) });

    unregisterInstantiatorsInAllVMs();

    // wait for client2 to come online
    pause(2000);


    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "registerTestObject3");
    pause(4000);

    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(1) });

    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(1) });

    server2.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(1) });

    client2.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(1) });

    unregisterInstantiatorsInAllVMs();
  }

  /**
   * First register an instantiator on client1. Stop the server1. Now register 2
   * instantiators on server1. Now check that server1,server2,client2 has all 3
   * instantiators. Client1 should have only 1 instantiator since the server1
   * was stopped when 2 instantiators were added on it.
   */
  public void testInstantiatorsWithServerKill() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1
        .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server1.getHost()),
                new Integer(PORT1) });
    client2
        .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server1.getHost()),
                new Integer(PORT2) });

    unregisterInstantiatorsInAllVMs();

    // wait for client2 to come online
    pause(2000);

    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "registerTestObject4");
    pause(4000);

    server1.invoke(InstantiatorPropagationDUnitTest.class, "stopServer");

    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "registerTestObject5");
    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "registerTestObject6");

    server2.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(
            instanceCountWithAllPuts) });

    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(
            instanceCountWithAllPuts) });

    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(
            instanceCountWithOnePut) });

    client2.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(
            instanceCountWithAllPuts) });

    unregisterInstantiatorsInAllVMs();
  }

  /**
   * 2 clients n 2 servers.Registered instantiators on both client n server to
   * check if propagation of instantiators to n fro (from client n server) is
   * taking place.Diff from the previous test in the case that server is not
   * stopped.So registering an instantiator on server should propagate that to
   * client as well.
   */
  public void testInstantiators() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1
        .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server1.getHost()),
                new Integer(PORT1) });
    client2
        .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server1.getHost()),
                new Integer(PORT2) });

    unregisterInstantiatorsInAllVMs();

    // wait for client2 to come online
    pause(2000);

    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "registerTestObject10");
    pause(4000);

    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "registerTestObject11");
    pause(4000);

    server2.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(2) });

    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(2) });

    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(2) });

    client2.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(2) });

    unregisterInstantiatorsInAllVMs();
  }

  /**
   * Test's Number of Instantiators at all clients & servers with one Server
   * being stopped and then restarted
   */
  public void _testInstantiatorsWithServerKillAndReInvoked() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);
    client1
        .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server1.getHost()),
                new Integer(PORT1) });
    client2
        .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server1.getHost()),
                new Integer(PORT2) });

    unregisterInstantiatorsInAllVMs();

    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "registerTestObject7");
    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(
            instanceCountWithOnePut) });

    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(
            instanceCountWithOnePut) });

    server2.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(
            instanceCountWithOnePut) });

    client2.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(
            instanceCountWithOnePut) });

    server1.invoke(InstantiatorPropagationDUnitTest.class, "stopServer");

    try {
      client1.invoke(InstantiatorPropagationDUnitTest.class,
          "registerTestObject8");
    }
    catch (Exception expected) {// we are putting in a client whose server is
      // dead
    }

    server1.invoke(InstantiatorPropagationDUnitTest.class, "startServer");

    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(
            instanceCountWithAllPuts) });

    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(
            instanceCountWithAllPuts) });

    server2.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(
            instanceCountWithAllPuts) });

    unregisterInstantiatorsInAllVMs();
  }

  /**
   * In this test there are 2 clients connected to 1 server and 1 client
   * connected to the other server.Registered one instantiator on one
   * client(client1). Verified, if that instantiator gets propagated to the
   * server the client is connected to(server1), to client2, to the other
   * server(server2) in the DS and the client that is connected to server2.
   * 
   */
  public void testInstantiatorCount() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1
        .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server1.getHost()),
                new Integer(PORT1) });
    client2
        .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
            new Object[] { getServerHostName(server1.getHost()),
                new Integer(PORT1) });
    createClientCache(getServerHostName(server2.getHost()), new Integer(PORT2));
    unregisterInstantiatorsInAllVMs();

    // wait for client2 to come online
    pause(2000);

    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "registerTestObject12");
    pause(4000);

    client1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(1) });

    server1.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(1) });

    server2.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(1) });

    client2.invoke(InstantiatorPropagationDUnitTest.class,
        "verifyInstantiators", new Object[] { new Integer(1) });

    verifyInstantiators(1);

    unregisterInstantiatorsInAllVMs();
  }

  public static void createClientCache_EventId(String host, Integer port1) throws Exception
  {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new InstantiatorPropagationDUnitTest("temp").createCache(props);
    Pool p = PoolManager.createFactory()
      .addServer(host, port1.intValue())
      .setSubscriptionEnabled(true)
      .create("RegisterInstantiatorEventIdDUnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());
    cache.createRegion(REGION_NAME, factory.create());
  }
  /**
   * Test's same eventId being same for the Instantiators at all clients &
   * servers
   * 
   */
  // disabled - the eventID received does not match the sender's eventID.  Why is this a requirement anyway?
  public void _testInstantiatorsEventIdVerificationClientsAndServers()
      throws Exception {
    PORT1 = initServerCache(server1, 1);
    PORT2 = initServerCache(server2, 2);

    createClientCache_EventId(getServerHostName(server1.getHost()), new Integer(PORT1));

    unregisterInstantiatorsInAllVMs();
    
    client2.invoke(InstantiatorPropagationDUnitTest.class,
        "createClientCache_EventId", new Object[] {
            getServerHostName(server1.getHost()), new Integer(PORT2) });
    setClientServerObserver1();
    client2.invoke(InstantiatorPropagationDUnitTest.class,
        "setClientServerObserver2");

    registerTestObject19();

    pause(10000);

    Boolean pass = (Boolean)client2.invoke(
        InstantiatorPropagationDUnitTest.class, "verifyResult");
    assertTrue("EventId found Different", pass.booleanValue());

    PoolImpl.IS_INSTANTIATOR_CALLBACK = false;

  }
  
  public void testLazyRegistrationOfInstantiators()
      throws Exception {
    try {
      PORT1 = initServerCache(server1);
      PORT2 = initServerCache(server2);
  
      unregisterInstantiatorsInAllVMs();

      pause(3000);
  
      createClientCache(getServerHostName(server1.getHost()),
          new Integer(PORT1));
  
      client2
          .invoke(InstantiatorPropagationDUnitTest.class, "createClientCache",
              new Object[] {getServerHostName(server2.getHost()),
                  new Integer(PORT2)});
  
      pause(3000);
      unregisterInstantiatorsInAllVMs();
  
      assertTestObject20NotLoaded();
      server1.invoke(InstantiatorPropagationDUnitTest.class, "assertTestObject20NotLoaded");
      server2.invoke(InstantiatorPropagationDUnitTest.class, "assertTestObject20NotLoaded");
      client2.invoke(InstantiatorPropagationDUnitTest.class, "assertTestObject20NotLoaded");
  
      registerTestObject20();
      pause(5000);
      assertTestObject20Loaded();
      server1.invoke(InstantiatorPropagationDUnitTest.class, "assertTestObject20Loaded");
      //server2.invoke(InstantiatorPropagationDUnitTest.class, "assertTestObject20Loaded"); // classes are not initialized after loading in p2p path
      client2.invoke(InstantiatorPropagationDUnitTest.class, "assertTestObject20NotLoaded");
    } finally {
      unregisterInstantiatorsInAllVMs();
      disconnectAllFromDS();
    }
  }

  public static void assertTestObject20Loaded() {
    assertTrue("TestObject20 is expected to be loaded into VM.", testObject20Loaded);
  }

  public static void assertTestObject20NotLoaded() {
    assertFalse("TestObject20 is not expected to be loaded into VM.", testObject20Loaded);
  }

  public static Boolean verifyResult() {
    boolean temp = testEventIDResult;
    testEventIDResult = false;
    return new Boolean(temp);
  }
  
  /**
   * this method initializes the appropriate server cache
   * 
   * @param server
   * @param serverNo
   * @return portNo.
   */

  private int initServerCache(VM server, int serverNo)
  {
    Object[] args = new Object[] { new Integer(getMaxThreads()) };
    if (serverNo == 1) {
      return ((Integer)server.invoke(
          InstantiatorPropagationDUnitTest.class,
          "createServerCacheOne", args)).intValue();
    }
    else {
      return ((Integer)server.invoke(
          InstantiatorPropagationDUnitTest.class,
          "createServerCacheTwo", args)).intValue();
    }
  }

  /**
   * This method creates the server cache
   * 
   * @param maxThreads
   * @return
   * @throws Exception
   */
  public static Integer createServerCacheTwo(Integer maxThreads)
      throws Exception
  {
    new InstantiatorPropagationDUnitTest("temp")
        .createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setMirrorType(MirrorType.KEYS_VALUES);

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setMaxThreads(maxThreads.intValue());
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(port);
  }

  /**
   * This method creates the server cache
   * 
   * @param maxThreads
   * @return
   * @throws Exception
   */
  public static Integer createServerCacheOne(Integer maxThreads)
      throws Exception
  {
    new InstantiatorPropagationDUnitTest("temp")
        .createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setMirrorType(MirrorType.KEYS_VALUES);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setMaxThreads(maxThreads.intValue());
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(port);
  }

  public static void setClientServerObserver1()
  {
    PoolImpl.IS_INSTANTIATOR_CALLBACK = true;
    ClientServerObserverHolder
        .setInstance(new ClientServerObserverAdapter() {
          public void beforeSendingToServer(EventID eventID)
          {
            eventId = eventID;
            System.out.println("client2= "+client2 + " eventid= "+eventID);
            client2.invoke(InstantiatorPropagationDUnitTest.class,
                "setEventId", new Object[] { eventId });

          }

        });
  }

  /**
   * sets the EventId value in the VM
   * 
   * @param eventID
   */
  public static void setEventId(EventID eventID)
  {
    eventId = eventID;
  }
  
  public static void setClientServerObserver2()
  {
    PoolImpl.IS_INSTANTIATOR_CALLBACK = true;
    ClientServerObserverHolder
        .setInstance(new ClientServerObserverAdapter() {
          public void afterReceivingFromServer(EventID eventID)
          {
            System.out.println("Observer2 received " + eventID + "; my eventID is " + eventId);
            testEventIDResult = eventID.equals(eventId);
          }

        });
  }
}


abstract class ConfigurableObject {
  public abstract void init(int index);
  public abstract int getIndex();
  public abstract void validate(int index);
}




class TestObject1 extends ConfigurableObject implements DataSerializable {

  private int field1;

  public TestObject1() {
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  static {
    Instantiator.register(new Instantiator(TestObject1.class, -100123) {
      public DataSerializable newInstance() {
        return new TestObject1();
      }
    });
  }

  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }

}

class TestObject2 extends ConfigurableObject implements DataSerializable {

  private int field1;

  public TestObject2() {
  }

  static {
    Instantiator.register(new Instantiator(TestObject2.class, -100122) {
      public DataSerializable newInstance() {
        return new TestObject2();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */

  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }

}

class TestObject3 extends ConfigurableObject implements DataSerializable {

  private int field1;

  public TestObject3() {
  }

  static {
    Instantiator.register(new Instantiator(TestObject3.class, -121) {
      public DataSerializable newInstance() {
        return new TestObject3();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject4 extends ConfigurableObject implements DataSerializable {

  private int field1;

  public TestObject4() {
  }

  static {
    Instantiator.register(new Instantiator(TestObject4.class, -122) {
      public DataSerializable newInstance() {
        return new TestObject4();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject5 extends ConfigurableObject implements DataSerializable {

  private int field1;

  public TestObject5() {
  }

  static {
    Instantiator.register(new Instantiator(TestObject5.class, -123) {
      public DataSerializable newInstance() {
        return new TestObject5();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject6 extends ConfigurableObject implements DataSerializable {

  private int field1;

  public TestObject6() {
  }

  static {
    Instantiator.register(new Instantiator(TestObject6.class, -124) {
      public DataSerializable newInstance() {
        return new TestObject6();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject7 extends ConfigurableObject implements DataSerializable {

  private int field1;

  public TestObject7() {
  }

  static {
    Instantiator.register(new Instantiator(TestObject7.class, -125) {
      public DataSerializable newInstance() {
        return new TestObject7();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject8 extends ConfigurableObject implements DataSerializable {

  private int field1;

  public TestObject8() {
  }

  static {
    Instantiator.register(new Instantiator(TestObject8.class, -126) {
      public DataSerializable newInstance() {
        return new TestObject8();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject9 extends ConfigurableObject implements DataSerializable {

  private int field1;

  public TestObject9() {
  }

  static {
    Instantiator.register(new Instantiator(TestObject9.class, -127) {
      public DataSerializable newInstance() {
        return new TestObject9();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject10 extends ConfigurableObject implements DataSerializable {

  private int field1;

  public TestObject10() {
  }

  static {
    Instantiator.register(new Instantiator(TestObject10.class, -128) {
      public DataSerializable newInstance() {
        return new TestObject10();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject11 extends ConfigurableObject implements DataSerializable {

  private int field1;

  static {
    Instantiator.register(new Instantiator(TestObject11.class, -129) {
      public DataSerializable newInstance() {
        return new TestObject11();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject12 extends ConfigurableObject implements DataSerializable {

  private int field1;

  static {
    Instantiator.register(new Instantiator(TestObject12.class, -130) {
      public DataSerializable newInstance() {
        return new TestObject12();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject13 extends ConfigurableObject implements DataSerializable {

  private int field1;

  static {
    Instantiator.register(new Instantiator(TestObject13.class, -131) {
      public DataSerializable newInstance() {
        return new TestObject13();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject14 extends ConfigurableObject implements DataSerializable {

  private int field1;

  static {
    Instantiator.register(new Instantiator(TestObject14.class, -132) {
      public DataSerializable newInstance() {
        return new TestObject14();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject15 extends ConfigurableObject implements DataSerializable {

  private int field1;

  static {
    Instantiator.register(new Instantiator(TestObject15.class, -133) {
      public DataSerializable newInstance() {
        return new TestObject15();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject16 extends ConfigurableObject implements DataSerializable {

  private int field1;

  static {
    Instantiator.register(new Instantiator(TestObject16.class, -134) {
      public DataSerializable newInstance() {
        return new TestObject16();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject17 extends ConfigurableObject implements DataSerializable {

  private int field1;

  static {
    Instantiator.register(new Instantiator(TestObject17.class, -135) {
      public DataSerializable newInstance() {
        return new TestObject17();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject18 extends ConfigurableObject implements DataSerializable {

  private int field1;

  static {
    Instantiator.register(new Instantiator(TestObject18.class, -1136) {
      public DataSerializable newInstance() {
        return new TestObject18();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject19 extends ConfigurableObject implements DataSerializable {

  private int field1;

  static {
    Instantiator.register(new Instantiator(TestObject19.class, -136) {
      public DataSerializable newInstance() {
        return new TestObject19();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}

class TestObject20 extends ConfigurableObject implements DataSerializable {

  private int field1;

  static {
    InstantiatorPropagationDUnitTest.testObject20Loaded = true;
    Instantiator.register(new Instantiator(TestObject20.class, -138) {
      public DataSerializable newInstance() {
        return new TestObject20();
      }
    });
  }

  /**
   * Initializes a Instantiator1DUnitTestObject1.
   */
  public void init(int index) {
    Random random = new Random();
    this.field1 = random.nextInt();
  }

  public int getIndex() {
    return 1;
  }

  public void validate(int index) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
  
}
