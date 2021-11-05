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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.Instantiator;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class InstantiatorPropagationDUnitTest extends JUnit4DistributedTestCase {

  private static Cache cache = null;

  private static VM client1 = null;

  private static VM client2 = null;

  private VM server1 = null;

  private static VM server2 = null;

  private int PORT1 = -1;

  private int PORT2 = -1;

  private static final int instanceCountWithAllPuts = 3;

  private static final int instanceCountWithOnePut = 1;

  private static final String REGION_NAME =
      InstantiatorPropagationDUnitTest.class.getSimpleName() + "_region";

  protected static EventID eventId;

  static boolean testEventIDResult = false;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    client1 = host.getVM(0).initializeAsClientVM();
    client2 = host.getVM(1).initializeAsClientVM();
    server1 = host.getVM(2);
    server2 = host.getVM(3);
  }

  private void createCache(Properties props) {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void createClientCache(String host, int port1) throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new InstantiatorPropagationDUnitTest().createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, port1).setMinConnections(1)
        .setSubscriptionEnabled(true).setPingInterval(200)
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
    return server.invoke(() -> createServerCache(getMaxThreads()));
  }

  public static Integer createServerCache(Integer maxThreads) throws Exception {
    new InstantiatorPropagationDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setMirrorType(MirrorType.KEYS_VALUES);
    RegionAttributes regionAttributes = factory.create();
    cache.createRegion(REGION_NAME, regionAttributes);
    int port = getRandomAvailableTCPPort();
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setMaxThreads(maxThreads);
    server1.start();
    return port;
  }

  @Override
  public final void preTearDown() throws Exception {
    // close the clients first
    closeCache();
    client1.invoke(InstantiatorPropagationDUnitTest::closeCache);
    client2.invoke(InstantiatorPropagationDUnitTest::closeCache);

    server1.invoke(InstantiatorPropagationDUnitTest::closeCache);
    server1.invoke(InstantiatorPropagationDUnitTest::closeCache);
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void unregisterInstantiatorsInAllVMs() {
    Invoke.invokeInEveryVM(DistributedTestUtils::unregisterInstantiatorsInThisVM);
  }

  public static void verifyInstantiators(final int numOfInstantiators) {
    WaitCriterion wc = new WaitCriterion() {

      @Override
      public boolean done() {
        return InternalInstantiator.getInstantiators().length == numOfInstantiators;
      }

      @Override
      public String description() {
        return "expected " + numOfInstantiators + " but got this "
            + InternalInstantiator.getInstantiators().length + " instantiators="
            + java.util.Arrays.toString(InternalInstantiator.getInstantiators());
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  public static void registerTestObject(String testObjectClassName, int classId) {
    try {
      Class<? extends DataSerializable> clazz =
          (Class<? extends DataSerializable>) Class
              .forName("org.apache.geode.internal.cache.tier.sockets." + testObjectClassName);
      Instantiator.register(new Instantiator(clazz, classId) {
        @Override
        public DataSerializable newInstance() {
          try {
            return getInstantiatedClass().newInstance();
          } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        }
      });
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in " + testObjectClassName, e);
    }
  }

  public static void stopServer() {
    try {
      assertEquals("Expected exactly one BridgeServer", 1, cache.getCacheServers().size());
      CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
      assertNotNull(bs);
      bs.stop();
    } catch (Exception ex) {
      fail("while setting stopServer  " + ex);
    }
  }

  public static void startServer() {
    try {
      Cache c = CacheFactory.getAnyInstance();
      assertEquals("Expected exactly one BridgeServer", 1, c.getCacheServers().size());
      CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
      assertNotNull(bs);
      bs.start();
    } catch (Exception ex) {
      fail("while startServer()  " + ex);
    }
  }

  /**
   * In this test the server is up first.2 Instantiators are registered on it. Verified if the 2
   * instantiators get propagated to client when client gets connected.
   */
  @Test
  public void testServerUpFirstClientLater() {
    PORT1 = initServerCache(server1);

    unregisterInstantiatorsInAllVMs();

    GeodeAwaitility.await().atMost(3, TimeUnit.SECONDS)
        .untilAsserted(this::assertAllInstantiatorsHaveBeenUnregisteredInAllVMs);

    server1
        .invoke(() -> InstantiatorPropagationDUnitTest.registerTestObject("TestObject1", -100123));
    server1
        .invoke(() -> InstantiatorPropagationDUnitTest.registerTestObject("TestObject2", -100122));

    server1.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(2));

    client1.invoke(() -> InstantiatorPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT1));

    //
    client1.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(2));
    //
    // // Put some entries from the client
    client1.invoke(new CacheSerializableRunnable("Put entries from client") {
      @Override
      public void run2() throws CacheException {
        Region region = cache.getRegion(REGION_NAME);
        for (int i = 1; i <= 10; i++) {
          region.put(i, i);
        }
      }
    });

    // Run getAll
    client1.invoke(new CacheSerializableRunnable("Get all entries from server") {
      @Override
      public void run2() throws CacheException {
        // Invoke getAll
        Region region = cache.getRegion(REGION_NAME);
        // Verify result size is correct
        assertEquals(1, region.get(1));
      }
    });

    server1.invoke(new CacheSerializableRunnable("Put entry from client") {
      @Override
      public void run2() throws CacheException {
        Region region = cache.getRegion(REGION_NAME);
        region.put(1, 20);
      }
    });
    //
    Wait.pause(3000);
    // Run getAll
    client1.invoke(new CacheSerializableRunnable("Get entry from client") {
      @Override
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
   * In this test there are 2 clients and 2 servers.Registered one instantiator on one client.
   * Verified, if that instantiator gets propagated to the server the client is connected
   * to(server1), to the other server(server2) in the DS and the client(client2) that is connected
   * to server2.
   */
  @Test
  public void testInstantiatorsWith2ClientsN2Servers() {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> InstantiatorPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT1));
    client2.invoke(() -> InstantiatorPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT2));

    unregisterInstantiatorsInAllVMs();

    GeodeAwaitility.await().atMost(3, TimeUnit.SECONDS)
        .untilAsserted(this::assertAllInstantiatorsHaveBeenUnregisteredInAllVMs);

    client1.invoke(() -> InstantiatorPropagationDUnitTest.registerTestObject("TestObject3", -121));
    GeodeAwaitility.await().atMost(3, TimeUnit.SECONDS)
        .untilAsserted(
            () -> InstantiatorPropagationDUnitTest.assertInstantiatorHasBeenRegisteredInAllVMs(1));

    client1.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(1));

    server1.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(1));

    server2.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(1));

    client2.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(1));

    unregisterInstantiatorsInAllVMs();
  }

  /**
   * First register an instantiator on client1. Stop the server1. Now register 2 instantiators on
   * server1. Now check that server1,server2,client2 has all 3 instantiators. Client1 should have
   * only 1 instantiator since the server1 was stopped when 2 instantiators were added on it.
   */
  @Test
  public void testInstantiatorsWithServerKill() {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> InstantiatorPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT1));
    client2.invoke(() -> InstantiatorPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT2));

    unregisterInstantiatorsInAllVMs();

    GeodeAwaitility.await().atMost(3, TimeUnit.SECONDS)
        .untilAsserted(this::assertAllInstantiatorsHaveBeenUnregisteredInAllVMs);

    client1.invoke(() -> InstantiatorPropagationDUnitTest.registerTestObject("TestObject4", -122));
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> InstantiatorPropagationDUnitTest.assertInstantiatorHasBeenRegisteredInAllVMs(1));

    server1.invoke(InstantiatorPropagationDUnitTest::stopServer);

    server1.invoke(() -> InstantiatorPropagationDUnitTest.registerTestObject("TestObject5", -123));
    server1.invoke(() -> InstantiatorPropagationDUnitTest.registerTestObject("TestObject6", -124));

    server2.invoke(() -> InstantiatorPropagationDUnitTest
        .verifyInstantiators(instanceCountWithAllPuts));

    server1.invoke(() -> InstantiatorPropagationDUnitTest
        .verifyInstantiators(instanceCountWithAllPuts));

    client1.invoke(() -> InstantiatorPropagationDUnitTest
        .verifyInstantiators(instanceCountWithOnePut));

    client2.invoke(() -> InstantiatorPropagationDUnitTest
        .verifyInstantiators(instanceCountWithAllPuts));

    unregisterInstantiatorsInAllVMs();
  }

  /**
   * 2 clients n 2 servers.Registered instantiators on both client n server to check if propagation
   * of instantiators to n fro (from client n server) is taking place.Diff from the previous test in
   * the case that server is not stopped.So registering an instantiator on server should propagate
   * that to client as well.
   */
  @Test
  public void testInstantiators() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> InstantiatorPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT1));
    client2.invoke(() -> InstantiatorPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT2));

    unregisterInstantiatorsInAllVMs();

    client1.invoke(() -> InstantiatorPropagationDUnitTest.registerTestObject("TestObject10", -127));
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> InstantiatorPropagationDUnitTest.assertInstantiatorHasBeenRegisteredInAllVMs(1));

    server1.invoke(() -> InstantiatorPropagationDUnitTest.registerTestObject("TestObject11", -128));
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> InstantiatorPropagationDUnitTest.assertInstantiatorHasBeenRegisteredInAllVMs(2));

    server2.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(2));

    server1.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(2));

    client1.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(2));

    client2.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(2));

    unregisterInstantiatorsInAllVMs();
  }

  /**
   * Test's Number of Instantiators at all clients & servers with one Server being stopped and then
   * restarted
   */
  @Ignore("TODO")
  @Test
  public void testInstantiatorsWithServerKillAndReInvoked() {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);
    client1.invoke(() -> InstantiatorPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT1));
    client2.invoke(() -> InstantiatorPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT2));

    unregisterInstantiatorsInAllVMs();

    client1.invoke(() -> InstantiatorPropagationDUnitTest.registerTestObject("TestObject7", -125));

    client1.invoke(
        () -> InstantiatorPropagationDUnitTest.verifyInstantiators(instanceCountWithOnePut));
    server1.invoke(
        () -> InstantiatorPropagationDUnitTest.verifyInstantiators(instanceCountWithOnePut));
    server2.invoke(
        () -> InstantiatorPropagationDUnitTest.verifyInstantiators(instanceCountWithOnePut));
    client2.invoke(
        () -> InstantiatorPropagationDUnitTest.verifyInstantiators(instanceCountWithOnePut));

    server1.invoke(InstantiatorPropagationDUnitTest::stopServer);

    try {
      client1
          .invoke(() -> InstantiatorPropagationDUnitTest.registerTestObject("TestObject8", -126));
    } catch (Exception expected) {// we are putting in a client whose server is dead
    }

    server1.invoke(InstantiatorPropagationDUnitTest::startServer);

    client1.invoke(
        () -> InstantiatorPropagationDUnitTest.verifyInstantiators(instanceCountWithAllPuts));
    server1.invoke(
        () -> InstantiatorPropagationDUnitTest.verifyInstantiators(instanceCountWithAllPuts));
    server2.invoke(
        () -> InstantiatorPropagationDUnitTest.verifyInstantiators(instanceCountWithAllPuts));

    unregisterInstantiatorsInAllVMs();
  }

  /**
   * In this test there are 2 clients connected to 1 server and 1 client connected to the other
   * server.Registered one instantiator on one client(client1). Verified, if that instantiator gets
   * propagated to the server the client is connected to(server1), to client2, to the other
   * server(server2) in the DS and the client that is connected to server2.
   */
  @Test
  public void testInstantiatorCount() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> InstantiatorPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT1));
    client2.invoke(() -> InstantiatorPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT1));
    createClientCache(NetworkUtils.getServerHostName(server2.getHost()), PORT2);
    unregisterInstantiatorsInAllVMs();

    GeodeAwaitility.await().atMost(3, TimeUnit.SECONDS)
        .untilAsserted(this::assertAllInstantiatorsHaveBeenUnregisteredInAllVMs);

    client1.invoke(() -> InstantiatorPropagationDUnitTest.registerTestObject("TestObject12", -130));
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> InstantiatorPropagationDUnitTest.assertTestObjectRegistered("TestObject12"));

    client1.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(1));
    server1.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(1));
    server2.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(1));
    client2.invoke(() -> InstantiatorPropagationDUnitTest.verifyInstantiators(1));

    verifyInstantiators(1);

    unregisterInstantiatorsInAllVMs();
  }

  public static void createClientCache_EventId(String host, Integer port1) throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new InstantiatorPropagationDUnitTest().createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, port1)
        .setSubscriptionEnabled(true).create("RegisterInstantiatorEventIdDUnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());
    cache.createRegion(REGION_NAME, factory.create());
  }

  /**
   * Test's same eventId being same for the Instantiators at all clients & servers
   */
  @Ignore("TODO: disabled - the eventID received does not match the sender's eventID.  Why is this a requirement anyway?")
  @Test
  public void testInstantiatorsEventIdVerificationClientsAndServers() throws Exception {
    PORT1 = initServerCache(server1, 1);
    PORT2 = initServerCache(server2, 2);

    createClientCache_EventId(NetworkUtils.getServerHostName(server1.getHost()), PORT1);

    unregisterInstantiatorsInAllVMs();

    client2.invoke(() -> InstantiatorPropagationDUnitTest.createClientCache_EventId(
        NetworkUtils.getServerHostName(server1.getHost()), PORT2));
    setClientServerObserver1();
    client2.invoke(InstantiatorPropagationDUnitTest::setClientServerObserver2);

    registerTestObject("TestObject19", -136);

    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> InstantiatorPropagationDUnitTest.assertTestObjectRegistered("TestObject19"));

    Boolean pass = client2.invoke(InstantiatorPropagationDUnitTest::verifyResult);
    assertTrue("EventId found Different", pass);

    PoolImpl.IS_INSTANTIATOR_CALLBACK = false;
  }

  @Test
  public void testRegistrationOfInstantiatorsWithLazyInitializationOfClass() throws Exception {
    try {
      PORT1 = initServerCache(server1);
      PORT2 = initServerCache(server2);

      unregisterInstantiatorsInAllVMs();

      GeodeAwaitility.await().atMost(3, TimeUnit.SECONDS)
          .untilAsserted(this::assertAllInstantiatorsHaveBeenUnregisteredInAllVMs);

      createClientCache(NetworkUtils.getServerHostName(server1.getHost()), PORT1);

      client2.invoke(() -> createClientCache(
          NetworkUtils.getServerHostName(server1.getHost()), PORT2));

      unregisterInstantiatorsInAllVMs();
      GeodeAwaitility.await().atMost(3, TimeUnit.SECONDS)
          .untilAsserted(this::assertAllInstantiatorsHaveBeenUnregisteredInAllVMs);

      assertTestObject20NotRegistered();
      server1.invoke(InstantiatorPropagationDUnitTest::assertTestObject20NotRegistered);
      server2.invoke(InstantiatorPropagationDUnitTest::assertTestObject20NotRegistered);
      client2.invoke(InstantiatorPropagationDUnitTest::assertTestObject20NotRegistered);

      registerTestObject("TestObject20", -138);
      GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS)
          .untilAsserted(
              () -> InstantiatorPropagationDUnitTest.assertTestObjectRegistered("TestObject20"));
      server1.invoke(InstantiatorPropagationDUnitTest::assertTestObject20RegisteredNotInitialized);
      server2.invoke(InstantiatorPropagationDUnitTest::assertTestObject20RegisteredNotInitialized);
      // classes are not initialized after loading in p2p path
      client2.invoke(InstantiatorPropagationDUnitTest::assertTestObject20RegisteredNotInitialized);
    } finally {
      unregisterInstantiatorsInAllVMs();
      disconnectAllFromDS();
    }
  }

  private void assertAllInstantiatorsHaveBeenUnregisteredInAllVMs() {
    Map<VM, Integer> vmIntegerMap =
        Invoke.invokeInEveryVM(() -> InternalInstantiator.getInstantiators().length);
    assertEquals(0, vmIntegerMap.values().stream().filter(integer -> integer != 0).count());
  }

  private static void assertInstantiatorHasBeenRegisteredInAllVMs(int numberOfInstantitors) {
    Map<VM, Integer> vmIntegerMap =
        Invoke.invokeInEveryVM(() -> InternalInstantiator.getInstantiators().length);
    // confirm that all VMs have the instantiator registered
    assertEquals(4,
        vmIntegerMap.values().stream().filter(integer -> integer == numberOfInstantitors).count());
  }

  public static void assertTestObjectRegistered(String objectClassName) {
    assertEquals(1, Arrays.stream(InternalInstantiator.getInstantiators())
        .filter(instantiator -> instantiator.getInstantiatedClass().getName()
            .equals("org.apache.geode.internal.cache.tier.sockets." + objectClassName))
        .count());
  }

  public static void assertTestObject20RegisteredNotInitialized() {
    assertTestObjectRegistered("TestObject20");
    assertNull(TestObject20.fieldRegistered);
  }

  public static void assertTestObject20NotRegistered() {
    assertEquals(0, Arrays.stream(InternalInstantiator.getInstantiators())
        .filter(instantiator -> instantiator.getInstantiatedClass().getName()
            .equals("org.apache.geode.internal.cache.tier.sockets.TestObject20"))
        .count());
  }

  public static Boolean verifyResult() {
    boolean temp = testEventIDResult;
    testEventIDResult = false;
    return temp;
  }

  /**
   * this method initializes the appropriate server cache
   */
  private int initServerCache(VM server, int serverNo) {
    Object[] args = new Object[] {getMaxThreads()};
    if (serverNo == 1) {
      return server.invoke(InstantiatorPropagationDUnitTest.class,
          "createServerCacheOne", args);
    } else {
      return server.invoke(InstantiatorPropagationDUnitTest.class,
          "createServerCacheTwo", args);
    }
  }

  public static void setClientServerObserver1() {
    PoolImpl.IS_INSTANTIATOR_CALLBACK = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      @Override
      public void beforeSendingToServer(EventID eventID) {
        eventId = eventID;
        System.out.println("client2= " + client2 + " eventid= " + eventID);
        client2.invoke(() -> InstantiatorPropagationDUnitTest.setEventId(eventId));

      }

    });
  }

  /**
   * sets the EventId value in the VM
   */
  public static void setEventId(EventID eventID) {
    eventId = eventID;
  }

  public static void setClientServerObserver2() {
    PoolImpl.IS_INSTANTIATOR_CALLBACK = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      @Override
      public void afterReceivingFromServer(EventID eventID) {
        System.out.println("Observer2 received " + eventID + "; my eventID is " + eventId);
        testEventIDResult = eventID.equals(eventId);
      }

    });
  }
}


// TODO: move the following classes to be inner classes
abstract class ConfigurableObject implements DataSerializable {
  private int field1;

  public ConfigurableObject() {
    this.field1 = new Random().nextInt();
  }

  public int getIndex() {
    return 1;
  };

  public void validate(int index) {};

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.field1 = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.field1);
  }
}


class TestObject1 extends ConfigurableObject {
}


class TestObject2 extends ConfigurableObject {
}


class TestObject3 extends ConfigurableObject {
}


class TestObject4 extends ConfigurableObject {
}


class TestObject5 extends ConfigurableObject {
}


class TestObject6 extends ConfigurableObject {
}


class TestObject7 extends ConfigurableObject {
}


class TestObject8 extends ConfigurableObject {
}


class TestObject10 extends ConfigurableObject {
}


class TestObject11 extends ConfigurableObject {
}


class TestObject12 extends ConfigurableObject {
}


class TestObject19 extends ConfigurableObject {
}


class TestObject20 extends ConfigurableObject {
  public static Boolean fieldRegistered = null;

  {
    fieldRegistered = Boolean.TRUE;
  }
}
