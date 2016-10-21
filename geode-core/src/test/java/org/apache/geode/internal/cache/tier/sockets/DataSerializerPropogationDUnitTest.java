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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.TestDataSerializer;
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
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.StoppableWaitCriterion;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class DataSerializerPropogationDUnitTest extends JUnit4DistributedTestCase {

  private static Cache cache = null;

  private static VM client1 = null;

  private static VM client2 = null;

  private VM server1 = null;

  private VM server2 = null;

  private int PORT1 = -1;

  private int PORT2 = -1;

  private static int instanceCountWithAllPuts = 3;

  private static int instanceCountWithOnePut = 1;

  private static final String REGION_NAME = "ClientServerDataSerializersRegistrationDUnitTest";

  protected static EventID eventId;

  static boolean testEventIDResult = false;

  public static boolean successfullyLoadedTestDataSerializer = false;

  @Override
  public final void postSetUp() throws Exception {
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

  public static void createClientCache(String host, Integer port1) throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new DataSerializerPropogationDUnitTest().createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, port1.intValue()).setMinConnections(1)
        .setSubscriptionEnabled(true).setPingInterval(200)
        .create("ClientServerDataSerializersRegistrationDUnitTestPool");
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
    Object[] args = new Object[] {new Integer(getMaxThreads())};
    return ((Integer) server.invoke(DataSerializerPropogationDUnitTest.class, "createServerCache",
        args)).intValue();
  }

  public static Integer createServerCache(Integer maxThreads) throws Exception {
    new DataSerializerPropogationDUnitTest().createCache(new Properties());
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

  @Override
  public final void preTearDown() throws Exception {
    try {
      // close the clients first
      client1.invoke(() -> DataSerializerPropogationDUnitTest.closeCache());
      client2.invoke(() -> DataSerializerPropogationDUnitTest.closeCache());
      closeCache();

      server1.invoke(() -> DataSerializerPropogationDUnitTest.closeCache());
      server2.invoke(() -> DataSerializerPropogationDUnitTest.closeCache());

      client1 = null;
      client2 = null;
      server1 = null;

    } finally {
      DataSerializerPropogationDUnitTest.successfullyLoadedTestDataSerializer = false;
      DistributedTestUtils.unregisterAllDataSerializersFromAllVms();
    }
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void verifyDataSerializers(final int numOfDataSerializers) {
    verifyDataSerializers(numOfDataSerializers, false);
  }

  public static void verifyDataSerializers(final int numOfDataSerializers,
      final boolean allowNonLocal) {
    WaitCriterion wc = new StoppableWaitCriterion() {
      String excuse;

      private DataSerializer[] getSerializers() {
        allowNonLocalTL.set(allowNonLocal);
        try {
          return InternalDataSerializer.getSerializers();
        } finally {
          allowNonLocalTL.remove();
        }
      }

      public boolean done() {
        return getSerializers().length == numOfDataSerializers;
      }

      public String description() {
        return "expected " + numOfDataSerializers + " but got this "
            + InternalDataSerializer.getSerializers().length + " serializers="
            + java.util.Arrays.toString(getSerializers());
      }

      public boolean stopWaiting() {
        return getSerializers().length > numOfDataSerializers;
      }
    };
    Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
  }

  public static final ThreadLocal<Boolean> allowNonLocalTL = new ThreadLocal<Boolean>();

  public static void registerDSObject1() throws Exception {

    try {
      InternalDataSerializer.register(DSObject1.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject1", e);
    }
  }

  public static void registerDSObject2() throws Exception {

    try {
      InternalDataSerializer.register(DSObject2.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject2", e);
    }
  }

  public static void registerDSObject3() throws Exception {

    try {
      InternalDataSerializer.register(DSObject3.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject2", e);
    }
  }

  public static void registerDSObject4() throws Exception {

    try {
      InternalDataSerializer.register(DSObject4.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject4", e);
    }
  }

  public static void registerDSObject5() throws Exception {

    try {
      InternalDataSerializer.register(DSObject5.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject5", e);
    }
  }

  public static void registerDSObject6() throws Exception {

    try {
      InternalDataSerializer.register(DSObject6.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject6", e);
    }
  }

  public static void registerDSObject7() throws Exception {

    try {
      InternalDataSerializer.register(DSObject7.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject7", e);
    }
  }

  public static void registerDSObject8() throws Exception {

    try {
      InternalDataSerializer.register(DSObject8.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject8", e);
    }
  }

  public static void registerDSObject9() throws Exception {

    try {
      InternalDataSerializer.register(DSObject9.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject9", e);
    }
  }

  public static void registerDSObject10() throws Exception {

    try {
      InternalDataSerializer.register(DSObject10.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject10", e);
    }
  }

  public static void registerDSObject11() throws Exception {

    try {
      InternalDataSerializer.register(DSObject11.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject11", e);
    }
  }

  public static void registerDSObject12() throws Exception {

    try {
      InternalDataSerializer.register(DSObject12.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject12", e);
    }
  }

  public static void registerDSObject13() throws Exception {

    try {
      InternalDataSerializer.register(DSObject13.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObject13", e);
    }
  }

  public static void registerDSObjectLocalOnly() throws Exception {

    try {
      InternalDataSerializer._register(new DSObjectLocalOnly(79), true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in DSObjectLocalOnly", e);
    }
  }

  public static void registerTestDataSerializer() throws Exception {
    try {
      InternalDataSerializer.register(TestDataSerializer.class, true);
    } catch (Exception e) {
      Assert.fail("Test failed due to exception in TestDataSerializer", e);
    }
  }

  public static void stopServer() {
    try {
      assertEquals("Expected exactly one CacheServer", 1, cache.getCacheServers().size());
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
      assertEquals("Expected exactly one CacheServer", 1, c.getCacheServers().size());
      CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
      assertNotNull(bs);
      bs.start();
    } catch (Exception ex) {
      fail("while startServer()  " + ex);
    }
  }

  /**
   * In this test the server is up first.2 DataSerializers are registered on it. Verified if the 2
   * DataSerializers get propogated to client when client gets connected.
   */
  @Test
  public void testServerUpFirstClientLater() throws Exception {
    PORT1 = initServerCache(server1);

    Wait.pause(3000);

    server1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject1());
    server1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject2());

    server1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(2)));

    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));

    // wait for client2 to come online
    Wait.pause(3000);

    client1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(2)));

    // Put some entries from the client
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
  }

  @Test
  public void testDataSerializersWith2ClientsN2Servers() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));

    // wait for client2 to come online
    Wait.pause(2000);

    client1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject3());
    Wait.pause(4000);

    client1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));

    server1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));

    server2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));

    client2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));
  }

  // this test is for bug 44112
  @Test
  public void testLocalOnlyDS() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));

    // wait for client2 to come online
    Wait.pause(2000);

    server1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObjectLocalOnly());

    Wait.pause(4000);

    server1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));

    server2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(0)));

    client1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1),
        Boolean.TRUE));

    client2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(0)));
  }

  @Test
  public void testDataSerializersWithServerKill() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));

    // wait for client2 to come online
    Wait.pause(2000);

    client1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject4());

    // wait for successful registration

    server1
        .invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(Integer.valueOf(1)));

    server2
        .invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(Integer.valueOf(1)));

    client2
        .invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(Integer.valueOf(1)));

    // can get server connectivity exception
    final IgnoredException expectedEx =
        IgnoredException.addIgnoredException("Server unreachable", client1);

    // stop the cache server

    server1.invoke(() -> DataSerializerPropogationDUnitTest.stopServer());

    server1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject5());
    server1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject6());

    server2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(3)));

    server1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(3)));

    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithOnePut)));

    client2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(3)));

    expectedEx.remove();
  }

  @Test
  public void testDataSerializers() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));

    // wait for client2 to come online
    Wait.pause(2000);

    client1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject10());

    server1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject11());
    Wait.pause(4000);

    server2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(2)));

    server1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(2)));

    client1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(2)));

    client2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(2)));
  }

  @Test
  public void testDataSerializersWithServerKillAndReInvoked() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);
    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));

    client1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject7());
    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithOnePut)));

    server1.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithOnePut)));

    server2.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithOnePut)));

    client2.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithOnePut)));

    // can get server connectivity exception
    final IgnoredException expectedEx =
        IgnoredException.addIgnoredException("Server unreachable", client1);

    server1.invoke(() -> DataSerializerPropogationDUnitTest.stopServer());

    try {
      client1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject8());
    } catch (Exception e) {// we are putting in a client whose server is dead

    }
    try {
      client1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject9());
    } catch (Exception e) {// we are putting in a client whose server is
      // dead
    }
    server1.invoke(() -> DataSerializerPropogationDUnitTest.startServer());

    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithAllPuts)));

    server1.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithAllPuts)));

    server2.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithAllPuts)));

    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithAllPuts)));

    expectedEx.remove();
  }

  @Test
  public void testDataSerializerCount() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2));

    // wait for client2 to come online
    Wait.pause(2000);

    client1.invoke(() -> DataSerializerPropogationDUnitTest.registerDSObject12());
    Wait.pause(4000);

    client1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));

    server1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));

    server2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));

    client2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));

    verifyDataSerializers(1);
  }

  /**
   * Test's same eventId being same for the dataserializers at all clients & servers
   * 
   */
  @Test
  public void testDataSerializersEventIdVerificationClientsAndServers() throws Exception {
    PORT1 = initServerCache(server1, 1);
    PORT2 = initServerCache(server2, 2);

    createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1));

    client2.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));
    setClientServerObserver1();
    client2.invoke(() -> DataSerializerPropogationDUnitTest.setClientServerObserver2());

    registerDSObject13();

    Wait.pause(10000);

    Boolean pass =
        (Boolean) client2.invoke(() -> DataSerializerPropogationDUnitTest.verifyResult());
    assertTrue("EventId found Different", pass.booleanValue());

    PoolImpl.IS_INSTANTIATOR_CALLBACK = false;

  }

  @Test
  public void testLazyLoadingOfDataSerializersWith2ClientsN2Servers() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropogationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT2)));

    // wait for client2 to come online
    Wait.pause(2000);

    client1.invoke(() -> DataSerializerPropogationDUnitTest.registerTestDataSerializer());
    Wait.pause(4000);

    client1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));

    // Verify through InternalDataSerializer's map entries
    client1.invoke(
        () -> DataSerializerPropogationDUnitTest.verifyLoadedDataSerializers(new Integer(1)));
    client1.invoke(
        () -> DataSerializerPropogationDUnitTest.verifyDataSerializerClassNamesMap(new Integer(0)));
    client1
        .invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializerIDMap(new Integer(0)));
    client1.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializerSupportedClassNamesMap(new Integer(0)));

    server1.invoke(
        () -> DataSerializerPropogationDUnitTest.verifyLoadedDataSerializers(new Integer(1)));
    server1.invoke(
        () -> DataSerializerPropogationDUnitTest.verifyDataSerializerClassNamesMap(new Integer(0)));
    server1
        .invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializerIDMap(new Integer(0)));
    server1.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializerSupportedClassNamesMap(new Integer(0)));

    server2.invoke(
        () -> DataSerializerPropogationDUnitTest.verifyLoadedDataSerializers(new Integer(1)));
    server2.invoke(
        () -> DataSerializerPropogationDUnitTest.verifyDataSerializerClassNamesMap(new Integer(0)));
    server2
        .invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializerIDMap(new Integer(0)));
    server2.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializerSupportedClassNamesMap(new Integer(0)));

    // Verify by boolean flag in static initializer.
    server1.invoke(() -> DataSerializerPropogationDUnitTest.verifyTestDataSerializerLoaded());
    server2.invoke(() -> DataSerializerPropogationDUnitTest.verifyTestDataSerializerLoaded());
    client2.invoke(() -> DataSerializerPropogationDUnitTest.verifyTestDataSerializerNotLoaded());

    // Verify through InternalDataSerializer.getSerializers()
    server1.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));
    server2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));

    // Verify through InternalDataSerializer's map entries
    client2.invoke(
        () -> DataSerializerPropogationDUnitTest.verifyLoadedDataSerializers(new Integer(0)));
    client2.invoke(
        () -> DataSerializerPropogationDUnitTest.verifyDataSerializerClassNamesMap(new Integer(1)));
    client2
        .invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializerIDMap(new Integer(1)));
    client2.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializerSupportedClassNamesMap(new Integer(3)));

    // Force TestDataSerializer to be loaded into vm by invoking
    // InternalDataSerialier.getSerializers()
    client2.invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializers(new Integer(1)));

    // Verify by boolean flag in static initializer.
    client2.invoke(() -> DataSerializerPropogationDUnitTest.verifyTestDataSerializerLoaded());
    // Verify through InternalDataSerializer's map entries
    client2.invoke(
        () -> DataSerializerPropogationDUnitTest.verifyLoadedDataSerializers(new Integer(1)));
    client2.invoke(
        () -> DataSerializerPropogationDUnitTest.verifyDataSerializerClassNamesMap(new Integer(0)));
    client2
        .invoke(() -> DataSerializerPropogationDUnitTest.verifyDataSerializerIDMap(new Integer(0)));
    client2.invoke(() -> DataSerializerPropogationDUnitTest
        .verifyDataSerializerSupportedClassNamesMap(new Integer(0)));
  }

  public static void verifyTestDataSerializerNotLoaded() {
    assertFalse("TestDataSerializer should not have been loaded in this vm.",
        successfullyLoadedTestDataSerializer);
  }

  public static void verifyTestDataSerializerLoaded() {
    assertTrue("TestDataSerializer should have been loaded in this vm.",
        successfullyLoadedTestDataSerializer);
  }

  public static void verifyLoadedDataSerializers(Integer expected) {
    int actual = InternalDataSerializer.getLoadedDataSerializers();
    assertTrue("Number of loaded data serializers, expected: " + expected + ", actual: " + actual,
        actual == expected);
  }

  public static void verifyDataSerializerClassNamesMap(Integer expected) {
    int actual = InternalDataSerializer.getDsClassesToHoldersMap().size();
    assertTrue(
        "Number of data serializer classnames, expected: " + expected + ", actual: " + actual,
        actual == expected);
  }

  public static void verifyDataSerializerIDMap(Integer expected) {
    int actual = InternalDataSerializer.getIdsToHoldersMap().size();
    assertTrue("Number of ids, expected: " + expected + ", actual: " + actual, actual == expected);
  }

  public static void verifyDataSerializerSupportedClassNamesMap(Integer expected) {
    int actual = InternalDataSerializer.getSupportedClassesToHoldersMap().size();
    assertTrue("Number of supported classnames, expected: " + expected + ", actual: " + actual,
        actual == expected);
  }

  public static Boolean verifyResult() {
    boolean temp = testEventIDResult;
    testEventIDResult = false;
    return new Boolean(temp);
  }

  private int initServerCache(VM server, int serverNo) {
    Object[] args = new Object[] {new Integer(getMaxThreads())};
    if (serverNo == 1) {
      return ((Integer) server.invoke(DataSerializerPropogationDUnitTest.class,
          "createServerCacheOne", args)).intValue();
    } else {
      return ((Integer) server.invoke(DataSerializerPropogationDUnitTest.class,
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
  public static Integer createServerCacheTwo(Integer maxThreads) throws Exception {
    new DataSerializerPropogationDUnitTest().createCache(new Properties());
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
  public static Integer createServerCacheOne(Integer maxThreads) throws Exception {
    new DataSerializerPropogationDUnitTest().createCache(new Properties());
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

  public static void setClientServerObserver1() {
    PoolImpl.IS_INSTANTIATOR_CALLBACK = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      @Override
      public void beforeSendingToServer(EventID eventID) {
        eventId = eventID;
        client2.invoke(() -> DataSerializerPropogationDUnitTest.setEventId(eventID));

      }

    });
  }

  /**
   * sets the EventId value in the VM
   * 
   * @param eventID
   */
  public static void setEventId(EventID eventID) {
    eventId = eventID;
  }

  public static void setClientServerObserver2() {
    PoolImpl.IS_INSTANTIATOR_CALLBACK = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      @Override
      public void afterReceivingFromServer(EventID eventID) {
        testEventIDResult = eventID.equals(eventId);
      }

    });
  }
}


class DSObject1 extends DataSerializer {
  int tempField = 5;

  public DSObject1() {

  }

  @Override
  public int getId() {
    return 1;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject2 extends DataSerializer {
  int tempField = 15;

  public DSObject2() {}

  @Override
  public int getId() {
    return 2;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject3 extends DataSerializer {
  int tempField = 25;

  public DSObject3() {}

  @Override
  public int getId() {
    return 3;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject4 extends DataSerializer {
  int tempField = 5;

  public DSObject4() {

  }

  @Override
  public int getId() {
    return 4;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject5 extends DataSerializer {
  int tempField = 15;

  public DSObject5() {}

  @Override
  public int getId() {
    return 5;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject6 extends DataSerializer {
  int tempField = 25;

  public DSObject6() {}

  @Override
  public int getId() {
    return 6;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject7 extends DataSerializer {
  int tempField = 5;

  public DSObject7() {

  }

  @Override
  public int getId() {
    return 7;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject8 extends DataSerializer {
  int tempField = 15;

  public DSObject8() {}

  @Override
  public int getId() {
    return 8;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject9 extends DataSerializer {
  int tempField = 25;

  public DSObject9() {}

  @Override
  public int getId() {
    return 9;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject10 extends DataSerializer {
  int tempField = 5;

  public DSObject10() {

  }

  @Override
  public int getId() {
    return 10;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject11 extends DataSerializer {
  int tempField = 15;

  public DSObject11() {}

  @Override
  public int getId() {
    return 11;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject12 extends DataSerializer {
  int tempField = 25;

  public DSObject12() {}

  @Override
  public int getId() {
    return 12;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject13 extends DataSerializer {
  int tempField = 25;

  public DSObject13() {}

  @Override
  public int getId() {
    return 19;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {this.getClass()};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    out.write(tempField);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class Bogus {
}


/**
 * This data serializer can be created locally but remote guys who call the default constructor will
 * fail.
 *
 */
class DSObjectLocalOnly extends DataSerializer {
  int tempField;

  public DSObjectLocalOnly(int v) {
    this.tempField = v;
  }

  public DSObjectLocalOnly() {
    Boolean b = DataSerializerPropogationDUnitTest.allowNonLocalTL.get();
    if (b == null || !b.booleanValue()) {
      throw new UnsupportedOperationException("DSObjectLocalOnly can not be deserialized");
    }
  }

  @Override
  public int getId() {
    return 20;
  }

  @Override
  public Class[] getSupportedClasses() {
    return new Class[] {Bogus.class};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    throw new RuntimeException("Did not expect toData to be called");
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    throw new RuntimeException("Did not expect fromData to be called");
  }
}
