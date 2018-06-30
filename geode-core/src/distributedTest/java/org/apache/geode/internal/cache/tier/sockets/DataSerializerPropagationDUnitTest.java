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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.TestDataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.ClientServerObserverAdapter;
import org.apache.geode.internal.cache.ClientServerObserverHolder;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({DistributedTest.class, ClientServerTest.class, SerializationTest.class})
public final class DataSerializerPropagationDUnitTest extends JUnit4DistributedTestCase {
  private static final int instanceCountWithAllPuts = 3;
  private static final int instanceCountWithOnePut = 1;
  private static final String REGION_NAME = "ClientServerDataSerializersRegistrationDUnitTest";

  private static Cache cache = null;

  private static VM client1 = null;
  private static VM client2 = null;

  private VM server1 = null;
  private VM server2 = null;

  // The port numbers for the two servers, once they've been started.
  private int PORT1 = -1;
  private int PORT2 = -1;

  protected static EventID eventId;

  private static volatile EventID observedEventID;

  private static EventID getObservedEventID() {
    return observedEventID;
  }

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    client1 = host.getVM(0);
    client2 = host.getVM(1);
    server1 = host.getVM(2);
    server2 = host.getVM(3);
    PORT1 = -1;
    PORT2 = -1;
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
    new DataSerializerPropagationDUnitTest().createCache(props);
    Pool p = PoolManager.createFactory().addServer(host, port1.intValue()).setMinConnections(1)
        .setSubscriptionEnabled(true).setPingInterval(200)
        .create("ClientServerDataSerializersRegistrationDUnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());
    Region r = cache.createRegion(REGION_NAME, factory.create());
    r.registerInterest("ALL_KEYS");
  }

  private int initServerCache(VM server) {
    return server.invoke(DataSerializerPropagationDUnitTest::createServerCache);
  }

  private void tearDownForVM() {
    eventId = null;
    observedEventID = null;
    PoolImpl.IS_INSTANTIATOR_CALLBACK = false;
    DataSerializerPropagationDUnitTest.closeCache();
  }

  @Override
  public final void preTearDown() throws Exception {
    try {
      // close the clients first
      client1.invoke(this::tearDownForVM);
      client2.invoke(this::tearDownForVM);
      tearDownForVM();

      server1.invoke(this::tearDownForVM);
      server2.invoke(this::tearDownForVM);

      client1 = null;
      client2 = null;
      server1 = null;
      server2 = null;

    } finally {
      TestDataSerializer.successfullyInstantiated = false;
      DistributedTestUtils.unregisterAllDataSerializersFromAllVms();
    }
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
  }

  private static void verifyDataSerializers(final int numOfDataSerializers) {
    Awaitility.await().atMost(60, TimeUnit.SECONDS)
        .until(() -> assertEquals(
            "serializers: " + Arrays.toString(InternalDataSerializer.getSerializers()),
            InternalDataSerializer.getSerializers().length, numOfDataSerializers));
  }

  private static void registerDataSerializer(Class klass) {
    InternalDataSerializer.register(klass, true);
  }

  private static void registerDSObjectLocalOnly() throws Exception {
    InternalDataSerializer._register(new DSObjectLocalOnly(79), true);
  }

  public static void stopServer() {
    assertEquals("Expected exactly one CacheServer", 1, cache.getCacheServers().size());
    CacheServerImpl bs = (CacheServerImpl) cache.getCacheServers().iterator().next();
    assertNotNull(bs);
    bs.stop();
  }

  public static void startServer() throws Exception {
    Cache c = CacheFactory.getAnyInstance();
    assertEquals("Expected exactly one CacheServer", 1, c.getCacheServers().size());
    CacheServerImpl bs = (CacheServerImpl) c.getCacheServers().iterator().next();
    assertNotNull(bs);
    bs.start();
  }

  /**
   * Register two DataSerializers on the server and verify that they get propagated to client.
   */
  @Test
  public void testServerUpFirstClientLater() throws Exception {
    PORT1 = initServerCache(server1);

    server1.invoke(() -> registerDataSerializer(DSObject1.class));
    server1.invoke(() -> registerDataSerializer(DSObject2.class));

    server1.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(2));

    final String hostName = NetworkUtils.getServerHostName();
    client1.invoke(() -> DataSerializerPropagationDUnitTest.createClientCache(hostName, PORT1));

    client1.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(2));

    // Put some entries from the client
    client1.invoke(() -> {
      Region region = cache.getRegion(REGION_NAME);
      for (int i = 1; i <= 10; i++) {
        region.put(i, i);
      }
    });

    client1.invoke(() -> {
      Region region = cache.getRegion(REGION_NAME);
      assertEquals(1, region.get(1));
    });

    server1.invoke(() -> {
      Region region = cache.getRegion(REGION_NAME);
      for (int i = 1; i <= 10; i++) {
        assertEquals(i, region.get(i));
      }
    });
  }

  @Test
  public void testDataSerializersWith2ClientsN2Servers() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));

    client1.invoke(() -> registerDataSerializer(DSObject1.class));

    client1.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(1)));

    server1.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(1)));

    server2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(1)));

    client2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(1)));
  }

  // this test is for bug 44112
  @Test
  public void testLocalOnlyDataSerializable() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));

    server1.invoke(() -> DataSerializerPropagationDUnitTest.registerDSObjectLocalOnly());

    server1.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(1)));

    server2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(0)));

    client1.invoke(() -> {
      DSObjectLocalOnly.allowDefaultInstantiation = true;
      try {
        DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(1));
      } finally {
        DSObjectLocalOnly.allowDefaultInstantiation = false;
      }
    });

    client2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(0)));
  }

  @Test
  public void testDataSerializersWithServerKill() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));

    client1.invoke(() -> registerDataSerializer(DSObject1.class));

    server1.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(1));

    server2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(1));

    client2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(1));

    // can get server connectivity exception
    final IgnoredException expectedEx =
        IgnoredException.addIgnoredException("Server unreachable", client1);

    // stop the cache server

    server1.invoke(() -> DataSerializerPropagationDUnitTest.stopServer());

    server1.invoke(() -> registerDataSerializer(DSObject2.class));
    server1.invoke(() -> registerDataSerializer(DSObject3.class));

    server2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(3)));

    server1.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(3)));

    client1.invoke(() -> DataSerializerPropagationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithOnePut)));

    client2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(3)));

    expectedEx.remove();
  }

  @Test
  public void testDataSerializers() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client1.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));

    client1.invoke(() -> registerDataSerializer(DSObject1.class));

    server1.invoke(() -> registerDataSerializer(DSObject2.class));

    server2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(2)));

    server1.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(2)));

    client1.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(2)));

    client2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(2)));
  }

  @Test
  public void testDataSerializersWithServerKillAndReInvoked() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);
    client1.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));

    client1.invoke(() -> registerDataSerializer(DSObject1.class));
    client1.invoke(() -> DataSerializerPropagationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithOnePut)));

    server1.invoke(() -> DataSerializerPropagationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithOnePut)));

    server2.invoke(() -> DataSerializerPropagationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithOnePut)));

    client2.invoke(() -> DataSerializerPropagationDUnitTest
        .verifyDataSerializers(new Integer(instanceCountWithOnePut)));

    // can get server connectivity exception
    try (IgnoredException removedLater =
        IgnoredException.addIgnoredException("Server unreachable", client1)) {

      server1.invoke(() -> DataSerializerPropagationDUnitTest.stopServer());

      try {
        client1.invoke(() -> registerDataSerializer(DSObject2.class));
      } catch (Exception e) {// we are putting in a client whose server is dead

      }
      try {
        client1.invoke(() -> registerDataSerializer(DSObject3.class));
      } catch (Exception e) {// we are putting in a client whose server is
        // dead
      }
      server1.invoke(() -> DataSerializerPropagationDUnitTest.startServer());

      client1.invoke(() -> DataSerializerPropagationDUnitTest
          .verifyDataSerializers(new Integer(instanceCountWithAllPuts)));

      server1.invoke(() -> DataSerializerPropagationDUnitTest
          .verifyDataSerializers(new Integer(instanceCountWithAllPuts)));

      server2.invoke(() -> DataSerializerPropagationDUnitTest
          .verifyDataSerializers(new Integer(instanceCountWithAllPuts)));

      client1.invoke(() -> DataSerializerPropagationDUnitTest
          .verifyDataSerializers(new Integer(instanceCountWithAllPuts)));
    }
  }

  @Test
  public void registerDataSerializerGetsPropagatedToAnotherClient() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1));

    client2.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server2.getHost()), new Integer(PORT2)));
    setClientServerObserver1();
    client2.invoke(DataSerializerPropagationDUnitTest::setClientServerObserver2);

    registerDataSerializer(DSObject1.class);

    assertNotNull(eventId);
    Awaitility.await("event propagates to client 2").until(() -> assertEquals(eventId,
        client2.invoke(DataSerializerPropagationDUnitTest::getObservedEventID)));
  }

  @Test
  public void testLazyLoadingOfDataSerializersWith2ClientsN2Servers() throws Exception {
    PORT1 = initServerCache(server1);
    PORT2 = initServerCache(server2);

    client2.bounce();

    client1.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> DataSerializerPropagationDUnitTest
        .createClientCache(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT2)));

    client1.invoke(() -> registerDataSerializer(TestDataSerializer.class));

    client1.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(1)));

    // Verify through InternalDataSerializer's map entries
    client1.invoke(
        () -> DataSerializerPropagationDUnitTest.verifyLoadedDataSerializers(new Integer(1)));
    client1.invoke(
        () -> DataSerializerPropagationDUnitTest.verifyDataSerializerClassNamesMap(new Integer(0)));
    client1
        .invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializerIDMap(new Integer(0)));
    client1.invoke(() -> DataSerializerPropagationDUnitTest
        .verifyDataSerializerSupportedClassNamesMap(new Integer(0)));

    server1.invoke(
        () -> DataSerializerPropagationDUnitTest.verifyLoadedDataSerializers(new Integer(1)));
    server1.invoke(
        () -> DataSerializerPropagationDUnitTest.verifyDataSerializerClassNamesMap(new Integer(0)));
    server1
        .invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializerIDMap(new Integer(0)));
    server1.invoke(() -> DataSerializerPropagationDUnitTest
        .verifyDataSerializerSupportedClassNamesMap(new Integer(0)));

    server2.invoke(
        () -> DataSerializerPropagationDUnitTest.verifyLoadedDataSerializers(new Integer(1)));
    server2.invoke(
        () -> DataSerializerPropagationDUnitTest.verifyDataSerializerClassNamesMap(new Integer(0)));
    server2
        .invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializerIDMap(new Integer(0)));
    server2.invoke(() -> DataSerializerPropagationDUnitTest
        .verifyDataSerializerSupportedClassNamesMap(new Integer(0)));

    // Verify by boolean flag in static initializer.
    server1.invoke(() -> DataSerializerPropagationDUnitTest.verifyTestDataSerializerLoaded());
    server2.invoke(() -> DataSerializerPropagationDUnitTest.verifyTestDataSerializerLoaded());
    client2.invoke(() -> DataSerializerPropagationDUnitTest.verifyTestDataSerializerNotLoaded());

    // Verify through InternalDataSerializer.getSerializers()
    server1.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(1)));
    server2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(1)));

    // Verify through InternalDataSerializer's map entries
    client2.invoke(
        () -> DataSerializerPropagationDUnitTest.verifyLoadedDataSerializers(new Integer(0)));
    client2.invoke(
        () -> DataSerializerPropagationDUnitTest.verifyDataSerializerClassNamesMap(new Integer(1)));
    client2
        .invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializerIDMap(new Integer(1)));
    client2.invoke(() -> DataSerializerPropagationDUnitTest
        .verifyDataSerializerSupportedClassNamesMap(new Integer(3)));

    // Force TestDataSerializer to be loaded into vm by invoking
    // InternalDataSerialier.getSerializers()
    client2.invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializers(new Integer(1)));

    // Verify by boolean flag in static initializer.
    client2.invoke(() -> DataSerializerPropagationDUnitTest.verifyTestDataSerializerLoaded());
    // Verify through InternalDataSerializer's map entries
    client2.invoke(
        () -> DataSerializerPropagationDUnitTest.verifyLoadedDataSerializers(new Integer(1)));
    client2.invoke(
        () -> DataSerializerPropagationDUnitTest.verifyDataSerializerClassNamesMap(new Integer(0)));
    client2
        .invoke(() -> DataSerializerPropagationDUnitTest.verifyDataSerializerIDMap(new Integer(0)));
    client2.invoke(() -> DataSerializerPropagationDUnitTest
        .verifyDataSerializerSupportedClassNamesMap(new Integer(0)));
  }

  private static void verifyTestDataSerializerNotLoaded() {
    assertFalse("TestDataSerializer should not have been loaded in this vm.",
        TestDataSerializer.successfullyInstantiated);
  }

  private static void verifyTestDataSerializerLoaded() {
    assertTrue("TestDataSerializer should have been loaded in this vm.",
        TestDataSerializer.successfullyInstantiated);
  }

  private static void verifyLoadedDataSerializers(int expected) {
    int actual = InternalDataSerializer.getLoadedDataSerializers();
    assertEquals(expected, actual);
  }

  private static void verifyDataSerializerClassNamesMap(int expected) {
    int actual = InternalDataSerializer.getDsClassesToHoldersMap().size();
    assertEquals(expected, actual);
  }

  private static void verifyDataSerializerIDMap(int expected) {
    int actual = InternalDataSerializer.getIdsToHoldersMap().size();
    assertEquals(expected, actual);
  }

  private static void verifyDataSerializerSupportedClassNamesMap(int expected) {
    int actual = InternalDataSerializer.getSupportedClassesToHoldersMap().size();
    assertEquals(expected, actual);
  }

  private static Integer createServerCache() throws Exception {
    new DataSerializerPropagationDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setMirrorType(MirrorType.KEYS_VALUES);

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setMaxThreads(0);
    server1.setNotifyBySubscription(true);
    server1.start();
    return new Integer(port);
  }

  private static void setClientServerObserver1() {
    PoolImpl.IS_INSTANTIATOR_CALLBACK = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      @Override
      public void beforeSendingToServer(EventID eventID) {
        eventId = eventID;
      }
    });
  }

  private static void setClientServerObserver2() {
    PoolImpl.IS_INSTANTIATOR_CALLBACK = true;
    ClientServerObserverHolder.setInstance(new ClientServerObserverAdapter() {
      @Override
      public void afterReceivingFromServer(EventID eventID) {
        observedEventID = eventID;
      }
    });
  }
}


class DSObject1 extends DataSerializer {
  public DSObject1() {
    // Do nothing.
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
    out.write(5);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject2 extends DataSerializer {
  public DSObject2() {
    // Do nothing.
  }

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
    out.write(15);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class DSObject3 extends DataSerializer {
  public DSObject3() {
    // Do nothing.
  }

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
    out.write(25);
    return false;
  }

  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    readInteger(in);
    return null;
  }
}


class Bogus {
  // Intentionally left blank.
}


/**
 * This data serializer can be created locally but remote guys who call the default constructor will
 * fail.
 */
class DSObjectLocalOnly extends DataSerializer {
  static boolean allowDefaultInstantiation = false;

  DSObjectLocalOnly(int v) {
    // this allows us to make one locally without setting the static.
  }

  // used for serialization
  public DSObjectLocalOnly() {
    if (!allowDefaultInstantiation) {
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
