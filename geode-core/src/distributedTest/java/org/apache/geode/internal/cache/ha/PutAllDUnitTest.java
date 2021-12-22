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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.QueueStateImpl.SequenceIdAndExpirationObject;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Test to verify correct propagation of operations eventID's for put all
 *
 * @since GemFire 5.1
 */
@Category({ClientSubscriptionTest.class})
public class PutAllDUnitTest extends JUnit4DistributedTestCase {

  /** server1 VM **/
  VM server1 = null;

  /** server2 VM **/
  VM server2 = null;

  /** client1 VM **/
  VM client1 = null;

  /** client2 VM **/
  VM client2 = null;

  /** port of server1 **/
  public int PORT1;

  /** port of server2 **/
  public int PORT2;

  /** region name **/
  private static final String REGION_NAME = "PutAllDUnitTest_Region";
  /** cache **/
  private static Cache cache = null;
  /** server **/
  static CacheServerImpl server = null;

  /** test constructor **/
  public PutAllDUnitTest() {
    super();
  }

  /** get the hosts and the VMs **/
  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    client1 = host.getVM(2);
    client2 = host.getVM(3);
  }

  /** close the caches **/
  @Override
  public final void preTearDown() throws Exception {
    client1.invoke(PutAllDUnitTest::closeCache);
    client2.invoke(PutAllDUnitTest::closeCache);
    // close server
    server1.invoke(PutAllDUnitTest::closeCache);
    server2.invoke(PutAllDUnitTest::closeCache);

    // close cache in the controller VM (ezoerner) Not doing this was causing CacheExistsExceptions
    // in other dunit tests
    closeCache();
  }

  /** stops the server **/
  private CacheSerializableRunnable stopServer() {
    CacheSerializableRunnable stopserver = new CacheSerializableRunnable("stopServer") {
      @Override
      public void run2() throws CacheException {
        server.stop();
      }
    };
    return stopserver;
  }

  /** function to create a 2 servers and 3 client (1 client will be in the unit controller VM) **/
  private void createClientServerConfiguration() {
    PORT1 = server1.invoke(PutAllDUnitTest::createServerCache).intValue();
    PORT2 = server2.invoke(PutAllDUnitTest::createServerCache).intValue();
    client1.invoke(() -> PutAllDUnitTest
        .createClientCache1(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1)));
    client2.invoke(() -> PutAllDUnitTest
        .createClientCache2(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT2)));
    try {
      createClientCache2(NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT2));
    } catch (Exception e) {
      fail(" test failed due to " + e);
    }
  }

  /** create the server **/
  public static Integer createServerCache() throws Exception {
    new PutAllDUnitTest().createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    CacheListener clientListener = new HAEventIdPropagationListenerForClient1();
    factory.setCacheListener(clientListener);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    server = (CacheServerImpl) cache.addCacheServer();
    assertNotNull(server);
    int port = getRandomAvailableTCPPort();
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  /** function to create cache **/
  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  private static PoolImpl pool = null;

  /**
   * function to create client cache with HAEventIdPropagationListenerForClient2 as the listener
   **/
  public static void createClientCache2(String host, Integer port1) throws Exception {
    int PORT1 = port1.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new PutAllDUnitTest().createCache(props);
    props.setProperty("retryAttempts", "2");
    props.setProperty("endpoints", "ep1=" + host + ":" + PORT1);
    props.setProperty("redundancyLevel", "-1");
    props.setProperty("establishCallbackConnection", "true");
    props.setProperty("LBPolicy", "Sticky");
    props.setProperty("readTimeout", "2000");
    props.setProperty("socketBufferSize", "1000");
    props.setProperty("retryInterval", "250");
    props.setProperty("connectionsPerServer", "2");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    PoolImpl p = (PoolImpl) ClientServerTestCase.configureConnectionPool(factory, host, PORT1, -1,
        true, -1, 2, null);
    CacheListener clientListener = new HAEventIdPropagationListenerForClient2();
    factory.setCacheListener(clientListener);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    Region region = cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(region);
    region.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    pool = p;
  }


  /** function to create client cache **/
  public static void createClientCache1(String host, Integer port1) throws Exception {
    int PORT1 = port1.intValue();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    new PutAllDUnitTest().createCache(props);
    props.setProperty("retryAttempts", "2");
    props.setProperty("endpoints", "ep1=" + host + ":" + PORT1);
    props.setProperty("redundancyLevel", "-1");
    props.setProperty("establishCallbackConnection", "true");
    props.setProperty("LBPolicy", "Sticky");
    props.setProperty("readTimeout", "2000");
    props.setProperty("socketBufferSize", "1000");
    props.setProperty("retryInterval", "250");
    props.setProperty("connectionsPerServer", "2");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    PoolImpl p = (PoolImpl) ClientServerTestCase.configureConnectionPool(factory, host, PORT1, -1,
        true, -1, 2, null);
    CacheListener clientListener = new HAEventIdPropagationListenerForClient1();
    factory.setCacheListener(clientListener);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    Region region = cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(region);
    region.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    pool = p;
  }

  /** function to close cache **/
  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      try {
        cache.close();
        cache.getDistributedSystem().disconnect();
      } catch (RuntimeException e) {
        // ignore
      }
    }
  }

  /** function to assert that the ThreadIdtoSequence id Map is not Null but is empty **/
  public static void assertThreadIdToSequenceIdMapisNotNullButEmpty() {
    Map map = pool.getThreadIdToSequenceIdMap();
    assertNotNull(map);
    // I didn't change this method name for merge purposes, but because of the
    // marker, the map will contain one entry
    assertTrue(map.size() == 1);

  }

  /** function to assert that the ThreadIdtoSequence id Map is not Null and has only one entry **/
  public static Object assertThreadIdToSequenceIdMapHasEntryId() {
    Map map = pool.getThreadIdToSequenceIdMap();
    assertNotNull(map);
    // The map size can now be 1 or 2 because of the server thread putting
    // the marker in the queue. If it is 2, the first entry is the server
    // thread; the second is the client thread. If it is 1, the entry is the
    // client thread. The size changes because of the map.clear call below.
    assertTrue(map.size() != 0);

    // Set the entry to the last entry
    Map.Entry entry = null;
    for (final Object o : map.entrySet()) {
      entry = (Map.Entry) o;
    }

    ThreadIdentifier tid = (ThreadIdentifier) entry.getKey();
    SequenceIdAndExpirationObject seo = (SequenceIdAndExpirationObject) entry.getValue();
    long sequenceId = seo.getSequenceId();
    EventID evId = new EventID(tid.getMembershipID(), tid.getThreadID(), sequenceId);
    synchronized (map) {
      map.clear();
    }
    return evId;
  }

  /** function to assert that the ThreadIdtoSequence id Map is not Null and has only one entry **/
  public static Object[] assertThreadIdToSequenceIdMapHasEntryIds() {
    EventID[] evids = new EventID[5];
    Map map = pool.getThreadIdToSequenceIdMap();
    assertNotNull(map);
    evids[0] = putAlleventId1;
    evids[1] = putAlleventId2;
    evids[2] = putAlleventId3;
    evids[3] = putAlleventId4;
    evids[4] = putAlleventId5;
    assertNotNull(evids[0]);
    assertNotNull(evids[1]);
    assertNotNull(evids[2]);
    assertNotNull(evids[3]);
    assertNotNull(evids[4]);
    return evids;
  }


  /** EventId * */
  protected static EventID eventId = null;
  protected static volatile EventID putAlleventId1 = null;
  protected static volatile EventID putAlleventId2 = null;
  protected static volatile EventID putAlleventId3 = null;
  protected static volatile EventID putAlleventId4 = null;
  protected static volatile EventID putAlleventId5 = null;
  protected static volatile EntryEvent putAllevent1 = null;
  protected static volatile EntryEvent putAllevent2 = null;
  protected static volatile EntryEvent putAllevent3 = null;
  protected static volatile EntryEvent putAllevent4 = null;
  protected static volatile EntryEvent putAllevent5 = null;

  protected static final String PUTALL_KEY1 = "putAllKey1";
  protected static final String PUTALL_KEY2 = "putAllKey2";
  protected static final String PUTALL_KEY3 = "putAllKey3";
  protected static final String PUTALL_KEY4 = "putAllKey4";
  protected static final String PUTALL_KEY5 = "putAllKey5";

  private static final String PUTALL_VALUE1 = "putAllValue1";
  private static final String PUTALL_VALUE2 = "putAllValue2";
  private static final String PUTALL_VALUE3 = "putAllValue3";
  private static final String PUTALL_VALUE4 = "putAllValue4";
  private static final String PUTALL_VALUE5 = "putAllValue5";



  /**
   * This test: 1) creates a client server configuration 2) asserts that the ThreadIdToSequenceIdMap
   * is not null but is empty (on the client) 3) does a put on the server 4) Wait till put is
   * received by the server (and also records the eventId in a variable) and returns the eventId
   * generated on the server 5) asserts that the ThreadIdToSequenceIdMap is not null and has one
   * entry (on the client side) and returns the eventId stored in the map 6) verifies the equality
   * of the two event ids
   *
   */
  @Test
  public void testPutAll() throws Exception {
    setReceivedOperationToFalse();
    client2.invoke(PutAllDUnitTest::setReceivedOperationToFalse);
    createClientServerConfiguration();

    EventID[] eventIds1 = (EventID[]) client1.invoke(PutAllDUnitTest::putAll);
    assertNotNull(eventIds1);
    // wait for key to propagate till client
    // assert map not null on client
    client2.invoke(PutAllDUnitTest::waitTillOperationReceived);

    waitTillOperationReceived();
    EventID[] eventIds2 = (EventID[]) client2
        .invoke(PutAllDUnitTest::assertThreadIdToSequenceIdMapHasEntryIds);
    assertNotNull(eventIds2);
    server1.invoke(PutAllDUnitTest::assertGotAllValues);
    server2.invoke(PutAllDUnitTest::assertGotAllValues);
    client1.invoke(PutAllDUnitTest::assertCallbackArgs);
    client2.invoke(PutAllDUnitTest::assertGotAllValues);
    client2.invoke(PutAllDUnitTest::assertCallbackArgs);
    server1.invoke(PutAllDUnitTest::assertCallbackArgs);
    server2.invoke(PutAllDUnitTest::assertCallbackArgs);
    assertGotAllValues();
    assertCallbackArgs();
    EventID[] eventIds3 = (EventID[]) assertThreadIdToSequenceIdMapHasEntryIds();
    for (int i = 0; i < 5; i++) {
      assertNotNull(eventIds1[i]);
      assertNotNull(eventIds2[i]);
      assertEquals(
          "Event id mismatch: eventIds1[" + i + "]" + eventIds1[i].expensiveToString()
              + ": eventIds2[" + i + "]" + eventIds2[i].expensiveToString(),
          eventIds1[i], eventIds2[i]);
    }
    for (int i = 0; i < 5; i++) {
      assertNotNull(eventIds1[i]);
      assertNotNull(eventIds3[i]);
      assertEquals(eventIds1[i], eventIds3[i]);
    }
  }

  public static void setReceivedOperationToFalse() {
    receivedOperation = false;
  }

  public static void assertGotAllValues() {
    Region region = cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(region);
    assertTrue(region.get(PUTALL_KEY1).equals(PUTALL_VALUE1));
    assertTrue(region.get(PUTALL_KEY2).equals(PUTALL_VALUE2));
    assertTrue(region.get(PUTALL_KEY3).equals(PUTALL_VALUE3));
    assertTrue(region.get(PUTALL_KEY4).equals(PUTALL_VALUE4));
    assertTrue(region.get(PUTALL_KEY5).equals(PUTALL_VALUE5));
  }

  public static void assertCallbackArgs() {
    assertEquals("putAllCallbackArg", putAllevent1.getCallbackArgument());
    assertEquals("putAllCallbackArg", putAllevent2.getCallbackArgument());
    assertEquals("putAllCallbackArg", putAllevent3.getCallbackArgument());
    assertEquals("putAllCallbackArg", putAllevent4.getCallbackArgument());
    assertEquals("putAllCallbackArg", putAllevent5.getCallbackArgument());
  }



  /**
   * does an update and return the eventid generated. Eventid is caught in the listener and stored
   * in a static variable*
   */
  public static Object[] putAll() {
    Region region = cache.getRegion(SEPARATOR + REGION_NAME);
    assertNotNull(region);
    try {
      Map map = new LinkedHashMap();
      map.put(PUTALL_KEY1, PUTALL_VALUE1);
      map.put(PUTALL_KEY2, PUTALL_VALUE2);
      map.put(PUTALL_KEY3, PUTALL_VALUE3);
      map.put(PUTALL_KEY4, PUTALL_VALUE4);
      map.put(PUTALL_KEY5, PUTALL_VALUE5);
      region.putAll(map, "putAllCallbackArg");
      EventID[] evids = new EventID[5];
      evids[0] = putAlleventId1;
      evids[1] = putAlleventId2;
      evids[2] = putAlleventId3;
      evids[3] = putAlleventId4;
      evids[4] = putAlleventId5;
      assertNotNull(evids[0]);
      assertNotNull(evids[1]);
      assertNotNull(evids[2]);
      assertNotNull(evids[3]);
      assertNotNull(evids[4]);
      return evids;
    } catch (Exception e) {
      fail("put failed due to " + e);
    }
    return null;
  }



  /** Object to wait on till create is received **/
  protected static Object lockObject = new Object();
  /** boolean to signify receipt of create **/
  protected static volatile boolean receivedOperation = false;

  /** wait till create is received. listener will send a notification if create is received **/
  public static void waitTillOperationReceived() {
    synchronized (lockObject) {
      if (!receivedOperation) {
        try {
          lockObject.wait(10000);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }
      if (!receivedOperation) {
        fail(" operation should have been received but it has not been received yet");
      }
    }

  }

  /**
   * Listener which sends a notification after create to waiting threads and also extracts the event
   * id storing it in a static variable
   *
   */
  static class HAEventIdPropagationListenerForClient2 extends CacheListenerAdapter {

    private int putAllReceivedCount = 0;

    @Override
    public void afterCreate(EntryEvent event) {
      boolean shouldNotify = false;
      Object key = event.getKey();
      if (key.equals(PUTALL_KEY1)) {
        putAllReceivedCount++;
        putAlleventId1 = (EventID) assertThreadIdToSequenceIdMapHasEntryId();
        putAllevent1 = event;
      } else if (key.equals(PUTALL_KEY2)) {
        putAllReceivedCount++;
        putAlleventId2 = (EventID) assertThreadIdToSequenceIdMapHasEntryId();
        putAllevent2 = event;
      } else if (key.equals(PUTALL_KEY3)) {
        putAllReceivedCount++;
        putAlleventId3 = (EventID) assertThreadIdToSequenceIdMapHasEntryId();
        putAllevent3 = event;
      } else if (key.equals(PUTALL_KEY4)) {
        putAllReceivedCount++;
        putAlleventId4 = (EventID) assertThreadIdToSequenceIdMapHasEntryId();
        putAllevent4 = event;
      } else if (key.equals(PUTALL_KEY5)) {
        putAllReceivedCount++;
        putAlleventId5 = (EventID) assertThreadIdToSequenceIdMapHasEntryId();
        putAllevent5 = event;
      }
      if (putAllReceivedCount == 5) {
        shouldNotify = true;
      }
      if (shouldNotify) {
        synchronized (lockObject) {
          receivedOperation = true;
          lockObject.notify();
        }
      }
    }
  }

  /**
   * Listener which sends a notification after create to waiting threads and also extracts the event
   * ids storing them in static variables
   *
   */
  static class HAEventIdPropagationListenerForClient1 extends CacheListenerAdapter {

    private int putAllReceivedCount = 0;

    @Override
    public void afterCreate(EntryEvent event) {
      LogWriterUtils.getLogWriter().fine(" entered after created with " + event.getKey());
      boolean shouldNotify = false;
      Object key = event.getKey();
      if (key.equals(PUTALL_KEY1)) {
        putAllReceivedCount++;
        putAlleventId1 = ((EntryEventImpl) event).getEventId();
        assertNotNull(putAlleventId1);
        putAllevent1 = event;
      } else if (key.equals(PUTALL_KEY2)) {
        putAllReceivedCount++;
        putAlleventId2 = ((EntryEventImpl) event).getEventId();
        assertNotNull(putAlleventId2);
        putAllevent2 = event;
      } else if (key.equals(PUTALL_KEY3)) {
        putAllReceivedCount++;
        putAlleventId3 = ((EntryEventImpl) event).getEventId();
        assertNotNull(putAlleventId3);
        putAllevent3 = event;
      } else if (key.equals(PUTALL_KEY4)) {
        putAllReceivedCount++;
        putAlleventId4 = ((EntryEventImpl) event).getEventId();
        assertNotNull(putAlleventId4);
        putAllevent4 = event;
      } else if (key.equals(PUTALL_KEY5)) {
        putAllReceivedCount++;
        putAlleventId5 = ((EntryEventImpl) event).getEventId();
        assertNotNull(putAlleventId5);
        putAllevent5 = event;
      }
      if (putAllReceivedCount == 5) {
        shouldNotify = true;
      }
      if (shouldNotify) {
        synchronized (lockObject) {
          receivedOperation = true;
          lockObject.notify();
        }
      }
    }

  }

}
