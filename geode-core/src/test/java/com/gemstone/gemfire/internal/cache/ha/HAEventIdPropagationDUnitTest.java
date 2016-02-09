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
package com.gemstone.gemfire.internal.cache.ha;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.QueueStateImpl.SequenceIdAndExpirationObject;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.RegionEventImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ConflationDUnitTest;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 *
 * Test to verify correct propagation of EventID from server to client
 *
 * @author Mitul Bid
 * @since 5.1
 */

public class HAEventIdPropagationDUnitTest extends DistributedTestCase
{

  /** server VM * */
  VM server1 = null;

  /** client VM * */
  VM client1 = null;

  /** region name* */
  private static final String REGION_NAME = "HAEventIdPropagationDUnitTest_Region";

  /** cache * */
  private static Cache cache = null;

  /** server * */
  static CacheServerImpl server = null;

  /** test constructor * */
  public HAEventIdPropagationDUnitTest(String name) {
    super(name);
  }

  /** get the hosts and the VMs * */
  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server1.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    client1 = host.getVM(2);
  }

  /** close the caches* */
  @Override
  protected final void preTearDown() throws Exception {
    client1.invoke(HAEventIdPropagationDUnitTest.class, "closeCache");
    // close server
    server1.invoke(HAEventIdPropagationDUnitTest.class, "closeCache");
  }

  /** stops the server * */
  private CacheSerializableRunnable stopServer()
  {

    CacheSerializableRunnable stopserver = new CacheSerializableRunnable(
        "stopServer") {
      public void run2() throws CacheException
      {
        server.stop();
      }

    };

    return stopserver;
  }

  /** function to create a server and a client * */
  private void createClientServerConfiguration()
  {

    int PORT1 = ((Integer)server1.invoke(HAEventIdPropagationDUnitTest.class,
        "createServerCache")).intValue();
    client1.invoke(HAEventIdPropagationDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(server1.getHost()), new Integer(PORT1) });
  }

  /** create the server * */
  public static Integer createServerCache() throws Exception
  {
    new HAEventIdPropagationDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    CacheListener clientListener = new HAEventIdPropagationListenerForServer();
    factory.setCacheListener(clientListener);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    server = (CacheServerImpl)cache.addCacheServer();
    assertNotNull(server);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }

  /** function to create cache * */
  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  private static PoolImpl pool = null;

  /** function to create client cache * */
  public static void createClientCache(String hostName, Integer port1) throws Exception
  {
    int PORT1 = port1.intValue();
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    new HAEventIdPropagationDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    PoolImpl pi = (PoolImpl)ClientServerTestCase.configureConnectionPool(factory, hostName, new int[] {PORT1}, true, -1, 2, null);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    CacheListener clientListener = new HAEventIdPropagationListenerForClient();
    factory.setCacheListener(clientListener);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);
    region.registerInterest("ALL_KEYS", InterestResultPolicy.NONE);
    System.out.println("KKKKKK:["+pi.getName()+"]");;
    PoolImpl p2 = (PoolImpl)PoolManager.find("testPool");
    System.out.println("QQQQ:"+p2);
    pool = pi;
  }

  /** function to close cache * */
  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }


  /**
   * function to assert that the ThreadIdtoSequence id Map is not Null but is
   * empty *
   */
  public static void assertThreadIdToSequenceIdMapisNotNullButEmpty()
  {
    final Map map = pool.getThreadIdToSequenceIdMap();
    assertNotNull(map);
    // changed to check for size 1 due to marker message
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        synchronized (map) {
          return map.size() == 1;
        }
      }
      public String description() {
        return null;
      }
    };
    Wait.waitForCriterion(ev, 10 * 1000, 200, true);
    synchronized(map) {
      LogWriterUtils.getLogWriter().info("assertThreadIdToSequenceIdMapisNotNullButEmpty: map size is " + map.size());
      assertTrue(map.size() == 1);
    }

  }

  /**
   * function to assert that the ThreadIdtoSequence id Map is not Null and has
   * only one entry *
   */
  public static Object assertThreadIdToSequenceIdMapHasEntryId()
  {
    Map map = pool.getThreadIdToSequenceIdMap();
    assertNotNull(map);
    // The map size can now be 1 or 2 because of the server thread putting
    // the marker in the queue. If it is 2, the first entry is the server
    // thread; the second is the client thread. If it is 1, the entry is the
    // client thread. The size changes because of the map.clear call below.
    assertTrue(map.size() != 0);

    // Set the entry to the last entry
    Map.Entry entry = null;
    for (Iterator threadIdToSequenceIdMapIterator = map.entrySet().iterator(); threadIdToSequenceIdMapIterator.hasNext();) {
      entry = (Map.Entry)threadIdToSequenceIdMapIterator.next();
    }

    ThreadIdentifier tid = (ThreadIdentifier)entry.getKey();
    SequenceIdAndExpirationObject seo = (SequenceIdAndExpirationObject)entry
        .getValue();
    long sequenceId = seo.getSequenceId();
    EventID evId = new EventID(tid.getMembershipID(), tid.getThreadID(),
        sequenceId);
    synchronized(map) {
      map.clear();
    }
    return evId;
  }

  /**
   * function to assert that the ThreadIdtoSequence id Map is not Null and has
   * only one entry *
   */
  public static Object[] assertThreadIdToSequenceIdMapHasEntryIds()
  {
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

  protected static EventID putAlleventId1 = null;

  protected static EventID putAlleventId2 = null;

  protected static EventID putAlleventId3 = null;

  protected static EventID putAlleventId4 = null;

  protected static EventID putAlleventId5 = null;

  protected final static String PUTALL_KEY1 = "putAllKey1";

  protected final static String PUTALL_KEY2 = "putAllKey2";

  protected final static String PUTALL_KEY3 = "putAllKey3";

  protected final static String PUTALL_KEY4 = "putAllKey4";

  protected final static String PUTALL_KEY5 = "putAllKey5";

  /**
   * This test: 1) creates a client server configuration 2) asserts that the
   * ThreadIdToSequenceIdMap is not null but is empty (on the client) 3) does a
   * operation on the server 4) Wait till operation is received by the server (and also
   * records the eventId in a variable) and returns the eventId generated on the
   * server 5) asserts that the ThreadIdToSequenceIdMap is not null and has one
   * entry (on the client side) and returns the eventId stored in the map 6)
   * verifies the equality of the two event ids
   *
   * @throws Exception
   */
  public void testEventIDPropagation() throws Exception
  {
    try {
      createClientServerConfiguration();
      client1.invoke(HAEventIdPropagationDUnitTest.class,"setReceivedOperationToFalse");
      client1.invoke(HAEventIdPropagationDUnitTest.class,
          "assertThreadIdToSequenceIdMapisNotNullButEmpty");
      Object eventId1 = server1.invoke(HAEventIdPropagationDUnitTest.class,
          "putKey1Val1");
      assertNotNull(eventId1);
      // wait for key to propagate till client
      // assert map not null on client
      client1.invoke(HAEventIdPropagationDUnitTest.class,
          "waitTillOperationReceived");
      Object eventId2 = client1.invoke(HAEventIdPropagationDUnitTest.class,
          "assertThreadIdToSequenceIdMapHasEntryId");
      assertNotNull(eventId2);
      if (!eventId1.equals(eventId2)) {
        fail("Test failed as the eventIds are not equal");
      }

      client1.invoke(HAEventIdPropagationDUnitTest.class,"setReceivedOperationToFalse");
      eventId1 = server1.invoke(HAEventIdPropagationDUnitTest.class,
          "updateKey1");
      assertNotNull(eventId1);
      // wait for key to propagate till client
      // assert map not null on client
      client1.invoke(HAEventIdPropagationDUnitTest.class,
          "waitTillOperationReceived");
      eventId2 = client1.invoke(HAEventIdPropagationDUnitTest.class,
          "assertThreadIdToSequenceIdMapHasEntryId");
      assertNotNull(eventId2);
      if (!eventId1.equals(eventId2)) {
        fail("Test failed as the eventIds are not equal");
      }

      client1.invoke(HAEventIdPropagationDUnitTest.class,"setReceivedOperationToFalse");
      eventId1 = server1.invoke(HAEventIdPropagationDUnitTest.class,
          "invalidateKey1");
      assertNotNull(eventId1);
      // wait for key to propagate till client
      // assert map not null on client
      client1.invoke(HAEventIdPropagationDUnitTest.class,
          "waitTillOperationReceived");
      eventId2 = client1.invoke(HAEventIdPropagationDUnitTest.class,
          "assertThreadIdToSequenceIdMapHasEntryId");
      assertNotNull(eventId2);
      if (!eventId1.equals(eventId2)) {
        fail("Test failed as the eventIds are not equal");
      }

      client1.invoke(HAEventIdPropagationDUnitTest.class,"setReceivedOperationToFalse");
      EventID[] eventIds1 = (EventID[])server1.invoke(
          HAEventIdPropagationDUnitTest.class, "putAll");
      assertNotNull(eventIds1);
      // wait for key to propagate till client
      // assert map not null on client
      client1.invoke(HAEventIdPropagationDUnitTest.class,
          "waitTillOperationReceived");
      EventID[] eventIds2 = (EventID[])client1.invoke(
          HAEventIdPropagationDUnitTest.class,
          "assertThreadIdToSequenceIdMapHasEntryIds");
      assertNotNull(eventIds2);
      for (int i = 0; i < 5; i++) {
        assertNotNull(eventIds1[i]);
        assertNotNull(eventIds2[i]);
        if (!eventIds1[i].equals(eventIds2[i])) {
          fail(" eventIds are not equal");
        }
      }


      client1.invoke(HAEventIdPropagationDUnitTest.class,"setReceivedOperationToFalse");
      eventId1 = server1.invoke(HAEventIdPropagationDUnitTest.class,
          "removePUTALL_KEY1");
      assertNotNull(eventId1);
      // wait for key to propagate till client
      // assert map not null on client
      client1.invoke(HAEventIdPropagationDUnitTest.class,
          "waitTillOperationReceived");
      eventId2 = client1.invoke(HAEventIdPropagationDUnitTest.class,
          "assertThreadIdToSequenceIdMapHasEntryId");
      assertNotNull(eventId2);
      if (!eventId1.equals(eventId2)) {
        fail("Test failed as the eventIds are not equal");
      }

      client1.invoke(HAEventIdPropagationDUnitTest.class,"setReceivedOperationToFalse");
      eventId1 = server1.invoke(HAEventIdPropagationDUnitTest.class,
          "destroyKey1");
      assertNotNull(eventId1);
      // wait for key to propagate till client
      // assert map not null on client
      client1.invoke(HAEventIdPropagationDUnitTest.class,
          "waitTillOperationReceived");
      eventId2 = client1.invoke(HAEventIdPropagationDUnitTest.class,
          "assertThreadIdToSequenceIdMapHasEntryId");
      assertNotNull(eventId2);
      if (!eventId1.equals(eventId2)) {
        fail("Test failed as the eventIds are not equal");
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      Assert.fail("test failed due to " + e, e);
    }
  }


  public void testEventIDPropagationForClear() throws Exception
  {
    createClientServerConfiguration();
    client1.invoke(HAEventIdPropagationDUnitTest.class,"setReceivedOperationToFalse");
    client1.invoke(HAEventIdPropagationDUnitTest.class,
        "assertThreadIdToSequenceIdMapisNotNullButEmpty");
    Object eventId1 = server1.invoke(HAEventIdPropagationDUnitTest.class,
    "clearRg");
    assertNotNull(eventId1);
    // wait for key to propagate till client
    // assert map not null on client
    client1.invoke(HAEventIdPropagationDUnitTest.class,
        "waitTillOperationReceived");
    Object eventId2 = client1.invoke(HAEventIdPropagationDUnitTest.class,
        "assertThreadIdToSequenceIdMapHasEntryId");
    assertNotNull(eventId2);
    if (!eventId1.equals(eventId2)) {
      fail("Test failed as the clear eventIds are not equal");
    }

  }


  public void _testEventIDPropagationForDestroyRegion() throws Exception
  {
    createClientServerConfiguration();
    client1.invoke(HAEventIdPropagationDUnitTest.class,"setReceivedOperationToFalse");
    Object eventId1 = server1.invoke(HAEventIdPropagationDUnitTest.class,
        "destroyRegion");
    assertNotNull(eventId1);
    // wait for key to propagate till client
    // assert map not null on client
    client1.invoke(HAEventIdPropagationDUnitTest.class,
        "waitTillOperationReceived");
    Object eventId2 = client1.invoke(HAEventIdPropagationDUnitTest.class,
        "assertThreadIdToSequenceIdMapHasEntryId");
    assertNotNull(eventId2);
    if (!eventId1.equals(eventId2)) {
      fail("Test failed as the eventIds are not equal");
    }
 }


  public static void setReceivedOperationToFalse()
  {
    receivedOperation = false;
  }

  /**
   * does a put and return the eventid generated. Eventid is caught in the
   * listener and stored in a static variable*
   */
  public static Object putKey1Val1()
  {
    try {
      Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(region);

      region.create("key1", "value1");
      return eventId;
    }
    catch (Exception e) {
      fail("put failed due to " + e);
    }
    return null;
  }

  /**
   * does an update and return the eventid generated. Eventid is caught in the
   * listener and stored in a static variable*
   */
  public static Object updateKey1()
  {
    try {
      Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(region);

      region.put("key1", "value2");
      return eventId;
    }
    catch (Exception e) {
      fail("put failed due to " + e);
    }
    return null;
  }

  /**
   * does an update and return the eventid generated. Eventid is caught in the
   * listener and stored in a static variable*
   */
  public static Object[] putAll()
  {
    try {
      Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(region);

      Map map = new LinkedHashMap();
      map.put(PUTALL_KEY1, "value1");
      map.put(PUTALL_KEY2, "value1");
      map.put(PUTALL_KEY3, "value1");
      map.put(PUTALL_KEY4, "value1");
      map.put(PUTALL_KEY5, "value1");
      region.putAll(map);
      Thread.sleep(5000);
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
    }
    catch (Exception e) {
      fail("put failed due to " + e);
    }
    return null;
  }

  /**
   * does an invalidate and return the eventid generated. Eventid is caught in
   * the listener and stored in a static variable*
   */
  public static Object invalidateKey1()
  {
    try {
      Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(region);

      region.invalidate("key1");
      return eventId;
    }
    catch (Exception e) {
      fail("put failed due to " + e);
    }
    return null;
  }

  /**
   * does a destroy and return the eventid generated. Eventid is caught in the
   * listener and stored in a static variable*
   */
  public static Object destroyKey1()
  {
    try {
      Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(region);

      region.destroy("key1");
      return eventId;
    }
    catch (Exception e) {
      fail("put failed due to " + e);
    }
    return null;
  }


  /**
   * does a remove and return the eventid generated. Eventid is caught in the
   * listener and stored in a static variable*
   */
  public static Object removePUTALL_KEY1()
  {
    try {
      Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(region);

      region.remove(PUTALL_KEY1);
      return eventId;
    }
    catch (Exception e) {
      fail("put failed due to " + e);
    }
    return null;
  }


  /**
   * does a clear and return the eventid generated. Eventid is caught in the
   * listener and stored in a static variable*
   */
  public static Object clearRg()
  {
    try {
      Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(region);
      region.clear();
      return eventId;
    }
    catch (Exception e) {
      fail("clear failed due to " + e);
    }
    return null;
  }


  /**
   * does a destroy region return the eventid generated. Eventid is caught in
   * the listener and stored in a static variable*
   */
  public static Object destroyRegion()
  {
    try {
      Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(region);

      region.destroyRegion();
      region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNull(region);
      return eventId;
    }
    catch (Exception e) {
      fail("Destroy failed due to " + e);
    }
    return null;
  }

  /** Object to wait on till create is received * */
  protected static Object lockObject = new Object();

  /** boolean to signify receipt of create * */
  protected static volatile boolean receivedOperation = false;

  /**
   * wait till create is received. listener will send a notification if create
   * is received*
   */
  public static void waitTillOperationReceived()
  {
    synchronized (lockObject) {
      if (!receivedOperation) {
        try {
          lockObject.wait(10000);
        }
        catch (InterruptedException e) {
          fail("interrupted");
        }
      }
      if (!receivedOperation) {
        fail(" operation should have been received but it has not been received yet");
      }
    }

  }

  /**
   * Listener which sends a notification after create to waiting threads and
   * also extracts teh event id storing it in a static variable
   *
   */
  static class HAEventIdPropagationListenerForClient extends
      CacheListenerAdapter
  {

    private int putAllReceivedCount = 0;

    public void afterCreate(EntryEvent event)
    {
      LogWriterUtils.getLogWriter().fine(" entered after created with " + event.getKey());
      boolean shouldNotify = false;
      Object key = event.getKey();
      if (key.equals(PUTALL_KEY1)) {
        putAllReceivedCount++;
        putAlleventId1 = (EventID)assertThreadIdToSequenceIdMapHasEntryId();
      }
      else if (key.equals(PUTALL_KEY2)) {
        putAllReceivedCount++;
        putAlleventId2 = (EventID)assertThreadIdToSequenceIdMapHasEntryId();
      }
      else if (key.equals(PUTALL_KEY3)) {
        putAllReceivedCount++;
        putAlleventId3 = (EventID)assertThreadIdToSequenceIdMapHasEntryId();
      }
      else if (key.equals(PUTALL_KEY4)) {
        putAllReceivedCount++;
        putAlleventId4 = (EventID)assertThreadIdToSequenceIdMapHasEntryId();
      }
      else if (key.equals(PUTALL_KEY5)) {
        putAllReceivedCount++;
        putAlleventId5 = (EventID)assertThreadIdToSequenceIdMapHasEntryId();
      }
      else {
        shouldNotify = true;
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

    public void afterUpdate(EntryEvent event)
    {
      synchronized (lockObject) {
        receivedOperation = true;
        lockObject.notify();
      }
    }

    public void afterInvalidate(EntryEvent event)
    {
      synchronized (lockObject) {
        receivedOperation = true;
        lockObject.notify();
      }
    }

    public void afterDestroy(EntryEvent event)
    {
      synchronized (lockObject) {
        receivedOperation = true;
        lockObject.notify();
      }
    }

    public void afterRegionDestroy(RegionEvent event)
    {
      synchronized (lockObject) {
        receivedOperation = true;
        lockObject.notify();

      }
    }

    public void afterRegionClear(RegionEvent event)
    {
      synchronized (lockObject) {
        receivedOperation = true;
        lockObject.notify();
      }
    }

  }

  /**
   * Listener which sends a notification after create to waiting threads and
   * also extracts teh event id storing it in a static variable
   *
   */
  static class HAEventIdPropagationListenerForServer extends
      CacheListenerAdapter
  {

    private int putAllReceivedCount = 0;

    public void afterCreate(EntryEvent event)
    {
      LogWriterUtils.getLogWriter().fine(" entered after created with " + event.getKey());
      boolean shouldNotify = false;
      Object key = event.getKey();
      if (key.equals(PUTALL_KEY1)) {
        putAllReceivedCount++;
        putAlleventId1 = ((EntryEventImpl)event).getEventId();
        assertNotNull(putAlleventId1);
      }
      else if (key.equals(PUTALL_KEY2)) {
        putAllReceivedCount++;
        putAlleventId2 = ((EntryEventImpl)event).getEventId();
        assertNotNull(putAlleventId2);
      }
      else if (key.equals(PUTALL_KEY3)) {
        putAllReceivedCount++;
        putAlleventId3 = ((EntryEventImpl)event).getEventId();
        assertNotNull(putAlleventId3);
      }
      else if (key.equals(PUTALL_KEY4)) {
        putAllReceivedCount++;
        putAlleventId4 = ((EntryEventImpl)event).getEventId();
        assertNotNull(putAlleventId4);
      }
      else if (key.equals(PUTALL_KEY5)) {
        putAllReceivedCount++;
        putAlleventId5 = ((EntryEventImpl)event).getEventId();
        assertNotNull(putAlleventId5);
      }
      else {
        shouldNotify = true;
        eventId = ((EntryEventImpl)event).getEventId();
        assertNotNull(eventId);
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

    public void afterUpdate(EntryEvent event)
    {
      eventId = ((EntryEventImpl)event).getEventId();
      assertNotNull(eventId);
      synchronized (lockObject) {
        receivedOperation = true;
        lockObject.notify();
      }
    }

    public void afterInvalidate(EntryEvent event)
    {
      eventId = ((EntryEventImpl)event).getEventId();
      assertNotNull(eventId);
      synchronized (lockObject) {
        receivedOperation = true;
        lockObject.notify();
      }
    }

    public void afterDestroy(EntryEvent event)
    {
      eventId = ((EntryEventImpl)event).getEventId();
      assertNotNull(eventId);
      synchronized (lockObject) {
        receivedOperation = true;
        lockObject.notify();
      }
    }

    public void afterRegionDestroy(RegionEvent event)
    {
      LogWriterUtils.getLogWriter().info("Before Regionestroy in Server");
      eventId = ((RegionEventImpl)event).getEventId();
      assertNotNull(eventId);
      synchronized (lockObject) {
        receivedOperation = true;
        lockObject.notify();
      }
      LogWriterUtils.getLogWriter().info("After RegionDestroy in Server");
    }

    public void afterRegionClear(RegionEvent event)
    {
      eventId = ((RegionEventImpl)event).getEventId();
      assertNotNull(eventId);
      synchronized (lockObject) {
        receivedOperation = true;
        lockObject.notify();
      }
    }
  }

}
