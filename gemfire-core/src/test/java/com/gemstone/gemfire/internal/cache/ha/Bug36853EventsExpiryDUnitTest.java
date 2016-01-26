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

import java.util.Properties;

import junit.framework.Assert;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.tier.sockets.ConflationDUnitTest;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This is a bug test for 36853 (Expiry logic in HA is used to expire early data
 * that a secondary picks up that is not in the primary. But it is also possible
 * that it would cause data that is in the primary queue to be expired. And this
 * can cause a data loss. This issue is mostly related to Expiry mechanism and
 * not HA, but it affects HA functionality).
 * 
 * This test has a cache-client connected to one cache-server. The expiry-time
 * of events in the queue for the client at the server is set low and dispatcher
 * is set for delayed start. This will make some of the events in the queue
 * expire before dispatcher can start picking them up for delivery to the
 * client.
 * 
 * @author Dinesh Patel
 * 
 */
public class Bug36853EventsExpiryDUnitTest extends CacheTestCase
{

  /** Cache-server */
  VM server = null;

  /** Client , connected to Cache-server */
  VM client = null;

  /** Name of the test region */
  private static final String REGION_NAME = "Bug36853EventsExpiryDUnitTest_region";

  /** The cache instance for test cases */
  protected static Cache cache = null;

  /** Boolean to indicate the client to proceed for validation */
  protected static volatile boolean proceedForValidation = false;

  /** Counter to indicate number of puts recieved by client */
  protected static volatile int putsRecievedByClient;

  /** The last key for operations, to notify for proceeding to validation */
  private static final String LAST_KEY = "LAST_KEY";

  /** The time in milliseconds by which the start of dispatcher will be delayed */
  private static final int DISPATCHER_SLOWSTART_TIME = 10000;

  /** Number of puts done for the test */
  private static final int TOTAL_PUTS = 5;

  /**
   * Constructor
   * 
   * @param name
   */
  public Bug36853EventsExpiryDUnitTest(String name) {
    super(name);
  }

  /**
   * Sets up the cache-server and client for the test
   * 
   * @throws Exception -
   *           thrown in any problem occurs in setUp
   */
  public void setUp() throws Exception
  {
    disconnectAllFromDS();
    super.setUp();
    final Host host = Host.getHost(0);
    server = host.getVM(0);
    client = host.getVM(1);
    server.invoke(ConflationDUnitTest.class, "setIsSlowStart");
    int PORT2 = ((Integer)server.invoke(Bug36853EventsExpiryDUnitTest.class,
        "createServerCache")).intValue();

    client.invoke(Bug36853EventsExpiryDUnitTest.class, "createClientCache",
        new Object[] { getServerHostName(host), new Integer(PORT2) });

  }

  /**
   * Creates the cache
   * 
   * @param props -
   *          distributed system props
   * @throws Exception -
   *           thrown in any problem occurs in creating cache
   */
  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  /**
   * Creates cache and starts the bridge-server
   * 
   * @throws thrown
   *           in any problem occurs in creating and starting cache-server
   */
  public static Integer createServerCache() throws Exception
  {
    System.setProperty(HARegionQueue.REGION_ENTRY_EXPIRY_TIME, "1");
    System.setProperty("slowStartTimeForTesting", String
        .valueOf(DISPATCHER_SLOWSTART_TIME));
    new Bug36853EventsExpiryDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
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
   * Creates the client cache
   *
   * @param hostName the name of the server's machine
   * @param port -
   *          bridgeserver port
   * @throws Exception -
   *           thrown if any problem occurs in setting up the client
   */
  public static void createClientCache(String hostName, Integer port)
    throws Exception {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    new Bug36853EventsExpiryDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ClientServerTestCase.configureConnectionPool(factory, hostName, port.intValue(),-1, true, -1, 2, null);

    factory.addCacheListener(new CacheListenerAdapter() {
      public void afterCreate(EntryEvent event)
      {
        String key = (String)event.getKey();
        getLogWriter().info("client2 : afterCreate : key =" + key);
        if (key.equals(LAST_KEY)) {

          synchronized (Bug36853EventsExpiryDUnitTest.class) {
            getLogWriter().info(
                "Notifying client2 to proceed for validation");
            proceedForValidation = true;
            Bug36853EventsExpiryDUnitTest.class.notify();
          }
        }
        else {
          putsRecievedByClient++;
        }
      }
    });
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(REGION_NAME, attrs);

    region.registerInterest("ALL_KEYS");
  }

  /**
   * First generates some events, then waits for the time equal to that of
   * delayed start of the dispatcher and then does put on the last key for few
   * iterations. The idea is to let the events added, before waiting, to expire
   * before the dispatcher to pick them up and then do a put on a LAST_KEY
   * couple of times so that atleast one of these is dispatched to client and
   * when client recieves this in the listener, the test is notified to proceed
   * for validation.
   * 
   * @throws Exception -
   *           thrown if any problem occurs in put operation
   */
  public static void generateEvents() throws Exception
  {
    String regionName = Region.SEPARATOR + REGION_NAME;
    Region region = cache.getRegion(regionName);
    for (int i = 0; i < TOTAL_PUTS; i++) {

      region.put("key" + i, "val-" + i);
    }
    Thread.sleep(DISPATCHER_SLOWSTART_TIME + 1000);
    for (int i = 0; i < 25; i++) {

      region.put(LAST_KEY, "LAST_VALUE");
    }
  }

  /**
   * First generates some events, then waits for the time equal to that of
   * delayed start of the dispatcher and then does put on the last key for few
   * iterations. Whenever the client the create corresponding to the LAST_KEY in
   * the listener, the test is notified to proceed for validation. Then, it is
   * validated that all the events that were added prior to the LAST_KEY are
   * dispatched to the client. Due to the bug#36853, those events will expire
   * and validation will fail.
   * 
   * @throws Exception -
   *           thrown if any exception occurs in test
   */
  public void testEventsExpiryBug() throws Exception
  {
    addExpectedException("Unexpected IOException");
    addExpectedException("Connection reset");
    server.invoke(Bug36853EventsExpiryDUnitTest.class, "generateEvents");
    client.invoke(Bug36853EventsExpiryDUnitTest.class,
        "validateEventCountAtClient");
  }

  /**
   * Waits for the listener to receive all events and validates that no
   * exception occured in client
   */
  public static void validateEventCountAtClient() throws Exception
  {
    if (!proceedForValidation) {
      synchronized (Bug36853EventsExpiryDUnitTest.class) {
        if (!proceedForValidation)
          try {
            getLogWriter().info(
                "Client2 going in wait before starting validation");
            Bug36853EventsExpiryDUnitTest.class.wait(5000);
          }
          catch (InterruptedException e) {
            fail("interrupted");
          }
      }
    }
    getLogWriter().info("Starting validation on client2");
    Assert.assertEquals(
        "Puts recieved by client not equal to the puts done at server.",
        TOTAL_PUTS, putsRecievedByClient);
    getLogWriter()
        .info("putsRecievedByClient = " + putsRecievedByClient);
    getLogWriter().info("Validation complete on client2");

  }

  /**
   * Closes the cache
   * 
   */
  public static void closeCache()
  {    
    System.setProperty(HARegionQueue.REGION_ENTRY_EXPIRY_TIME, "");
    CacheTestCase.closeCache();
  }

  /**
   * Closes the caches on clients and servers
   * 
   * @throws Exception -
   *           thrown if any problem occurs in closing client and server caches.
   */
  public void tearDown2() throws Exception
  {
    // close client
    client.invoke(Bug36853EventsExpiryDUnitTest.class, "closeCache");
    // close server
    server.invoke(Bug36853EventsExpiryDUnitTest.class, "closeCache");

  }

}
