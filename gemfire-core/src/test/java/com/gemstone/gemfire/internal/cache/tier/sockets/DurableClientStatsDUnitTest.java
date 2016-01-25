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
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * 
 * @author Deepkumar Varma
 * 
 * The DUnitTest checks whether the following Three counts are incremented
 * correctly or not:
 * 1) DurableReconnectionCount -> Incremented Each time 
 *      a Proxy present in server for a DurableClient
 * 2) QueueDroppedCount -> Incremented Each time 
 *      a queue for a durable client is dropped after durable Timeout
 * 3) EventsEnqueuedWhileClientAwayCount -> Incremented Each time
 *      an entry is made when client is away.
 *      
 * In the given test DurableClient comes up and goes down discreetly with
 * different DurableClientTimeouts so as to increment the counts
 */
public class DurableClientStatsDUnitTest extends DistributedTestCase {

  private VM server1VM;

  private VM durableClientVM;

  private String regionName;

  private int PORT1;

  private final String K1 = "Key1";

  public DurableClientStatsDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    this.server1VM = host.getVM(0);
    this.durableClientVM = host.getVM(1);
    regionName = DurableClientStatsDUnitTest.class.getName() + "_region";
    CacheServerTestUtil.disableShufflingOfEndpoints();
  }

  @Override
  public void tearDown2() throws Exception {
    // Stop server 1
    this.server1VM.invoke(CacheServerTestUtil.class, "closeCache");
    CacheServerTestUtil.resetDisableShufflingOfEndpointsFlag();
  }
  
  public void testNonDurableClientStatistics() {


    // Step 1: Starting the servers
    PORT1 = ((Integer)this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] { regionName, new Boolean(true)
             })).intValue();
    this.server1VM.invoke(DurableClientStatsDUnitTest.class, "checkStatistics");
    // Step 2: Bring Up the Client
    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final int durableClientTimeout = 600; // keep the client alive for 600
    // seconds

    startAndCloseNonDurableClientCache(durableClientTimeout);
    startAndCloseNonDurableClientCache(1);      //////// -> Reconnection1
    pause(1400);        //////// -> Queue Dropped1
    startAndCloseNonDurableClientCache(1);
    pause(1400);        //////// -> Queue Dropped2
    
    startRegisterAndCloseNonDurableClientCache( durableClientTimeout);
    pause(500);

    this.server1VM.invoke(DurableClientStatsDUnitTest.class, "putValue",
        new Object[] { K1, "Value1" });         //////// -> Enqueue Message1

    pause(500);
    startAndCloseNonDurableClientCache(1);      //////// -> Reconnection2
    pause(1400);        //////// -> Queue Dropped3
    startAndCloseNonDurableClientCache(1);
    pause(1400);        //////// -> Queue Dropped4
    startRegisterAndCloseNonDurableClientCache( durableClientTimeout);
    pause(500);

    this.server1VM.invoke(DurableClientStatsDUnitTest.class, "putValue",
        new Object[] { K1, "NewValue1" });      //////// -> Enqueue Message2

    startAndCloseNonDurableClientCache(durableClientTimeout);   //////// -> Reconnection3

    this.server1VM.invoke(DurableClientStatsDUnitTest.class,
        "checkStatisticsWithExpectedValues", new Object[] { new Integer(0),
            new Integer(0), new Integer(0) });
  
    
  }
  public void testDurableClientStatistics() {

    // Step 1: Starting the servers
    PORT1 = ((Integer)this.server1VM.invoke(CacheServerTestUtil.class,
        "createCacheServer", new Object[] { regionName, new Boolean(true)
      })).intValue();
    this.server1VM.invoke(DurableClientStatsDUnitTest.class, "checkStatistics");
    // Step 2: Bring Up the Client
    // Start a durable client that is not kept alive on the server when it
    // stops normally
    final int durableClientTimeout = 600; // keep the client alive for 600
    // seconds

    startAndCloseDurableClientCache(durableClientTimeout);
    startAndCloseDurableClientCache(1);      //////// -> Reconnection1
    pause(1400);        //////// -> Queue Dropped1
    startAndCloseDurableClientCache(1);
    pause(1400);        //////// -> Queue Dropped2
    
    startRegisterAndCloseDurableClientCache( durableClientTimeout);
    pause(500);

    this.server1VM.invoke(DurableClientStatsDUnitTest.class, "putValue",
        new Object[] { K1, "Value1" });         //////// -> Enqueue Message1

    pause(500);
    startAndCloseDurableClientCache(1);      //////// -> Reconnection2
    pause(1400);        //////// -> Queue Dropped3
    startAndCloseDurableClientCache(1);
    pause(1400);        //////// -> Queue Dropped4
    startRegisterAndCloseDurableClientCache( durableClientTimeout);
    pause(500);

    this.server1VM.invoke(DurableClientStatsDUnitTest.class, "putValue",
        new Object[] { K1, "NewValue1" });      //////// -> Enqueue Message2

    startAndCloseDurableClientCache(durableClientTimeout);   //////// -> Reconnection3

    this.server1VM.invoke(DurableClientStatsDUnitTest.class,
        "checkStatisticsWithExpectedValues", new Object[] { new Integer(3),
            new Integer(4), new Integer(2) });
  }
  
  
  
  public void startRegisterAndCloseDurableClientCache(int durableClientTimeout) {
    final String durableClientId = getName() + "_client";

    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(durableClientVM.getHost()), PORT1, true, 0),
            regionName,
            getDurableClientDistributedSystemProperties(durableClientId,
                durableClientTimeout), Boolean.TRUE });

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable(
        "Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    this.durableClientVM.invoke(DurableClientStatsDUnitTest.class,
        "registerKey", new Object[] { K1, new Boolean(true) });

    this.durableClientVM.invoke(DurableClientStatsDUnitTest.class, "closeCache");
  }

  public void startRegisterAndCloseNonDurableClientCache(int durableClientTimeout) {
    final String durableClientId = getName() + "_client";

    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(durableClientVM.getHost()), PORT1, true, 0),
            regionName,
            getNonDurableClientDistributedSystemProperties(durableClientId,
                durableClientTimeout), Boolean.TRUE });

    // Send clientReady message
//    this.durableClientVM.invoke(new CacheSerializableRunnable(
//        "Send clientReady") {
//      public void run2() throws CacheException {
//        CacheServerTestUtil.getCache().readyForEvents();
//      }
//    });

    this.durableClientVM.invoke(DurableClientStatsDUnitTest.class,
        "registerKey", new Object[] { K1, new Boolean(false) });

    this.durableClientVM.invoke(DurableClientStatsDUnitTest.class, "closeCache");
  }
  
  public void startAndCloseDurableClientCache(int durableClientTimeout) {

    final String durableClientId = getName() + "_client";

    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(durableClientVM.getHost()), PORT1, true, 0),
            regionName,
            getDurableClientDistributedSystemProperties(durableClientId,
                durableClientTimeout), Boolean.TRUE });

    // Send clientReady message
    this.durableClientVM.invoke(new CacheSerializableRunnable(
        "Send clientReady") {
      @Override
      public void run2() throws CacheException {
        CacheServerTestUtil.getCache().readyForEvents();
      }
    });

    this.durableClientVM.invoke(DurableClientStatsDUnitTest.class, "closeCache");

  }

  public void startAndCloseNonDurableClientCache(int durableClientTimeout) {

    final String durableClientId = getName() + "_client";

    this.durableClientVM.invoke(CacheServerTestUtil.class, "createCacheClient",
        new Object[] {
            getClientPool(getServerHostName(durableClientVM.getHost()), PORT1, true, 0),
            regionName,
            getNonDurableClientDistributedSystemProperties(durableClientId,
                durableClientTimeout), Boolean.TRUE });

    // Send clientReady message
//    this.durableClientVM.invoke(new CacheSerializableRunnable(
//        "Send clientReady") {
//      public void run2() throws CacheException {
//        CacheServerTestUtil.getCache().readyForEvents();
//      }
//    });

    this.durableClientVM.invoke(DurableClientStatsDUnitTest.class, "closeCache");

  }
  
  public static void checkStatistics() {
    try {
      Cache cache = CacheServerTestUtil.getCache();
      com.gemstone.gemfire.LogWriter logger = cache.getLogger();
      CacheServerImpl currentServer = (CacheServerImpl)(new ArrayList(cache
          .getCacheServers()).get(0));
      AcceptorImpl ai = currentServer.getAcceptor();
      CacheClientNotifier notifier = ai.getCacheClientNotifier();
      CacheClientNotifierStats stats = notifier.getStats();
      logger.info("Stats:" + "\nDurableReconnectionCount:"
          + stats.get_durableReconnectionCount() + "\nQueueDroppedCount"
          + stats.get_queueDroppedCount()
          + "\nEventsEnqueuedWhileClientAwayCount"
          + stats.get_eventEnqueuedWhileClientAwayCount());
    }
    catch (Exception e) {
      fail("Exception thrown while executing checkStatistics()");
    }
  }

  public static void checkStatisticsWithExpectedValues(int reconnectionCount,
      int queueDropCount, int enqueueCount) {
    try {
      Cache cache = CacheServerTestUtil.getCache();
      com.gemstone.gemfire.LogWriter logger = cache.getLogger();
      CacheServerImpl currentServer = (CacheServerImpl)(new ArrayList(cache
          .getCacheServers()).get(0));
      AcceptorImpl ai = currentServer.getAcceptor();
      CacheClientNotifier notifier = ai.getCacheClientNotifier();
      CacheClientNotifierStats stats = notifier.getStats();
      logger.info("Stats:" + "\nDurableReconnectionCount:"
          + stats.get_durableReconnectionCount() + "\nQueueDroppedCount"
          + stats.get_queueDroppedCount()
          + "\nEventsEnqueuedWhileClientAwayCount"
          + stats.get_eventEnqueuedWhileClientAwayCount());
      assertEquals(reconnectionCount, stats.get_durableReconnectionCount());
      assertEquals(queueDropCount, stats.get_queueDroppedCount());
      assertEquals(enqueueCount, stats.get_eventEnqueuedWhileClientAwayCount());
    }
    catch (Exception e) {
      fail("Exception thrown while executing checkStatisticsWithExpectedValues()");
    }
  }

  public static void closeCache() {
    Cache cache = CacheServerTestUtil.getCache();
    if (cache != null && !cache.isClosed()) {
      // might fail in DataSerializerRecoveryListener.RecoveryTask in shutdown
      cache.getLogger().info("<ExpectedException action=add>" +
          RejectedExecutionException.class.getName() + "</ExpectedException>");
      cache.close(true);
      cache.getDistributedSystem().disconnect();
    }
  }

  private static void registerKey(String key, boolean isDurable)
      throws Exception {
    try {
      // Get the region
      Region region = CacheServerTestUtil.getCache().getRegion(
          DurableClientStatsDUnitTest.class.getName() + "_region");
      // Region region =
      // CacheServerTestUtil.getCache().getRegion(regionName);
      assertNotNull(region);
      region.registerInterest(key, InterestResultPolicy.NONE, isDurable);
    }
    catch (Exception ex) {
      fail("failed while registering interest in registerKey function", ex);
    }
  }

  private static void putValue(String key, String value) {
    try {
      Region r = CacheServerTestUtil.getCache().getRegion(
          DurableClientStatsDUnitTest.class.getName() + "_region");
      // Region r = CacheServerTestUtil.getCache().getRegion(regionName);
      assertNotNull(r);
      if (r.getEntry(key) != null) {
        r.put(key, value);
      }
      else {
        r.create(key, value);
      }
      assertEquals(value, r.getEntry(key).getValue());
    }
    catch (Exception e) {

      fail("Put in Server has some fight");

    }
  }

  private Pool getClientPool(String host, int server1Port,
      boolean establishCallbackConnection, int redundancyLevel) {
    PoolFactory pf = PoolManager.createFactory();
    pf.addServer(host, server1Port)
      .setSubscriptionEnabled(establishCallbackConnection)
      .setSubscriptionRedundancy(redundancyLevel);
    return ((PoolFactoryImpl)pf).getPoolAttributes();
  }

  private Properties getDurableClientDistributedSystemProperties(
      String durableClientId, int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.setProperty(DistributionConfig.LOCATORS_NAME, "");
    properties.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME,
        durableClientId);
    properties.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME,
        String.valueOf(durableClientTimeout));
    return properties;
  }
  
  private Properties getNonDurableClientDistributedSystemProperties(
      String durableClientId, int durableClientTimeout) {
    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.setProperty(DistributionConfig.LOCATORS_NAME, "");
    return properties;
  }
}
