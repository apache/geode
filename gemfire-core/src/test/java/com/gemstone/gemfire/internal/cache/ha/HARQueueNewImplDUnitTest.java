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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.ha;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.ha.HAContainerMap;
import com.gemstone.gemfire.internal.cache.ha.HAContainerRegion;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ConflationDUnitTest;
import com.gemstone.gemfire.internal.cache.tier.sockets.HAEventWrapper;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

import util.TestException;

/**
 * This DUnit contains various tests to ensure new implementation of ha region
 * queues works as expected.
 * 
 * @author ashetkar
 * @since 5.7
 * 
 */
public class HARQueueNewImplDUnitTest extends DistributedTestCase {
  protected static Cache cache = null;

  protected static VM serverVM0 = null;

  private static VM serverVM1 = null;

  protected static VM clientVM1 = null;

  protected static VM clientVM2 = null;

  private static int PORT1;

  private static int PORT2;

  private static final String regionName = "HARQueueNewImplDUnitTest";

  private static final Map map = new HashMap();

  protected static LogWriter logger = null;

  protected static int numOfCreates = 0;

  protected static int numOfUpdates = 0;

  protected static int numOfInvalidates = 0;
  
  private static Object[] deletedValues = null;

  /**
   * @param name
   *          name of the test
   */
  public HARQueueNewImplDUnitTest(String name) {
    super(name);
  }

  /**
   * Sets up the test.
   */
  public void setUp() throws Exception {
    super.setUp();

    final Host host = Host.getHost(0);
    serverVM0 = host.getVM(0);
    serverVM1 = host.getVM(1);
    clientVM1 = host.getVM(2);
    clientVM2 = host.getVM(3);

    PORT1 = ((Integer)serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    PORT2 = ((Integer)serverVM1.invoke(HARQueueNewImplDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_ENTRY })).intValue();
    numOfCreates = 0;
    numOfUpdates = 0;
    numOfInvalidates = 0;
  }

  /**
   * Tears down the test.
   */
  public void tearDown2() throws Exception {
    super.tearDown2();
    closeCache();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "closeCache");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "closeCache");
    
    // Unset the isSlowStartForTesting flag
    serverVM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    serverVM1.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    // then close the servers
    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "closeCache");
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "closeCache");
    disconnectAllFromDS();
  }

  private void createCache(Properties props) throws Exception {
    props.setProperty(DistributionConfig.DELTA_PROPAGATION_PROP_NAME, "false");
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static Integer createServerCache() throws Exception {
    return createServerCache(null);
  }

  public static Integer createServerCache(String ePolicy) throws Exception {
    return createServerCache(ePolicy, new Integer(1));
  }

  public static Integer createServerCache(String ePolicy, Integer cap)
      throws Exception {
    new HARQueueNewImplDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(regionName, attrs);
    logger = cache.getLogger();

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    if (ePolicy != null) {
      File overflowDirectory = new File("bsi_overflow_"+port);
      overflowDirectory.mkdir();
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      File[] dirs1 = new File[] {overflowDirectory};

      server1.getClientSubscriptionConfig().setEvictionPolicy(ePolicy);
      server1.getClientSubscriptionConfig().setCapacity(cap.intValue());
      // specify diskstore for this server
      server1.getClientSubscriptionConfig().setDiskStoreName(dsf.setDiskDirs(dirs1).create("bsi").getName());
    }
    server1.start();
    return new Integer(server1.getPort());
  }

  public static Integer createOneMoreBridgeServer(Boolean notifyBySubscription)
      throws Exception {
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(notifyBySubscription.booleanValue());
    server1.getClientSubscriptionConfig().setEvictionPolicy(
        HARegionQueue.HA_EVICTION_POLICY_MEMORY);
    // let this server to use default diskstore
    server1.start();
    return new Integer(server1.getPort());
  }

  public static void createClientCache(String host, Integer port1, Integer port2,
      String rLevel, Boolean addListener) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new HARQueueNewImplDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    ClientServerTestCase.configureConnectionPool(factory, host, port1
        .intValue(), port2.intValue(), true, Integer.parseInt(rLevel), 2, null,
        1000, 250, false);

    factory.setScope(Scope.LOCAL);

    if (addListener.booleanValue()) {
      factory.addCacheListener(new CacheListenerAdapter() {
        public void afterInvalidate(EntryEvent event) {
          logger.fine("Invalidate Event: <" + event.getKey() + ", "
              + event.getNewValue() + ">");
          numOfInvalidates++;
        }

        public void afterCreate(EntryEvent event) {
          logger.fine("Create Event: <" + event.getKey() + ", "
              + event.getNewValue() + ">");
          numOfCreates++;
        }

        public void afterUpdate(EntryEvent event) {
          logger.fine("Update Event: <" + event.getKey() + ", "
              + event.getNewValue() + ">");
          numOfUpdates++;
        }
      });
    }

    RegionAttributes attrs = factory.create();
    cache.createRegion(regionName, attrs);
    logger = cache.getLogger();
  }

  public static void createClientCache(String host, Integer port1, Integer port2,
      String rLevel) throws Exception {
    createClientCache(host, port1, port2, rLevel, Boolean.FALSE);
  }

  public static void registerInterestListAll() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.registerInterest("ALL_KEYS");
    }
    catch (Exception ex) {
      fail("failed in registerInterestListAll", ex);
    }
  }

  public static void registerInterestList() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.registerInterest("k1");
      r.registerInterest("k3");
      r.registerInterest("k5");
    }
    catch (Exception ex) {
      fail("failed while registering keys", ex);
    }
  }

  public static void putEntries() {
    try {

      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      r.put("k1", "pv1");
      r.put("k2", "pv2");
      r.put("k3", "pv3");
      r.put("k4", "pv4");
      r.put("k5", "pv5");
    }
    catch (Exception ex) {
      fail("failed in putEntries()", ex);
    }
  }

  public static void createEntries() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      r.create("k1", "v1");
      r.create("k2", "v2");
      r.create("k3", "v3");
      r.create("k4", "v4");
      r.create("k5", "v5");
    }
    catch (Exception ex) {
      fail("failed in createEntries()", ex);
    }
  }

  public static void createEntries(Long num) {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      for (long i = 0; i < num.longValue(); i++) {
        r.create("k" + i, "v" + i);
      }
    }
    catch (Exception ex) {
      fail("failed in createEntries(Long)", ex);
    }
  }

  public static void putHeavyEntries(Integer num) {
    try {
      byte[] val = null;
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      for (long i = 0; i < num.intValue(); i++) {
        val = new byte[1024 * 1024 * 5]; // 5 MB
        r.put("k0", val);
      }
    }
    catch (Exception ex) {
      fail("failed in putHeavyEntries(Long)", ex);
    }
  }

  /**
   * This test verifies that the client-messages-region does not store duplicate
   * ClientUpdateMessageImpl instances, during a normal put path as well as the
   * GII path.
   * 
   * @throws Exception
   */
  public void testClientMsgsRegionSize() throws Exception {
    // slow start for dispatcher
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });
    serverVM1.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM2.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");

    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "stopServer");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "createEntries");

    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "startServer");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyRegionSize",
        new Object[] { new Integer(5), new Integer(5), new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyRegionSize",
        new Object[] { new Integer(5), new Integer(5), new Integer(PORT2) });
  }

  /**
   * This test verifies that the ha-region-queues increment the reference count
   * of their respective HAEventWrapper instances in the client-messages-region
   * correctly, during put as well as GII path.
   * 
   * @throws Exception
   */
  public void testRefCountForNormalAndGIIPut() throws Exception {
    // slow start for dispatcher
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });
    serverVM1.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM2.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");

    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "stopServer");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "createEntries");

    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "startServer");
    Thread.sleep(3000); // TODO: Find a better 'n reliable alternative

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "updateMapForVM0");
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "updateMapForVM1");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyQueueData",
        new Object[] { new Integer(5), new Integer(5), new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyQueueData",
        new Object[] { new Integer(5), new Integer(5), new Integer(PORT2) });
  }

  /**
   * This test verifies that the ha-region-queues decrement the reference count
   * of their respective HAEventWrapper instances in the client-messages-region
   * correctly, after the events have been peeked and removed from the queue.
   * 
   * @throws Exception
   */
  public void testRefCountForPeekAndRemove() throws Exception {
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM2.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "createEntries");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyRegionSize",
        new Object[] { new Integer(5), new Integer(5), new Integer(PORT1) });

    serverVM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "waitTillMessagesAreDispatched", new Object[] { new Integer(PORT1),
            new Long(5000) });

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyRegionSize",
        new Object[] { new Integer(5), new Integer(0), new Integer(PORT1) });
  }

  /**
   * This test verifies that the processing of the QRM messages results in
   * decrementing the reference count of corresponding HAEventWrapper instances,
   * correctly.
   * 
   * @throws Exception
   */
  public void testRefCountForQRM() throws Exception {
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM2.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");

    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "stopServer");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "createEntries");

    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "startServer");

    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyRegionSize",
        new Object[] { new Integer(5), new Integer(5), new Integer(PORT2) });

    serverVM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");

    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyRegionSize",
        new Object[] { new Integer(5), new Integer(0), new Integer(PORT2) });
  }

  /**
   * This test verifies that the destruction of a ha-region (caused by
   * proxy/client disconnect), causes the reference count of all HAEventWrapper
   * instances belonging to the ha-region-queue to be decremented by one, and
   * makes it visible to the client-messages-region.
   * 
   * @throws Exception
   */
  public void testRefCountForDestroy() throws Exception {
    // slow start for dispatcher
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });
    serverVM1.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM2.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");
    // 1. stop the second server
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "stopServer");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "createEntries");
    // 3. start the second server.
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "startServer");
    Thread.sleep(3000);

    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "closeCache");

    Thread.sleep(1000);
    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "updateMap1");
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "updateMap1");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyQueueData",
        new Object[] { new Integer(5), new Integer(5), new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyQueueData",
        new Object[] { new Integer(5), new Integer(5), new Integer(PORT2) });

    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "closeCache");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "updateMap2");
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "updateMap2");

    Thread.sleep(1000);
    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyQueueData",
        new Object[] { new Integer(5), new Integer(5), new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyQueueData",
        new Object[] { new Integer(5), new Integer(5), new Integer(PORT2) });
  }

  /**
   * Addresses the bug 39179. If a clientUpdateMessage is dispatched to the
   * client while its GII was under way, then it should not be put into the
   * HARegionQueue of a client at receiving server side.
   * 
   * @throws Exception
   */
  public void testConcurrentGIIAndDispatch() throws Exception {
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "40000" });
    serverVM1.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "40000" });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM2.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestListAll");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestListAll");
    // 1. stop the second server
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "stopServer");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "createEntries");
    serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "makeValuesOfSomeKeysNullInClientMsgsRegion", new Object[] {
            new Integer(PORT1), new String[] { "k1", "k3" } });
    // 3. start the second server.
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "startServer");
    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyRegionSize",
        new Object[] { new Integer(5), new Integer(3), new Integer(PORT1) });

    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyNullValuesInCMR",
        new Object[] { new Integer(3), new Integer(PORT2),
            new String[] { "k1", "k3" } });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyRegionSize",
        new Object[] { new Integer(5), new Integer(3), new Integer(PORT2) });

    serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "populateValuesOfSomeKeysInClientMsgsRegion", new Object[] {
            new Integer(PORT1), new String[] { "k1", "k3" } });

    serverVM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    serverVM1.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
  }

  /*
   * public void PtestRefCountForConcurrentPutAndRemove() throws Exception { }
   * public void PtestRefCountForConcurrentGIIPut() throws Exception { }
   */

  /**
   * This test verifies that when two BridgeServerImpl instances are created in
   * a single VM, they do share the client-messages-region.
   * 
   * @throws Exception
   */
  public void testTwoBridgeServersInOneVMDoShareCMR() throws Exception {
    // slow start for dispatcher
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });

    Integer port3 = (Integer)serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "createOneMoreBridgeServer", new Object[] { Boolean.TRUE });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), port3, "0");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM2.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "createEntries");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyRegionSize",
        new Object[] { new Integer(5), new Integer(5), new Integer(PORT1) });
    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyRegionSize",
        new Object[] { new Integer(5), new Integer(5), port3 });
  }

  /**
   * This test verifies that two clients, connected to two bridge servers with
   * different notifyBySubscription values, on a single VM, receive
   * updates/invalidates depending upon their notifyBySubscription value.
   * 
   * @throws Exception
   */
  public void testUpdatesWithTwoBridgeServersInOneVM() throws Exception {
    Integer port3 = (Integer)serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "createOneMoreBridgeServer", new Object[] { Boolean.FALSE });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1", Boolean.TRUE);
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, port3, new Integer(PORT2), "1", Boolean.TRUE });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestListAll");

    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createEntries");
    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "putEntries");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "waitTillMessagesAreDispatched", new Object[] { new Integer(PORT1),
            new Long(5000) });
    serverVM0
        .invoke(HARQueueNewImplDUnitTest.class,
            "waitTillMessagesAreDispatched", new Object[] { port3,
                new Long(5000) });

    // expect updates
    verifyUpdatesReceived(new Integer(5), Boolean.TRUE, new Long(5000));
    // expect invalidates
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyUpdatesReceived",
        new Object[] { new Integer(5), Boolean.TRUE, new Long(5000) });
  }

  /**
   * This test verifies that the HAEventWrapper instances present in the
   * client-messages-region give up the references to their respective
   * ClientUpdateMessageImpl instances.
   * 
   * @throws Exception
   */
  public void testHAEventWrapperDoesNotHoldCUMOnceInsideCMR() throws Exception {
    // slow start for dispatcher
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM2.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");

    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "stopServer");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "createEntries",
        new Object[] { new Long(1000) });

    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "startServer");
    Thread.sleep(2000);

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyNullCUMReference",
        new Object[] { new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyNullCUMReference",
        new Object[] { new Integer(PORT2) });
  }

  /**
   * This test verifies that client-messages-regions are not created for the
   * bridge servers who have eviction policy as 'none'. Instead, such bridge
   * servers will have simple HashMap structures. Also, it verifies that such a
   * structure (referred to as haContainer, in general) is destroyed when its
   * bridge server is stopped.
   * 
   * @throws Exception
   */
  public void testCMRNotCreatedForNoneEvictionPolicy() throws Exception {
    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "closeCache");
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "closeCache");
    Thread.sleep(2000);
    PORT1 = ((Integer)serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_NONE })).intValue();
    PORT2 = ((Integer)serverVM1.invoke(HARQueueNewImplDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_NONE })).intValue();
    Boolean isRegion = Boolean.FALSE;
    // slow start for dispatcher
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM2.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyHaContainerType",
        new Object[] { isRegion, new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyHaContainerType",
        new Object[] { isRegion, new Integer(PORT2) });

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "stopOneBridgeServer",
        new Object[] { new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "stopOneBridgeServer",
        new Object[] { new Integer(PORT2) });

    serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "verifyHaContainerDestroyed", new Object[] { isRegion,
            new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class,
        "verifyHaContainerDestroyed", new Object[] { isRegion,
            new Integer(PORT2) });
  }

  /**
   * This test verifies that client-messages-regions are created for the bridge
   * servers who have eviction policy either as 'mem' or as 'entry'. Also, it
   * verifies that such a client-messages-region is destroyed when its bridge
   * server is stopped.
   * 
   * @throws Exception
   */
  public void testCMRCreatedForMemOrEntryEvictionPolicy() throws Exception {
    Boolean isRegion = Boolean.TRUE;
    // slow start for dispatcher
    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "30000" });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM1.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "verifyHaContainerType",
        new Object[] { isRegion, new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "verifyHaContainerType",
        new Object[] { isRegion, new Integer(PORT2) });

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "stopOneBridgeServer",
        new Object[] { new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "stopOneBridgeServer",
        new Object[] { new Integer(PORT2) });

    serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "verifyHaContainerDestroyed", new Object[] { isRegion,
            new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class,
        "verifyHaContainerDestroyed", new Object[] { isRegion,
            new Integer(PORT2) });
  }

  /**
   * This test verifies that the Cache.rootRegions() method does not return the
   * client-messages-region of any of the cache's attached bridge servers.
   * 
   * @throws Exception
   */
  public void testCMRNotReturnedByRootRegionsMethod() throws Exception {

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM2.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestList");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "createEntries");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "verifyRootRegionsDoesNotReturnCMR",
        new Object[] { new Integer(PORT1) });
    serverVM1.invoke(HARQueueNewImplDUnitTest.class,
        "verifyRootRegionsDoesNotReturnCMR",
        new Object[] { new Integer(PORT2) });
  }

  /**
   * This test verifies that the memory foorprint of the ha region queues is
   * less when ha-overflow is enabled (with an appropriate value of haCapacity)
   * compared to when it is disabled, for the same amount of data feed.
   * 
   * @throws Exception
   */
  public void _testMemoryFootprintOfHARegionQueuesWithAndWithoutOverflow()
      throws Exception {
    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "closeCache");
    serverVM1.invoke(HARQueueNewImplDUnitTest.class, "closeCache");
    Thread.sleep(2000);
    Integer numOfEntries = new Integer(30);

    PORT1 = ((Integer)serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "createServerCache", new Object[] {
            HARegionQueue.HA_EVICTION_POLICY_MEMORY, new Integer(30) }))
        .intValue();
    PORT2 = ((Integer)serverVM1.invoke(HARQueueNewImplDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_NONE })).intValue();

    serverVM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "60000" });
    serverVM1.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "60000" });

    createClientCache(getServerHostName(Host.getHost(0)), new Integer(PORT1), new Integer(PORT2), "1");
    final String client1Host = getServerHostName(clientVM1.getHost());
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client1Host, new Integer(PORT1), new Integer(PORT2), "1" });
    final String client2Host = getServerHostName(clientVM2.getHost());
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "createClientCache",
        new Object[] { client2Host, new Integer(PORT1), new Integer(PORT2), "1" });

    registerInterestListAll();
    clientVM1.invoke(HARQueueNewImplDUnitTest.class, "registerInterestListAll");
    clientVM2.invoke(HARQueueNewImplDUnitTest.class, "registerInterestListAll");

    serverVM0.invoke(HARQueueNewImplDUnitTest.class, "putHeavyEntries",
        new Object[] { numOfEntries });

    Long usedMemInVM0 = (Long)serverVM0.invoke(HARQueueNewImplDUnitTest.class,
        "getUsedMemoryAndVerifyRegionSize", new Object[] { new Integer(1),
            numOfEntries, new Integer(PORT1) });
    Long usedMemInVM1 = (Long)serverVM1.invoke(HARQueueNewImplDUnitTest.class,
        "getUsedMemoryAndVerifyRegionSize", new Object[] { new Integer(1),
            numOfEntries, new Integer(-1) });

    serverVM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    serverVM1.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");

    logger
        .fine("Used Mem: " + usedMemInVM1.longValue() + "(without overflow), "
            + usedMemInVM0.longValue() + "(with overflow)");

    assertTrue(usedMemInVM0.longValue() < usedMemInVM1.longValue());
  }

  private static void verifyNullCUMReference(Integer port) {
    Region r = cache.getRegion("/"
        + CacheServerImpl.generateNameForClientMsgsRegion(port.intValue()));
    assertNotNull(r);

    Object[] arr = r.keySet().toArray();
    for (int i = 0; i < arr.length; i++) {
      assertNull(((HAEventWrapper)arr[i]).getClientUpdateMessage());
    }

  }

  private static void verifyHaContainerDestroyed(Boolean isRegion, Integer port) {
    Map r = cache.getRegion("/"
        + CacheServerImpl.generateNameForClientMsgsRegion(port.intValue()));

    if (isRegion.booleanValue()) {
      if (r != null) {
        assertTrue(((Region)r).isDestroyed());
      }
    }
    else {
      r = ((CacheServerImpl)cache.getCacheServers().toArray()[0])
          .getAcceptor().getCacheClientNotifier().getHaContainer();
      if (r != null) {
        assertTrue(r.isEmpty());
      }
    }
  }

  static Long getUsedMemoryAndVerifyRegionSize(Integer rSize,
      Integer haContainerSize, Integer port) {
    Long retVal = null;
    try {
      retVal = new Long(Runtime.getRuntime().totalMemory()
          - Runtime.getRuntime().freeMemory());
      if (port.intValue() != -1) {
        verifyRegionSize(rSize, haContainerSize, port);
      }
      else {
        verifyRegionSize(rSize, haContainerSize);
      }
    }
    catch (Exception e) {
      fail("failed in getUsedMemory()" + e);
    }
    return retVal;
  }

  private static void setHACapacity(Integer cap) {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        server.getClientSubscriptionConfig().setCapacity(cap.intValue());
      }
    }
    catch (Exception e) {
      fail("failed in setHACapacity()" + e);
    }
  }

  private static void stopOneBridgeServer(Integer port) {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        if (server.getPort() == port.intValue()) {
          server.stop();
        }
      }
    }
    catch (Exception e) {
      fail("failed in stopOneBridgeServer()" + e);
    }
  }

  public static void stopServer() {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        server.stop();
      }
    }
    catch (Exception e) {
      fail("failed in stopServer()" + e);
    }
  }

  public static void updateMapForVM0() {
    try {
      map.put("k1", new Long(3));
      map.put("k2", new Long(1));
      map.put("k3", new Long(3));
      map.put("k4", new Long(1));
      map.put("k5", new Long(3));
    }
    catch (Exception e) {
      fail("failed in updateMapForVM0()" + e);
    }
  }

  public static void updateMap1() {
    try {
      map.put("k1", new Long(2));
      map.put("k2", new Long(1));
      map.put("k3", new Long(2));
      map.put("k4", new Long(1));
      map.put("k5", new Long(2));
    }
    catch (Exception e) {
      fail("failed in updateMap1()" + e);
    }
  }

  public static void updateMap2() {
    try {
      map.put("k1", new Long(1));
      map.put("k2", new Long(1));
      map.put("k3", new Long(1));
      map.put("k4", new Long(1));
      map.put("k5", new Long(1));
    }
    catch (Exception e) {
      fail("failed in updateMap2()" + e);
    }
  }

  public static void updateMapForVM1() {
    try {
      updateMapForVM0();
    }
    catch (Exception e) {
      fail("failed in updateMapForVM1()" + e);
    }
  }

  public static void printMsg(String msg) {
    try {
      logger.fine(msg);
    }
    catch (Exception e) {
      fail("failed in printMsg()" + e);
    }
  }

  public static void haQueuePut() {
    Set set = HARegionQueue.getDispatchedMessagesMapForTesting().keySet();
    Iterator iter = set.iterator();
    logger.fine("# of HAQueues: " + set.size());
    while (iter.hasNext()) {
//      HARegion haRegion = (HARegion)
      cache.getRegion(Region.SEPARATOR
          + (String)iter.next());
      // haRegion.getOwner().put();
    }
  }

  public static void verifyNullValuesInCMR(final Integer numOfEntries, 
      final Integer port,
      String[] keys) {
    final Region msgsRegion = cache.getRegion(CacheServerImpl
        .generateNameForClientMsgsRegion(port.intValue()));
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        int sz = msgsRegion.size();
        return sz == numOfEntries.intValue();
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 60 * 1000, 1000, true);

    Set entries = msgsRegion.entrySet();
    Iterator iter = entries.iterator();
    for (; iter.hasNext();) {
      Region.Entry entry = (Region.Entry)iter.next();
      ClientUpdateMessage cum = (ClientUpdateMessage)entry.getValue();
      for (int i = 0; i < keys.length; i++) {
        logger.fine("cum.key: " + cum.getKeyToConflate());
        // assert that the keys are not present in entries set
        assertTrue(!keys[i].equals(cum.getKeyToConflate()));
      }
    }
  }

  public static void makeValuesOfSomeKeysNullInClientMsgsRegion(Integer port,
      String[] keys) {
    Region msgsRegion = cache.getRegion(CacheServerImpl
        .generateNameForClientMsgsRegion(port.intValue()));
    assertNotNull(msgsRegion);

    Set entries = msgsRegion.entrySet();
    Iterator iter = entries.iterator();
    deletedValues = new Object[keys.length];
    while (iter.hasNext()) {
      Region.Entry entry = (Region.Entry)iter.next();
      ClientUpdateMessage cum = (ClientUpdateMessage)entry.getValue();
      for (int i = 0; i < keys.length; i++) {
        if (keys[i].equals(cum.getKeyToConflate())) {
          logger.fine("HARQueueNewImplDUnit: Removing "
              + cum.getKeyOfInterest());
          deletedValues[i] = msgsRegion.remove(entry.getKey());
        }
      }
    }
  }

  public static void populateValuesOfSomeKeysInClientMsgsRegion(Integer port,
      String[] keys) {
    Region msgsRegion = cache.getRegion(CacheServerImpl
        .generateNameForClientMsgsRegion(port.intValue()));
    assertNotNull(msgsRegion);

    for (int i = 0; i < keys.length; i++) {
      logger.fine("HARQueueNewImplDUnit: populating "
          + deletedValues[i]);
      msgsRegion.put(keys[1], deletedValues[i]);
    }
  }

  public static void startServer() {
    try {
      Iterator iter = cache.getCacheServers().iterator();
      if (iter.hasNext()) {
        CacheServer server = (CacheServer)iter.next();
        server.start();
      }
    }
    catch (Exception e) {
      fail("failed in startServer()" + e);
    }
  }

  public static void verifyQueueData(Integer regionsize,
      Integer msgsRegionsize, Integer port) {
    try {
      // Get the clientMessagesRegion and check the size.
      Region msgsRegion = cache.getRegion(CacheServerImpl
          .generateNameForClientMsgsRegion(port.intValue()));
      Region region = cache.getRegion("/" + regionName);
      logger.fine("size<serverRegion, clientMsgsRegion>: " + region.size()
          + ", " + msgsRegion.size());
      assertEquals(regionsize.intValue(), region.size());
      assertEquals(msgsRegionsize.intValue(), msgsRegion.size());

      Iterator iter = msgsRegion.entrySet().iterator();
      while (iter.hasNext()) {
        Region.Entry entry = (Region.Entry)iter.next();
        HAEventWrapper wrapper = (HAEventWrapper)entry.getKey();
        ClientUpdateMessage cum = (ClientUpdateMessage)entry.getValue();
        Object key = cum.getKeyOfInterest();
        logger.fine("key<feedCount, regionCount>: " + key + "<"
            + ((Long)map.get(key)).longValue() + ", "
            + wrapper.getReferenceCount() + ">");
        assertEquals(((Long)map.get(key)).longValue(), wrapper.getReferenceCount());
      }
    }
    catch (Exception e) {
      fail("failed in verifyQueueData()" + e);
    }
  }

  public static void verifyRegionSize(final Integer regionSize,
      final Integer msgsRegionsize, final Integer port) {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        try {
          // Get the clientMessagesRegion and check the size.
          Region region = cache.getRegion("/" + regionName);
//          logger.fine("size<serverRegion, clientMsgsRegion>: " + region.size()
//              + ", " + msgsRegion.size());
          int sz = region.size();
          if (regionSize.intValue() != sz) {
            excuse = "expected regionSize = " + regionSize + ", actual = " + sz;
            return false;
          }
          
          Iterator iter = cache.getCacheServers().iterator();
          if (iter.hasNext()) {
            CacheServerImpl server = (CacheServerImpl)iter.next();
            Map msgsRegion = server.getAcceptor().getCacheClientNotifier()
            .getHaContainer();
            //Region msgsRegion = cache.getRegion(BridgeServerImpl
            //  .generateNameForClientMsgsRegion(port.intValue()));

            sz = msgsRegion.size();
            if (msgsRegionsize.intValue() != sz) {
              excuse = "expected msgsRegionsize = " + msgsRegionsize + ", actual = " + sz;
              return false;
            }
          }
          return true;
        }
        catch (Exception e) {
          excuse = "Caught exception " + e;
          return false;
        }
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 120 * 1000, 1000, true);
  }

  public static void verifyRegionSize(final Integer regionSize, 
      final Integer msgsRegionsize) {
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        try {
          // Get the clientMessagesRegion and check the size.
          Region region = cache.getRegion("/" + regionName);
          int sz = region.size();
          if (regionSize.intValue() != sz) {
            excuse = "Expected regionSize = " + regionSize.intValue() + 
                ", actual = " + sz;
            return false;
          }
          Iterator iter = cache.getCacheServers().iterator();
          if (!iter.hasNext()) {
            return true;
          }
          CacheServerImpl server = (CacheServerImpl)iter.next();
          sz = server.getAcceptor().getCacheClientNotifier().getHaContainer().size();
          if (sz != msgsRegionsize.intValue()) {
            excuse = "Expected msgsRegionsize = " + msgsRegionsize.intValue() +
                ", actual = " + sz;
            return false;
          }
          return true;
        } catch (Exception e) {
          excuse = "failed due to " + e;
          return false;
        }
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 120 * 1000, 1000, true);
  }

  public static void verifyHaContainerType(Boolean isRegion, Integer port) {
    try {
      Map haMap = cache.getRegion(CacheServerImpl
          .generateNameForClientMsgsRegion(port.intValue()));
      if (isRegion.booleanValue()) {
        assertNotNull(haMap);
        assertTrue(haMap instanceof LocalRegion);
        haMap = ((CacheServerImpl)cache.getCacheServers().toArray()[0])
            .getAcceptor().getCacheClientNotifier().getHaContainer();
        assertNotNull(haMap);
        assertTrue(haMap instanceof HAContainerRegion);
      }
      else {
        assertNull(haMap);
        haMap = ((CacheServerImpl)cache.getCacheServers().toArray()[0])
            .getAcceptor().getCacheClientNotifier().getHaContainer();
        assertNotNull(haMap);
        assertTrue(haMap instanceof HAContainerMap);
      }
      logger.fine("haContainer: " + haMap);
    }
    catch (Exception e) {
      fail("failed in verifyHaContainerType()" + e);
    }
  }

  public static void verifyRootRegionsDoesNotReturnCMR(Integer port) {
    try {
      String cmrName = CacheServerImpl.generateNameForClientMsgsRegion(port
          .intValue());
      Map haMap = cache.getRegion(cmrName);
      assertNotNull(haMap);
      String rName = "";
      Iterator iter = cache.rootRegions().iterator();

      while (iter.hasNext()) {
        rName = ((Region)iter.next()).getName();
        if (cmrName.equals(rName)) {
          throw new TestException(
              "Cache.rootRegions() method should not return the client_messages_region.");
        }
        logger.fine("Region name returned from cache.rootRegions(): " + rName);
      }
    }
    catch (Exception e) {
      fail("failed in verifyRootRegionsDoesNotReturnCMR()" + e);
    }
  }

  public static void verifyUpdatesReceived(final Integer num, Boolean isUpdate,
      Long waitLimit) {
    try {
      if (isUpdate.booleanValue()) {
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return num.intValue() == numOfUpdates;
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, waitLimit.longValue(), 200, true);
      }
      else {
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return num.intValue() == numOfInvalidates; 
          }
          public String description() {
            return null;
          }
        };
        DistributedTestCase.waitForCriterion(ev, waitLimit.longValue(), 200, true);
      }
    }
    catch (Exception e) {
      fail("failed in verifyUpdatesReceived()" + e);
    }
  }

  public static void waitTillMessagesAreDispatched(Integer port, Long waitLimit) {
    try {
      Map haContainer = null;
      haContainer = cache.getRegion(Region.SEPARATOR
          + CacheServerImpl.generateNameForClientMsgsRegion(port.intValue()));
      if (haContainer == null) {
        Object[] servers = cache.getCacheServers().toArray();
        for (int i = 0; i < servers.length; i++) {
          if (port.intValue() == ((CacheServerImpl)servers[i]).getPort()) {
            haContainer = ((CacheServerImpl)servers[i]).getAcceptor()
                .getCacheClientNotifier().getHaContainer();
            break;
          }
        }
      }
      final Map m = haContainer;
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return m.size() == 0;
        }
        public String description() {
          return null;
        }
      };
      DistributedTestCase.waitForCriterion(ev, waitLimit.longValue(), 200, true);
    }
    catch (Exception e) {
      fail("failed in waitTillMessagesAreDispatched()" + e);
    }
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

}
