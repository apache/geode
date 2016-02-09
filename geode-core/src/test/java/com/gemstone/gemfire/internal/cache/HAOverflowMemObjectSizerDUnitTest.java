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
package com.gemstone.gemfire.internal.cache;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.lru.EnableLRU;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ConflationDUnitTest;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests the size of clientUpdateMessageImpl with the size calculated by
 * {@link com.gemstone.gemfire.internal.cache.lru.MemLRUCapacityController} for HA overFlow
 * @author aingle
 * @since 5.7
 */
public class HAOverflowMemObjectSizerDUnitTest extends DistributedTestCase {

  /* entry over head used by memCapacityController */
  private static final int OVERHEAD_PER_ENTRY = 250;

  protected static InternalLocator locator;

  /** The cache instance */
  static Cache cache;
  /** The distributedSystem instance */
  static DistributedSystem ds = null;

  static String regionName = "HAOverflowMemObjectSizerDUnitTest-region";

  /* handler for LRU capacity controller */
  private static EnableLRU cc = null;

  static VM client = null;

  static VM serverVM = null;

  static Integer serverPort1 = null;

  static Integer serverPort2 = null;

  static String ePolicy = "mem";

  static int capacity = 1;

  /* store the reference of Client Messages Region */
  static Region region = null;

  /* Create new instance of HAOverflowMemObjectSizerTest */
  public HAOverflowMemObjectSizerDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    disconnectAllFromDS();
    super.setUp();
    Host host = Host.getHost(0);
    client = host.getVM(1);
    serverVM = host.getVM(3);
  }

  @Override
  protected final void preTearDown() throws Exception {
    serverVM.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    client.invoke(HAOverflowMemObjectSizerDUnitTest.class, "closeCache");
    serverVM.invoke(HAOverflowMemObjectSizerDUnitTest.class, "closeCache");
  }

  public static void cleanUp(Long limit) {
    ConflationDUnitTest.unsetIsSlowStart();
    if (region != null) {
      Set entries = region.entrySet();
      entries = region.entrySet();
      long timeElapsed = 0, startTime = System.currentTimeMillis();      
      while (entries.size() > 0 && timeElapsed <= limit.longValue()) {
        // doing it to clean up the queue
        // making sure that dispacher will dispached all events
        try {
          // sleep in small chunks
          Thread.sleep(50);
          timeElapsed = System.currentTimeMillis() - startTime;
        }
        catch (InterruptedException e) {
          fail("interrupted");
        }
        entries = region.entrySet();
      }
    }
  }

  /**
   * Creates cache and starts the bridge-server
   *
   *  @param notification property of BridgeServer
   */
  public static Integer createCacheServer(Boolean notification) throws Exception {
    new HAOverflowMemObjectSizerDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.NORMAL);
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(regionName, attrs);
    assertNotNull(region);
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    server1.setNotifyBySubscription(notification.booleanValue());
    server1.getClientSubscriptionConfig().setCapacity(capacity);
    server1.getClientSubscriptionConfig().setEvictionPolicy(ePolicy);
    server1.start();
    assertTrue(server1.isRunning());
    /*
     * storing capacity controller reference
     */
    cc = ((VMLRURegionMap)((LocalRegion)cache.getRegion(Region.SEPARATOR
        + CacheServerImpl.generateNameForClientMsgsRegion(port))).entries)
        ._getCCHelper();
    return new Integer(server1.getPort());
  }

  /**
   * create client cache
   */
  public static void createCacheClient(Integer port1, String host)
      throws Exception {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    new HAOverflowMemObjectSizerDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.NORMAL);
    ClientServerTestCase.configureConnectionPool(factory, host, port1.intValue(), -1, true, -1, 2, null, -1, -1, false);
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(regionName, attrs);
    assertNotNull(region);
    region.registerInterest("ALL_KEYS");
  }

  /**
   * This test does the following :<br>
   * Configuration: notification by subscription is <b>true </b><br>
   * 1)Verify size calculated by getSizeInByte() of ClientUpdateMessagesImpl is
   * equal to the size calculated by memCapacity controller <br>
   */
  public void testSizerImplementationofMemCapacityControllerWhenNotificationBySubscriptionIsTrue() {

    Integer port1 = (Integer)serverVM.invoke(
        HAOverflowMemObjectSizerDUnitTest.class, "createCacheServer",
        new Object[] { new Boolean(true) });
    serverPort1 = port1;
    serverVM.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "15000" });

    client.invoke(HAOverflowMemObjectSizerDUnitTest.class,
                  "createCacheClient",
                  new Object[] { port1, 
                  NetworkUtils.getServerHostName(client.getHost()) });

    serverVM.invoke(HAOverflowMemObjectSizerDUnitTest.class, "performPut",
        new Object[] { new Long(0L), new Long(100L) });
    serverVM.invoke(HAOverflowMemObjectSizerDUnitTest.class,
        "sizerTestForMemCapacityController", new Object[] { serverPort1 });
  }

  /**
   * This test does the following :<br>
   * Configuration: notification by subscription is<b> false </b><br>
   * 1)Verify size calculated by getSizeInByte() of ClientUpdateMessagesImpl is
   * equal to the size calculated by memCapacity controller <br>
   */
  public void testSizerImplementationofMemCapacityControllerWhenNotificationBySubscriptionIsFalse() {
    Integer port2 = (Integer)serverVM.invoke(
        HAOverflowMemObjectSizerDUnitTest.class, "createCacheServer",
        new Object[] { new Boolean(false) });
    serverPort2 = port2;

    serverVM.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "15000" });

    client.invoke(HAOverflowMemObjectSizerDUnitTest.class,
                  "createCacheClient", 
                  new Object[] { port2,
                  NetworkUtils.getServerHostName(client.getHost()) });

    serverVM.invoke(HAOverflowMemObjectSizerDUnitTest.class, "performPut",
        new Object[] { new Long(101L), new Long(200L) });
    serverVM.invoke(HAOverflowMemObjectSizerDUnitTest.class,
        "sizerTestForMemCapacityController", new Object[] { serverPort2 });
  }

  /**
   * Check for size return by ClientUpdateMessagesImpl getSizeInByte()
   * with size return by memCapacity controller
   *
   * @param port - BridgeServer port required to get ClientMessagesRegion
   */
  public static void sizerTestForMemCapacityController(Integer port) {
    region = cache.getRegion(Region.SEPARATOR
        + CacheServerImpl.generateNameForClientMsgsRegion(port.intValue()));
    assertNotNull(region);
    Set entries = region.entrySet();
    assertTrue(entries.size() > 0);
    Iterator iter = entries.iterator();
    for (; iter.hasNext();) {
      Region.Entry entry = (Region.Entry)iter.next();
      ClientUpdateMessageImpl cum = (ClientUpdateMessageImpl)entry.getValue();
      // passed null to get the size of value ie CUM only ,
      // but this function also add overhead per entry
      // so to get exact size calculated by memCapacityController
      // we need substract this over head
      // as this default value is private static in MemLRUCapacityController
      // cannot access directly
      assertTrue("cum size is not equal",
          (cc.entrySize(null, entry.getValue()) - OVERHEAD_PER_ENTRY) == cum
              .getSizeInBytes());
    }
    cache.getLogger().fine("Test passed. Now, doing a cleanup job.");
    // added here as sleep should be on server where CMR is present and
    // dispacher supposed to run
    cleanUp(new Long(20000));
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

  /* close cache */
  public static void closeCache() {
    try {
      if (cache != null && !cache.isClosed()) {
        cache.close();
        cache.getDistributedSystem().disconnect();
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
  }
/**
 * perform put on server region that will put entries on CMR region
 * @param lowerLimit
 * @param higerlimit - lower and upper limit on put
 */
  public static void performPut(Long lowerLimit, Long higerlimit) {
    assertNotNull(lowerLimit);
    assertNotNull(higerlimit);
    LocalRegion region = (LocalRegion)cache.getRegion(Region.SEPARATOR
        + regionName);
    assertNotNull(region);
    for (long i = lowerLimit.longValue(); i < higerlimit.longValue(); i++) {
      region.put(new Long(i), new Long(i));
    }
  }
}
