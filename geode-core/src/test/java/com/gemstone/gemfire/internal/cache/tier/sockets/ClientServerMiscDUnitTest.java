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

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.NoAvailableServersException;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.RegisterInterestTracker;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.LRUEvictionControllerDUnitTest;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import junit.framework.AssertionFailedError;

/**
 * Tests client server corner cases between Region and Pool
 *
 * @author Yogesh Mahajan
 *
 */
public class ClientServerMiscDUnitTest extends CacheTestCase
{
//  private static Cache cache = null;

  protected static PoolImpl pool = null;

  protected static Connection conn = null;
  
  private static Cache static_cache;

  private static int PORT1;

  private static final String k1 = "k1";

  private static final String k2 = "k2";

  private static final String server_k1 = "server-k1";

  private static final String server_k2 = "server-k2";

  private static final String REGION_NAME1 = "ClientServerMiscDUnitTest_region1";

  private static final String REGION_NAME2 = "ClientServerMiscDUnitTest_region2";

  private static final String PR_REGION_NAME = "ClientServerMiscDUnitTest_PRregion";

  private static Host host;

  private static VM server1;

  private static VM server2;

  private static RegionAttributes attrs;


  // variables for concurrent map API test
  Properties props = new Properties();
  final int putRange_1Start = 1;
  final int putRange_1End = 5;
  final int putRange_2Start = 6;
  final int putRange_2End = 10;
  final int putRange_3Start = 11;
  final int putRange_3End = 15;
  final int putRange_4Start = 16;
  final int putRange_4End = 20;
  final int removeRange_1Start = 2;
  final int removeRange_1End = 4;
  final int removeRange_2Start = 7;
  final int removeRange_2End = 9;

  
  
  /** constructor */
  public ClientServerMiscDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception
  {
    super.setUp();
    host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
  }

  private int initServerCache(boolean notifyBySub) {
    Object[] args = new Object[] { notifyBySub, getMaxThreads()};
    return ((Integer)server1.invoke(ClientServerMiscDUnitTest.class,
                                    "createServerCache",
                                    args)).intValue();
  }
  
  private int initServerCache2(boolean notifyBySub) {
    Object[] args = new Object[] {notifyBySub, getMaxThreads()};
    return ((Integer)server2.invoke(ClientServerMiscDUnitTest.class,
                                    "createServerCache",
                                    args)).intValue();
  }

  
  public void testConcurrentOperationsWithDRandPR() throws Exception {
    int port1 = initServerCache(true); // vm0
    int port2 = initServerCache2(true); // vm1
    String serverName = NetworkUtils.getServerHostName(Host.getHost(0));
    host.getVM(2).invoke(this.getClass(), "createClientCacheV", new Object[]{serverName, port1});
    host.getVM(3).invoke(this.getClass(), "createClientCacheV", new Object[]{serverName, port2});
    LogWriterUtils.getLogWriter().info("Testing concurrent map operations from a client with a distributed region");
    concurrentMapTest(host.getVM(2), "/" + REGION_NAME1);
    // TODO add verification in vm3
    LogWriterUtils.getLogWriter().info("Testing concurrent map operations from a client with a partitioned region");
    concurrentMapTest(host.getVM(2), "/" + PR_REGION_NAME);
    // TODO add verification in vm3
  }

  public void testConcurrentOperationsWithDRandPRandEmptyClient() throws Exception {
    int port1 = initServerCache(true); // vm0
    int port2 = initServerCache2(true); // vm1
    String serverName = NetworkUtils.getServerHostName(Host.getHost(0));
    host.getVM(2).invoke(this.getClass(), "createEmptyClientCache", new Object[]{serverName, port1});
    host.getVM(3).invoke(this.getClass(), "createClientCacheV", new Object[]{serverName, port2});
    LogWriterUtils.getLogWriter().info("Testing concurrent map operations from a client with a distributed region");
    concurrentMapTest(host.getVM(2), "/" + REGION_NAME1);
    // TODO add verification in vm3
    LogWriterUtils.getLogWriter().info("Testing concurrent map operations from a client with a partitioned region");
    concurrentMapTest(host.getVM(2), "/" + PR_REGION_NAME);
    // TODO add verification in vm3
  }

  /**
   * Do putIfAbsent(), replace(Object, Object),
   * replace(Object, Object, Object), remove(Object, Object) operations
   */
  public void concurrentMapTest(final VM clientVM, final String rName) {
    
    //String exceptionStr = "";
    clientVM.invoke(new CacheSerializableRunnable("doConcurrentMapOperations") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        final Region pr = cache.getRegion(rName);
        assertNotNull(rName + " not created", pr);
        boolean isEmpty = pr.getAttributes().getDataPolicy() == DataPolicy.EMPTY;
        
        // test successful putIfAbsent
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object putResult = pr.putIfAbsent(Integer.toString(i),
                                            Integer.toString(i));
          assertNull("Expected null, but got " + putResult + " for key " + i,
                     putResult);
        }
        int size;
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
              pr.isEmpty());
        }
        
        // test unsuccessful putIfAbsent
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object putResult = pr.putIfAbsent(Integer.toString(i),
                                            Integer.toString(i + 1));
          assertEquals("for i=" + i, Integer.toString(i), putResult);
          assertEquals("for i=" + i, Integer.toString(i), pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
              pr.isEmpty());
        }
               
        // test successful replace(key, oldValue, newValue)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
         boolean replaceSucceeded = pr.replace(Integer.toString(i),
                                               Integer.toString(i),
                                               "replaced" + i);
          assertTrue("for i=" + i, replaceSucceeded);
          assertEquals("for i=" + i, "replaced" + i, pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
              pr.isEmpty());
        }
        
        // test unsuccessful replace(key, oldValue, newValue)
        for (int i = putRange_1Start; i <= putRange_2End; i++) {
         boolean replaceSucceeded = pr.replace(Integer.toString(i),
                                               Integer.toString(i), // wrong expected old value
                                               "not" + i);
         assertFalse("for i=" + i, replaceSucceeded);
         assertEquals("for i=" + i,
                      i <= putRange_1End ? "replaced" + i : null,
                      pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
              pr.isEmpty());
        }
                                    
        // test successful replace(key, value)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          Object replaceResult = pr.replace(Integer.toString(i),
                                            "twice replaced" + i);
          assertEquals("for i=" + i, "replaced" + i, replaceResult);
          assertEquals("for i=" + i,
                       "twice replaced" + i,
                       pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
              pr.isEmpty());
        }
                                    
        // test unsuccessful replace(key, value)
        for (int i = putRange_2Start; i <= putRange_2End; i++) {
          Object replaceResult = pr.replace(Integer.toString(i),
                                           "thrice replaced" + i);
          assertNull("for i=" + i, replaceResult);
          assertNull("for i=" + i, pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
              pr.isEmpty());
        }
                                    
        // test unsuccessful remove(key, value)
        for (int i = putRange_1Start; i <= putRange_2End; i++) {
          boolean removeResult = pr.remove(Integer.toString(i),
                                           Integer.toString(-i));
          assertFalse("for i=" + i, removeResult);
          assertEquals("for i=" + i,
                       i <= putRange_1End ? "twice replaced" + i : null,
                       pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", putRange_1End, size);
          assertFalse("isEmpty doesnt return proper state of the PartitionedRegion", 
              pr.isEmpty());
        }

        // test successful remove(key, value)
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          boolean removeResult = pr.remove(Integer.toString(i),
                                           "twice replaced" + i);
          assertTrue("for i=" + i, removeResult);
          assertEquals("for i=" + i, null, pr.get(Integer.toString(i)));
        }
        if (!isEmpty) {
          size = pr.size();
          assertEquals("Size doesn't return expected value", 0, size);
          pr.localClear();
          assertTrue("isEmpty doesnt return proper state of the PartitionedRegion", 
              pr.isEmpty());
        }
        
        if (!isEmpty) {
          // bug #42169 - entry not updated on server when locally destroyed on client
          String key42169 = "key42169";
          pr.put(key42169, "initialValue42169");
          pr.localDestroy(key42169);
          boolean success = pr.replace(key42169, "initialValue42169", "newValue42169");
          assertTrue("expected replace to succeed", success);
          pr.destroy(key42169);
          pr.put(key42169, "secondRound");
          pr.localDestroy(key42169);
          Object result = pr.putIfAbsent(key42169, null);
          assertEquals("expected putIfAbsent to fail", result, "secondRound");
          pr.destroy(key42169);
        }
        
        if (isEmpty) {
          String key41265 = "key41265";
          boolean success = pr.remove(key41265, null);
          assertFalse("expected remove to fail because key does not exist", success);
        }
        
        // test null values
        
        // putIfAbsent with null value creates invalid entry
        Object oldValue = pr.putIfAbsent("keyForNull", null);
        assertNull(oldValue);
        if (!isEmpty) {
          assertTrue(pr.containsKey("keyForNull"));
          assertTrue(!pr.containsValueForKey("keyForNull"));
        }
        
        // replace allows null value for oldValue, meaning invalidated entry
        assertTrue(pr.replace("keyForNull", null, "no longer invalid"));
        
        // replace does not allow null value for new value
        try {
          pr.replace("keyForNull", "no longer invalid", null);
          fail("expected a NullPointerException");
        } catch (NullPointerException expected) {
        }
        
        // other variant of replace does not allow null value for new value
        try {
          pr.replace("keyForNull", null);
          fail ("expected a NullPointerException");
        } catch (NullPointerException expected) {
        }
        
        // replace with null oldvalue matches invalidated entry
        pr.putIfAbsent("otherKeyForNull", null);
        int puts = ((GemFireCacheImpl)pr.getCache()).getCachePerfStats().getPuts();
        boolean success = pr.replace("otherKeyForNull", null, "no longer invalid");
        assertTrue(success);
        int newputs = ((GemFireCacheImpl)pr.getCache()).getCachePerfStats().getPuts();
        assertTrue("stats not updated properly or replace malfunctioned", newputs == puts+1);

      }
    });
  }

  /**
   * Test two regions: notify by subscription is true.
   * For region1 the interest list is empty , for region 2 the intetest list is all keys.
   * If an update/create is made on region1 , the client should not receive any.
   * If the create/update is on region2 , the client should receive the update.
   */
  public void testForTwoRegionHavingDifferentInterestList()
      throws Exception
  {
    // start server first
    PORT1 = initServerCache(true);
    createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1);
    populateCache();
    registerInterest();
    server1.invoke(ClientServerMiscDUnitTest.class, "put");

//    pause(5000 + 5000 + 10000);
    /*final int maxWaitTime = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT).intValue();
    try {
      Thread.yield();
       Thread.sleep(maxWaitTime);
    }
    catch (InterruptedException e) {
      fail("interrupted");
    }*/
    verifyUpdates();

  }

  /**
   * Test two regions: notify by subscription is true.
   * Both the regions have registered interest in all the keys.
   * Now close region1 on the client.
   * The region1 should get removed from the interest list on CCP at server.
   * Any update on region1 on server should not get pushed to the client.
   * Ensure that the message related is not added to the client's queue at all
   * ( which is diferent from not receiving a callbak on the client).
   * If an update on region2 is made on the server , then client should receive the calback
   */
  public void testForTwoRegionHavingALLKEYSInterest()
      throws Exception
  {
    // start server first
    PORT1 = initServerCache(true);
    createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1);
    populateCache();
    registerInterestInBothTheRegions();
    closeRegion1();
    Wait.pause(6000);
    server1.invoke(ClientServerMiscDUnitTest.class,
        "verifyInterestListOnServer");
    server1.invoke(ClientServerMiscDUnitTest.class, "put");
    //pause(5000);
    verifyUpdatesOnRegion2();
  }

  /** Test two regions: notify by subscription is true.
   * Both the regions have registered interest in all the keys.
   * Close both the regions. When the last region is closed ,
   * it should close the ConnectionProxy on the client ,
   * close all the server connection threads on the server &
   * remove the CacheClientProxy from the CacheClient notifier
   */
  public void testRegionClose() throws Exception
  {
    // start server first
    PORT1 = initServerCache(true);
    pool = (PoolImpl)createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)),PORT1);
    populateCache();
    registerInterestInBothTheRegions();
    closeBothRegions();
    //pause(5000);
    assertEquals(false, pool.isDestroyed());
    pool.destroy();
    assertEquals(true, pool.isDestroyed());
    server1.invoke(ClientServerMiscDUnitTest.class,
        "verifyNoCacheClientProxyOnServer");

  }

  /**
   * Test two regions: notify by
   * subscription is true. Both the regions have registered interest in all the
   * keys. Destroy region1 on the client. It should reach the server , kill the
   * region on the server , propagate it to the interested clients , but it
   * should keep CacheClient Proxy alive. Destroy Region2 . It should reach
   * server , close conenction proxy , destroy the region2 on the server ,
   * remove the cache client proxy from the cache client notifier & propagate it
   * to the clients. Then create third region and verify that no
   * CacheClientProxy is created on server
   */
  public void testCCPDestroyOnLastDestroyRegion() throws Exception
  {
    PORT1 = initServerCache(true);
    PoolImpl pool = (PoolImpl)createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)),PORT1);
    destroyRegion1();
    // pause(5000);
    server1.invoke(ClientServerMiscDUnitTest.class,
        "verifyCacheClientProxyOnServer", new Object[] { new String(
            REGION_NAME1) });
    Connection conn = pool.acquireConnection();
    assertNotNull(conn);
    assertEquals(1, pool.getConnectedServerCount());
    assertEquals(false, pool.isDestroyed());
    destroyRegion2();
    assertEquals(false, pool.isDestroyed());
    destroyPRRegion();
    assertEquals(false, pool.isDestroyed());
    pool.destroy();
    assertEquals(true, pool.isDestroyed());
    // pause(5000);
    server1.invoke(ClientServerMiscDUnitTest.class,
        "verifyNoCacheClientProxyOnServer");
    try {
      getCache().createRegion(REGION_NAME2, attrs);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
  }

  /**
   * Test two regions:If notify by
   * subscription is false , both the regions should receive invalidates for the
   * updates on server in their respective regions
   *
   */
  public void testInvalidatesPropagateOnTwoRegions()
      throws Exception
  {
    // start server first
    PORT1 = initServerCache(false);
    createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1);
    registerInterestForInvalidatesInBothTheRegions();
    populateCache();
    server1.invoke(ClientServerMiscDUnitTest.class, "put");
    //pause(5000);
    verifyInvalidatesOnBothRegions();

  }

  /**
   * Test for bug 43407, where LRU in the client caused an entry to be
   * evicted with DESTROY(), then the client invalidated the entry and
   * did a get().  After the get() the entry was not seen to be in the
   * client's cache.  This turned out to be expected behavior, but we
   * now have this test to guarantee that the product behaves as expected.
   */
  public void testBug43407()
      throws Exception
  {
    // start server first
    PORT1 = initServerCache(false);
    createClientCache(NetworkUtils.getServerHostName(Host.getHost(0)), PORT1);
    registerInterestForInvalidatesInBothTheRegions();
    Region region = static_cache.getRegion(REGION_NAME1);
    populateCache();
    region.put("invalidationKey", "invalidationValue");
    region.localDestroy("invalidationKey");
    if (region.containsKey("invalidationKey")) {
      fail("region still contains invalidationKey");
    }
    region.invalidate("invalidationKey");
    if (region.containsKey("invalidationKey")) {
      fail("this test expects the entry is not created on invalidate() if not there before the operation");
    }
    Object value = region.get("invalidationKey");
    if (value != null) {
      fail("this test expected a null response to get('invalidationKey')");
    }
    if (!region.containsKeyOnServer("invalidationKey")) {
      fail("expected an entry on the server after invalidation");
    }
    // bug 43407 asserts that there should be an entry, but the product does not
    // do this.  This verifies that the product does not behave as asserted in that bug
    if (region.containsKey("invalidationKey")) {
      fail("expected no entry after invalidation when entry was not in client but was on server");
    }
  }

  /**
   * Create cache, create pool, notify-by-subscription=false,
   * create a region and on client and on server.
   * Do not attach pool to region ,
   * populate some entries on region both on client and server.
   * Update the entries on server the client.
   * The client should not have entry invalidate.
   *
   * @throws Exception
   */
  public void testInvalidatesPropagateOnRegionHavingNoPool()
      throws Exception
  {
    // start server first
    PORT1 = initServerCache(false);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new ClientServerMiscDUnitTest("temp").createCache(props);
    String host = NetworkUtils.getServerHostName(server1.getHost());
    PoolImpl p = (PoolImpl)PoolManager.createFactory()
      .addServer(host, PORT1)
      .setSubscriptionEnabled(true)
      .setThreadLocalConnections(true)
      .setReadTimeout(1000)
      .setSocketBufferSize(32768)
      .setMinConnections(3)
      .setSubscriptionRedundancy(-1)
      .setPingInterval(2000)
      // .setRetryAttempts(5)
      // .setRetryInterval(2000)
      .create("testInvalidatesPropagateOnRegionHavingNoPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    // factory.setPoolName(p.getName());

    attrs = factory.create();
    final Region region1 = getCache().createRegion(REGION_NAME1, attrs);
    final Region region2 = getCache().createRegion(REGION_NAME2, attrs);
    assertNotNull(region1);
    assertNotNull(region2);
    pool = p;
    conn = pool.acquireConnection();
    assertNotNull(conn);

    populateCache();
    server1.invoke(ClientServerMiscDUnitTest.class, "put");
    // pause(5000);
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        Object val = region1.getEntry(k1).getValue();
        return k1.equals(val);
      }
      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
    
    // assertEquals(region1.getEntry(k1).getValue(), k1);
    wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        Object val = region1.getEntry(k2).getValue();
        return k2.equals(val);
      }
      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
    
    wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        Object val = region2.getEntry(k1).getValue();
        return k1.equals(val);
      }
      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
    
    // assertEquals(region1.getEntry(k2).getValue(), k2);
    // assertEquals(region2.getEntry(k1).getValue(), k1);
    wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        Object val = region2.getEntry(k2).getValue();
        return k2.equals(val);
      }
      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
    
    // assertEquals(region2.getEntry(k2).getValue(), k2);
  }

  /**
   * Create proxy before cache creation, create cache, create two regions,
   * attach same bridge writer to both of the regions Region interests AL_KEYS
   * on both the regions, notify-by-subscription=true . The CCP should have both
   * the regions in interest list.
   *
   * @throws Exception
   */

  public void testProxyCreationBeforeCacheCreation() throws Exception
  {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    PORT1 = initServerCache(true);
    String host = NetworkUtils.getServerHostName(server1.getHost());
    Pool p = PoolManager.createFactory()
      .addServer(host, PORT1)
      .setSubscriptionEnabled(true)
      .setSubscriptionRedundancy(-1)
      // .setRetryAttempts(5)
      .create("testProxyCreationBeforeCacheCreationPool");

    Cache cache = getCache();
    assertNotNull(cache);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes myAttrs = factory.create();
    Region region1 = cache.createRegion(REGION_NAME1, myAttrs);
    Region region2 = cache.createRegion(REGION_NAME2, myAttrs);
    assertNotNull(region1);
    assertNotNull(region2);
    //region1.registerInterest(CacheClientProxy.ALL_KEYS);
    region2.registerInterest("ALL_KEYS");
    Wait.pause(6000);
    server1.invoke(ClientServerMiscDUnitTest.class,
        "verifyInterestListOnServer");

  }
  /**
   * 
   * Cycling a DistributedSystem with an initialized pool causes interest registration NPE
   * 
   * Test Scenario:
   *  
   * Create a DistributedSystem (DS1). 
   * Create a pool, initialize (creates a proxy with DS1 memberid) 
   * Disconnect DS1.  Create a DistributedSystem (DS2). 
   * Create a Region with pool, it attempts to register interest using DS2 memberid, gets NPE.
   *  
   * @throws Exception
   */
  public void testBug35380() throws Exception
  {
    //work around GEODE-477
    IgnoredException.addIgnoredException("Connection reset");
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    
    PORT1 = initServerCache(true);
    String host = NetworkUtils.getServerHostName(server1.getHost());
    Pool p = PoolManager.createFactory()
      .addServer(host, PORT1)
      .setSubscriptionEnabled(true)
      .setSubscriptionRedundancy(-1)
      //.setRetryAttempts(5)
      .create("testBug35380Pool");

    Cache cache = getCache();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes myAttrs = factory.create();
    Region region1 = cache.createRegion(REGION_NAME1, myAttrs);
    Region region2 = cache.createRegion(REGION_NAME2, myAttrs);
    assertNotNull(region1);
    assertNotNull(region2);
    
    region2.registerInterest("ALL_KEYS");
    
    ds.disconnect();
    Properties prop = new Properties();
    prop.setProperty("mcast-port", "0");
    prop.setProperty("locators", "");
    ds = getSystem(prop);
    
    cache = getCache();
    assertNotNull(cache);

    AttributesFactory factory1 = new AttributesFactory();
    factory1.setScope(Scope.DISTRIBUTED_ACK);
    //reuse writer from prev DS
    factory1.setPoolName(p.getName());

    RegionAttributes attrs1 = factory1.create();
    try {
      cache.createRegion(REGION_NAME1, attrs1);
      fail("expected ShutdownException");
    }
    catch(IllegalStateException expected) {
    }
    catch (DistributedSystemDisconnectedException expected) {
    } 
  }
  

  private void createCache(Properties props) throws Exception {
    createCacheV(props);
  }
  private Cache createCacheV(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    Cache cache = getCache();
    assertNotNull(cache);
    return cache;
  }

  public static void createClientCacheV(String h, int port)
  throws Exception {
    _createClientCache(h, port, false);
  }

  public static void createEmptyClientCache(String h, int port)
  throws Exception {
    _createClientCache(h, port, true);
  }

  public static Pool createClientCache(String h, int port)
  throws Exception  {
    return _createClientCache(h, port, false);
  }
  
  public static Pool _createClientCache(String h, int port, boolean empty)
  throws Exception  {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    Cache cache = new ClientServerMiscDUnitTest("temp").createCacheV(props);
    ClientServerMiscDUnitTest.static_cache = cache;
    PoolImpl p = (PoolImpl)PoolManager.createFactory()
      .addServer(h, port)
      .setSubscriptionEnabled(true)
      .setThreadLocalConnections(true)
      .setReadTimeout(1000)
      .setSocketBufferSize(32768)
      .setMinConnections(3)
      .setSubscriptionRedundancy(-1)
      .setPingInterval(2000)
      // .setRetryAttempts(5)
      // .setRetryInterval(2000)
      .create("ClientServerMiscDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    if (empty) {
      factory.setDataPolicy(DataPolicy.EMPTY);
    }
    factory.setPoolName(p.getName());

    attrs = factory.create();
    Region region1 = cache.createRegion(REGION_NAME1, attrs);
    Region region2 = cache.createRegion(REGION_NAME2, attrs);
    Region prRegion = cache.createRegion(PR_REGION_NAME, attrs);
    assertNotNull(region1);
    assertNotNull(region2);
    assertNotNull(prRegion);
    pool = p;
//    conn = pool.acquireConnection();
//    assertNotNull(conn);
    // TODO does this WaitCriterion actually help?
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        try {
          conn = pool.acquireConnection();
          if (conn == null) {
            excuse = "acquireConnection returned null?";
            return false;
          }
          return true;
        } catch (NoAvailableServersException e) {
          excuse = "Cannot find a server: " + e;
          return false;
        }
      }
      public String description() {
        return excuse;
      }
    };
    Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
    
    return p;
  }

  public static Integer createServerCache(Boolean notifyBySubscription, Integer maxThreads)
  throws Exception {
    Cache cache = new ClientServerMiscDUnitTest("temp").createCacheV(new Properties());
    unsetSlowDispatcherFlag();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes myAttrs = factory.create();
    Region r1 = cache.createRegion(REGION_NAME1, myAttrs);
    Region r2 = cache.createRegion(REGION_NAME2, myAttrs);
    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.PARTITION);
    RegionAttributes prAttrs = factory.create();
    Region pr = cache.createRegion(PR_REGION_NAME, prAttrs);
    assertNotNull(r1);
    assertNotNull(r2);
    assertNotNull(pr);

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    r1.getCache().getDistributedSystem().getLogWriter().info("Starting server on port " + port);
    server.setPort(port);
    server.setMaxThreads(maxThreads.intValue());
    server.setNotifyBySubscription(notifyBySubscription.booleanValue());
    server.start();
    r1.getCache().getDistributedSystem().getLogWriter().info("Started server on port " + server.getPort());
    return new Integer(server.getPort());

  }

  protected int getMaxThreads() {
    return 0; 
  }

  public static void registerInterest()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r);
      //r.registerInterestRegex(CacheClientProxy.ALL_KEYS);
      r.registerInterest("ALL_KEYS");
    }
    catch (CacheWriterException e) {
      e.printStackTrace();
      Assert.fail("Test failed due to CacheWriterException during registerInterest", e);
    }
  }

  public static void registerInterestForInvalidatesInBothTheRegions()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      assertNotNull(r1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r2);
      r1.registerInterest("ALL_KEYS", false, false);
      r2.registerInterest("ALL_KEYS", false, false);
    }
    catch (CacheWriterException e) {
      e.printStackTrace();
      Assert.fail(
          "Test failed due to CacheWriterException during registerInterestnBothRegions",
          e);
    }
  }

  public static void registerInterestInBothTheRegions()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      assertNotNull(r1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r2);
      r1.registerInterest("ALL_KEYS");
      r2.registerInterest("ALL_KEYS");
    }
    catch (CacheWriterException e) {
      e.printStackTrace();
      Assert.fail(
          "Test failed due to CacheWriterException during registerInterestnBothRegions",
          e);
    }
  }

  public static void closeRegion1()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      assertNotNull(r1);
      r1.close();
    }
    catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Test failed due to Exception during closeRegion1", e);
    }
  }

  public static void closeBothRegions()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      Region pr = cache.getRegion(Region.SEPARATOR + PR_REGION_NAME);
      assertNotNull(r1);
      assertNotNull(r2);
      assertNotNull(pr);
      r1.close();
      r2.close();
      pr.close();
    }
    catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Test failed due to Exception during closeBothRegions", e);
    }
  }

  public static void destroyRegion1()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      assertNotNull(r1);
      r1.destroyRegion();
    }
    catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Test failed due to Exception during closeBothRegions", e);
    }
  }

  public static void destroyRegion2()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r2);
      r2.destroyRegion();
    }
    catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Test failed due to Exception during closeBothRegions", e);
    }
  }

  public static void destroyPRRegion()  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      Region r2 = cache.getRegion(Region.SEPARATOR + PR_REGION_NAME);
      assertNotNull(r2);
      r2.destroyRegion();
    } catch (Exception e) {
     // e.printStackTrace();
      Assert.fail("Test failed due to Exception during closeBothRegions", e);
    }
  }

  public static void verifyInterestListOnServer()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      assertEquals("More than one BridgeServer", 1, cache.getCacheServers()
          .size());
      CacheServerImpl bs = (CacheServerImpl)cache.getCacheServers()
          .iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      assertNotNull(bs.getAcceptor().getCacheClientNotifier());
      Iterator iter_prox = bs.getAcceptor().getCacheClientNotifier()
          .getClientProxies().iterator();
      while (iter_prox.hasNext()) {
        CacheClientProxy ccp = (CacheClientProxy)iter_prox.next();
        // CCP should not contain region1
        Set akr = ccp.cils[RegisterInterestTracker.interestListIndex].regions;
        assertNotNull(akr);
        assertTrue(!akr.contains(Region.SEPARATOR + REGION_NAME1));
        // CCP should contain region2
        assertTrue(akr.contains(Region.SEPARATOR + REGION_NAME2));
        assertEquals(1, akr.size());
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("while setting verifyInterestListOnServer  " + ex);
    }
  }

  public static void verifyNoCacheClientProxyOnServer()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      assertEquals("More than one BridgeServer", 1, cache.getCacheServers()
          .size());
      CacheServerImpl bs = (CacheServerImpl)cache.getCacheServers()
          .iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();

      assertNotNull(ccn);
      WaitCriterion wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          return ccn.getClientProxies().size() == 0;
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 40 * 1000, 1000, true);
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("while setting verifyNoCacheClientProxyOnServer  " + ex);
    }
  }

  public static void verifyCacheClientProxyOnServer(String regionName)
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      assertNull(cache.getRegion(Region.SEPARATOR + regionName));
       verifyCacheClientProxyOnServer();

      //assertEquals(1, bs.getAcceptor().getCacheClientNotifier().getClientProxies().size());
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("while setting verifyNoCacheClientProxyOnServer  " + ex);
    }
  }

  public static void verifyCacheClientProxyOnServer()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      assertEquals("More than one BridgeServer", 1, cache.getCacheServers()
          .size());
      CacheServerImpl bs = (CacheServerImpl)cache.getCacheServers()
          .iterator().next();
      assertNotNull(bs);
      assertNotNull(bs.getAcceptor());
      final CacheClientNotifier ccn = bs.getAcceptor().getCacheClientNotifier();

      assertNotNull(ccn);
      WaitCriterion wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          return ccn.getClientProxies().size() == 1;
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 40 * 1000, 1000, true);
    }
    catch (Exception ex) {
      ex.printStackTrace();
      fail("while setting verifyNoCacheClientProxyOnServer  " + ex);
    }
  }

  public static void populateCache()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r1);
      assertNotNull(r2);

      if (!r1.containsKey(k1))
        r1.create(k1, k1);
      if (!r1.containsKey(k2))
        r1.create(k2, k2);
      if (!r2.containsKey(k1))
        r2.create(k1, k1);
      if (!r2.containsKey(k2))
        r2.create(k2, k2);

      assertEquals(r1.getEntry(k1).getValue(), k1);
      assertEquals(r1.getEntry(k2).getValue(), k2);
      assertEquals(r2.getEntry(k1).getValue(), k1);
      assertEquals(r2.getEntry(k2).getValue(), k2);
    }
    catch (Exception ex) {
      Assert.fail("failed while createEntries()", ex);
    }
  }

  public static void put()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r1);
      assertNotNull(r2);

      r1.put(k1, server_k1);
      r1.put(k2, server_k2);

      r2.put(k1, server_k1);
      r2.put(k2, server_k2);

      assertEquals(r1.getEntry(k1).getValue(), server_k1);
      assertEquals(r1.getEntry(k2).getValue(), server_k2);
      assertEquals(r2.getEntry(k1).getValue(), server_k1);
      assertEquals(r2.getEntry(k2).getValue(), server_k2);
    }
    catch (Exception ex) {
      Assert.fail("failed while put()", ex);
    }
  }

  public static void verifyUpdates()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      final Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      final Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r1);
      assertNotNull(r2);
      // verify updates
      WaitCriterion wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = r1.getEntry(k1).getValue();
          return k1.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      // assertEquals(k1, r1.getEntry(k1).getValue());
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = r1.getEntry(k2).getValue();
          return k2.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      // assertEquals(k2, r1.getEntry(k2).getValue());
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = r2.getEntry(k1).getValue();
          return server_k1.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      // assertEquals(server_k1, r2.getEntry(k1).getValue());
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = r2.getEntry(k2).getValue();
          return server_k2.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      // assertEquals(server_k2, r2.getEntry(k2).getValue());
    }
    catch (Exception ex) {
      Assert.fail("failed while verifyUpdates()", ex);
    }
  }

  public static void verifyInvalidatesOnBothRegions()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      final Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME1);
      final Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r1);
      assertNotNull(r2);
      
      WaitCriterion wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = r1.getEntry(k1).getValue();
          return val == null;
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 90 * 1000, 1000, true);
      
      // assertNull(r1.getEntry(k1).getValue());
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = r1.getEntry(k1).getValue();
          return val == null;
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 90 * 1000, 1000, true);
      
      // assertNull(r1.getEntry(k2).getValue());
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = r1.getEntry(k2).getValue();
          return val == null;
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 90 * 1000, 1000, true);
      

      // assertNull(r2.getEntry(k1).getValue());
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = r2.getEntry(k1).getValue();
          return val == null;
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 90 * 1000, 1000, true);
      
      // assertNull(r2.getEntry(k2).getValue());
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = r2.getEntry(k2).getValue();
          return val == null;
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 90 * 1000, 1000, true);
    }
    catch (Exception ex) {
      Assert.fail("failed while verifyInvalidatesOnBothRegions()", ex);
    }
  }

  public static void verifyUpdatesOnRegion2()
  {
    try {
      Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
      final Region r2 = cache.getRegion(Region.SEPARATOR + REGION_NAME2);
      assertNotNull(r2);
      WaitCriterion wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = r2.getEntry(k1).getValue();
          return server_k1.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      // assertEquals(server_k1, r2.getEntry(k1).getValue());
      wc = new WaitCriterion() {
        String excuse;
        public boolean done() {
          Object val = r2.getEntry(k2).getValue();
          return server_k2.equals(val);
        }
        public String description() {
          return excuse;
        }
      };
      Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
      
      // assertEquals(server_k2, r2.getEntry(k2).getValue());
    }
    catch (Exception ex) {
      Assert.fail("failed while verifyUpdatesOnRegion2()", ex);
    }
  }

  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    // close the clients first
    closeCache();
    // then close the servers
    server1.invoke(ClientServerMiscDUnitTest.class, "closeCache");
  }

  public static void closeCache()
  {
    Cache cache = new ClientServerMiscDUnitTest("temp").getCache();
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  /**
   * set the boolean for starting the dispatcher thread a bit later to FALSE.
   * This is just a precaution in case any test set it to true and did not unset
   * it on completion.
   *
   */
  public static void unsetSlowDispatcherFlag()
  {
    CacheClientProxy.isSlowStartForTesting = false;
  }

}
