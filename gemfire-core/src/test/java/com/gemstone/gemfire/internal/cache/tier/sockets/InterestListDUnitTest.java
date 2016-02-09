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
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.AssertionFailedError;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.ClientSession;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestRegistrationEvent;
import com.gemstone.gemfire.cache.InterestRegistrationListener;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.cache.NoSubscriptionServersAvailableException;

/**
 * Test Scenario :
 *
 * Test 1:  testInterestListRegistration()
 *
 * one server two clients
 * create Entries in all the vms
 * c1 : register (k1)
 * c2 : register (k2)
 * c1 : put (k1 -> vm1-k1) AND (k2 -> vm1-k2)
 * c2 : validate (k1 == k1) AND (k2 == vm1-k2) // as k1 is not registered
 * c2 : put (k1 -> vm2-k1)
 * c1 : validate (k1 == vm2-k1) AND (k2 == vm1-k2)// as k2 is not registered
 * c1 : unregister(k1)
 * c2 : unregister(k2)
 * c1 : put (k1 -> vm1-k1-again) AND (k2 -> vm1-k2-again)
 * c2 : validate (k1 == vm2-k1) AND (k2 == vm2-k2) // as both are not registered
 * c2 : put (k1 -> vm2-k1-again) AND (k2 -> vm2-k2-again)
 * c1 : validate (k1 == vm1-k1-again) AND (k2 == vm1-k2-again)// as both are not registered
 *
 * Test2: testInterestListRegistration_ALL_KEYS
 *
 * one server two clients
 * create Entries in all the vms
 * register ALL_KEYS and verifies that updates are receving to all the keys
 *
 *
 * Test3:  testInitializationOfRegionFromInterestList
 *
 * one server two clients
 * create Entries in all the vms
 * server directly puts some values
 * then both client connects to the server
 * c1 register(k1,k2,k3) and c2 register (k4,k5)
 * then verify that updates has occurred as a result of interest registration.
 *
 *
 * @author Yogesh Mahajan
 * @author Suyog Bhokare
 *
 */
public class InterestListDUnitTest extends DistributedTestCase
{
  static Cache cache = null;

  VM vm0 = null;

  VM vm1 = null;

  VM vm2 = null;

  /** the server cache's port number */
  static  int PORT1;

  // using a Integer instead of String to make sure ALL_KEYS works
  // on non-String keys
  final static Integer key1 = new Integer(1);
  final static Integer key2 = new Integer(2);
  final static String key1_originalValue = "key-1-orig-value";
  final static String key2_originalValue = "key-2-orig-value";

  static final String REGION_NAME = "InterestListDUnitTest_region";

  /** some tests use this to hold the server for invoke() access */
  static CacheServer server;

  /** interestListener listens in cache server vms */
  static InterestListener interestListener;

  /** constructor */
  public InterestListDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
    Wait.pause(10000);
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    // start servers first
    PORT1 =  ((Integer) vm0.invoke(InterestListDUnitTest.class, "createServerCache")).intValue();

  }
/**
   * one server two clients
   * create Entries in all the vms
   * c1 : register (k1)
   * c2 : register (k2)
   * c1 : put (k1 -> vm1-k1) AND (k2 -> vm1-k2)
   * c2 : validate (k1 == k1) AND (k2 == vm1-k2) // as k1 is not registered
   * c2 : put (k1 -> vm2-k1)
   * c1 : validate (k1 == vm2-k1) AND (k2 == vm1-k2)// as k2 is not registered
   * c1 : unregister(k1)
   * c2 : unregister(k2)
   * c1 : put (k1 -> vm1-k1-again) AND (k2 -> vm1-k2-again)
   * c2 : validate (k1 == vm2-k1) AND (k2 == vm2-k2) // as both are not registered
   * c2 : put (k1 -> vm2-k1-again) AND (k2 -> vm2-k2-again)
   * c1 : validate (k1 == vm1-k1-again) AND (k2 == vm1-k2-again)// as both are not registered
   *
   */
    public void testInterestListRegistration()
    {

      vm1.invoke(InterestListDUnitTest.class, "createClientCache",new Object[] {
        NetworkUtils.getServerHostName(vm0.getHost()), new Integer(PORT1)});
      vm2.invoke(InterestListDUnitTest.class, "createClientCache",new Object[] {
        NetworkUtils.getServerHostName(vm0.getHost()), new Integer(PORT1)});

      vm1.invoke(InterestListDUnitTest.class, "createEntriesK1andK2");
      vm2.invoke(InterestListDUnitTest.class, "createEntriesK1andK2");

      vm1.invoke(InterestListDUnitTest.class, "registerKey",
          new Object[] { key1 });
      vm2.invoke(InterestListDUnitTest.class, "registerKey",
          new Object[] { key2 });

      vm1.invoke(InterestListDUnitTest.class, "put", new Object[] { "vm1" });
      Wait.pause(10000);
      vm2.invoke(InterestListDUnitTest.class, "validateEntriesK1andK2",
          new Object[] { "vm2" });
      vm2.invoke(InterestListDUnitTest.class, "put", new Object[] { "vm2" });
      Wait.pause(10000);
      vm1.invoke(InterestListDUnitTest.class, "validateEntriesK1andK2",
          new Object[] { "vm1" });

      vm1.invoke(InterestListDUnitTest.class, "unregisterKey",
          new Object[] { key1 });
      vm2.invoke(InterestListDUnitTest.class, "unregisterKey",
          new Object[] { key2 });

      vm1.invoke(InterestListDUnitTest.class, "putAgain", new Object[] { "vm1" });
      Wait.pause(10000);
      vm2.invoke(InterestListDUnitTest.class, "validateEntriesAgain",
          new Object[] { "vm2" });
      vm2.invoke(InterestListDUnitTest.class, "putAgain", new Object[] { "vm2" });
      Wait.pause(10000);
      vm1.invoke(InterestListDUnitTest.class, "validateEntriesAgain",
          new Object[] { "vm1" });
    }

/**
 * one server two clients
 * create Entries in all the vms
 *
 * STEP 1:
 * c2:  put (k2 -> vm-k2)
 * c1:  validate k2 == k2 (not updated because no interest)
 *
 * STEP 2
 * c1:  register k2
 * c1 : validate k2 == vm-k2 (updated because of registerInterest)
 * c1:  validate k1 == k1 (other key not updated because still no interest)
 *
 * STEP 3:
 * c1:  put (k1 -> vm-k1)
 * c2:  validate k1 == k1 (not updated because no interest)
 * c2:  register k1
 * c2:  validate k1 == vm-k1 (updated because of registerInterest)
 *
 * STEP 4:
 * c2:  unregister k1
 * c1:  put k1->k1 (old value)
 * c2:  validate k1 == vm-k1 (no interest, so missing update)
 */
  public void testValueRefresh()
  {

    // Initialization
    vm1.invoke(InterestListDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(Host.getHost(0)), new Integer(PORT1)});
    vm2.invoke(InterestListDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(Host.getHost(0)), new Integer(PORT1)});

    vm1.invoke(InterestListDUnitTest.class, "createEntriesK1andK2");
    vm2.invoke(InterestListDUnitTest.class, "createEntriesK1andK2");

    // STEP 1
    vm2.invoke(InterestListDUnitTest.class, "putSingleEntry",
        new Object[] { key2, "vm2" });
    Wait.pause(2000);
    vm1.invoke(InterestListDUnitTest.class, "validateSingleEntry",
        new Object[] {key2, key2_originalValue});

    // STEP 2
    // Force key2 to synchronize with server cache
    vm1.invoke(InterestListDUnitTest.class, "registerKey",
        new Object[] { key2 });
    // Verify that new value is present
    vm1.invoke(InterestListDUnitTest.class, "validateSingleEntry",
        new Object[] {key2, "vm2"}); // value now magically changed
    // but the other key should not have changed
    vm1.invoke(InterestListDUnitTest.class, "validateSingleEntry",
        new Object[] {key1, key1_originalValue}); // still unchanged

    // STEP 3
    vm1.invoke(InterestListDUnitTest.class, "putSingleEntry",
        new Object[] { key1, "vm1" });
    Wait.pause(2000);
    vm2.invoke(InterestListDUnitTest.class, "validateSingleEntry",
        new Object[] {key1, key1_originalValue}); // still unchanged
    vm2.invoke(InterestListDUnitTest.class, "registerKey",
        new Object[] { key1 });
    // Verify that new value is present
    vm2.invoke(InterestListDUnitTest.class, "validateSingleEntry",
        new Object[] {key1, "vm1"}); // value now magically changed

    // STEP 4
    // unregister on one key
    vm2.invoke(InterestListDUnitTest.class, "unregisterKey",
        new Object[] { key1 });
    vm1.invoke(InterestListDUnitTest.class, "putSingleEntry",
        new Object[] { key1, key1_originalValue });
    Wait.pause(2000);
    vm2.invoke(InterestListDUnitTest.class, "validateSingleEntry",
        new Object[] {key1, "vm1"}); // update lost
  }

/**
 * one server two clients
 * create Entries in all the vms
 * register ALL_KEYS and verifies that updates are receiving to all the keys
 *
 */
  public void testInterestListRegistration_ALL_KEYS()
  {

    vm1.invoke(InterestListDUnitTest.class, "createClientCache",new Object[] {
      NetworkUtils.getServerHostName(vm0.getHost()), new Integer(PORT1)});
    vm2.invoke(InterestListDUnitTest.class, "createClientCache",new Object[] {
      NetworkUtils.getServerHostName(vm0.getHost()), new Integer(PORT1)});

    vm1.invoke(InterestListDUnitTest.class, "createEntriesK1andK2");
    vm2.invoke(InterestListDUnitTest.class, "createEntriesK1andK2");

    vm2.invoke(InterestListDUnitTest.class, "registerALL_KEYS");

    vm1.invoke(InterestListDUnitTest.class, "put_ALL_KEYS");
    Wait.pause(10000);
    vm2.invoke(InterestListDUnitTest.class, "validate_ALL_KEYS");

  }
 /**
  * one server two clients
  * create Entries in all the vms
  * server directly puts some values
  * then both clients connect to the server
  * c1 register(k1,k2,k3) and c2 register (k4,k5)
  * then verify that updates has occured as a result of interest registration.
  *
  */
  public void testInitializationOfRegionFromInterestList()
  {
    // directly put on server
    vm0.invoke(InterestListDUnitTest.class, "multiple_put");
    Wait.pause(1000);
    // create clients to connect to that server
    vm1.invoke(InterestListDUnitTest.class, "createClientCache",new Object[] {
      NetworkUtils.getServerHostName(vm0.getHost()), new Integer(PORT1)});
    vm2.invoke(InterestListDUnitTest.class, "createClientCache",new Object[] {
      NetworkUtils.getServerHostName(vm0.getHost()), new Integer(PORT1)});

    // register interest
    vm1.invoke(InterestListDUnitTest.class, "registerKeys");
    vm2.invoke(InterestListDUnitTest.class, "registerKeysAgain");
    Wait.pause(10000);
    // verify the values for registered keys
    vm1.invoke(InterestListDUnitTest.class,
        "validateRegionEntriesFromInterestListInVm1");
    vm2.invoke(InterestListDUnitTest.class,
        "validateRegionEntriesFromInterestListInVm2");

  }

  /**
   * one server two clients
   * create Entries in all the vms
   * s1 : register (k1) for c1
   * s1 : register (k2) for c2
   * c1 : put (k1 -> vm1-k1) AND (k2 -> vm1-k2)
   * c2 : validate (k1 == k1) AND (k2 == vm1-k2) // as k1 is not registered
   * c2 : put (k1 -> vm2-k1)
   * c1 : validate (k1 == vm2-k1) AND (k2 == vm1-k2)// as k2 is not registered
   * s1 : unregister(k1) for c1
   * s1 : unregister(k2) for c2
   * c1 : put (k1 -> vm1-k1-again) AND (k2 -> vm1-k2-again)
   * c2 : validate (k1 == vm2-k1) AND (k2 == vm2-k2) // as both are not registered
   * c2 : put (k1 -> vm2-k1-again) AND (k2 -> vm2-k2-again)
   * c1 : validate (k1 == vm1-k1-again) AND (k2 == vm1-k2-again)// as both are not registered
   *
   */
    public void testInterestListRegistrationOnServer()
    {

      DistributedMember c1 = (DistributedMember)vm1
        .invoke(InterestListDUnitTest.class, "createClientCache",new Object[] {
          NetworkUtils.getServerHostName(vm0.getHost()), PORT1});
      DistributedMember c2 = (DistributedMember)vm2
        .invoke(InterestListDUnitTest.class, "createClientCache",new Object[] {
          NetworkUtils.getServerHostName(vm0.getHost()), PORT1});

      vm1.invoke(InterestListDUnitTest.class, "createEntriesK1andK2");
      vm2.invoke(InterestListDUnitTest.class, "createEntriesK1andK2");

      vm0.invoke(InterestListDUnitTest.class, "registerKeyForClient",
          new Object[] { c1, key1 });
      vm0.invoke(InterestListDUnitTest.class, "registerKeyForClient",
          new Object[] { c2, key2 });

      vm0.invoke(InterestListDUnitTest.class, "flushQueues");

      vm1.invoke(InterestListDUnitTest.class, "put", new Object[] { "vm1" });

      vm0.invoke(InterestListDUnitTest.class, "flushQueues");

      vm2.invoke(InterestListDUnitTest.class, "validateEntriesK1andK2",
          new Object[] { "vm2" });
      vm2.invoke(InterestListDUnitTest.class, "put", new Object[] { "vm2" });

      vm0.invoke(InterestListDUnitTest.class, "flushQueues");

      vm1.invoke(InterestListDUnitTest.class, "validateEntriesK1andK2",
          new Object[] { "vm1" });

      vm0.invoke(InterestListDUnitTest.class, "unregisterKeyForClient",
          new Object[] { c1, key1 });
      vm0.invoke(InterestListDUnitTest.class, "unregisterKeyForClient",
          new Object[] { c2, key2 });

      vm1.invoke(InterestListDUnitTest.class, "putAgain", new Object[] { "vm1" });

      vm0.invoke(InterestListDUnitTest.class, "flushQueues");

      vm2.invoke(InterestListDUnitTest.class, "validateEntriesAgain",
          new Object[] { "vm2" });
      vm2.invoke(InterestListDUnitTest.class, "putAgain", new Object[] { "vm2" });

      vm0.invoke(InterestListDUnitTest.class, "flushQueues");

      vm1.invoke(InterestListDUnitTest.class, "validateEntriesAgain",
          new Object[] { "vm1" });
    }

    /**
     * two servers one client
     * create Entries in all the vms
     * register interest in various ways and ensure that registration listeners
     * are properly invoked
     */
    public void testInterestRegistrationListeners() {
      int port2 = 0;

      try {
        createCache();
        server = addCacheServer();
        port2 = server.getPort();
      } catch (Exception ex) {
        Assert.fail("Cache creation threw an exception", ex);
      }

      addRegisterInterestListener();
      vm0.invoke(InterestListDUnitTest.class, "addRegisterInterestListener");

      // servers are set up, now do the clients
      DistributedMember c1 = (DistributedMember)vm1
      .invoke(InterestListDUnitTest.class, "createClientCache",new Object[] {
        NetworkUtils.getServerHostName(vm0.getHost()), PORT1, port2});
      DistributedMember c2 = (DistributedMember)vm2
      .invoke(InterestListDUnitTest.class, "createClientCache",new Object[] {
        NetworkUtils.getServerHostName(vm0.getHost()), PORT1, port2});

      vm1.invoke(InterestListDUnitTest.class, "createEntriesK1andK2");
      vm2.invoke(InterestListDUnitTest.class, "createEntriesK1andK2");

      // interest registration from clients should cause listeners to be invoked
      // in both servers
      LogWriterUtils.getLogWriter().info("test phase 1");
      vm1.invoke(InterestListDUnitTest.class, "registerKey",
          new Object[] { key1 });
      vm2.invoke(InterestListDUnitTest.class, "registerKey",
          new Object[] { key2 });

      Integer zero = new Integer(0);
      Integer two = new Integer(2);

      interestListener.verifyCountsAndClear(2, 0);
      vm0.invoke(InterestListDUnitTest.class, "verifyCountsAndClear",
          new Object[]{ two, zero });

      // unregistration from clients should invoke listeners on both servers
      LogWriterUtils.getLogWriter().info("test phase 2");
      vm1.invoke(InterestListDUnitTest.class, "unregisterKey",
          new Object[] { key1 });
      vm2.invoke(InterestListDUnitTest.class, "unregisterKey",
          new Object[] { key2 });

      interestListener.verifyCountsAndClear(0, 2);
      vm0.invoke(InterestListDUnitTest.class, "verifyCountsAndClear",
          new Object[]{ zero, two });

      // now the primary server for eache client will register and unregister
      LogWriterUtils.getLogWriter().info("test phase 3");
      registerKeyForClient(c1, key1);
      vm0.invoke(InterestListDUnitTest.class, "registerKeyForClient",
          new Object[] { c1, key1 });
      registerKeyForClient(c2, key2);
      vm0.invoke(InterestListDUnitTest.class, "registerKeyForClient",
          new Object[] { c2, key2 });

      interestListener.verifyCountsAndClear(2, 0);
      vm0.invoke(InterestListDUnitTest.class, "verifyCountsAndClear",
          new Object[]{ two, zero });

      LogWriterUtils.getLogWriter().info("test phase 4");
      unregisterKeyForClient(c1, key1);
      vm0.invoke(InterestListDUnitTest.class, "unregisterKeyForClient",
          new Object[] { c1, key1 });
      unregisterKeyForClient(c2, key2);
      vm0.invoke(InterestListDUnitTest.class, "unregisterKeyForClient",
          new Object[] { c2, key2 });

      interestListener.verifyCountsAndClear(0, 2);
      vm0.invoke(InterestListDUnitTest.class, "verifyCountsAndClear",
          new Object[]{ zero, two });
    }

  /**
   * This tests whether an exception is thrown in register/unregister when no
   * server is available.
   */
  public void testNoAvailableServer() {

    // Register interest in key1.
    vm1.invoke(InterestListDUnitTest.class, "createClientCache",
        new Object[] { NetworkUtils.getServerHostName(vm0.getHost()), new Integer(PORT1) });
    vm1.invoke(InterestListDUnitTest.class, "registerKey",
        new Object[] { key1 });

    // Stop the server.
    vm0.invoke(InterestListDUnitTest.class, "closeCache");

    // Try to unregister interest in key1 -- should not throw an exception.
    vm1.invoke(InterestListDUnitTest.class, "unregisterKeyEx",
        new Object[] { key1 });

    // Now try registration of interest in key2 -- should throw an exception.
    vm1.invoke(InterestListDUnitTest.class, "registerKeyEx",
        new Object[] { key2 });
  }

  public void testRegisterInterestOnReplicatedRegionWithCacheLoader() {
    runRegisterInterestWithCacheLoaderTest(true);
  }

  public void testRegisterInterestOnPartitionedRegionWithCacheLoader() {
    runRegisterInterestWithCacheLoaderTest(false);
  }

  private void runRegisterInterestWithCacheLoaderTest(boolean addReplicatedRegion) {
    // Stop the server (since it was already started with a replicated region)
    vm0.invoke(InterestListDUnitTest.class, "closeCache");

    // Start two servers with the appropriate region
    int port1 = ((Integer) vm0.invoke(InterestListDUnitTest.class, "createServerCache", new Object[] { addReplicatedRegion })).intValue();
    VM server2VM = Host.getHost(0).getVM(3);
    int port2 =  ((Integer) server2VM.invoke(InterestListDUnitTest.class, "createServerCache", new Object[] { addReplicatedRegion })).intValue();

    // Add a cache loader to the region in both cache servers
    vm0.invoke(InterestListDUnitTest.class, "addCacheLoader");
    server2VM.invoke(InterestListDUnitTest.class, "addCacheLoader");

    // Create client cache
    vm1.invoke(InterestListDUnitTest.class, "createClientCache",new Object[] {
      NetworkUtils.getServerHostName(vm0.getHost()), port1, port2});
    
    // Register interest in all keys
    vm1.invoke(InterestListDUnitTest.class, "registerALL_KEYS");
    
    // Add CacheListener
    int numEvents = 100;
    vm1.invoke(InterestListDUnitTest.class, "addCacheListener", new Object[] {numEvents});
    
    // Do gets on the client
    vm1.invoke(InterestListDUnitTest.class, "doGets", new Object[] {numEvents});
    
    // Wait for cache listener create events
    vm1.invoke(InterestListDUnitTest.class, "waitForCacheListenerCreates");
    
    // Confirm there are no cache listener update events
    vm1.invoke(InterestListDUnitTest.class, "confirmNoCacheListenerUpdates");

    // Confirm there are no cache listener invalidate events
    vm1.invoke(InterestListDUnitTest.class, "confirmNoCacheListenerInvalidates");
  }
  
  private  void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static DistributedMember createClientCache(String host, int port) throws Exception {
    return createClientCache(host, port, 0);
  }

  public static DistributedMember createClientCache(String host,
      int port, int port2) throws Exception
  {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    props.setProperty(DistributionConfig.DELTA_PROPAGATION_PROP_NAME, "false");

    new InterestListDUnitTest("temp").createCache(props);
    PoolFactory pfactory = PoolManager.createFactory()
      .addServer(host, port)
      .setThreadLocalConnections(true)
      .setMinConnections(3)
      .setSubscriptionEnabled(true)
      .setSubscriptionRedundancy(-1)
      .setReadTimeout(10000)
      .setSocketBufferSize(32768);
      // .setRetryInterval(10000)
      // .setRetryAttempts(5)
    if (port2 > 0) {
      pfactory.addServer(host, port2);
    }
    Pool p = pfactory.create("InterestListDUnitTestPool");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);

    return cache.getDistributedSystem().getDistributedMember();
  }

  private static void createCache() throws Exception {
    createCache(true);
  }

  private static void createCache(boolean addReplicatedRegion) throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.DELTA_PROPAGATION_PROP_NAME, "false");
    new InterestListDUnitTest("temp").createCache(props);
    if (addReplicatedRegion) {
      addReplicatedRegion();
    } else {
      addPartitionedRegion();
    }
  }

  private static void addReplicatedRegion() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableSubscriptionConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
  }

  private static void addPartitionedRegion() {
    cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).create(REGION_NAME);
  }

  private static CacheServer addCacheServer() throws Exception {
    CacheServer s = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET) ;
    s.setPort(port);
    s.start();
    return s;
  }

  // this method is for use in vm0 where the CacheServer used by
  // most of these tests resides.  This server is held in the
  // static variable 'server1'
  public static Integer createServerCache() throws Exception {
    return createServerCache(true);
  }

  public static Integer createServerCache(boolean addReplicatedRegion) throws Exception {
    createCache(addReplicatedRegion);
    server = addCacheServer();
    return new Integer(server.getPort());
  }

  /** wait for queues to drain in the server */
  public static void flushQueues() throws Exception {
    CacheServerImpl impl = (CacheServerImpl)server;
    for (CacheClientProxy proxy: (Set<CacheClientProxy>)impl.getAllClientSessions()) {
      final CacheClientProxy fproxy = proxy;
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return fproxy.getHARegionQueue().size() == 0;
        }
        public String description() {
          return "waiting for queues to drain for " + fproxy.getProxyID();
        }
      };
      Wait.waitForCriterion(ev, 5 * 10 * 1000, 200, true);
    }
  }

  public static void addRegisterInterestListener() {
    interestListener = new InterestListener();
    List<CacheServer> servers = cache.getCacheServers();
    for (CacheServer s: servers) {
      s.registerInterestRegistrationListener(interestListener);
    }
  }

  private static void addCacheLoader() {
    // Add a cache loader
    Region region = cache.getRegion(REGION_NAME);
    region.getAttributesMutator().setCacheLoader(new ReturnKeyCacheLoader());
  }

  private static void addCacheListener(int expectedCreates) {
    // Add a cache listener to count the number of create and update events
    Region region = cache.getRegion(REGION_NAME);
    region.getAttributesMutator().setCacheListener(new EventCountingCacheListener(expectedCreates));
  }

  private static void doGets(int numGets) {
    // Do gets.
    // These gets cause the CacheLoader to be invoked on the server
    Region region = cache.getRegion(REGION_NAME);
    for (int i=0; i<numGets; i++) {
      region.get(i);
    }
  }
  
  private static void waitForCacheListenerCreates() throws Exception {
    // Wait for the EventCountingCacheListener to receive all of its create events
    Region region = cache.getRegion(REGION_NAME);
    final EventCountingCacheListener fCacheListener = (EventCountingCacheListener) region.getAttributes().getCacheListener();
  
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return fCacheListener.hasReceivedAllCreateEvents();
      }
      public String description() {
        return "waiting for " + fCacheListener.getExpectedCreates() + " create events";
      }
    };
    Wait.waitForCriterion(ev, 5 * 10 * 1000, 200, true);
  }
  
  private static void confirmNoCacheListenerUpdates() throws Exception {
    // Confirm there are no EventCountingCacheListener update events.
    // These would be coming from the subscription channel.
    Region region = cache.getRegion(REGION_NAME);
    EventCountingCacheListener cacheListener = (EventCountingCacheListener) region.getAttributes().getCacheListener();
    assertEquals(0/*expected*/, cacheListener.getUpdates()/*actual*/);
  }
  
  private static void confirmNoCacheListenerInvalidates() throws Exception {
    // Confirm there are no EventCountingCacheListener invalidate events.
    // These would be coming from the subscription channel.
    Region region = cache.getRegion(REGION_NAME);
    EventCountingCacheListener cacheListener = (EventCountingCacheListener) region.getAttributes().getCacheListener();
    assertEquals(0/*expected*/, cacheListener.getInvalidates()/*actual*/);
  }

  public static void verifyCountsAndClear(int count1, int count2) {
    interestListener.verifyCountsAndClear(count1, count2);
  }

  public static void createEntriesK1andK2()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      if (!r.containsKey(key1)) {
        r.create(key1, key1_originalValue);
      }
      if (!r.containsKey(key2)) {
        r.create(key2, key2_originalValue);
      }
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry(key1).getValue(), key1_originalValue);
      assertEquals(r.getEntry(key2).getValue(), key2_originalValue);
    }
    catch (Exception ex) {
      Assert.fail("failed while createEntries()", ex);
    }
  }

  private static void registerKeyOnly(Object key) {

    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r);
    r.registerInterest(key);
  }

  public static void registerKey(Object key)
  {
    try {
      registerKeyOnly(key);
    }
    catch (Exception ex) {
      Assert.fail("failed while registering key(" + key + ")", ex);
    }
  }

  /**
   * exercises the ClientSession interface to register interest in a server
   * on behalf of a client
   * @param clientId the DM of the client
   * @param key the key that the client is interested in
   */
  public static void registerKeyForClient(DistributedMember clientId, Object key) {
    try {
      ClientSession cs = server.getClientSession(clientId);
      if (cs.isPrimary()) {
        cs.registerInterest(Region.SEPARATOR + REGION_NAME, key,
          InterestResultPolicy.KEYS_VALUES, false);
      }
    } catch (Exception ex) {
      Assert.fail("failed while registering key(" + key + ")", ex);
    }
  }

  public static void registerKeyEx(Object key) {

    try {
      registerKeyOnly(key);
      fail("Expected an exception during register interest with no available servers.");
    }
    catch (NoSubscriptionServersAvailableException ex) {
      // expected an exception
      LogWriterUtils.getLogWriter().info("Got expected exception in registerKey: ");
    }
  }

  public static void registerALL_KEYS()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.registerInterest("ALL_KEYS");
    }
    catch (Exception ex) {
      Assert.fail("failed while registering ALL_KEYS", ex);
    }
  }

  public static void put_ALL_KEYS()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.put(key1, "vm1-key-1");
      r.put(key2, "vm1-key-2");
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry(key1).getValue(), "vm1-key-1");
      assertEquals(r.getEntry(key2).getValue(), "vm1-key-2");
    }
    catch (Exception ex) {
      Assert.fail("failed while put_ALL_KEY()", ex);
    }
  }

  public static void validate_ALL_KEYS()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      assertEquals(r.getEntry(key1).getValue(), "vm1-key-1");
      assertEquals(r.getEntry(key2).getValue(), "vm1-key-2");
    }
    catch (Exception ex) {
      Assert.fail("failed while validate_ALL_KEY", ex);
    }
  }

  public static void registerKeys()
  {
    List list = new ArrayList();
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      list.add("k1");
      list.add("k2");
      r.registerInterest(list);
    }
    catch (Exception ex) {
      Assert.fail("failed while registering keys" + list + "", ex);
    }
  }

  public static void registerKeysAgain()
  {
    List list = new ArrayList();
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      list.add("k3");
      list.add("k4");
      list.add("k5");
      r.registerInterest(list);
    }
    catch (Exception ex) {
      Assert.fail("failed while registering keys" + list + "", ex);
    }
  }

  private static void unregisterKeyOnly(Object key) {

    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r);
    r.unregisterInterest(key);
  }

  public static void unregisterKey(Object key)
  {
    try {
      unregisterKeyOnly(key);
    }
    catch (Exception ex) {
      Assert.fail("failed while un-registering key(" + key + ")", ex);
    }
  }

  /**
   * unregister a key for a particular client in the server
   * @param clientId the client's ID
   * @param key the key it's no longer interest in
   */
  public static void unregisterKeyForClient(DistributedMember clientId, Object key)
  {
    try {
      ClientSession cs = server.getClientSession(clientId);
      if (cs.isPrimary()) {
        cs.unregisterInterest(Region.SEPARATOR + REGION_NAME, key, false);
      }
    }
    catch (Exception ex) {
      Assert.fail("failed while un-registering key(" + key + ") for client " + clientId, ex);
    }
  }

  public static void unregisterKeyEx(Object key) {
    unregisterKeyOnly(key);
  }

  public static void validateRegionEntriesFromInterestListInVm1()
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r);
    Region.Entry k1, k2;
    k1 = r.getEntry("k1");
    k2 = r.getEntry("k2");
    assertNotNull(k1);
    assertNotNull(k2);
    assertEquals(k1.getValue(), "server1");
    assertEquals(k2.getValue(), "server2");
  }

  public static void validateRegionEntriesFromInterestListInVm2()
  {
    Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(r);
    assertEquals(r.getEntry("k3").getValue(), "server3");
    assertEquals(r.getEntry("k4").getValue(), "server4");
    assertEquals(r.getEntry("k5").getValue(), "server5");
  }

  public static void putSingleEntry(Object key, String value) {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.put(key, value);
      // Verify that no invalidates occurred to this region
      assertEquals(r.getEntry(key).getValue(), value);
    }
    catch (Exception ex) {
      Assert.fail("failed while r.put()", ex);
    }
  }

  public static void put(String vm)
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      if (vm.equals("vm1")) {
        r.put(key1, "vm1-key-1");
        r.put(key2, "vm1-key-2");
        // Verify that no invalidates occurred to this region
        assertEquals("vm1-key-1", r.getEntry(key1).getValue());
        assertEquals("vm1-key-2", r.getEntry(key2).getValue());
      }
      else {
        r.put(key1, "vm2-key-1");
        r.put(key2, "vm2-key-2");
        // Verify that no invalidates occurred to this region
        assertEquals("vm2-key-1", r.getEntry(key1).getValue());
        assertEquals("vm2-key-2", r.getEntry(key2).getValue());
      }

    }
    catch (Exception ex) {
      Assert.fail("failed while r.put()", ex);
    }
  }

  public static void multiple_put()
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      r.put("k1", "server1");
      r.put("k2", "server2");
      r.put("k3", "server3");
      r.put("k4", "server4");
      r.put("k5", "server5");

    }
    catch (Exception ex) {
      Assert.fail("failed while r.put()", ex);
    }
  }

  public static void putAgain(String vm)
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      if (vm.equals("vm1")) {
        r.put(key1, "vm1-key-1-again");
        r.put(key2, "vm1-key-2-again");
        // Verify that no invalidates occurred to this region
        assertEquals(r.getEntry(key1).getValue(), "vm1-key-1-again");
        assertEquals(r.getEntry(key2).getValue(), "vm1-key-2-again");
      }
      else {
        r.put(key1, "vm2-key-1-again");
        r.put(key2, "vm2-key-2-again");
        // Verify that no invalidates occurred to this region
        assertEquals(r.getEntry(key1).getValue(), "vm2-key-1-again");
        assertEquals(r.getEntry(key2).getValue(), "vm2-key-2-again");
      }

    }
    catch (Exception ex) {
      Assert.fail("failed while r.putAgain()", ex);
    }
  }

  public static void validateEntriesK1andK2(final String vm)
  {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
          assertNotNull(r);
          if (vm.equals("vm1")) {
            // Verify that 'key-1' was updated, but 'key-2' was not and contains the
            // original value
            assertEquals("vm2-key-1", r.getEntry(key1).getValue());
            assertEquals("vm1-key-2", r.getEntry(key2).getValue());
          }
          else {
            // Verify that 'key-2' was updated, but 'key-1' was not and contains the
            // original value
            assertEquals(key1_originalValue, r.getEntry(key1).getValue());
            assertEquals("vm1-key-2", r.getEntry(key2).getValue());
          }
          return true;
        }
        catch (AssertionFailedError ex) {
          return false;
        }
      }
      public String description() {
        return "waiting for client to apply events from server";
      }
    };
    Wait.waitForCriterion(ev, 5 * 10 * 1000, 200, true);
  }

  public static void validateSingleEntry(Object key, String value) {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertEquals(value, r.getEntry(key).getValue());
    }
    catch (Exception ex) {
      Assert.fail("failed while validateSingleEntry()", ex);
    }
  }

  public static void validateEntriesAgain(String vm)
  {
    try {
      Region r = cache.getRegion(Region.SEPARATOR + REGION_NAME);
      assertNotNull(r);
      if (vm.equals("vm1")) {
        // Verify that neither 'key-1' 'key-2' was updated and contain the
        // original value
        assertEquals(r.getEntry(key1).getValue(), "vm1-key-1-again");
        assertEquals(r.getEntry(key2).getValue(), "vm1-key-2-again");
      }
      else {
        // Verify that no updates occurred to this region
        assertEquals(r.getEntry(key1).getValue(), "vm2-key-1");
        assertEquals(r.getEntry(key2).getValue(), "vm2-key-2");
      }
    }
    catch (Exception ex) {
      Assert.fail("failed while r.put()", ex);
    }
  }

  @Override
  protected final void preTearDown() throws Exception {
    // close the clients first
    vm1.invoke(InterestListDUnitTest.class, "closeCache");
    vm2.invoke(InterestListDUnitTest.class, "closeCache");
    // then close the servers
    vm0.invoke(InterestListDUnitTest.class, "closeCache");
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
  }

  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  static class InterestListener implements InterestRegistrationListener {
    private int registrationCount;
    private int unregistrationCount;

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.InterestRegistrationListener#afterRegisterInterest(com.gemstone.gemfire.cache.InterestRegistrationEvent)
     */
    public void afterRegisterInterest(InterestRegistrationEvent event) {
      LogWriterUtils.getLogWriter().info("InterestListener.afterRegisterInterest invoked with this event: " + event);
      registrationCount++;
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.InterestRegistrationListener#afterUnregisterInterest(com.gemstone.gemfire.cache.InterestRegistrationEvent)
     */
    public void afterUnregisterInterest(InterestRegistrationEvent event) {
      LogWriterUtils.getLogWriter().info("InterestListener.afterUnregisterInterest invoked with this event: " + event);
      unregistrationCount++;
    }

    /**
     * invoke this method after a predetermined number of operations have been performed
     *
     * @param expectedRegistrations expected count of interest registrations
     * @param expectedUnregistrations expected count of unregistrations
     */
    public void verifyCountsAndClear(int expectedRegistrations, int expectedUnregistrations) {
      assertEquals(expectedRegistrations, registrationCount);
      assertEquals(expectedUnregistrations, unregistrationCount);
      registrationCount = 0;
      unregistrationCount = 0;
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.CacheCallback#close()
     */
    public void close() {
    }

    public InterestListener() {
    }
  }
  
  private static class EventCountingCacheListener extends CacheListenerAdapter {

    private AtomicInteger creates = new AtomicInteger();

    private AtomicInteger updates = new AtomicInteger();

    private AtomicInteger invalidates = new AtomicInteger();

    private int expectedCreates;
    
    public EventCountingCacheListener(int expectedCreates) {
      this.expectedCreates = expectedCreates;
    }

    public int getExpectedCreates() {
      return this.expectedCreates;
    }

    public void afterCreate(EntryEvent event) {
      incrementCreates();
    }

    public void afterUpdate(EntryEvent event) {
      incrementUpdates();
      event.getRegion().getCache().getLogger().warning("Received update event " + getUpdates() + " for " + event.getKey());
    }

    public void afterInvalidate(EntryEvent event) {
      incrementInvalidates();
      event.getRegion().getCache().getLogger().warning("Received invalidate event " + getInvalidates() + " for " + event.getKey());
    }

    private void incrementCreates() {
      this.creates.incrementAndGet();
    }
    
    private int getCreates() {
      return this.creates.get();
    }

    private void incrementUpdates() {
      this.updates.incrementAndGet();
    }
    
    private int getUpdates() {
      return this.updates.get();
    }

    private void incrementInvalidates() {
      this.invalidates.incrementAndGet();
    }

    private int getInvalidates() {
      return this.invalidates.get();
    }

    public boolean hasReceivedAllCreateEvents() {
      return this.expectedCreates == getCreates();
    }
  }
  
  private static class ReturnKeyCacheLoader implements CacheLoader {

    @Override
    public void close() {
      // Do nothing
    }
  
    @Override
    public Object load(LoaderHelper helper) throws CacheLoaderException {
      return helper.getKey();
    }
  }
}
