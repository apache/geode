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
package com.gemstone.gemfire.cache.query.cq.dunit;

import java.util.HashSet;
import java.util.concurrent.CountDownLatch;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.cq.CqQueryImpl;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.CertifiableTestCacheListener;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This class tests the ContiunousQuery mechanism in GemFire.
 * This includes the test with different data activities.
 *
 * @author anil
 */
public class CqDataDUnitTest extends CacheTestCase {

  protected CqQueryDUnitTest cqDUnitTest = new CqQueryDUnitTest("CqDataDUnitTest");
  
  public CqDataDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    
    // avoid IllegalStateException from HandShake by connecting all vms tor
    // system before creating ConnectionPools
    getSystem();
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
    
  }
    
  /**
   * Tests with client acting as feeder/publisher and registering cq.
   * Added wrt bug 37161.
   * In case of InterestList the events are not sent back to the client
   * if its the originator, this is not true for cq.
   * 
   * @throws Exception
   */
  public void testClientWithFeederAndCQ() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server);

    final int port = server.invokeInt(CqQueryDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    // Create client.
    cqDUnitTest.createClient(client, port, host0);


    cqDUnitTest.createCQ(client, "testClientWithFeederAndCQ_0", cqDUnitTest.cqs[0]);
    cqDUnitTest.executeCQ(client, "testClientWithFeederAndCQ_0", false, null);

    final int size = 10;
    cqDUnitTest.createValues(client, cqDUnitTest.regions[0], size);
    cqDUnitTest.waitForCreated(client, "testClientWithFeederAndCQ_0", CqQueryDUnitTest.KEY+size);

    cqDUnitTest.validateCQ(client, "testClientWithFeederAndCQ_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
    
  }

  /**
   * Test for CQ Fail over/HA with redundancy level set.
   * @throws Exception
   */
  public void testCQHAWithState() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    
    VM client = host.getVM(3);
    
    //Killing servers can cause this message on the client side.
    addExpectedException("Could not find any server");
    cqDUnitTest.createServer(server1);
    
    final int port1 = server1.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(server1.getHost());
    
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    
    cqDUnitTest.createServer(server2, ports[0]);
    final int port2 = server2.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
            
    // Create client - With 3 server endpoints and redundancy level set to 2.
    
    // Create client with redundancyLevel 1
    cqDUnitTest.createClient(client, new int[] {port1, port2, ports[1]}, host0, "1");
    
    // Create CQs.
    int numCQs = 1;
    for (int i=0; i < numCQs; i++) {
      // Create CQs.
      cqDUnitTest.createCQ(client, "testCQHAWithState_" + i, cqDUnitTest.cqs[i]);
      cqDUnitTest.executeCQ(client, "testCQHAWithState_" + i, false, null);
    }
    
    pause(1 * 1000);
    
    int size = 10;
    
    // CREATE.
    cqDUnitTest.createValues(server1, cqDUnitTest.regions[0], size);
    cqDUnitTest.createValues(server1, cqDUnitTest.regions[1], size);
    
    for (int i=1; i <= size; i++) {
      cqDUnitTest.waitForCreated(client, "testCQHAWithState_0", CqQueryDUnitTest.KEY + i);
    }
    
    // Clients expected initial result.
    int[] resultsCnt = new int[] {10, 1, 2};

    for (int i=0; i < numCQs; i++) {
      cqDUnitTest.validateCQ(client, "testCQHAWithState_" + i, CqQueryDUnitTest.noTest, resultsCnt[i], 0, 0);
    }    

    // Close server1.
    // To maintain the redundancy; it will make connection to endpoint-3.
    cqDUnitTest.closeServer(server1);
    pause(3 * 1000);
    
    
    // UPDATE-1.
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[0], 10);
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[1], 10);
    
    for (int i=1; i <= size; i++) {
      cqDUnitTest.waitForUpdated(client, "testCQHAWithState_0", CqQueryDUnitTest.KEY + size);
    }
    
    for (int i=0; i < numCQs; i++) {
      cqDUnitTest.validateCQ(client, "testCQHAWithState_" + i, CqQueryDUnitTest.noTest, resultsCnt[i], resultsCnt[i], CqQueryDUnitTest.noTest);
    }    

    //Stop cq.
    cqDUnitTest.stopCQ(client, "testCQHAWithState_0");
    
    pause(2 * 1000);
    
    // UPDATE with stop.
    cqDUnitTest.createServer(server3, ports[1]);
    server3.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
    pause(2 * 1000);
    
    cqDUnitTest.clearCQListenerEvents(client, "testCQHAWithState_0");
    
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[0], 10);    
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[1], 10);
    
    // Wait for events at client.
    try {
      cqDUnitTest.waitForUpdated(client, "testCQHAWithState_0", CqQueryDUnitTest.KEY + 1);
      fail("Events not expected since CQ is in stop state.");
    } catch (Exception ex) {
      // Success.
    }
    
    cqDUnitTest.executeCQ(client, "testCQHAWithState_0", false, null);
    pause(2 * 1000);
    
    // Update - 2 
    cqDUnitTest.createValues(server3, cqDUnitTest.regions[0], 10);    
    cqDUnitTest.createValues(server3, cqDUnitTest.regions[1], 10);
    
    for (int i=1; i <= size; i++) {
      cqDUnitTest.waitForUpdated(client, "testCQHAWithState_0", CqQueryDUnitTest.KEY + size);
    }

    for (int i=0; i < numCQs; i++) {
      cqDUnitTest.validateCQ(client, "testCQHAWithState_" + i, CqQueryDUnitTest.noTest, resultsCnt[i], resultsCnt[i] * 2, CqQueryDUnitTest.noTest);
    }    
    
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server2);
    cqDUnitTest.closeServer(server3);
  }

  
  
  /**
   * Tests propogation of invalidates and destorys to the clients. Bug 37242.
   * 
   * @throws Exception
   */
  public void testCQWithDestroysAndInvalidates() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    VM producer = host.getVM(2);
    cqDUnitTest.createServer(server, 0, true);
    final int port = server.invokeInt(CqQueryDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    // Create client.
    cqDUnitTest.createClient(client, port, host0);
    // producer is not doing any thing.
    cqDUnitTest.createClient(producer, port, host0);

    final int size = 10;
    final String name = "testQuery_4";
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);

    cqDUnitTest.createCQ(client, name, cqDUnitTest.cqs[4]);
    cqDUnitTest.executeCQ(client, name, true, null);
    
    // do destroys and invalidates.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException
      {
        Region region1 = getRootRegion().getSubregion(cqDUnitTest.regions[0]);
        for (int i = 1; i <= 5; i++) {
          region1.destroy( CqQueryDUnitTest.KEY + i);
        }
      }
    });
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForDestroyed(client, name , CqQueryDUnitTest.KEY+i);
    }
    // recreate the key values from 1 - 5
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 5);
    // wait for all creates to arrive.
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForCreated(client, name , CqQueryDUnitTest.KEY+i);
    }
    
    // do more puts to push first five key-value to disk.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 10);
    // do invalidates on fisrt five keys.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException
      {
        Region region1 = getRootRegion().getSubregion(cqDUnitTest.regions[0]);
        for (int i = 1; i <= 5; i++) {
          region1.invalidate( CqQueryDUnitTest.KEY + i);
        }
      }
    });
    // wait for invalidates now.
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForInvalidated(client, name , CqQueryDUnitTest.KEY+i);
    }
        
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  
  }
  
  /**
   * Tests make sure that the second client doesnt get more
   * events then there should be. This will test the fix for 
   * bug 37295.
   * 
   * @author rdubey
   */
  public void testCQWithMultipleClients() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);
    VM client3 = host.getVM(3);
    
    /* Create Server and Client */
    cqDUnitTest.createServer(server);
    final int port = server.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());
    cqDUnitTest.createClient(client1, port, host0);
    cqDUnitTest.createClient(client2, port, host0);
    
    /* Create CQs. and initialize the region */
    // this should statisfy every thing since id is always greater than 
    // zero.
    cqDUnitTest.createCQ(client1, "testCQWithMultipleClients_0", cqDUnitTest.cqs[0]);
    cqDUnitTest.executeCQ(client1, "testCQWithMultipleClients_0", false, null);
    // should only satisfy one key-value pair in the region.
    cqDUnitTest.createCQ(client2, "testCQWithMultipleClients_0", cqDUnitTest.cqs[1]);
    cqDUnitTest.executeCQ(client2, "testCQWithMultipleClients_0", false, null);
    
    int size = 10;
    
    // Create Values on Server.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);
    
    
    cqDUnitTest.waitForCreated(client1, "testCQWithMultipleClients_0", CqQueryDUnitTest.KEY + 10);
    
    
    /* Validate the CQs */
    cqDUnitTest.validateCQ(client1, "testCQWithMultipleClients_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    
    cqDUnitTest.waitForCreated(client2, "testCQWithMultipleClients_0", CqQueryDUnitTest.KEY + 2 );
    
    
    cqDUnitTest.validateCQ(client2, "testCQWithMultipleClients_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ 1,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ 1,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ 1);
         
        
    /* Close Server and Client */
    cqDUnitTest.closeClient(client2);
    cqDUnitTest.closeClient(client3);
    cqDUnitTest.closeServer(server);
  }

  /**
   * Test for CQ when region is populated with net load.
   * @throws Exception
   */
  public void testCQWithLoad() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    
    VM client = host.getVM(2);
    
    cqDUnitTest.createServer(server1, 0, false, MirrorType.KEYS_VALUES);
    cqDUnitTest.createServer(server2, 0, false, MirrorType.KEYS);
        
    final int port1 = server1.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(server1.getHost());
      
    cqDUnitTest.createClient(client, port1, host0);
    
    // Create CQs.
    cqDUnitTest.createCQ(client, "testCQWithLoad_0", cqDUnitTest.cqs[0]);  
    cqDUnitTest.executeCQ(client, "testCQWithLoad_0", false, null); 
    
    pause(2 * 1000);
    
    final int size = 10;
    
    // CREATE VALUES.
    cqDUnitTest.createValues(server2, cqDUnitTest.regions[0], size);
    
    server1.invoke(new CacheSerializableRunnable("Load from second server") {
      public void run2() throws CacheException {
        Region region1 = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i=1; i <= size; i++){
          region1.get(CqQueryDUnitTest.KEY + i);
        }
      }
    });
    
    for (int i=1; i <= size; i++) {
      cqDUnitTest.waitForCreated(client, "testCQWithLoad_0", CqQueryDUnitTest.KEY + i);
    }
        
    cqDUnitTest.validateCQ(client, "testCQWithLoad_0", CqQueryDUnitTest.noTest, size, 0, 0);
        
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
    cqDUnitTest.closeServer(server2);
  }

  /**
   * Test for CQ when entries are evicted from region.
   * @throws Exception
   */
  public void testCQWithEviction() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM client = host.getVM(2);
    
    final int evictionThreshold = 1;
    server1.invoke(new CacheSerializableRunnable("Create Cache Server") {
      public void run2() throws CacheException {
        getLogWriter().info("### Create Cache Server. ###");
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        //factory.setMirrorType(MirrorType.NONE);
        // setting the eviction attributes.
        EvictionAttributes evictAttrs = EvictionAttributes.createLRUEntryAttributes(evictionThreshold, 
            EvictionAction.OVERFLOW_TO_DISK);
        factory.setEvictionAttributes(evictAttrs);
      
        for (int i = 0; i < cqDUnitTest.regions.length; i++) { 
          Region region = createRegion(cqDUnitTest.regions[i], factory.createRegionAttributes());
          // Set CacheListener.
          region.getAttributesMutator().setCacheListener(new CertifiableTestCacheListener(getLogWriter()));  
        } 
        pause(2000);
        
        try {
          cqDUnitTest.startBridgeServer(0, true);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }
        pause(2000);

      }
    });
        
    final int port1 = server1.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(server1.getHost());
      
    cqDUnitTest.createClient(client, port1, host0);
    
    // Create CQs.
    cqDUnitTest.createCQ(client, "testCQWithEviction_0", cqDUnitTest.cqs[0]);  
    
    final int size = 10;
    
    // CREATE VALUES.
    cqDUnitTest.createValues(server1, cqDUnitTest.regions[0], size);
    
    cqDUnitTest.executeCQ(client, "testCQWithEviction_0", false, "CqException"); 
    
    pause(1 * 1000);

    // Update VALUES.
    cqDUnitTest.createValues(server1, cqDUnitTest.regions[0], size);

    for (int i=1; i <= size; i++) {
      cqDUnitTest.waitForUpdated(client, "testCQWithEviction_0", cqDUnitTest.KEY + i);
    }

    cqDUnitTest.validateCQ(client, "testCQWithEviction_0", cqDUnitTest.noTest, 0, 10, 0);
        
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
  }

  /**
   * Test for CQ with ConnectionPool.
   * @throws Exception
   */
  public void testCQWithConnectionPool() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server1, 0, false, MirrorType.KEYS_VALUES);

    final int port1 = server1.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
    final String serverHost = getServerHostName(server1.getHost());

    final String[] regions = cqDUnitTest.regions;
    final int[] serverPorts = new int[] {port1};

    // createClientWithConnectionPool
    SerializableRunnable createClientWithPool =
      new CacheSerializableRunnable("createClientWithPool") {
      public void run2() throws CacheException {
        getLogWriter().info("### Create Client. ###");
        // Initialize CQ Service.
        try {
          getCache().getQueryService();
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to getCQService.");
        }

        AttributesFactory regionFactory = new AttributesFactory();
        regionFactory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(regionFactory, serverHost, serverPorts[0], -1, false, -1, -1, null);
        for (int i=0; i < regions.length; i++) {        
          createRegion(regions[i], regionFactory.create() );
          getLogWriter().info("### Successfully Created Region on Client :" + regions[i]);
        }
      }
    };

    client.invoke(createClientWithPool);

    // Create CQs.
    cqDUnitTest.createCQ(client, "testCQWithPool_0", cqDUnitTest.cqs[0]);  

    // This should fail as Region doesn't have ConnectionPool
    try {
      cqDUnitTest.executeCQ(client, "testCQWithPool_0", false,"CqException");
      fail("CQ Execution should have failed with BridgeClient/Writer not found.");
    } catch (Exception ex) {
      // Expected.
    }

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
  }
  
  /**
   * Test for CQ with BridgeClient.
   * @throws Exception
   */
  public void testCQWithBridgeClient() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server1, 0, false, MirrorType.KEYS_VALUES);

    final int port1 = server1.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
    final String serverHost = getServerHostName(server1.getHost());

    final String[] regions = cqDUnitTest.regions;
    final int[] serverPorts = new int[] {port1};

    // createClientWithBridgeClient
    SerializableRunnable createClientWithPool =
      new CacheSerializableRunnable("createClientWithPool") {
      public void run2() throws CacheException {
        getLogWriter().info("### Create Client. ###");
        //Region region1 = null;
        // Initialize CQ Service.
        try {
          getCache().getQueryService();
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to getCQService.");
        }

        AttributesFactory regionFactory = new AttributesFactory();
        regionFactory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(regionFactory, serverHost, serverPorts[0], -1, true, -1, -1, null);

        for (int i=0; i < regions.length; i++) {        
          createRegion(regions[i], regionFactory.createRegionAttributes());
          getLogWriter().info("### Successfully Created Region on Client :" + regions[i]);
        }
      }
    };

    client.invoke(createClientWithPool);

    // Create CQs.
    cqDUnitTest.createCQ(client, "testCQWithPool_1", cqDUnitTest.cqs[0]);  

    // This should pass.
    cqDUnitTest.executeCQ(client, "testCQWithPool_1", false,null);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
  }
  
  /**
   * Test for CQ with ConnectionPool.
   * @throws Exception
   */
  public void testCQWithPool() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server1, 0, false, MirrorType.KEYS_VALUES);

    final int port1 = server1.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
    final String serverHost = getServerHostName(server1.getHost());

    final String[] regions = cqDUnitTest.regions;
    final int[] serverPorts = new int[] {port1};

    // createClientWithConnectionPool
    SerializableRunnable createClientWithConnectionPool =
      new CacheSerializableRunnable("createClientWithConnectionPool") {
      public void run2() throws CacheException {
        getLogWriter().info("### Create Client. ###");
        //Region region1 = null;
        // Initialize CQ Service.
        try {
          getCache().getQueryService();
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to getCQService.");
        }

        AttributesFactory regionFactory = new AttributesFactory();
        regionFactory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(regionFactory, serverHost, serverPorts[0], -1, true, -1, -1, null);
        for (int i=0; i < regions.length; i++) {        
          createRegion(regions[i], regionFactory.createRegionAttributes());
          getLogWriter().info("### Successfully Created Region on Client :" + regions[i]);
        }
      }
    };

    client.invoke(createClientWithConnectionPool);

    // Create CQs.
    cqDUnitTest.createCQ(client, "testCQWithPool_2", cqDUnitTest.cqs[0]);  

    // This should pass.
    cqDUnitTest.executeCQ(client, "testCQWithPool_2", false, null);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
  }

  /**
   * Test for CQ with establishCallBackConnection.
   * @throws Exception
   */
  public void testCQWithEstablishCallBackConnection() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server1, 0, false, MirrorType.KEYS_VALUES);

    final int port1 = server1.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
    final String serverHost = getServerHostName(server1.getHost());

    final String[] regions = cqDUnitTest.regions;
    final int[] serverPorts = new int[] {port1};

    // createClientWithPool
    SerializableRunnable createClientWithPool =
      new CacheSerializableRunnable("createClientWithPool") {
      public void run2() throws CacheException {
        getLogWriter().info("### Create Client. ###");
        //Region region1 = null;
        // Initialize CQ Service.
        try {
          getCache().getQueryService();
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to getCQService.");
        }

        AttributesFactory regionFactory = new AttributesFactory();
        regionFactory.setScope(Scope.LOCAL);
        
        ClientServerTestCase.configureConnectionPool(regionFactory, serverHost, serverPorts[0], -1, false, -1, -1, null);

        for (int i=0; i < regions.length; i++) {        
          createRegion(regions[i], regionFactory.createRegionAttributes());
          getLogWriter().info("### Successfully Created Region on Client :" + regions[i]);
        }
      }
    };

    
    client.invoke(createClientWithPool);

    // Create CQs.
    cqDUnitTest.createCQ(client, "testCQWithEstablishCallBackConnection_0", cqDUnitTest.cqs[0]);  

    // This should fail.
    try {
      cqDUnitTest.executeCQ(client, "testCQWithEstablishCallBackConnection_0", false, "CqException");
      fail("Test should have failed with connection with establishCallBackConnection not found.");
    } catch (Exception ex) {
      // Expected.
    }

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
  }
  
  /**
   * Test for:
   * Region destroy, calls close on the server.
   * Region clear triggers cqEvent with query op region clear.
   * Region invalidate triggers cqEvent with query op region invalidate.
   * @throws Exception
   */
  public void testRegionEvents() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    cqDUnitTest.createServer(server);
    final int port = server.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());
    
    cqDUnitTest.createClient(client, port, host0);
    
    // Create CQ on regionA
    cqDUnitTest.createCQ(client, "testRegionEvents_0", cqDUnitTest.cqs[0]);
    cqDUnitTest.executeCQ(client, "testRegionEvents_0", false, null);
    
    // Create CQ on regionB
    cqDUnitTest.createCQ(client, "testRegionEvents_1", cqDUnitTest.cqs[2]);
    cqDUnitTest.executeCQ(client, "testRegionEvents_1", false, null);

    // Test for Event on Region Clear.
    server.invoke(new CacheSerializableRunnable("testRegionEvents"){
      public void run2()throws CacheException {
        getLogWriter().info("### Clearing the region on the server ###");
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <=5; i++) {
          region.put(CqQueryDUnitTest.KEY+i, new Portfolio(i));
        }
        region.clear();
      }
    });
    
    cqDUnitTest.waitForRegionClear(client,"testRegionEvents_0");

    // Test for Event on Region invalidate.
    server.invoke(new CacheSerializableRunnable("testRegionEvents"){
      public void run2()throws CacheException {
        getLogWriter().info("### Invalidate the region on the server ###");
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <=5; i++) {
          region.put(CqQueryDUnitTest.KEY+i, new Portfolio(i));
        }
        region.invalidateRegion();
      }
    });

    cqDUnitTest.waitForRegionInvalidate(client,"testRegionEvents_0");

    // Test for Event on Region destroy.
    server.invoke(new CacheSerializableRunnable("testRegionEvents"){
      public void run2()throws CacheException {
        getLogWriter().info("### Destroying the region on the server ###");
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[1]);
        for (int i = 1; i <=5; i++) {
          region.put(CqQueryDUnitTest.KEY+i, new Portfolio(i));
        }
        // this should close one cq on client.
        region.destroyRegion();
      }
    });

    pause(1000); // wait for cq to close becuse of region destroy on server.
    //cqDUnitTest.waitForClose(client,"testRegionEvents_1");
    cqDUnitTest.validateCQCount(client,1);

        
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);

  }


  /**
   * Test for events created during the CQ query execution.
   * When CQs are executed using executeWithInitialResults 
   * there may be possibility that the region changes during
   * that time may not be reflected in the query result set
   * thus making the query data and region data inconsistent.
   * @throws Exception
   */
  public void testEventsDuringQueryExecution() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    final String cqName = "testEventsDuringQueryExecution_0";
    cqDUnitTest.createServer(server);
    final int port = server.invokeInt(CqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());
    
    // Initialize Client.
    cqDUnitTest.createClient(client, port, host0);
    
    // create CQ.
    cqDUnitTest.createCQ(client, cqName, cqDUnitTest.cqs[0]);
    
    final int numObjects = 200;
    final int totalObjects = 500;
    
    // initialize Region.
    server.invoke(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
      }
    });
    
    // Execute CQ while update is in progress.
    AsyncInvocation processCqs = client.invokeAsync(new CacheSerializableRunnable("Execute CQ") {
      public void run2()throws CacheException {
        QueryService cqService = getCache().getQueryService();
        // Get CqQuery object.
        CqQuery cq1 = cqService.getCq(cqName);
        if (cq1 == null) {
          fail("Failed to get CQ " + cqName);
        }
        
        SelectResults cqResults = null;

        try {
          cqResults = cq1.executeWithInitialResults();
        } catch (Exception ex){
          AssertionError err = new AssertionError("Failed to execute  CQ " + cqName);
          err.initCause(ex);
          throw err;
        }
        
        //getLogWriter().info("initial result size = " + cqResults.size());
        
        CqQueryTestListener cqListener = (CqQueryTestListener)cq1.getCqAttributes().getCqListener();
        // Wait for the last key to arrive.
        cqListener.waitForCreated("" + totalObjects);
        
        // Check if the events from CqListener are in order.
        int oldId = 0;
        for (Object cqEvent : cqListener.events.toArray()) { 
          int newId = new Integer(cqEvent.toString()).intValue();
          if (oldId > newId){
            fail("Queued events for CQ Listener during execution with " + 
                "Initial results is not in the order in which they are created.");
          }
          oldId = newId;
        }
        
        // Check if all the IDs are present as part of Select Results and CQ Events.
        HashSet ids = new HashSet(cqListener.events);
        for (Object o : cqResults.asList()) {
          Struct s = (Struct)o;
          ids.add(s.get("key"));
        }

        //Iterator iter = cqResults.asSet().iterator();
        //while (iter.hasNext()) {
        //  Portfolio p = (Portfolio)iter.next();
        //  ids.add(p.getPk());
        //  //getLogWriter().info("Result set value : " + p.getPk());
        //}
        
        HashSet missingIds = new HashSet();
        String key = "";
        for (int i = 1; i <= totalObjects; i++) {
          key = "" + i;
          if (!(ids.contains(key))){
            missingIds.add(key);
          }
        }
        
        if (!missingIds.isEmpty()) {
          fail("Missing Keys in either ResultSet or the Cq Event list. " +
              " Missing keys : [size : " + missingIds.size() + "]" + missingIds +
              " Ids in ResultSet and CQ Events :" + ids);
        }
        
      } 
    });
    
    // Keep updating region (async invocation).
    server.invokeAsync(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        //Wait to allow client a chance to register the cq
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = numObjects + 1; i <= totalObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
      }
    });

    //wait for 60 seconds for test to complete
    DistributedTestCase.join(processCqs, 60 * 1000, getLogWriter());
    
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }
  
  /**
   * This test was created to test executeWithInitialResults being called
   * multiple times. Previously, the queueEvents would be overwritten and we
   * would lose data. This test will execute the method twice. The first time,
   * the first execution will block it's own child thread (TC1). The second
   * execution will block until TC1 is completed (based on how
   * executeWithInitialResults is implemented) A third thread will be awaken and
   * release the latch in the testhook for TC1 to complete.
   * 
   * @throws Exception
   */
  public void testMultipleExecuteWithInitialResults() throws Exception {
    final int numObjects = 200;
    final int totalObjects = 500;
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    client.invoke(setTestHook());
    final String cqName = "testMultiExecuteWithInitialResults";

    // initialize server and retreive host and port values
    cqDUnitTest.createServer(server);
    final int port = server.invokeInt(CqQueryDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    // Initialize Client.
    cqDUnitTest.createClient(client, port, host0);

    // create CQ.
    cqDUnitTest.createCQ(client, cqName, cqDUnitTest.cqs[0]);

    // initialize Region.
    server.invoke(new CacheSerializableRunnable("Update Region") {
      public void run2() throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put("" + i, p);
        }
      }
    });

    // Keep updating region (async invocation).
    server.invokeAsync(new CacheSerializableRunnable("Update Region") {
      public void run2() throws CacheException {
        //Wait to give client a chance to register the cq
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = numObjects + 1; i <= totalObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put("" + i, p);
        }
      }
    });
    
    // the thread that validates all results and executes first
    AsyncInvocation processCqs =client.invokeAsync(new CacheSerializableRunnable("Execute CQ first") {
      public void run2() throws CacheException {
        SelectResults cqResults = null;
        QueryService cqService = getCache().getQueryService();
        // Get CqQuery object.
        CqQuery cq1 = cqService.getCq(cqName);
        if (cq1 == null) {
          fail("Failed to get CQ " + cqName);
        }
        try {
          cqResults = cq1.executeWithInitialResults();

        } catch (Exception e) {
          AssertionError err = new AssertionError("Failed to execute  CQ "
              + cqName);
          err.initCause(e);
          throw err;
        }

        CqQueryTestListener cqListener = (CqQueryTestListener) cq1
            .getCqAttributes().getCqListener();
        // Wait for the last key to arrive.
        cqListener.waitForCreated("" + totalObjects);
        // Check if the events from CqListener are in order.
        int oldId = 0;
        for (Object cqEvent : cqListener.events.toArray()) {
          int newId = new Integer(cqEvent.toString()).intValue();
          if (oldId > newId) {
            fail("Queued events for CQ Listener during execution with "
                + "Initial results is not in the order in which they are created.");
          }
          oldId = newId;
        }

        // Check if all the IDs are present as part of Select Results and CQ
        // Events.
        HashSet ids = new HashSet(cqListener.events);
        for (Object o : cqResults.asList()) {
          Struct s = (Struct) o;
          ids.add(s.get("key"));
        }

        HashSet missingIds = new HashSet();
        String key = "";
        for (int i = 1; i <= totalObjects; i++) {
          key = "" + i;
          if (!(ids.contains(key))) {
            missingIds.add(key);
          }
        }

        if (!missingIds.isEmpty()) {
          fail("Missing Keys in either ResultSet or the Cq Event list. "
              + " Missing keys : [size : " + missingIds.size() + "]"
              + missingIds + " Ids in ResultSet and CQ Events :" + ids);
        }
      }
    });

    // the second call to executeWithInitialResults. Goes to sleep hopefully
    // long enough
    // for the first call to executeWithInitialResults first
    client.invokeAsync(new CacheSerializableRunnable("Execute CQ second") {
      public void run2() throws CacheException {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        QueryService cqService = getCache().getQueryService();
        // Get CqQuery object.
        CqQuery cq1 = cqService.getCq(cqName);
        if (cq1 == null) {
          fail("Failed to get CQ " + cqName);
        }
        try {
          cq1.executeWithInitialResults();
        } catch (IllegalStateException e) {
          // we expect an error due to the cq having already being in run state
        } catch (Exception e) {
          AssertionError err = new AssertionError("test hook lock interrupted"
              + cqName);
          err.initCause(e);
          throw err;
        }
      }
    });

    // thread that unlatches the test hook, sleeping long enough for both
    // the other two threads to execute first
    client.invokeAsync(new CacheSerializableRunnable("Release latch") {
      public void run2() throws CacheException {
        // we wait to release the testHook and hope the other two threads have
        // had a chance to invoke executeWithInitialResults
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          AssertionError err = new AssertionError("test hook lock interrupted"
              + cqName);
          err.initCause(e);
          throw err;
        }
        CqQueryImpl.testHook.ready();
      }
    });

    //wait for 60 seconds for test to complete
    DistributedTestCase.join(processCqs, 60 * 1000, getLogWriter());
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  public CacheSerializableRunnable setTestHook() {
    SerializableRunnable sr = new CacheSerializableRunnable("TestHook") {
      public void run2() {
        class CqQueryTestHook implements CqQueryImpl.TestHook {

          CountDownLatch latch = new CountDownLatch(1);

          public void pauseUntilReady() {
            try {
              latch.await();
            } catch (Exception e) {
              e.printStackTrace();
              Thread.currentThread().interrupt();
            }
          }

          public void ready() {
            latch.countDown();
          }

          @Override
          public int numQueuedEvents() {
            // TODO Auto-generated method stub
            return 0;
          }

          @Override
          public void setEventCount(int count) {
            // TODO Auto-generated method stub
            
          }

        }
        ;
        CqQueryImpl.testHook = new CqQueryTestHook();
      }
    };
    return (CacheSerializableRunnable) sr;
  }

}
