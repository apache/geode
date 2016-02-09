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

import hydra.Log;

import java.io.IOException;
import java.util.HashSet;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
/**
 * Test class for Partitioned Region and CQs
 * 
 * @author rdubey
 * @since 5.5
 */
public class PartitionedRegionCqQueryDUnitTest extends CacheTestCase {

  
  public PartitionedRegionCqQueryDUnitTest(String name) {
    super(name);
  }
  
  static public final String[] regions = new String[] {
      "regionA",
      "regionB"
  };
  
  static public final String KEY = "key-";
  
  protected final CqQueryDUnitTest cqHelper = new CqQueryDUnitTest("HelperPartitonedRegionCqTest");
  
  public final String[] cqs = new String [] {
      //0 - Test for ">" 
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.ID > 0",
      
      //1 -  Test for "=" and "and".
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.ID = 2 and p.status='active'",
      
      //2 -  Test for "<" and "and".
      "SELECT ALL * FROM /root/" + regions[1] + " p where p.ID < 5 and p.status='active'",
      
      // FOLLOWING CQS ARE NOT TESTED WITH VALUES; THEY ARE USED TO TEST PARSING LOGIC WITHIN CQ.
      //3
      "SELECT * FROM /root/" + regions[0] + " ;",
      //4
      "SELECT ALL * FROM /root/" + regions[0],
      //5
      "import com.gemstone.gemfire.cache.\"query\".data.Portfolio; " +
      "SELECT ALL * FROM /root/" + regions[0] +  " TYPE Portfolio",
      //6
      "import com.gemstone.gemfire.cache.\"query\".data.Portfolio; " +
      "SELECT ALL * FROM /root/" + regions[0] +  " p TYPE Portfolio",
      //7
      "SELECT ALL * FROM /root/" + regions[1] + " p where p.ID < 5 and p.status='active';",
      //8
      "SELECT ALL * FROM /root/" + regions[0] + "  ;",
      //9
      "SELECT ALL * FROM /root/" + regions[0] +" p where p.description = NULL",
      
      // 10
      "SELECT ALL * FROM /root/" + regions[1] +" p where p.ID > 0",
  };
  
  public final String[] cqsWithoutRoot = new String [] {
      //0 - Test for ">" 
      "SELECT ALL * FROM /" + regions[0] + " p where p.ID > 0"
      
  };
  
  private static int bridgeServerPort;
  
  public void testCQLeakWithPartitionedRegion() throws Exception {
    // creating servers.
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    createServer(server1);       
    createServer(server2);    
       
    
    // create client 
    
    final int port = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    createClient(client, port, host0);
    
    // register cq.
    createCQ(client, "testCQEvents_0", cqs[0]);
    cqHelper.executeCQ(client, "testCQEvents_0", false, null);
    Wait.pause(2 * 1000);
    
    // create values
    int size = 40;
    createValues(server1, regions[0], size);
    
    // wait for last creates...
    
    cqHelper.waitForCreated(client, "testCQEvents_0", KEY+size);
    
    // validate cq..
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    
    int cc1 = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCqCountFromRegionProfile");
    int cc2 = server2.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCqCountFromRegionProfile");
    assertEquals("Should have one", 1, cc1);
    assertEquals("Should have one", 1, cc2);
    
    server1.bounce();
    
    cqHelper.closeClient(client);
    Wait.pause(10 * 1000);
    //cc1 = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCqCountFromRegionProfile");
    cc2 = server2.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCqCountFromRegionProfile");
    
    //assertEquals("Should have one", 0, cc1);
    assertEquals("Should have one", 0, cc2);
    
    cqHelper.closeServer(server2);
    //cqHelper.closeServer(server1);
  }
  
  public void testCQAndPartitionedRegion() throws Exception {
    // creating servers.
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    createServer(server1);
    
    createServer(server2);
    
    // create client 
    
    final int port = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    createClient(client, port, host0);
    
    // register cq.
    createCQ(client, "testCQEvents_0", cqs[0]);
    cqHelper.executeCQ(client, "testCQEvents_0", false, null);
    Wait.pause(2 * 1000);
    
    // create values
    int size = 40;
    createValues(server1, regions[0], size);
    
    // wait for last creates...
    
    cqHelper.waitForCreated(client, "testCQEvents_0", KEY+size);
    
    // validate cq..
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    //size = 2;
    
    // do updates
    createValues(server1, regions[0], size);
    
    cqHelper.waitForUpdated(client, "testCQEvents_0", KEY+size);
    
    // validate cqs again
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ (size+size));
    
    
    // destroy all the values.
    int numDestroys = size;
    cqHelper.deleteValues(server2,regions[0], numDestroys);
    
    cqHelper.waitForDestroyed(client, "testCQEvents_0", KEY+numDestroys);
    
    // validate cqs after destroyes on server2.
 
        
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ numDestroys,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ numDestroys,
        /* totalEvents: */ (size+size+numDestroys));
    
    // invalidate some entries.
    /*final int numInvalidates = 5;
    
    server2.invoke(new CacheSerializableRunnable("Invalidate values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = numInvalidates; i <= (numInvalidates+4); i++) {
          region1.invalidate(KEY+i);
        }
      }
    });
     
    cqHelper.waitForInvalidated(client, "testCQEvents_0", KEY+(numInvalidates+4));
    */
   // cqHelper.validateCQ(client, "testCQEvents_0",
    //    /* resultSize: */ cqHelper.noTest,
    //    /* creates: */ size,
    //    /* updates: */ size,
    //    /* deletes; */ (numDestroys+numInvalidates),
    //    /* queryInserts: */ size,
     //   /* queryUpdates: */ size,
     //   /* queryDeletes: */ (numDestroys+numInvalidates),
    //    /* totalEvents: */ (size+size+numDestroys + numInvalidates));
    
    cqHelper.closeClient(client);
    cqHelper.closeServer(server2);
    cqHelper.closeServer(server1);
  }
  
  /**
   * test for registering cqs on a bridge server with local max memory zero.
   */
  public void testPartitionedCqOnAccessorBridgeServer() throws Exception {
 // creating servers.
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    // creating an accessor vm with Bridge Server installed.
    createServer(server1,true);
    
    createServer(server2);
    
    // create client 
    
    final int port = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    createClient(client, port, host0);
    
    // register cq.
    createCQ(client, "testCQEvents_0", cqs[0]);
    cqHelper.executeCQ(client, "testCQEvents_0", false, null);
    
    
    // create values
    final int size = 1000;
    createValues(server1, regions[0], size);
    
    // wait for last creates...

    for (int i=1; i <= size; i++){
      cqHelper.waitForCreated(client, "testCQEvents_0", KEY+i);
    }

    // validate cq..
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    //size = 2;
    
    // do updates
    createValues(server1, regions[0], size);
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForUpdated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cqs again.
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ (size+size));
    
    
    // destroy all the values.
    int numDestroys = size;
    cqHelper.deleteValues(server2,regions[0], numDestroys);
    
    for (int i=1; i <= numDestroys; i++){
      cqHelper.waitForDestroyed(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cqs after destroyes on server2.
 
        
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ numDestroys,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ numDestroys,
        /* totalEvents: */ (size+size+numDestroys));
    
    cqHelper.closeClient(client);
    cqHelper.closeServer(server2);
    cqHelper.closeServer(server1);
  }
  
  /**
   * test for registering cqs on single Bridge server hosting all the data. This
   * will generate all the events locally and should always have the old value 
   * and should not sent the profile update on wire.
   */
  public void testPartitionedCqOnSingleBridgeServer() throws Exception { 
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
//    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    // creating an accessor vm with Bridge Server installed.
    createServer(server1);
    final int port = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    createClient(client, port, host0);
    
    // register cq.
    createCQ(client, "testCQEvents_0", cqs[0]);
    cqHelper.executeCQ(client, "testCQEvents_0", false, null);
    
    
    // create values
    final int size = 400;
    createValues(server1, regions[0], size);
    
    // wait for last creates...
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForCreated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cq..
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    //size = 2;
    
    // do updates
    createValues(server1, regions[0], size);
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForUpdated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cqs again.
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ (size+size));
    
    
    // destroy all the values.
    int numDestroys = size;
    cqHelper.deleteValues(server1,regions[0], numDestroys);
    
    for (int i=1; i <= numDestroys; i++){
      cqHelper.waitForDestroyed(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cqs after destroyes on server2.
 
        
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ numDestroys,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ numDestroys,
        /* totalEvents: */ (size+size+numDestroys));
    
    cqHelper.closeClient(client);
    cqHelper.closeServer(server1);
    
  }
  
  /**
   * test for registering cqs on single Bridge server hosting all the data. This
   * will generate all the events locally but the puts, updates and destroys originate
   * at an accessor vm.
   */
  public void testPRCqOnSingleBridgeServerUpdatesOriginatingAtAccessor() throws Exception { 
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    // creating an accessor vm with Bridge Server installed.
    createServer(server1,true);
    
    assertLocalMaxMemory(server1);
    
    createServer(server2);
    
    // create client 
    
    final int port = server2.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server2.getHost());
    
    createClient(client, port, host0);
    
    // register cq.
    createCQ(client, "testCQEvents_0", cqs[0]);
    cqHelper.executeCQ(client, "testCQEvents_0", false, null);
    
    
    // create values
    final int size = 400;
    createValues(server1, regions[0], size);
    
    // wait for last creates...
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForCreated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cq..
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    //size = 2;
    
    // do updates
    createValues(server1, regions[0], size);
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForUpdated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cqs again.
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ (size+size));
    
    
    // destroy all the values.
    int numDestroys = size;
    cqHelper.deleteValues(server1,regions[0], numDestroys);
    
    for (int i=1; i <= numDestroys; i++){
      cqHelper.waitForDestroyed(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cqs after destroyes on server2.
 
        
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ numDestroys,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ numDestroys,
        /* totalEvents: */ (size+size+numDestroys));
    
    cqHelper.closeClient(client);
    cqHelper.closeServer(server2);
    cqHelper.closeServer(server1);
  }
  
  /**
   * test to check invalidates on bridge server hosting datastores as well.
   */
  public void testPRCqWithInvalidatesOnBridgeServer()  {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    // creating Bridge Server with data store. clients will connect to this 
    // bridge server.
    createServer(server1);
     
    // create another server with data store.
    createServer(server2);
    
    
    final int port = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    createClient(client, port, host0);
    
    // register cq.
    createCQ(client, "testCQEvents_0", cqs[0]);
    cqHelper.executeCQ(client, "testCQEvents_0", false, null);
    
    
    // create values
    final int size = 400;
    createValues(server1, regions[0], size);
    
    // wait for last creates...
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForCreated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cq..
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    //size = 2;
    
    // do updates
    createValues(server1, regions[0], size);
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForUpdated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cqs again.
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ (size+size));
    
    
    // invalidate all the values.
    int numInvalidates = size;
    cqHelper.invalidateValues(server2,regions[0], numInvalidates);
    
    for (int i=1; i <= numInvalidates; i++){
      cqHelper.waitForInvalidated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cqs after invalidates on server2.
         
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ numInvalidates,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ numInvalidates,
        /* totalEvents: */ (size+size+numInvalidates));
    
    cqHelper.closeClient(client);
    cqHelper.closeServer(server2);
    cqHelper.closeServer(server1);
  }
  
  /**
   * test cqs with invalidates on bridge server not hosting datastores.
   * 
   */
  public void testPRCqWithInvalidatesOnAccessorBridgeServer() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    // creating Bridge Server with data store. clients will connect to this 
    // bridge server.
    createServer(server1, true);
     
    // create another server with data store.
    createServer(server2);
    
    
    final int port = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    createClient(client, port, host0);
    
    // register cq.
    createCQ(client, "testCQEvents_0", cqs[0]);
    cqHelper.executeCQ(client, "testCQEvents_0", false, null);
    
    
    // create values
    final int size = 400;
    createValues(server1, regions[0], size);
    
    // wait for last creates...
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForCreated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cq..
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    //size = 2;
    
    // do updates
    createValues(server1, regions[0], size);
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForUpdated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cqs again.
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ (size+size));
    
    
    // invalidate all the values.
    int numInvalidates = size;
    cqHelper.invalidateValues(server1,regions[0], numInvalidates);
    
    for (int i=1; i <= numInvalidates; i++){
      cqHelper.waitForInvalidated(client, "testCQEvents_0", KEY+i);
    } 
    // validate cqs after invalidates on server2.
         
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ numInvalidates,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ numInvalidates,
        /* totalEvents: */ (size+size+numInvalidates));
    
    cqHelper.closeClient(client);
    cqHelper.closeServer(server2);
    cqHelper.closeServer(server1);
  }
  
  /**
   * test cqs with create updates and destroys from client on bridge server
   * hosting datastores.
   */
  public void testPRCqWithUpdatesFromClients() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    VM client2 = host.getVM(3);
    
    // creating Bridge Server with data store. clients will connect to this 
    // bridge server.
    createServer(server1, false, 1);
     
    // create another server with data store.
    createServer(server2, false , 1);
    
    
    final int port = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    createClient(client, port, host0);
    createClient(client2, port, host0);
    
    // register cq.
    createCQ(client, "testCQEvents_0", cqs[0]);
    cqHelper.executeCQ(client, "testCQEvents_0", false, null);
    
    
    // create values
    final int size = 400;
    createValues(client2, regions[0], size);
    
    // wait for last creates...
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForCreated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cq..
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    //size = 2;
    
    // do updates
    createValues(client2, regions[0], size);
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForUpdated(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cqs again.
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ (size+size));
    
    
    // invalidate all the values.
    int numDelets = size;
    
    cqHelper.deleteValues(client2,regions[0], numDelets);
    
    for (int i=1; i <= numDelets; i++){
      cqHelper.waitForDestroyed(client, "testCQEvents_0", KEY+i);
    }
    
    // validate cqs after invalidates on server2.
         
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ numDelets,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ numDelets,
        /* totalEvents: */ (size+size+numDelets));
    
    cqHelper.closeClient(client);
    cqHelper.closeClient(client2);
    cqHelper.closeServer(server2);
    cqHelper.closeServer(server1);    
  }
  
  /**
   * test cqs on multiple partitioned region hosted by bridge servers.
   * 
   */
  public void testPRCqWithMultipleRegionsOnServer() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    VM client2 = host.getVM(3);
    
    // creating Bridge Server with data store. clients will connect to this 
    // bridge server.
    createServer(server1, false, 1);
     
    // create another server with data store.
    createServer(server2, false , 1);
    
    // Wait for server to initialize.
    Wait.pause(2000);
    
    final int port = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    createClient(client, port, host0);
    createClient(client2, port, host0);
    
    // register cq.
    createCQ(client, "testCQEvents_0", cqs[0]);
    createCQ(client, "testCQEvents_1", cqs[10]);
    cqHelper.executeCQ(client, "testCQEvents_0", false, null);
    cqHelper.executeCQ(client, "testCQEvents_1", false, null);
    
    // create values
    final int size = 400;
    createValues(client2, regions[0], size);
    createValues(client2, regions[1], size);
    
    // wait for last creates...
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForCreated(client, "testCQEvents_0", KEY+i);
      cqHelper.waitForCreated(client, "testCQEvents_1", KEY+i);
    }
    
    // validate cq..
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    cqHelper.validateCQ(client, "testCQEvents_1",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    
    //size = 2;
    
    // do updates
    createValues(client2, regions[0], size);
    createValues(client2, regions[1], size);
    
 
    for (int i=1; i <= size; i++){
      cqHelper.waitForUpdated(client, "testCQEvents_0", KEY+i);
      cqHelper.waitForUpdated(client, "testCQEvents_1", KEY+i);
    }
    
    // validate cqs again.
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ (size+size));
    
    cqHelper.validateCQ(client, "testCQEvents_1",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ (size+size));
    
    // invalidate all the values.
    int numInvalidates = size;
    //cqHelper.invalidateValues(server1,regions[0], numInvalidates);
    cqHelper.deleteValues(client2,regions[0], numInvalidates);
    cqHelper.deleteValues(client2,regions[1], numInvalidates);
    

    for (int i=1; i <= numInvalidates; i++){
      cqHelper.waitForDestroyed(client, "testCQEvents_0", KEY+i);
      cqHelper.waitForDestroyed(client, "testCQEvents_1", KEY+i);
    }
    
    
    // validate cqs after invalidates on server2.
         
    cqHelper.validateCQ(client, "testCQEvents_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ numInvalidates,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ numInvalidates,
        /* totalEvents: */ (size+size+numInvalidates));
    
    
    cqHelper.validateCQ(client, "testCQEvents_1",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ numInvalidates,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ numInvalidates,
        /* totalEvents: */ (size+size+numInvalidates));
    
    cqHelper.closeClient(client);
    cqHelper.closeClient(client2);
    cqHelper.closeServer(server2);
    cqHelper.closeServer(server1);         
  }
  
  /**
   * tests multiple cqs on partitioned region on bridge servers with profile update 
   * for not requiring old values.
   * 
   */
  public void testPRWithCQsAndProfileUpdates() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    VM client2 = host.getVM(3);
    
    // creating Bridge Server with data store. clients will connect to this 
    // bridge server.
    createServer(server1, false, 1);
     
    // create another server with data store.
    createServer(server2, false , 1);
    
    
    final int port = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    createClient(client, port, host0);
    createClient(client2, port, host0);
    
    // register cq.
    createCQ(client, "testPRWithCQsAndProfileUpdates_0", cqs[0]); // SAME CQ REGISTERED TWICE.
    createCQ(client, "testPRWithCQsAndProfileUpdates_1", cqs[0]); 
    cqHelper.executeCQ(client, "testPRWithCQsAndProfileUpdates_0", false, null);
    cqHelper.executeCQ(client, "testPRWithCQsAndProfileUpdates_1", false, null);
    
    // create values
    final int size = 400;
    createValues(client2, regions[0], size);
    createValues(client2, regions[1], size);
    
    // wait for last creates...

    for (int i=1; i <= size; i++){
      cqHelper.waitForCreated(client,"testPRWithCQsAndProfileUpdates_0", KEY+i);
      cqHelper.waitForCreated(client,"testPRWithCQsAndProfileUpdates_1", KEY+i);
    }
    
    // validate cq..
    cqHelper.validateCQ(client, "testPRWithCQsAndProfileUpdates_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    cqHelper.validateCQ(client, "testPRWithCQsAndProfileUpdates_1",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    
    //size = 2;
    
    // do updates
    createValues(client2, regions[0], size);
    createValues(client2, regions[1], size);
    
    for (int i=1; i <= size; i++){
      cqHelper.waitForUpdated(client,"testPRWithCQsAndProfileUpdates_0", KEY+i);
      cqHelper.waitForUpdated(client,"testPRWithCQsAndProfileUpdates_1", KEY+i);
    }
    
    // validate cqs again.
    cqHelper.validateCQ(client, "testPRWithCQsAndProfileUpdates_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ (size+size));
    
    cqHelper.validateCQ(client, "testPRWithCQsAndProfileUpdates_1",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ (size+size));
    
    // invalidate all the values.
    int numInvalidates = size;
    //cqHelper.invalidateValues(server1,regions[0], numInvalidates);
    cqHelper.deleteValues(client2,regions[0], numInvalidates);
    cqHelper.deleteValues(client2,regions[1], numInvalidates);
    
    for (int i=1; i <= numInvalidates; i++){
      cqHelper.waitForDestroyed(client,"testPRWithCQsAndProfileUpdates_0",KEY+i);
      cqHelper.waitForDestroyed(client,"testPRWithCQsAndProfileUpdates_1",KEY+i);
    }
    
    
    // validate cqs after invalidates on server2.
         
    cqHelper.validateCQ(client, "testPRWithCQsAndProfileUpdates_0",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ numInvalidates,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ numInvalidates,
        /* totalEvents: */ (size+size+numInvalidates));
    
    
    cqHelper.validateCQ(client, "testPRWithCQsAndProfileUpdates_1",
        /* resultSize: */ CqQueryDUnitTest.noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ numInvalidates,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ numInvalidates,
        /* totalEvents: */ (size+size+numInvalidates));
    
    // check for requries old value set.
    /*
    server1.invoke(new CacheSerializableRunnable("Check requires old value") {
      public void run2()
      {
        Cache cc = getCache();
        PartitionedRegion region1 = (PartitionedRegion)cc
            .getRegion("/root/regionA");
        Set requiresOldValue = region1.getRegionAdvisor()
            .adviseRequiresOldValue();
        getLogWriter().info(
            "ize of requires old value at server1 before closing cqs : "
                + requiresOldValue.size());
        assertTrue("The size of requiresOldValue shoule be zero on server1",
            (0 == requiresOldValue.size()));
      }
    });

    server2.invoke(new CacheSerializableRunnable("Check requires old value") {
      public void run2()
      {
        Cache cc = getCache();
        PartitionedRegion region1 = (PartitionedRegion)cc
            .getRegion("/root/regionA");
        Set requiresOldValue = region1.getRegionAdvisor()
            .adviseRequiresOldValue();
        getLogWriter().info(
            "size of requires old value at server2 before closing cqs :"
                + requiresOldValue.size());
        assertTrue("The size of requiresOldValue should be one on server2 ",
            (1 == requiresOldValue.size()));
      }
    });
    */
    
    cqHelper.closeCQ(client, "testPRWithCQsAndProfileUpdates_0");
    cqHelper.closeCQ(client, "testPRWithCQsAndProfileUpdates_1");
    
    
    // check for requires old value set after closing all the cqs.
    /*
    REQUIRES OLD VALUES requirement is removed in the eventFilterOpt_dev_Jun09 
    branch. The old values are no more sent to the peer, instead CQs are processed
    at the source (where change happens). Replace requiresOldValue test with 
    appropriate test.
    
    server1.invoke(new CacheSerializableRunnable("Check requires old value") {
      public void run2()
      {
        Cache cc = getCache();
        PartitionedRegion region1 = (PartitionedRegion)cc
            .getRegion("/root/regionA");
        Set requiresOldValue = region1.getRegionAdvisor()
            .adviseRequiresOldValue();
        getLogWriter().info(
            "size of requires old value set at the end server1 : "
                + requiresOldValue.size());
        assertTrue("The size of requiresOldValue shoule be zero on server1",
            (0 == requiresOldValue.size()));
      }
    });
    
    
    server2.invoke(new CacheSerializableRunnable("Check requires old value") {
      public void run2()
      {
        Cache cc = getCache();
        PartitionedRegion region1 = (PartitionedRegion)cc
            .getRegion("/root/regionA");
        Set requiresOldValue = region1.getRegionAdvisor()
            .adviseRequiresOldValue();
        getLogWriter().info(
            " size of requires old value set at the end server2 : "
                + requiresOldValue.size());
        assertTrue(
            "The size of requiresOldValue shoule be zero on server2 as well after closing all the cqs",
            (0 == requiresOldValue.size()));
      }
    });
    */
    cqHelper.closeClient(client);
    cqHelper.closeClient(client2);
    cqHelper.closeServer(server2);
    cqHelper.closeServer(server1);     
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
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    final String cqName = "testEventsDuringQueryExecution_0";
    
    // Server.
    createServer(server1);
    createServer(server2);
    
    final int port = server1.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    // Initialize Client.
    createClient(client, port, host0);
    
    // create CQ.
    createCQ(client, cqName, cqs[0]);
    
    
    final int numObjects = 200;
    final int totalObjects = 500;
    
    // initialize Region.
    server1.invoke(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
      }
    });
      
    // Keep updating region (async invocation).
    server1.invokeAsync(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + regions[0]);
        for (int i = numObjects + 1; i <= totalObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
      }
    });
    
    // Execute CQ while update is in progress.
    client.invoke(new CacheSerializableRunnable("Execute CQ") {
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
        for (int i=0; i < 4; i++) {
          try {
            cqListener.waitForCreated("" + totalObjects);
            // Found skip from the loop.
            break;
          } catch (CacheException ex) {
            if (i == 3) {
              throw ex;
            }
          }
        }
        
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
    
    cqHelper.closeClient(client);
    cqHelper.closeServer(server2);
    cqHelper.closeServer(server1);    
  }

  
  
  public void testDestroyRegionEventOnClientsWithCQRegistered() throws Exception{
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);
    
    final String cqName = "testDestroyEventOnClientsWithCQRegistered_0";
    
    createServerWithoutRootRegion(server, 0, false, 0);
    
    final int port = server.invokeInt(PartitionedRegionCqQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    
    // Initialize Client.
    createCacheClient(client1, port, host0);
    createCacheClient(client2, port, host0);
    
    createCQ(client1, cqName, cqsWithoutRoot[0]);
    cqHelper.executeCQ(client1, cqName, false, null);
    
    final int numObjects = 10;
    client1.invoke(new CacheSerializableRunnable("Populate region") {

      @Override
      public void run2() throws CacheException {
        Region region = getCache().getRegion("/" + regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          getCache().getLogger().fine("### DOING PUT with key: " + ("KEY-" + i)); 
          Portfolio p = new Portfolio(i);
          region.put("KEY-"+i, p);
        }
      }
      
    });
    
    client1.invokeAsync(new CacheSerializableRunnable("Wait for CqEvent") {
      
      @Override
      public void run2() throws CacheException {
        //Check for region destroyed event from server.
        Region localRegion = getCache().getRegion("/"+ regions[0]);
        assertNotNull(localRegion);
        
        CqQueryTestListener cqListener = (CqQueryTestListener)getCache().getQueryService().getCq(cqName).getCqAttributes().getCqListener();
        assertNotNull(cqListener);
        
        cqListener.waitForTotalEvents(numObjects + 1 /*Destroy region event*/);
                
      }
    });
    
    client2.invoke(new CacheSerializableRunnable("Destroy region on server") {
      
      @Override
      public void run2() throws CacheException {
        //Check for region destroyed event from server.
        Region localRegion = getCache().getRegion("/"+ regions[0]);
        assertNotNull(localRegion);
        
        localRegion.destroyRegion();
      }
    });
    
    client1.invoke(new CacheSerializableRunnable("Check for destroyed region and closed CQ") {
      
      @Override
      public void run2() throws CacheException {
        // Check for region destroyed event from server.
        Region localRegion = getCache().getRegion("/" + regions[0]);
        // IF NULL - GOOD
        // ELSE - get listener and wait for destroyed.
        if (localRegion != null) {

          // REGION NULL
          Log.getLogWriter().info("Local region is NOT null in client 1");
          
          Wait.pause(5*1000);
          CqQuery[] cqs = getCache().getQueryService().getCqs();
          if (cqs != null && cqs.length > 0) {
            assertTrue(cqs[0].isClosed());
          }

          // If cqs length is zero then Cq got closed and removed from CQService.
          assertNull(
              "Region is still available on client1 even after performing destroyRegion from client2 on server."
                  + "Client1 must have received destroyRegion message from server with CQ parts in it.",
              getCache().getRegion("/" + regions[0]));

        }
      }
    });
    
    cqHelper.closeServer(server);
  }
  
  // helper methods.
  
  /**
   * create bridge server with default attributes for partitioned region.
   */
  public void createServer(VM server) {
    createServer(server, 0, false, 0);
  }
  
  /**
   * create accessor vm if the given accessor parameter variable is true.
   * @param server VM to create bridge server.
   * @param accessor boolean if true creates an accessor bridge server.
   */
  public void createServer(VM server, boolean accessor){
    createServer(server, 0, accessor, 0);
  }
  
  /**
   * create server with partitioned region with redundant copies.
   * @param server VM where to create the bridge server.
   * @param accessor boolean if true create partitioned region with local max memory zero.
   * @param redundantCopies number of redundant copies for a partitioned region.
   */
  public void createServer(VM server, boolean accessor, int redundantCopies){
    createServer(server, 0, accessor, redundantCopies);
  }
  
  /**
   * Create a bridge server with partitioned region.
   * @param server VM where to create the bridge server.
   * @param port bridge server port.
   * @param isAccessor if true the under lying partitioned region will not host data on this vm.
   * @param redundantCopies number of redundant copies for the primary bucket.
   */
  public void createServer(VM server, final int port,final boolean isAccessor, final int redundantCopies)
  {
    SerializableRunnable createServer = new CacheSerializableRunnable(
        "Create Cache Server") {
      public void run2() throws CacheException
      {
          LogWriterUtils.getLogWriter().info("### Create Cache Server. ###");
          //AttributesFactory factory = new AttributesFactory();
          //factory.setScope(Scope.DISTRIBUTED_ACK);
          //factory.setMirrorType(MirrorType.KEYS_VALUES);
          
          //int maxMem = 0;
          AttributesFactory attr = new AttributesFactory();
          //attr.setValueConstraint(valueConstraint);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          if (isAccessor){
            paf.setLocalMaxMemory(0);
          }
          PartitionAttributes prAttr = paf.setTotalNumBuckets(197).setRedundantCopies(redundantCopies).create();
          attr.setPartitionAttributes(prAttr);
          
          assertFalse(getSystem().isLoner());
          //assertTrue(getSystem().getDistributionManager().getOtherDistributionManagerIds().size() > 0);
          for (int i = 0; i < regions.length; i++) {
            Region r = createRegion(regions[i], attr.create());
            LogWriterUtils.getLogWriter().info("Server created the region: "+r);
          }
//          pause(2000);
          try {
            startBridgeServer(port, true);
          }
          catch (Exception ex) {
            Assert.fail("While starting CacheServer", ex);
          }
//          pause(2000);
       
      }
    };

    server.invoke(createServer);
  }
  
  /**
   * Create a bridge server with partitioned region.
   * @param server VM where to create the bridge server.
   * @param port bridge server port.
   * @param isAccessor if true the under lying partitioned region will not host data on this vm.
   * @param redundantCopies number of redundant copies for the primary bucket.
   */
  public void createServerWithoutRootRegion(VM server, final int port,final boolean isAccessor, final int redundantCopies)
  {
    SerializableRunnable createServer = new CacheSerializableRunnable(
        "Create Cache Server") {
      public void run2() throws CacheException
      {
          LogWriterUtils.getLogWriter().info("### Create Cache Server. ###");
          //AttributesFactory factory = new AttributesFactory();
          //factory.setScope(Scope.DISTRIBUTED_ACK);
          //factory.setMirrorType(MirrorType.KEYS_VALUES);
          
          //int maxMem = 0;
          AttributesFactory attr = new AttributesFactory();
          //attr.setValueConstraint(valueConstraint);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          if (isAccessor){
            paf.setLocalMaxMemory(0);
          }
          PartitionAttributes prAttr = paf.setTotalNumBuckets(1).setRedundantCopies(redundantCopies).create();
          attr.setPartitionAttributes(prAttr);
          
          assertFalse(getSystem().isLoner());
          //assertTrue(getSystem().getDistributionManager().getOtherDistributionManagerIds().size() > 0);
          for (int i = 0; i < regions.length; i++) {
            Region r = createRegionWithoutRoot(regions[i], attr.create());
            LogWriterUtils.getLogWriter().info("Server created the region: "+r);
          }
//          pause(2000);
          try {
            startBridgeServer(port, true);
          }
          catch (Exception ex) {
            Assert.fail("While starting CacheServer", ex);
          }
//          pause(2000);
      }

      private Region createRegionWithoutRoot(String regionName,
          RegionAttributes create) {
        getCache().createRegion(regionName, create);
        return null;
      }
    };

    server.invoke(createServer);
  }
  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   *
   * @since 5.5
   */
  protected void startBridgeServer(int port, boolean notifyBySubscription)
  throws IOException {
    
    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }
  
  /* Create Client */
  public void createClient(VM client, final int serverPort, final String serverHost) {
    int[] serverPorts = new int[] {serverPort};
    createClient(client, serverPorts, serverHost, null); 
  }
  
  
  /* Create Client */
  public void createClient(VM client, final int[] serverPorts, final String serverHost, final String redundancyLevel) {
    SerializableRunnable createQService =
      new CacheSerializableRunnable("Create Client") {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Create Client. ###");
        LogWriterUtils.getLogWriter().info(
            "Will connect to server at por: " + serverPorts[0] + " and at host : "
             + serverHost);
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
        
        if (redundancyLevel != null){
          ClientServerTestCase.configureConnectionPool(regionFactory, serverHost, serverPorts[0],-1, true, Integer.parseInt(redundancyLevel), -1, null);
        } else {
          ClientServerTestCase.configureConnectionPool(regionFactory, serverHost, serverPorts[0], -1, true, -1, -1, null);
        }
        
        for (int i=0; i < regions.length; i++) {        
          Region clientRegion = createRegion(regions[i], regionFactory.createRegionAttributes());
          LogWriterUtils.getLogWriter().info("### Successfully Created Region on Client :" + clientRegion);
          //region1.getAttributesMutator().setCacheListener(new CqListener());
        }
      }
    };
    
    client.invoke(createQService);
  }
  
  public void createCQ(VM vm, final String cqName, final String queryStr) {
    vm.invoke(new CacheSerializableRunnable("Create CQ :" + cqName) {
      public void run2() throws CacheException {
        //pause(60 * 1000);
        //getLogWriter().info("### DEBUG CREATE CQ START ####");
        //pause(20 * 1000);
        
        LogWriterUtils.getLogWriter().info("### Create CQ. ###" + cqName);
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to getCQService.");
        }
        // Create CQ Attributes.
        CqAttributesFactory cqf = new CqAttributesFactory();
        CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};
        ((CqQueryTestListener)cqListeners[0]).cqName = cqName;
        
        cqf.initCqListeners(cqListeners);
        CqAttributes cqa = cqf.create();
        
        // Create CQ.
        try {
          CqQuery cq1 = cqService.newCq(cqName, queryStr, cqa);
          assertTrue("newCq() state mismatch", cq1.getState().isStopped());
          LogWriterUtils.getLogWriter().info("Created a new CqQuery : "+cq1);
        } catch (Exception ex){
          AssertionError err = new AssertionError("Failed to create CQ " + cqName + " . ");
          err.initCause(ex);
          LogWriterUtils.getLogWriter().info("CqService is :" + cqService, err);
          throw err;
        }
      }
    });   
  } 
  
  /* Returs Cache Server Port */
  private static int getCacheServerPort() {
    return bridgeServerPort;
  }
  
  private static String[] getCqs() {
    CqQuery[] cqs = CacheFactory.getAnyInstance().getQueryService().getCqs();
    
    String[] cqnames = new String[cqs.length];
    int idx = 0;
    for(CqQuery cq : cqs) {
      cqnames[idx++] = cq.getName();
    }
    
    return cqnames;
  }
  
  private static int getCqCountFromRegionProfile() {
    
    LocalRegion region1 = (LocalRegion)CacheFactory.getAnyInstance().getRegion("/root/regionA");
    
    return region1.getFilterProfile().getCqCount();
  }
  
  /* Create/Init values */
  public void createValues(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values for region : "+regionName) {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          region1.put(KEY+i, new Portfolio(i));
        }
        LogWriterUtils.getLogWriter().info("### Number of Entries in Region :" + region1.keys().size());
      }
    });
  }
  
  private void assertLocalMaxMemory (VM vm){
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        for (int i = 0; i< regions.length ; i++) {
          Region region = getRootRegion().getSubregion(regions[i]);
          assertEquals("The region should be configure with local max memory zero : "
                  + region, region.getAttributes().getPartitionAttributes()
                  .getLocalMaxMemory(), 0);
        }
        
      }
    });
  }
  
  
  public void createCacheClient(VM client, final int serverPort, final String serverHost){
    createCacheClient(client, new String[]{serverHost}, new int[]{serverPort}, null);
  }
  
  public void createCacheClient(VM vm, final String[] serverHosts, final int[] serverPorts, final String redundancyLevel) {
    vm.invoke(new CacheSerializableRunnable("createCacheClient") {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info(
            "Will connect to server at por: " + serverPorts[0] + " and at host : "
             + serverHosts[0]);
        ClientCacheFactory ccf = new ClientCacheFactory();
        ccf.addPoolServer(serverHosts[0]/*getServerHostName(Host.getHost(0))*/, serverPorts[0]);
        ccf.setPoolSubscriptionEnabled(true);
        ccf.set("log-level", LogWriterUtils.getDUnitLogLevel());
        
        // Create Client Cache.
        getClientCache(ccf);
        
        //Create regions
        // Initialize CQ Service.
        try {
          getCache().getQueryService();
        } catch (Exception cqe) {
          cqe.printStackTrace();
          fail("Failed to getCQService.");
        }
        
        AttributesFactory regionFactory = new AttributesFactory();
        regionFactory.setScope(Scope.LOCAL);
        
        if (redundancyLevel != null){
          ClientServerTestCase.configureConnectionPool(regionFactory, serverHosts[0], serverPorts[0],-1, true, Integer.parseInt(redundancyLevel), -1, null);
        } else {
          ClientServerTestCase.configureConnectionPool(regionFactory, serverHosts[0], serverPorts[0], -1, true, -1, -1, null);
        }
        
        for (int i=0; i < regions.length; i++) {        
          Region clientRegion = ((ClientCache)getCache()).createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create(regions[i]);
          LogWriterUtils.getLogWriter().info("### Successfully Created Region on Client :" + clientRegion);
          //region1.getAttributesMutator().setCacheListener(new CqListener());
        }
      }
    });   
  }
}


