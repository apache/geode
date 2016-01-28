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
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.DeltaTestImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CqListenerAdapter;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ConflationDUnitTest;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * 
 * Tests the PR delta propagation functionality.
 * 
 */
public class PRDeltaPropagationDUnitTest extends DistributedTestCase {
  
  private final static Compressor compressor = SnappyCompressor.getDefaultInstance();

  private static final long serialVersionUID = 1L;

  protected static Cache cache = null;

  protected static VM dataStore1 = null;

  protected static VM dataStore2 = null;

  protected static VM dataStore3 = null;

  protected static VM client1 = null;

  protected static Region deltaPR = null;
  
  private static final int NO_PUTS = 10;
  
  private static PRDeltaTestImpl prDelta = null;
  
  static PoolImpl pool = null;
  
  public static String DELTA_KEY = "DELTA_KEY";
  
  public static String LAST_KEY = "LAST_KEY";
  
  static boolean isFailed = false;
  private static boolean procced = false;
  
  private static boolean forOldNewCQVarification = false;
  
  private static boolean isBadToDelta = false;

  private static boolean isBadFromDelta = false;

  /** name of the test region */
  private static final String REGION_NAME = "PRDeltaPropagationDUnitTest_Region";

  /*
   * cq 
   */
  //private static final String CQ = "SELECT * FROM "+Region.SEPARATOR+REGION_NAME;
  private static final String CQ = "SELECT * FROM "+Region.SEPARATOR+REGION_NAME + " p where p.intVar < 9";
    
  private static int numValidCqEvents = 0;
  static boolean lastKeyReceived = false;
  private static boolean queryUpdateExecuted = false;
  private static boolean queryDestroyExecuted = false;
  private static boolean notADeltaInstanceObj = false;

  public PRDeltaPropagationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
    client1 = host.getVM(2);
    dataStore3 = host.getVM(3);
    
    DeltaTestImpl.resetDeltaInvokationCounters();
    dataStore1.invoke(PRDeltaPropagationDUnitTest.class,
        "resetAll");
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class,
        "resetAll");
    dataStore3.invoke(PRDeltaPropagationDUnitTest.class,
    "resetAll");
  }

  /**
   *  1) Put delta objects on accessor node.
   *  2) From accessor to primary delta gets propagated as part of <code>PutMessage</code> delta.
   *  3) From primary to secondary delta gets propagated as part RR distribution.
   * 
   */
  public void testDeltaPropagationForPR() throws Throwable {
    createCacheInAllPRVms();
    createDeltaPR(Boolean.FALSE);
    put();
    Boolean deltaUsed1 = (Boolean)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "checkForDelta");
    Boolean deltaUsed2 = (Boolean)dataStore2.invoke(
        PRDeltaPropagationDUnitTest.class, "checkForDelta");
    assertTrue("Delta Propagation Not Used in PR", (deltaUsed1 && deltaUsed2));
  }

  /**
   * Check delta propagation works properly with PR failover.    
   */

  public void testDeltaPropagationForPRFailover() throws Throwable {
    Object args[] = new Object[] { REGION_NAME, new Integer(1), new Integer(50),
        new Integer(8), Boolean.FALSE, null };
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args);
    Integer port2 = (Integer)dataStore2.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args);
    // Do puts after slowing the dispatcher.
    dataStore1.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "60000" });
    dataStore2.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "60000" });
    createClientCache(port1,port2);
    client1.invoke(PRDeltaPropagationDUnitTest.class,"createClientCache", new Object[] {port1,port2});
    
    int deltaSent = putsWhichReturnsDeltaSent();
    
    VM primary = null;
    VM secondary = null;
    if (pool.getPrimaryPort() == port1) {
      primary = dataStore1;
      secondary = dataStore2;
    }
    else {
      primary = dataStore2;
      secondary = dataStore1;
    }
    
    primary.invoke(PRDeltaPropagationDUnitTest.class, "closeCache");
    Thread.sleep(5000);    
    secondary.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "checkDeltaInvoked", new Object[]{new Integer(deltaSent)});    
  }

  public void testDeltaPropagationForPRFailoverWithCompression() throws Throwable {
    Object args[] = new Object[] { REGION_NAME, new Integer(1), new Integer(50),
        new Integer(8), Boolean.FALSE, compressor };
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args);
    Integer port2 = (Integer)dataStore2.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args);
    
    dataStore1.invoke(new SerializableRunnable() {
      public void run() { assertTrue(cache.getRegion(REGION_NAME).getAttributes().getCompressor() != null); }
      });

    // Do puts after slowing the dispatcher.    
    dataStore1.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "60000" });
    dataStore2.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "60000" });
    createClientCache(port1,port2);
    client1.invoke(PRDeltaPropagationDUnitTest.class,"createClientCache", new Object[] {port1,port2});
    
    int deltaSent = putsWhichReturnsDeltaSent();
    
    VM primary = null;
    VM secondary = null;
    if (pool.getPrimaryPort() == port1) {
      primary = dataStore1;
      secondary = dataStore2;
    }
    else {
      primary = dataStore2;
      secondary = dataStore1;
    }
    
    primary.invoke(PRDeltaPropagationDUnitTest.class, "closeCache");
    Thread.sleep(5000);    
    secondary.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "checkDeltaInvoked", new Object[]{new Integer(deltaSent)});    
  }
  
  /**
   * Check full object gets resend if delta can not be applied    
   */

  public void testDeltaPropagationForPRWithExpiry() throws Throwable {
    createCacheInAllPRVms();
    createDeltaPR(Boolean.TRUE);
    putWithExpiry();
    dataStore1.invoke(PRDeltaPropagationDUnitTest.class, "checkForFullObject");
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "checkForFullObject");
  }

  /**
   *  1) Put delta objects on client feeder connected PR accessor bridge server.
   *  2) From accessor to data store delta gets propagated as part of <code>PutMessage</code> delta.
   *  3) From data store to client delta should get propagated.
   * 
   */
  public void testDeltaPropagationPRAccessorAsBridgeServer() throws Throwable {
    Object args1[] = new Object[] { REGION_NAME, new Integer(0), new Integer(0),
        new Integer(8), Boolean.FALSE, null };
    Object args2[] = new Object[] { REGION_NAME, new Integer(0), new Integer(50),
        new Integer(8), Boolean.FALSE, null };
    Integer port2 = (Integer)dataStore2.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args2);
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args1);
    createClientCache(port1, new Boolean(true), new Boolean(false), new Boolean(false));
    client1.invoke(PRDeltaPropagationDUnitTest.class, "createClientCache",
        new Object[] { port2, new Boolean(true), new Boolean(false), new Boolean(false)});    
    int deltaSent = putsWhichReturnsDeltaSent();
    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "checkDeltaInvoked", new Object[]{new Integer(deltaSent)});    
  }
  
  /**
   * 1) Put delta objects on client feeder connected PR accessor bridge server.
   * 2) From accessor to data store delta gets propagated as part of
   * <code>PutMessage</code> delta.
   * 3) Exception occurs when applying delta on
   * datastore node. This invalid delta exception propagated back to client
   * through accessor. 
   * 4) Client sends full object in response.
   * 
   */
  public void testDeltaPropagationPRAccessorAsBridgeServerWithDeltaException() throws Throwable {
    Object args1[] = new Object[] { REGION_NAME, new Integer(0), new Integer(0),
        new Integer(8), Boolean.FALSE, null };
    Object args2[] = new Object[] { REGION_NAME, new Integer(0), new Integer(50),
        new Integer(8), Boolean.FALSE, null };
    Integer port2 = (Integer)dataStore2.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args2);
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args1);
    createClientCache(port1, new Boolean(false), new Boolean(false),  new Boolean(false));
    client1.invoke(PRDeltaPropagationDUnitTest.class, "createClientCache",
        new Object[] { port2, new Boolean(true) , new Boolean(false), new Boolean(false)});    

    // feed delta
    DeltaTestImpl test = new DeltaTestImpl();
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    // perform invalidate on accessor
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "invalidateDeltaKey");
    
    /*Thread.sleep(2000);*/

    test = new DeltaTestImpl();
    test.setStr("DELTA");
    deltaPR.put(DELTA_KEY, test);

/*    putWithExpiry();*/    
    deltaPR.put(LAST_KEY, "");    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "checkForFullObject");    
  }
  
  /**
   * 1) Put delta objects on client feeder with data policy as empty, connected PR accessor bridge server.
   * 2) From accessor to data store delta gets propagated as part of
   * <code>PutMessage</code> delta.
   * 3) Exception occurs when applying delta on
   * datastore node. This invalid delta exception propagated back to client
   * through accessor. 
   * 4) Client sends full object in response.
   * 
   */
  public void testDeltaPropagationClientEmptyPRAccessorAsBridgeServerWithDeltaException()
      throws Throwable {
    Object args1[] = new Object[] { REGION_NAME, new Integer(0),
        new Integer(0), new Integer(8), Boolean.FALSE, null };
    Object args2[] = new Object[] { REGION_NAME, new Integer(0),
        new Integer(50), new Integer(8), Boolean.FALSE, null };
    Integer port2 = (Integer)dataStore2.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args2);
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args1);
    createClientCache(port1, new Boolean(false), new Boolean(true),  new Boolean(false));
    client1.invoke(PRDeltaPropagationDUnitTest.class, "createClientCache",
        new Object[] { port2, new Boolean(true), new Boolean(false), new Boolean(false)});
    
    // feed delta
    DeltaTestImpl test = new DeltaTestImpl();
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    // perform invalidate on accessor
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "invalidateDeltaKey");

    test = new DeltaTestImpl();
    test.setStr("DELTA");
    deltaPR.put(DELTA_KEY, test);
    
    deltaPR.put(LAST_KEY, "");
    
    checkToDeltaCounter(new Integer(2));
    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");
    client1.invoke(PRDeltaPropagationDUnitTest.class, "checkForFullObject");
    
  }
  
  /**
   * 1) Put delta objects on client feeder connected accessor bridge server.
   * 2) From accessor to data store delta gets propagated as part of
   * <code>UpdateMessage</code> delta. 
   * 3) Exception occurs when applying delta on
   * datastore node. This invalid delta exception propagated back to client
   * through accessor. 
   * 4) Client sends full object in response.
   * 
   */
  public void testDeltaPropagationReplicatedRegionPeerWithDeltaException() throws Throwable {
    Object args1[] = new Object[] {Boolean.FALSE, Boolean.TRUE};
    Object args2[] = new Object[] {Boolean.TRUE, Boolean.FALSE};
//  server 1 with empty data policy
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createServerCache", args1);
    
    // server 2 with non empty data policy
   dataStore2.invoke(
        PRDeltaPropagationDUnitTest.class, "createServerCache", args2);
    
    createClientCache(port1, Boolean.FALSE, Boolean.FALSE,  Boolean.FALSE);

    // feed delta
    DeltaTestImpl test = new DeltaTestImpl();
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    // perform invalidate on accessor
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "invalidateDeltaKey");

    test = new DeltaTestImpl();
    test.setStr("DELTA");
    deltaPR.put(DELTA_KEY, test);

    deltaPR.put(LAST_KEY, "");
    
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");
    // check and reset isFailed flag
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "checkIsFailed");
    
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "fromDeltaCounter",
        new Object[] { new Integer(1) });
  }
  
  /**
   * 1) Put delta objects on feeder connected accessor bridge server.
   * 2) Second client attached to datastore. Register CQ.
   * 3) Varifies that no data loss, event revcieved on second client 
   */
  public void testCqClientConnectAccessorAndDataStore()
      throws Throwable {
    Object args1[] = new Object[] { REGION_NAME, new Integer(1),
        new Integer(0), new Integer(8), Boolean.FALSE, null };
    Object args2[] = new Object[] { REGION_NAME, new Integer(1),
        new Integer(50), new Integer(8), Boolean.FALSE, null };
    Integer port2 = (Integer)dataStore2.invoke(PRDeltaPropagationDUnitTest.class,
        "createCacheServerWithPR", args2);
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args1);

    // client attached to accessor server1
    createClientCache(port1, new Boolean(false), new Boolean(true),
        new Boolean(false));
    
    // enable CQ listner validation for this test for this client
    client1.invoke(PRDeltaPropagationDUnitTest.class,
        "setForOldNewCQVarification", new Object[] { new Boolean(true) });
    
    // Not registering any interest but register cq server2
    client1.invoke(PRDeltaPropagationDUnitTest.class, "createClientCache",
        new Object[] { port2, new Boolean(false), new Boolean(false),
            new Boolean(true) });
    
    // check cloning is disabled
    dataStore1.invoke(PRDeltaPropagationDUnitTest.class, "checkCloning");
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "checkCloning");
    client1.invoke(PRDeltaPropagationDUnitTest.class, "checkCloning");
    checkCloning();
    
    // feed delta
    DeltaTestImpl test = new DeltaTestImpl(8, "");
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    deltaPR.put(LAST_KEY, new DeltaTestImpl(5, ""));
    
    // wait for last key
    client1.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");
    // full object, server will send full object as only CQ are registered
    client1.invoke(PRDeltaPropagationDUnitTest.class, "fromDeltaCounter",
        new Object[] { new Integer(0) });
    boolean failed = ((Boolean)client1.invoke(
        PRDeltaPropagationDUnitTest.class, "isFailed")).booleanValue();
    // no cq events should get miss
    assertTrue("EVENT Missed", failed == true);
    
    // region size should be zero in second client as no registration happens
    client1.invoke(PRDeltaPropagationDUnitTest.class, "checkRegionSize",
        new Object[] { new Integer(0)});
  }
  
  /**
   * Toplogy: PR: Accessor, dataStore.
   * client and client1 connected to PR accessor; 
   * client puts delta objects on dataStore via accessor
   * Accesor gets adjunctMessage about put
   * Verify on client1 that queryUpdate and queryDestroy are executed properly
   */
  public void testClientOnAccessorReceivesCqEvents()
      throws Throwable {
    Object args1[] = new Object[] { REGION_NAME, new Integer(1),
        new Integer(0), new Integer(8), Boolean.FALSE, null };
    Object args2[] = new Object[] { REGION_NAME, new Integer(1),
        new Integer(50), new Integer(8), Boolean.FALSE, null };
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class,
        "createCacheServerWithPR", args2);
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args1);

    // both clients are attached to accessor
    createClientCache(port1, new Boolean(false), new Boolean(true),
        new Boolean(false));
    // no register interest but register cq
    client1.invoke(PRDeltaPropagationDUnitTest.class, "createClientCache",
        new Object[] { port1, new Boolean(false), new Boolean(false),
            new Boolean(true) });

    // feed delta
    // This delta obj satisfies CQ
    DeltaTestImpl test = new DeltaTestImpl(8,"");
    deltaPR.put(DELTA_KEY, test);

    // newValue does not satisfy CQ while oldValue does
    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    deltaPR.put(LAST_KEY, new DeltaTestImpl(8,""));

    client1.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");
    boolean flag = ((Boolean)client1.invoke(PRDeltaPropagationDUnitTest.class,
        "verifyQueryUpdateExecuted")).booleanValue();
    assertTrue("client update cq not executed properly", flag);
    flag = ((Boolean)client1.invoke(PRDeltaPropagationDUnitTest.class,
        "verifyQueryDestroyExecuted")).booleanValue();
    assertTrue("client destroy cq not executed properly", flag);        
  }

  /**
   * Toplogy: PR: Accessor,DataStore,Bridge server;
   * configured for 2 buckets and redundancy 1
   * DataStore has primary while BridgeServer has secondary of bucket.
   * client connects to PR Accessor
   * client1 connects to PR BridgeServer
   * client1 registers CQ
   * client puts delta objects on accessor
   * Verify on client1 that queryUpdate and queryDestroy are executed properly
   */  
  public void testCQClientOnRedundantBucketReceivesCQEvents()
      throws Throwable {
    // args for accessor
    Object args1[] = new Object[] { REGION_NAME, new Integer(1),
        new Integer(0), new Integer(2), Boolean.FALSE, null };
    // args for dataStore with 2 buckets
    Object args2[] = new Object[] { REGION_NAME, new Integer(1),
        new Integer(50), new Integer(2), Boolean.FALSE, null };
      
    // dataStore2 is DataStore
    // implicit put of DELTA_KEY creates primary bucket on dataStore2
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class,
        "createCacheServerWithPR", args2);
    // dataStore3 is BridgeServer
    // this has secondary bucket
    Integer port3 = (Integer)dataStore3.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args2);
    // dataStore1 is accessor
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args1);
  
    // this client is attached to accessor - (port1)
    createClientCache(port1, new Boolean(false), new Boolean(true),
        new Boolean(false));
    // client1 is attached to BridgeServer dataStore3
    // client1 does not registerInterest but registers cq
    client1.invoke(PRDeltaPropagationDUnitTest.class, "createClientCache",
        new Object[] { port3, new Boolean(false), new Boolean(false),
            new Boolean(true) });

    // create delta keys (1 primary 1 redundant bucket on each dataStore)
    DeltaTestImpl test = new DeltaTestImpl(8, "");
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    deltaPR.put(LAST_KEY, new DeltaTestImpl(8, ""));
  
    client1.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");
    // verify no delta is sent by server to client1
    dataStore3.invoke(PRDeltaPropagationDUnitTest.class, "verifyDeltaSent",
        new Object[] { Integer.valueOf(1) });
    boolean flag = ((Boolean)client1.invoke(PRDeltaPropagationDUnitTest.class,
        "verifyQueryUpdateExecuted")).booleanValue();
    assertTrue("client update cq not executed properly", flag);
    flag = ((Boolean)client1.invoke(PRDeltaPropagationDUnitTest.class,
        "verifyQueryDestroyExecuted")).booleanValue();
    assertTrue("client destroy cq not executed properly", flag);        
  }

  /**
   * Toplogy: PR: Accessor,DataStore,Bridge server;
   * configured for 2 buckets and redundancy 1
   * DataStore has primary while BridgeServer has secondary of bucket.
   * client connects to PR Accessor
   * client1 connects to PR BridgeServer
   * client1 registers Interest as well as CQ
   * client puts delta objects on accessor
   * Verify that client1 receives 2 deltas for 2 updates (due to RI)
   * Verify on client1 that queryUpdate and queryDestroy are executed properly
   */  
  public void testCQRIClientOnRedundantBucketReceivesDeltaAndCQEvents()
      throws Throwable {
    // args for accessor
    Object args1[] = new Object[] { REGION_NAME, new Integer(1),
        new Integer(0), new Integer(2), Boolean.FALSE, null };
    // args for dataStore with 2 buckets
    Object args2[] = new Object[] { REGION_NAME, new Integer(1),
        new Integer(50), new Integer(2), Boolean.FALSE, null };    
  
    // dataStore2 is DataStore
    // implicit put of DELTA_KEY creates primary bucket on dataStore2
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class,
        "createCacheServerWithPR", args2);
    // dataStore3 is BridgeServer
    // this has secondary bucket
    Integer port3 = (Integer)dataStore3.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args2);
    // dataStore1 is accessor
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args1);
  
    // this client is attached to accessor - (port1)
    createClientCache(port1, new Boolean(false), new Boolean(true),
        new Boolean(false));
    // client1 is attached to BridgeServer dataStore3
    // client1 registers Interest as well as cq
    client1.invoke(PRDeltaPropagationDUnitTest.class, "createClientCache",
        new Object[] { port3, new Boolean(true), new Boolean(false),
            new Boolean(true) });

    // create delta keys (1 primary 1 redundant bucket on each dataStore)
    DeltaTestImpl test = new DeltaTestImpl(8, "");
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);
  
    deltaPR.put(LAST_KEY, new DeltaTestImpl(8, ""));
  
    client1.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");
    client1.invoke(PRDeltaPropagationDUnitTest.class, "fromDeltaCounter",
        new Object[] { new Integer(1) });
    boolean flag = ((Boolean)client1.invoke(PRDeltaPropagationDUnitTest.class,
        "verifyQueryUpdateExecuted")).booleanValue();
    assertTrue("client update cq not executed properly", flag);
    flag = ((Boolean)client1.invoke(PRDeltaPropagationDUnitTest.class,
        "verifyQueryDestroyExecuted")).booleanValue();
    assertTrue("client destroy cq not executed properly", flag);        
  }

  /**
   *  1) Put delta objects on client feeder connected to PR accessor bridge server.
   *  2) From accessor to data store delta gets propagated as part of <code>PutMessage</code> delta.
   *  3) From data store to accessor delta + full value gets propagated as part of Adjunct Message.
   *  4) From accessor to client delta should get propagated.
   * 
   */
  public void testDeltaPropagationWithAdjunctMessaging() throws Throwable {
    Object args1[] = new Object[] { REGION_NAME, new Integer(0), new Integer(0),
        new Integer(8), Boolean.FALSE, null };
    Object args2[] = new Object[] { REGION_NAME, new Integer(0), new Integer(50),
        new Integer(8), Boolean.FALSE, null };
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class,
        "createCacheServerWithPR", args2);
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args1);
    createClientCache(port1, new Boolean(true), new Boolean(false), new Boolean(false));
    client1.invoke(PRDeltaPropagationDUnitTest.class, "createClientCache",
        new Object[] { port1, new Boolean(true), new Boolean(false), new Boolean(false)});    
    int deltaSent = putsWhichReturnsDeltaSent();
    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "checkDeltaInvoked", new Object[]{new Integer(deltaSent)});    
  }
  
  /**
   *  1) Accessor is a Feeder.From accessor to data store delta gets propagated as part of <code>PutMessage</code> delta.
   *  2) From data store to accessor delta + full value gets propagted as part of Adjunct Message.
   *  3) From accessor to a client with data policy normal delta should get propagated.
   *  4) From accessor to client with data policy empty full value should get propagated.
   * 
   */
  public void testDeltaPropagationWithAdjunctMessagingForEmptyClient() throws Throwable {
    Object args1[] = new Object[] { REGION_NAME, new Integer(0), new Integer(0),
        new Integer(8), Boolean.FALSE, null };
    Object args2[] = new Object[] { REGION_NAME, new Integer(0), new Integer(50),
        new Integer(8), Boolean.FALSE, null };
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class,
        "createCacheServerWithPR", args2);
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", args1);
    //Empty data policy on client
    createClientCache(port1, new Boolean(true), new Boolean(true), new Boolean(false));
    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "createClientCache",
        new Object[] { port1, new Boolean(true), new Boolean(false), new Boolean(false)});    
    
    //Feed on an accessor
    int deltaSent =  (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class,"putsWhichReturnsDeltaSent");
    
    waitForLastKey();
    checkDeltaInvoked(new Integer(0));
    client1.invoke(PRDeltaPropagationDUnitTest.class, "waitForLastKey");    
    client1.invoke(PRDeltaPropagationDUnitTest.class, "checkDeltaInvoked", new Object[]{new Integer(deltaSent)});    
  }
  
  /**
   *  1) One accessor and one datastore is defined with a PR with zero redundant copies.
   *  2) Client is connected only to the accessor.
   *  3) One delta put is performed on datastore.
   *  4) Flag to cause toDelta() throw an exception is set on datastore.
   *  5) Another delta put is performed on datastore.
   *  6) Verify that the second delta put fails and value on datastore is same as the one put by first delta update. 
   */
  public void testDeltaPropagationWithAdjunctMessagingAndBadDelta() throws Throwable {
    Object accessor[] = new Object[] { REGION_NAME, 0, 0, 8, Boolean.FALSE, null };
    Object dataStore[] = new Object[] { REGION_NAME, 0, 50, 8, Boolean.FALSE, null };

    dataStore2.invoke(PRDeltaPropagationDUnitTest.class,
        "createCacheServerWithPR", dataStore);
    Integer port1 = (Integer)dataStore1.invoke(
        PRDeltaPropagationDUnitTest.class, "createCacheServerWithPR", accessor);

    createClientCache(port1, true, false, false);

    PRDeltaTestImpl val1 = new PRDeltaTestImpl();
    val1.setIntVar(11);
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "put", new Object[]{val1});

    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "setBadToDelta", new Object[]{true});
    try {
      PRDeltaTestImpl val2 = new PRDeltaTestImpl();
      val2.setIntVar(22);
      dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "put", new Object[]{val2});
      fail("Did not expect successful delta put.");
    } catch (Exception e) {
      // expected
    }
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "checkVal", new Object[]{val1});
  }
  
  public static void checkToDeltaCounter(Integer count) {
    assertTrue("ToDelta counters do not match, expected: " + count.intValue()
        + ", actual: " + DeltaTestImpl.getToDeltaInvokations(), DeltaTestImpl
        .getToDeltaInvokations() == count.intValue());
    DeltaTestImpl.resetDeltaInvokationCounters();
  }

  
  
  // check and reset delta counters
  public static void fromDeltaCounter(Integer count) {
    assertTrue("FromDelta counters do not match, expected: " + count.intValue()
        + ", but actual: " + DeltaTestImpl.getFromDeltaInvokations(),
        DeltaTestImpl.getFromDeltaInvokations() == count.intValue());
    DeltaTestImpl.resetDeltaInvokationCounters();
  }
  
  public static void checkIsFailed() {
    assertFalse("Full value is not recieved by server", isFailed);
  }
  
  public static Boolean isFailed() {
    return isFailed;
  }
  
  public static void checkRegionSize(Integer i) {
    assertTrue("Region size is not zero ",
        cache.getRegion(REGION_NAME).size() == i);
  }
  
  public static void checkCloning() {
    assertFalse("Cloning is enabled ", cache.getRegion(REGION_NAME)
        .getAttributes().getCloningEnabled());
  }
  
  public static void invalidateDeltaKey(){
    deltaPR.invalidate(DELTA_KEY);
  }
  
  public static void createCacheInVm() {
    new PRDeltaPropagationDUnitTest("temp").createCache(new Properties());
  }

  public void createCache(Properties props) {
    try {
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
    }
    catch (Exception e) {
      fail("Failed while creating the cache", e);
    }
  }

  private static void createDeltaPR(Boolean flag) {
    Object args[] = new Object[] { "DeltaPR", new Integer(1), new Integer(50),
        new Integer(8), flag, null };
    createPR("DeltaPR", new Integer(1), new Integer(0), new Integer(8), flag, null);
    dataStore1.invoke(PRDeltaPropagationDUnitTest.class, "createPR", args);
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "createPR", args);

  }

  public static void createPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, Boolean setExpiry, Compressor compressor) {

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy.intValue())
        .setLocalMaxMemory(localMaxMemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).create();
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(prAttr);
    attr.setDataPolicy(DataPolicy.PARTITION);
    attr.setConcurrencyChecksEnabled(true);
    if (setExpiry) {
      attr.setStatisticsEnabled(true);
      attr.setEntryIdleTimeout(new ExpirationAttributes(1,
          ExpirationAction.INVALIDATE));
    }
    if (compressor != null) {
      attr.setCompressor(compressor);
    }
    //attr.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(5));
    assertNotNull(cache);
    deltaPR = cache.createRegion(partitionedRegionName, attr.create());
    assertNotNull(deltaPR);
    getLogWriter().info(
        "Partitioned Region " + partitionedRegionName
            + " created Successfully :" + deltaPR);
  }

  public static Integer createCacheServerWithPR(String partitionedRegionName,
      Integer redundancy, Integer localMaxMemory, Integer totalNumBuckets,
      Boolean setExpiry, Compressor compressor) {
    new PRDeltaPropagationDUnitTest("temp").createCache(new Properties());
    createPR(partitionedRegionName, redundancy, localMaxMemory,
        totalNumBuckets, setExpiry, compressor);
    assertNotNull(deltaPR);
    deltaPR.put(DELTA_KEY, new PRDeltaTestImpl());
    
    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    try {
      server1.start();
    }
    catch (IOException e) {
      fail("Failed to start the Server", e);
    }
    assertTrue(server1.isRunning());
    return new Integer(server1.getPort());
  }

  public static Integer createServerCache(Boolean isListAttach,
      Boolean isEmpty) throws Exception {

    Properties props = new Properties();
    new PRDeltaPropagationDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setConcurrencyChecksEnabled(true);
    if (isEmpty.booleanValue()) {
      factory.setDataPolicy(DataPolicy.EMPTY);
    }
    else {
      factory.setDataPolicy(DataPolicy.REPLICATE);
    }
    
    lastKeyReceived = false;
    RegionAttributes attrs = factory.create();
    deltaPR = cache.createRegion(REGION_NAME, attrs);
    AttributesMutator am = deltaPR.getAttributesMutator();
    if (isListAttach.booleanValue()) {
      am.addCacheListener(new CacheListenerAdapter() {
        public void afterCreate(EntryEvent event) {
          if (event.getNewValue() == null)
            isFailed = true;

          if (event.getKey().equals(LAST_KEY)) {
            lastKeyReceived = true;
          }
        }
        
        public void afterUpdate(EntryEvent event) {
          if (event.getNewValue() == null)
            isFailed = true;
          }
      });
    }

    CacheServer server = cache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server.setPort(port);
    // ensures updates to be sent instead of invalidations
    server.setNotifyBySubscription(true);
    server.start();
    return new Integer(server.getPort());
  }
  
  public static void createClientCache(Integer port1,
      Boolean subscriptionEnable, Boolean isEmpty, Boolean isCq) throws Exception {
    PRDeltaPropagationDUnitTest test = new PRDeltaPropagationDUnitTest("temp");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    test.createCache(props);

    lastKeyReceived = false;
    queryUpdateExecuted = false;
    queryDestroyExecuted = false;
    notADeltaInstanceObj = false;
    isFailed = false;
    procced = false;
    numValidCqEvents=0;
    PoolImpl p = (PoolImpl)PoolManager.createFactory().addServer("localhost",
        port1).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(0).setThreadLocalConnections(true)
        .setMinConnections(6).setReadTimeout(20000).setPingInterval(10000)
        .setRetryAttempts(5).create("PRDeltaPropagationDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);
    
    if(isEmpty.booleanValue()){
      factory.setSubscriptionAttributes(new SubscriptionAttributes(
          InterestPolicy.ALL));
      factory.setDataPolicy(DataPolicy.EMPTY);
    }
    
    factory.setPoolName(p.getName());
    factory.setCloningEnabled(false);
    factory.addCacheListener(new CacheListenerAdapter(){
    public void afterCreate(EntryEvent event) {
        if (LAST_KEY.equals(event.getKey())) {          
        lastKeyReceived = true;
      }}});

    RegionAttributes attrs = factory.create();
    deltaPR = cache.createRegion(REGION_NAME, attrs);
    //deltaPR.create(DELTA_KEY, new PRDeltaTestImpl());
    if(subscriptionEnable.booleanValue())
      deltaPR.registerInterest("ALL_KEYS");
    pool = p;
    if (isCq.booleanValue()) {
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListenerAdapter cqlist = new CqListenerAdapter() {
        @SuppressWarnings("synthetic-access")
        public void onEvent(CqEvent cqEvent) {
          if (LAST_KEY.equals(cqEvent.getKey().toString())) {
            lastKeyReceived = true;
          }
          else if (!(cqEvent.getNewValue() instanceof Delta)) {
            notADeltaInstanceObj = true;
          } 
          else if( cqEvent.getQueryOperation().isUpdate() &&
              cqEvent.getBaseOperation().isUpdate() &&
              DELTA_KEY.equals(cqEvent.getKey().toString())) {            
            queryUpdateExecuted = true;            
          } 
          else if( cqEvent.getQueryOperation().isDestroy() &&
              cqEvent.getBaseOperation().isUpdate() &&
              DELTA_KEY.equals(cqEvent.getKey().toString())) {            
            queryDestroyExecuted = true;
          }
          
          if (forOldNewCQVarification) {
            if (DELTA_KEY.equals(cqEvent.getKey().toString())) {
              if(numValidCqEvents == 0 && ((DeltaTestImpl)cqEvent
                  .getNewValue()).getIntVar() == 8){
                procced = true;
              }else if (procced && numValidCqEvents == 1 && ((DeltaTestImpl)cqEvent
                  .getNewValue()).getIntVar() == 10) {
                // this tell us that every thing is fine
                isFailed = true;
              }
            }
          }
          numValidCqEvents++;
        }
      };  
      cqf.addCqListener(cqlist);
      CqAttributes cqa = cqf.create();
      CqQuery cq = cache.getQueryService().newCq("CQ_Delta", CQ, cqa);
      cq.execute();
    }
  }

  public static void createClientCache(Integer port1, Integer port2) throws Exception {
    PRDeltaPropagationDUnitTest test = new PRDeltaPropagationDUnitTest("temp");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    test.createCache(props);

    lastKeyReceived = false;
    
    PoolImpl p = (PoolImpl)PoolManager.createFactory().addServer("localhost",
        port1).addServer("localhost", port2).setSubscriptionEnabled(true)
        .setSubscriptionRedundancy(1).setThreadLocalConnections(true)
        .setMinConnections(6).setReadTimeout(20000).setPingInterval(10000)
        .setRetryAttempts(5).create("PRDeltaPropagationDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());
    factory.setCloningEnabled(false);
    factory.setConcurrencyChecksEnabled(true);
    factory.addCacheListener(new CacheListenerAdapter(){
    public void afterCreate(EntryEvent event) {
        if (LAST_KEY.equals(event.getKey())) {
        lastKeyReceived = true;
      }}});

    RegionAttributes attrs = factory.create();
    deltaPR = cache.createRegion(REGION_NAME, attrs);
    //deltaPR.create(DELTA_KEY, new PRDeltaTestImpl());
    deltaPR.registerInterest("ALL_KEYS");
    pool = p;

  }

  public static void createCacheInAllPRVms() {
    createCacheInVm();
    dataStore1.invoke(PRDeltaPropagationDUnitTest.class, "createCacheInVm");
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "createCacheInVm");
  }

  public static void put() throws Exception {
    DeltaTestImpl test = new DeltaTestImpl();
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    test = new DeltaTestImpl();
    test.setStr("DELTA");
    deltaPR.put(DELTA_KEY, test);

  }

  public static void put(PRDeltaTestImpl val) throws Exception {
    deltaPR.put(DELTA_KEY, val);
  }

  public static void checkVal(PRDeltaTestImpl val) throws Exception {
    PRDeltaTestImpl localVal = (PRDeltaTestImpl)deltaPR.get(DELTA_KEY);
    assertEquals(val, localVal);
  }

  public static void putWithExpiry() throws Exception {
    DeltaTestImpl test = new DeltaTestImpl();
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    Thread.sleep(2000);

    test = new DeltaTestImpl();
    test.setStr("DELTA");
    deltaPR.put(DELTA_KEY, test);
    
  }

  public static int putsWhichReturnsDeltaSent() throws Exception {
    prDelta = new PRDeltaTestImpl();
    for (int i = 0; i < NO_PUTS; i++) {
      prDelta.setIntVar(i);
      deltaPR.put(DELTA_KEY, prDelta);
    }
    deltaPR.put(LAST_KEY, "");
    
    return prDelta.deltaSent;
  }

  public static Boolean checkForDelta() {
    if (DeltaTestImpl.fromDeltaFeatureUsed()) {
      assertTrue(((DeltaTestImpl)deltaPR.getEntry("DELTA_KEY").getValue())
          .getIntVar() == 10);
      assertTrue(((DeltaTestImpl)deltaPR.getEntry("DELTA_KEY").getValue())
          .getStr().equals("DELTA"));
      return Boolean.TRUE;
    }
    else {
      return Boolean.FALSE;
    }
  }

  public static void checkForFullObject() {
    assertFalse(((DeltaTestImpl)deltaPR.getEntry("DELTA_KEY").getValue())
        .getIntVar() == 10);
    assertTrue(((DeltaTestImpl)deltaPR.getEntry("DELTA_KEY").getValue())
        .getStr().equals("DELTA"));
  }
  
  public static void checkDeltaInvoked(Integer deltaSent) {
    assertTrue("Delta applied :"
        + ((PRDeltaTestImpl)deltaPR.get("DELTA_KEY")).getDeltaApplied()
        + "\n Delta sent :" + deltaSent, ((PRDeltaTestImpl)deltaPR
        .get("DELTA_KEY")).getDeltaApplied() == deltaSent);
  }
  
  public static void waitForLastKey() {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return PRDeltaPropagationDUnitTest.isLastKeyReceived();
      }
      public String description() {
        return "Last key NOT received.";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 10*1000, 100, true);
  }

  public static Boolean verifyQueryUpdateExecuted() {
    return PRDeltaPropagationDUnitTest.queryUpdateExecuted;
  }
  public static Boolean verifyQueryDestroyExecuted() {
    return PRDeltaPropagationDUnitTest.queryDestroyExecuted;
  }
  public static Boolean checkVMRecievesDeltaObjectThrCQListner() {
    return PRDeltaPropagationDUnitTest.notADeltaInstanceObj;
  }
  public static boolean isLastKeyReceived() {
    return lastKeyReceived;
  }
  public static void verifyDeltaSent(Integer deltas) {
    CacheClientNotifier ccn = ((CacheServerImpl)cache
        .getCacheServers().toArray()[0]).getAcceptor()
        .getCacheClientNotifier();

    int numOfDeltasSent = ((CacheClientProxy)ccn.getClientProxies().toArray()[0])
        .getStatistics().getDeltaMessagesSent();
    assertTrue("Expected " + deltas + " deltas to be sent but "
        + numOfDeltasSent + " were sent.", numOfDeltasSent == deltas);
  }
  
  public static void resetAll() {
    DeltaTestImpl.resetDeltaInvokationCounters();
    ConflationDUnitTest.unsetIsSlowStart();
  }

  public void tearDown2() throws Exception {
    super.tearDown2();    
    closeCache();
    client1.invoke(PRDeltaPropagationDUnitTest.class, "closeCache");
    dataStore1.invoke(PRDeltaPropagationDUnitTest.class, "closeCache");
    dataStore2.invoke(PRDeltaPropagationDUnitTest.class, "closeCache");
    dataStore3.invoke(PRDeltaPropagationDUnitTest.class, "closeCache");
    
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
      DeltaTestImpl.resetDeltaInvokationCounters();
      isFailed = false;
    }
    isBadToDelta = false;
    isBadFromDelta = false;
  }

  public static void setBadToDelta(Boolean bool) {
    isBadToDelta = bool;
  }

  public static void setBadFromDelta(Boolean bool) {
    isBadToDelta = bool;
  }

  static class PRDeltaTestImpl extends DeltaTestImpl {
    int deltaSent = 0;
    int deltaApplied = 0;

    public PRDeltaTestImpl() {

    }

    public void toDelta(DataOutput out) throws IOException {
      super.toDelta(out);
      if (isBadToDelta) {
        throw new RuntimeException("This is bad toDelta()");
      }
      deltaSent++;
    }

    public void fromDelta(DataInput in) throws IOException {
      super.fromDelta(in);
      if (isBadFromDelta) {
        throw new RuntimeException("This is bad fromDelta()");
      }
      deltaApplied++;      
    }

    public int getDeltaSent() {
      return deltaSent;
    }

    public int getDeltaApplied() {
      return deltaApplied;
    }

    public String toString() {
      return "PRDeltaTestImpl[deltaApplied=" + deltaApplied + "]"
          + super.toString();
    }
  }

  public static void setForOldNewCQVarification(Boolean forOldNewCQVarification) {
    PRDeltaPropagationDUnitTest.forOldNewCQVarification = forOldNewCQVarification
        .booleanValue();
  }
}
