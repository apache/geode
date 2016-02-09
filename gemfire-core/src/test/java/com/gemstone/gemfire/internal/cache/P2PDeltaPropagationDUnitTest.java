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

import java.util.Properties;

import com.gemstone.gemfire.DeltaTestImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.tcp.ConnectionTable;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * 
 * Tests the P2P delta propagation functionality.
 * 
 */
public class P2PDeltaPropagationDUnitTest extends DistributedTestCase
{

  static VM server1 = null;

  static VM server2 = null;

  static VM server3 = null;
  
  /** the cache */
  private static Cache cache = null;

  /** port for the cache server */
  private static int PORT1;

  private static int PORT2;
  
  private static final int NEW_INT = 11;
  
  private static final String NEW_STR = "DELTA";
  
  private static final int NUM_OF_CREATES = 3;
  
  static PoolImpl pool = null;

  /** name of the test region */
  private static final String REGION_NAME = "P2PDeltaPropagationDUnitTest_Region";
  
  private static int numOfUpdates = 0;

  private static int hasDeltaBytes = 0;

  private static boolean check = false;

  protected static Object waitLock = new Object();
  /**
   * Constructor
   */
  public P2PDeltaPropagationDUnitTest(String name) {
    super(name);
  }
  
  /*
   * Delta gets distributed in P2P D-ACK.
   */
    
  public void testP2PDeltaPropagationEnableScopeDAck() throws Exception
  {
    Object args[] = new Object[] { Boolean.TRUE, DataPolicy.REPLICATE,
        Scope.DISTRIBUTED_ACK, Boolean.FALSE };
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",args);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",args);
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",args);
    //only delta should get send to server2 and server3
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "put");
    Thread.sleep(5000);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "getOnDeltaEnabledServer");
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "getOnDeltaEnabledServer");
    
  }

  /*
   * Delta gets distributed in P2P GLOBAL.
   */
    
  public void testP2PDeltaPropagationEnableScopeGlobal() throws Exception
  {
    Object args[] = new Object[] { Boolean.TRUE, DataPolicy.REPLICATE,
        Scope.GLOBAL, Boolean.FALSE };
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",args);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",args);
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",args); 
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "put");
    Thread.sleep(5000);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "getOnDeltaEnabledServer");
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "getOnDeltaEnabledServer");
  }
  
  /*
   * Full object gets resend in P2P D-ACK if delta can not be applied.
   */
  public void testP2PDACKInvalidDeltaException() throws Exception
  {
    server1.invoke(P2PDeltaPropagationDUnitTest.class,
    "createServerCache", new Object[] {Boolean.TRUE});
    server2.invoke(P2PDeltaPropagationDUnitTest.class,
    "createServerCache",new Object[] {Boolean.TRUE});
    server3.invoke(P2PDeltaPropagationDUnitTest.class,
    "createServerCache",new Object[] {Boolean.TRUE});     
   
    //Delta apply should fail on server2 and server3 as values are not there
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "invalidate");
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "destroy");
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "putDelta");
    Thread.sleep(5000);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "getOnDeltaEnabledWithInvalidate");//Full object
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "getOnDeltaEnabledWithDestroy");
  }
  
  /*
   *  Full object will be send in case of P2P D-ACK(direct-ack = true). 
   */
  public void testP2PDeltaPropagationEnableDirectAckTrue() throws Exception
  {
    
    Object args[] = new Object[] { Boolean.TRUE, DataPolicy.NORMAL,
        Scope.DISTRIBUTED_ACK, Boolean.FALSE };
    ConnectionTable.threadWantsOwnResources();
    createServerCache(Boolean.TRUE, DataPolicy.NORMAL,Scope.DISTRIBUTED_ACK, Boolean.FALSE);
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",args);      
    put();
    Thread.sleep(5000);
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "getOnDeltaEnabledServer");
    
    ConnectionTable.threadWantsSharedResources();
  }
  
  /*
   * Full object will be send in case of P2P D-NO-ACK 
   */
  public void testP2PDeltaPropagationEnableScopeDNoAck()throws Exception
  {
    Object args[] = new Object[] { Boolean.TRUE, DataPolicy.NORMAL,
        Scope.DISTRIBUTED_NO_ACK, Boolean.FALSE };
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",args);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",args);
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "put");
    Thread.sleep(5000);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "getOnDeltaDisabledServer");
  }
  
  
  /*
   * Check for full object gets distributed when DS level delta property is OFF.
   */
  public void testP2PDeltaPropagationDisable() throws Exception
  {
    server1.invoke(P2PDeltaPropagationDUnitTest.class,
    "createServerCache", new Object[] {Boolean.FALSE});
    server2.invoke(P2PDeltaPropagationDUnitTest.class,
    "createServerCache",new Object[] {Boolean.FALSE}); 
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "put");
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "getOnDeltaDisabledServer");
  }

  /*
   * Delta gets distributed in P2P D-ACK with data policy empty on feeder.
   */
  public void testP2PDeltaPropagationEnableScopeDAckDataPolicyEmpty()
      throws Exception {
    Object args[] = new Object[] { Boolean.TRUE, DataPolicy.REPLICATE,
        Scope.DISTRIBUTED_ACK, Boolean.FALSE };
    Object args1[] = new Object[] { Boolean.TRUE, DataPolicy.EMPTY,
        Scope.DISTRIBUTED_ACK, Boolean.FALSE };
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        args1);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        args);
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        args);
    // only delta should get send to server2 and server3
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "put");
    Thread.sleep(5000);
    server2.invoke(P2PDeltaPropagationDUnitTest.class,
        "getOnDeltaEnabledServer");
    server3.invoke(P2PDeltaPropagationDUnitTest.class,
        "getOnDeltaEnabledServer");
  } 
  
  /*
   * Full Onject is gets distributed in P2P D-ACK with data policy empty on feeder when its uses regions create API.
   */
  public void testP2PDeltaPropagationEnableScopeDAckDataPolicyEmptyWithRegionsCreateApi()
      throws Exception {
    Object args[] = new Object[] { Boolean.TRUE, DataPolicy.REPLICATE,
        Scope.DISTRIBUTED_ACK, Boolean.FALSE };
    Object args1[] = new Object[] { Boolean.TRUE, DataPolicy.EMPTY,
        Scope.DISTRIBUTED_ACK, Boolean.FALSE };
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        args1);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        args);
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        args);
    /* clean flags */
    server1.invoke(P2PDeltaPropagationDUnitTest.class,"resetFlags");
    server2.invoke(P2PDeltaPropagationDUnitTest.class,"resetFlags");    
    server3.invoke(P2PDeltaPropagationDUnitTest.class,"resetFlags");
    
    // only delta should get send to server2 and server3
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "create");
    
    server2.invoke(P2PDeltaPropagationDUnitTest.class,
        "verifyNoFailurePeer");
    server3.invoke(P2PDeltaPropagationDUnitTest.class,
        "verifyNoFailurePeer");
  }

  public void testPeerWithEmptyRegionIterestPolicyALLReceivesNoDelta() throws Exception {
    // 1. Setup three peers, one with a region data policy set to EMPTY.
    // 2. Do delta feeds on any one of the two peers with non-EMPTY region data policy.
    // 3. Assert that peer with non-EMPTY data policy receives delta.
    // 4. Assert that peer with EMPTY data policy receives full value in the first attempt itself.

    Object replicate[] = new Object[] { Boolean.TRUE/* Delta */,
        DataPolicy.REPLICATE, Scope.DISTRIBUTED_ACK, Boolean.TRUE /* listener */};
    Object empty[] = new Object[] { Boolean.TRUE/* Delta */, DataPolicy.EMPTY,
        Scope.DISTRIBUTED_ACK, Boolean.TRUE/* listener */, Boolean.TRUE /* ALL interest policy */};

    server1.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        replicate);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        replicate);
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        empty);

    server1.invoke(P2PDeltaPropagationDUnitTest.class, "put");

    server2.invoke(P2PDeltaPropagationDUnitTest.class, "verifyDeltaReceived",
        new Object[] { Integer.valueOf(3) });
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "verifyNoDeltaReceived",
        new Object[] { Integer.valueOf(3) });
  }

  public void testPeerWithEmptyRegionDefaultIterestPolicyReceivesNoEvents() throws Exception {
    // 1. Setup three peers, one with a region data policy set to EMPTY.
    // 2. Do delta feeds on any one of the two peers with non-EMPTY region data policy.
    // 3. Assert that peer with non-EMPTY data policy receives delta.
    // 4. Assert that peer with EMPTY data policy receives full value in the first attempt itself.

    Object replicate[] = new Object[] { Boolean.TRUE/* Delta */,
        DataPolicy.REPLICATE, Scope.DISTRIBUTED_ACK, Boolean.TRUE /* listener */};
    Object empty[] = new Object[] { Boolean.TRUE/* Delta */, DataPolicy.EMPTY,
        Scope.DISTRIBUTED_ACK, Boolean.TRUE /* listener */};

    server1.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        replicate);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        replicate);
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        empty);

    server1.invoke(P2PDeltaPropagationDUnitTest.class, "put");

    server2.invoke(P2PDeltaPropagationDUnitTest.class, "verifyDeltaReceived",
        new Object[] { Integer.valueOf(3) });
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "verifyNoDeltaReceived",
        new Object[] { Integer.valueOf(0/* no events */) });
  }

  public void testPeerWithEmptyRegionAndNoCacheServerReceivesOnlyFullValue() throws Exception {
    // 1. Setup three peers, two with region data policy set to EMPTY.
    // 2. Of these two EMPTY peers, only one has a cache server.
    // 2. Do delta feeds on the peer with non-EMPTY region data policy.
    // 3. Assert that the peer with cache server receives delta bytes along with the full value.
    // 4. Assert that peer with no cache server receives full value but no delta bytes.

    int port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    Object replicate[] = new Object[] {Boolean.TRUE/* Delta */,
        DataPolicy.REPLICATE, Scope.DISTRIBUTED_ACK, Boolean.FALSE /* listener */};
    Object emptyWithServer[] = new Object[] {Boolean.TRUE/* Delta */,
        DataPolicy.EMPTY, Scope.DISTRIBUTED_ACK, Boolean.TRUE/* listener */,
        Boolean.TRUE /* ALL interest policy */, port1};
    Object emptyWithoutServer[] = new Object[] {Boolean.TRUE/* Delta */,
        DataPolicy.EMPTY, Scope.DISTRIBUTED_ACK, Boolean.TRUE/* listener */,
        Boolean.TRUE /* ALL interest policy */};

    server1.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        replicate);
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        emptyWithServer);
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "createServerCache",
        emptyWithoutServer);

    server1.invoke(P2PDeltaPropagationDUnitTest.class, "put");

    server2.invoke(P2PDeltaPropagationDUnitTest.class, "verifyDeltaBytesReceived",
        new Object[] { Integer.valueOf(2) });
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "verifyDeltaBytesReceived",
        new Object[] { Integer.valueOf(0) });
  }

  public static void put() throws Exception
  {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    DeltaTestImpl test = new DeltaTestImpl();    
    r1.put("KEY", test);    
    
    test = new DeltaTestImpl();
    test.setIntVar(NEW_INT);
    r1.put("KEY",test);
    
    test = new DeltaTestImpl();
    test.setStr(NEW_STR);
    r1.put("KEY",test);
  }  

  public static void create() throws Exception
  {    
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    DeltaTestImpl test = new DeltaTestImpl();    
    r1.create("KEY", test);    
    
    test = new DeltaTestImpl();
    test.setIntVar(NEW_INT);
    r1.create("KEY1",test);
    
    test = new DeltaTestImpl();
    test.setStr(NEW_STR);
    r1.create("KEY2",test);
  } 
 
  public static void putDelta() throws Exception
  {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    DeltaTestImpl test = new DeltaTestImpl(9999,NEW_STR);
    test.setIntVar(NEW_INT);
    r1.put("KEY",test);   
  }
   
  public static void invalidate() throws Exception
  {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r1.localInvalidate("KEY");
  }

  public static void destroy() throws Exception
  {
    Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    r1.localDestroy("KEY");
  }

  public static void getOnDeltaEnabledWithInvalidate() throws Exception
   {
     Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);     
     assertTrue(((DeltaTestImpl)r1.getEntry("KEY").getValue()).getIntVar() == NEW_INT);
     assertTrue(((DeltaTestImpl)r1.getEntry("KEY").getValue()).getStr().equals(NEW_STR));
   }
   
   public static void getOnDeltaEnabledWithDestroy() throws Exception
   {
     Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
     assertNull(((DeltaTestImpl)r1.getEntry("KEY")));
   }
      
   public static void getOnDeltaEnabledServer() throws Exception
   {
     Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
     assertTrue(((DeltaTestImpl)r1.getEntry("KEY").getValue()).getIntVar() == NEW_INT);
     assertTrue(((DeltaTestImpl)r1.getEntry("KEY").getValue()).getStr().equals(NEW_STR));
   }
   
   public static void getOnDeltaDisabledServer() throws Exception
   {
     Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);
     assertFalse(((DeltaTestImpl)r1.getEntry("KEY").getValue()).getIntVar() == NEW_INT);//should be overwritten as delta is disabled 
     assertTrue(((DeltaTestImpl)r1.getEntry("KEY").getValue()).getStr().equals(NEW_STR));
   }
   
   public static void checkForNoFullObjectResend() throws Exception
   {
     Region r1 = cache.getRegion(Region.SEPARATOR + REGION_NAME);     
     assertNull(((DeltaTestImpl)r1.getEntry("KEY").getValue()));
   }
   
   public static void checkForFlag() throws Exception
   {
     assertFalse(check);
   }
   
  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server3 = host.getVM(2);
    resetFlags();
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "resetFlags");
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "resetFlags");
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "resetFlags");
  }

  private Cache createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  public static void createServerCache(Boolean flag) throws Exception
  {
    ConnectionTable.threadWantsSharedResources();
    createServerCache(flag, DataPolicy.DEFAULT, Scope.DISTRIBUTED_ACK, false);
  }
  
  public static void createServerCache(Boolean flag, DataPolicy policy,
      Scope scope, Boolean listener) throws Exception {
    createServerCache(flag, policy, scope, listener, Boolean.FALSE);
  }

  public static void createServerCache(Boolean flag, DataPolicy policy,
      Scope scope, Boolean listener, Boolean interestPolicyAll) throws Exception {
    createServerCache(flag, policy, scope, listener, interestPolicyAll, null);
  }

  public static void createServerCache(Boolean flag, DataPolicy policy,
      Scope scope, Boolean listener, Boolean interestPolicyAll,
      Integer port) throws Exception {
    P2PDeltaPropagationDUnitTest test = new P2PDeltaPropagationDUnitTest("temp");
    Properties props = new Properties();
    if (!flag) {
      props.setProperty(DistributionConfig.DELTA_PROPAGATION_PROP_NAME, "false");
    }    
    cache = test.createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(scope);
    factory.setDataPolicy(policy);
    if (policy == DataPolicy.EMPTY && interestPolicyAll) {
      factory.setSubscriptionAttributes(new SubscriptionAttributes(
          InterestPolicy.ALL));
    }

    if (listener) {
      factory.addCacheListener(new CacheListenerAdapter() {
        @SuppressWarnings("synthetic-access")
        public void afterUpdate(EntryEvent event) {
          numOfUpdates++;
          cache.getLoggerI18n().fine(
              "afterUpdate(): numOfUpdates = " + numOfUpdates);
          cache.getLoggerI18n().fine(
              "(key, val): " + event.getKey() + ", " + event.getNewValue());
          if (event.getOldValue() != null) {
            if (event.getOldValue() == event.getNewValue()) {
              check = Boolean.TRUE;
            }
          }
          if (((EntryEventImpl)event).getDeltaBytes() != null) {
            cache.getLoggerI18n().fine("delta bytes received. " + hasDeltaBytes);
            assertTrue("No full value received for event " + event,
                ((EntryEventImpl)event).getNewValue() != null);
            hasDeltaBytes++;
          } else {
            cache.getLoggerI18n().fine("delta bytes not received.");
          }
        }
      });
    }
    
    Region region = cache.createRegion(REGION_NAME, factory.create());
    if (!policy.isReplicate()) {
      region.create("KEY", "KEY");
    }
    if (port != null) {
      CacheServer server1 = cache.addCacheServer();
      server1.setPort(port);
      server1.start();
    }
  }

  @Override
  protected final void preTearDown() throws Exception {
    closeCache();
    server1.invoke(P2PDeltaPropagationDUnitTest.class, "closeCache");
    server2.invoke(P2PDeltaPropagationDUnitTest.class, "closeCache");
    server3.invoke(P2PDeltaPropagationDUnitTest.class, "closeCache");
  }

  public static void closeCache()
  {
    check = false;    
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
  
  
  public static void verifyNoFailurePeer() throws Exception
  {
    Region reg = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    long elapsed = 0;
    long start = System.currentTimeMillis();
    while(elapsed < 10000 && reg.size() < NUM_OF_CREATES){
      try {
        elapsed = System.currentTimeMillis() - start;
        Thread.sleep(100);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    assertTrue("create's are missing", reg.size() == NUM_OF_CREATES);
    
    // start validation
    CachePerfStats stats = ((DistributedRegion)cache.getRegion(REGION_NAME))
        .getCachePerfStats();

    long deltaFailures = stats.getDeltaFailedUpdates();

    assertTrue("delta failures count is not zero", deltaFailures == 0);
    assertTrue("fromDelta invoked", !DeltaTestImpl.fromDeltaFeatureUsed());
  }

  public static void verifyDeltaReceived(Integer updates) {
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    CachePerfStats stats = ((DistributedRegion)cache.getRegion(REGION_NAME))
        .getCachePerfStats();
    long deltaFailures = stats.getDeltaFailedUpdates();
    long deltas = stats.getDeltaUpdates();

    assertTrue("Failures while processing delta at receiver.",
        deltaFailures == 0);
    assertTrue("Expected 2 deltas to be processed at receiver but were "
        + deltas + " (statistics)", deltas == 2);
    assertTrue(
        "Expected 2 deltas to be processed at receiver but were "
            + DeltaTestImpl.getFromDeltaInvokations()
            + " (implementation counter)", DeltaTestImpl
            .getFromDeltaInvokations() == 2);
    assertTrue(
        "Expected " + updates + " updates but found " + numOfUpdates,
        numOfUpdates == updates);
    DeltaTestImpl val = (DeltaTestImpl)region.getEntry("KEY").getValue();
    assertTrue("Latest value not received, found: " + val, NEW_STR.equals(val
        .getStr()));
  }

  public static void verifyNoDeltaReceived(Integer updates) {
    CachePerfStats stats = ((DistributedRegion)cache.getRegion(REGION_NAME))
        .getCachePerfStats();
    long deltaFailures = stats.getDeltaFailedUpdates();
    long deltas = stats.getDeltaUpdates();
    
    assertTrue(
        "Failures while processing delta at receiver. But deltas were not expected.",
        deltaFailures == 0);
    assertFalse(
        "Expected no deltas to be processed at receiver but processed were "
            + deltas + " (statistics)", deltas > 0);
    assertFalse(
        "Expected no deltas to be processed at receiver but processed were "
            + DeltaTestImpl.getFromDeltaInvokations()
            + " (implementation counter)", DeltaTestImpl.fromDeltaFeatureUsed());
    assertTrue("Expected " + updates + " updates but found " + numOfUpdates,
        numOfUpdates == updates);
  }

  public static void verifyDeltaBytesReceived(Integer num) {
    assertTrue("Expected " + num + " events with delta bytes in it but found "
        + hasDeltaBytes, hasDeltaBytes == num);
  }

  public static void resetFlags() {
    DeltaTestImpl.resetDeltaInvokationCounters();
    numOfUpdates = 0;
    hasDeltaBytes = 0;
    check = false;
  }
}

