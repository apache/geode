/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.Assert.fail;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.Delta;
import org.apache.geode.DeltaTestImpl;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CqListenerAdapter;
import org.apache.geode.compression.Compressor;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ConflationDUnitTestHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.test.junit.categories.SerializationTest;

/**
 * Tests the PR delta propagation functionality.
 */
@Category({SerializationTest.class, ClientSubscriptionTest.class})
public class PRDeltaPropagationDUnitTest extends DistributedTestCase {

  private static final Compressor compressor = SnappyCompressor.getDefaultInstance();
  private static final int NO_PUTS = 10;
  private static final String DELTA_KEY = "DELTA_KEY";
  private static final String LAST_KEY = "LAST_KEY";
  private static final String REGION_NAME = "PRDeltaPropagationDUnitTest_Region";
  private static final String CQ =
      "SELECT * FROM " + Region.SEPARATOR + REGION_NAME + " p where p.intVar < 9";

  private static Cache cache = null;
  private static Region deltaPR = null;
  private static PRDeltaTestImpl prDelta = null;
  private static PoolImpl pool = null;
  private static boolean isFailed = false;
  private static boolean procced = false;
  private static boolean forOldNewCQVerification = false;
  private static boolean isBadToDelta = false;
  private static boolean isBadFromDelta = false;
  private static int numValidCqEvents = 0;
  private static boolean lastKeyReceived = false;
  private static boolean queryUpdateExecuted = false;
  private static boolean queryDestroyExecuted = false;
  private static boolean notADeltaInstanceObj = false;

  private static VM dataStore1 = null;
  private static VM dataStore2 = null;
  private static VM dataStore3 = null;
  private static VM client1 = null;

  @Before
  public void setUp() throws Exception {
    dataStore1 = getHost(0).getVM(0);
    dataStore2 = getHost(0).getVM(1);
    client1 = getHost(0).getVM(2);
    dataStore3 = getHost(0).getVM(3);

    DeltaTestImpl.resetDeltaInvokationCounters();
    dataStore1.invoke(() -> resetAll());
    dataStore2.invoke(() -> resetAll());
    dataStore3.invoke(() -> resetAll());
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
    Invoke.invokeInEveryVM(() -> {
      DeltaTestImpl.resetDeltaInvokationCounters();
      isFailed = false;
      isBadToDelta = false;
      isBadFromDelta = false;
    });
  }

  /**
   * 1) Put delta objects on accessor node. 2) From accessor to primary delta gets propagated as
   * part of <code>PutMessage</code> delta. 3) From primary to secondary delta gets propagated as
   * part RR distribution.
   */
  @Test
  public void testDeltaPropagationForPR() throws Exception {
    createCacheInAllPRVms();
    createDeltaPR(false);
    put();
    boolean deltaUsed1 = dataStore1.invoke(() -> checkForDelta());
    boolean deltaUsed2 = dataStore2.invoke(() -> checkForDelta());
    assertTrue("Delta Propagation Not Used in PR", (deltaUsed1 && deltaUsed2));
  }

  /**
   * Monitor number of times constructor is called Without copy or cloning, we should have 1
   * instance
   */
  @Test
  public void testConstructorCountWithoutCloning() throws Exception {
    clearConstructorCounts();
    createCacheInAllPRVms();
    createDeltaPR(false);

    // Verify that cloning is disabled
    assertFalse(((LocalRegion) deltaPR).getCloningEnabled());

    verifyNoCopy();

    putInitial(); // Does multiple puts

    verifyConstructorCount(1);
  }

  /**
   * Monitor number of times constructor is called With cloning, we should have more than 1 instance
   * on members receiving delta updates
   */
  @Test
  public void testConstructorCountWithCloning() throws Exception {
    clearConstructorCounts();
    createCacheInAllPRVms();
    createDeltaPRWithCloning(false);

    // Verify that cloning is enabled
    assertTrue(((LocalRegion) deltaPR).getCloningEnabled());

    verifyNoCopy();

    putInitial(); // Does multiple puts

    verifyConstructorCount(2);
  }

  /**
   * Create partition with cloning disabled, then enable cloning and verify proper operation
   */
  @Test
  public void testConstructorCountWithMutator() throws Exception {
    clearConstructorCounts();
    createCacheInAllPRVms();
    createDeltaPR(false);

    // Verify that cloning is disabled
    assertFalse(((LocalRegion) deltaPR).getCloningEnabled());

    verifyNoCopy();

    PRDeltaTestImpl myDTI = putInitial(); // Does multiple puts

    // With cloning disabled only single instance
    verifyConstructorCount(1);

    // Now set cloning enabled
    setPRCloning(true);
    // Verify that cloning is enabled
    assertTrue(((LocalRegion) deltaPR).getCloningEnabled());

    // With cloning enabled each put/delta should create a new instance
    putMore(myDTI);

    verifyConstructorCount(3);
  }

  /**
   * Check delta propagation works properly with PR failover.
   */
  @Test
  public void testDeltaPropagationForPRFailover() throws Exception {
    int port1 =
        dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 50, 8, false, null));
    int port2 =
        dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 50, 8, false, null));

    // Do puts after slowing the dispatcher.
    dataStore1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
    dataStore2.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
    createClientCache(port1, port2);
    client1.invoke(() -> createClientCache(port1, port2));

    int deltaSent = putsWhichReturnsDeltaSent();

    VM primary;
    VM secondary;
    if (pool.getPrimaryPort() == port1) {
      primary = dataStore1;
      secondary = dataStore2;
    } else {
      primary = dataStore2;
      secondary = dataStore1;
    }

    primary.invoke(() -> disconnectFromDS());

    Thread.sleep(5000);

    secondary.invoke(() -> ConflationDUnitTestHelper.unsetIsSlowStart());

    client1.invoke(() -> waitForLastKey());
    client1.invoke(() -> checkDeltaInvoked(deltaSent));
  }

  @Test
  public void testDeltaPropagationForPRFailoverWithCompression() throws Exception {
    int port1 =
        dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 50, 8, false, compressor));
    int port2 =
        dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 50, 8, false, compressor));

    dataStore1.invoke(() -> {
      assertTrue(cache.getRegion(REGION_NAME).getAttributes().getCompressor() != null);
    });

    // Do puts after slowing the dispatcher.
    dataStore1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
    dataStore2.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
    createClientCache(port1, port2);
    client1.invoke(() -> createClientCache(port1, port2));

    int deltaSent = putsWhichReturnsDeltaSent();

    VM primary;
    VM secondary;
    if (pool.getPrimaryPort() == port1) {
      primary = dataStore1;
      secondary = dataStore2;
    } else {
      primary = dataStore2;
      secondary = dataStore1;
    }

    primary.invoke(() -> disconnectFromDS());

    Thread.sleep(5000);

    secondary.invoke(() -> ConflationDUnitTestHelper.unsetIsSlowStart());

    client1.invoke(() -> waitForLastKey());
    client1.invoke(() -> checkDeltaInvoked(deltaSent));
  }

  /**
   * Check full object gets resend if delta can not be applied
   */
  @Test
  public void testDeltaPropagationForPRWithExpiry() throws Exception {
    createCacheInAllPRVms();
    createDeltaPR(true);
    putWithExpiry();
    dataStore1.invoke(() -> checkForFullObject());
    dataStore2.invoke(() -> checkForFullObject());
  }

  /**
   * 1) Put delta objects on client feeder connected PR accessor cache server. 2) From accessor to
   * data store delta gets propagated as part of <code>PutMessage</code> delta. 3) From data store
   * to client delta should get propagated.
   */
  @Test
  public void testDeltaPropagationPRAccessorAsBridgeServer() throws Exception {
    int port2 =
        dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 50, 8, false, null));
    int port1 = dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 0, 8, false, null));

    createClientCache(port1, true, false, false);
    client1.invoke(() -> createClientCache(port2, true, false, false));
    int deltaSent = putsWhichReturnsDeltaSent();

    client1.invoke(() -> waitForLastKey());
    client1.invoke(() -> checkDeltaInvoked(deltaSent));
  }

  /**
   * 1) Put delta objects on client feeder connected PR accessor cache server. 2) From accessor to
   * data store delta gets propagated as part of <code>PutMessage</code> delta. 3) Exception occurs
   * when applying delta on datastore node. This invalid delta exception propagated back to client
   * through accessor. 4) Client sends full object in response.
   */
  @Test
  public void testDeltaPropagationPRAccessorAsBridgeServerWithDeltaException() throws Exception {
    int port2 =
        dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 50, 8, false, null));
    int port1 = dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 0, 8, false, null));

    createClientCache(port1, false, false, false);
    client1.invoke(() -> createClientCache(port2, true, false, false));

    // feed delta
    DeltaTestImpl test = new DeltaTestImpl();
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    // perform invalidate on accessor
    dataStore2.invoke(() -> invalidateDeltaKey());

    test = new DeltaTestImpl();
    test.setStr("DELTA");
    deltaPR.put(DELTA_KEY, test);

    deltaPR.put(LAST_KEY, "");
    client1.invoke(() -> waitForLastKey());
    client1.invoke(() -> checkForFullObject());
  }

  /**
   * 1) Put delta objects on client feeder with data policy as empty, connected PR accessor bridge
   * server. 2) From accessor to data store delta gets propagated as part of <code>PutMessage</code>
   * delta. 3) Exception occurs when applying delta on datastore node. This invalid delta exception
   * propagated back to client through accessor. 4) Client sends full object in response.
   */
  @Test
  public void testDeltaPropagationClientEmptyPRAccessorAsBridgeServerWithDeltaException()
      throws Exception {
    int port2 =
        dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 50, 8, false, null));
    int port1 = dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 0, 8, false, null));

    createClientCache(port1, false, true, false);
    client1.invoke(() -> createClientCache(port2, true, false, false));

    // feed delta
    DeltaTestImpl test = new DeltaTestImpl();
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    // perform invalidate on accessor
    dataStore2.invoke(() -> invalidateDeltaKey());

    test = new DeltaTestImpl();
    test.setStr("DELTA");
    deltaPR.put(DELTA_KEY, test);

    deltaPR.put(LAST_KEY, "");

    checkToDeltaCounter(2);

    client1.invoke(() -> waitForLastKey());
    client1.invoke(() -> checkForFullObject());
  }

  /**
   * 1) Put delta objects on client feeder connected accessor cache server. 2) From accessor to
   * data store delta gets propagated as part of <code>UpdateMessage</code> delta. 3) Exception
   * occurs when applying delta on datastore node. This invalid delta exception propagated back to
   * client through accessor. 4) Client sends full object in response.
   */
  @Test
  public void testDeltaPropagationReplicatedRegionPeerWithDeltaException() throws Exception {
    // server 1 with empty data policy
    int port1 = dataStore1.invoke(() -> createServerCache(false, true));

    // server 2 with non empty data policy
    dataStore2.invoke(() -> createServerCache(true, false));

    createClientCache(port1, false, false, false);

    // feed delta
    DeltaTestImpl test = new DeltaTestImpl();
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    // perform invalidate on accessor
    dataStore2.invoke(() -> invalidateDeltaKey());

    test = new DeltaTestImpl();
    test.setStr("DELTA");
    deltaPR.put(DELTA_KEY, test);

    deltaPR.put(LAST_KEY, "");

    dataStore2.invoke(() -> waitForLastKey());
    // check and reset isFailed flag
    dataStore2.invoke(() -> checkIsFailed());

    dataStore2.invoke(() -> fromDeltaCounter(1));
  }

  /**
   * 1) Put delta objects on feeder connected accessor cache server. 2) Second client attached to
   * datastore. Register CQ. 3) Varifies that no data loss, event revcieved on second client
   */
  @Test
  public void testCqClientConnectAccessorAndDataStore() throws Exception {
    int port2 =
        dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 50, 8, false, null));
    int port1 = dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 0, 8, false, null));

    // client attached to accessor server1
    createClientCache(port1, false, true, false);

    // enable CQ listner validation for this test for this client
    client1.invoke(() -> setForOldNewCQVerification(true));

    // Not registering any interest but register cq server2
    client1.invoke(() -> createClientCache(port2, false, false, true));

    // check cloning is disabled
    dataStore1.invoke(() -> checkCloning());
    dataStore2.invoke(() -> checkCloning());
    client1.invoke(() -> checkCloning());
    checkCloning();

    // feed delta
    DeltaTestImpl test = new DeltaTestImpl(8, "");
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    deltaPR.put(LAST_KEY, new DeltaTestImpl(5, ""));

    // wait for last key
    client1.invoke(() -> waitForLastKey());
    // full object, server will send full object as only CQ are registered
    client1.invoke(() -> fromDeltaCounter(0));
    boolean failed = client1.invoke(() -> isFailed());
    // no cq events should get miss
    assertTrue("EVENT Missed", failed == true);

    // region size should be zero in second client as no registration happens
    client1.invoke(() -> checkRegionSize(0));
  }

  /**
   * Topology: PR: Accessor, dataStore. client and client1 connected to PR accessor; client puts
   * delta objects on dataStore via accessor Accessor gets adjunctMessage about put Verify on
   * client1 that queryUpdate and queryDestroy are executed properly
   */
  @Test
  public void testClientOnAccessorReceivesCqEvents() throws Exception {
    Object args1[] = new Object[] {REGION_NAME, 1, 0, 8, false, null};
    Object args2[] = new Object[] {REGION_NAME, 1, 50, 8, false, null};

    dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 50, 8, false, null));
    int port1 = dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 0, 8, false, null));

    // both clients are attached to accessor
    createClientCache(port1, false, true, false);
    // no register interest but register cq
    client1.invoke(() -> createClientCache(port1, false, false, true));

    // feed delta
    // This delta obj satisfies CQ
    DeltaTestImpl test = new DeltaTestImpl(8, "");
    deltaPR.put(DELTA_KEY, test);

    // newValue does not satisfy CQ while oldValue does
    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    deltaPR.put(LAST_KEY, new DeltaTestImpl(8, ""));

    client1.invoke(() -> waitForLastKey());
    boolean flag = client1.invoke(() -> verifyQueryUpdateExecuted());
    assertTrue("client update cq not executed properly", flag);
    flag = client1.invoke(() -> verifyQueryDestroyExecuted());
    assertTrue("client destroy cq not executed properly", flag);
  }

  /**
   * Topology: PR: Accessor,DataStore,cache server; configured for 2 buckets and redundancy 1
   * DataStore has primary while BridgeServer has secondary of bucket. client connects to PR
   * Accessor client1 connects to PR BridgeServer client1 registers CQ client puts delta objects on
   * accessor Verify on client1 that queryUpdate and queryDestroy are executed properly
   */
  @Test
  public void testCQClientOnRedundantBucketReceivesCQEvents() throws Exception {
    // dataStore2 is DataStore
    // implicit put of DELTA_KEY creates primary bucket on dataStore2
    dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 50, 2, false, null));

    // dataStore3 is BridgeServer
    // this has secondary bucket
    int port3 =
        dataStore3.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 50, 2, false, null));

    // dataStore1 is accessor
    int port1 = dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 0, 2, false, null));

    // this client is attached to accessor - (port1)
    createClientCache(port1, false, true, false);

    // client1 is attached to BridgeServer dataStore3
    // client1 does not registerInterest but registers cq
    client1.invoke(() -> createClientCache(port3, false, false, true));

    // create delta keys (1 primary 1 redundant bucket on each dataStore)
    DeltaTestImpl test = new DeltaTestImpl(8, "");
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    deltaPR.put(LAST_KEY, new DeltaTestImpl(8, ""));

    client1.invoke(() -> waitForLastKey());
    // verify no delta is sent by server to client1
    dataStore3.invoke(() -> verifyDeltaSent(1));
    boolean flag = client1.invoke(() -> verifyQueryUpdateExecuted());
    assertTrue("client update cq not executed properly", flag);
    flag = client1.invoke(() -> verifyQueryDestroyExecuted());
    assertTrue("client destroy cq not executed properly", flag);
  }

  /**
   * Topology: PR: Accessor,DataStore,cache server; configured for 2 buckets and redundancy 1
   * DataStore has primary while BridgeServer has secondary of bucket. client connects to PR
   * Accessor client1 connects to PR BridgeServer client1 registers Interest as well as CQ client
   * puts delta objects on accessor Verify that client1 receives 2 deltas for 2 updates (due to RI)
   * Verify on client1 that queryUpdate and queryDestroy are executed properly
   */
  @Test
  public void testCQRIClientOnRedundantBucketReceivesDeltaAndCQEvents() throws Exception {
    // dataStore2 is DataStore
    // implicit put of DELTA_KEY creates primary bucket on dataStore2
    dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 50, 2, false, null));

    // dataStore3 is BridgeServer
    // this has secondary bucket
    int port3 =
        dataStore3.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 50, 2, false, null));

    // dataStore1 is accessor
    int port1 = dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 1, 0, 2, false, null));

    // this client is attached to accessor - (port1)
    createClientCache(port1, false, true, false);

    // client1 is attached to BridgeServer dataStore3
    // client1 registers Interest as well as cq
    client1.invoke(() -> createClientCache(port3, true, false, true));

    // create delta keys (1 primary 1 redundant bucket on each dataStore)
    DeltaTestImpl test = new DeltaTestImpl(8, "");
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    deltaPR.put(LAST_KEY, new DeltaTestImpl(8, ""));

    client1.invoke(() -> waitForLastKey());
    client1.invoke(() -> fromDeltaCounter(1));
    boolean flag = client1.invoke(() -> verifyQueryUpdateExecuted());
    assertTrue("client update cq not executed properly", flag);
    flag = client1.invoke(() -> verifyQueryDestroyExecuted());
    assertTrue("client destroy cq not executed properly", flag);
  }

  /**
   * 1) Put delta objects on client feeder connected to PR accessor cache server. 2) From accessor
   * to data store delta gets propagated as part of <code>PutMessage</code> delta. 3) From data
   * store to accessor delta + full value gets propagated as part of Adjunct Message. 4) From
   * accessor to client delta should get propagated.
   */
  @Test
  public void testDeltaPropagationWithAdjunctMessaging() throws Exception {
    dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 50, 8, false, null));

    int port1 = dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 0, 8, false, null));

    createClientCache(port1, true, false, false);
    client1.invoke(() -> createClientCache(port1, true, false, false));
    int deltaSent = putsWhichReturnsDeltaSent();

    client1.invoke(() -> waitForLastKey());
    client1.invoke(() -> checkDeltaInvoked(deltaSent));
  }

  /**
   * 1) Accessor is a Feeder.From accessor to data store delta gets propagated as part of
   * <code>PutMessage</code> delta. 2) From data store to accessor delta + full value gets propagted
   * as part of Adjunct Message. 3) From accessor to a client with data policy normal delta should
   * get propagated. 4) From accessor to client with data policy empty full value should get
   * propagated.
   */
  @Test
  public void testDeltaPropagationWithAdjunctMessagingForEmptyClient() throws Exception {
    dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 50, 8, false, null));

    Integer port1 =
        dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 0, 8, false, null));

    // Empty data policy on client
    createClientCache(port1, true, true, false);

    client1.invoke(() -> createClientCache(port1, true, false, false));

    // Feed on an accessor
    int deltaSent = dataStore1.invoke(() -> putsWhichReturnsDeltaSent());

    waitForLastKey();
    checkDeltaInvoked(0);
    client1.invoke(() -> waitForLastKey());
    client1.invoke(() -> checkDeltaInvoked(deltaSent));
  }

  /**
   * 1) One accessor and one datastore is defined with a PR with zero redundant copies. 2) Client is
   * connected only to the accessor. 3) One delta put is performed on datastore. 4) Flag to cause
   * toDelta() throw an exception is set on datastore. 5) Another delta put is performed on
   * datastore. 6) Verify that the second delta put fails and value on datastore is same as the one
   * put by first delta update.
   */
  @Test
  public void testDeltaPropagationWithAdjunctMessagingAndBadDelta() throws Exception {
    dataStore2.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 50, 8, false, null));

    int port1 = dataStore1.invoke(() -> createCacheServerWithPR(REGION_NAME, 0, 0, 8, false, null));

    createClientCache(port1, true, false, false);

    PRDeltaTestImpl val1 = new PRDeltaTestImpl();
    val1.setIntVar(11);
    dataStore2.invoke(() -> put(val1));

    dataStore2.invoke(() -> setBadToDelta(true));
    try {
      PRDeltaTestImpl val2 = new PRDeltaTestImpl();
      val2.setIntVar(22);
      dataStore2.invoke(() -> put(val2));
      fail("Did not expect successful delta put.");
    } catch (Exception expected) {
      // expected
    }
    dataStore2.invoke(() -> checkVal(val1));
  }

  private void checkToDeltaCounter(int count) {
    assertTrue(
        "ToDelta counters do not match, expected: " + count + ", actual: "
            + DeltaTestImpl.getToDeltaInvokations(),
        DeltaTestImpl.getToDeltaInvokations() == count);
    DeltaTestImpl.resetDeltaInvokationCounters();
  }

  // check and reset delta counters
  private void fromDeltaCounter(int count) {
    assertTrue(
        "FromDelta counters do not match, expected: " + count + ", but actual: "
            + DeltaTestImpl.getFromDeltaInvokations(),
        DeltaTestImpl.getFromDeltaInvokations() == count);
    DeltaTestImpl.resetDeltaInvokationCounters();
  }

  private void checkIsFailed() {
    assertFalse("Full value is not reeived by server", isFailed);
  }

  private boolean isFailed() {
    return isFailed;
  }

  private void checkRegionSize(int value) {
    assertTrue("Region size is not zero ", cache.getRegion(REGION_NAME).size() == value);
  }

  private void checkCloning() {
    assertFalse("Cloning is enabled ",
        cache.getRegion(REGION_NAME).getAttributes().getCloningEnabled());
  }

  private void invalidateDeltaKey() {
    deltaPR.invalidate(DELTA_KEY);
  }

  private void createCacheInVm() {
    createCache(new Properties());
  }

  public void createCache(Properties config) {
    DistributedSystem ds = getSystem(config);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(config);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  private void createDeltaPR(boolean expiry) {
    createPR("DeltaPR", 1, 0, 8, expiry, false, null);
    dataStore1.invoke(() -> createPR("DeltaPR", 1, 50, 8, expiry, false, null));
    dataStore2.invoke(() -> createPR("DeltaPR", 1, 50, 8, expiry, false, null));
  }

  private void createDeltaPRWithCloning(boolean expiry) {
    createPR("DeltaPR", 1, 0, 8, expiry, true, null);
    dataStore1.invoke(() -> createPR("DeltaPR", 1, 50, 8, expiry, true, null));
    dataStore2.invoke(() -> createPR("DeltaPR", 1, 50, 8, expiry, true, null));
  }

  private void createPR(String partitionedRegionName, int redundancy, int localMaxMemory,
      int totalNumBuckets, boolean expiry, boolean withCloning, Compressor compressor) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy)
        .setLocalMaxMemory(localMaxMemory).setTotalNumBuckets(totalNumBuckets).create();
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(prAttr);
    attr.setDataPolicy(DataPolicy.PARTITION);
    attr.setConcurrencyChecksEnabled(true);
    attr.setCloningEnabled(withCloning);
    if (expiry) {
      attr.setStatisticsEnabled(true);
      attr.setEntryIdleTimeout(new ExpirationAttributes(1, ExpirationAction.INVALIDATE));
    }
    if (compressor != null) {
      attr.setCompressor(compressor);
    }
    // attr.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(5));
    assertNotNull(cache);
    deltaPR = cache.createRegion(partitionedRegionName, attr.create());
    assertNotNull(deltaPR);
  }

  private int createCacheServerWithPR(String partitionedRegionName, int redundancy,
      int localMaxMemory, int totalNumBuckets, boolean expiry, Compressor compressor)
      throws IOException {
    createCacheInVm();

    createPR(partitionedRegionName, redundancy, localMaxMemory, totalNumBuckets, expiry, false,
        compressor);
    deltaPR.put(DELTA_KEY, new PRDeltaTestImpl());

    CacheServer server1 = cache.addCacheServer();
    assertNotNull(server1);

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server1.setPort(port);
    server1.start();
    assertTrue(server1.isRunning());
    return server1.getPort();
  }

  private int createServerCache(Boolean isListAttach, Boolean isEmpty) throws IOException {
    Properties props = new Properties();
    createCache(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setConcurrencyChecksEnabled(true);
    if (isEmpty) {
      factory.setDataPolicy(DataPolicy.EMPTY);
    } else {
      factory.setDataPolicy(DataPolicy.REPLICATE);
    }

    lastKeyReceived = false;
    RegionAttributes attrs = factory.create();
    deltaPR = cache.createRegion(REGION_NAME, attrs);
    AttributesMutator am = deltaPR.getAttributesMutator();
    if (isListAttach) {
      am.addCacheListener(new CacheListenerAdapter() {
        @Override
        public void afterCreate(EntryEvent event) {
          if (event.getNewValue() == null)
            isFailed = true;

          if (event.getKey().equals(LAST_KEY)) {
            lastKeyReceived = true;
          }
        }

        @Override
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
    return server.getPort();
  }

  public void createClientCache(int port1, boolean subscriptionEnable, boolean isEmpty,
      boolean isCq) throws Exception {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    createCache(config);

    lastKeyReceived = false;
    queryUpdateExecuted = false;
    queryDestroyExecuted = false;
    notADeltaInstanceObj = false;
    isFailed = false;
    procced = false;
    numValidCqEvents = 0;

    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer("localhost", port1)
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0).setThreadLocalConnections(true)
        .setMinConnections(6).setReadTimeout(20000).setPingInterval(10000).setRetryAttempts(5)
        .create("PRDeltaPropagationDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(true);

    if (isEmpty) {
      factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      factory.setDataPolicy(DataPolicy.EMPTY);
    }

    factory.setPoolName(p.getName());
    factory.setCloningEnabled(false);
    factory.addCacheListener(new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        if (LAST_KEY.equals(event.getKey())) {
          lastKeyReceived = true;
        }
      }
    });

    RegionAttributes attrs = factory.create();
    deltaPR = cache.createRegion(REGION_NAME, attrs);
    if (subscriptionEnable) {
      deltaPR.registerInterest("ALL_KEYS");
    }
    pool = p;
    if (isCq) {
      CqAttributesFactory cqf = new CqAttributesFactory();
      CqListenerAdapter cqlist = new CqListenerAdapter() {
        @Override
        public void onEvent(CqEvent cqEvent) {
          if (LAST_KEY.equals(cqEvent.getKey().toString())) {
            lastKeyReceived = true;
          } else if (!(cqEvent.getNewValue() instanceof Delta)) {
            notADeltaInstanceObj = true;
          } else if (cqEvent.getQueryOperation().isUpdate() && cqEvent.getBaseOperation().isUpdate()
              && DELTA_KEY.equals(cqEvent.getKey().toString())) {
            queryUpdateExecuted = true;
          } else if (cqEvent.getQueryOperation().isDestroy()
              && cqEvent.getBaseOperation().isUpdate()
              && DELTA_KEY.equals(cqEvent.getKey().toString())) {
            queryDestroyExecuted = true;
          }

          if (forOldNewCQVerification) {
            if (DELTA_KEY.equals(cqEvent.getKey().toString())) {
              if (numValidCqEvents == 0
                  && ((DeltaTestImpl) cqEvent.getNewValue()).getIntVar() == 8) {
                procced = true;
              } else if (procced && numValidCqEvents == 1
                  && ((DeltaTestImpl) cqEvent.getNewValue()).getIntVar() == 10) {
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

  public void createClientCache(Integer port1, Integer port2) throws Exception {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    createCache(config);

    lastKeyReceived = false;

    PoolImpl p = (PoolImpl) PoolManager.createFactory().addServer("localhost", port1)
        .addServer("localhost", port2).setSubscriptionEnabled(true).setSubscriptionRedundancy(1)
        .setThreadLocalConnections(true).setMinConnections(6).setReadTimeout(20000)
        .setPingInterval(10000).setRetryAttempts(5).create("PRDeltaPropagationDUnitTestPool");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());
    factory.setCloningEnabled(false);
    factory.setConcurrencyChecksEnabled(true);

    factory.addCacheListener(new CacheListenerAdapter() {
      @Override
      public void afterCreate(EntryEvent event) {
        if (LAST_KEY.equals(event.getKey())) {
          lastKeyReceived = true;
        }
      }
    });

    RegionAttributes attrs = factory.create();
    deltaPR = cache.createRegion(REGION_NAME, attrs);
    deltaPR.registerInterest("ALL_KEYS");
    pool = p;
  }

  private void createCacheInAllPRVms() {
    createCacheInVm();
    dataStore1.invoke(() -> createCacheInVm());
    dataStore2.invoke(() -> createCacheInVm());
  }

  public void put() throws Exception {
    PRDeltaTestImpl test = new PRDeltaTestImpl();
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    test = new PRDeltaTestImpl();

    test.setStr("DELTA");
    deltaPR.put(DELTA_KEY, test);
  }

  private PRDeltaTestImpl putInitial() throws Exception {
    PRDeltaTestImpl test = new PRDeltaTestImpl();
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    test.setStr("DELTA");
    deltaPR.put(DELTA_KEY, test);

    return test;
  }

  private void putMore(PRDeltaTestImpl val) throws Exception {
    val.setIntVar(13);
    put(val);

    val.setStr("DELTA2");
    put(val);
  }

  public void put(PRDeltaTestImpl val) throws Exception {
    deltaPR.put(DELTA_KEY, val);
  }

  private void checkVal(PRDeltaTestImpl val) throws Exception {
    PRDeltaTestImpl localVal = (PRDeltaTestImpl) deltaPR.get(DELTA_KEY);
    assertEquals(val, localVal);
  }

  private void putWithExpiry() throws Exception {
    DeltaTestImpl test = new DeltaTestImpl();
    deltaPR.put(DELTA_KEY, test);

    test.setIntVar(10);
    deltaPR.put(DELTA_KEY, test);

    Thread.sleep(2000);

    test = new DeltaTestImpl();
    test.setStr("DELTA");
    deltaPR.put(DELTA_KEY, test);
  }

  private int putsWhichReturnsDeltaSent() throws Exception {
    prDelta = new PRDeltaTestImpl();
    for (int i = 0; i < NO_PUTS; i++) {
      prDelta.setIntVar(i);
      deltaPR.put(DELTA_KEY, prDelta);
    }
    deltaPR.put(LAST_KEY, "");

    return prDelta.deltaSent;
  }

  private Boolean checkForDelta() {
    if (DeltaTestImpl.fromDeltaFeatureUsed()) {
      assertTrue(((DeltaTestImpl) deltaPR.getEntry("DELTA_KEY").getValue()).getIntVar() == 10);
      assertTrue(
          ((DeltaTestImpl) deltaPR.getEntry("DELTA_KEY").getValue()).getStr().equals("DELTA"));
      return true;
    } else {
      return false;
    }
  }

  private void checkForFullObject() {
    assertFalse(((DeltaTestImpl) deltaPR.getEntry("DELTA_KEY").getValue()).getIntVar() == 10);
    assertTrue(((DeltaTestImpl) deltaPR.getEntry("DELTA_KEY").getValue()).getStr().equals("DELTA"));
  }

  private void checkDeltaInvoked(Integer deltaSent) {
    assertTrue(
        "Delta applied :" + ((PRDeltaTestImpl) deltaPR.get("DELTA_KEY")).getDeltaApplied()
            + "\n Delta sent :" + deltaSent,
        ((PRDeltaTestImpl) deltaPR.get("DELTA_KEY")).getDeltaApplied() == deltaSent);
  }

  private void waitForLastKey() {
    WaitCriterion wc = new WaitCriterion() {
      @Override
      public boolean done() {
        return isLastKeyReceived();
      }

      @Override
      public String description() {
        return "Last key NOT received.";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  private boolean verifyQueryUpdateExecuted() {
    return queryUpdateExecuted;
  }

  private boolean verifyQueryDestroyExecuted() {
    return queryDestroyExecuted;
  }

  public boolean checkVMReceivesDeltaObjectThrCQListner() {
    return notADeltaInstanceObj;
  }

  public boolean isLastKeyReceived() {
    return lastKeyReceived;
  }

  private void verifyDeltaSent(int deltas) {
    CacheClientNotifier ccn = ((CacheServerImpl) cache.getCacheServers().toArray()[0]).getAcceptor()
        .getCacheClientNotifier();

    int numOfDeltasSent = ((CacheClientProxy) ccn.getClientProxies().toArray()[0]).getStatistics()
        .getDeltaMessagesSent();
    assertTrue("Expected " + deltas + " deltas to be sent but " + numOfDeltasSent + " were sent.",
        numOfDeltasSent == deltas);
  }

  private void resetAll() {
    DeltaTestImpl.resetDeltaInvokationCounters();
    ConflationDUnitTestHelper.unsetIsSlowStart();
  }

  private void setBadToDelta(boolean value) {
    isBadToDelta = value;
  }

  public void setBadFromDelta(boolean value) {
    isBadToDelta = value;
  }

  private void setForOldNewCQVerification(boolean value) {
    forOldNewCQVerification = value;
  }

  private void verifyConstructorCount(int timesConstructed) throws Exception {
    long buildCount0 = getBuildCount();
    long buildCount1 = dataStore1.invoke(() -> getBuildCount());
    long buildCount2 = dataStore2.invoke(() -> getBuildCount());

    for (Exception exception : DeltaTestImpl.getInstantiations()) {
      exception.printStackTrace();
    }

    assertEquals(1, buildCount0);
    assertEquals(timesConstructed, buildCount1);
    assertEquals(timesConstructed, buildCount2);
  }

  private void setPRCloning(boolean value) {
    setCloningEnabled(value);
    dataStore1.invoke(() -> setCloningEnabled(value));
    dataStore2.invoke(() -> setCloningEnabled(value));
  }

  private void setCloningEnabled(boolean value) {
    ((LocalRegion) deltaPR).setCloningEnabled(value);
  }

  private void verifyNoCopy() {
    // Ensure not some other reason to make a copy
    FilterProfile fp = ((LocalRegion) deltaPR).getFilterProfile();
    boolean copy = ((LocalRegion) deltaPR).getCompressor() == null
        && (((LocalRegion) deltaPR).isCopyOnRead() || (fp != null && fp.getCqCount() > 0));
    assertFalse(copy);
  }

  private void clearConstructorCounts() throws Exception {
    setBuildCount(0);
    dataStore1.invoke(() -> setBuildCount(0));
    dataStore2.invoke(() -> setBuildCount(0));
  }

  private long getBuildCount() throws Exception {
    return DeltaTestImpl.getTimesConstructed();
  }

  private void setBuildCount(long cnt) throws Exception {
    DeltaTestImpl.setTimesConstructed(cnt);
  }

  static class PRDeltaTestImpl extends DeltaTestImpl {
    int deltaSent = 0;
    int deltaApplied = 0;

    public PRDeltaTestImpl() {
      // nothing
    }

    @Override
    public void toDelta(DataOutput out) throws IOException {
      super.toDelta(out);
      if (isBadToDelta) {
        throw new RuntimeException("This is bad toDelta()");
      }
      deltaSent++;
    }

    @Override
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

    int getDeltaApplied() {
      return deltaApplied;
    }

    @Override
    public String toString() {
      return "PRDeltaTestImpl[deltaApplied=" + deltaApplied + "]" + super.toString();
    }
  }
}
