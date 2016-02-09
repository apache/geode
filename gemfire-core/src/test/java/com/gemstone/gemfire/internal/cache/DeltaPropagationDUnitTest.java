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

import java.io.File;
import java.util.Properties;

import com.gemstone.gemfire.DeltaTestImpl;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.PartitionedRegionLocalMaxMemoryDUnitTest.TestObject1;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.lru.EnableLRU;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.cache.tier.sockets.ConflationDUnitTest;
import com.gemstone.gemfire.internal.tcp.ConnectionTable;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * @since 6.1
 */
public class DeltaPropagationDUnitTest extends DistributedTestCase {
  private final static Compressor compressor = SnappyCompressor.getDefaultInstance();
  
  protected static Cache cache = null;

  protected static Pool pool = null;

  protected static VM VM0 = null;

  protected static VM VM1 = null;

  protected static VM VM2 = null;

  protected static VM VM3 = null;

  private static int PORT1;

  private static int PORT2;

  private static final String regionName = "DeltaPropagationDUnitTest";

  private static LogWriter logger = null;

  public static final int EVENTS_SIZE = 6;

  private static boolean lastKeyReceived = false;

  private static boolean markerReceived = false;

  private static int numOfCreates;

  private static int numOfUpdates;

  private static int numOfInvalidates;

  private static int numOfDestroys;

  private static int numOfEvents;

  private static DeltaTestImpl[] deltaPut = new DeltaTestImpl[EVENTS_SIZE];

  private static boolean areListenerResultsValid = true;
  
  private static boolean closeCache = false;

  private static StringBuffer listenerError = new StringBuffer("");

  public static String DELTA_KEY = "DELTA_KEY";

  public static String LAST_KEY = "LAST_KEY";

  public static final int NO_LISTENER = 0;

  public static final int CLIENT_LISTENER = 1;

  public static final int SERVER_LISTENER = 2;

  public static final int C2S2S_SERVER_LISTENER = 3;
  
  public static final int LAST_KEY_LISTENER = 4;
  
  public static final int DURABLE_CLIENT_LISTENER = 5;

  public static final int CLIENT_LISTENER_2 = 6;

  public static final String CREATE = "CREATE";

  public static final String UPDATE = "UPDATE";

  public static final String INVALIDATE = "INVALIDATE";

  public static final String DESTROY = "DESTROY";

  /**
   * @param name
   */
  public DeltaPropagationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();

    final Host host = Host.getHost(0);
    VM0 = host.getVM(0);
    VM1 = host.getVM(1);
    VM2 = host.getVM(2);
    VM3 = host.getVM(3);

    VM0.invoke(DeltaPropagationDUnitTest.class, "resetAll");
    VM1.invoke(DeltaPropagationDUnitTest.class, "resetAll");
    VM2.invoke(DeltaPropagationDUnitTest.class, "resetAll");
    VM3.invoke(DeltaPropagationDUnitTest.class, "resetAll");
    DeltaPropagationDUnitTest.resetAll();
  }

  @Override
  protected final void preTearDown() throws Exception {
    DeltaPropagationDUnitTest.closeCache();
    VM2.invoke(DeltaPropagationDUnitTest.class, "closeCache");
    VM3.invoke(DeltaPropagationDUnitTest.class, "closeCache");

    // Unset the isSlowStartForTesting flag
    VM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    VM1.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    // then close the servers
    VM0.invoke(DeltaPropagationDUnitTest.class, "closeCache");
    VM1.invoke(DeltaPropagationDUnitTest.class, "closeCache");
    disconnectAllFromDS();
  }

  public void testS2CSuccessfulDeltaPropagationWithCompression() throws Exception {
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache", new Object[] {
            HARegionQueue.HA_EVICTION_POLICY_NONE, new Integer(1),
            new Integer(NO_LISTENER), Boolean.FALSE, compressor })).intValue();
    
    VM0.invoke(new SerializableRunnable() {
      public void run() { assertTrue(cache.getRegion(regionName).getAttributes().getCompressor() != null); }
      });

    createClientCache(new Integer(PORT1), new Integer(-1), "0", new Integer(
        CLIENT_LISTENER));
    
    registerInterestListAll();

    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");
    prepareDeltas();

    VM0.invoke(DeltaPropagationDUnitTest.class, "createAndUpdateDeltas");
    
    waitForLastKey();
    
    long toDeltas = ((Long)VM0.invoke(DeltaTestImpl.class, "getToDeltaInvokations"));
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations();
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were "
        + toDeltas, toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were "
        + fromDeltas, fromDeltas == toDeltas);

    verifyData(2, EVENTS_SIZE - 1);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }
  
  public void testS2CSuccessfulDeltaPropagation() throws Exception {
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    
    createClientCache(new Integer(PORT1), new Integer(-1), "0", new Integer(
        CLIENT_LISTENER));
    registerInterestListAll();

    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");
    prepareDeltas();

    VM0.invoke(DeltaPropagationDUnitTest.class, "createAndUpdateDeltas");

    waitForLastKey();
    
    long toDeltas = ((Long)VM0.invoke(DeltaTestImpl.class, "getToDeltaInvokations"));
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations();
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were "
        + toDeltas, toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were "
        + fromDeltas, fromDeltas == toDeltas);

    verifyData(2, EVENTS_SIZE - 1);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }

  public void testS2CFailureInToDeltaMethod() throws Exception {
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    
    createClientCache(new Integer(PORT1), new Integer(-1), "0", new Integer(
        CLIENT_LISTENER_2));
    registerInterestListAll();

    VM0.invoke(DeltaPropagationDUnitTest.class,
        "prepareErroneousDeltasForToDelta");
    prepareErroneousDeltasForToDelta();

    VM0.invoke(DeltaPropagationDUnitTest.class, "createAndUpdateDeltas");

    waitForLastKey();

    long toDeltas = ((Long)VM0.invoke(DeltaTestImpl.class, "getToDeltaInvokations"));
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations();
    long toDeltafailures = ((Long)VM0.invoke(DeltaTestImpl.class, "getToDeltaFailures"));
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were "
        + toDeltas, toDeltas == (EVENTS_SIZE - 1));
    assertTrue(
        (EVENTS_SIZE - 1 - 1/*
                             * This is because the one failed in toDelta will be
                             * sent as full value. So client will not see it as
                             * 'delta'.
                             */)
            + " deltas were to be received but were " + fromDeltas,
        fromDeltas == (EVENTS_SIZE - 1 - 1));
    assertTrue(1 + " deltas were to be failed while extracting but were "
        + toDeltafailures, toDeltafailures == 1);

    verifyData(2, EVENTS_SIZE - 1 - 1 /*Full value no more sent if toDelta() fails*/);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }

  public void testS2CFailureInFromDeltaMethod() throws Exception {
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    
    createClientCache(new Integer(PORT1), new Integer(-1), "0", new Integer(
        CLIENT_LISTENER));
    registerInterestListAll();

    VM0.invoke(DeltaPropagationDUnitTest.class,
        "prepareErroneousDeltasForFromDelta");
    prepareErroneousDeltasForFromDelta();

    VM0.invoke(DeltaPropagationDUnitTest.class, "createAndUpdateDeltas");

    waitForLastKey();

    long toDeltas = ((Long)VM0.invoke(DeltaTestImpl.class, "getToDeltaInvokations"));
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations();
    long fromDeltafailures = DeltaTestImpl.getFromDeltaFailures();
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were "
        + toDeltas, toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were "
        + fromDeltas, fromDeltas == toDeltas);
    assertTrue(1 + " deltas were to be failed while applying but were "
        + fromDeltafailures, fromDeltafailures == 1);

    verifyData(2, EVENTS_SIZE - 1);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }

  public void testS2CWithOldValueAtClientOverflownToDisk() throws Exception {
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    
    EvictionAttributes evAttr = EvictionAttributes.createLRUEntryAttributes(1,
        EvictionAction.OVERFLOW_TO_DISK);

    createClientCache(new Integer(PORT1), new Integer(-1), "0",
        Boolean.TRUE/* add listener */, evAttr);
    registerInterestListAll();

    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");
    prepareDeltas();

    VM0.invoke(DeltaPropagationDUnitTest.class, "createDelta");
    VM0.invoke(DeltaPropagationDUnitTest.class, "createAnEntry");
    Thread.sleep(5000); // TODO: Find a better 'n reliable alternative
    // assert overflow occured on client vm
    verifyOverflowOccured(1L, 2);
    VM0.invoke(DeltaPropagationDUnitTest.class, "updateDelta");

    waitForLastKey();

    long toDeltas = ((Long)VM0.invoke(DeltaTestImpl.class,
        "getToDeltaInvokations")).longValue();
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations().longValue();

    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were "
        + toDeltas, toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were "
        + fromDeltas, fromDeltas == (EVENTS_SIZE - 1));

    verifyData(3, EVENTS_SIZE - 1);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }

  public void testS2CWithLocallyDestroyedOldValueAtClient() throws Exception {
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    
    EvictionAttributes evAttr = EvictionAttributes.createLRUEntryAttributes(1,
        EvictionAction.LOCAL_DESTROY);

    createClientCache(new Integer(PORT1), new Integer(-1), "0",
        Boolean.TRUE/* add listener */, evAttr);
    registerInterestListAll();

    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");
    prepareDeltas();

    VM0.invoke(DeltaPropagationDUnitTest.class, "createDelta");
    VM0.invoke(DeltaPropagationDUnitTest.class, "createAnEntry");
    Thread.sleep(5000); // TODO: Find a better 'n reliable alternative
    // assert overflow occured on client vm
    verifyOverflowOccured(1L, 1);
    VM0.invoke(DeltaPropagationDUnitTest.class, "updateDelta");

    waitForLastKey();

    long toDeltas = ((Long)VM0.invoke(DeltaTestImpl.class,
        "getToDeltaInvokations")).longValue();
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations().longValue();

    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were "
        + toDeltas, toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1 - 1/* destroyed */)
        + " deltas were to be received but were " + fromDeltas,
        fromDeltas == (EVENTS_SIZE - 1 - 1));

    verifyData(4, EVENTS_SIZE - 2);
  }

  public void testS2CWithInvalidatedOldValueAtClient() throws Exception {
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    
    createClientCache(new Integer(PORT1), new Integer(-1), "0", new Integer(
        CLIENT_LISTENER));
    registerInterestListAll();

    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");
    prepareDeltas();

    VM0.invoke(DeltaPropagationDUnitTest.class, "createDelta");
    VM0.invoke(DeltaPropagationDUnitTest.class, "invalidateDelta");
    VM0.invoke(DeltaPropagationDUnitTest.class, "updateDelta");

    waitForLastKey();

    long toDeltas = ((Long)VM0.invoke(DeltaTestImpl.class,
        "getToDeltaInvokations")).longValue();
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations().longValue();

    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were "
        + toDeltas, toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1 - 1/* invalidated */)
        + " deltas were to be received but were " + fromDeltas,
        fromDeltas == (EVENTS_SIZE - 1 - 1));

    verifyData(2, EVENTS_SIZE - 1);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }

  public void testS2CDeltaPropagationWithClientConflationON() throws Exception {
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    
    createClientCache(new Integer(PORT1), new Integer(-1), "0",
        DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_ON, new Integer(
            LAST_KEY_LISTENER), null, null);

    registerInterestListAll();
    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");

    VM0.invoke(DeltaPropagationDUnitTest.class, "createAndUpdateDeltas");

    waitForLastKey();

    // TODO: (Amogh) get CCPStats and assert 0 deltas sent.
    assertTrue("Delta Propagation feature used.", DeltaTestImpl
        .getFromDeltaInvokations().longValue() == 0);
  }

  public void testS2CDeltaPropagationWithServerConflationON() throws Exception {
    VM0.invoke(DeltaPropagationDUnitTest.class, "closeCache");
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache", new Object[] {
            HARegionQueue.HA_EVICTION_POLICY_MEMORY, Integer.valueOf(1),
            Integer.valueOf(NO_LISTENER), Boolean.TRUE /* conflate */, null}))
        .intValue();

    createClientCache(new Integer(PORT1), new Integer(-1), "0",
        DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT, new Integer(
            LAST_KEY_LISTENER), null, null);

    VM3.invoke(DeltaPropagationDUnitTest.class, "createClientCache",
        new Object[] { new Integer(PORT1), new Integer(-1), "0",
            DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_OFF,
            new Integer(LAST_KEY_LISTENER), null, null });

    registerInterestListAll();
    VM3.invoke(DeltaPropagationDUnitTest.class, "registerInterestListAll");

    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");
    VM0.invoke(DeltaPropagationDUnitTest.class, "createAndUpdateDeltas");

    waitForLastKey();
    VM3.invoke(DeltaPropagationDUnitTest.class, "waitForLastKey");

    // TODO: (Amogh) use CCPStats.
    assertTrue("Delta Propagation feature used.", DeltaTestImpl
        .getFromDeltaInvokations().longValue() == 0);
    long fromDeltaInvocations = (Long)VM3.invoke(DeltaTestImpl.class, "getFromDeltaInvokations");
    assertTrue("Expected " + (EVENTS_SIZE - 1) + " fromDelta() invocations but found " + "",
        (fromDeltaInvocations == (EVENTS_SIZE - 1)));
  }

  public void testS2CDeltaPropagationWithOnlyCreateEvents() throws Exception {
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    
    createClientCache(new Integer(PORT1), new Integer(-1), "0", new Integer(
        LAST_KEY_LISTENER));
    registerInterestListAll();

    VM0.invoke(DeltaPropagationDUnitTest.class, "createDeltas");
    waitForLastKey();

    assertTrue("Delta Propagation feature used.", ((Long)VM0.invoke(
        DeltaTestImpl.class, "getToDeltaInvokations")).longValue() == 0);
    assertTrue("Delta Propagation feature used.", DeltaTestImpl
        .getFromDeltaInvokations().longValue() == 0);
  }

  /**
   * Tests that an update on a server with full Delta object causes distribution
   * of the full Delta instance, and not its delta bits, to other peers, even if
   * that instance's <code>hasDelta()</code> returns true.
   * 
   * @throws Exception
   */
  public void testC2S2SDeltaPropagation() throws Exception {
    prepareDeltas();
    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");
    VM1.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");

    DeltaTestImpl val = deltaPut[1];
    VM0.invoke(DeltaPropagationDUnitTest.class, "closeCache");

    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache", new Object[] {
            HARegionQueue.HA_EVICTION_POLICY_MEMORY, new Integer(1),
            new Integer(C2S2S_SERVER_LISTENER) })).intValue();
    PORT2 = ((Integer)VM1.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache", new Object[] {
            HARegionQueue.HA_EVICTION_POLICY_MEMORY, new Integer(1),
            new Integer(C2S2S_SERVER_LISTENER) })).intValue();

    createClientCache(new Integer(PORT1), new Integer(-1), "0", new Integer(
        NO_LISTENER));

    Region r = cache.getRegion("/" + regionName);
    assertNotNull(r);

    r.create(DELTA_KEY, deltaPut[0]);

    // Invalidate the value at both the servers.
    VM0.invoke(DeltaPropagationDUnitTest.class, "doLocalOp", new Object[] {
        INVALIDATE, regionName, DELTA_KEY });
    VM1.invoke(DeltaPropagationDUnitTest.class, "doLocalOp", new Object[] {
        INVALIDATE, regionName, DELTA_KEY });

    VM0.invoke(DeltaPropagationDUnitTest.class, "assertOp", new Object[] {
        INVALIDATE, new Integer(1) });
    VM1.invoke(DeltaPropagationDUnitTest.class, "assertOp", new Object[] {
        INVALIDATE, new Integer(1) });

    r.put(DELTA_KEY, val);
    Thread.sleep(5000);

    // Assert that VM0 distributed val as full value to VM1.
    VM1.invoke(DeltaPropagationDUnitTest.class, "assertValue", new Object[] {
        regionName, DELTA_KEY, val });

    assertTrue("Delta Propagation feature used.", !((Boolean)VM0.invoke(
        DeltaTestImpl.class, "deltaFeatureUsed")).booleanValue());
    assertTrue("Delta Propagation feature used.", !((Boolean)VM1.invoke(
        DeltaTestImpl.class, "deltaFeatureUsed")).booleanValue());
    assertTrue("Delta Propagation feature NOT used.", DeltaTestImpl
        .deltaFeatureUsed());
  }

  public void testS2S2CDeltaPropagationWithHAOverflow() throws Exception {
    prepareDeltas();
    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");
    VM1.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");

    VM0.invoke(DeltaPropagationDUnitTest.class, "closeCache");

    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache", new Object[] {
            HARegionQueue.HA_EVICTION_POLICY_NONE, new Integer(1) }))
        .intValue();
    PORT2 = ((Integer)VM1.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache", new Object[] {
            HARegionQueue.HA_EVICTION_POLICY_ENTRY, new Integer(1) }))
        .intValue();

    VM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "60000" });
    VM1.invoke(ConflationDUnitTest.class, "setIsSlowStart",
        new Object[] { "60000" });

    createClientCache(new Integer(PORT2), new Integer(-1), "0", new Integer(
        CLIENT_LISTENER));

    Region r = cache.getRegion("/" + regionName);
    assertNotNull(r);
    r.registerInterest("ALL_KEYS");

    VM0.invoke(DeltaPropagationDUnitTest.class, "createAndUpdateDeltas");
    VM1.invoke(DeltaPropagationDUnitTest.class, "confirmEviction",
        new Object[] { new Integer(PORT2) });

    VM1.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");

    waitForLastKey();

    long toDeltasOnServer1 = ((Long)VM0.invoke(DeltaTestImpl.class,
        "getToDeltaInvokations")).longValue();
    long fromDeltasOnServer2 = ((Long)VM1.invoke(DeltaTestImpl.class,
        "getFromDeltaInvokations")).longValue();
    long toDeltasOnServer2 = ((Long)VM1.invoke(DeltaTestImpl.class,
        "getToDeltaInvokations")).longValue();
    long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations()
        .longValue();

    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were "
        + toDeltasOnServer1, toDeltasOnServer1 == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were "
        + fromDeltasOnServer2, fromDeltasOnServer2 == (EVENTS_SIZE - 1));
    assertTrue("0 toDelta() were to be invoked but were "
        + toDeltasOnServer2, toDeltasOnServer2 == 0);
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were "
        + fromDeltasOnClient, fromDeltasOnClient == (EVENTS_SIZE - 1));
  }

  public void testS2CDeltaPropagationWithGIIAndFailover() throws Exception {
    prepareDeltas();
    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");
    VM1.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");
    VM2.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");

    VM0.invoke(DeltaPropagationDUnitTest.class, "closeCache");

    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache", new Object[] {
            HARegionQueue.HA_EVICTION_POLICY_NONE, new Integer(1),
            new Integer(NO_LISTENER) })).intValue();
    PORT2 = ((Integer)VM1.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache", new Object[] {
            HARegionQueue.HA_EVICTION_POLICY_NONE, new Integer(1),
            new Integer(NO_LISTENER) })).intValue();
    int port3 = ((Integer)VM2.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache", new Object[] {
            HARegionQueue.HA_EVICTION_POLICY_NONE, new Integer(1),
            new Integer(NO_LISTENER) })).intValue();

    // Do puts after slowing the dispatcher.
    try {
      VM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
          new Object[] { "60000" });
      VM1.invoke(ConflationDUnitTest.class, "setIsSlowStart",
          new Object[] { "60000" });
      VM2.invoke(ConflationDUnitTest.class, "setIsSlowStart",
          new Object[] { "60000" });
  
      createClientCache(new int[] { PORT1, PORT2, port3 }, "1",
          DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT, new Integer(
              CLIENT_LISTENER), null, null);
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.registerInterest("ALL_KEYS");
  
      VM primary = (((PoolImpl)pool).getPrimaryPort() == PORT1) ? VM0
          : ((((PoolImpl)pool).getPrimaryPort() == PORT2) ? VM1 : VM2);
  
      primary.invoke(DeltaPropagationDUnitTest.class, "createAndUpdateDeltas");
      Thread.sleep(5000);
  
      primary.invoke(DeltaPropagationDUnitTest.class, "closeCache");
      Thread.sleep(5000);
  
      primary = (((PoolImpl)pool).getPrimaryPort() == PORT1) ? VM0
          : ((((PoolImpl)pool).getPrimaryPort() == PORT2) ? VM1 : VM2);

      VM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
      VM1.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
      VM2.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");

      primary.invoke(DeltaPropagationDUnitTest.class, "closeCache");
      Thread.sleep(5000);
  
      primary = (((PoolImpl)pool).getPrimaryPort() == PORT1) ? VM0
          : ((((PoolImpl)pool).getPrimaryPort() == PORT2) ? VM1 : VM2);
  
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("waiting for client to receive last_key");
      waitForLastKey();
  
      long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations()
          .longValue();
      assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were "
          + fromDeltasOnClient, fromDeltasOnClient == (EVENTS_SIZE - 1));
    }
    finally {
      VM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
      VM1.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
      VM2.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    }
  }

  public void testBug40165ClientReconnects() throws Exception {
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    
    /**
     * 1. Create a cache server with slow dispatcher
     * 2. Start a durable client with a custom cache listener
     *    which shuts itself down as soon as it recieves a marker message.
     * 3. Do some puts on the server region
     * 4. Let the dispatcher start dispatching
     * 5. Verify that durable client is disconnected as soon as it processes the marker.
     *    Server will retain its queue which has some events (containing deltas) in it.
     * 6. Restart the durable client without the self-destructing listener.
     * 7. Wait till the durable client processes all its events.
     * 8. Verify that no deltas are received by it. 
     */

    // Step 0
    prepareDeltas();
    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");

    // Step 1
    try {
      VM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
          new Object[] { "60000" });
  
      // Step 2
      String durableClientId = getName() + "_client";
      PoolFactory pf = PoolManager.createFactory();
      pf.addServer("localhost", PORT1)
        .setSubscriptionEnabled(true)
        .setSubscriptionAckInterval(1);
      ((PoolFactoryImpl)pf).getPoolAttributes();
  
      Properties properties = new Properties();
      properties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      properties.setProperty(DistributionConfig.LOCATORS_NAME, "");
      properties.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME, durableClientId);
      properties.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME, String.valueOf(60));
  
      createDurableCacheClient(((PoolFactoryImpl)pf).getPoolAttributes(),
          regionName, properties, new Integer(DURABLE_CLIENT_LISTENER), Boolean.TRUE);
  
      // Step 3
      VM0.invoke(DeltaPropagationDUnitTest.class, "doPuts");

      // Step 4
      VM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");

      // Step 5
      //verifyDurableClientDisconnected();
      Thread.sleep(5000);
      
      // Step 6
      createDurableCacheClient(((PoolFactoryImpl)pf).getPoolAttributes(),
          regionName, properties, new Integer(DURABLE_CLIENT_LISTENER), Boolean.FALSE);
      
      // Step 7
      waitForLastKey();
      
      // Step 8
      long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations()
          .longValue();
      assertTrue("No deltas were to be received but received: "
          + fromDeltasOnClient, fromDeltasOnClient < 1);
    } finally {
      // Step 4
      VM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    }

  }

  public void testBug40165ClientFailsOver() throws Exception {
    PORT1 = ((Integer)VM0.invoke(DeltaPropagationDUnitTest.class,
        "createServerCache",
        new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
    
    /**
     * 1. Create two cache servers with slow dispatcher
     * 2. Start a durable client with a custom cache listener
     * 3. Do some puts on the server region
     * 4. Let the dispatcher start dispatching
     * 5. Wait till the durable client receives marker from its primary.
     * 6. Kill the primary server, so that the second one becomes primary.
     * 7. Wait till the durable client processes all its events.
     * 8. Verify that expected number of deltas are received by it. 
     */

    // Step 0
    prepareDeltas();
    VM0.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");
    VM1.invoke(DeltaPropagationDUnitTest.class, "prepareDeltas");

    try {
      // Step 1
      VM0.invoke(ConflationDUnitTest.class, "setIsSlowStart",
          new Object[] { "60000" });
      PORT2 = ((Integer)VM1.invoke(DeltaPropagationDUnitTest.class,
          "createServerCache",
          new Object[] { HARegionQueue.HA_EVICTION_POLICY_MEMORY })).intValue();
      VM1.invoke(ConflationDUnitTest.class, "setIsSlowStart",
          new Object[] { "60000" });
  
      // Step 2
      String durableClientId = getName() + "_client";
      PoolFactory pf = PoolManager.createFactory();
      pf.addServer("localhost", PORT1)
        .addServer("localhost", PORT2)
        .setSubscriptionEnabled(true)
        .setSubscriptionAckInterval(1)
        .setSubscriptionRedundancy(2);
      ((PoolFactoryImpl)pf).getPoolAttributes();
  
      Properties properties = new Properties();
      properties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      properties.setProperty(DistributionConfig.LOCATORS_NAME, "");
      properties.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME, durableClientId);
      properties.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME, String.valueOf(60));
  
      createDurableCacheClient(((PoolFactoryImpl)pf).getPoolAttributes(),
          regionName, properties, new Integer(DURABLE_CLIENT_LISTENER), Boolean.FALSE);
  
      // Step 3
      VM0.invoke(DeltaPropagationDUnitTest.class, "doPuts");
    } finally {
      // Step 4
      VM0.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
      VM1.invoke(ConflationDUnitTest.class, "unsetIsSlowStart");
    }

    // Step 5
    VM pVM = (((PoolImpl)pool).getPrimaryPort() == PORT1) ? VM0 : VM1;
    while (!markerReceived) {
      Thread.sleep(50);
    }

    // Step 6
    pVM.invoke(DeltaPropagationDUnitTest.class, "closeCache");
    Thread.sleep(5000);
    
    // Step 7
    waitForLastKey();
    
    // Step 8
    long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations()
        .longValue();
    assertTrue("Atleast 99 deltas were to be received but received: "
        + fromDeltasOnClient, fromDeltasOnClient >= 99);
  }

  public static void doLocalOp(String op, String rName, String key) {
    try {
      Region r = cache.getRegion("/" + rName);
      assertNotNull(r);
      if (INVALIDATE.equals(op)) {
        r.localInvalidate(key);
      }
      else if (DESTROY.equals(op)) {
        r.localDestroy(key);
      }
    }
    catch (Exception e) {
      com.gemstone.gemfire.test.dunit.Assert.fail("failed in doLocalOp()", e);
    }
  }

  public static void assertOp(String op, Integer num) {
    final int expected = num.intValue();
    WaitCriterion wc = null;
    if (INVALIDATE.equals(op)) {
      wc = new WaitCriterion() {
        public boolean done() {
          return numOfInvalidates == expected;
        }

        public String description() {
          return "numOfInvalidates was expected to be " + expected + " but is "
              + numOfInvalidates;
        }
      };
    }
    else if (DESTROY.equals(op)) {
      wc = new WaitCriterion() {
        public boolean done() {
          return numOfInvalidates == expected;
        }

        public String description() {
          return "numOfDestroys was expected to be " + expected + " but is "
              + numOfDestroys;
        }
      };
    }
    Wait.waitForCriterion(wc, 5 * 1000, 100, true);
  }

  public static void assertValue(String rName, String key, Object expected) {
    try {
      Region r = cache.getRegion("/" + rName);
      assertNotNull(r);
      Object value = r.getEntry(key).getValue();
      assertTrue("Value against " + key + " is " + value + ". It should be "
          + expected, expected.equals(value));
    }
    catch (Exception e) {
      com.gemstone.gemfire.test.dunit.Assert.fail("failed in assertValue()", e);
    }
  }

  public static void confirmEviction(Integer port) {
    final EnableLRU cc = ((VMLRURegionMap)((LocalRegion)cache
        .getRegion(Region.SEPARATOR
            + CacheServerImpl.generateNameForClientMsgsRegion(port))).entries)
        ._getCCHelper();

    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return cc.getStats().getEvictions() > 0;
      }

      public String description() {
        return "HA Overflow did not occure.";
      }
    };
    Wait.waitForCriterion(wc, 10 * 1000, 100, true);
  }

  public static void waitForLastKey() {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return DeltaPropagationDUnitTest.isLastKeyReceived();
      }

      public String description() {
        return "Last key NOT received.";
      }
    };
    Wait.waitForCriterion(wc, 10 * 1000, 100, true);
  }

  public static void prepareDeltas() {
    for (int i = 0; i < EVENTS_SIZE; i++) {
      deltaPut[i] = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
          new TestObject1("0", 0));
    }
    deltaPut[1].setIntVar(5);
    deltaPut[2].setIntVar(5);
    deltaPut[3].setIntVar(5);
    deltaPut[4].setIntVar(5);
    deltaPut[5].setIntVar(5);

    deltaPut[2].resetDeltaStatus();
    deltaPut[2].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltaPut[3].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltaPut[4].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltaPut[5].setByteArr(new byte[] { 1, 2, 3, 4, 5 });

    deltaPut[3].resetDeltaStatus();
    deltaPut[3].setDoubleVar(new Double(5));
    deltaPut[4].setDoubleVar(new Double(5));
    deltaPut[5].setDoubleVar(new Double(5));

    deltaPut[4].resetDeltaStatus();
    deltaPut[4].setStr("str changed");
    deltaPut[5].setStr("str changed");

    deltaPut[5].resetDeltaStatus();
    deltaPut[5].setIntVar(100);
    deltaPut[5].setTestObj(new TestObject1("CHANGED", 100));
  }

  public static void prepareErroneousDeltasForToDelta() {
    for (int i = 0; i < EVENTS_SIZE; i++) {
      deltaPut[i] = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
          new TestObject1("0", 0));
    }
    deltaPut[1].setIntVar(5);
    deltaPut[2].setIntVar(5);
    deltaPut[3].setIntVar(DeltaTestImpl.ERRONEOUS_INT_FOR_TO_DELTA);
    deltaPut[4].setIntVar(5);
    deltaPut[5].setIntVar(5);

    deltaPut[2].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltaPut[3].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltaPut[4].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltaPut[5].setByteArr(new byte[] { 1, 2, 3, 4, 5 });

    deltaPut[3].setDoubleVar(new Double(5));
    deltaPut[4].setDoubleVar(new Double(5));
    deltaPut[5].setDoubleVar(new Double(5));

    deltaPut[4].setStr("str changed");
    deltaPut[5].setStr("str changed");

    deltaPut[5].setIntVar(100);
    deltaPut[5].setTestObj(new TestObject1("CHANGED", 100));
  }

  public static void prepareErroneousDeltasForFromDelta() {
    for (int i = 0; i < EVENTS_SIZE; i++) {
      deltaPut[i] = new DeltaTestImpl(0, "0", new Double(0), new byte[0],
          new TestObject1("0", 0));
    }
    deltaPut[1].setIntVar(5);
    deltaPut[2].setIntVar(5);
    deltaPut[3].setIntVar(5);
    deltaPut[4].setIntVar(5);
    deltaPut[5].setIntVar(5);

    deltaPut[2].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltaPut[3].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltaPut[4].setByteArr(new byte[] { 1, 2, 3, 4, 5 });
    deltaPut[5].setByteArr(new byte[] { 1, 2, 3, 4, 5 });

    deltaPut[3].setDoubleVar(new Double(5));
    deltaPut[4].setDoubleVar(new Double(5));
    deltaPut[5].setDoubleVar(new Double(5));

    deltaPut[4].setStr("str changed");
    deltaPut[5].setStr(DeltaTestImpl.ERRONEOUS_STRING_FOR_FROM_DELTA);

    deltaPut[5].setIntVar(100);
    deltaPut[5].setTestObj(new TestObject1("CHANGED", 100));
  }
  
  public static void doPuts() {
    doPuts(100);
  }
  
  public static void doPuts(Integer num) {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      for (int i = 0; i < num; i++) {
        DeltaTestImpl val = new DeltaTestImpl();
        val.setStr("" + i);
        r.put(DELTA_KEY, val);
      }
      r.put(LAST_KEY, "");
    }
    catch (Exception ex) {
      com.gemstone.gemfire.test.dunit.Assert.fail("failed in createDelta()", ex);
    }
  }

  public static void createAndUpdateDeltas() {
    createDelta();
    updateDelta();
  }

  public static void createDelta() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      r.create(DELTA_KEY, deltaPut[0]);
    }
    catch (Exception ex) {
      com.gemstone.gemfire.test.dunit.Assert.fail("failed in createDelta()", ex);
    }
  }

  public static void updateDelta() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      for (int i = 1; i < EVENTS_SIZE; i++) {
        try {
          r.put(DELTA_KEY, deltaPut[i]);
        }
        catch (InvalidDeltaException ide) {
          assertTrue(
              "InvalidDeltaException not expected for deltaPut[" + i + "]",
              deltaPut[i].getIntVar() == DeltaTestImpl.ERRONEOUS_INT_FOR_TO_DELTA);
        }
      }
      r.put(LAST_KEY, "");
    }
    catch (Exception ex) {
      com.gemstone.gemfire.test.dunit.Assert.fail("failed in updateDelta()", ex);
    }
  }

  public static void createDeltas() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      for (int i = 0; i < 100; i++) {
        r.create(DELTA_KEY + i, new DeltaTestImpl());
      }
      r.create(LAST_KEY, "");
    }
    catch (Exception ex) {
      com.gemstone.gemfire.test.dunit.Assert.fail("failed in createDeltas()", ex);
    }
  }

  public static void createAnEntry() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      r.create("KEY-A", "I push the delta out to disk :)");
    }
    catch (Exception ex) {
      com.gemstone.gemfire.test.dunit.Assert.fail("failed in createAnEntry()", ex);
    }
  }

  public static void invalidateDelta() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      r.invalidate(DELTA_KEY);
    }
    catch (Exception ex) {
      com.gemstone.gemfire.test.dunit.Assert.fail("failed in invalidateDelta()", ex);
    }
  }

  public static void verifyOverflowOccured(long evictions, int regionsize) {
    EnableLRU cc = ((VMLRURegionMap)((LocalRegion)cache.getRegion(regionName)).entries)
        ._getCCHelper();
    Assert.assertTrue(cc.getStats().getEvictions() == evictions,
        "Number of evictions expected to be " + evictions + " but was "
            + cc.getStats().getEvictions());
    int rSize = ((LocalRegion)cache.getRegion(regionName)).getRegionMap()
        .size();
    Assert.assertTrue(rSize == regionsize, "Region size expected to be "
        + regionsize + " but was " + rSize);
  }

  public static void verifyData(int creates, int updates) {
    assertEquals(creates, numOfCreates);
    assertEquals(updates, numOfUpdates);
  }

  public static Integer createServerCache(String ePolicy) throws Exception {
    return createServerCache(ePolicy, Integer.valueOf(1));
  }

  public static Integer createServerCache(String ePolicy, Integer cap)
      throws Exception {
    return createServerCache(ePolicy, cap, new Integer(NO_LISTENER));
  }

  public static Integer createServerCache(String ePolicy, Integer cap,
      Integer listenerCode) throws Exception {
    return createServerCache(ePolicy, cap, listenerCode, Boolean.FALSE, null);
  }

  public static Integer createServerCache(String ePolicy, Integer cap,
      Integer listenerCode, Boolean conflate, Compressor compressor) throws Exception {
    ConnectionTable.threadWantsSharedResources();
    new DeltaPropagationDUnitTest("temp").createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setEnableSubscriptionConflation(conflate);
    if (listenerCode.intValue() != 0) {
      factory.addCacheListener(getCacheListener(listenerCode));
    }
    if (compressor != null) {
      factory.setCompressor(compressor);
    }
    if (listenerCode.intValue() == C2S2S_SERVER_LISTENER) {
      factory.setScope(Scope.DISTRIBUTED_NO_ACK);
      factory.setDataPolicy(DataPolicy.NORMAL);
      factory.setConcurrencyChecksEnabled(false);
      RegionAttributes attrs = factory.create();
      Region r = cache.createRegion(regionName, attrs);
      logger = cache.getLogger();
      r.create(DELTA_KEY, deltaPut[0]);
    }
    else {
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setConcurrencyChecksEnabled(false);
      RegionAttributes attrs = factory.create();
      cache.createRegion(regionName, attrs);
      logger = cache.getLogger();
    }

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

  public static CacheListener getCacheListener(Integer code) {
    CacheListener listener = null;
    switch (code.intValue()) {
      case 0:
        break;
      case SERVER_LISTENER:
        //listener = new CacheListenerAdapter() {};
        break;
      case CLIENT_LISTENER:
        listener = new CacheListenerAdapter() {
          public void afterCreate(EntryEvent event) {
            numOfCreates++;
            logger.fine("Create Event: <" + event.getKey() + ", "
                + event.getNewValue() + ">");
            if (DELTA_KEY.equals(event.getKey())
                && !deltaPut[0].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("Create event:\n |-> sent: " + deltaPut[0]
                  + "\n |-> rcvd: " + event.getNewValue() + "\n");
            }
            else if (LAST_KEY.equals(event.getKey())) {
              lastKeyReceived = true;
            }
          }

          public void afterUpdate(EntryEvent event) {
            numOfUpdates++;
            logger
                .fine("Update Event: <" + event.getKey() + ", "
                    + event.getNewValue() + ">" + ", numOfUpdates: "
                    + numOfUpdates);
            if (!deltaPut[numOfUpdates].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("\nUpdate event(" + numOfUpdates
                  + "):\n |-> sent: " + deltaPut[numOfUpdates]
                  + "\n |-> recd: " + event.getNewValue());
            }
          }
        };
        break;
      case CLIENT_LISTENER_2:
        listener = new CacheListenerAdapter() {
          public void afterCreate(EntryEvent event) {
            numOfCreates++;
            logger.fine("Create Event: <" + event.getKey() + ", "
                + event.getNewValue() + ">");
            if (DELTA_KEY.equals(event.getKey())
                && !deltaPut[0].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("Create event:\n |-> sent: " + deltaPut[0]
                  + "\n |-> rcvd: " + event.getNewValue() + "\n");
            }
            else if (LAST_KEY.equals(event.getKey())) {
              lastKeyReceived = true;
            }
          }

          public void afterUpdate(EntryEvent event) {
            int tmp = ++numOfUpdates;
            logger
                .fine("Update Event: <" + event.getKey() + ", "
                    + event.getNewValue() + ">" + ", numOfUpdates: "
                    + numOfUpdates);
            // Hack to ignore illegal delta put
            tmp = (tmp >= 3) ? ++tmp : tmp;
            if (!deltaPut[tmp].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("\nUpdate event(" + numOfUpdates
                  + "):\n |-> sent: " + deltaPut[tmp]
                  + "\n |-> recd: " + event.getNewValue());
            }
          }
        };
        break;
      case C2S2S_SERVER_LISTENER:
        listener = new CacheListenerAdapter() {
          public void afterCreate(EntryEvent event) {
            numOfCreates++;
            logger.fine("Create Event: <" + event.getKey() + ", "
                + event.getNewValue() + ">");
            if (LAST_KEY.equals(event.getKey())) {
              lastKeyReceived = true;
            }
          }

          public void afterUpdate(EntryEvent event) {
            numOfUpdates++;
            logger
                .fine("Update Event: <" + event.getKey() + ", "
                    + event.getNewValue() + ">" + ", numOfUpdates: "
                    + numOfUpdates);
          }

          public void afterInvalidate(EntryEvent event) {
            numOfInvalidates++;
            logger.fine("Invalidate Event: <" + event.getKey() + ", "
                + event.getOldValue() + ">" + ", numOfInvalidates: "
                + numOfInvalidates);
          }

          public void afterDestroy(EntryEvent event) {
            numOfDestroys++;
            logger.fine("Destroy Event: <" + event.getKey() + ", "
                + event.getOldValue() + ">" + ", numOfDestroys: "
                + numOfDestroys);
          }
        };
        break;
      case LAST_KEY_LISTENER:
        listener = new CacheListenerAdapter() {
          public void afterCreate(EntryEvent event) {
            if (LAST_KEY.equals(event.getKey())) {
              lastKeyReceived = true;
            }
          }
        };
        break;
      case DURABLE_CLIENT_LISTENER:
        listener = new CacheListenerAdapter() {
          public void afterRegionLive(RegionEvent event) {
            logger.fine("Marker received");
            if (Operation.MARKER == event.getOperation()) {
              markerReceived = true;
              if (closeCache) {
                logger.fine("Closing the durable client cache...");
                closeCache(true); // keepAlive
              }
            }
          }

          public void afterCreate(EntryEvent event) {
            logger.fine("CREATE received");
            if (LAST_KEY.equals(event.getKey())) {
              logger.fine("LAST KEY received");
              lastKeyReceived = true;
            }
          }
          
          public void afterUpdate(EntryEvent event) {
            assertNotNull(event.getNewValue());
          }
        };
        break;
      default:
        fail("Invalid listener code");
        break;
    }
    return listener;

  }

  public static void createClientCache(Integer port1, Integer port2,
      String rLevel) throws Exception {
    createClientCache(port1, port2, rLevel,
        DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT, new Integer(
            CLIENT_LISTENER), null, null);
  }

  public static void createClientCache(Integer port1, Integer port2,
      String rLevel, Boolean addListener, EvictionAttributes evictAttrs)
      throws Exception {
    createClientCache(port1, port2, rLevel,
        DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT, new Integer(
            CLIENT_LISTENER), evictAttrs, null);
  }

  public static void createClientCache(Integer port1, Integer port2,
      String rLevel, Boolean addListener, ExpirationAttributes expAttrs)
      throws Exception {
    createClientCache(port1, port2, rLevel,
        DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT, new Integer(
            CLIENT_LISTENER), null, expAttrs);
  }

  public static void createClientCache(Integer port1, Integer port2,
      String rLevel, Integer listener) throws Exception {
    createClientCache(port1, port2, rLevel,
        DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT, listener,
        null, null);
  }

  public static void createClientCache(Integer port1, Integer port2,
      String rLevel, String conflate, Integer listener,
      EvictionAttributes evictAttrs, ExpirationAttributes expAttrs)
      throws Exception {
    int[] ports = null;
    if (port2 != -1) {
      ports = new int[] { port1, port2 };
    }
    else {
      ports = new int[] { port1 };
    }
    assertTrue("No server ports provided", ports != null);
    createClientCache(ports, rLevel, conflate, listener, evictAttrs, expAttrs);
  }

  public static void createClientCache(int[] ports, String rLevel,
      String conflate, Integer listener, EvictionAttributes evictAttrs,
      ExpirationAttributes expAttrs) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    props.setProperty(DistributionConfig.CLIENT_CONFLATION_PROP_NAME, conflate);
    new DeltaPropagationDUnitTest("temp").createCache(props);
    AttributesFactory factory = new AttributesFactory();
    pool = ClientServerTestCase.configureConnectionPool(factory, "localhost", ports,
        true, Integer.parseInt(rLevel), 2, null, 1000, 250, false, -2);

    factory.setScope(Scope.LOCAL);

    if (listener.intValue() != 0) {
      factory.addCacheListener(getCacheListener(listener.intValue()));
    }

    if (evictAttrs != null) {
      factory.setEvictionAttributes(evictAttrs);
    }
    if (expAttrs != null) {
      factory.setEntryTimeToLive(expAttrs);
    }
    if (evictAttrs!=null && evictAttrs.getAction().isOverflowToDisk()) {
      // create diskstore with overflow dir
      // since it's overflow, no need to recover, so we can use random number as dir name
      int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      File dir = new File("overflow_"+port);
      if (!dir.exists()) {
        dir.mkdir();
      }
      File[] dir1 = new File[] { dir };
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      factory.setDiskStoreName(dsf.setDiskDirs(dir1).create("client_overflow_ds").getName());
    }
    factory.setConcurrencyChecksEnabled(false);
    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(regionName, attrs);
    logger = cache.getLogger();
  }

  private void createCache(Properties props) throws Exception {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertNotNull(ds);
    cache = CacheFactory.create(ds);
    assertNotNull(cache);
  }

  public static void verifyRegionSize(Integer regionSize,
      Integer msgsRegionsize, Integer port) {
    try {
      // Get the clientMessagesRegion and check the size.
      Region region = (Region)cache.getRegion("/" + regionName);
      Region msgsRegion = (Region)cache.getRegion(CacheServerImpl
          .generateNameForClientMsgsRegion(port.intValue()));
      logger.fine("size<serverRegion, clientMsgsRegion>: " + region.size()
          + ", " + msgsRegion.size());
      assertEquals(regionSize.intValue(), region.size());
      assertEquals(msgsRegionsize.intValue(), msgsRegion.size());
    }
    catch (Exception e) {
      fail("failed in verifyRegionSize()" + e);
    }
  }

  public static void createDurableCacheClient(Pool poolAttr, String regionName,
      Properties dsProperties, Integer listenerCode, Boolean close) throws Exception {
    new DeltaPropagationDUnitTest("temp").createCache(dsProperties);
    PoolFactoryImpl pf = (PoolFactoryImpl)PoolManager.createFactory();
    pf.init(poolAttr);
    PoolImpl p = (PoolImpl)pf.create("DeltaPropagationDUnitTest");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(false);
    factory.setPoolName(p.getName());
    if (listenerCode.intValue() != 0) {
      factory.addCacheListener(getCacheListener(listenerCode));
    }
    RegionAttributes attrs = factory.create();
    Region r = cache.createRegion(regionName, attrs);
    r.registerInterest("ALL_KEYS");
    pool = p;
    cache.readyForEvents();
    logger = cache.getLogger();
    closeCache = close.booleanValue();
  }
  
  /*
   * public static void createDeltaEntries(Long num) { }
   */

  public static void registerInterestListAll() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.registerInterest("ALL_KEYS");
    }
    catch (Exception ex) {
      com.gemstone.gemfire.test.dunit.Assert.fail("failed in registerInterestListAll", ex);
    }
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void closeCache(boolean keepalive) {
    if (cache != null && !cache.isClosed()) {
      cache.close(keepalive);
      cache.getDistributedSystem().disconnect();
    }
  }
  
  public static boolean isLastKeyReceived() {
    return lastKeyReceived;
  }

  public static void setLastKeyReceived(boolean val) {
    lastKeyReceived = val;
  }

  public static void resetAll() {
    DeltaTestImpl.resetDeltaInvokationCounters();
    numOfCreates = numOfUpdates = numOfInvalidates = numOfDestroys = numOfEvents = 0;
    lastKeyReceived = false;
    markerReceived = false;
    areListenerResultsValid = true;
    listenerError = new StringBuffer("");
  }
}
