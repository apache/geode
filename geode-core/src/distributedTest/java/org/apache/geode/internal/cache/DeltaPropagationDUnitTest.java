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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.CONFLATE_EVENTS;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.cache.CacheServerImpl.generateNameForClientMsgsRegion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.DeltaTestImpl;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.compression.Compressor;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.internal.cache.tier.sockets.ConflationDUnitTestHelper;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

/**
 * @since GemFire 6.1
 */

public class DeltaPropagationDUnitTest extends JUnit4DistributedTestCase {

  private final Compressor compressor = SnappyCompressor.getDefaultInstance();

  protected static Cache cache = null;

  protected VM vm0 = null;

  protected VM vm1 = null;

  protected VM vm2 = null;

  protected VM vm3 = null;

  private int PORT1;

  private int PORT2;

  private static final String regionName = DeltaPropagationDUnitTest.class.getSimpleName();

  private static LogWriter logger = null;

  private static final int EVENTS_SIZE = 6;

  private static boolean lastKeyReceived = false;

  private boolean markerReceived = false;

  private int numOfCreates;

  private int numOfUpdates;

  private static int numOfInvalidates;

  private static int numOfDestroys;

  private static DeltaTestImpl[] deltaPut = new DeltaTestImpl[EVENTS_SIZE];

  private boolean areListenerResultsValid = true;

  private boolean closeCache = false;

  private StringBuffer listenerError = new StringBuffer("");

  private static final String DELTA_KEY = "DELTA_KEY";

  private static final String LAST_KEY = "LAST_KEY";

  private static final int NO_LISTENER = 0;

  private static final int CLIENT_LISTENER = 1;

  private static final int SERVER_LISTENER = 2;

  private static final int C2S2S_SERVER_LISTENER = 3;

  private static final int LAST_KEY_LISTENER = 4;

  private static final int DURABLE_CLIENT_LISTENER = 5;

  private static final int CLIENT_LISTENER_2 = 6;

  private static final String CREATE = "CREATE";

  private static final String UPDATE = "UPDATE";

  private static final String INVALIDATE = "INVALIDATE";

  private static final String DESTROY = "DESTROY";

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);

    vm0.invoke(this::resetAll);
    vm1.invoke(this::resetAll);
    vm2.invoke(this::resetAll);
    vm3.invoke(this::resetAll);
    resetAll();
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();
    vm2.invoke((SerializableCallableIF) this::closeCache);
    vm3.invoke((SerializableCallableIF) this::closeCache);

    // Unset the isSlowStartForTesting flag
    vm0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    vm1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    // then close the servers
    vm0.invoke((SerializableRunnableIF) this::closeCache);
    vm1.invoke((SerializableRunnableIF) this::closeCache);
    disconnectAllFromDS();
  }

  @Test
  public void testS2CSuccessfulDeltaPropagationWithCompression() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE, 1,
        NO_LISTENER, false, compressor));

    vm0.invoke(
        () -> assertTrue(cache.getRegion(regionName).getAttributes().getCompressor() != null));

    createClientCache(PORT1, -1, "0", CLIENT_LISTENER);

    registerInterestListAll();

    vm0.invoke(this::prepareDeltas);
    prepareDeltas();

    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvokations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations();
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas,
        toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltas,
        fromDeltas == toDeltas);

    verifyData(2, EVENTS_SIZE - 1);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }

  @Test
  public void testS2CSuccessfulDeltaPropagation() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, -1, "0", CLIENT_LISTENER);
    registerInterestListAll();

    vm0.invoke(this::prepareDeltas);
    prepareDeltas();

    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvokations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations();
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas,
        toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltas,
        fromDeltas == toDeltas);

    verifyData(2, EVENTS_SIZE - 1);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }

  @Test
  public void testS2CFailureInToDeltaMethod() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, -1, "0", CLIENT_LISTENER_2);
    registerInterestListAll();

    vm0.invoke(this::prepareErroneousDeltasForToDelta);
    prepareErroneousDeltasForToDelta();

    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvokations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations();
    long toDeltafailures = vm0.invoke(DeltaTestImpl::getToDeltaFailures);
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas,
        toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1 - 1/*
                                    * This is because the one failed in toDelta will be sent as full
                                    * value. So client will not see it as 'delta'.
                                    */) + " deltas were to be received but were " + fromDeltas,
        fromDeltas == (EVENTS_SIZE - 1 - 1));
    assertTrue(1 + " deltas were to be failed while extracting but were " + toDeltafailures,
        toDeltafailures == 1);

    verifyData(2, EVENTS_SIZE - 1 - 1 /* Full value no more sent if toDelta() fails */);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }

  @Test
  public void testS2CFailureInFromDeltaMethod() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, -1, "0", CLIENT_LISTENER);
    registerInterestListAll();

    vm0.invoke(this::prepareErroneousDeltasForFromDelta);
    prepareErroneousDeltasForFromDelta();

    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvokations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations();
    long fromDeltafailures = DeltaTestImpl.getFromDeltaFailures();
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas,
        toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltas,
        fromDeltas == toDeltas);
    assertTrue(1 + " deltas were to be failed while applying but were " + fromDeltafailures,
        fromDeltafailures == 1);

    verifyData(2, EVENTS_SIZE - 1);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }

  @Test
  public void testS2CWithOldValueAtClientOverflownToDisk() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    EvictionAttributes evAttr =
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK);

    createClientCache(PORT1, -1, "0", true/* add listener */, evAttr);
    registerInterestListAll();

    vm0.invoke(this::prepareDeltas);
    prepareDeltas();

    vm0.invoke(this::createDelta);
    vm0.invoke(this::createAnEntry);
    Thread.sleep(5000); // TODO: Find a better 'n reliable alternative
    // assert overflow occurred on client vm
    verifyOverflowOccurred(1L, 2);
    vm0.invoke(this::updateDelta);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvokations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations();

    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas,
        toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltas,
        fromDeltas == (EVENTS_SIZE - 1));

    verifyData(3, EVENTS_SIZE - 1);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }

  @Test
  public void testS2CWithLocallyDestroyedOldValueAtClient() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    EvictionAttributes evAttr =
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.LOCAL_DESTROY);

    createClientCache(PORT1, -1, "0", true/* add listener */, evAttr);
    registerInterestListAll();

    vm0.invoke(this::prepareDeltas);
    prepareDeltas();

    vm0.invoke(this::createDelta);
    vm0.invoke(this::createAnEntry);
    Thread.sleep(5000); // TODO: Find a better 'n reliable alternative
    // assert overflow occurred on client vm
    verifyOverflowOccurred(1L, 1);
    vm0.invoke(this::updateDelta);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvokations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations();

    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas,
        toDeltas == (EVENTS_SIZE - 1));
    assertTrue(
        (EVENTS_SIZE - 1 - 1/* destroyed */) + " deltas were to be received but were " + fromDeltas,
        fromDeltas == (EVENTS_SIZE - 1 - 1));

    verifyData(4, EVENTS_SIZE - 2);
  }

  @Test
  public void testS2CWithInvalidatedOldValueAtClient() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, -1, "0", CLIENT_LISTENER);
    registerInterestListAll();

    vm0.invoke(this::prepareDeltas);
    prepareDeltas();

    vm0.invoke(this::createDelta);
    vm0.invoke(this::invalidateDelta);
    vm0.invoke(this::updateDelta);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvokations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvokations();

    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas,
        toDeltas == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1 - 1/* invalidated */) + " deltas were to be received but were "
        + fromDeltas, fromDeltas == (EVENTS_SIZE - 1 - 1));

    verifyData(2, EVENTS_SIZE - 1);
    assertTrue(listenerError.toString(), areListenerResultsValid);
  }

  @Test
  public void testS2CDeltaPropagationWithClientConflationON() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, -1, "0", DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_ON,
        LAST_KEY_LISTENER, null, null);

    registerInterestListAll();
    vm0.invoke(this::prepareDeltas);

    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();

    // TODO: (Amogh) get CCPStats and assert 0 deltas sent.
    assertEquals(0, DeltaTestImpl.getFromDeltaInvokations().longValue());
  }

  @Test
  public void testS2CDeltaPropagationWithServerConflationON() throws Exception {
    vm0.invoke((SerializableRunnableIF) this::closeCache);
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY, 1,
        NO_LISTENER, true /* conflate */, null));

    createClientCache(PORT1, -1, "0", DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT,
        LAST_KEY_LISTENER, null, null);

    vm3.invoke(() -> createClientCache(PORT1, -1, "0",
        DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_OFF, LAST_KEY_LISTENER, null, null));

    registerInterestListAll();
    vm3.invoke(this::registerInterestListAll);

    vm0.invoke(this::prepareDeltas);
    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();
    vm3.invoke(this::waitForLastKey);

    // TODO: (Amogh) use CCPStats.
    assertEquals("Delta Propagation feature used.", 0,
        DeltaTestImpl.getFromDeltaInvokations().longValue());
    long fromDeltaInvocations = vm3.invoke(DeltaTestImpl::getFromDeltaInvokations);
    assertEquals((EVENTS_SIZE - 1), fromDeltaInvocations);
  }

  @Test
  public void testS2CDeltaPropagationWithOnlyCreateEvents() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, -1, "0", LAST_KEY_LISTENER);
    registerInterestListAll();

    vm0.invoke(this::createDeltas);
    waitForLastKey();

    assertEquals(0l, ((Long) vm0.invoke(DeltaTestImpl::getToDeltaInvokations)).longValue());
    assertTrue("Delta Propagation feature used.", DeltaTestImpl.getFromDeltaInvokations() == 0);
  }

  /**
   * Tests that an update on a server with full Delta object causes distribution of the full Delta
   * instance, and not its delta bits, to other peers, even if that instance's
   * <code>hasDelta()</code> returns true.
   */
  @Test
  public void testC2S2SDeltaPropagation() throws Exception {
    prepareDeltas();
    vm0.invoke(this::prepareDeltas);
    vm1.invoke(this::prepareDeltas);

    DeltaTestImpl val = deltaPut[1];
    vm0.invoke((SerializableRunnableIF) this::closeCache);

    PORT1 = vm0.invoke(
        () -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY, 1, C2S2S_SERVER_LISTENER));
    PORT2 = vm1.invoke(
        () -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY, 1, C2S2S_SERVER_LISTENER));

    createClientCache(PORT1, -1, "0", NO_LISTENER);

    Region r = cache.getRegion("/" + regionName);
    assertNotNull(r);

    r.create(DELTA_KEY, deltaPut[0]);

    // Invalidate the value at both the servers.
    vm0.invoke(() -> doLocalOp(INVALIDATE, regionName, DELTA_KEY));
    vm1.invoke(() -> doLocalOp(INVALIDATE, regionName, DELTA_KEY));

    vm0.invoke(() -> assertOp(INVALIDATE, 1));
    vm1.invoke(() -> assertOp(INVALIDATE, 1));

    r.put(DELTA_KEY, val);
    Thread.sleep(5000);

    // Assert that vm0 distributed val as full value to vm1.
    vm1.invoke(() -> assertValue(regionName, DELTA_KEY, val));

    assertTrue("Delta Propagation feature used.", !vm0.invoke(DeltaTestImpl::deltaFeatureUsed));
    assertTrue("Delta Propagation feature used.", !vm1.invoke(DeltaTestImpl::deltaFeatureUsed));
    assertTrue("Delta Propagation feature NOT used.", DeltaTestImpl.deltaFeatureUsed());
  }

  @Test
  public void testS2S2CDeltaPropagationWithHAOverflow() throws Exception {
    prepareDeltas();
    vm0.invoke(this::prepareDeltas);
    vm1.invoke(this::prepareDeltas);

    vm0.invoke((SerializableRunnableIF) this::closeCache);

    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE, 1));
    PORT2 = vm1.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_ENTRY, 1));

    vm0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
    vm1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));

    createClientCache(PORT2, -1, "0", CLIENT_LISTENER);

    Region r = cache.getRegion("/" + regionName);
    assertNotNull(r);
    r.registerInterest("ALL_KEYS");

    vm0.invoke(this::createAndUpdateDeltas);
    vm1.invoke(() -> confirmEviction(PORT2));

    vm1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);

    waitForLastKey();

    long toDeltasOnServer1 = (Long) vm0.invoke(DeltaTestImpl::getToDeltaInvokations);
    long fromDeltasOnServer2 = (Long) vm1.invoke(DeltaTestImpl::getFromDeltaInvokations);
    long toDeltasOnServer2 = (Long) vm1.invoke(DeltaTestImpl::getToDeltaInvokations);
    long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations();

    assertTrue((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltasOnServer1,
        toDeltasOnServer1 == (EVENTS_SIZE - 1));
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltasOnServer2,
        fromDeltasOnServer2 == (EVENTS_SIZE - 1));
    assertTrue("0 toDelta() were to be invoked but were " + toDeltasOnServer2,
        toDeltasOnServer2 == 0);
    assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltasOnClient,
        fromDeltasOnClient == (EVENTS_SIZE - 1));
  }

  @Test
  public void testS2CDeltaPropagationWithGIIAndFailover() throws Exception {
    prepareDeltas();
    vm0.invoke(this::prepareDeltas);
    vm1.invoke(this::prepareDeltas);
    vm2.invoke(this::prepareDeltas);

    vm0.invoke((SerializableRunnableIF) this::closeCache);

    PORT1 =
        vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE, 1, NO_LISTENER));
    PORT2 =
        vm1.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE, 1, NO_LISTENER));
    int port3 =
        vm2.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE, 1, NO_LISTENER));

    // Do puts after slowing the dispatcher.
    try {
      vm0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
      vm1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
      vm2.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));

      createClientCache(new int[] {PORT1, PORT2, port3}, "1",
          DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT, CLIENT_LISTENER, null, null);
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.registerInterest("ALL_KEYS");

      Pool testPool = PoolManager.getAll().values().stream().findFirst().get();

      VM primary = (((PoolImpl) testPool).getPrimaryPort() == PORT1) ? vm0
          : ((((PoolImpl) testPool).getPrimaryPort() == PORT2) ? vm1 : vm2);

      primary.invoke(this::createAndUpdateDeltas);
      Thread.sleep(5000);

      primary.invoke((SerializableRunnableIF) this::closeCache);
      Thread.sleep(5000);

      primary = (((PoolImpl) testPool).getPrimaryPort() == PORT1) ? vm0
          : ((((PoolImpl) testPool).getPrimaryPort() == PORT2) ? vm1 : vm2);

      vm0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
      vm1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
      vm2.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);

      primary.invoke((SerializableRunnableIF) this::closeCache);
      Thread.sleep(5000);

      org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
          .info("waiting for client to receive last_key");
      waitForLastKey();

      long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations();
      assertTrue((EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltasOnClient,
          fromDeltasOnClient == (EVENTS_SIZE - 1));
    } finally {
      vm0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
      vm1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
      vm2.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    }
  }

  @Test
  public void testBug40165ClientReconnects() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    /**
     * 1. Create a cache server with slow dispatcher 2. Start a durable client with a custom cache
     * listener which shuts itself down as soon as it receives a marker message. 3. Do some puts on
     * the server region 4. Let the dispatcher start dispatching 5. Verify that durable client is
     * disconnected as soon as it processes the marker. Server will retain its queue which has some
     * events (containing deltas) in it. 6. Restart the durable client without the self-destructing
     * listener. 7. Wait till the durable client processes all its events. 8. Verify that no deltas
     * are received by it.
     */

    // Step 0
    prepareDeltas();
    vm0.invoke(this::prepareDeltas);

    // Step 1
    try {
      vm0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));

      // Step 2
      String durableClientId = getName() + "_client";
      PoolFactory pf = PoolManager.createFactory();
      pf.addServer("localhost", PORT1).setSubscriptionEnabled(true).setSubscriptionAckInterval(1);
      ((PoolFactoryImpl) pf).getPoolAttributes();

      Properties properties = new Properties();
      properties.setProperty(MCAST_PORT, "0");
      properties.setProperty(LOCATORS, "");
      properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
      properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(60));

      createDurableCacheClient(((PoolFactoryImpl) pf).getPoolAttributes(), regionName, properties,
          DURABLE_CLIENT_LISTENER, true);

      // Step 3
      vm0.invoke((SerializableRunnableIF) this::doPuts);

      // Step 4
      vm0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);

      // Step 5
      // verifyDurableClientDisconnected();
      Thread.sleep(5000);

      // Step 6
      createDurableCacheClient(((PoolFactoryImpl) pf).getPoolAttributes(), regionName, properties,
          DURABLE_CLIENT_LISTENER, false);

      // Step 7
      waitForLastKey();

      // Step 8
      long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations();
      assertTrue("No deltas were to be received but received: " + fromDeltasOnClient,
          fromDeltasOnClient < 1);
    } finally {
      // Step 4
      vm0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    }

  }

  @Test
  public void testBug40165ClientFailsOver() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    /**
     * 1. Create two cache servers with slow dispatcher 2. Start a durable client with a custom
     * cache listener 3. Do some puts on the server region 4. Let the dispatcher start dispatching
     * 5. Wait till the durable client receives marker from its primary. 6. Kill the primary server,
     * so that the second one becomes primary. 7. Wait till the durable client processes all its
     * events. 8. Verify that expected number of deltas are received by it.
     */

    // Step 0
    prepareDeltas();
    vm0.invoke(this::prepareDeltas);
    vm1.invoke(this::prepareDeltas);

    try {
      // Step 1
      vm0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
      PORT2 = vm1.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));
      vm1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));

      // Step 2
      String durableClientId = getName() + "_client";
      PoolFactory pf = PoolManager.createFactory();
      pf.addServer("localhost", PORT1).addServer("localhost", PORT2).setSubscriptionEnabled(true)
          .setSubscriptionAckInterval(1).setSubscriptionRedundancy(2);
      ((PoolFactoryImpl) pf).getPoolAttributes();

      Properties properties = new Properties();
      properties.setProperty(MCAST_PORT, "0");
      properties.setProperty(LOCATORS, "");
      properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
      properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(60));

      createDurableCacheClient(((PoolFactoryImpl) pf).getPoolAttributes(), regionName, properties,
          DURABLE_CLIENT_LISTENER, false);

      // Step 3
      vm0.invoke((SerializableRunnableIF) this::doPuts);
    } finally {
      // Step 4
      vm0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
      vm1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    }

    // Step 5
    Pool testPool = PoolManager.getAll().values().stream().findFirst().get();
    VM pVM = (((PoolImpl) testPool).getPrimaryPort() == PORT1) ? vm0 : vm1;
    while (!markerReceived) {
      Thread.sleep(50);
    }

    // Step 6
    pVM.invoke((SerializableRunnableIF) this::closeCache);
    Thread.sleep(5000);

    // Step 7
    waitForLastKey();

    // Step 8
    long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations();
    assertTrue("Atleast 99 deltas were to be received but received: " + fromDeltasOnClient,
        fromDeltasOnClient >= 99);
  }

  public void doLocalOp(String op, String rName, String key) {
    try {
      Region r = cache.getRegion("/" + rName);
      assertNotNull(r);
      if (INVALIDATE.equals(op)) {
        r.localInvalidate(key);
      } else if (DESTROY.equals(op)) {
        r.localDestroy(key);
      }
    } catch (Exception e) {
      org.apache.geode.test.dunit.Assert.fail("failed in doLocalOp()", e);
    }
  }

  public void assertOp(String op, Integer num) {
    final int expected = num;
    WaitCriterion wc = null;
    if (INVALIDATE.equals(op)) {
      wc = new WaitCriterion() {
        public boolean done() {
          return numOfInvalidates == expected;
        }

        public String description() {
          return "numOfInvalidates was expected to be " + expected + " but is " + numOfInvalidates;
        }
      };
    } else if (DESTROY.equals(op)) {
      wc = new WaitCriterion() {
        public boolean done() {
          return numOfInvalidates == expected;
        }

        public String description() {
          return "numOfDestroys was expected to be " + expected + " but is " + numOfDestroys;
        }
      };
    }
    GeodeAwaitility.await().untilAsserted(wc);
  }

  private void assertValue(String rName, String key, Object expected) {
    try {
      Region r = cache.getRegion("/" + rName);
      assertNotNull(r);
      Object value = r.getEntry(key).getValue();
      assertTrue("Value against " + key + " is " + value + ". It should be " + expected,
          expected.equals(value));
    } catch (Exception e) {
      org.apache.geode.test.dunit.Assert.fail("failed in assertValue()", e);
    }
  }

  private void confirmEviction(Integer port) {
    final EvictionController cc = ((VMLRURegionMap) ((LocalRegion) cache.getRegion(
        SEPARATOR + generateNameForClientMsgsRegion(port))).entries)
            .getEvictionController();

    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return cc.getCounters().getEvictions() > 0;
      }

      public String description() {
        return "HA Overflow did not occure.";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  private void waitForLastKey() {
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return isLastKeyReceived();
      }

      public String description() {
        return "Last key NOT received.";
      }
    };
    GeodeAwaitility.await().untilAsserted(wc);
  }

  private void prepareDeltas() {
    for (int i = 0; i < EVENTS_SIZE; i++) {
      deltaPut[i] =
          new DeltaTestImpl(0, "0", 0d, new byte[0], new TestObjectWithIdentifier("0", 0));
    }
    deltaPut[1].setIntVar(5);
    deltaPut[2].setIntVar(5);
    deltaPut[3].setIntVar(5);
    deltaPut[4].setIntVar(5);
    deltaPut[5].setIntVar(5);

    deltaPut[2].resetDeltaStatus();
    deltaPut[2].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltaPut[3].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltaPut[4].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltaPut[5].setByteArr(new byte[] {1, 2, 3, 4, 5});

    deltaPut[3].resetDeltaStatus();
    deltaPut[3].setDoubleVar(5d);
    deltaPut[4].setDoubleVar(5d);
    deltaPut[5].setDoubleVar(5d);

    deltaPut[4].resetDeltaStatus();
    deltaPut[4].setStr("str changed");
    deltaPut[5].setStr("str changed");

    deltaPut[5].resetDeltaStatus();
    deltaPut[5].setIntVar(100);
    deltaPut[5].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));
  }

  private void prepareErroneousDeltasForToDelta() {
    for (int i = 0; i < EVENTS_SIZE; i++) {
      deltaPut[i] =
          new DeltaTestImpl(0, "0", 0d, new byte[0], new TestObjectWithIdentifier("0", 0));
    }
    deltaPut[1].setIntVar(5);
    deltaPut[2].setIntVar(5);
    deltaPut[3].setIntVar(DeltaTestImpl.ERRONEOUS_INT_FOR_TO_DELTA);
    deltaPut[4].setIntVar(5);
    deltaPut[5].setIntVar(5);

    deltaPut[2].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltaPut[3].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltaPut[4].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltaPut[5].setByteArr(new byte[] {1, 2, 3, 4, 5});

    deltaPut[3].setDoubleVar(5d);
    deltaPut[4].setDoubleVar(5d);
    deltaPut[5].setDoubleVar(5d);

    deltaPut[4].setStr("str changed");
    deltaPut[5].setStr("str changed");

    deltaPut[5].setIntVar(100);
    deltaPut[5].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));
  }

  private void prepareErroneousDeltasForFromDelta() {
    for (int i = 0; i < EVENTS_SIZE; i++) {
      deltaPut[i] =
          new DeltaTestImpl(0, "0", 0d, new byte[0], new TestObjectWithIdentifier("0", 0));
    }
    deltaPut[1].setIntVar(5);
    deltaPut[2].setIntVar(5);
    deltaPut[3].setIntVar(5);
    deltaPut[4].setIntVar(5);
    deltaPut[5].setIntVar(5);

    deltaPut[2].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltaPut[3].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltaPut[4].setByteArr(new byte[] {1, 2, 3, 4, 5});
    deltaPut[5].setByteArr(new byte[] {1, 2, 3, 4, 5});

    deltaPut[3].setDoubleVar(5d);
    deltaPut[4].setDoubleVar(5d);
    deltaPut[5].setDoubleVar(5d);

    deltaPut[4].setStr("str changed");
    deltaPut[5].setStr(DeltaTestImpl.ERRONEOUS_STRING_FOR_FROM_DELTA);

    deltaPut[5].setIntVar(100);
    deltaPut[5].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));
  }

  private void doPuts() {
    doPuts(100);
  }

  private void doPuts(Integer num) {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      for (int i = 0; i < num; i++) {
        DeltaTestImpl val = new DeltaTestImpl();
        val.setStr("" + i);
        r.put(DELTA_KEY, val);
      }
      r.put(LAST_KEY, "");
    } catch (Exception ex) {
      org.apache.geode.test.dunit.Assert.fail("failed in createDelta()", ex);
    }
  }

  private void createAndUpdateDeltas() {
    createDelta();
    updateDelta();
  }

  private void createDelta() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      r.create(DELTA_KEY, deltaPut[0]);
    } catch (Exception ex) {
      org.apache.geode.test.dunit.Assert.fail("failed in createDelta()", ex);
    }
  }

  private void updateDelta() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      for (int i = 1; i < EVENTS_SIZE; i++) {
        try {
          r.put(DELTA_KEY, deltaPut[i]);
        } catch (InvalidDeltaException ide) {
          assertTrue("InvalidDeltaException not expected for deltaPut[" + i + "]",
              deltaPut[i].getIntVar() == DeltaTestImpl.ERRONEOUS_INT_FOR_TO_DELTA);
        }
      }
      r.put(LAST_KEY, "");
    } catch (Exception ex) {
      org.apache.geode.test.dunit.Assert.fail("failed in updateDelta()", ex);
    }
  }

  private void createDeltas() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      for (int i = 0; i < 100; i++) {
        r.create(DELTA_KEY + i, new DeltaTestImpl());
      }
      r.create(LAST_KEY, "");
    } catch (Exception ex) {
      org.apache.geode.test.dunit.Assert.fail("failed in createDeltas()", ex);
    }
  }

  private void createAnEntry() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      r.create("KEY-A", "I push the delta out to disk :)");
    } catch (Exception ex) {
      org.apache.geode.test.dunit.Assert.fail("failed in createAnEntry()", ex);
    }
  }

  private void invalidateDelta() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);

      r.invalidate(DELTA_KEY);
    } catch (Exception ex) {
      fail("failed in invalidateDelta()" + ex.getMessage());
    }
  }

  private void verifyOverflowOccurred(long evictions, int regionsize) {
    EvictionController cc = ((VMLRURegionMap) ((LocalRegion) cache.getRegion(regionName)).entries)
        .getEvictionController();
    Assert.assertTrue(cc.getCounters().getEvictions() == evictions,
        "Number of evictions expected to be " + evictions + " but was "
            + cc.getCounters().getEvictions());
    int rSize = ((LocalRegion) cache.getRegion(regionName)).getRegionMap().size();
    Assert.assertTrue(rSize == regionsize,
        "Region size expected to be " + regionsize + " but was " + rSize);
  }

  private void verifyData(int creates, int updates) {
    assertEquals(creates, numOfCreates);
    assertEquals(updates, numOfUpdates);
  }

  private Integer createServerCache(String ePolicy) throws Exception {
    return createServerCache(ePolicy, 1);
  }

  private Integer createServerCache(String ePolicy, Integer cap) throws Exception {
    return createServerCache(ePolicy, cap, NO_LISTENER);
  }

  private Integer createServerCache(String ePolicy, Integer cap, Integer listenerCode)
      throws Exception {
    return createServerCache(ePolicy, cap, listenerCode, false, null);
  }

  private Integer createServerCache(String ePolicy, Integer cap, Integer listenerCode,
      Boolean conflate, Compressor compressor) throws Exception {
    ConnectionTable.threadWantsSharedResources();
    createCache(new Properties());
    AttributesFactory factory = new AttributesFactory();
    factory.setEnableSubscriptionConflation(conflate);
    if (listenerCode != 0) {
      factory.addCacheListener(getCacheListener(listenerCode));
    }
    if (compressor != null) {
      factory.setCompressor(compressor);
    }
    if (listenerCode == C2S2S_SERVER_LISTENER) {
      factory.setScope(Scope.DISTRIBUTED_NO_ACK);
      factory.setDataPolicy(DataPolicy.NORMAL);
      factory.setConcurrencyChecksEnabled(false);
      RegionAttributes attrs = factory.create();
      Region r = cache.createRegion(regionName, attrs);
      logger = cache.getLogger();
      r.create(DELTA_KEY, deltaPut[0]);
    } else {
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
      File overflowDirectory = new File("bsi_overflow_" + port);
      overflowDirectory.mkdir();
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      File[] dirs1 = new File[] {overflowDirectory};

      server1.getClientSubscriptionConfig().setEvictionPolicy(ePolicy);
      server1.getClientSubscriptionConfig().setCapacity(cap);
      // specify diskstore for this server
      server1.getClientSubscriptionConfig()
          .setDiskStoreName(dsf.setDiskDirs(dirs1).create("bsi").getName());
    }
    server1.start();
    return server1.getPort();
  }

  private CacheListener getCacheListener(Integer code) {
    CacheListener listener = null;
    switch (code) {
      case 0:
        break;
      case SERVER_LISTENER:
        // listener = new CacheListenerAdapter() {};
        break;
      case CLIENT_LISTENER:
        listener = new CacheListenerAdapter() {
          public void afterCreate(EntryEvent event) {
            numOfCreates++;
            logger.fine("Create Event: <" + event.getKey() + ", " + event.getNewValue() + ">");
            if (DELTA_KEY.equals(event.getKey()) && !deltaPut[0].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("Create event:\n |-> sent: " + deltaPut[0] + "\n |-> rcvd: "
                  + event.getNewValue() + "\n");
            } else if (LAST_KEY.equals(event.getKey())) {
              lastKeyReceived = true;
            }
          }

          public void afterUpdate(EntryEvent event) {
            numOfUpdates++;
            logger.fine("Update Event: <" + event.getKey() + ", " + event.getNewValue() + ">"
                + ", numOfUpdates: " + numOfUpdates);
            if (!deltaPut[numOfUpdates].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("\nUpdate event(" + numOfUpdates + "):\n |-> sent: "
                  + deltaPut[numOfUpdates] + "\n |-> recd: " + event.getNewValue());
            }
          }
        };
        break;
      case CLIENT_LISTENER_2:
        listener = new CacheListenerAdapter() {
          public void afterCreate(EntryEvent event) {
            numOfCreates++;
            logger.fine("Create Event: <" + event.getKey() + ", " + event.getNewValue() + ">");
            if (DELTA_KEY.equals(event.getKey()) && !deltaPut[0].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("Create event:\n |-> sent: " + deltaPut[0] + "\n |-> rcvd: "
                  + event.getNewValue() + "\n");
            } else if (LAST_KEY.equals(event.getKey())) {
              lastKeyReceived = true;
            }
          }

          public void afterUpdate(EntryEvent event) {
            int tmp = ++numOfUpdates;
            logger.fine("Update Event: <" + event.getKey() + ", " + event.getNewValue() + ">"
                + ", numOfUpdates: " + numOfUpdates);
            // Hack to ignore illegal delta put
            tmp = (tmp >= 3) ? ++tmp : tmp;
            if (!deltaPut[tmp].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("\nUpdate event(" + numOfUpdates + "):\n |-> sent: "
                  + deltaPut[tmp] + "\n |-> recd: " + event.getNewValue());
            }
          }
        };
        break;
      case C2S2S_SERVER_LISTENER:
        listener = new CacheListenerAdapter() {
          public void afterCreate(EntryEvent event) {
            numOfCreates++;
            logger.fine("Create Event: <" + event.getKey() + ", " + event.getNewValue() + ">");
            if (LAST_KEY.equals(event.getKey())) {
              lastKeyReceived = true;
            }
          }

          public void afterUpdate(EntryEvent event) {
            numOfUpdates++;
            logger.fine("Update Event: <" + event.getKey() + ", " + event.getNewValue() + ">"
                + ", numOfUpdates: " + numOfUpdates);
          }

          public void afterInvalidate(EntryEvent event) {
            numOfInvalidates++;
            logger.fine("Invalidate Event: <" + event.getKey() + ", " + event.getOldValue() + ">"
                + ", numOfInvalidates: " + numOfInvalidates);
          }

          public void afterDestroy(EntryEvent event) {
            numOfDestroys++;
            logger.fine("Destroy Event: <" + event.getKey() + ", " + event.getOldValue() + ">"
                + ", numOfDestroys: " + numOfDestroys);
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

  private void createClientCache(Integer port1, Integer port2, String rLevel) throws Exception {
    createClientCache(port1, port2, rLevel, DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT,
        CLIENT_LISTENER, null, null);
  }

  private void createClientCache(Integer port1, Integer port2, String rLevel, Boolean addListener,
      EvictionAttributes evictAttrs) throws Exception {
    createClientCache(port1, port2, rLevel, DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT,
        CLIENT_LISTENER, evictAttrs, null);
  }

  private void createClientCache(Integer port1, Integer port2, String rLevel, Boolean addListener,
      ExpirationAttributes expAttrs) throws Exception {
    createClientCache(port1, port2, rLevel, DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT,
        CLIENT_LISTENER, null, expAttrs);
  }

  private void createClientCache(Integer port1, Integer port2, String rLevel, Integer listener)
      throws Exception {
    createClientCache(port1, port2, rLevel, DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT,
        listener, null, null);
  }

  private void createClientCache(Integer port1, Integer port2, String rLevel, String conflate,
      Integer listener, EvictionAttributes evictAttrs, ExpirationAttributes expAttrs)
      throws Exception {
    int[] ports = null;
    if (port2 != -1) {
      ports = new int[] {port1, port2};
    } else {
      ports = new int[] {port1};
    }
    assertTrue("No server ports provided", ports != null);
    createClientCache(ports, rLevel, conflate, listener, evictAttrs, expAttrs);
  }

  private void createClientCache(int[] ports, String rLevel, String conflate, Integer listener,
      EvictionAttributes evictAttrs, ExpirationAttributes expAttrs) throws Exception {
    CacheServerTestUtil.disableShufflingOfEndpoints();

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(CONFLATE_EVENTS, conflate);
    createCache(props);
    AttributesFactory factory = new AttributesFactory();
    ClientServerTestCase.configureConnectionPool(factory, "localhost", ports, true,
        Integer.parseInt(rLevel), 2, null, 1000, 250, false, -2);

    factory.setScope(Scope.LOCAL);

    if (listener != 0) {
      factory.addCacheListener(getCacheListener(listener));
    }

    if (evictAttrs != null) {
      factory.setEvictionAttributes(evictAttrs);
    }
    if (expAttrs != null) {
      factory.setEntryTimeToLive(expAttrs);
    }
    if (evictAttrs != null && evictAttrs.getAction().isOverflowToDisk()) {
      // create diskstore with overflow dir
      // since it's overflow, no need to recover, so we can use random number as dir name
      int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      File dir = new File("overflow_" + port);
      if (!dir.exists()) {
        dir.mkdir();
      }
      File[] dir1 = new File[] {dir};
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      factory.setDiskStoreName(dsf.setDiskDirs(dir1).create("client_overflow_ds").getName());
    }
    factory.setConcurrencyChecksEnabled(false);
    RegionAttributes attrs = factory.create();
    cache.createRegion(regionName, attrs);
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

  private void createDurableCacheClient(Pool poolAttr, String regionName, Properties dsProperties,
      Integer listenerCode, Boolean close) throws Exception {
    new DeltaPropagationDUnitTest().createCache(dsProperties);
    PoolFactoryImpl poolFactory = (PoolFactoryImpl) PoolManager.createFactory();
    poolFactory.init(poolAttr);
    PoolImpl pool = (PoolImpl) poolFactory.create("DeltaPropagationDUnitTest");
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(false);
    factory.setPoolName(pool.getName());
    if (listenerCode != 0) {
      factory.addCacheListener(getCacheListener(listenerCode));
    }
    RegionAttributes attrs = factory.create();
    Region r = cache.createRegion(regionName, attrs);
    r.registerInterest("ALL_KEYS");
    cache.readyForEvents();
    logger = cache.getLogger();
    closeCache = close;
  }

  private void registerInterestListAll() {
    try {
      Region r = cache.getRegion("/" + regionName);
      assertNotNull(r);
      r.registerInterest("ALL_KEYS");
    } catch (Exception ex) {
      org.apache.geode.test.dunit.Assert.fail("failed in registerInterestListAll", ex);
    }
  }

  private Object closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
      return null;
    }
    return null;
  }

  private void closeCache(boolean keepalive) {
    if (cache != null && !cache.isClosed()) {
      cache.close(keepalive);
      cache.getDistributedSystem().disconnect();
    }
  }

  private boolean isLastKeyReceived() {
    return lastKeyReceived;
  }

  private void resetAll() {
    DeltaTestImpl.resetDeltaInvokationCounters();
    numOfCreates = numOfUpdates = numOfInvalidates = numOfDestroys = 0;
    lastKeyReceived = false;
    markerReceived = false;
    areListenerResultsValid = true;
    listenerError = new StringBuffer("");
  }
}
