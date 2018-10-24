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

import static org.apache.geode.distributed.ConfigurationProperties.CONFLATE_EVENTS;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.geode.DeltaTestImpl;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
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
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil;
import org.apache.geode.internal.cache.tier.sockets.ConflationDUnitTestHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

/**
 * @since GemFire 6.1
 */

public class DeltaPropagationDUnitTest extends JUnit4DistributedTestCase {

  private final Compressor compressor = SnappyCompressor.getDefaultInstance();

  private static Cache cache = null;

  private VM vm0 = null;

  private VM vm1 = null;

  private VM vm2 = null;

  private VM vm3 = null;

  private int PORT1;

  private int PORT2;

  private static final String regionName = DeltaPropagationDUnitTest.class.getSimpleName();

  private static final int EVENTS_SIZE = 6;

  private static volatile boolean lastKeyReceived = false;

  private final AtomicBoolean markerReceived = new AtomicBoolean(false);

  private final AtomicBoolean closeCacheFinished = new AtomicBoolean(false);

  private int numOfCreates;

  private int numOfUpdates;

  private static int numOfInvalidates;

  private static int numOfDestroys;

  private static final DeltaTestImpl[] deltaPut = new DeltaTestImpl[EVENTS_SIZE];

  private boolean areListenerResultsValid = true;

  private boolean shouldCloseCache = false;

  private StringBuffer listenerError = new StringBuffer();

  private static final String DELTA_KEY = "DELTA_KEY";

  private static final String LAST_KEY = "LAST_KEY";

  private static final int NO_LISTENER = 0;

  private static final int CLIENT_LISTENER = 1;

  private static final int SERVER_LISTENER = 2;

  private static final int C2S2S_SERVER_LISTENER = 3;

  private static final int LAST_KEY_LISTENER = 4;

  private static final int DURABLE_CLIENT_LISTENER = 5;

  private static final int CLIENT_LISTENER_2 = 6;

  private static final Logger logger = LogService.getLogger();

  @Override
  public final void postSetUp() {

    vm0 = VM.getVM(0);
    vm1 = VM.getVM(1);
    vm2 = VM.getVM(2);
    vm3 = VM.getVM(3);

    vm0.invoke(this::resetAll);
    vm1.invoke(this::resetAll);
    vm2.invoke(this::resetAll);
    vm3.invoke(this::resetAll);
    resetAll();
  }

  @Override
  public final void preTearDown() {
    closeCache();
    vm2.invoke(() -> shouldCloseCache);
    vm3.invoke(() -> shouldCloseCache);

    // Unset the isSlowStartForTesting flag
    vm0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    vm1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    // then close the servers
    vm0.invoke(() -> shouldCloseCache);
    vm1.invoke(() -> shouldCloseCache);
    disconnectAllFromDS();
  }

  @Test
  public void testS2CSuccessfulDeltaPropagationWithCompression() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE, 1,
        NO_LISTENER, false, compressor));

    vm0.invoke(
        () -> assertTrue(cache.getRegion(regionName).getAttributes().getCompressor() != null));

    createClientCache(PORT1, CLIENT_LISTENER);

    registerInterestListAll();

    vm0.invoke(this::prepareDeltas);
    prepareDeltas();

    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvocations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvocations();
    assertThat(toDeltas)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas)
        .isEqualTo(EVENTS_SIZE - 1);
    assertThat(toDeltas)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltas)
        .isEqualTo(fromDeltas);

    verifyData(2, EVENTS_SIZE - 1);
    assertThat(areListenerResultsValid).describedAs(listenerError.toString()).isTrue();
  }

  @Test
  public void testS2CSuccessfulDeltaPropagation() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, CLIENT_LISTENER);
    registerInterestListAll();

    vm0.invoke(this::prepareDeltas);
    prepareDeltas();

    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvocations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvocations();
    assertThat(toDeltas)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas)
        .isEqualTo(EVENTS_SIZE - 1);
    assertThat(toDeltas)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltas)
        .isEqualTo(fromDeltas);

    verifyData(2, EVENTS_SIZE - 1);
    assertThat(areListenerResultsValid).describedAs(listenerError.toString()).isTrue();
  }


  @Test
  public void testS2CFailureInToDeltaMethod() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, CLIENT_LISTENER_2);
    registerInterestListAll();

    vm0.invoke(this::prepareErroneousDeltasForToDelta);
    prepareErroneousDeltasForToDelta();

    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvocations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvocations();
    long toDeltafailures = vm0.invoke(DeltaTestImpl::getToDeltaFailures);
    assertThat(toDeltas)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas)
        .isEqualTo((EVENTS_SIZE - 1));
    /*
     * EVENTS_SIZE - 1 - 1: This is because the one failed in toDelta will be sent as full
     * value. So client will not see it as 'delta'.
     */
    assertThat(fromDeltas)
        .describedAs((EVENTS_SIZE - 1 - 1) + " deltas were to be received but were " + fromDeltas)
        .isEqualTo(EVENTS_SIZE - 1 - 1);

    assertThat(1)
        .describedAs(1 + " deltas were to be failed while extracting but were " + toDeltafailures)
        .isEqualTo(toDeltafailures);

    verifyData(2, EVENTS_SIZE - 1 - 1 /* Full value no more sent if toDelta() fails */);
    assertThat(areListenerResultsValid).describedAs(listenerError.toString()).isTrue();
  }

  @Test
  public void testS2CFailureInFromDeltaMethod() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, CLIENT_LISTENER);
    registerInterestListAll();

    vm0.invoke(this::prepareErroneousDeltasForFromDelta);
    prepareErroneousDeltasForFromDelta();

    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvocations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvocations();
    long fromDeltafailures = DeltaTestImpl.getFromDeltaFailures();
    assertThat(toDeltas)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas)
        .isEqualTo((EVENTS_SIZE - 1));
    assertThat(fromDeltas)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltas)
        .isEqualTo(toDeltas);
    assertThat(1)
        .describedAs(1 + " deltas were to be failed while applying but were " + fromDeltafailures)
        .isEqualTo(fromDeltafailures);

    verifyData(2, EVENTS_SIZE - 1);
    assertThat(areListenerResultsValid).describedAs(listenerError.toString()).isTrue();
  }

  @Test
  public void testS2CWithOldValueAtClientOverflownToDisk() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    EvictionAttributes evAttr =
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK);

    createClientCache(PORT1, /* add listener */ evAttr);
    registerInterestListAll();

    vm0.invoke(this::prepareDeltas);
    prepareDeltas();

    vm0.invoke(this::createDelta);
    vm0.invoke(this::createAnEntry);
    Thread.sleep(5000); // TODO: Find a better 'n reliable alternative
    // assert overflow occurred on client vm
    verifyOverflowOccurred(2);
    vm0.invoke(this::updateDelta);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvocations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvocations();

    assertThat(toDeltas)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas)
        .isEqualTo((EVENTS_SIZE - 1));
    assertThat(fromDeltas)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltas)
        .isEqualTo((EVENTS_SIZE - 1));

    verifyData(3, EVENTS_SIZE - 1);
    assertThat(areListenerResultsValid).describedAs(listenerError.toString()).isTrue();
  }

  @Test
  public void testS2CWithLocallyDestroyedOldValueAtClient() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    EvictionAttributes evAttr =
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.LOCAL_DESTROY);

    createClientCache(PORT1, /* add listener */ evAttr);
    registerInterestListAll();

    vm0.invoke(this::prepareDeltas);
    prepareDeltas();

    vm0.invoke(this::createDelta);
    vm0.invoke(this::createAnEntry);
    Thread.sleep(5000); // TODO: Find a better 'n reliable alternative
    // assert overflow occurred on client vm
    verifyOverflowOccurred(1);
    vm0.invoke(this::updateDelta);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvocations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvocations();

    assertThat(toDeltas)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas)
        .isEqualTo((EVENTS_SIZE - 1));
    assertThat(fromDeltas).describedAs(
        (EVENTS_SIZE - 1 - 1/* destroyed */) + " deltas were to be received but were " + fromDeltas)
        .isEqualTo((EVENTS_SIZE - 1 - 1));

    verifyData(4, EVENTS_SIZE - 2);
  }

  @Test
  public void testS2CWithInvalidatedOldValueAtClient() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, CLIENT_LISTENER);
    registerInterestListAll();

    vm0.invoke(this::prepareDeltas);
    prepareDeltas();

    vm0.invoke(this::createDelta);
    vm0.invoke(this::invalidateDelta);
    vm0.invoke(this::updateDelta);

    waitForLastKey();

    long toDeltas = vm0.invoke(DeltaTestImpl::getToDeltaInvocations);
    long fromDeltas = DeltaTestImpl.getFromDeltaInvocations();

    assertThat(toDeltas)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltas)
        .isEqualTo((EVENTS_SIZE - 1));
    assertThat(fromDeltas).describedAs(
        (EVENTS_SIZE - 1 - 1/* invalidated */) + " deltas were to be received but were "
            + fromDeltas)
        .isEqualTo(EVENTS_SIZE - 1 - 1);

    verifyData(2, EVENTS_SIZE - 1);
    assertThat(areListenerResultsValid).describedAs(listenerError.toString()).isTrue();
  }

  @Test
  public void testS2CDeltaPropagationWithClientConflationON() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_ON,
        LAST_KEY_LISTENER, null);

    registerInterestListAll();
    vm0.invoke(this::prepareDeltas);

    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();

    // TODO: (Amogh) get CCPStats and assert 0 deltas sent.
    assertThat(DeltaTestImpl.getFromDeltaInvocations().longValue()).isEqualTo(0);
  }

  @Test
  public void testS2CDeltaPropagationWithServerConflationON() throws Exception {
    vm0.invoke((SerializableRunnableIF) this::closeCache);
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY, 1,
        NO_LISTENER, true /* conflate */, null));

    createClientCache(PORT1, DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT,
        LAST_KEY_LISTENER, null);

    vm3.invoke(() -> createClientCache(PORT1,
        DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_OFF, LAST_KEY_LISTENER, null));

    registerInterestListAll();
    vm3.invoke(this::registerInterestListAll);

    vm0.invoke(this::prepareDeltas);
    vm0.invoke(this::createAndUpdateDeltas);

    waitForLastKey();
    vm3.invoke(this::waitForLastKey);

    // TODO: (Amogh) use CCPStats.
    assertThat(0).describedAs("Delta Propagation feature used.")
        .isEqualTo(DeltaTestImpl.getFromDeltaInvocations().longValue());
    long fromDeltaInvocations = vm3.invoke(DeltaTestImpl::getFromDeltaInvocations);
    assertThat(fromDeltaInvocations).isEqualTo(EVENTS_SIZE - 1);
  }

  @Test
  public void testS2CDeltaPropagationWithOnlyCreateEvents() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    createClientCache(PORT1, LAST_KEY_LISTENER);
    registerInterestListAll();

    vm0.invoke(this::createDeltas);
    waitForLastKey();

    assertThat(vm0.invoke(DeltaTestImpl::getToDeltaInvocations).longValue()).isEqualTo(0L);
    assertThat(0).describedAs("Delta Propagation feature used.")
        .isEqualTo((long) DeltaTestImpl.getFromDeltaInvocations());
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

    createClientCache(PORT1, NO_LISTENER);

    Region<String, DeltaTestImpl> r = cache.getRegion("/" + regionName);
    assertThat(r).isNotNull();

    r.create(DELTA_KEY, deltaPut[0]);

    // Invalidate the value at both the servers.
    vm0.invoke(this::doLocalOp);
    vm1.invoke(this::doLocalOp);

    vm0.invoke(this::assertOp);
    vm1.invoke(this::assertOp);

    r.put(DELTA_KEY, val);
    Thread.sleep(5000);

    // Assert that vm0 distributed val as full value to vm1.
    vm1.invoke(() -> assertValue(val));

    assertThat(!vm0.invoke(DeltaTestImpl::deltaFeatureUsed))
        .describedAs("Delta Propagation feature used.").isTrue();
    assertThat(!vm1.invoke(DeltaTestImpl::deltaFeatureUsed))
        .describedAs("Delta Propagation feature used.").isTrue();
    assertThat(DeltaTestImpl.deltaFeatureUsed()).describedAs("Delta Propagation feature NOT used.")
        .isTrue();
  }

  @Test
  public void testS2S2CDeltaPropagationWithHAOverflow() throws Exception {
    prepareDeltas();
    vm0.invoke(this::prepareDeltas);
    vm1.invoke(this::prepareDeltas);

    vm0.invoke((SerializableRunnableIF) this::closeCache);

    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE));
    PORT2 = vm1.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_ENTRY));

    vm0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
    vm1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));

    createClientCache(PORT2, CLIENT_LISTENER);

    Region<String, DeltaTestImpl> r = cache.getRegion("/" + regionName);
    assertThat(r).isNotNull();
    r.registerInterestForAllKeys();

    vm0.invoke(this::createAndUpdateDeltas);
    vm1.invoke(() -> confirmEviction(PORT2));

    vm1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);

    waitForLastKey();

    long toDeltasOnServer1 = vm0.invoke(DeltaTestImpl::getToDeltaInvocations);
    long fromDeltasOnServer2 = vm1.invoke(DeltaTestImpl::getFromDeltaInvocations);
    long toDeltasOnServer2 = vm1.invoke(DeltaTestImpl::getToDeltaInvocations);
    long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvocations();

    assertThat(toDeltasOnServer1)
        .describedAs((EVENTS_SIZE - 1) + " deltas were to be sent but were " + toDeltasOnServer1)
        .isEqualTo((EVENTS_SIZE - 1));
    assertThat(fromDeltasOnServer2).describedAs(
        (EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltasOnServer2)
        .isEqualTo((EVENTS_SIZE - 1));
    assertThat(0).describedAs("0 toDelta() were to be invoked but were " + toDeltasOnServer2)
        .isEqualTo(toDeltasOnServer2);
    assertThat(fromDeltasOnClient).describedAs(
        (EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltasOnClient)
        .isEqualTo((EVENTS_SIZE - 1));
  }

  @Test
  public void testS2CDeltaPropagationWithGIIAndFailover() throws Exception {
    prepareDeltas();
    vm0.invoke(this::prepareDeltas);
    vm1.invoke(this::prepareDeltas);
    vm2.invoke(this::prepareDeltas);

    vm0.invoke((SerializableRunnableIF) this::closeCache);

    PORT1 =
        vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE, NO_LISTENER));
    PORT2 =
        vm1.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE, NO_LISTENER));
    int port3 =
        vm2.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_NONE, NO_LISTENER));

    // Do puts after slowing the dispatcher.
    try {
      vm0.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
      vm1.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));
      vm2.invoke(() -> ConflationDUnitTestHelper.setIsSlowStart("60000"));

      createClientCache(new int[] {PORT1, PORT2, port3}, "1",
          DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT, CLIENT_LISTENER, null);
      Region<String, DeltaTestImpl> r = cache.getRegion("/" + regionName);
      assertThat(r).isNotNull();
      r.registerInterestForAllKeys();

      assertThat(PoolManager.getAll().values().stream().findFirst().isPresent()).isTrue();

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

      logger.info("waiting for client to receive last_key");
      waitForLastKey();

      long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvocations();
      assertThat(fromDeltasOnClient).describedAs(
          (EVENTS_SIZE - 1) + " deltas were to be received but were " + fromDeltasOnClient)
          .isEqualTo((EVENTS_SIZE - 1));
    } finally {
      vm0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
      vm1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
      vm2.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    }
  }

  @Test
  public void testBug40165ClientReconnects() throws Exception {

    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    /*
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

      Properties properties = new Properties();
      properties.setProperty(MCAST_PORT, "0");
      properties.setProperty(LOCATORS, "");
      properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
      properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(60));

      createDurableCacheClient(((PoolFactoryImpl) pf).getPoolAttributes(), properties,
          true);

      // Step 3
      vm0.invoke(this::doPuts);

      // Step 4
      vm0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);


      // Step 5
      GeodeAwaitility.await().until(closeCacheFinished::get);


      // Step 6
      createDurableCacheClient(((PoolFactoryImpl) pf).getPoolAttributes(), properties,
          false);

      GeodeAwaitility.await().until(markerReceived::get);

      // Step 7
      waitForLastKey();

      // Step 8
      long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvocations();
      assertThat(fromDeltasOnClient < 1)
          .describedAs("No deltas were to be received but received: " + fromDeltasOnClient)
          .isTrue();
    } finally {
      // Step 4

      vm0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    }

  }

  @Test
  public void testBug40165ClientFailsOver() throws Exception {
    PORT1 = vm0.invoke(() -> createServerCache(HARegionQueue.HA_EVICTION_POLICY_MEMORY));

    /*
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

      Properties properties = new Properties();
      properties.setProperty(MCAST_PORT, "0");
      properties.setProperty(LOCATORS, "");
      properties.setProperty(DURABLE_CLIENT_ID, durableClientId);
      properties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(60));

      createDurableCacheClient(((PoolFactoryImpl) pf).getPoolAttributes(), properties,
          false);

      // Step 3
      vm0.invoke(this::doPuts);
    } finally {
      // Step 4
      vm0.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
      vm1.invoke(ConflationDUnitTestHelper::unsetIsSlowStart);
    }

    // Step 5
    assertThat(PoolManager.getAll().values().stream().findFirst().isPresent()).isTrue();
    Pool testPool = PoolManager.getAll().values().stream().findFirst().get();
    VM pVM = (((PoolImpl) testPool).getPrimaryPort() == PORT1) ? vm0 : vm1;

    GeodeAwaitility.await().until(markerReceived::get);

    // Step 6
    pVM.invoke((SerializableRunnableIF) this::closeCache);
    Thread.sleep(5000);


    // Step 7
    waitForLastKey();

    // Step 8
    long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvocations();
    assertThat(fromDeltasOnClient >= 99)
        .describedAs("Atleast 99 deltas were to be received but received: " + fromDeltasOnClient)
        .isTrue();
  }

  private void doLocalOp() {
    try {
      Region<String, DeltaTestImpl> r = cache.getRegion("/" + DeltaPropagationDUnitTest.regionName);
      assertThat(r).isNotNull();
      r.localInvalidate(DeltaPropagationDUnitTest.DELTA_KEY);
    } catch (Exception e) {
      Assertions.fail("failed in doLocalOp()", e);
    }
  }

  private void assertOp() {
    final int expected = 1;

    GeodeAwaitility
        .await("numOfInvalidates was expected to be " + expected + " but is " + numOfInvalidates)
        .until(() -> numOfInvalidates == expected);

  }

  private void assertValue(Object expected) {
    try {
      Region<String, DeltaTestImpl> r = cache.getRegion("/" + DeltaPropagationDUnitTest.regionName);
      assertThat(r).isNotNull();
      Object value = r.getEntry(DeltaPropagationDUnitTest.DELTA_KEY).getValue();
      assertThat(value)
          .describedAs("Value against " + DeltaPropagationDUnitTest.DELTA_KEY + " is " + value
              + ". It should be " + expected)
          .isEqualTo(expected);
    } catch (Exception e) {
      Assertions.fail("failed in assertValue()", e);
    }
  }

  private void confirmEviction(Integer port) {
    final EvictionController cc = ((LocalRegion) cache.getRegion(
        Region.SEPARATOR + CacheServerImpl.generateNameForClientMsgsRegion(port))).entries
            .getEvictionController();

    GeodeAwaitility.await("HA Overflow did not occur.")
        .until(() -> cc.getCounters().getEvictions() > 0);
  }

  private void waitForLastKey() {
    GeodeAwaitility.await("Last key NOT received.").until(this::isLastKeyReceived);
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
    try {
      Region<String, DeltaTestImpl> r = cache.getRegion("/" + regionName);
      assertThat(r).isNotNull();
      DeltaTestImpl val;
      for (int i = 0; i < 100; i++) {
        val = new DeltaTestImpl();
        val.setStr("" + i);
        r.put(DELTA_KEY, val);
      }
      val = new DeltaTestImpl();
      val.setStr("");
      r.put(LAST_KEY, val);
    } catch (Exception ex) {
      Assertions.fail("failed in createDelta()", ex);
    }
  }

  private void createAndUpdateDeltas() {
    createDelta();
    updateDelta();
  }

  private void createDelta() {
    try {
      Region<String, DeltaTestImpl> r = cache.getRegion("/" + regionName);
      assertThat(r).isNotNull();

      r.create(DELTA_KEY, deltaPut[0]);
    } catch (Exception ex) {
      Assertions.fail("failed in createDelta()", ex);
    }
  }

  private void updateDelta() {
    try {
      Region<String, DeltaTestImpl> r = cache.getRegion("/" + regionName);
      assertThat(r).isNotNull();

      for (int i = 1; i < EVENTS_SIZE; i++) {
        try {
          r.put(DELTA_KEY, deltaPut[i]);
        } catch (InvalidDeltaException ide) {
          assertThat(deltaPut[i].getIntVar())
              .describedAs("InvalidDeltaException not expected for deltaPut[" + i + "]")
              .isEqualTo(DeltaTestImpl.ERRONEOUS_INT_FOR_TO_DELTA);
        }
      }
      r.put(LAST_KEY, deltaPut[0]);
    } catch (Exception ex) {
      Assertions.fail("failed in updateDelta()", ex);
    }
  }

  private void createDeltas() {
    try {
      Region<String, DeltaTestImpl> r = cache.getRegion("/" + regionName);
      assertThat(r).isNotNull();

      for (int i = 0; i < 100; i++) {
        r.create(DELTA_KEY + i, new DeltaTestImpl());
      }
      r.create(LAST_KEY, new DeltaTestImpl());
    } catch (Exception ex) {
      Assertions.fail("failed in createDeltas()", ex);
    }
  }

  private void createAnEntry() {
    try {
      Region<String, DeltaTestImpl> r = cache.getRegion("/" + regionName);
      assertThat(r).isNotNull();
      DeltaTestImpl val = new DeltaTestImpl();
      val.setStr("I push the delta out to disk :)");
      r.create("KEY-A", val);
    } catch (Exception ex) {
      Assertions.fail("failed in createAnEntry()", ex);
    }
  }

  private void invalidateDelta() {
    try {
      Region<String, DeltaTestImpl> r = cache.getRegion("/" + regionName);
      assertThat(r).isNotNull();

      r.invalidate(DELTA_KEY);
    } catch (Exception ex) {
      Assertions.fail("failed in invalidateDelta()" + ex.getMessage());
    }
  }

  private void verifyOverflowOccurred(int regionsize) {
    EvictionController cc = ((LocalRegion) cache.getRegion(regionName)).entries
        .getEvictionController();
    assertThat(cc.getCounters().getEvictions() == 1L)
        .describedAs("Number of evictions expected to be " + 1L + " but was "
            + cc.getCounters().getEvictions())
        .isTrue();
    int rSize = ((LocalRegion) cache.getRegion(regionName)).getRegionMap().size();
    assertThat(rSize == regionsize)
        .describedAs("Region size expected to be " + regionsize + " but was " + rSize).isTrue();
  }

  private void verifyData(int creates, int updates) {
    assertThat(numOfCreates).isEqualTo(creates);
    assertThat(numOfUpdates).isEqualTo(updates);
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
    AttributesFactory<String, DeltaTestImpl> factory = new AttributesFactory<>();
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
      RegionAttributes<String, DeltaTestImpl> attrs = factory.create();
      Region<String, DeltaTestImpl> r = cache.createRegion(regionName, attrs);
      r.create(DELTA_KEY, deltaPut[0]);
    } else {
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setConcurrencyChecksEnabled(false);
      RegionAttributes<String, DeltaTestImpl> attrs = factory.create();
      cache.createRegion(regionName, attrs);
    }

    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    if (ePolicy != null) {
      File overflowDirectory = new File("bsi_overflow_" + port);
      if (!overflowDirectory.mkdir()) {
        throw new Exception("mkdir failed");
      }
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

  private CacheListener<String, DeltaTestImpl> getCacheListener(Integer code) {
    CacheListener<String, DeltaTestImpl> listener = null;
    switch (code) {
      case 0:
        break;
      case SERVER_LISTENER:
        // listener = new CacheListenerAdapter() {};
        break;
      case CLIENT_LISTENER:
        listener = new CacheListenerAdapter<String, DeltaTestImpl>() {
          @Override
          public void afterCreate(EntryEvent event) {
            numOfCreates++;
            logger.debug("Create Event: <" + event.getKey() + ", " + event.getNewValue() + ">");
            if (DELTA_KEY.equals(event.getKey()) && !deltaPut[0].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("Create event:\n |-> sent: ").append(deltaPut[0])
                  .append("\n |-> rcvd: ").append(event.getNewValue()).append("\n");
            } else if (LAST_KEY.equals(event.getKey())) {
              lastKeyReceived = true;
            }
          }

          @Override
          public void afterUpdate(EntryEvent event) {
            numOfUpdates++;
            logger.debug("Update Event: <" + event.getKey() + ", " + event.getNewValue() + ">"
                + ", numOfUpdates: " + numOfUpdates);
            if (!deltaPut[numOfUpdates].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("\nUpdate event(").append(numOfUpdates).append("):\n |-> sent: ")
                  .append(deltaPut[numOfUpdates]).append("\n |-> recd: ")
                  .append(event.getNewValue());
            }
          }
        };
        break;
      case CLIENT_LISTENER_2:
        listener = new CacheListenerAdapter<String, DeltaTestImpl>() {
          @Override
          public void afterCreate(EntryEvent event) {
            numOfCreates++;
            logger.debug("Create Event: <" + event.getKey() + ", " + event.getNewValue() + ">");
            if (DELTA_KEY.equals(event.getKey()) && !deltaPut[0].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("Create event:\n |-> sent: ").append(deltaPut[0])
                  .append("\n |-> rcvd: ").append(event.getNewValue()).append("\n");
            } else if (LAST_KEY.equals(event.getKey())) {
              lastKeyReceived = true;
            }
          }

          @Override
          public void afterUpdate(EntryEvent event) {
            int tmp = ++numOfUpdates;
            logger.debug("Update Event: <" + event.getKey() + ", " + event.getNewValue() + ">"
                + ", numOfUpdates: " + numOfUpdates);
            // Hack to ignore illegal delta put
            tmp = (tmp >= 3) ? ++tmp : tmp;
            if (!deltaPut[tmp].equals(event.getNewValue())) {
              areListenerResultsValid = false;
              listenerError.append("\nUpdate event(").append(numOfUpdates).append("):\n |-> sent: ")
                  .append(deltaPut[tmp]).append("\n |-> recd: ").append(event.getNewValue());
            }
          }
        };
        break;
      case C2S2S_SERVER_LISTENER:
        listener = new CacheListenerAdapter<String, DeltaTestImpl>() {
          @Override
          public void afterCreate(EntryEvent event) {
            numOfCreates++;
            logger.debug("Create Event: <" + event.getKey() + ", " + event.getNewValue() + ">");
            if (LAST_KEY.equals(event.getKey())) {
              lastKeyReceived = true;
            }
          }

          @Override
          public void afterUpdate(EntryEvent event) {
            numOfUpdates++;
            logger.debug("Update Event: <" + event.getKey() + ", " + event.getNewValue() + ">"
                + ", numOfUpdates: " + numOfUpdates);
          }

          @Override
          public void afterInvalidate(EntryEvent event) {
            numOfInvalidates++;
            logger.debug("Invalidate Event: <" + event.getKey() + ", " + event.getOldValue() + ">"
                + ", numOfInvalidates: " + numOfInvalidates);
          }

          @Override
          public void afterDestroy(EntryEvent event) {
            numOfDestroys++;
            logger.debug("Destroy Event: <" + event.getKey() + ", " + event.getOldValue() + ">"
                + ", numOfDestroys: " + numOfDestroys);
          }
        };
        break;
      case LAST_KEY_LISTENER:
        listener = new CacheListenerAdapter<String, DeltaTestImpl>() {
          @Override
          public void afterCreate(EntryEvent event) {
            if (LAST_KEY.equals(event.getKey())) {
              lastKeyReceived = true;
            }
          }
        };
        break;
      case DURABLE_CLIENT_LISTENER:
        listener = new CacheListenerAdapter<String, DeltaTestImpl>() {
          @Override
          public void afterRegionLive(RegionEvent event) {
            logger.info("Marker received");
            if (Operation.MARKER == event.getOperation()) {
              markerReceived.compareAndSet(false, true);
              if (shouldCloseCache) {
                closeCache(); // keepAlive
              }
            }
          }

          @Override
          public void afterCreate(EntryEvent event) {
            logger.debug("CREATE received");
            if (LAST_KEY.equals(event.getKey())) {
              logger.info("LAST KEY received");
              lastKeyReceived = true;
            }
          }

          @Override
          public void afterUpdate(EntryEvent event) {
            assertThat(event.getNewValue()).isNotNull();
          }
        };
        break;
      default:
        Assertions.fail("Invalid listener code");
        break;
    }
    return listener;

  }


  private void createClientCache(Integer port1,
      EvictionAttributes evictAttrs) throws Exception {
    createClientCache(port1, DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT,
        CLIENT_LISTENER, evictAttrs);
  }

  private void createClientCache(Integer port1, Integer listener)
      throws Exception {
    createClientCache(port1, DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT,
        listener, null);
  }

  private void createClientCache(Integer port1, String conflate,
      Integer listener, EvictionAttributes evictAttrs) throws Exception {
    int[] ports;
    ports = new int[] {port1};

    assertThat(ports).describedAs("No server ports provided").isNotNull();
    createClientCache(ports, "0", conflate, listener, evictAttrs);
  }

  private void createClientCache(int[] ports, String rLevel, String conflate, Integer listener,
      EvictionAttributes evictAttrs) throws Exception {

    CacheServerTestUtil.disableShufflingOfEndpoints();

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(CONFLATE_EVENTS, conflate);
    createCache(props);
    AttributesFactory<String, DeltaTestImpl> factory = new AttributesFactory<>();
    ClientServerTestCase.configureConnectionPool(factory, "localhost", ports, true,
        Integer.parseInt(rLevel), 2, null, 1000, 250, false, -2);

    factory.setScope(Scope.LOCAL);

    if (listener != 0) {
      factory.addCacheListener(getCacheListener(listener));
    }

    if (evictAttrs != null) {
      factory.setEvictionAttributes(evictAttrs);
    }

    if (evictAttrs != null && evictAttrs.getAction().isOverflowToDisk()) {
      // create diskstore with overflow dir
      // since it's overflow, no need to recover, so we can use random number as dir name
      int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      File dir = new File("overflow_" + port);
      if (!dir.exists()) {
        if (!dir.mkdir()) {
          throw new Exception("mkdir failed");
        }
      }
      File[] dir1 = new File[] {dir};
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      factory.setDiskStoreName(dsf.setDiskDirs(dir1).create("client_overflow_ds").getName());
    }
    factory.setConcurrencyChecksEnabled(false);
    RegionAttributes<String, DeltaTestImpl> attrs = factory.create();
    cache.createRegion(regionName, attrs);

  }

  private void createCache(Properties props) {
    DistributedSystem ds = getSystem(props);
    ds.disconnect();
    ds = getSystem(props);
    assertThat(ds).isNotNull();
    cache = CacheFactory.create(ds);
    assertThat(cache).isNotNull();
  }

  private void createDurableCacheClient(Pool poolAttr, Properties dsProperties,
      Boolean close) throws Exception {
    new DeltaPropagationDUnitTest().createCache(dsProperties);

    PoolFactoryImpl poolFactory = (PoolFactoryImpl) PoolManager.createFactory();
    poolFactory.init(poolAttr);
    PoolImpl pool = null;

    GeodeAwaitility.await()
        .until(() -> !cache.isReconnecting() && !cache.isClosed());


    while (pool == null) {
      try {
        pool = (PoolImpl) poolFactory.create("DeltaPropagationDUnitTest");
        if (pool == null) {
          Thread.sleep(100);
        }
      } catch (org.apache.geode.cache.NoSubscriptionServersAvailableException e) {
        Thread.sleep(100);
      }
    }

    AttributesFactory<String, DeltaTestImpl> factory = new AttributesFactory<>();
    factory.setScope(Scope.LOCAL);
    factory.setConcurrencyChecksEnabled(false);
    factory.setPoolName(pool.getName());
    if (DeltaPropagationDUnitTest.DURABLE_CLIENT_LISTENER != 0) {
      factory.addCacheListener(getCacheListener(DeltaPropagationDUnitTest.DURABLE_CLIENT_LISTENER));
    }
    RegionAttributes<String, DeltaTestImpl> attrs = factory.create();
    Region<String, DeltaTestImpl> r =
        cache.createRegion(DeltaPropagationDUnitTest.regionName, attrs);
    GeodeAwaitility.await()
        .until(() -> !cache.isReconnecting() && !cache.isClosed());

    r.registerInterestForAllKeys();
    cache.readyForEvents();
    shouldCloseCache = close;
  }

  private void registerInterestListAll() {
    try {
      Region<String, DeltaTestImpl> r = cache.getRegion("/" + regionName);
      assertThat(r).isNotNull();
      r.registerInterestForAllKeys();
    } catch (Exception ex) {
      Assertions.fail("failed in registerInterestListAll", ex);
    }
  }

  private void closeCache() {

    if (cache != null && !cache.isClosed()) {

      cache.close(true);
    }
    closeCacheFinished.compareAndSet(false, true);
    cache = null;
  }

  private boolean isLastKeyReceived() {
    return lastKeyReceived;
  }

  private void resetAll() {
    DeltaTestImpl.resetDeltaInvocationCounters();
    numOfCreates = numOfUpdates = numOfInvalidates = numOfDestroys = 0;
    lastKeyReceived = false;
    markerReceived.set(false);
    areListenerResultsValid = true;
    listenerError = new StringBuffer();
  }
}
