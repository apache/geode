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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.cache.EvictionAction.LOCAL_DESTROY;
import static org.apache.geode.cache.EvictionAction.OVERFLOW_TO_DISK;
import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.cache.client.PoolFactory.DEFAULT_SUBSCRIPTION_REDUNDANCY;
import static org.apache.geode.distributed.ConfigurationProperties.CONFLATE_EVENTS;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.internal.DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_DEFAULT;
import static org.apache.geode.distributed.internal.DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_OFF;
import static org.apache.geode.distributed.internal.DistributionConfig.CLIENT_CONFLATION_PROP_VALUE_ON;
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.apache.geode.internal.cache.CacheServerImpl.generateNameForClientMsgsRegion;
import static org.apache.geode.internal.cache.tier.sockets.CacheClientProxyFactory.INTERNAL_FACTORY_PROPERTY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEMFIRE_PREFIX;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.dunit.rules.DistributedRule.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.shiro.subject.Subject;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DeltaTestImpl;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.client.internal.InternalPool;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.compression.Compressor;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxyFactory.InternalCacheClientProxyFactory;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.MessageDispatcher;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * @since GemFire 6.1
 */
@SuppressWarnings("serial")
public class DeltaPropagationDUnitTest implements Serializable {

  private static final int EVENTS_SIZE = 6;
  private static final String DELTA_KEY = "DELTA_KEY";
  private static final String LAST_KEY = "LAST_KEY";
  private static final InternalCache DUMMY_CACHE = mock(InternalCache.class);
  private static final InternalClientCache DUMMY_CLIENT_CACHE = mock(InternalClientCache.class);
  private static final String CACHE_CLIENT_PROXY_FACTORY_NAME =
      CustomCacheClientProxyFactory.class.getName();

  private static final AtomicReference<CountDownLatch> LATCH =
      new AtomicReference<>(new CountDownLatch(0));

  private static final AtomicBoolean LAST_KEY_RECEIVED = new AtomicBoolean();
  private static final AtomicInteger INVALIDATE_COUNTER = new AtomicInteger();
  private static final AtomicInteger CREATE_COUNTER = new AtomicInteger();
  private static final AtomicInteger UPDATE_COUNTER = new AtomicInteger();
  private static final AtomicBoolean MARKER_RECEIVED = new AtomicBoolean();

  private static final AtomicReference<InternalCache> CACHE = new AtomicReference<>(DUMMY_CACHE);
  private static final AtomicReference<InternalClientCache> CLIENT_CACHE =
      new AtomicReference<>(DUMMY_CLIENT_CACHE);

  private static final DeltaTestImpl[] DELTA_VALUES = new DeltaTestImpl[EVENTS_SIZE];

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  private String regionName;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public DistributedRestoreSystemProperties restoreProps = new DistributedRestoreSystemProperties();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
  @Rule
  public SerializableTestName testName = new SerializableTestName();
  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    for (VM vm : toArray(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        DeltaTestImpl.resetDeltaInvokationCounters();
        CREATE_COUNTER.set(0);
        UPDATE_COUNTER.set(0);
        INVALIDATE_COUNTER.set(0);
        LAST_KEY_RECEIVED.set(false);
        MARKER_RECEIVED.set(false);
        LATCH.set(new CountDownLatch(0));
      });
    }

    regionName = getName() + "_region";
  }

  @After
  public void tearDown() {
    for (VM vm : toArray(getController(), vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        LATCH.get().countDown();
        closeClientCache(false);
        closeCache();
        PoolManager.close();
      });
    }
  }

  @Test
  public void testS2CSuccessfulDeltaPropagationWithCompression() {
    VM clientVM = getController();
    VM serverVM = vm0;

    int serverPort = serverVM.invoke(() -> new ServerFactory()
        .compressor(new SnappyCompressor())
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    serverVM.invoke(() -> {
      assertThat(getCache().getRegion(regionName).getAttributes().getCompressor()).isNotNull();
    });

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new ValidatingClientListener(errorCollector))
          .serverPorts(serverPort)
          .create();

      Region<String, Object> region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");
    });

    for (VM vm : toArray(serverVM, clientVM)) {
      vm.invoke(() -> prepareDeltas());
    }

    serverVM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
      for (int i = 1; i < EVENTS_SIZE; i++) {
        region.put(DELTA_KEY, DELTA_VALUES[i]);
      }
      region.put(LAST_KEY, "");
    });

    clientVM.invoke(() -> await().until(() -> LAST_KEY_RECEIVED.get()));

    long toDeltas = serverVM.invoke(() -> DeltaTestImpl.getToDeltaInvokations());
    long fromDeltas = clientVM.invoke(() -> DeltaTestImpl.getFromDeltaInvokations());

    assertThat(toDeltas).isEqualTo(EVENTS_SIZE - 1);
    assertThat(fromDeltas).isEqualTo(EVENTS_SIZE - 1);

    clientVM.invoke(() -> {
      assertThat(CREATE_COUNTER.get()).isEqualTo(2);
      assertThat(UPDATE_COUNTER.get()).isEqualTo(EVENTS_SIZE - 1);
    });
  }

  @Test
  public void testS2CSuccessfulDeltaPropagation() {
    VM clientVM = getController();
    VM serverVM = vm0;

    int serverPort = serverVM.invoke(() -> new ServerFactory()
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new ClientListener())
          .serverPorts(serverPort)
          .create();

      Region<String, Object> region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");
    });

    for (VM vm : toArray(serverVM, clientVM)) {
      vm.invoke(() -> prepareDeltas());
    }

    serverVM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
      for (int i = 1; i < EVENTS_SIZE; i++) {
        region.put(DELTA_KEY, DELTA_VALUES[i]);
      }
      region.put(LAST_KEY, "");
    });

    clientVM.invoke(() -> await().until(() -> LAST_KEY_RECEIVED.get()));

    long toDeltas = serverVM.invoke(() -> DeltaTestImpl.getToDeltaInvokations());
    long fromDeltas = clientVM.invoke(() -> DeltaTestImpl.getFromDeltaInvokations());

    assertThat(toDeltas).isEqualTo(EVENTS_SIZE - 1);
    assertThat(fromDeltas).isEqualTo(EVENTS_SIZE - 1);

    clientVM.invoke(() -> {
      assertThat(CREATE_COUNTER.get()).isEqualTo(2);
      assertThat(UPDATE_COUNTER.get()).isEqualTo(EVENTS_SIZE - 1);
    });
  }

  @Test
  public void testS2CFailureInToDeltaMethod() {
    VM clientVM = getController();
    VM serverVM = vm0;

    int serverPort = serverVM.invoke(() -> new ServerFactory()
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new SkipThirdDeltaValue(errorCollector))
          .serverPorts(serverPort)
          .create();

      Region<String, Object> region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");
    });

    for (VM vm : toArray(serverVM, clientVM)) {
      vm.invoke(() -> prepareErroneousDeltasForToDelta());
    }

    serverVM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
      for (int i = 1; i < EVENTS_SIZE; i++) {
        try {
          // Note: this may or may not throw
          region.put(DELTA_KEY, DELTA_VALUES[i]);
        } catch (InvalidDeltaException ide) {
          assertThat(DELTA_VALUES[i].getIntVar())
              .isEqualTo(DeltaTestImpl.ERRONEOUS_INT_FOR_TO_DELTA);
        }
      }
      region.put(LAST_KEY, "");
    });

    clientVM.invoke(() -> await().until(() -> LAST_KEY_RECEIVED.get()));

    long toDeltas = serverVM.invoke(() -> DeltaTestImpl.getToDeltaInvokations());
    long fromDeltas = clientVM.invoke(() -> DeltaTestImpl.getFromDeltaInvokations());
    long toDeltaFailures = serverVM.invoke(() -> DeltaTestImpl.getToDeltaFailures());

    assertThat(toDeltas).isEqualTo(EVENTS_SIZE - 1);
    // -1 below is because the one failed in toDelta will be sent as full
    // value. So client will not see it as 'delta'.
    assertThat(fromDeltas).isEqualTo(EVENTS_SIZE - 1 - 1);
    assertThat(toDeltaFailures).isOne();

    clientVM.invoke(() -> {
      // Full value no more sent if toDelta() fails
      assertThat(CREATE_COUNTER.get()).isEqualTo(2);
      assertThat(UPDATE_COUNTER.get()).isEqualTo(EVENTS_SIZE - 1 - 1);
    });
  }

  @Test
  public void testS2CFailureInFromDeltaMethod() {
    VM clientVM = getController();
    VM serverVM = vm0;

    int serverPort = serverVM.invoke(() -> new ServerFactory()
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new ClientListener())
          .serverPorts(serverPort)
          .create();

      Region<String, Object> region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");
    });

    for (VM vm : toArray(serverVM, clientVM)) {
      vm.invoke(() -> prepareErroneousDeltasForFromDelta());
    }

    serverVM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
      for (int i = 1; i < EVENTS_SIZE; i++) {
        region.put(DELTA_KEY, DELTA_VALUES[i]);
      }
      region.put(LAST_KEY, "");
    });

    clientVM.invoke(() -> await().until(() -> LAST_KEY_RECEIVED.get()));

    long toDeltas = serverVM.invoke(() -> DeltaTestImpl.getToDeltaInvokations());
    long fromDeltas = clientVM.invoke(() -> DeltaTestImpl.getFromDeltaInvokations());
    long fromDeltaFailures = clientVM.invoke(() -> DeltaTestImpl.getFromDeltaFailures());

    assertThat(toDeltas).isEqualTo(EVENTS_SIZE - 1);
    assertThat(fromDeltas).isEqualTo(EVENTS_SIZE - 1);
    assertThat(fromDeltaFailures).isOne();

    clientVM.invoke(() -> {
      assertThat(CREATE_COUNTER.get()).isEqualTo(2);
      assertThat(UPDATE_COUNTER.get()).isEqualTo(EVENTS_SIZE - 1);
    });
  }

  @Test
  public void testS2CWithOldValueAtClientOverflownToDisk() {
    VM clientVM = getController();
    VM serverVM = vm0;

    int serverPort = serverVM.invoke(() -> new ServerFactory()
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new ClientListener())
          .evictionAttributes(createLRUEntryAttributes(1, OVERFLOW_TO_DISK))
          .serverPorts(serverPort)
          .create();

      Region<String, Object> region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");
    });

    for (VM vm : toArray(serverVM, clientVM)) {
      vm.invoke(() -> prepareDeltas());
    }

    serverVM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
      region.create("KEY-A", "I push the delta out to disk :)");
    });

    clientVM.invoke(() -> {
      // assert overflow occurred on client vm
      await().untilAsserted(() -> verifyOverflowOccurred(1, 2));
    });

    serverVM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      for (int i = 1; i < EVENTS_SIZE; i++) {
        region.put(DELTA_KEY, DELTA_VALUES[i]);
      }
      region.put(LAST_KEY, "");
    });

    clientVM.invoke(() -> await().until(() -> LAST_KEY_RECEIVED.get()));

    long toDeltas = serverVM.invoke(() -> DeltaTestImpl.getToDeltaInvokations());
    long fromDeltas = clientVM.invoke(() -> DeltaTestImpl.getFromDeltaInvokations());

    assertThat(toDeltas).isEqualTo(EVENTS_SIZE - 1);
    assertThat(fromDeltas).isEqualTo(EVENTS_SIZE - 1);

    clientVM.invoke(() -> {
      assertThat(CREATE_COUNTER.get()).isEqualTo(3);
      assertThat(UPDATE_COUNTER.get()).isEqualTo(EVENTS_SIZE - 1);
    });
  }

  @Test
  public void testS2CWithLocallyDestroyedOldValueAtClient() {
    VM clientVM = getController();
    VM serverVM = vm0;

    int serverPort = serverVM.invoke(() -> new ServerFactory()
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new ClientListener())
          .evictionAttributes(createLRUEntryAttributes(1, LOCAL_DESTROY))
          .serverPorts(serverPort)
          .create();

      Region<String, Object> region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");
    });

    for (VM vm : toArray(serverVM, clientVM)) {
      vm.invoke(() -> prepareDeltas());
    }

    serverVM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
      region.create("KEY-A", "I push the delta out to disk :)");
    });

    clientVM.invoke(() -> {
      // assert overflow occurred on client vm
      await().untilAsserted(() -> verifyOverflowOccurred(1, 1));
    });

    serverVM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      for (int i = 1; i < EVENTS_SIZE; i++) {
        region.put(DELTA_KEY, DELTA_VALUES[i]);
      }
      region.put(LAST_KEY, "");
    });

    clientVM.invoke(() -> await().until(() -> LAST_KEY_RECEIVED.get()));

    long toDeltas = serverVM.invoke(() -> DeltaTestImpl.getToDeltaInvokations());
    long fromDeltas = clientVM.invoke(() -> DeltaTestImpl.getFromDeltaInvokations());

    assertThat(toDeltas).isEqualTo(EVENTS_SIZE - 1);
    assertThat(fromDeltas).isEqualTo(EVENTS_SIZE - 1 - 1);

    clientVM.invoke(() -> {
      assertThat(CREATE_COUNTER.get()).isEqualTo(4);
      assertThat(UPDATE_COUNTER.get()).isEqualTo(EVENTS_SIZE - 2);
    });
  }

  @Test
  public void testS2CWithInvalidatedOldValueAtClient() {
    VM clientVM = getController();
    VM serverVM = vm0;

    int serverPort = serverVM.invoke(() -> new ServerFactory()
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new ClientListener())
          .serverPorts(serverPort)
          .create();

      Region<String, Object> region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");
    });

    for (VM vm : toArray(serverVM, clientVM)) {
      vm.invoke(() -> prepareDeltas());
    }

    serverVM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
      region.invalidate(DELTA_KEY);
      for (int i = 1; i < EVENTS_SIZE; i++) {
        region.put(DELTA_KEY, DELTA_VALUES[i]);
      }
      region.put(LAST_KEY, "");
    });

    clientVM.invoke(() -> await().until(() -> LAST_KEY_RECEIVED.get()));

    long toDeltas = serverVM.invoke(() -> DeltaTestImpl.getToDeltaInvokations());
    long fromDeltas = clientVM.invoke(() -> DeltaTestImpl.getFromDeltaInvokations());

    assertThat(toDeltas).isEqualTo(EVENTS_SIZE - 1);
    assertThat(fromDeltas).isEqualTo(EVENTS_SIZE - 1 - 1);

    clientVM.invoke(() -> {
      assertThat(CREATE_COUNTER.get()).isEqualTo(2);
      assertThat(UPDATE_COUNTER.get()).isEqualTo(EVENTS_SIZE - 1);
    });
  }

  @Test
  public void testS2CDeltaPropagationWithClientConflationON() {
    VM clientVM = getController();
    VM serverVM = vm0;

    int serverPort = serverVM.invoke(() -> new ServerFactory()
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new LastKeyListener())
          .clientConflation(CLIENT_CONFLATION_PROP_VALUE_ON)
          .serverPorts(serverPort)
          .create();

      Region<String, Object> region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");
    });

    serverVM.invoke(() -> {
      prepareDeltas();
      Region<String, Object> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
      for (int i = 1; i < EVENTS_SIZE; i++) {
        region.put(DELTA_KEY, DELTA_VALUES[i]);
      }
      region.put(LAST_KEY, "");
    });

    clientVM.invoke(() -> {
      await().until(() -> LAST_KEY_RECEIVED.get());

      assertThat(DeltaTestImpl.getFromDeltaInvokations()).isZero();
    });
  }

  @Test
  public void testS2CDeltaPropagationWithServerConflationON() {
    VM client1VM = getController();
    VM client2VM = vm3;
    VM serverVM = vm0;

    int serverPort = serverVM.invoke(() -> new ServerFactory()
        .enableSubscriptionConflation(true)
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    client1VM.invoke(() -> new ClientFactory()
        .cacheListener(new LastKeyListener())
        .serverPorts(serverPort)
        .create());

    client2VM.invoke(() -> new ClientFactory()
        .cacheListener(new LastKeyListener())
        .clientConflation(CLIENT_CONFLATION_PROP_VALUE_OFF)
        .serverPorts(serverPort)
        .create());

    for (VM clientVM : toArray(client1VM, client2VM)) {
      clientVM.invoke(() -> {
        Region<String, Object> region = getClientCache().getRegion(regionName);
        region.registerInterest("ALL_KEYS");
      });
    }

    serverVM.invoke(() -> {
      prepareDeltas();
      Region<String, Object> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
      for (int i = 1; i < EVENTS_SIZE; i++) {
        region.put(DELTA_KEY, DELTA_VALUES[i]);
      }
      region.put(LAST_KEY, "");
    });

    for (VM clientVM : toArray(client1VM, client2VM)) {
      clientVM.invoke(() -> await().until(() -> LAST_KEY_RECEIVED.get()));
    }

    client1VM.invoke(() -> {
      assertThat(DeltaTestImpl.getFromDeltaInvokations()).isZero();
    });

    client2VM.invoke(() -> {
      assertThat(DeltaTestImpl.getFromDeltaInvokations()).isEqualTo(EVENTS_SIZE - 1);
    });
  }

  @Test
  public void testS2CDeltaPropagationWithOnlyCreateEvents() {
    VM clientVM = getController();
    VM serverVM = vm0;

    int serverPort = serverVM.invoke(() -> new ServerFactory()
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new LastKeyListener())
          .serverPorts(serverPort)
          .create();

      Region<String, Object> region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");
    });

    serverVM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      for (int i = 0; i < 100; i++) {
        region.create(DELTA_KEY + i, new DeltaTestImpl());
      }
      region.create(LAST_KEY, "");
    });

    clientVM.invoke(() -> await().until(() -> LAST_KEY_RECEIVED.get()));

    serverVM.invoke(() -> {
      assertThat(DeltaTestImpl.getToDeltaInvokations()).isZero();
    });

    clientVM.invoke(() -> {
      assertThat(DeltaTestImpl.getFromDeltaInvokations()).isZero();
    });
  }

  /**
   * Tests that an update on a server with full Delta object causes distribution of the full Delta
   * instance, and not its delta bits, to other peers, even if that instance's
   * {@code hasDelta()} returns true.
   */
  @Test
  public void testC2S2SDeltaPropagation() {
    VM clientVM = getController();
    VM server1VM = vm0;
    VM server2VM = vm1;

    for (VM vm : toArray(clientVM, server1VM, server2VM)) {
      vm.invoke(() -> prepareDeltas());
    }

    int serverPort = server1VM.invoke(() -> {
      int port = new ServerFactory()
          .cacheListener(new ClientToServerToServerListener())
          .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
          .create();

      Region<String, DeltaTestImpl> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);

      return port;
    });
    server2VM.invoke(() -> {
      new ServerFactory()
          .cacheListener(new ClientToServerToServerListener())
          .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
          .create();

      Region<String, DeltaTestImpl> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
    });

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new ClientListener())
          .serverPorts(serverPort)
          .create();

      Region<String, DeltaTestImpl> region = getClientCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
    });

    // Invalidate the value at both the servers.
    for (VM serverVM : toArray(server1VM, server2VM)) {
      serverVM.invoke(() -> {
        Region<String, DeltaTestImpl> region = getCache().getRegion(regionName);
        region.localInvalidate(DELTA_KEY);
      });
    }
    for (VM serverVM : toArray(server1VM, server2VM)) {
      serverVM.invoke(() -> {
        await().untilAsserted(() -> assertThat(INVALIDATE_COUNTER.get()).isEqualTo(1));
      });
    }

    clientVM.invoke(() -> {
      Region<String, DeltaTestImpl> region = getClientCache().getRegion(regionName);
      region.put(DELTA_KEY, DELTA_VALUES[1]);
    });

    // Assert that server1VM distributed val as full value to server2VM.
    server2VM.invoke(() -> await().untilAsserted(() -> {
      Region<String, DeltaTestImpl> region = getCache().getRegion(regionName);
      DeltaTestImpl deltaValue = region.getEntry(DELTA_KEY).getValue();

      assertThat(deltaValue).isEqualTo(DELTA_VALUES[1]);
    }));

    for (VM vm : toArray(server1VM, server2VM)) {
      vm.invoke(() -> {
        assertThat(DeltaTestImpl.deltaFeatureUsed()).isFalse();
      });
    }

    clientVM.invoke(() -> {
      assertThat(DeltaTestImpl.deltaFeatureUsed()).isTrue();
    });
  }

  @Test
  public void testS2S2CDeltaPropagationWithHAOverflow() {
    VM clientVM = getController();
    VM server1VM = vm0;
    VM server2VM = vm1;

    for (VM vm : toArray(clientVM, server1VM, server2VM)) {
      vm.invoke(() -> prepareDeltas());
    }

    for (VM vm : toArray(server1VM, server2VM)) {
      vm.invoke(() -> {
        System.setProperty(INTERNAL_FACTORY_PROPERTY, CACHE_CLIENT_PROXY_FACTORY_NAME);
        LATCH.set(new CountDownLatch(1));
      });
    }

    server1VM.invoke(() -> new ServerFactory()
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());
    int serverPort = server2VM.invoke(() -> new ServerFactory()
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_ENTRY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new ClientListener())
          .serverPorts(serverPort)
          .create();

      Region<String, DeltaTestImpl> region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");
    });

    server1VM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      region.create(DELTA_KEY, DELTA_VALUES[0]);
      for (int i = 1; i < EVENTS_SIZE; i++) {
        region.put(DELTA_KEY, DELTA_VALUES[i]);
      }
      region.put(LAST_KEY, "");
    });

    server2VM.invoke(() -> {
      Region<String, Object> region =
          getCache().getRegion(generateNameForClientMsgsRegion(serverPort));

      EvictionController evictionController = ((DiskRecoveryStore) region)
          .getRegionMap()
          .getEvictionController();

      await().untilAsserted(() -> {
        assertThat(evictionController.getCounters().getEvictions()).isGreaterThan(0);
      });

      LATCH.get().countDown();
    });

    clientVM.invoke(() -> {
      await().until(() -> LAST_KEY_RECEIVED.get());

      long toDeltasOnServer1 = server1VM.invoke(() -> DeltaTestImpl.getToDeltaInvokations());
      long fromDeltasOnServer2 = server2VM.invoke(() -> DeltaTestImpl.getFromDeltaInvokations());
      long toDeltasOnServer2 = server2VM.invoke(() -> DeltaTestImpl.getToDeltaInvokations());
      long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations();

      assertThat(toDeltasOnServer1).isEqualTo(EVENTS_SIZE - 1);
      assertThat(fromDeltasOnServer2).isEqualTo(EVENTS_SIZE - 1);
      assertThat(toDeltasOnServer2).isZero();
      assertThat(fromDeltasOnClient).isEqualTo(EVENTS_SIZE - 1);
    });
  }

  @Test
  public void testS2CDeltaPropagationWithGIIAndFailover() {
    VM clientVM = getController();
    VM server1VM = vm0;
    VM server2VM = vm1;
    VM server3VM = vm2;

    for (VM vm : toArray(clientVM, server1VM, server2VM, server3VM)) {
      vm.invoke(() -> prepareDeltas());
    }

    // Do puts after slowing the dispatcher.
    for (VM vm : toArray(server1VM, server2VM, server3VM)) {
      vm.invoke(() -> {
        System.setProperty(INTERNAL_FACTORY_PROPERTY, CACHE_CLIENT_PROXY_FACTORY_NAME);
        LATCH.set(new CountDownLatch(1));
      });
    }

    int serverPort1 = server1VM.invoke(() -> new ServerFactory()
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());
    int serverPort2 = server2VM.invoke(() -> new ServerFactory()
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());
    int serverPort3 = server3VM.invoke(() -> new ServerFactory()
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    clientVM.invoke(() -> {
      new ClientFactory()
          .cacheListener(new ClientListener())
          .serverPorts(serverPort1, serverPort2, serverPort3)
          .subscriptionRedundancy(1)
          .create();

      Region<String, DeltaTestImpl> region = getClientCache().getRegion(regionName);
      region.registerInterest("ALL_KEYS");

      InternalPool pool = (InternalPool) PoolManager.getAll().values().stream().findFirst().get();

      VM primaryServerVM = pool.getPrimaryPort() == serverPort1 ? server1VM
          : pool.getPrimaryPort() == serverPort2 ? server2VM : server3VM;

      primaryServerVM.invoke(() -> {
        Region<String, Object> regionOnServer = getCache().getRegion(regionName);
        regionOnServer.create(DELTA_KEY, DELTA_VALUES[0]);
        for (int i = 1; i < EVENTS_SIZE; i++) {
          regionOnServer.put(DELTA_KEY, DELTA_VALUES[i]);
        }
        regionOnServer.put(LAST_KEY, "");
        closeCache();
      });

      await().until(() -> LAST_KEY_RECEIVED.get());

      VM primaryServer2VM = pool.getPrimaryPort() == serverPort1 ? server1VM
          : pool.getPrimaryPort() == serverPort2 ? server2VM : server3VM;

      for (VM vm : toArray(server1VM, server2VM, server3VM)) {
        vm.invoke(() -> LATCH.get().countDown());
      }

      primaryServer2VM.invoke(() -> closeCache());

      long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations();
      assertThat(fromDeltasOnClient).isEqualTo(EVENTS_SIZE - 1);
    });
  }

  /**
   * <pre>
   * 1. Create a cache server with slow dispatcher
   * 2. Start a durable client with a custom cache listener which shuts itself down as soon as it
   *    receives a marker message.
   * 3. Do some puts on the server region
   * 4. Let the dispatcher start dispatching
   * 5. Verify that durable client is disconnected as soon as it processes the marker. Server will
   *    retain its queue which has some events (containing deltas) in it.
   * 6. Restart the durable client without the self-destructing listener.
   * 7. Wait till the durable client processes all its events.
   * 8. Verify that no deltas are received by it.
   * </pre>
   */
  @Test
  public void testBug40165ClientReconnects() {
    VM clientVM = getController();
    VM serverVM = vm1;

    int serverPort = serverVM.invoke(() -> {
      // Step 1
      System.setProperty(INTERNAL_FACTORY_PROPERTY, CACHE_CLIENT_PROXY_FACTORY_NAME);
      LATCH.set(new CountDownLatch(1));

      return new ServerFactory()
          .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
          .regionShortcut(RegionShortcut.REPLICATE)
          .create();
    });

    for (VM vm : toArray(clientVM, serverVM)) {
      vm.invoke(() -> prepareDeltas());
    }

    String durableClientId = getName() + "_client";

    Properties clientProperties = new Properties();
    clientProperties.setProperty(LOCATORS, "");
    clientProperties.setProperty(DURABLE_CLIENT_ID, durableClientId);
    clientProperties.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(60));

    // Step 2
    clientVM.invoke(() -> {
      new ClientFactory().create(new ClientCacheFactory(clientProperties));

      Pool pool = PoolManager.createFactory()
          .addServer("localhost", serverPort)
          .setSubscriptionEnabled(true)
          .setSubscriptionAckInterval(1)
          .create("DeltaPropagationDUnitTest");

      LATCH.set(new CountDownLatch(1));

      ClientRegionFactory<String, Object> clientRegionFactory =
          getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);

      clientRegionFactory
          .addCacheListener(new CacheListenerAdapter<String, Object>() {
            @Override
            public void afterRegionLive(RegionEvent event) {
              if (Operation.MARKER == event.getOperation()) {
                closeClientCache(true);
                LATCH.get().countDown();
              }
            }
          });

      clientRegionFactory
          .setConcurrencyChecksEnabled(false);

      clientRegionFactory
          .setPoolName(pool.getName());

      Region<String, Object> region = clientRegionFactory.create(regionName);

      region.registerInterest("ALL_KEYS");

      getClientCache().readyForEvents();
    });

    serverVM.invoke(() -> {
      // Step 3
      Region<String, Object> region = getCache().getRegion(regionName);
      for (int i = 0; i < 100; i++) {
        DeltaTestImpl value = new DeltaTestImpl();
        value.setStr(String.valueOf(i));
        region.put(DELTA_KEY, value);
      }
      region.put(LAST_KEY, "");

      // Step 4
      LATCH.get().countDown();
    });

    clientVM.invoke(() -> {
      // Step 5
      LATCH.get().await(getTimeout().toMillis(), MILLISECONDS);

      // Step 6
      new ClientFactory().create(new ClientCacheFactory(clientProperties));

      Pool pool = PoolManager.createFactory()
          .addServer("localhost", serverPort)
          .setSubscriptionAckInterval(1)
          .setSubscriptionEnabled(true)
          .create("DeltaPropagationDUnitTest");

      AtomicReference<Object> afterCreateKey = new AtomicReference<>();

      ClientRegionFactory<String, Object> clientRegionFactory =
          getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);

      clientRegionFactory
          .addCacheListener(new CacheListenerAdapter<String, Object>() {
            @Override
            public void afterCreate(EntryEvent<String, Object> event) {
              afterCreateKey.set(event.getKey());
            }
          });

      clientRegionFactory
          .setConcurrencyChecksEnabled(false);

      clientRegionFactory
          .setPoolName(pool.getName());

      Region<String, Object> region = clientRegionFactory.create(regionName);

      region.registerInterest("ALL_KEYS");

      getClientCache().readyForEvents();

      // Step 7
      await().untilAsserted(() -> assertThat(afterCreateKey.get()).isEqualTo(LAST_KEY));

      // Step 8
      long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations();
      assertThat(fromDeltasOnClient).isLessThan(1);
    });
  }

  @Test
  public void testBug40165ClientFailsOver() {
    VM clientVM = getController();
    VM server1VM = vm0;
    VM server2VM = vm1;

    int serverPort1 = server1VM.invoke(() -> new ServerFactory()
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    // 1. Create two cache servers with slow dispatcher
    // 2. Start a durable client with a custom cache listener
    // 3. Do some puts on the server region
    // 4. Let the dispatcher start dispatching
    // 5. Wait till the durable client receives marker from its primary.
    // 6. Kill the primary server, so that the second one becomes primary.
    // 7. Wait till the durable client processes all its events.
    // 8. Verify that expected number of deltas are received by it.

    // Step 0
    for (VM vm : toArray(getController(), server1VM, server2VM)) {
      vm.invoke(() -> prepareDeltas());
    }

    // Step 1
    for (VM serverVM : toArray(server1VM, server2VM)) {
      serverVM.invoke(() -> {
        System.setProperty(INTERNAL_FACTORY_PROPERTY, CACHE_CLIENT_PROXY_FACTORY_NAME);
        LATCH.set(new CountDownLatch(1));
      });
    }

    int serverPort2 = server2VM.invoke(() -> new ServerFactory()
        .evictionPolicy(HARegionQueue.HA_EVICTION_POLICY_MEMORY)
        .regionShortcut(RegionShortcut.REPLICATE)
        .create());

    // Step 2
    clientVM.invoke(() -> {
      String durableClientId = getName() + "_client";

      Properties props = new Properties();
      props.setProperty(LOCATORS, "");
      props.setProperty(DURABLE_CLIENT_ID, durableClientId);
      props.setProperty(DURABLE_CLIENT_TIMEOUT, String.valueOf(60));

      new ClientFactory().create(new ClientCacheFactory(props));

      Pool pool = PoolManager.createFactory()
          .addServer("localhost", serverPort1)
          .addServer("localhost", serverPort2)
          .setSubscriptionEnabled(true)
          .setSubscriptionAckInterval(1)
          .setSubscriptionRedundancy(2)
          .create(getName() + "_pool");

      ClientRegionFactory<String, Object> clientRegionFactory =
          getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);

      clientRegionFactory
          .addCacheListener(new DurableClientListener(errorCollector));

      clientRegionFactory
          .setConcurrencyChecksEnabled(false);

      clientRegionFactory
          .setPoolName(pool.getName());

      Region<String, Object> region = clientRegionFactory.create(regionName);

      region.registerInterest("ALL_KEYS");
      getClientCache().readyForEvents();
    });

    // Step 3
    server1VM.invoke(() -> {
      Region<String, Object> region = getCache().getRegion(regionName);
      for (int i = 0; i < 100; i++) {
        DeltaTestImpl value = new DeltaTestImpl();
        value.setStr(String.valueOf(i));
        region.put(DELTA_KEY, value);
      }
      region.put(LAST_KEY, "");
    });

    for (VM serverVM : toArray(server1VM, server2VM)) {
      serverVM.invoke(() -> LATCH.get().countDown());
    }

    // Step 5
    clientVM.invoke(() -> {
      InternalPool pool = (InternalPool) PoolManager.getAll().values().stream().findFirst().get();
      VM primaryVM = pool.getPrimaryPort() == serverPort1 ? server1VM : server2VM;

      await().until(() -> MARKER_RECEIVED.get());

      // Step 6
      primaryVM.invoke(() -> closeCache());

      // Step 7
      await().until(() -> LAST_KEY_RECEIVED.get());

      // Step 8
      long fromDeltasOnClient = DeltaTestImpl.getFromDeltaInvokations();
      assertThat(fromDeltasOnClient).isGreaterThanOrEqualTo(99);
    });
  }

  private String getName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  private InternalCache getCache() {
    return CACHE.get();
  }

  private void closeCache() {
    CACHE.getAndSet(DUMMY_CACHE).close();
  }

  private InternalClientCache getClientCache() {
    return CLIENT_CACHE.get();
  }

  private void closeClientCache(boolean keepAlive) {
    CLIENT_CACHE.getAndSet(DUMMY_CLIENT_CACHE).close(keepAlive);
  }

  private void prepareDeltas() {
    for (int i = 0; i < EVENTS_SIZE; i++) {
      DELTA_VALUES[i] =
          new DeltaTestImpl(0, "0", 0d, new byte[0], new TestObjectWithIdentifier("0", 0));
    }
    DELTA_VALUES[1].setIntVar(5);
    DELTA_VALUES[2].setIntVar(5);
    DELTA_VALUES[3].setIntVar(5);
    DELTA_VALUES[4].setIntVar(5);
    DELTA_VALUES[5].setIntVar(5);

    DELTA_VALUES[2].resetDeltaStatus();
    DELTA_VALUES[2].setByteArr(new byte[] {1, 2, 3, 4, 5});
    DELTA_VALUES[3].setByteArr(new byte[] {1, 2, 3, 4, 5});
    DELTA_VALUES[4].setByteArr(new byte[] {1, 2, 3, 4, 5});
    DELTA_VALUES[5].setByteArr(new byte[] {1, 2, 3, 4, 5});

    DELTA_VALUES[3].resetDeltaStatus();
    DELTA_VALUES[3].setDoubleVar(5d);
    DELTA_VALUES[4].setDoubleVar(5d);
    DELTA_VALUES[5].setDoubleVar(5d);

    DELTA_VALUES[4].resetDeltaStatus();
    DELTA_VALUES[4].setStr("str changed");
    DELTA_VALUES[5].setStr("str changed");

    DELTA_VALUES[5].resetDeltaStatus();
    DELTA_VALUES[5].setIntVar(100);
    DELTA_VALUES[5].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));
  }

  private void prepareErroneousDeltasForToDelta() {
    for (int i = 0; i < EVENTS_SIZE; i++) {
      DELTA_VALUES[i] =
          new DeltaTestImpl(0, "0", 0d, new byte[0], new TestObjectWithIdentifier("0", 0));
    }
    DELTA_VALUES[1].setIntVar(5);
    DELTA_VALUES[2].setIntVar(5);
    DELTA_VALUES[3].setIntVar(DeltaTestImpl.ERRONEOUS_INT_FOR_TO_DELTA);
    DELTA_VALUES[4].setIntVar(5);
    DELTA_VALUES[5].setIntVar(5);

    DELTA_VALUES[2].setByteArr(new byte[] {1, 2, 3, 4, 5});
    DELTA_VALUES[3].setByteArr(new byte[] {1, 2, 3, 4, 5});
    DELTA_VALUES[4].setByteArr(new byte[] {1, 2, 3, 4, 5});
    DELTA_VALUES[5].setByteArr(new byte[] {1, 2, 3, 4, 5});

    DELTA_VALUES[3].setDoubleVar(5d);
    DELTA_VALUES[4].setDoubleVar(5d);
    DELTA_VALUES[5].setDoubleVar(5d);

    DELTA_VALUES[4].setStr("str changed");
    DELTA_VALUES[5].setStr("str changed");

    DELTA_VALUES[5].setIntVar(100);
    DELTA_VALUES[5].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));
  }

  private void prepareErroneousDeltasForFromDelta() {
    for (int i = 0; i < EVENTS_SIZE; i++) {
      DELTA_VALUES[i] =
          new DeltaTestImpl(0, "0", 0d, new byte[0], new TestObjectWithIdentifier("0", 0));
    }
    DELTA_VALUES[1].setIntVar(5);
    DELTA_VALUES[2].setIntVar(5);
    DELTA_VALUES[3].setIntVar(5);
    DELTA_VALUES[4].setIntVar(5);
    DELTA_VALUES[5].setIntVar(5);

    DELTA_VALUES[2].setByteArr(new byte[] {1, 2, 3, 4, 5});
    DELTA_VALUES[3].setByteArr(new byte[] {1, 2, 3, 4, 5});
    DELTA_VALUES[4].setByteArr(new byte[] {1, 2, 3, 4, 5});
    DELTA_VALUES[5].setByteArr(new byte[] {1, 2, 3, 4, 5});

    DELTA_VALUES[3].setDoubleVar(5d);
    DELTA_VALUES[4].setDoubleVar(5d);
    DELTA_VALUES[5].setDoubleVar(5d);

    DELTA_VALUES[4].setStr("str changed");
    DELTA_VALUES[5].setStr(DeltaTestImpl.ERRONEOUS_STRING_FOR_FROM_DELTA);

    DELTA_VALUES[5].setIntVar(100);
    DELTA_VALUES[5].setTestObj(new TestObjectWithIdentifier("CHANGED", 100));
  }

  private void verifyOverflowOccurred(long expectedEvictions, int expectedRegionSize) {
    DiskRecoveryStore region = (DiskRecoveryStore) getClientCache().getRegion(regionName);
    RegionMap regionMap = region.getRegionMap();

    long evictions = regionMap.getEvictionController().getCounters().getEvictions();
    assertThat(evictions).isEqualTo(expectedEvictions);

    int regionSize = regionMap.size();
    assertThat(regionSize).isEqualTo(expectedRegionSize);
  }

  private class ServerFactory {

    private CacheListener<String, Object> cacheListener;
    private Compressor compressor;
    private boolean enableSubscriptionConflation;
    private String evictionPolicy = HARegionQueue.HA_EVICTION_POLICY_NONE;
    private RegionShortcut regionShortcut;

    private ServerFactory cacheListener(CacheListener<String, Object> cacheListener) {
      this.cacheListener = cacheListener;
      return this;
    }

    private ServerFactory compressor(Compressor compressor) {
      this.compressor = compressor;
      return this;
    }

    private ServerFactory enableSubscriptionConflation(boolean enableSubscriptionConflation) {
      this.enableSubscriptionConflation = enableSubscriptionConflation;
      return this;
    }

    private ServerFactory evictionPolicy(String evictionPolicy) {
      this.evictionPolicy = evictionPolicy;
      return this;
    }

    private ServerFactory regionShortcut(RegionShortcut regionShortcut) {
      this.regionShortcut = regionShortcut;
      return this;
    }

    private int create() {
      try {
        return doCreate();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private int doCreate() throws IOException {
      ConnectionTable.threadWantsSharedResources();

      CACHE.set((InternalCache) new CacheFactory(getDistributedSystemProperties()).create());

      RegionFactory<String, Object> factory;
      if (regionShortcut == null) {
        factory = getCache().createRegionFactory();
        factory.setDataPolicy(DataPolicy.NORMAL);
        factory.setScope(Scope.DISTRIBUTED_NO_ACK);
      } else {
        factory = getCache().createRegionFactory(regionShortcut);
      }

      if (cacheListener != null) {
        factory.addCacheListener(cacheListener);
      }
      if (compressor != null) {
        factory.setCompressor(compressor);
      }

      factory.setConcurrencyChecksEnabled(false);
      factory.setEnableSubscriptionConflation(enableSubscriptionConflation);

      factory.create(regionName);

      int port = getRandomAvailablePort(SOCKET);
      CacheServer cacheServer = getCache().addCacheServer();
      cacheServer.setPort(port);

      if (evictionPolicy != null) {
        File overflowDirectory = temporaryFolder.newFolder("bsi_overflow_" + port);

        DiskStore diskStore = getCache().createDiskStoreFactory()
            .setDiskDirs(new File[] {overflowDirectory})
            .create("bsi");

        ClientSubscriptionConfig clientSubscriptionConfig =
            cacheServer.getClientSubscriptionConfig();

        clientSubscriptionConfig.setCapacity(1);
        clientSubscriptionConfig.setDiskStoreName(diskStore.getName());
        clientSubscriptionConfig.setEvictionPolicy(evictionPolicy);
      }

      cacheServer.start();
      return cacheServer.getPort();
    }
  }

  private class ClientFactory {

    private CacheListener<String, Object> cacheListener;
    private String clientConflation = CLIENT_CONFLATION_PROP_VALUE_DEFAULT;
    private EvictionAttributes evictionAttributes;
    private int[] serverPorts;
    private int subscriptionRedundancy = DEFAULT_SUBSCRIPTION_REDUNDANCY;

    private ClientFactory cacheListener(CacheListener<String, Object> cacheListener) {
      this.cacheListener = cacheListener;
      return this;
    }

    private ClientFactory clientConflation(String clientConflation) {
      this.clientConflation = clientConflation;
      return this;
    }

    private ClientFactory evictionAttributes(EvictionAttributes evictionAttributes) {
      this.evictionAttributes = evictionAttributes;
      return this;
    }

    private ClientFactory serverPorts(int... serverPorts) {
      this.serverPorts = serverPorts;
      return this;
    }

    private ClientFactory subscriptionRedundancy(int subscriptionRedundancy) {
      this.subscriptionRedundancy = subscriptionRedundancy;
      return this;
    }

    private void create() {
      try {
        doCreate();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private void create(ClientCacheFactory clientCacheFactory) {
      CLIENT_CACHE.set((InternalClientCache) clientCacheFactory.create());
    }

    private void doCreate() throws IOException {
      System.setProperty(GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints", "true");

      Properties clientProperties = getDistributedSystemProperties();
      clientProperties.setProperty(LOCATORS, "");
      clientProperties.setProperty(CONFLATE_EVENTS, clientConflation);

      create(new ClientCacheFactory(clientProperties));

      PoolFactory poolFactory = PoolManager.createFactory();
      for (int port : serverPorts) {
        poolFactory.addServer("localhost", port);
      }

      Pool pool = poolFactory
          .setMinConnections(2 * serverPorts.length)
          .setPingInterval(1000)
          .setIdleTimeout(250)
          .setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(subscriptionRedundancy)
          .setSubscriptionAckInterval(1)
          .create(getName() + "_pool");

      ClientRegionFactory<String, Object> regionFactory =
          getClientCache().createClientRegionFactory(ClientRegionShortcut.LOCAL);

      regionFactory.setPoolName(pool.getName());

      if (cacheListener != null) {
        regionFactory.addCacheListener(cacheListener);
      }
      if (evictionAttributes != null) {
        regionFactory.setEvictionAttributes(evictionAttributes);
      }
      if (evictionAttributes != null && evictionAttributes.getAction().isOverflowToDisk()) {
        // create diskstore with overflow dir
        // since it's overflow, no need to recover, so we can use random number as dir name
        File overflowDir = temporaryFolder.newFolder("overflow_" + getVMId());

        DiskStore diskStore = getClientCache().createDiskStoreFactory()
            .setDiskDirs(new File[] {overflowDir})
            .create("client_overflow_ds");

        regionFactory.setDiskStoreName(diskStore.getName());
      }

      regionFactory.setConcurrencyChecksEnabled(false);

      regionFactory.create(regionName);
    }
  }

  private static class ClientListener extends CacheListenerAdapter<String, Object> {

    @Override
    public void afterCreate(EntryEvent event) {
      CREATE_COUNTER.incrementAndGet();
      if (LAST_KEY.equals(event.getKey())) {
        LAST_KEY_RECEIVED.set(true);
      }
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      UPDATE_COUNTER.incrementAndGet();
    }
  }

  private static class ValidatingClientListener extends CacheListenerAdapter<String, Object> {

    private final SharedErrorCollector errorCollector;

    private ValidatingClientListener(SharedErrorCollector errorCollector) {
      this.errorCollector = errorCollector;
    }

    @Override
    public void afterCreate(EntryEvent event) {
      CREATE_COUNTER.incrementAndGet();
      if (DELTA_KEY.equals(event.getKey())) {
        errorCollector.checkThat(event.getNewValue(), equalTo(DELTA_VALUES[0]));
      } else if (LAST_KEY.equals(event.getKey())) {
        LAST_KEY_RECEIVED.set(true);
      }
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      int index = UPDATE_COUNTER.incrementAndGet();
      errorCollector.checkThat(event.getNewValue(), equalTo(DELTA_VALUES[index]));
    }
  }

  private static class SkipThirdDeltaValue extends ValidatingClientListener {

    private final SharedErrorCollector errorCollector;

    private SkipThirdDeltaValue(SharedErrorCollector errorCollector) {
      super(errorCollector);
      this.errorCollector = errorCollector;
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      int index = UPDATE_COUNTER.incrementAndGet();

      // Hack to ignore illegal delta put (skip the 3rd delta value)
      index = index >= 3 ? ++index : index;

      errorCollector.checkThat(event.getNewValue(), equalTo(DELTA_VALUES[index]));
    }
  }

  private static class ClientToServerToServerListener extends CacheListenerAdapter<String, Object> {

    @Override
    public void afterCreate(EntryEvent event) {
      CREATE_COUNTER.incrementAndGet();
      if (LAST_KEY.equals(event.getKey())) {
        LAST_KEY_RECEIVED.set(true);
      }
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      UPDATE_COUNTER.incrementAndGet();
    }

    @Override
    public void afterInvalidate(EntryEvent event) {
      INVALIDATE_COUNTER.incrementAndGet();
    }
  }

  private static class LastKeyListener extends CacheListenerAdapter<String, Object> {

    @Override
    public void afterCreate(EntryEvent event) {
      if (LAST_KEY.equals(event.getKey())) {
        LAST_KEY_RECEIVED.set(true);
      }
    }
  }

  private static class DurableClientListener extends CacheListenerAdapter<String, Object> {

    private final SharedErrorCollector errorCollector;

    private DurableClientListener(SharedErrorCollector errorCollector) {
      this.errorCollector = errorCollector;
    }

    @Override
    public void afterRegionLive(RegionEvent event) {
      if (Operation.MARKER == event.getOperation()) {
        MARKER_RECEIVED.set(true);
      }
    }

    @Override
    public void afterCreate(EntryEvent event) {
      if (LAST_KEY.equals(event.getKey())) {
        LAST_KEY_RECEIVED.set(true);
      }
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      errorCollector.checkThat(event.getNewValue(), notNullValue());
    }
  }

  public static class CustomCacheClientProxyFactory implements InternalCacheClientProxyFactory {

    @Override
    public CacheClientProxy create(CacheClientNotifier notifier, Socket socket,
        ClientProxyMembershipID proxyId, boolean isPrimary, byte clientConflation,
        Version clientVersion, long acceptorId, boolean notifyBySubscription,
        SecurityService securityService, Subject subject, StatisticsClock statisticsClock)
        throws CacheException {
      return new CustomCacheClientProxy(notifier, socket, proxyId, isPrimary, clientConflation,
          clientVersion, acceptorId, notifyBySubscription, securityService, subject,
          statisticsClock);
    }
  }

  private static class CustomCacheClientProxy extends CacheClientProxy {

    private CustomCacheClientProxy(CacheClientNotifier notifier, Socket socket,
        ClientProxyMembershipID proxyId, boolean isPrimary, byte clientConflation,
        Version clientVersion, long acceptorId, boolean notifyBySubscription,
        SecurityService securityService, Subject subject, StatisticsClock statisticsClock)
        throws CacheException {
      super(notifier.getCache(), notifier, socket, proxyId, isPrimary, clientConflation,
          clientVersion, acceptorId, notifyBySubscription, securityService, subject,
          statisticsClock,
          notifier.getCache().getInternalDistributedSystem().getStatisticsManager(),
          DEFAULT_CACHECLIENTPROXYSTATSFACTORY, CustomMessageDispatcher::new);
    }
  }

  private static class CustomMessageDispatcher extends MessageDispatcher {

    private CustomMessageDispatcher(CacheClientProxy proxy, String name,
        StatisticsClock statisticsClock) throws CacheException {
      super(proxy, name, statisticsClock);
    }

    @Override
    public void run() {
      try {
        LATCH.get().await(getTimeout().toMillis(), MILLISECONDS);
      } catch (InterruptedException ignore) {
        // ignored
      }
      runDispatcher();
    }
  }
}
