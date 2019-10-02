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
package org.apache.geode.management;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.internal.DistributionConfig.RESTRICT_MEMBERSHIP_PORT_RANGE;
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.NetworkUtils.getServerHostName;
import static org.apache.geode.test.dunit.Wait.pause;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.net.ConnectException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.Role;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.InternalClientMembership;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.management.membership.ClientMembership;
import org.apache.geode.management.membership.ClientMembershipEvent;
import org.apache.geode.management.membership.ClientMembershipListener;
import org.apache.geode.management.membership.MembershipEvent;
import org.apache.geode.management.membership.MembershipListener;
import org.apache.geode.management.membership.UniversalMembershipListenerAdapter;
import org.apache.geode.management.membership.UniversalMembershipListenerAdapter.AdaptedMembershipEvent;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;

/**
 * Distributed tests for {@link UniversalMembershipListenerAdapter}.
 *
 * @since GemFire 4.2.1
 */

public class UniversalMembershipListenerAdapterDUnitTest extends ClientServerTestCase {

  private static final int JOINED = 0;
  private static final int LEFT = 1;
  private static final int CRASHED = 2;

  /** Maximum millis allowed for client to fully connect before test fails */
  private static final int JOIN_FAIL_MILLIS = 30 * 1000;

  /**
   * Millis to wait for notification of all three event listeners to be notified
   * <p>
   *
   * Use Integer.MAX_VALUE for debugging
   */
  private static final int ASYNC_EVENT_WAIT_MILLIS = 30 * 1000;

  private static int serverPort;

  private final MembershipNotification joinSystemNotification = new MembershipNotification();
  private final MembershipNotification leftSystemNotification = new MembershipNotification();
  private final MembershipNotification crashedSystemNotification = new MembershipNotification();

  private final MembershipNotification joinAdapterNotification = new MembershipNotification();
  private final MembershipNotification leftAdapterNotification = new MembershipNotification();
  private final MembershipNotification crashedAdapterNotification = new MembershipNotification();

  private final MembershipNotification joinClientNotification = new MembershipNotification();
  private final MembershipNotification leftClientNotification = new MembershipNotification();
  private final MembershipNotification crashedClientNotification = new MembershipNotification();

  @Rule
  public DistributedRestoreSystemProperties distributedRestoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
    InternalClientMembership.unregisterAllListeners();
    ServerConnection.setForceClientCrashEvent(false);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.put(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    return config;
  }

  /**
   * Tests wrapping of ClientMembershipEvent fired as MembershipEvent.
   */
  @Test
  public void testAdaptedBridgeEvents() throws Exception {
    getSystem();

    // apparently construction registers with ClientMembership
    new UniversalMembershipListenerAdapter() {
      @Override
      public void memberJoined(final MembershipEvent event) {
        joinAdapterNotification.notify(event);
      }
    };

    DistributedMember member = new FakeDistributedMember("member");
    InternalClientMembership.notifyClientJoined(member);

    joinAdapterNotification.awaitNotification(30, SECONDS);
    joinAdapterNotification.validate(member);
  }

  /**
   * Tests use of history to prevent duplicate events.
   */
  @Test
  public void testNoDuplicates() throws Exception {
    getSystem();

    // apparently construction registers with ClientMembership
    new UniversalMembershipListenerAdapter() {
      @Override
      public void memberJoined(final MembershipEvent event) {
        joinAdapterNotification.notify(event);
      }

      @Override
      public void memberLeft(final MembershipEvent event) {
        joinAdapterNotification.notify(event);
      }

      @Override
      public void memberCrashed(final MembershipEvent event) {
        joinAdapterNotification.notify(event);
      }
    };

    DistributedMember member = new FakeDistributedMember("member");

    // first join
    InternalClientMembership.notifyClientJoined(member);

    joinAdapterNotification.awaitNotification(30, SECONDS);
    joinAdapterNotification.validate(member);
    joinAdapterNotification.reset();

    // duplicate join
    InternalClientMembership.notifyClientJoined(member);

    joinAdapterNotification.awaitWithoutNotification(5, SECONDS);
    joinAdapterNotification.validateNotNotified();

    // first left
    InternalClientMembership.notifyClientLeft(member);

    joinAdapterNotification.awaitNotification(30, SECONDS);
    joinAdapterNotification.validate(member);
    joinAdapterNotification.reset();

    // duplicate left
    InternalClientMembership.notifyClientLeft(member);

    joinAdapterNotification.awaitWithoutNotification(5, SECONDS);
    joinAdapterNotification.validateNotNotified();

    // rejoin
    InternalClientMembership.notifyClientJoined(member);

    joinAdapterNotification.awaitNotification(30, SECONDS);
    joinAdapterNotification.validate(member);
  }

  /**
   * Tests notification of events for loner bridge clients in server process.
   */
  @Test
  public void testLonerClientEventsInServer() throws Exception {
    MembershipListener systemListener = new MembershipListener() {
      @Override
      public void memberJoined(final MembershipEvent event) {
        joinSystemNotification.notify(event);
      }

      @Override
      public void memberLeft(final MembershipEvent event) {
        leftSystemNotification.notify(event);
      }

      @Override
      public void memberCrashed(final MembershipEvent event) {
        crashedSystemNotification.notify(event);
      }
    };

    UniversalMembershipListenerAdapter adapter = new UniversalMembershipListenerAdapter() {
      @Override
      public void memberJoined(final MembershipEvent event) {
        joinAdapterNotification.notify((AdaptedMembershipEvent) event);
      }

      @Override
      public void memberLeft(final MembershipEvent event) {
        leftAdapterNotification.notify((AdaptedMembershipEvent) event);
      }

      @Override
      public synchronized void memberCrashed(final MembershipEvent event) {
        crashedAdapterNotification.notify((AdaptedMembershipEvent) event);
      }
    };

    ClientMembershipListener bridgeListener = new ClientMembershipListener() {
      @Override
      public void memberJoined(final ClientMembershipEvent event) {
        joinClientNotification.notify(event);
      }

      @Override
      public void memberLeft(final ClientMembershipEvent event) {
        leftClientNotification.notify(event);
      }

      @Override
      public void memberCrashed(final ClientMembershipEvent event) {
        crashedClientNotification.notify(event);
      }
    };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    String name = this.getUniqueName();
    int[] ports = new int[1];

    // create CacheServer in controller vm...
    getSystem();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region region = createRegion(name, factory.create());

    assertThat(region).isNotNull();
    assertThat(getRootRegion().getSubregion(name)).isNotNull();

    ports[0] = startBridgeServer(0);
    assertThat(ports[0] != 0).isTrue();

    DistributedMember serverMember = getMemberId();
    String serverMemberId = serverMember.getId();

    // register the bridge listener
    ClientMembership.registerClientMembershipListener(bridgeListener);

    InternalCache cache = getInternalCache();
    ManagementService service = ManagementService.getExistingManagementService(cache);
    // register the system listener
    service.addMembershipListener(systemListener);

    // register the universal adapter. Not required as this test is only for BridgeClient test
    adapter.registerMembershipListener(service);

    SerializableCallable createBridgeClient = new SerializableCallable("Create bridge client") {
      @Override
      public Object call() {
        Properties config = new Properties();
        config.setProperty(MCAST_PORT, "0");
        config.setProperty(LOCATORS, "");
        config.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");
        getSystem(config);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, getServerHostName(host), ports, false,
            -1, -1, null);
        createRegion(name, factory.create());
        assertThat(getRootRegion().getSubregion(name)).isNotNull();
        return getMemberId();
      }
    };

    // create bridge client in vm0...
    DistributedMember clientMember = (DistributedMember) vm0.invoke(createBridgeClient);
    String clientMemberId = clientMember.getId();

    // should trigger both adapter and bridge listener but not system listener
    joinAdapterNotification.awaitNotification(30, SECONDS);
    joinClientNotification.awaitNotification(30, SECONDS);

    joinSystemNotification.validateNotNotified();
    joinAdapterNotification.validate(clientMember);
    joinClientNotification.validate(clientMember);

    joinSystemNotification.reset();
    joinAdapterNotification.reset();
    joinClientNotification.reset();

    vm0.invoke(new SerializableRunnable("Wait for client to fully connect") {
      @Override
      public void run() {
        String poolName = getRootRegion().getSubregion(name).getAttributes().getPoolName();
        PoolImpl pool = (PoolImpl) PoolManager.find(poolName);
        waitForClientToFullyConnect(pool);
      }
    });

    vm0.invoke(new SerializableRunnable("Close bridge client region") {
      @Override
      public void run() {
        getRootRegion().getSubregion(name).close();
        PoolManager.close();
      }
    });

    leftAdapterNotification.awaitNotification(30, SECONDS);
    leftClientNotification.awaitNotification(30, SECONDS);

    leftSystemNotification.validateNotNotified();
    leftAdapterNotification.validate(clientMember);
    leftClientNotification.validate(clientMember);

    leftSystemNotification.reset();
    leftAdapterNotification.reset();
    leftClientNotification.reset();

    // reconnect bridge client to test for crashed event
    clientMember = (DistributedMember) vm0.invoke(createBridgeClient);

    joinAdapterNotification.awaitNotification(30, SECONDS);
    joinClientNotification.awaitNotification(30, SECONDS);

    joinSystemNotification.validateNotNotified();
    joinAdapterNotification.validate(clientMember);
    joinClientNotification.validate(clientMember);

    joinSystemNotification.reset();
    joinAdapterNotification.reset();
    joinClientNotification.reset();

    vm0.invoke(new SerializableRunnable("Wait for client to fully connect") {
      @Override
      public void run() {
        String poolName = getRootRegion().getSubregion(name).getAttributes().getPoolName();
        PoolImpl pool = (PoolImpl) PoolManager.find(poolName);
        waitForClientToFullyConnect(pool);
      }
    });

    ServerConnection.setForceClientCrashEvent(true);

    vm0.invoke(new SerializableRunnable("Stop bridge client") {
      @Override
      public void run() {
        getRootRegion().getSubregion(name).close();
        PoolManager.close();
      }
    });

    crashedAdapterNotification.awaitNotification(30, SECONDS);
    crashedClientNotification.awaitNotification(30, SECONDS);

    crashedSystemNotification.validateNotNotified();
    crashedAdapterNotification.validate(clientMember);
    crashedClientNotification.validate(clientMember);
  }

  /**
   * Tests notification of events for loner bridge clients in server process.
   */
  @Test
  public void testSystemClientEventsInServer() throws Exception {
    boolean[] firedSystem = new boolean[3];
    DistributedMember[] memberSystem = new DistributedMember[3];
    String[] memberIdSystem = new String[3];
    boolean[] isClientSystem = new boolean[3];

    boolean[] firedAdapter = new boolean[3];
    DistributedMember[] memberAdapter = new DistributedMember[3];
    String[] memberIdAdapter = new String[3];
    boolean[] isClientAdapter = new boolean[3];

    boolean[] firedBridge = new boolean[3];
    DistributedMember[] memberBridge = new DistributedMember[3];
    String[] memberIdBridge = new String[3];
    boolean[] isClientBridge = new boolean[3];

    boolean[] firedSystemDuplicate = new boolean[3];
    boolean[] firedAdapterDuplicate = new boolean[3];
    boolean[] firedBridgeDuplicate = new boolean[3];

    MembershipListener systemListener = new MembershipListener() {
      @Override
      public synchronized void memberJoined(MembershipEvent event) {
        firedSystemDuplicate[JOINED] = firedSystem[JOINED];
        firedSystem[JOINED] = true;
        memberSystem[JOINED] = event.getDistributedMember();
        memberIdSystem[JOINED] = event.getMemberId();
        notifyAll();
      }

      @Override
      public synchronized void memberLeft(MembershipEvent event) {
        firedSystemDuplicate[LEFT] = firedSystem[LEFT];
        firedSystem[LEFT] = true;
        memberSystem[LEFT] = event.getDistributedMember();
        memberIdSystem[LEFT] = event.getMemberId();
        notifyAll();
      }

      @Override
      public synchronized void memberCrashed(MembershipEvent event) {
        firedSystemDuplicate[CRASHED] = firedSystem[CRASHED];
        firedSystem[CRASHED] = true;
        memberSystem[CRASHED] = event.getDistributedMember();
        memberIdSystem[CRASHED] = event.getMemberId();
        notifyAll();
      }
    };

    UniversalMembershipListenerAdapter adapter = new UniversalMembershipListenerAdapter() {
      @Override
      public synchronized void memberJoined(MembershipEvent event) {
        firedAdapterDuplicate[JOINED] = firedAdapter[JOINED];
        firedAdapter[JOINED] = true;
        memberAdapter[JOINED] = event.getDistributedMember();
        memberIdAdapter[JOINED] = event.getMemberId();
        if (event instanceof AdaptedMembershipEvent) {
          isClientAdapter[JOINED] = ((AdaptedMembershipEvent) event).isClient();
        }
        notifyAll();
      }

      @Override
      public synchronized void memberLeft(MembershipEvent event) {
        firedAdapterDuplicate[LEFT] = firedAdapter[LEFT];
        firedAdapter[LEFT] = true;
        memberAdapter[LEFT] = event.getDistributedMember();
        memberIdAdapter[LEFT] = event.getMemberId();
        if (event instanceof AdaptedMembershipEvent) {
          isClientAdapter[LEFT] = ((AdaptedMembershipEvent) event).isClient();
        }
        notifyAll();
      }

      @Override
      public synchronized void memberCrashed(MembershipEvent event) {
        firedAdapterDuplicate[CRASHED] = firedAdapter[CRASHED];
        firedAdapter[CRASHED] = true;
        memberAdapter[CRASHED] = event.getDistributedMember();
        memberIdAdapter[CRASHED] = event.getMemberId();
        if (event instanceof AdaptedMembershipEvent) {
          isClientAdapter[CRASHED] = ((AdaptedMembershipEvent) event).isClient();
        }
        notifyAll();
      }
    };

    ClientMembershipListener bridgeListener = new ClientMembershipListener() {
      @Override
      public synchronized void memberJoined(ClientMembershipEvent event) {
        firedBridgeDuplicate[JOINED] = firedBridge[JOINED];
        firedBridge[JOINED] = true;
        memberBridge[JOINED] = event.getMember();
        memberIdBridge[JOINED] = event.getMemberId();
        isClientBridge[JOINED] = event.isClient();
        notifyAll();
      }

      @Override
      public synchronized void memberLeft(ClientMembershipEvent event) {
        firedBridgeDuplicate[LEFT] = firedBridge[LEFT];
        firedBridge[LEFT] = true;
        memberBridge[LEFT] = event.getMember();
        memberIdBridge[LEFT] = event.getMemberId();
        isClientBridge[LEFT] = event.isClient();
        notifyAll();
      }

      @Override
      public synchronized void memberCrashed(ClientMembershipEvent event) {
        firedBridgeDuplicate[CRASHED] = firedBridge[CRASHED];
        firedBridge[CRASHED] = true;
        memberBridge[CRASHED] = event.getMember();
        memberIdBridge[CRASHED] = event.getMemberId();
        isClientBridge[CRASHED] = event.isClient();
        notifyAll();
      }
    };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    String name = this.getUniqueName();
    int[] ports = new int[1];

    // create CacheServer in controller vm...
    getSystem();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region region = createRegion(name, factory.create());
    assertThat(region).isNotNull();
    assertThat(getRootRegion().getSubregion(name)).isNotNull();

    ports[0] = startBridgeServer(0);
    assertThat(ports[0] != 0).isTrue();
    DistributedMember serverMember = getMemberId();
    String serverMemberId = serverMember.getId();
    Properties serverProperties = getSystem().getProperties();

    // Below removed properties are already got copied as cluster SSL properties
    serverProperties.remove(CLUSTER_SSL_ENABLED);
    serverProperties.remove(CLUSTER_SSL_CIPHERS);
    serverProperties.remove(CLUSTER_SSL_PROTOCOLS);
    serverProperties.remove(CLUSTER_SSL_REQUIRE_AUTHENTICATION);

    // register the bridge listener
    ClientMembership.registerClientMembershipListener(bridgeListener);

    InternalCache cache = getInternalCache();
    ManagementService service = ManagementService.getExistingManagementService(cache);
    // register the system listener
    service.addMembershipListener(systemListener);

    // register the universal adapter.
    adapter.registerMembershipListener(service);

    SerializableCallable createBridgeClient = new SerializableCallable("Create bridge client") {
      @Override
      public Object call() {
        System.setProperty(RESTRICT_MEMBERSHIP_PORT_RANGE, "false");
        assertThat(getSystem(serverProperties).isConnected()).isTrue();
        assertThat(getCache().isClosed()).isFalse();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, getServerHostName(host), ports, false,
            -1, -1, null);
        createRegion(name, factory.create());
        assertThat(getRootRegion().getSubregion(name)).isNotNull();
        return getMemberId();
      }
    };

    // create bridge client in vm0...
    DistributedMember clientMember = (DistributedMember) vm0.invoke(createBridgeClient);
    String clientMemberId = clientMember.getId();

    // should trigger both adapter and bridge listener but not system listener
    synchronized (adapter) {
      while (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized (bridgeListener) {
      while (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized (systemListener) {
      while (!firedSystem[JOINED]) {
        systemListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }

    assertArrayFalse(firedSystemDuplicate);
    assertArrayFalse(firedAdapterDuplicate);
    assertArrayFalse(firedBridgeDuplicate);

    assertThat(firedBridge[JOINED]).isTrue();
    assertThat(memberBridge[JOINED]).isEqualTo(clientMember);
    assertThat(memberIdBridge[JOINED]).isEqualTo(clientMemberId);
    assertThat(isClientBridge[JOINED]).isTrue();
    assertThat(firedBridge[LEFT]).isFalse();
    assertThat(memberBridge[LEFT]).isNull();
    assertThat(memberIdBridge[LEFT]).isNull();
    assertThat(isClientBridge[LEFT]).isFalse();
    assertThat(firedBridge[CRASHED]).isFalse();
    assertThat(memberBridge[CRASHED]).isNull();
    assertThat(memberIdBridge[CRASHED]).isNull();
    assertThat(isClientBridge[CRASHED]).isFalse();
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);

    assertThat(firedSystem[JOINED]).isTrue();
    assertThat(memberSystem[JOINED]).isEqualTo(clientMember);
    assertThat(memberIdSystem[JOINED]).isEqualTo(clientMemberId);
    assertThat(isClientSystem[JOINED]).isFalse();
    assertThat(firedSystem[LEFT]).isFalse();
    assertThat(memberSystem[LEFT]).isNull();
    assertThat(memberIdSystem[LEFT]).isNull();
    assertThat(isClientSystem[LEFT]).isFalse();
    assertThat(firedSystem[CRASHED]).isFalse();
    assertThat(memberSystem[CRASHED]).isNull();
    assertThat(memberIdSystem[CRASHED]).isNull();
    assertThat(isClientSystem[CRASHED]).isFalse();
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertThat(firedAdapter[JOINED]).isTrue();
    assertThat(memberAdapter[JOINED]).isEqualTo(clientMember);
    assertThat(memberIdAdapter[JOINED]).isEqualTo(clientMemberId);
    // assertThat(isClientAdapter[JOINED]).isTrue();
    assertThat(firedAdapter[LEFT]).isFalse();
    assertThat(memberAdapter[LEFT]).isNull();
    assertThat(memberIdAdapter[LEFT]).isNull();
    assertThat(isClientAdapter[LEFT]).isFalse();
    assertThat(firedAdapter[CRASHED]).isFalse();
    assertThat(memberAdapter[CRASHED]).isNull();
    assertThat(memberIdAdapter[CRASHED]).isNull();
    assertThat(isClientAdapter[CRASHED]).isFalse();
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    vm0.invoke(new SerializableRunnable("Wait for client to fully connect") {
      @Override
      public void run() {
        String pl = getRootRegion().getSubregion(name).getAttributes().getPoolName();
        PoolImpl pi = (PoolImpl) PoolManager.find(pl);
        waitForClientToFullyConnect(pi);
      }
    });

    // close bridge client region
    vm0.invoke(new SerializableRunnable("Close bridge client region") {
      @Override
      public void run() {
        getRootRegion().getSubregion(name).close();
        PoolManager.close();
      }
    });

    synchronized (adapter) {
      while (!firedAdapter[LEFT]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized (bridgeListener) {
      while (!firedBridge[LEFT]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }

    assertArrayFalse(firedSystemDuplicate);
    assertArrayFalse(firedAdapterDuplicate);
    assertArrayFalse(firedBridgeDuplicate);

    assertThat(firedBridge[JOINED]).isFalse();
    assertThat(memberIdBridge[JOINED]).isNull();
    assertThat(memberBridge[JOINED]).isNull();
    assertThat(isClientBridge[JOINED]).isFalse();
    assertThat(firedBridge[LEFT]).isTrue();
    assertThat(memberBridge[LEFT]).isEqualTo(clientMember);
    assertThat(memberIdBridge[LEFT]).isEqualTo(clientMemberId);
    assertThat(isClientBridge[LEFT]).isTrue();
    assertThat(firedBridge[CRASHED]).isFalse();
    assertThat(memberBridge[CRASHED]).isNull();
    assertThat(memberIdBridge[CRASHED]).isNull();
    assertThat(isClientBridge[CRASHED]).isFalse();
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);

    assertThat(firedSystem[JOINED]).isFalse();
    assertThat(memberSystem[JOINED]).isNull();
    assertThat(memberIdSystem[JOINED]).isNull();
    assertThat(isClientSystem[JOINED]).isFalse();
    assertThat(firedSystem[LEFT]).isFalse();
    assertThat(memberSystem[LEFT]).isNull();
    assertThat(memberIdSystem[LEFT]).isNull();
    assertThat(isClientSystem[LEFT]).isFalse();
    assertThat(firedSystem[CRASHED]).isFalse();
    assertThat(memberSystem[CRASHED]).isNull();
    assertThat(memberIdSystem[CRASHED]).isNull();
    assertThat(isClientSystem[CRASHED]).isFalse();
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertThat(firedAdapter[JOINED]).isFalse();
    assertThat(memberAdapter[JOINED]).isNull();
    assertThat(memberIdAdapter[JOINED]).isNull();
    assertThat(isClientAdapter[JOINED]).isFalse();
    assertThat(firedAdapter[LEFT]).isTrue();
    assertThat(memberAdapter[LEFT]).isEqualTo(clientMember);
    assertThat(memberIdAdapter[LEFT]).isEqualTo(clientMemberId);
    assertThat(isClientAdapter[LEFT]).isTrue();
    assertThat(firedAdapter[CRASHED]).isFalse();
    assertThat(memberAdapter[CRASHED]).isNull();
    assertThat(memberIdAdapter[CRASHED]).isNull();
    assertThat(isClientAdapter[CRASHED]).isFalse();
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    // reconnect bridge client
    clientMember = (DistributedMember) vm0.invoke(createBridgeClient);
    clientMemberId = clientMember.getId();

    synchronized (adapter) {
      while (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized (bridgeListener) {
      while (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }

    assertArrayFalse(firedSystemDuplicate);
    assertArrayFalse(firedAdapterDuplicate);
    assertArrayFalse(firedBridgeDuplicate);

    assertThat(firedBridge[JOINED]).isTrue();
    assertThat(memberBridge[JOINED]).isEqualTo(clientMember);
    assertThat(memberIdBridge[JOINED]).isEqualTo(clientMemberId);
    assertThat(isClientBridge[JOINED]).isTrue();
    assertThat(firedBridge[LEFT]).isFalse();
    assertThat(memberBridge[LEFT]).isNull();
    assertThat(memberIdBridge[LEFT]).isNull();
    assertThat(isClientBridge[LEFT]).isFalse();
    assertThat(firedBridge[CRASHED]).isFalse();
    assertThat(memberBridge[CRASHED]).isNull();
    assertThat(memberIdBridge[CRASHED]).isNull();
    assertThat(isClientBridge[CRASHED]).isFalse();
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);

    assertThat(firedSystem[JOINED]).isFalse();
    assertThat(memberSystem[JOINED]).isNull();
    assertThat(memberIdSystem[JOINED]).isNull();
    assertThat(isClientSystem[JOINED]).isFalse();
    assertThat(firedSystem[LEFT]).isFalse();
    assertThat(memberSystem[LEFT]).isNull();
    assertThat(memberIdSystem[LEFT]).isNull();
    assertThat(isClientSystem[LEFT]).isFalse();
    assertThat(firedSystem[CRASHED]).isFalse();
    assertThat(memberSystem[CRASHED]).isNull();
    assertThat(memberIdSystem[CRASHED]).isNull();
    assertThat(isClientSystem[CRASHED]).isFalse();
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertThat(firedAdapter[JOINED]).isTrue();
    assertThat(memberAdapter[JOINED]).isEqualTo(clientMember);
    assertThat(memberIdAdapter[JOINED]).isEqualTo(clientMemberId);
    // assertThat(isClientAdapter[JOINED]).isTrue();
    assertThat(firedAdapter[LEFT]).isFalse();
    assertThat(memberAdapter[LEFT]).isNull();
    assertThat(memberIdAdapter[LEFT]).isNull();
    assertThat(isClientAdapter[LEFT]).isFalse();
    assertThat(firedAdapter[CRASHED]).isFalse();
    assertThat(memberAdapter[CRASHED]).isNull();
    assertThat(memberIdAdapter[CRASHED]).isNull();
    assertThat(isClientAdapter[CRASHED]).isFalse();
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    vm0.invoke(new SerializableRunnable("Wait for client to fully connect") {
      @Override
      public void run() {
        String poolName = getRootRegion().getSubregion(name).getAttributes().getPoolName();
        PoolImpl pool = (PoolImpl) PoolManager.find(poolName);
        waitForClientToFullyConnect(pool);
      }
    });

    // have bridge client disconnect from system
    vm0.invoke(new SerializableRunnable("Disconnect bridge client") {
      @Override
      public void run() {
        closeCache();
        disconnectFromDS();
      }
    });

    synchronized (adapter) {
      while (!firedAdapter[LEFT]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized (systemListener) {
      while (!firedSystem[LEFT]) {
        systemListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized (bridgeListener) {
      while (!firedBridge[LEFT]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }

    assertArrayFalse(firedSystemDuplicate);
    assertArrayFalse(firedAdapterDuplicate);
    assertArrayFalse(firedBridgeDuplicate);

    assertThat(firedBridge[JOINED]).isFalse();
    assertThat(memberBridge[JOINED]).isNull();
    assertThat(memberIdBridge[JOINED]).isNull();
    assertThat(isClientBridge[JOINED]).isFalse();
    assertThat(firedBridge[LEFT]).isTrue();
    assertThat(memberBridge[LEFT]).isEqualTo(clientMember);
    assertThat(memberIdBridge[LEFT]).isEqualTo(clientMemberId);
    assertThat(isClientBridge[LEFT]).isTrue();
    assertThat(firedBridge[CRASHED]).isFalse();
    assertThat(memberBridge[CRASHED]).isNull();
    assertThat(memberIdBridge[CRASHED]).isNull();
    assertThat(isClientBridge[CRASHED]).isFalse();
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);

    assertThat(firedSystem[JOINED]).isFalse();
    assertThat(memberSystem[JOINED]).isNull();
    assertThat(memberIdSystem[JOINED]).isNull();
    assertThat(isClientSystem[JOINED]).isFalse();
    assertThat(firedSystem[LEFT]).isTrue();
    assertThat(memberSystem[LEFT]).isEqualTo(clientMember);
    assertThat(memberIdSystem[LEFT]).isEqualTo(clientMemberId);
    assertThat(isClientSystem[LEFT]).isFalse();
    assertThat(firedSystem[CRASHED]).isFalse();
    assertThat(memberSystem[CRASHED]).isNull();
    assertThat(memberIdSystem[CRASHED]).isNull();
    assertThat(isClientSystem[CRASHED]).isFalse();
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertThat(firedAdapter[JOINED]).isFalse();
    assertThat(memberAdapter[JOINED]).isNull();
    assertThat(memberIdAdapter[JOINED]).isNull();
    assertThat(isClientAdapter[JOINED]).isFalse();
    assertThat(firedAdapter[LEFT]).isTrue();
    assertThat(memberAdapter[LEFT]).isEqualTo(clientMember);
    assertThat(memberIdAdapter[LEFT]).isEqualTo(clientMemberId);
    // assertThat(isClientAdapter[LEFT]).isTrue();
    assertThat(firedAdapter[CRASHED]).isFalse();
    assertThat(memberAdapter[CRASHED]).isNull();
    assertThat(memberIdAdapter[CRASHED]).isNull();
    assertThat(isClientAdapter[CRASHED]).isFalse();
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    // reconnect bridge client
    clientMember = (DistributedMember) vm0.invoke(createBridgeClient);
    clientMemberId = clientMember.getId();

    synchronized (adapter) {
      while (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized (systemListener) {
      while (!firedSystem[JOINED]) {
        systemListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized (bridgeListener) {
      while (!firedBridge[JOINED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }

    assertArrayFalse(firedSystemDuplicate);
    assertArrayFalse(firedAdapterDuplicate);
    assertArrayFalse(firedBridgeDuplicate);

    assertThat(firedBridge[JOINED]).isTrue();
    assertThat(memberBridge[JOINED]).isEqualTo(clientMember);
    assertThat(memberIdBridge[JOINED]).isEqualTo(clientMemberId);
    assertThat(isClientBridge[JOINED]).isTrue();
    assertThat(firedBridge[LEFT]).isFalse();
    assertThat(memberBridge[LEFT]).isNull();
    assertThat(memberIdBridge[LEFT]).isNull();
    assertThat(isClientBridge[LEFT]).isFalse();
    assertThat(firedBridge[CRASHED]).isFalse();
    assertThat(memberBridge[CRASHED]).isNull();
    assertThat(memberIdBridge[CRASHED]).isNull();
    assertThat(isClientBridge[CRASHED]).isFalse();
    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);

    assertThat(firedSystem[JOINED]).isTrue();
    assertThat(memberSystem[JOINED]).isEqualTo(clientMember);
    assertThat(memberIdSystem[JOINED]).isEqualTo(clientMemberId);
    assertThat(isClientSystem[JOINED]).isFalse();
    assertThat(firedSystem[LEFT]).isFalse();
    assertThat(memberSystem[LEFT]).isNull();
    assertThat(memberIdSystem[LEFT]).isNull();
    assertThat(isClientSystem[LEFT]).isFalse();
    assertThat(firedSystem[CRASHED]).isFalse();
    assertThat(memberSystem[CRASHED]).isNull();
    assertThat(memberIdSystem[CRASHED]).isNull();
    assertThat(isClientSystem[CRASHED]).isFalse();
    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertThat(firedAdapter[JOINED]).isTrue();
    assertThat(memberAdapter[JOINED]).isEqualTo(clientMember);
    assertThat(memberIdAdapter[JOINED]).isEqualTo(clientMemberId);
    // assertThat(isClientAdapter[JOINED]).isTrue();
    assertThat(firedAdapter[LEFT]).isFalse();
    assertThat(memberAdapter[LEFT]).isNull();
    assertThat(memberIdAdapter[LEFT]).isNull();
    assertThat(isClientAdapter[LEFT]).isFalse();
    assertThat(firedAdapter[CRASHED]).isFalse();
    assertThat(memberAdapter[CRASHED]).isNull();
    assertThat(memberIdAdapter[CRASHED]).isNull();
    assertThat(isClientAdapter[CRASHED]).isFalse();
    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    vm0.invoke(new SerializableRunnable("Wait for client to fully connect") {
      @Override
      public void run() {
        String poolName = getRootRegion().getSubregion(name).getAttributes().getPoolName();
        PoolImpl pool = (PoolImpl) PoolManager.find(poolName);
        waitForClientToFullyConnect(pool);
      }
    });

    // close bridge client region with test hook for crash
    ServerConnection.setForceClientCrashEvent(true);

    vm0.invoke(new SerializableRunnable("Close bridge client region") {
      @Override
      public void run() {
        getRootRegion().getSubregion(name).close();
        PoolManager.close();
      }
    });

    synchronized (adapter) {
      while (!firedAdapter[CRASHED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized (bridgeListener) {
      while (!firedBridge[CRASHED]) {
        bridgeListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }

    assertArrayFalse(firedSystemDuplicate);
    assertArrayFalse(firedAdapterDuplicate);
    assertArrayFalse(firedBridgeDuplicate);

    assertThat(firedBridge[JOINED]).isFalse();
    assertThat(memberBridge[JOINED]).isNull();
    assertThat(memberIdBridge[JOINED]).isNull();
    assertThat(isClientBridge[JOINED]).isFalse();
    assertThat(firedBridge[LEFT]).isFalse();
    assertThat(memberBridge[LEFT]).isNull();
    assertThat(memberIdBridge[LEFT]).isNull();
    assertThat(isClientBridge[LEFT]).isFalse();
    assertThat(firedBridge[CRASHED]).isTrue();
    assertThat(memberBridge[CRASHED]).isEqualTo(clientMember);
    assertThat(memberIdBridge[CRASHED]).isEqualTo(clientMemberId);
    assertThat(isClientBridge[CRASHED]).isTrue();

    assertThat(firedSystem[JOINED]).isFalse();
    assertThat(memberSystem[JOINED]).isNull();
    assertThat(memberIdSystem[JOINED]).isNull();
    assertThat(isClientSystem[JOINED]).isFalse();
    assertThat(firedSystem[LEFT]).isFalse();
    assertThat(memberSystem[LEFT]).isNull();
    assertThat(memberIdSystem[LEFT]).isNull();
    assertThat(isClientSystem[LEFT]).isFalse();
    assertThat(firedSystem[CRASHED]).isFalse();
    assertThat(memberSystem[CRASHED]).isNull();
    assertThat(memberIdSystem[CRASHED]).isNull();
    assertThat(isClientSystem[CRASHED]).isFalse();

    assertThat(firedAdapter[JOINED]).isFalse();
    assertThat(memberAdapter[JOINED]).isNull();
    assertThat(memberIdAdapter[JOINED]).isNull();
    assertThat(isClientAdapter[JOINED]).isFalse();
    assertThat(firedAdapter[LEFT]).isFalse();
    assertThat(memberAdapter[LEFT]).isNull();
    assertThat(memberIdAdapter[LEFT]).isNull();
    assertThat(isClientAdapter[LEFT]).isFalse();
    assertThat(firedAdapter[CRASHED]).isTrue();
    assertThat(memberAdapter[CRASHED]).isEqualTo(clientMember);
    assertThat(memberIdAdapter[CRASHED]).isEqualTo(clientMemberId);
    assertThat(isClientAdapter[CRASHED]).isTrue();
  }

  /**
   * Waits for client to create an expected number of connections. Note: This probably won't work if
   * the pool has more than one Endpoint.
   */
  private void waitForClientToFullyConnect(PoolImpl pool) {
    long failMillis = System.currentTimeMillis() + JOIN_FAIL_MILLIS;
    boolean fullyConnected = false;
    while (!fullyConnected) {
      pause(100);
      fullyConnected = pool.getConnectionCount() >= pool.getMinConnections();
      assertThat(System.currentTimeMillis()).isLessThan(failMillis);
    }
  }

  /**
   * Resets all elements of arrays used for listener testing. Boolean values are reset to false.
   * String values are reset to null.
   */
  private void resetArraysForTesting(boolean[] fired, DistributedMember[] member, String[] memberId,
      boolean[] isClient) {
    for (int i = 0; i < fired.length; i++) {
      fired[i] = false;
      member[i] = null;
      memberId[i] = null;
      isClient[i] = false;
    }
  }

  /**
   * Asserts all elements in the array are false.
   */
  private void assertArrayFalse(boolean[] array) {
    assertArrayFalse(null, array);
  }

  private void assertArrayFalse(String msg, boolean[] array) {
    for (boolean value : array) {
      if (msg == null) {
        assertThat(value).isFalse();
      } else {
        assertThat(value).as(msg).isFalse();
      }
    }
  }

  private void assertArrayTrue(String msg, boolean[] array) {
    for (int i = 0; i < array.length; i++) {
      if (msg == null) {
        assertThat(array[i]).isTrue();
      } else {
        assertThat(array[i]).as(msg).isTrue();
      }
    }
  }

  /**
   * Tests notification of events for cache server in system bridge client process.
   */
  @Test
  public void testServerEventsInPeerSystem() throws Exception {
    boolean[] firedSystem = new boolean[3];
    DistributedMember[] memberSystem = new DistributedMember[3];
    String[] memberIdSystem = new String[3];
    boolean[] isClientSystem = new boolean[3];

    boolean[] firedAdapter = new boolean[3];
    DistributedMember[] memberAdapter = new DistributedMember[3];
    String[] memberIdAdapter = new String[3];
    boolean[] isClientAdapter = new boolean[3];

    boolean[] firedBridge = new boolean[3];
    DistributedMember[] memberBridge = new DistributedMember[3];
    String[] memberIdBridge = new String[3];
    boolean[] isClientBridge = new boolean[3];

    boolean[] firedSystemDuplicate = new boolean[3];
    boolean[] firedAdapterDuplicate = new boolean[3];
    boolean[] firedBridgeDuplicate = new boolean[3];

    MembershipListener systemListener = new MembershipListener() {
      @Override
      public synchronized void memberJoined(MembershipEvent event) {
        firedSystemDuplicate[JOINED] = firedSystem[JOINED];
        firedSystem[JOINED] = true;
        memberSystem[JOINED] = event.getDistributedMember();
        memberIdSystem[JOINED] = event.getMemberId();
        notifyAll();
      }

      @Override
      public synchronized void memberLeft(MembershipEvent event) {
        firedSystemDuplicate[LEFT] = firedSystem[LEFT];
        firedSystem[LEFT] = true;
        memberSystem[LEFT] = event.getDistributedMember();
        memberIdSystem[LEFT] = event.getMemberId();
        notifyAll();
      }

      @Override
      public synchronized void memberCrashed(MembershipEvent event) {
        firedSystemDuplicate[CRASHED] = firedSystem[CRASHED];
        firedSystem[CRASHED] = true;
        memberSystem[CRASHED] = event.getDistributedMember();
        memberIdSystem[CRASHED] = event.getMemberId();
        notifyAll();
      }
    };

    UniversalMembershipListenerAdapter adapter = new UniversalMembershipListenerAdapter() {
      @Override
      public synchronized void memberJoined(MembershipEvent event) {
        firedAdapterDuplicate[JOINED] = firedAdapter[JOINED];
        firedAdapter[JOINED] = true;
        memberAdapter[JOINED] = event.getDistributedMember();
        memberIdAdapter[JOINED] = event.getMemberId();
        if (event instanceof AdaptedMembershipEvent) {
          isClientAdapter[JOINED] = ((AdaptedMembershipEvent) event).isClient();
        }
        notifyAll();
      }

      @Override
      public synchronized void memberLeft(MembershipEvent event) {
        firedAdapterDuplicate[LEFT] = firedAdapter[LEFT];
        firedAdapter[LEFT] = true;
        memberAdapter[LEFT] = event.getDistributedMember();
        memberIdAdapter[LEFT] = event.getMemberId();
        if (event instanceof AdaptedMembershipEvent) {
          isClientAdapter[LEFT] = ((AdaptedMembershipEvent) event).isClient();
        }
        notifyAll();
      }

      @Override
      public synchronized void memberCrashed(MembershipEvent event) {
        firedAdapterDuplicate[CRASHED] = firedAdapter[CRASHED];
        firedAdapter[CRASHED] = true;
        memberAdapter[CRASHED] = event.getDistributedMember();
        memberIdAdapter[CRASHED] = event.getMemberId();
        if (event instanceof AdaptedMembershipEvent) {
          isClientAdapter[CRASHED] = ((AdaptedMembershipEvent) event).isClient();
        }
        notifyAll();
      }
    };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    String name = this.getUniqueName();
    int[] ports = new int[] {getRandomAvailablePort(SOCKET)};
    assertThat(ports[0] != 0).isTrue();

    // create BridgeServer in controller vm...
    getSystem();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region region = createRegion(name, factory.create());
    assertThat(region).isNotNull();
    assertThat(getRootRegion().getSubregion(name)).isNotNull();

    ports[0] = startBridgeServer(0);
    assertThat(ports[0] != 0).isTrue();

    DistributedMember serverMember = getMemberId();
    String serverMemberId = serverMember.getId();
    Properties serverProperties = getSystem().getProperties();

    serverProperties.remove(CLUSTER_SSL_ENABLED);
    serverProperties.remove(CLUSTER_SSL_CIPHERS);
    serverProperties.remove(CLUSTER_SSL_PROTOCOLS);
    serverProperties.remove(CLUSTER_SSL_REQUIRE_AUTHENTICATION);

    InternalCache cache = getInternalCache();
    ManagementService service = ManagementService.getExistingManagementService(cache);
    // register the system listener
    service.addMembershipListener(systemListener);

    // register the universal adapter.
    adapter.registerMembershipListener(service);

    // create BridgeServer in vm0...
    SerializableCallable createPeerCache = new SerializableCallable("Create Peer Cache") {
      @Override
      public Object call() {
        getSystem(serverProperties);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Region region = createRegion(name, factory.create());
        assertThat(region).isNotNull();
        assertThat(getRootRegion().getSubregion(name)).isNotNull();
        return basicGetSystem().getDistributedMember();
      }
    };

    DistributedMember peerMember = (DistributedMember) vm0.invoke(createPeerCache);
    String peerMemberId = peerMember.getId();

    synchronized (systemListener) {
      if (!firedSystem[JOINED]) {
        systemListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized (adapter) {
      if (!firedAdapter[JOINED]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }

    assertArrayFalse(firedSystemDuplicate);

    assertThat(firedSystem[JOINED]).isTrue();
    assertThat(memberSystem[JOINED]).isEqualTo(peerMember);
    assertThat(memberIdSystem[JOINED]).isEqualTo(peerMemberId);
    assertThat(isClientSystem[JOINED]).isFalse();
    assertThat(firedSystem[LEFT]).isFalse();
    assertThat(memberSystem[LEFT]).isNull();
    assertThat(memberIdSystem[LEFT]).isNull();
    assertThat(isClientSystem[LEFT]).isFalse();
    assertThat(firedSystem[CRASHED]).isFalse();
    assertThat(memberSystem[CRASHED]).isNull();
    assertThat(memberIdSystem[CRASHED]).isNull();
    assertThat(isClientSystem[CRASHED]).isFalse();

    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertThat(firedAdapter[JOINED]).isTrue();
    assertThat(memberAdapter[JOINED]).isNotNull();
    assertThat(memberIdAdapter[JOINED]).isNotNull();
    assertThat(memberAdapter[JOINED]).isEqualTo(peerMember);
    assertThat(memberIdAdapter[JOINED]).isEqualTo(peerMemberId);
    assertThat(isClientAdapter[JOINED]).isFalse();
    assertThat(firedAdapter[LEFT]).isFalse();
    assertThat(memberAdapter[LEFT]).isNull();
    assertThat(memberIdAdapter[LEFT]).isNull();
    assertThat(isClientAdapter[LEFT]).isFalse();
    assertThat(firedAdapter[CRASHED]).isFalse();
    assertThat(memberAdapter[CRASHED]).isNull();
    assertThat(memberIdAdapter[CRASHED]).isNull();
    assertThat(isClientAdapter[CRASHED]).isFalse();

    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    addIgnoredException(ServerConnectivityException.class.getName());
    addIgnoredException(IOException.class.getName());

    vm0.invoke(new SerializableRunnable("Disconnect Peer server") {
      @Override
      public void run() {
        disconnectFromDS();
      }
    });

    synchronized (systemListener) {
      if (!firedSystem[LEFT]) {
        systemListener.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }
    synchronized (adapter) {
      if (!firedAdapter[LEFT]) {
        adapter.wait(ASYNC_EVENT_WAIT_MILLIS);
      }
    }

    // done with IgnoredExceptions

    assertArrayFalse(firedSystemDuplicate);
    assertArrayFalse(firedAdapterDuplicate);

    assertThat(firedSystem[JOINED]).isFalse();
    assertThat(memberSystem[JOINED]).isNull();
    assertThat(memberIdSystem[JOINED]).isNull();
    assertThat(isClientSystem[JOINED]).isFalse();
    assertThat(firedSystem[LEFT]).isTrue();
    assertThat(memberSystem[LEFT]).isEqualTo(peerMember);
    assertThat(memberIdSystem[LEFT]).isEqualTo(peerMemberId);
    assertThat(isClientSystem[LEFT]).isFalse();
    assertThat(firedSystem[CRASHED]).isFalse();
    assertThat(memberSystem[CRASHED]).isNull();
    assertThat(memberIdSystem[CRASHED]).isNull();
    assertThat(isClientSystem[CRASHED]).isFalse();

    resetArraysForTesting(firedSystem, memberSystem, memberIdSystem, isClientSystem);

    assertThat(firedAdapter[JOINED]).isFalse();
    assertThat(memberIdAdapter[JOINED]).isNull();
    assertThat(isClientAdapter[JOINED]).isFalse();
    // LEFT fired by System listener
    assertThat(firedAdapter[LEFT]).isTrue();
    assertThat(memberAdapter[LEFT]).isEqualTo(peerMember);
    assertThat(memberIdAdapter[LEFT]).isEqualTo(peerMemberId);
    assertThat(isClientAdapter[LEFT]).isFalse();

    // There won't be an adapter crashed event because since the two VMs
    // are in the same distributed system, and the server's real member
    // id is used now. In this case, two events are sent - one from
    // jgroups (memberDeparted), and one from the server (a memberCrashed).
    // The memberCrashed event is deemed a duplicate and not sent - see
    // UniversalMembershipListenerAdapter.MembershipListener.isDuplicate

    assertThat(firedAdapter[CRASHED]).isFalse();
    assertThat(memberAdapter[CRASHED]).isNull();
    assertThat(memberIdAdapter[CRASHED]).isNull();
    assertThat(isClientAdapter[CRASHED]).isFalse();

    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
  }

  /**
   * Tests notification of events for cache server in system bridge client process.
   */
  @Test
  public void testServerEventsInLonerClient() throws Exception {
    boolean[] firedAdapter = new boolean[3];
    DistributedMember[] memberAdapter = new DistributedMember[3];
    String[] memberIdAdapter = new String[3];
    boolean[] isClientAdapter = new boolean[3];

    boolean[] firedBridge = new boolean[3];
    DistributedMember[] memberBridge = new DistributedMember[3];
    String[] memberIdBridge = new String[3];
    boolean[] isClientBridge = new boolean[3];

    boolean[] firedAdapterDuplicate = new boolean[3];
    boolean[] firedBridgeDuplicate = new boolean[3];

    UniversalMembershipListenerAdapter adapter = new UniversalMembershipListenerAdapter() {
      @Override
      public synchronized void memberJoined(MembershipEvent event) {
        firedAdapterDuplicate[JOINED] = firedAdapter[JOINED];
        firedAdapter[JOINED] = true;
        memberAdapter[JOINED] = event.getDistributedMember();
        memberIdAdapter[JOINED] = event.getMemberId();
        if (event instanceof AdaptedMembershipEvent) {
          isClientAdapter[JOINED] = ((AdaptedMembershipEvent) event).isClient();
        }
        notifyAll();
      }

      @Override
      public synchronized void memberLeft(MembershipEvent event) {
        firedAdapterDuplicate[LEFT] = firedAdapter[LEFT];
        firedAdapter[LEFT] = true;
        memberAdapter[LEFT] = event.getDistributedMember();
        memberIdAdapter[LEFT] = event.getMemberId();
        if (event instanceof AdaptedMembershipEvent) {
          isClientAdapter[LEFT] = ((AdaptedMembershipEvent) event).isClient();
        }
        notifyAll();
      }

      @Override
      public synchronized void memberCrashed(MembershipEvent event) {
        firedAdapterDuplicate[CRASHED] = firedAdapter[CRASHED];
        firedAdapter[CRASHED] = true;
        memberAdapter[CRASHED] = event.getDistributedMember();
        memberIdAdapter[CRASHED] = event.getMemberId();
        if (event instanceof AdaptedMembershipEvent) {
          isClientAdapter[CRASHED] = ((AdaptedMembershipEvent) event).isClient();
        }
        notifyAll();
      }
    };

    ClientMembershipListener bridgeListener = new ClientMembershipListener() {
      @Override
      public synchronized void memberJoined(ClientMembershipEvent event) {
        firedBridgeDuplicate[JOINED] = firedBridge[JOINED];
        firedBridge[JOINED] = true;
        memberBridge[JOINED] = event.getMember();
        memberIdBridge[JOINED] = event.getMemberId();
        isClientBridge[JOINED] = event.isClient();
        notifyAll();
      }

      @Override
      public synchronized void memberLeft(ClientMembershipEvent event) {
        firedBridgeDuplicate[LEFT] = firedBridge[LEFT];
        firedBridge[LEFT] = true;
        memberBridge[LEFT] = event.getMember();
        memberIdBridge[LEFT] = event.getMemberId();
        isClientBridge[LEFT] = event.isClient();
        notifyAll();
      }

      @Override
      public synchronized void memberCrashed(ClientMembershipEvent event) {
        firedBridgeDuplicate[CRASHED] = firedBridge[CRASHED];
        firedBridge[CRASHED] = true;
        memberBridge[CRASHED] = event.getMember();
        memberIdBridge[CRASHED] = event.getMemberId();
        isClientBridge[CRASHED] = event.isClient();
        notifyAll();
      }
    };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    String name = getUniqueName();
    int[] ports = new int[] {getRandomAvailablePort(SOCKET)};
    assertThat(ports[0] != 0).isTrue();

    Properties config = new Properties();
    config.put(MCAST_PORT, "0");
    config.put(LOCATORS, "");
    config.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    getSystem(config);

    // register the bridge listener
    ClientMembership.registerClientMembershipListener(bridgeListener);

    // create CacheServer in vm0...
    SerializableCallable createBridgeServer = new SerializableCallable("Create BridgeServer") {
      @Override
      public Object call() {
        getSystem();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Region region = createRegion(name, factory.create());
        assertThat(region).isNotNull();
        assertThat(getRootRegion().getSubregion(name)).isNotNull();

        try {
          serverPort = startBridgeServer(ports[0]);
        } catch (IOException e) {
          throw new AssertionError(e);
        }

        return basicGetSystem().getDistributedMember();
      }
    };

    vm0.invoke(createBridgeServer);

    // gather details for later creation of pool...
    assertThat((int) vm0.invoke("getServerPort", () -> serverPort)).isEqualTo(ports[0]);

    // create region which connects to cache server
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    configureConnectionPool(factory, getServerHostName(host), ports, false, -1, -1, null);
    createRegion(name, factory.create());
    assertThat(getRootRegion().getSubregion(name)).isNotNull();

    await("wait for join").until(() -> {
      synchronized (adapter) {
        return firedAdapter[JOINED];
      }
    });
    await("wait for join").until(() -> {
      synchronized (bridgeListener) {
        return firedBridge[JOINED];
      }
    });

    assertArrayFalse(firedAdapterDuplicate);
    assertArrayFalse(firedBridgeDuplicate);

    assertThat(firedBridge[JOINED]).isTrue();
    assertThat(memberBridge[JOINED]).isNotNull();
    assertThat(memberIdBridge[JOINED]).isNotNull();
    assertThat(isClientBridge[JOINED]).isFalse();
    assertThat(firedBridge[LEFT]).isFalse();
    assertThat(memberBridge[LEFT]).isNull();
    assertThat(memberIdBridge[LEFT]).isNull();
    assertThat(isClientBridge[LEFT]).isFalse();
    assertThat(firedBridge[CRASHED]).isFalse();
    assertThat(memberBridge[CRASHED]).isNull();
    assertThat(memberIdBridge[CRASHED]).isNull();
    assertThat(isClientBridge[CRASHED]).isFalse();

    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);

    assertThat(firedAdapter[JOINED]).isTrue();
    assertThat(memberIdAdapter[JOINED]).isNotNull();
    assertThat(isClientAdapter[JOINED]).isFalse();
    assertThat(firedAdapter[LEFT]).isFalse();
    assertThat(memberAdapter[LEFT]).isNull();
    assertThat(memberIdAdapter[LEFT]).isNull();
    assertThat(isClientAdapter[LEFT]).isFalse();
    assertThat(firedAdapter[CRASHED]).isFalse();
    assertThat(memberAdapter[CRASHED]).isNull();
    assertThat(memberIdAdapter[CRASHED]).isNull();
    assertThat(isClientAdapter[CRASHED]).isFalse();

    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    String poolName = getRootRegion().getSubregion(name).getAttributes().getPoolName();
    PoolImpl pool = (PoolImpl) PoolManager.find(poolName);
    waitForClientToFullyConnect(pool);

    addIgnoredException(IOException.class.getName());
    addIgnoredException(ConnectException.class.getName());

    vm0.invoke(new SerializableRunnable("Disconnect cache server") {
      @Override
      public void run() {
        closeCache();
      }
    });

    await("wait for server to leave").until(() -> {
      synchronized (adapter) {
        return firedAdapter[LEFT] || firedAdapter[CRASHED];
      }
    });
    await("wait for server to leave").until(() -> {
      synchronized (bridgeListener) {
        return firedBridge[LEFT] || firedBridge[CRASHED];
      }
    });

    // done with IgnoredExceptions

    assertArrayFalse(firedAdapterDuplicate);
    assertArrayFalse(firedBridgeDuplicate);

    assertThat(firedBridge[JOINED]).isFalse();
    assertThat(memberIdBridge[JOINED]).isNull();
    assertThat(memberBridge[JOINED]).isNull();
    assertThat(isClientBridge[JOINED]).isFalse();
    assertThat(firedBridge[LEFT]).isFalse();
    assertThat(memberBridge[LEFT]).isNull();
    assertThat(memberIdBridge[LEFT]).isNull();
    assertThat(isClientBridge[LEFT]).isFalse();
    assertThat(firedBridge[CRASHED]).isTrue();
    assertThat(memberBridge[CRASHED]).isNotNull();
    assertThat(memberIdBridge[CRASHED]).isNotNull();
    assertThat(isClientBridge[CRASHED]).isFalse();

    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);

    assertThat(firedAdapter[JOINED]).isFalse();
    assertThat(memberAdapter[JOINED]).isNull();
    assertThat(memberIdAdapter[JOINED]).isNull();
    assertThat(isClientAdapter[JOINED]).isFalse();
    assertThat(firedAdapter[LEFT]).isFalse();
    assertThat(memberAdapter[LEFT]).isNull();
    assertThat(memberIdAdapter[LEFT]).isNull();
    assertThat(isClientAdapter[LEFT]).isFalse();
    // CRASHED fired by Bridge listener
    assertThat(firedAdapter[CRASHED]).isTrue();
    assertThat(memberAdapter[CRASHED]).isNotNull();
    assertThat(memberIdAdapter[CRASHED]).isNotNull();
    assertThat(isClientAdapter[CRASHED]).isFalse();

    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);

    // reconnect bridge client to test for crashed event
    vm0.invoke(createBridgeServer);

    // gather details for later creation of pool...
    assertThat((int) vm0.invoke(() -> serverPort)).isEqualTo(ports[0]);

    await("wait for join").until(() -> {
      synchronized (adapter) {
        return firedAdapter[JOINED];
      }
    });
    await("wait for join").until(() -> {
      synchronized (bridgeListener) {
        return firedBridge[JOINED];
      }
    });

    assertArrayFalse(firedAdapterDuplicate);
    assertArrayFalse(firedBridgeDuplicate);

    assertThat(firedBridge[JOINED]).isTrue();
    assertThat(memberBridge[JOINED]).isNotNull();
    assertThat(memberIdBridge[JOINED]).isNotNull();
    assertThat(isClientBridge[JOINED]).isFalse();
    assertThat(firedBridge[LEFT]).isFalse();
    assertThat(memberBridge[LEFT]).isNull();
    assertThat(memberIdBridge[LEFT]).isNull();
    assertThat(isClientBridge[LEFT]).isFalse();
    assertThat(firedBridge[CRASHED]).isFalse();
    assertThat(memberBridge[CRASHED]).isNull();
    assertThat(memberIdBridge[CRASHED]).isNull();
    assertThat(isClientBridge[CRASHED]).isFalse();

    resetArraysForTesting(firedBridge, memberBridge, memberIdBridge, isClientBridge);

    assertThat(firedAdapter[JOINED]).isTrue();
    assertThat(memberAdapter[JOINED]).isNotNull();
    assertThat(memberIdAdapter[JOINED]).isNotNull();
    assertThat(isClientAdapter[JOINED]).isFalse();
    assertThat(firedAdapter[LEFT]).isFalse();
    assertThat(memberAdapter[LEFT]).isNull();
    assertThat(memberIdAdapter[LEFT]).isNull();
    assertThat(isClientAdapter[LEFT]).isFalse();
    assertThat(firedAdapter[CRASHED]).isFalse();
    assertThat(memberAdapter[CRASHED]).isNull();
    assertThat(memberIdAdapter[CRASHED]).isNull();
    assertThat(isClientAdapter[CRASHED]).isFalse();

    resetArraysForTesting(firedAdapter, memberAdapter, memberIdAdapter, isClientAdapter);
  }

  private static InternalCache getInternalCache() {
    InternalCache cache = (InternalCache) CacheFactory.getAnyInstance();
    assertThat(cache).isNotNull();
    return cache;
  }

  private static class MembershipNotification implements Serializable {

    private final AtomicBoolean notified = new AtomicBoolean();
    private final AtomicReference<DistributedMember> member = new AtomicReference<>();
    private final AtomicReference<String> memberId = new AtomicReference<>();
    private final AtomicBoolean client = new AtomicBoolean();

    void notify(final MembershipEvent event) {
      validateNotNotified();

      notified.set(true);
      member.set(event.getDistributedMember());
      memberId.set(event.getMemberId());
    }

    void notify(final AdaptedMembershipEvent event) {
      validateNotNotified();

      notified.set(true);
      member.set(event.getDistributedMember());
      memberId.set(event.getMemberId());
      client.set(event.isClient());
    }

    void notify(final ClientMembershipEvent event) {
      validateNotNotified();

      notified.set(true);
      member.set(event.getMember());
      memberId.set(event.getMemberId());
      client.set(event.isClient());
    }

    void reset() {
      notified.set(false);
      member.set(null);
      memberId.set(null);
      client.set(false);
    }

    void awaitNotification(final long timeout, final TimeUnit unit) {
      await().until(() -> notified.get());
    }

    void awaitWithoutNotification(final long timeout, final TimeUnit unit) {
      try {
        await().atMost(timeout, unit).until(() -> notified.get());
      } catch (ConditionTimeoutException expected) {
        // do nothing
      }
    }

    void validate(DistributedMember clientJoined) {
      assertThat(notified.get()).isTrue();
      assertThat(member.get()).isEqualTo(clientJoined);
      assertThat(memberId.get()).isEqualTo(clientJoined.getId());
    }

    void validateNotNotified() {
      assertThat(notified.get()).isFalse();
      assertThat(member.get()).isNull();
      assertThat(memberId.get()).isNull();
      assertThat(client.get()).isFalse();
    }
  }

  private static class FakeDistributedMember implements DistributedMember {

    private String host;

    FakeDistributedMember(String host) {
      this.host = host;
    }

    @Override
    public String getName() {
      return "";
    }

    @Override
    public String getHost() {
      return this.host;
    }

    @Override
    public Set<Role> getRoles() {
      return Collections.emptySet();
    }

    @Override
    public int getProcessId() {
      return 0;
    }

    @Override
    public String getId() {
      return this.host;
    }

    @Override
    public String getUniqueId() {
      return host;
    }

    @Override
    public int compareTo(DistributedMember o) {
      if ((o == null) || !(o instanceof FakeDistributedMember)) {
        throw new InternalGemFireException("Invalidly comparing TestDistributedMember to " + o);
      }

      FakeDistributedMember fakeDistributedMember = (FakeDistributedMember) o;
      return getHost().compareTo(fakeDistributedMember.getHost());
    }

    @Override
    public boolean equals(Object obj) {
      return (obj != null) && obj instanceof FakeDistributedMember
          && compareTo((FakeDistributedMember) obj) == 0;
    }

    @Override
    public int hashCode() {
      return getHost().hashCode();
    }

    @Override
    public DurableClientAttributes getDurableClientAttributes() {
      return null;
    }

    @Override
    public List<String> getGroups() {
      return Collections.emptyList();
    }
  }
}
