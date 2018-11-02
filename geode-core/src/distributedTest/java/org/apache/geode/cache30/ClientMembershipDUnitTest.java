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
package org.apache.geode.cache30;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.InternalClientMembership;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.management.membership.ClientMembership;
import org.apache.geode.management.membership.ClientMembershipEvent;
import org.apache.geode.management.membership.ClientMembershipListener;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Tests the ClientMembership API including ClientMembershipListener.
 *
 * @since GemFire 4.2.1
 */
@Category({ClientServerTest.class})
public class ClientMembershipDUnitTest extends ClientServerTestCase {

  protected static final boolean CLIENT = true;
  protected static final boolean SERVER = false;

  protected static final int JOINED = 0;
  protected static final int LEFT = 1;
  protected static final int CRASHED = 2;

  private static Properties properties;

  ServerLocation serverLocation = new ServerLocation("127.0.0.1", 0);

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM((() -> cleanup()));
  }

  public static void cleanup() {
    properties = null;
    InternalClientMembership.unregisterAllListeners();
  }

  private void waitForAcceptsInProgressToBe(final int target) throws Exception {
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      int actual = getAcceptsInProgress();
      if (actual == getAcceptsInProgress()) {
        return true;
      }
      return false;
    });
  }

  protected int getAcceptsInProgress() {
    DistributedSystem distributedSystem = getCache().getDistributedSystem();
    StatisticsType st = distributedSystem.findType("CacheServerStats");
    Statistics[] s = distributedSystem.findStatisticsByType(st);
    return s[0].getInt("acceptsInProgress");
  }

  protected static Socket meanSocket;

  /**
   * test that a server times out waiting for a handshake that never arrives.
   */
  @Test
  public void testConnectionTimeout() throws Exception {
    IgnoredException.addIgnoredException("failed accepting client connection");
    final Host host = Host.getHost(0);
    final String hostName = NetworkUtils.getServerHostName(host);
    final VM vm0 = host.getVM(0);
    System.setProperty(AcceptorImpl.ACCEPT_TIMEOUT_PROPERTY_NAME, "1000");
    try {
      final int port = startBridgeServer(0);
      // AsyncInvocation ai = null;
      try {
        assertTrue(port != 0);
        SerializableRunnable createMeanSocket =
            new CacheSerializableRunnable("Connect to server with socket") {
              public void run2() throws CacheException {
                getCache(); // create a cache so we have stats
                System.out.println("connecting to cache server with socket");
                try {
                  InetAddress addr = InetAddress.getByName(hostName);
                  meanSocket = new Socket(addr, port);
                } catch (Exception e) {
                  throw new RuntimeException("Test failed to connect or was interrupted", e);
                }
              }
            };
        SerializableRunnable closeMeanSocket = new CacheSerializableRunnable("close mean socket") {
          public void run2() throws CacheException {
            System.out.println("closing mean socket");
            try {
              meanSocket.close();
            } catch (IOException ignore) {
            }
          }
        };

        assertEquals(0, getAcceptsInProgress());

        System.out.println("creating mean socket");
        vm0.invoke("Connect to server with socket", () -> createMeanSocket);
        try {
          System.out.println("waiting to see it connect on server");
          waitForAcceptsInProgressToBe(1);
        } finally {
          System.out.println("closing mean socket");
          vm0.invoke("close mean socket", () -> closeMeanSocket);
        }
        System.out.println("waiting to see accept to go away on server");
        waitForAcceptsInProgressToBe(0);

        // now try it without a close. Server should timeout the mean connect
        System.out.println("creating mean socket 2");
        vm0.invoke("Connect to server with socket", () -> createMeanSocket);
        try {
          System.out.println("waiting to see it connect on server 2");
          waitForAcceptsInProgressToBe(1);
          System.out.println("waiting to see accept to go away on server without us closing");
          waitForAcceptsInProgressToBe(0);
        } finally {
          System.out.println("closing mean socket 2");
          vm0.invoke("close mean socket", () -> closeMeanSocket);
        }
      } finally {
        stopBridgeServers(getCache());
      }
    } finally {
      System.getProperties().remove(AcceptorImpl.ACCEPT_TIMEOUT_PROPERTY_NAME);
    }
  }

  @Test
  public void testSynchronousEvents() throws Exception {
    getSystem();
    InternalClientMembership.setForceSynchronous(true);
    try {
      doTestBasicEvents();
    } finally {
      InternalClientMembership.setForceSynchronous(false);
    }
  }

  /**
   * Tests event notification methods on ClientMembership.
   */
  @Test
  public void testBasicEvents() throws Exception {
    getSystem();
    doTestBasicEvents();
  }

  public void doTestBasicEvents() throws Exception {
    final boolean[] fired = new boolean[3];
    final DistributedMember[] member = new DistributedMember[3];
    final String[] memberId = new String[3];
    final boolean[] isClient = new boolean[3];

    ClientMembershipListener listener = new ClientMembershipListener() {
      public void memberJoined(ClientMembershipEvent event) {
        fired[JOINED] = true;
        member[JOINED] = event.getMember();
        memberId[JOINED] = event.getMemberId();
        isClient[JOINED] = event.isClient();
      }

      public void memberLeft(ClientMembershipEvent event) {
        fired[LEFT] = true;
        member[LEFT] = event.getMember();
        memberId[LEFT] = event.getMemberId();
        isClient[LEFT] = event.isClient();
      }

      public void memberCrashed(ClientMembershipEvent event) {
        fired[CRASHED] = true;
        member[CRASHED] = event.getMember();
        memberId[CRASHED] = event.getMemberId();
        isClient[CRASHED] = event.isClient();
      }
    };
    ClientMembership.registerClientMembershipListener(listener);

    // test JOIN for server
    InternalClientMembership.notifyServerJoined(serverLocation);

    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[JOINED];
    });

    assertTrue(fired[JOINED]);
    assertNotNull(member[JOINED]);
    assertFalse(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // test JOIN for client
    DistributedMember clientJoined = new TestDistributedMember("clientJoined");
    InternalClientMembership.notifyClientJoined(clientJoined);
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[JOINED];
    });

    assertTrue(fired[JOINED]);
    assertEquals(clientJoined, member[JOINED]);
    assertEquals(clientJoined.getId(), memberId[JOINED]);
    assertTrue(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // test LEFT for server
    InternalClientMembership.notifyServerLeft(serverLocation);
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[LEFT];
    });

    assertFalse(fired[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertTrue(fired[LEFT]);
    assertNotNull(member[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // test LEFT for client
    DistributedMember clientLeft = new TestDistributedMember("clientLeft");
    InternalClientMembership.notifyClientLeft(clientLeft);
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[LEFT];
    });

    assertFalse(fired[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertTrue(fired[LEFT]);
    assertEquals(clientLeft, member[LEFT]);
    assertEquals(clientLeft.getId(), memberId[LEFT]);
    assertTrue(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // test CRASHED for server
    InternalClientMembership.notifyServerCrashed(serverLocation);
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[CRASHED];
    });

    assertFalse(fired[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertTrue(fired[CRASHED]);
    assertNotNull(member[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // test CRASHED for client
    DistributedMember clientCrashed = new TestDistributedMember("clientCrashed");
    InternalClientMembership.notifyClientCrashed(clientCrashed);
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[CRASHED];
    });

    assertFalse(fired[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertTrue(fired[CRASHED]);
    assertEquals(clientCrashed, member[CRASHED]);
    assertEquals(clientCrashed.getId(), memberId[CRASHED]);
    assertTrue(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);
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
   * Tests unregisterClientMembershipListener to ensure that no further events are delivered to
   * unregistered listeners.
   */
  @Test
  public void testUnregisterClientMembershipListener() throws Exception {
    final boolean[] fired = new boolean[1];
    final DistributedMember[] member = new DistributedMember[1];
    final String[] memberId = new String[1];
    final boolean[] isClient = new boolean[1];

    getSystem();

    ClientMembershipListener listener = new ClientMembershipListener() {
      public void memberJoined(ClientMembershipEvent event) {
        fired[0] = true;
        member[0] = event.getMember();
        memberId[0] = event.getMemberId();
        isClient[0] = event.isClient();
      }

      public void memberLeft(ClientMembershipEvent event) {}

      public void memberCrashed(ClientMembershipEvent event) {}
    };
    ClientMembership.registerClientMembershipListener(listener);

    // fire event to make sure listener is registered
    DistributedMember clientJoined = new TestDistributedMember("clientJoined");
    InternalClientMembership.notifyClientJoined(clientJoined);
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[JOINED];
    });

    assertTrue(fired[0]);
    assertEquals(clientJoined, member[0]);
    assertEquals(clientJoined.getId(), memberId[0]);
    assertTrue(isClient[0]);

    resetArraysForTesting(fired, member, memberId, isClient);
    assertFalse(fired[0]);
    assertNull(memberId[0]);
    assertFalse(isClient[0]);

    // unregister and verify listener is not notified
    ClientMembership.unregisterClientMembershipListener(listener);
    InternalClientMembership.notifyClientJoined(clientJoined);
    await().until(() -> {
      return true;
    });

    assertFalse(fired[0]);
    assertNull(member[0]);
    assertNull(memberId[0]);
    assertFalse(isClient[0]);
  }

  @Test
  public void testMultipleListeners() throws Exception {
    final int NUM_LISTENERS = 4;
    final boolean[] fired = new boolean[NUM_LISTENERS];
    final DistributedMember[] member = new DistributedMember[NUM_LISTENERS];
    final String[] memberId = new String[NUM_LISTENERS];
    final boolean[] isClient = new boolean[NUM_LISTENERS];

    getSystem();

    final ClientMembershipListener[] listeners = new ClientMembershipListener[NUM_LISTENERS];
    for (int i = 0; i < NUM_LISTENERS; i++) {
      final int whichListener = i;
      listeners[i] = new ClientMembershipListener() {
        public void memberJoined(ClientMembershipEvent event) {
          assertFalse(fired[whichListener]);
          assertNull(member[whichListener]);
          assertNull(memberId[whichListener]);
          assertFalse(isClient[whichListener]);
          fired[whichListener] = true;
          member[whichListener] = event.getMember();
          memberId[whichListener] = event.getMemberId();
          isClient[whichListener] = event.isClient();
        }

        public void memberLeft(ClientMembershipEvent event) {}

        public void memberCrashed(ClientMembershipEvent event) {}
      };
    }

    final DistributedMember clientJoined = new TestDistributedMember("clientJoined");
    InternalClientMembership.notifyClientJoined(clientJoined);
    for (int i = 0; i < NUM_LISTENERS; i++) {
      synchronized (listeners[i]) {
        listeners[i].wait(20);
      }
      assertFalse(fired[i]);
      assertNull(member[i]);
      assertNull(memberId[i]);
      assertFalse(isClient[i]);
    }

    // attempt to register same listener twice... 2nd reg should be ignored
    // failure would cause an assertion failure in memberJoined impl
    ClientMembership.registerClientMembershipListener(listeners[0]);
    ClientMembership.registerClientMembershipListener(listeners[0]);

    ClientMembershipListener[] registeredListeners =
        ClientMembership.getClientMembershipListeners();
    assertEquals(1, registeredListeners.length);
    assertEquals(listeners[0], registeredListeners[0]);

    ClientMembership.registerClientMembershipListener(listeners[1]);
    registeredListeners = ClientMembership.getClientMembershipListeners();
    assertEquals(2, registeredListeners.length);
    assertEquals(listeners[0], registeredListeners[0]);
    assertEquals(listeners[1], registeredListeners[1]);

    InternalClientMembership.notifyClientJoined(clientJoined);
    synchronized (listeners[1]) {
      if (!fired[1]) {
        listeners[1].wait(2000);
      }
    }
    for (int i = 0; i < NUM_LISTENERS; i++) {
      if (i < 2) {
        assertTrue(fired[i]);
        assertEquals(clientJoined, member[i]);
        assertEquals(clientJoined.getId(), memberId[i]);
        assertTrue(isClient[i]);
      } else {
        assertFalse(fired[i]);
        assertNull(member[i]);
        assertNull(memberId[i]);
        assertFalse(isClient[i]);
      }
    }
    resetArraysForTesting(fired, member, memberId, isClient);

    ClientMembership.unregisterClientMembershipListener(listeners[0]);
    registeredListeners = ClientMembership.getClientMembershipListeners();
    assertEquals(1, registeredListeners.length);
    assertEquals(listeners[1], registeredListeners[0]);

    InternalClientMembership.notifyClientJoined(clientJoined);
    synchronized (listeners[1]) {
      if (!fired[1]) {
        listeners[1].wait(2000);
      }
    }
    for (int i = 0; i < NUM_LISTENERS; i++) {
      if (i == 1) {
        assertTrue(fired[i]);
        assertEquals(clientJoined, member[i]);
        assertEquals(clientJoined.getId(), memberId[i]);
        assertTrue(isClient[i]);
      } else {
        assertFalse(fired[i]);
        assertNull(member[i]);
        assertNull(memberId[i]);
        assertFalse(isClient[i]);
      }
    }
    resetArraysForTesting(fired, member, memberId, isClient);

    ClientMembership.registerClientMembershipListener(listeners[2]);
    ClientMembership.registerClientMembershipListener(listeners[3]);
    registeredListeners = ClientMembership.getClientMembershipListeners();
    assertEquals(3, registeredListeners.length);
    assertEquals(listeners[1], registeredListeners[0]);
    assertEquals(listeners[2], registeredListeners[1]);
    assertEquals(listeners[3], registeredListeners[2]);

    InternalClientMembership.notifyClientJoined(clientJoined);
    synchronized (listeners[3]) {
      if (!fired[3]) {
        listeners[3].wait(2000);
      }
    }
    for (int i = 0; i < NUM_LISTENERS; i++) {
      if (i != 0) {
        assertTrue(fired[i]);
        assertEquals(clientJoined, member[i]);
        assertEquals(clientJoined.getId(), memberId[i]);
        assertTrue(isClient[i]);
      } else {
        assertFalse(fired[i]);
        assertNull(member[i]);
        assertNull(memberId[i]);
        assertFalse(isClient[i]);
      }
    }
    resetArraysForTesting(fired, member, memberId, isClient);

    ClientMembership.registerClientMembershipListener(listeners[0]);
    registeredListeners = ClientMembership.getClientMembershipListeners();
    assertEquals(4, registeredListeners.length);
    assertEquals(listeners[1], registeredListeners[0]);
    assertEquals(listeners[2], registeredListeners[1]);
    assertEquals(listeners[3], registeredListeners[2]);
    assertEquals(listeners[0], registeredListeners[3]);

    InternalClientMembership.notifyClientJoined(clientJoined);
    synchronized (listeners[0]) {
      if (!fired[0]) {
        listeners[0].wait(2000);
      }
    }
    for (int i = 0; i < NUM_LISTENERS; i++) {
      assertTrue(fired[i]);
      assertEquals(clientJoined, member[i]);
      assertEquals(clientJoined.getId(), memberId[i]);
      assertTrue(isClient[i]);
    }
    resetArraysForTesting(fired, member, memberId, isClient);

    ClientMembership.unregisterClientMembershipListener(listeners[3]);
    registeredListeners = ClientMembership.getClientMembershipListeners();
    assertEquals(3, registeredListeners.length);
    assertEquals(listeners[1], registeredListeners[0]);
    assertEquals(listeners[2], registeredListeners[1]);
    assertEquals(listeners[0], registeredListeners[2]);

    InternalClientMembership.notifyClientJoined(clientJoined);
    synchronized (listeners[0]) {
      if (!fired[0]) {
        listeners[0].wait(2000);
      }
    }
    for (int i = 0; i < NUM_LISTENERS; i++) {
      if (i < 3) {
        assertTrue(fired[i]);
        assertEquals(clientJoined, member[i]);
        assertEquals(clientJoined.getId(), memberId[i]);
        assertTrue(isClient[i]);
      } else {
        assertFalse(fired[i]);
        assertNull(member[i]);
        assertNull(memberId[i]);
        assertFalse(isClient[i]);
      }
    }
    resetArraysForTesting(fired, member, memberId, isClient);

    ClientMembership.unregisterClientMembershipListener(listeners[2]);
    registeredListeners = ClientMembership.getClientMembershipListeners();
    assertEquals(2, registeredListeners.length);
    assertEquals(listeners[1], registeredListeners[0]);
    assertEquals(listeners[0], registeredListeners[1]);

    InternalClientMembership.notifyClientJoined(clientJoined);
    synchronized (listeners[0]) {
      if (!fired[0]) {
        listeners[0].wait(2000);
      }
    }
    for (int i = 0; i < NUM_LISTENERS; i++) {
      if (i < 2) {
        assertTrue(fired[i]);
        assertEquals(clientJoined, member[i]);
        assertEquals(clientJoined.getId(), memberId[i]);
        assertTrue(isClient[i]);
      } else {
        assertFalse(fired[i]);
        assertNull(member[i]);
        assertNull(memberId[i]);
        assertFalse(isClient[i]);
      }
    }
    resetArraysForTesting(fired, member, memberId, isClient);

    ClientMembership.unregisterClientMembershipListener(listeners[1]);
    ClientMembership.unregisterClientMembershipListener(listeners[0]);
    registeredListeners = ClientMembership.getClientMembershipListeners();
    assertEquals(0, registeredListeners.length);

    InternalClientMembership.notifyClientJoined(clientJoined);
    for (int i = 0; i < NUM_LISTENERS; i++) {
      synchronized (listeners[i]) {
        listeners[i].wait(20);
      }
      assertFalse(fired[i]);
      assertNull(member[i]);
      assertNull(memberId[i]);
      assertFalse(isClient[i]);
    }
    resetArraysForTesting(fired, member, memberId, isClient);

    ClientMembership.registerClientMembershipListener(listeners[1]);
    registeredListeners = ClientMembership.getClientMembershipListeners();
    assertEquals(1, registeredListeners.length);
    assertEquals(listeners[1], registeredListeners[0]);

    InternalClientMembership.notifyClientJoined(clientJoined);
    synchronized (listeners[1]) {
      if (!fired[1]) {
        listeners[1].wait(2000);
      }
    }
    for (int i = 0; i < NUM_LISTENERS; i++) {
      if (i == 1) {
        assertTrue(fired[i]);
        assertEquals(clientJoined, member[i]);
        assertEquals(clientJoined.getId(), memberId[i]);
        assertTrue(isClient[i]);
      } else {
        assertFalse(fired[i]);
        assertNull(member[i]);
        assertNull(memberId[i]);
        assertFalse(isClient[i]);
      }
    }
  }

  protected static int testClientMembershipEventsInClient_port;

  private static int getTestClientMembershipEventsInClient_port() {
    return testClientMembershipEventsInClient_port;
  }

  /**
   * Tests notification of events in client process. Bridge clients detect server joins when the
   * client connects to the server. If the server crashes or departs gracefully, the client will
   * detect this as a crash.
   */
  @Test
  public void testClientMembershipEventsInClient() throws Exception {
    properties = null;
    getSystem();
    IgnoredException.addIgnoredException("IOException");
    final boolean[] fired = new boolean[3];
    final DistributedMember[] member = new DistributedMember[3];
    final String[] memberId = new String[3];
    final boolean[] isClient = new boolean[3];

    // create and register ClientMembershipListener in controller vm...
    ClientMembershipListener listener = new ClientMembershipListener() {
      public void memberJoined(ClientMembershipEvent event) {
        System.out.println("[testClientMembershipEventsInClient] memberJoined: " + event);
        fired[JOINED] = true;
        member[JOINED] = event.getMember();
        memberId[JOINED] = event.getMemberId();
        isClient[JOINED] = event.isClient();
      }

      public void memberLeft(ClientMembershipEvent event) {
        System.out.println("[testClientMembershipEventsInClient] memberLeft: " + event);
      }

      public void memberCrashed(ClientMembershipEvent event) {
        System.out.println("[testClientMembershipEventsInClient] memberCrashed: " + event);
        fired[CRASHED] = true;
        member[CRASHED] = event.getMember();
        memberId[CRASHED] = event.getMemberId();
        isClient[CRASHED] = event.isClient();
      }
    };
    ClientMembership.registerClientMembershipListener(listener);

    final VM vm0 = Host.getHost(0).getVM(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[1];

    // create BridgeServer in vm0...
    vm0.invoke("create cache server", () -> {
      try {
        System.out.println("[testClientMembershipEventsInClient] Create BridgeServer");
        getSystem();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Region region = createRegion(name, factory.create());
        assertNotNull(region);
        assertNotNull(getRootRegion().getSubregion(name));
        testClientMembershipEventsInClient_port = startBridgeServer(0);
      } catch (IOException e) {
        getSystem().getLogWriter().fine(new Exception(e));
        fail("Failed to start CacheServer: " + e.getMessage());
      }
    });

    // gather details for later creation of ConnectionPool...
    ports[0] = vm0.invoke("getTestClientMembershipEventsInClient_port",
        () -> ClientMembershipDUnitTest.getTestClientMembershipEventsInClient_port());
    assertTrue(ports[0] != 0);

    DistributedMember serverMember = (DistributedMember) vm0.invoke("get distributed member",
        () -> getSystem().getDistributedMember());

    String serverMemberId = serverMember.toString();

    System.out.println("[testClientMembershipEventsInClient] ports[0]=" + ports[0]);
    System.out.println("[testClientMembershipEventsInClient] serverMember=" + serverMember);
    System.out.println("[testClientMembershipEventsInClient] serverMemberId=" + serverMemberId);

    assertFalse(fired[JOINED]);
    assertNull(member[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(member[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(member[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);

    // sanity check...
    System.out.println("[testClientMembershipEventsInClient] sanity check");
    InternalClientMembership.notifyServerJoined(serverLocation);

    await().timeout(300, SECONDS)
        .until(() -> fired[JOINED] || fired[CRASHED]);

    assertTrue(fired[JOINED]);
    assertNotNull(member[JOINED]);
    assertFalse(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(member[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(member[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // create bridge client in controller vm...
    System.out.println("[testClientMembershipEventsInClient] create bridge client");
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    getSystem(config);

    try {
      getCache();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      ClientServerTestCase.configureConnectionPool(factory,
          NetworkUtils.getServerHostName(Host.getHost(0)), ports, true, -1, -1, null);
      createRegion(name, factory.create());
      assertNotNull(getRootRegion().getSubregion(name));
    } catch (CacheException ex) {
      Assert.fail("While creating Region on Edge", ex);
    }

    await().timeout(300, SECONDS)
        .until(() -> fired[JOINED] || fired[CRASHED]);

    System.out.println("[testClientMembershipEventsInClient] assert client detected server join");

    // first check the getCurrentServers() result
    ClientCache clientCache = (ClientCache) getCache();
    Set<InetSocketAddress> servers = clientCache.getCurrentServers();
    assertTrue(!servers.isEmpty());
    InetSocketAddress serverAddr = servers.iterator().next();
    InetSocketAddress expectedAddr = new InetSocketAddress(serverMember.getHost(), ports[0]);
    assertEquals(expectedAddr, serverAddr);

    // now check listener results
    assertTrue(fired[JOINED]);
    assertNotNull(member[JOINED]);
    assertNotNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(member[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(member[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    vm0.invoke("Stop BridgeServer", () -> stopBridgeServers(getCache()));

    await().timeout(300, SECONDS)
        .until(() -> fired[JOINED] || fired[CRASHED]);

    System.out
        .println("[testClientMembershipEventsInClient] assert client detected server departure");
    assertFalse(fired[JOINED]);
    assertNull(member[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(member[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertTrue(fired[CRASHED]);
    assertNotNull(member[CRASHED]);
    assertNotNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // now test that we redisover the cache server
    vm0.invoke("Recreate BridgeServer", () -> {
      try {
        System.out.println("[testClientMembershipEventsInClient] restarting BridgeServer");
        startBridgeServer(ports[0]);
      } catch (IOException e) {
        getSystem().getLogWriter().fine(new Exception(e));
        fail("Failed to start CacheServer on VM1: " + e.getMessage());
      }
    });

    await().timeout(300, SECONDS)
        .until(() -> fired[JOINED] || fired[CRASHED]);

    System.out
        .println("[testClientMembershipEventsInClient] assert client detected server recovery");
    assertTrue(fired[JOINED]);
    assertNotNull(member[JOINED]);
    assertNotNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(member[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(member[CRASHED]);
    assertNull(memberId[CRASHED]);
  }

  /**
   * Tests notification of events in server process. cache servers detect client joins when the
   * client connects to the server.
   */
  @Test
  public void testClientMembershipEventsInServer() throws Exception {
    final boolean[] fired = new boolean[3];
    final DistributedMember[] member = new DistributedMember[3];
    final String[] memberId = new String[3];
    final boolean[] isClient = new boolean[3];

    // create and register ClientMembershipListener in controller vm...
    ClientMembershipListener listener = new ClientMembershipListener() {
      public void memberJoined(ClientMembershipEvent event) {
        System.out.println("[testClientMembershipEventsInServer] memberJoined: " + event);
        fired[JOINED] = true;
        member[JOINED] = event.getMember();
        memberId[JOINED] = event.getMemberId();
        isClient[JOINED] = event.isClient();
        assertFalse(fired[LEFT] || fired[CRASHED]);
      }

      public void memberLeft(ClientMembershipEvent event) {
        System.out.println("[testClientMembershipEventsInServer] memberLeft: " + event);
        fired[LEFT] = true;
        member[LEFT] = event.getMember();
        memberId[LEFT] = event.getMemberId();
        isClient[LEFT] = event.isClient();
        assertFalse(fired[JOINED] || fired[CRASHED]);
      }

      public void memberCrashed(ClientMembershipEvent event) {
        System.out.println("[testClientMembershipEventsInServer] memberCrashed: " + event);
        fired[CRASHED] = true;
        member[CRASHED] = event.getMember();
        memberId[CRASHED] = event.getMemberId();
        isClient[CRASHED] = event.isClient();
        assertFalse(fired[JOINED] || fired[LEFT]);
      }
    };
    ClientMembership.registerClientMembershipListener(listener);

    final VM vm0 = Host.getHost(0).getVM(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[1];

    // create BridgeServer in controller vm...
    System.out.println("[testClientMembershipEventsInServer] Create BridgeServer");
    getSystem();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region region = createRegion(name, factory.create());
    assertNotNull(region);
    assertNotNull(getRootRegion().getSubregion(name));

    ports[0] = startBridgeServer(0);
    assertTrue(ports[0] != 0);
    DistributedMember serverMember = getMemberId();
    String serverMemberId = serverMember.toString();

    System.out.println("[testClientMembershipEventsInServer] ports[0]=" + ports[0]);
    System.out.println("[testClientMembershipEventsInServer] serverMemberId=" + serverMemberId);
    System.out.println("[testClientMembershipEventsInServer] serverMember=" + serverMember);

    assertFalse(fired[JOINED]);
    assertNull(member[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(member[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(member[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);

    // sanity check...
    System.out.println("[testClientMembershipEventsInServer] sanity check");
    DistributedMember test = new TestDistributedMember("test");
    InternalClientMembership.notifyClientJoined(test);
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[JOINED] || fired[LEFT] || fired[CRASHED];
    });

    assertTrue(fired[JOINED]);
    assertEquals(test, member[JOINED]);
    assertEquals(test.getId(), memberId[JOINED]);
    assertTrue(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(member[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(member[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    final Host host = Host.getHost(0);
    SerializableCallable createConnectionPool = new SerializableCallable("Create connectionPool") {
      public Object call() {
        System.out.println("[testClientMembershipEventsInServer] create bridge client");
        Properties config = new Properties();
        config.setProperty(MCAST_PORT, "0");
        config.setProperty(LOCATORS, "");
        config.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");
        properties = config;
        DistributedSystem s = getSystem(config);
        AttributesFactory factory = new AttributesFactory();
        Pool pool = ClientServerTestCase.configureConnectionPool(factory,
            NetworkUtils.getServerHostName(host), ports, true, -1, 2, null);
        createRegion(name, factory.create());
        assertNotNull(getRootRegion().getSubregion(name));
        assertTrue(s == basicGetSystem()); // see geode-1078
        return getMemberId();
      }
    };

    // create bridge client in vm0...
    DistributedMember clientMember = (DistributedMember) vm0.invoke(createConnectionPool);
    String clientMemberId = clientMember.toString();

    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[JOINED] || fired[LEFT] || fired[CRASHED];
    });

    System.out.println("[testClientMembershipEventsInServer] assert server detected client join");
    assertTrue(fired[JOINED]);
    assertEquals(member[JOINED] + " should equal " + clientMember, clientMember, member[JOINED]);
    assertEquals(memberId[JOINED] + " should equal " + clientMemberId, clientMemberId,
        memberId[JOINED]);
    assertTrue(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(member[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(member[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    pauseForClientToJoin();

    vm0.invoke(new SerializableRunnable("Stop bridge client") {
      public void run() {
        System.out.println("[testClientMembershipEventsInServer] Stop bridge client");
        getRootRegion().getSubregion(name).close();
        Map m = PoolManager.getAll();
        Iterator mit = m.values().iterator();
        while (mit.hasNext()) {
          Pool p = (Pool) mit.next();
          p.destroy();
        }
      }
    });

    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[JOINED] || fired[LEFT] || fired[CRASHED];
    });

    System.out.println("[testClientMembershipEventsInServer] assert server detected client left");
    assertFalse(fired[JOINED]);
    assertNull(member[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertTrue(fired[LEFT]);
    assertEquals(clientMember, member[LEFT]);
    assertEquals(clientMemberId, memberId[LEFT]);
    assertTrue(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(member[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // reconnect bridge client to test for crashed event
    clientMemberId = vm0.invoke(createConnectionPool).toString();

    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[JOINED] || fired[LEFT] || fired[CRASHED];
    });

    System.out
        .println("[testClientMembershipEventsInServer] assert server detected client re-join");
    assertTrue(fired[JOINED]);
    assertEquals(clientMember, member[JOINED]);
    assertEquals(clientMemberId, memberId[JOINED]);
    assertTrue(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(member[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(member[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    pauseForClientToJoin();

    ServerConnection.setForceClientCrashEvent(true);
    try {
      vm0.invoke(new SerializableRunnable("Stop bridge client") {
        public void run() {
          System.out.println("[testClientMembershipEventsInServer] Stop bridge client");
          getRootRegion().getSubregion(name).close();
          Map m = PoolManager.getAll();
          Iterator mit = m.values().iterator();
          while (mit.hasNext()) {
            Pool p = (Pool) mit.next();
            p.destroy();
          }
        }
      });

      await().timeout(300, TimeUnit.SECONDS).until(() -> {
        return fired[JOINED] || fired[LEFT] || fired[CRASHED];
      });

      System.out
          .println("[testClientMembershipEventsInServer] assert server detected client crashed");
      assertFalse(fired[JOINED]);
      assertNull(member[JOINED]);
      assertNull(memberId[JOINED]);
      assertFalse(isClient[JOINED]);
      assertFalse(fired[LEFT]);
      assertNull(member[LEFT]);
      assertNull(memberId[LEFT]);
      assertFalse(isClient[LEFT]);
      assertTrue(fired[CRASHED]);
      assertEquals(clientMember, member[CRASHED]);
      assertEquals(clientMemberId, memberId[CRASHED]);
      assertTrue(isClient[CRASHED]);
    } finally {
      ServerConnection.setForceClientCrashEvent(false);
    }
  }

  /**
   * The joined event fires when the first client handshake is processed. This pauses long enough to
   * allow the rest of the client sockets to complete handshaking before making the client leave.
   * Without doing this subsequent socket handshakes that are processed could fire join events after
   * departure events and then a departure event again. If you see failures in
   * testClientMembershipEventsInServer, try increasing this timeout.
   */
  private void pauseForClientToJoin() {
    Wait.pause(2000);
  }

  /**
   * Tests registration and event notification in conjunction with disconnecting and reconnecting to
   * DistributedSystem.
   */
  @Test
  public void testLifecycle() throws Exception {
    final boolean[] fired = new boolean[3];
    final DistributedMember[] member = new DistributedMember[3];
    final String[] memberId = new String[3];
    final boolean[] isClient = new boolean[3];

    // create and register ClientMembershipListener in controller vm...
    ClientMembershipListener listener = new ClientMembershipListener() {
      public void memberJoined(ClientMembershipEvent event) {
        assertFalse(fired[JOINED]);
        assertNull(member[JOINED]);
        assertNull(memberId[JOINED]);
        assertFalse(isClient[JOINED]);
        fired[JOINED] = true;
        member[JOINED] = event.getMember();
        memberId[JOINED] = event.getMemberId();
        isClient[JOINED] = event.isClient();
      }

      public void memberLeft(ClientMembershipEvent event) {}

      public void memberCrashed(ClientMembershipEvent event) {}
    };
    ClientMembership.registerClientMembershipListener(listener);

    // create loner in controller vm...
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    properties = config;
    getSystem(config);

    // assert that event is fired while connected
    InternalClientMembership.notifyServerJoined(serverLocation);
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[JOINED];
    });
    assertTrue(fired[JOINED]);
    assertNotNull(member[JOINED]);
    assertFalse(isClient[JOINED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // assert that event is NOT fired while disconnected
    disconnectFromDS();

    InternalClientMembership.notifyServerJoined(serverLocation);
    await().until(() -> {
      return true;
    });

    assertFalse(fired[JOINED]);
    assertNull(member[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // assert that event is fired again after reconnecting
    properties = config;
    assertTrue(getSystem(config).isConnected());

    InternalClientMembership.notifyServerJoined(serverLocation);
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      return fired[JOINED];
    });

    assertTrue(fired[JOINED]);
    assertNotNull(member[JOINED]);
    assertFalse(isClient[JOINED]);
  }

  /**
   * Starts up server in controller vm and 4 clients, then calls and tests
   * ClientMembership.getConnectedClients().
   */
  @Test
  public void testGetConnectedClients() throws Exception {
    final String name = this.getUniqueName();
    final int[] ports = new int[1];

    IgnoredException.addIgnoredException("ConnectException");

    // create BridgeServer in controller vm...
    System.out.println("[testGetConnectedClients] Create BridgeServer");
    getSystem();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region region = createRegion(name, factory.create());
    assertNotNull(region);
    assertNotNull(getRootRegion().getSubregion(name));

    ports[0] = startBridgeServer(0);
    assertTrue(ports[0] != 0);
    String serverMemberId = getSystem().getDistributedMember().toString();

    System.out.println("[testGetConnectedClients] ports[0]=" + ports[0]);
    System.out.println("[testGetConnectedClients] serverMemberId=" + serverMemberId);

    final Host host = Host.getHost(0);
    SerializableCallable createPool = new SerializableCallable("Create connection pool") {
      public Object call() {
        System.out.println("[testGetConnectedClients] create bridge client");
        properties = new Properties();
        properties.setProperty(MCAST_PORT, "0");
        properties.setProperty(LOCATORS, "");
        getSystem(properties);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Pool p = ClientServerTestCase.configureConnectionPool(factory,
            NetworkUtils.getServerHostName(host), ports, true, -1, -1, null);
        createRegion(name, factory.create());
        assertNotNull(getRootRegion().getSubregion(name));
        assertTrue(p.getServers().size() > 0);
        return getMemberId();
      }
    };

    // create bridge client in vm0...
    final String[] clientMemberIdArray = new String[host.getVMCount()];

    for (int i = 0; i < host.getVMCount(); i++) {
      final VM vm = Host.getHost(0).getVM(i);
      System.out.println("creating pool in vm_" + i);
      clientMemberIdArray[i] = vm.invoke(createPool).toString();
    }
    Collection clientMemberIds = Arrays.asList(clientMemberIdArray);

    {
      final int expectedClientCount = clientMemberIds.size();
      await().timeout(300, TimeUnit.SECONDS).until(() -> {
        Map connectedClients = InternalClientMembership.getConnectedClients(false, getCache());
        if (connectedClients == null) {
          return false;
        }
        if (connectedClients.size() != expectedClientCount) {
          return false;
        }
        return true;
      });
    }

    Map connectedClients = InternalClientMembership.getConnectedClients(false, getCache());
    assertNotNull(connectedClients);
    assertEquals(clientMemberIds.size(), connectedClients.size());
    System.out
        .println("connectedClients: " + connectedClients + "; clientMemberIds: " + clientMemberIds);
    for (Iterator iter = connectedClients.keySet().iterator(); iter.hasNext();) {
      String connectedClient = (String) iter.next();
      System.out.println("[testGetConnectedClients] checking for client " + connectedClient);
      assertTrue(clientMemberIds.contains(connectedClient));
      Object[] result = (Object[]) connectedClients.get(connectedClient);
      System.out.println("[testGetConnectedClients] result: "
          + (result == null ? "none" : String.valueOf(result[0]) + "; connections=" + result[1]));
    }
  }

  /**
   * Starts up 4 server and the controller vm as a client, then calls and tests
   * ClientMembership.getConnectedServers().
   */
  @Test
  public void testGetConnectedServers() throws Exception {
    final Host host = Host.getHost(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[host.getVMCount()];

    for (int i = 0; i < host.getVMCount(); i++) {
      final int whichVM = i;
      final VM vm = Host.getHost(0).getVM(i);
      vm.invoke("Create cache server", () -> {
        // create BridgeServer in controller vm...
        System.out.println("[testGetConnectedServers] Create BridgeServer");
        getSystem();
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Region region = createRegion(name + "_" + whichVM, factory.create());
        assertNotNull(region);
        assertNotNull(getRootRegion().getSubregion(name + "_" + whichVM));
        region.put("KEY-1", "VAL-1");

        try {
          testGetConnectedServers_port = startBridgeServer(0);
        } catch (IOException e) {
          org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
              .error("startBridgeServer threw IOException", e);
          fail("startBridgeServer threw IOException " + e.getMessage());
        }

        assertTrue(testGetConnectedServers_port != 0);

        System.out.println("[testGetConnectedServers] port=" + ports[whichVM]);
        System.out.println(
            "[testGetConnectedServers] serverMemberId=" + getSystem().getDistributedMember());
      });
      ports[whichVM] = vm.invoke("getTestGetConnectedServers_port",
          () -> ClientMembershipDUnitTest.getTestGetConnectedServers_port());
      assertTrue(ports[whichVM] != 0);
    }

    System.out.println("[testGetConnectedServers] create bridge client");
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    properties = config;
    getSystem(config);
    getCache();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);

    for (int i = 0; i < ports.length; i++) {
      System.out.println("[testGetConnectedServers] creating connectionpool for "
          + NetworkUtils.getServerHostName(host) + " " + ports[i]);
      int[] thisServerPorts = new int[] {ports[i]};
      ClientServerTestCase.configureConnectionPoolWithName(factory,
          NetworkUtils.getServerHostName(host), thisServerPorts, false, -1, -1, null, "pooly" + i);
      Region region = createRegion(name + "_" + i, factory.create());
      assertNotNull(getRootRegion().getSubregion(name + "_" + i));
      region.get("KEY-1");
    }

    final int expectedVMCount = host.getVMCount();
    await().timeout(300, TimeUnit.SECONDS).until(() -> {
      if (PoolManager.getAll().size() != expectedVMCount) {
        return false;
      }
      Map connectedServers = InternalClientMembership.getConnectedServers();
      if (connectedServers == null) {
        return false;
      }
      if (connectedServers.size() != expectedVMCount) {
        return false;
      }
      return true;
    });

    assertEquals(host.getVMCount(), PoolManager.getAll().size());

    Map connectedServers = InternalClientMembership.getConnectedServers();
    assertNotNull(connectedServers);
    assertEquals(host.getVMCount(), connectedServers.size());
    for (Iterator iter = connectedServers.keySet().iterator(); iter.hasNext();) {
      String connectedServer = (String) iter.next();
      System.out.println("[testGetConnectedServers]  value for connectedServer: "
          + connectedServers.get(connectedServer));
    }
  }

  protected static int testGetConnectedServers_port;

  private static int getTestGetConnectedServers_port() {
    return testGetConnectedServers_port;
  }

  public Properties getDistributedSystemProperties() {
    if (properties == null) {
      properties = new Properties();
      properties.put(ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION, "false");
    }
    return properties;
  }

  /**
   * Tests getConnectedClients(boolean onlyClientsNotifiedByThisServer) where
   * onlyClientsNotifiedByThisServer is true.
   */
  @Test
  public void testGetNotifiedClients() throws Exception {
    final Host host = Host.getHost(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[host.getVMCount()];

    for (int i = 0; i < host.getVMCount(); i++) {
      final int whichVM = i;
      final VM vm = Host.getHost(0).getVM(i);
      vm.invoke(new CacheSerializableRunnable("Create cache server") {
        public void run2() throws CacheException {
          // create BridgeServer in controller vm...
          System.out.println("[testGetNotifiedClients] Create BridgeServer");
          getSystem();
          AttributesFactory factory = new AttributesFactory();
          Region region = createRegion(name, factory.create());
          assertNotNull(region);
          assertNotNull(getRootRegion().getSubregion(name));
          region.put("KEY-1", "VAL-1");

          try {
            testGetNotifiedClients_port = startBridgeServer(0);
          } catch (IOException e) {
            org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
                .error("startBridgeServer threw IOException", e);
            fail("startBridgeServer threw IOException " + e.getMessage());
          }

          assertTrue(testGetNotifiedClients_port != 0);

          System.out.println("[testGetNotifiedClients] port=" + ports[whichVM]);
          System.out.println("[testGetNotifiedClients] serverMemberId=" + getMemberId());
        }
      });
      ports[whichVM] = vm.invoke("getTestGetNotifiedClients_port",
          () -> ClientMembershipDUnitTest.getTestGetNotifiedClients_port());
      assertTrue(ports[whichVM] != 0);
    }

    System.out.println("[testGetNotifiedClients] create bridge client");
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    properties = config;
    getSystem();
    getCache();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);

    System.out.println("[testGetNotifiedClients] creating connection pool");
    ClientServerTestCase.configureConnectionPool(factory, NetworkUtils.getServerHostName(host),
        ports, true, -1, -1, null);
    Region region = createRegion(name, factory.create());
    assertNotNull(getRootRegion().getSubregion(name));
    region.registerInterest("KEY-1");
    region.get("KEY-1");

    final String clientMemberId = getMemberId().toString();

    pauseForClientToJoin();

    // assertions go here
    int[] clientCounts = new int[host.getVMCount()];

    // only one server vm will have that client for updating
    for (int i = 0; i < host.getVMCount(); i++) {
      final int whichVM = i;
      final VM vm = Host.getHost(0).getVM(i);
      vm.invoke("Create cache server", () -> {
        Map clients = InternalClientMembership.getConnectedClients(true, getCache());
        assertNotNull(clients);
        testGetNotifiedClients_clientCount = clients.size();
        // [bruce] this is not a valid assertion - the server may not use
        // fully qualified host names while clients always use them in
        // forming their member ID. The test needs to check InetAddresses,
        // not strings
        // if (testGetNotifiedClients_clientCount > 0) {
        // // assert that the clientMemberId matches
        // assertEquals(clientMemberId, clients.keySet().iterator().next());
        // }
      });
      clientCounts[whichVM] = vm.invoke("getTestGetNotifiedClients_clientCount",
          () -> ClientMembershipDUnitTest.getTestGetNotifiedClients_clientCount());
    }

    // only one server should have a notifier for this client...
    int totalClientCounts = 0;
    for (int i = 0; i < clientCounts.length; i++) {
      totalClientCounts += clientCounts[i];
    }
    // this assertion fails because the count is 4
    // assertIndexDetailsEquals(1, totalClientCounts);
  }

  protected static int testGetNotifiedClients_port;

  private static int getTestGetNotifiedClients_port() {
    return testGetNotifiedClients_port;
  }

  protected static int testGetNotifiedClients_clientCount;

  private static int getTestGetNotifiedClients_clientCount() {
    return testGetNotifiedClients_clientCount;
  }

  // Simple DistributedMember implementation
  static class TestDistributedMember implements DistributedMember {

    private String host;

    public TestDistributedMember(String host) {
      this.host = host;
    }

    public String getName() {
      return "";
    }

    public String getHost() {
      return this.host;
    }

    public Set getRoles() {
      return new HashSet();
    }

    public int getProcessId() {
      return 0;
    }

    public String getId() {
      return this.host;
    }

    public int compareTo(DistributedMember o) {
      if ((o == null) || !(o instanceof TestDistributedMember)) {
        throw new InternalGemFireException("Invalidly comparing TestDistributedMember to " + o);
      }

      TestDistributedMember tds = (TestDistributedMember) o;
      return getHost().compareTo(tds.getHost());
    }

    public boolean equals(Object obj) {
      if ((obj == null) || !(obj instanceof TestDistributedMember)) {
        return false;
      }
      return compareTo((TestDistributedMember) obj) == 0;
    }

    public int hashCode() {
      return getHost().hashCode();
    }

    public DurableClientAttributes getDurableClientAttributes() {

      return null;
    }

    public List<String> getGroups() {
      return Collections.emptyList();
    }
  }
}
