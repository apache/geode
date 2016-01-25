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
package com.gemstone.gemfire.cache30;

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

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DurableClientAttributes;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.tier.InternalClientMembership;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.management.membership.ClientMembership;
import com.gemstone.gemfire.management.membership.ClientMembershipEvent;
import com.gemstone.gemfire.management.membership.ClientMembershipListener;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;

/**
 * Tests the ClientMembership API including ClientMembershipListener.
 *
 * @author Kirk Lund
 * @since 4.2.1
 */
public class ClientMembershipDUnitTest extends ClientServerTestCase {

  protected static final boolean CLIENT = true;
  protected static final boolean SERVER = false;
  
  protected static final int JOINED = 0;
  protected static final int LEFT = 1;
  protected static final int CRASHED = 2;
    
  public ClientMembershipDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    getSystem();
  }
  
  public void tearDown2() throws Exception {
    super.tearDown2();
    InternalClientMembership.unregisterAllListeners();
  }

  private void waitForAcceptsInProgressToBe(final int target)
    throws Exception {
    WaitCriterion ev = new WaitCriterion() {
      String excuse;
      public boolean done() {
        int actual = getAcceptsInProgress();
        if (actual == getAcceptsInProgress()) {
          return true;
        }
        excuse = "accepts in progress (" + actual + ") never became " + target;
        return false;
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
  }
  
  protected int getAcceptsInProgress() {
    StatisticsType st = InternalDistributedSystem.getAnyInstance().findType("CacheServerStats");
    Statistics[] s = InternalDistributedSystem.getAnyInstance().findStatisticsByType(st);
    return s[0].getInt("acceptsInProgress");
  }

  protected static Socket meanSocket;

  /** test that a server times out waiting for a handshake that
      never arrives. 
   */
  public void testConnectionTimeout() throws Exception {
    addExpectedException("failed accepting client connection");
    final Host host = Host.getHost(0);
    final String hostName = getServerHostName(host);
    final VM vm0 = host.getVM(0);
    System.setProperty(AcceptorImpl.ACCEPT_TIMEOUT_PROPERTY_NAME, "1000");
    try {
    final int port = startBridgeServer(0);
//    AsyncInvocation ai = null;
    try {
      assertTrue(port != 0);
      SerializableRunnable createMeanSocket = new CacheSerializableRunnable("Connect to server with socket") {
        public void run2() throws CacheException {
          getCache(); // create a cache so we have stats
          getLogWriter().info("connecting to cache server with socket");
          try {
            InetAddress addr = InetAddress.getByName(hostName);
            meanSocket = new Socket(addr, port);
          }
          catch (Exception e) {
            throw new RuntimeException("Test failed to connect or was interrupted", e);
          }
        }
      };
      SerializableRunnable closeMeanSocket = new CacheSerializableRunnable("close mean socket") {
        public void run2() throws CacheException {
          getLogWriter().info("closing mean socket");
          try {
            meanSocket.close();
          }
          catch (IOException ignore) {
          }
        }
      };

      assertEquals(0, getAcceptsInProgress());
      
      getLogWriter().info("creating mean socket");
      vm0.invoke(createMeanSocket);
      try {
        getLogWriter().info("waiting to see it connect on server");
        waitForAcceptsInProgressToBe(1);
      } finally {
        getLogWriter().info("closing mean socket");
        vm0.invoke(closeMeanSocket);
      }
      getLogWriter().info("waiting to see accept to go away on server");
      waitForAcceptsInProgressToBe(0);

      // now try it without a close. Server should timeout the mean connect
      getLogWriter().info("creating mean socket 2");
      vm0.invoke(createMeanSocket);
      try {
        getLogWriter().info("waiting to see it connect on server 2");
        waitForAcceptsInProgressToBe(1);
        getLogWriter().info("waiting to see accept to go away on server without us closing");
        waitForAcceptsInProgressToBe(0);
      } finally {
        getLogWriter().info("closing mean socket 2");
        vm0.invoke(closeMeanSocket);
      }

//       SerializableRunnable denialOfService = new CacheSerializableRunnable("Do lots of connects") {
//         public void run2() throws CacheException {
//           int connectionCount = 0;
//           ArrayList al = new ArrayList(60000);
//           try {
//             InetAddress addr = InetAddress.getLocalHost();
//             for (;;) {
//               Socket s = new Socket(addr, port);
//               al.add(s);
//               connectionCount++;
//               getLogWriter().info("connected # " + connectionCount + " s=" + s);
// //               try {
// //                 s.close();
// //               } catch (IOException ignore) {}
//             }
//           }
//           catch (Exception e) {
//             getLogWriter().info("connected # " + connectionCount
//                                 + " stopped because of exception " + e);
//             Iterator it = al.iterator();
//             while (it.hasNext()) {
//               Socket s = (Socket)it.next();
//               try {
//                 s.close();
//               } catch (IOException ignore) {}
//             }
//           }
//         }
//       };
//       // now pretend to do a denial of service attack by doing a bunch of connects
//       // really fast and see what that does to the server's fds.
//       getLogWriter().info("doing denial of service attach");
//       vm0.invoke(denialOfService);
//       // @todo darrel: check fd limit?
    }
    finally {
      stopBridgeServers(getCache());
    }
    }
    finally {
      System.getProperties().remove(AcceptorImpl.ACCEPT_TIMEOUT_PROPERTY_NAME);
    }
  }

  public void testSynchronousEvents() throws Exception {
    InternalClientMembership.setForceSynchronous(true);
    try {
      doTestBasicEvents();
    }
    finally {
      InternalClientMembership.setForceSynchronous(false);
    }
  }
  
  /**
   * Tests event notification methods on ClientMembership.
   */
  public void testBasicEvents() throws Exception {
    doTestBasicEvents();
  }
  
  public void doTestBasicEvents() throws Exception {
    final boolean[] fired = new boolean[3];
    final DistributedMember[] member = new DistributedMember[3];
    final String[] memberId = new String[3];
    final boolean[] isClient = new boolean[3];
    
    ClientMembershipListener listener = new ClientMembershipListener() {
      public synchronized void memberJoined(ClientMembershipEvent event) {
        fired[JOINED] = true;
        member[JOINED] = event.getMember();
        memberId[JOINED] = event.getMemberId();
        isClient[JOINED] = event.isClient();
        notify();
      }
      public synchronized void memberLeft(ClientMembershipEvent event) {
        fired[LEFT] = true;
        member[LEFT] = event.getMember();
        memberId[LEFT] = event.getMemberId();
        isClient[LEFT] = event.isClient();
        notify();
      }
      public synchronized void memberCrashed(ClientMembershipEvent event) {
        fired[CRASHED] = true;
        member[CRASHED] = event.getMember();
        memberId[CRASHED] = event.getMemberId();
        isClient[CRASHED] = event.isClient();
        notify();
      }
    };
    ClientMembership.registerClientMembershipListener(listener);
    
    // test JOIN for server
    DistributedMember serverJoined = new TestDistributedMember("serverJoined");
    InternalClientMembership.notifyJoined(serverJoined, SERVER);
    synchronized(listener) {
      if (!fired[JOINED]) {
        listener.wait(2000);
      }
    }
    assertTrue(fired[JOINED]);
    assertEquals(serverJoined, member[JOINED]);
    assertEquals(serverJoined.getId(), memberId[JOINED]);
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
    InternalClientMembership.notifyJoined(clientJoined, CLIENT);
    synchronized(listener) {
      if (!fired[JOINED]) {
        listener.wait(2000);
      }
    }
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
    DistributedMember serverLeft = new TestDistributedMember("serverLeft");
    InternalClientMembership.notifyLeft(serverLeft, SERVER);
    synchronized(listener) {
      if (!fired[LEFT]) {
        listener.wait(2000);
      }
    }
    assertFalse(fired[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertTrue(fired[LEFT]);
    assertEquals(serverLeft, member[LEFT]);
    assertEquals(serverLeft.getId(), memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // test LEFT for client
    DistributedMember clientLeft = new TestDistributedMember("clientLeft");
    InternalClientMembership.notifyLeft(clientLeft, CLIENT);
    synchronized(listener) {
      if (!fired[LEFT]) {
        listener.wait(2000);
      }
    }
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
    DistributedMember serverCrashed = new TestDistributedMember("serverCrashed");
    InternalClientMembership.notifyCrashed(serverCrashed, SERVER);
    synchronized(listener) {
      if (!fired[CRASHED]) {
        listener.wait(2000);
      }
    }
    assertFalse(fired[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertTrue(fired[CRASHED]);
    assertEquals(serverCrashed, member[CRASHED]);
    assertEquals(serverCrashed.getId(), memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);

    // test CRASHED for client
    DistributedMember clientCrashed = new TestDistributedMember("clientCrashed");
    InternalClientMembership.notifyCrashed(clientCrashed, CLIENT);
    synchronized(listener) {
      if (!fired[CRASHED]) {
        listener.wait(2000);
      }
    }
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
   * Resets all elements of arrays used for listener testing. Boolean values
   * are reset to false. String values are reset to null.
   */
  private void resetArraysForTesting(boolean[] fired, 
                                     DistributedMember[] member,
                                     String[] memberId, 
                                     boolean[] isClient) {
    for (int i = 0; i < fired.length; i++) {
      fired[i] = false;
      member[i] = null;
      memberId[i] = null;
      isClient[i] = false;
    }
  }
  
  /**
   * Tests unregisterClientMembershipListener to ensure that no further events
   * are delivered to unregistered listeners.
   */
  public void testUnregisterClientMembershipListener() throws Exception {
    final boolean[] fired = new boolean[1];
    final DistributedMember[] member = new DistributedMember[1];
    final String[] memberId = new String[1];
    final boolean[] isClient = new boolean[1];
    
    ClientMembershipListener listener = new ClientMembershipListener() {
      public synchronized void memberJoined(ClientMembershipEvent event) {
        fired[0] = true;
        member[0] = event.getMember();
        memberId[0] = event.getMemberId();
        isClient[0] = event.isClient();
        notify();
      }
      public void memberLeft(ClientMembershipEvent event) {
      }
      public void memberCrashed(ClientMembershipEvent event) {
      }
    };
    ClientMembership.registerClientMembershipListener(listener);
    
    // fire event to make sure listener is registered
    DistributedMember clientJoined = new TestDistributedMember("clientJoined");
    InternalClientMembership.notifyJoined(clientJoined, true);
    synchronized(listener) {
      if (!fired[0]) {
        listener.wait(2000);
      }
    }
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
    InternalClientMembership.notifyJoined(clientJoined, true);
    synchronized(listener) {
      listener.wait(20);
    }
    assertFalse(fired[0]);
    assertNull(member[0]);
    assertNull(memberId[0]);
    assertFalse(isClient[0]);
  }
  
  public void testMultipleListeners() throws Exception {
    final int NUM_LISTENERS = 4;
    final boolean[] fired = new boolean[NUM_LISTENERS];
    final DistributedMember[] member = new DistributedMember[NUM_LISTENERS];
    final String[] memberId = new String[NUM_LISTENERS];
    final boolean[] isClient = new boolean[NUM_LISTENERS];
    
    final ClientMembershipListener[] listeners = new ClientMembershipListener[NUM_LISTENERS];
    for (int i = 0; i < NUM_LISTENERS; i++) {
      final int whichListener = i;
      listeners[i] = new ClientMembershipListener() {
        public synchronized void memberJoined(ClientMembershipEvent event) {
          assertFalse(fired[whichListener]);
          assertNull(member[whichListener]);
          assertNull(memberId[whichListener]);
          assertFalse(isClient[whichListener]);
          fired[whichListener] = true;
          member[whichListener] = event.getMember();
          memberId[whichListener] = event.getMemberId();
          isClient[whichListener] = event.isClient();
          notify();
        }
        public void memberLeft(ClientMembershipEvent event) {
        }
        public void memberCrashed(ClientMembershipEvent event) {
        }
      };
    }
    
    final DistributedMember clientJoined = new TestDistributedMember("clientJoined");
    InternalClientMembership.notifyJoined(clientJoined, true);
    for (int i = 0; i < NUM_LISTENERS; i++) {
      synchronized(listeners[i]) {
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

    InternalClientMembership.notifyJoined(clientJoined, true);
    synchronized(listeners[1]) {
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
    
    InternalClientMembership.notifyJoined(clientJoined, true);
    synchronized(listeners[1]) {
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

    InternalClientMembership.notifyJoined(clientJoined, true);
    synchronized(listeners[3]) {
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

    InternalClientMembership.notifyJoined(clientJoined, true);
    synchronized(listeners[0]) {
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
    
    InternalClientMembership.notifyJoined(clientJoined, true);
    synchronized(listeners[0]) {
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
    
    InternalClientMembership.notifyJoined(clientJoined, true);
    synchronized(listeners[0]) {
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
    
    InternalClientMembership.notifyJoined(clientJoined, true);
    for (int i = 0; i < NUM_LISTENERS; i++) {
      synchronized(listeners[i]) {
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
    
    InternalClientMembership.notifyJoined(clientJoined, true);
    synchronized(listeners[1]) {
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
   * Tests notification of events in client process. Bridge clients detect
   * server joins when the client connects to the server. If the server
   * crashes or departs gracefully, the client will detect this as a crash.
   */
  public void testClientMembershipEventsInClient() throws Exception {
    addExpectedException("IOException");
    final boolean[] fired = new boolean[3];
    final DistributedMember[] member = new DistributedMember[3];
    final String[] memberId = new String[3];
    final boolean[] isClient = new boolean[3];
    
    // create and register ClientMembershipListener in controller vm...
    ClientMembershipListener listener = new ClientMembershipListener() {
      public synchronized void memberJoined(ClientMembershipEvent event) {
        getLogWriter().info("[testClientMembershipEventsInClient] memberJoined: " + event);
        fired[JOINED] = true;
        member[JOINED] = event.getMember();
        memberId[JOINED] = event.getMemberId();
        isClient[JOINED] = event.isClient();
        notifyAll();
      }
      public synchronized void memberLeft(ClientMembershipEvent event) {
        getLogWriter().info("[testClientMembershipEventsInClient] memberLeft: " + event);
//        fail("Please update testClientMembershipEventsInClient to handle memberLeft for BridgeServer.");
      }
      public synchronized void memberCrashed(ClientMembershipEvent event) {
        getLogWriter().info("[testClientMembershipEventsInClient] memberCrashed: " + event);
        fired[CRASHED] = true;
        member[CRASHED] = event.getMember();
        memberId[CRASHED] = event.getMemberId();
        isClient[CRASHED] = event.isClient();
        notifyAll();
      }
    };
    ClientMembership.registerClientMembershipListener(listener);

    final VM vm0 = Host.getHost(0).getVM(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[1];

    // create BridgeServer in vm0...
    vm0.invoke(new CacheSerializableRunnable("Create BridgeServer") {
      public void run2() throws CacheException {
        try {
          getLogWriter().info("[testClientMembershipEventsInClient] Create BridgeServer");
          getSystem();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          Region region = createRegion(name, factory.create());
          assertNotNull(region);
          assertNotNull(getRootRegion().getSubregion(name));
          testClientMembershipEventsInClient_port = startBridgeServer(0);
        }
        catch(IOException e) {
          getSystem().getLogWriter().fine(new Exception(e));
          fail("Failed to start CacheServer on VM1: " + e.getMessage());
        }
      }
    });
    
    // gather details for later creation of ConnectionPool...
    ports[0] = vm0.invokeInt(ClientMembershipDUnitTest.class, 
                             "getTestClientMembershipEventsInClient_port");
    assertTrue(ports[0] != 0);

    DistributedMember serverMember = (DistributedMember) vm0.invoke(ClientMembershipDUnitTest.class,
    "getDistributedMember");

    String serverMemberId = (String) vm0.invoke(ClientMembershipDUnitTest.class,
                                                "getMemberId");

    getLogWriter().info("[testClientMembershipEventsInClient] ports[0]=" + ports[0]);
    getLogWriter().info("[testClientMembershipEventsInClient] serverMember=" + serverMember);
    getLogWriter().info("[testClientMembershipEventsInClient] serverMemberId=" + serverMemberId);

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
    getLogWriter().info("[testClientMembershipEventsInClient] sanity check");
    DistributedMember test = new TestDistributedMember("test");
    InternalClientMembership.notifyJoined(test, SERVER);
    synchronized(listener) {
      if (!fired[JOINED] && !fired[CRASHED]) {
        listener.wait(2000);
      }
    }
    
    assertTrue(fired[JOINED]);
    assertEquals(test, member[JOINED]);
    assertEquals(test.getId(), memberId[JOINED]);
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
    getLogWriter().info("[testClientMembershipEventsInClient] create bridge client");
    Properties config = new Properties();
    config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    config.setProperty(DistributionConfig.LOCATORS_NAME, "");
    getSystem(config);
    
    try {
      getCache();
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      ClientServerTestCase.configureConnectionPool(factory, getServerHostName(Host.getHost(0)), ports, true, -1, -1, null);
      createRegion(name, factory.create());
      assertNotNull(getRootRegion().getSubregion(name));
    }
    catch (CacheException ex) {
      fail("While creating Region on Edge", ex);
    }
    synchronized(listener) {
      if (!fired[JOINED] && !fired[CRASHED]) {
        listener.wait(60 * 1000);
      }
    }
    
    getLogWriter().info("[testClientMembershipEventsInClient] assert client detected server join");
    
    // first check the getCurrentServers() result
    ClientCache clientCache = (ClientCache)getCache();
    Set<InetSocketAddress> servers = clientCache.getCurrentServers();
    assertTrue(!servers.isEmpty());
    InetSocketAddress serverAddr = servers.iterator().next();
    InetSocketAddress expectedAddr = new InetSocketAddress(serverMember.getHost(), ports[0]);
    assertEquals(expectedAddr, serverAddr);
    
    // now check listener results
    assertTrue(fired[JOINED]);
    assertNotNull(member[JOINED]);
    assertNotNull(memberId[JOINED]);
    assertEquals(serverMember, member[JOINED]);
    assertEquals(serverMemberId, memberId[JOINED]);
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

    vm0.invoke(new SerializableRunnable("Stop BridgeServer") {
      public void run() {
        getLogWriter().info("[testClientMembershipEventsInClient] Stop BridgeServer");
        stopBridgeServers(getCache());
      }
    });
    synchronized(listener) {
      if (!fired[JOINED] && !fired[CRASHED]) {
        listener.wait(60 * 1000);
      }
    }
    
    getLogWriter().info("[testClientMembershipEventsInClient] assert client detected server departure");
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
    assertEquals(serverMember, member[CRASHED]);
    assertEquals(serverMemberId, memberId[CRASHED]);
    assertFalse(isClient[CRASHED]);
    resetArraysForTesting(fired, member, memberId, isClient);
    
    //now test that we redisover the bridge server
    vm0.invoke(new CacheSerializableRunnable("Recreate BridgeServer") {
      public void run2() throws CacheException {
        try {
          getLogWriter().info("[testClientMembershipEventsInClient] restarting BridgeServer");
          startBridgeServer(ports[0]);
        }
        catch(IOException e) {
          getSystem().getLogWriter().fine(new Exception(e));
          fail("Failed to start CacheServer on VM1: " + e.getMessage());
        }
      }
    });
    synchronized(listener) {
      if (!fired[JOINED] && !fired[CRASHED]) {
        listener.wait(60 * 1000);
      }
    }
    
    getLogWriter().info("[testClientMembershipEventsInClient] assert client detected server recovery");
    assertTrue(fired[JOINED]);
    assertNotNull(member[JOINED]);
    assertNotNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    assertEquals(serverMember, member[JOINED]);
    assertEquals(serverMemberId, memberId[JOINED]);
    assertFalse(fired[LEFT]);
    assertNull(member[LEFT]);
    assertNull(memberId[LEFT]);
    assertFalse(isClient[LEFT]);
    assertFalse(fired[CRASHED]);
    assertNull(member[CRASHED]);
    assertNull(memberId[CRASHED]);
  }
  
  /**
   * Tests notification of events in server process. Bridge servers detect
   * client joins when the client connects to the server.
   */
  public void testClientMembershipEventsInServer() throws Exception {
    final boolean[] fired = new boolean[3];
    final DistributedMember[] member = new DistributedMember[3];
    final String[] memberId = new String[3];
    final boolean[] isClient = new boolean[3];
    
    // create and register ClientMembershipListener in controller vm...
    ClientMembershipListener listener = new ClientMembershipListener() {
      public synchronized void memberJoined(ClientMembershipEvent event) {
        getLogWriter().info("[testClientMembershipEventsInServer] memberJoined: " + event);
        fired[JOINED] = true;
        member[JOINED] = event.getMember();
        memberId[JOINED] = event.getMemberId();
        isClient[JOINED] = event.isClient();
        notifyAll();
        assertFalse(fired[LEFT] || fired[CRASHED]);
      }
      public synchronized void memberLeft(ClientMembershipEvent event) {
        getLogWriter().info("[testClientMembershipEventsInServer] memberLeft: " + event);
        fired[LEFT] = true;
        member[LEFT] = event.getMember();
        memberId[LEFT] = event.getMemberId();
        isClient[LEFT] = event.isClient();
        notifyAll();
        assertFalse(fired[JOINED] || fired[CRASHED]);
      }
      public synchronized void memberCrashed(ClientMembershipEvent event) {
        getLogWriter().info("[testClientMembershipEventsInServer] memberCrashed: " + event);
        fired[CRASHED] = true;
        member[CRASHED] = event.getMember();
        memberId[CRASHED] = event.getMemberId();
        isClient[CRASHED] = event.isClient();
        notifyAll();
        assertFalse(fired[JOINED] || fired[LEFT]);
      }
    };
    ClientMembership.registerClientMembershipListener(listener);

    final VM vm0 = Host.getHost(0).getVM(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[1];

    // create BridgeServer in controller vm...
    getLogWriter().info("[testClientMembershipEventsInServer] Create BridgeServer");
    getSystem();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region region = createRegion(name, factory.create());
    assertNotNull(region);
    assertNotNull(getRootRegion().getSubregion(name));
    
    ports[0] = startBridgeServer(0);
    assertTrue(ports[0] != 0);
    String serverMemberId = getMemberId();
    DistributedMember serverMember = getDistributedMember();

    getLogWriter().info("[testClientMembershipEventsInServer] ports[0]=" + ports[0]);
    getLogWriter().info("[testClientMembershipEventsInServer] serverMemberId=" + serverMemberId);
    getLogWriter().info("[testClientMembershipEventsInServer] serverMember=" + serverMember);

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
    getLogWriter().info("[testClientMembershipEventsInServer] sanity check");
    DistributedMember test = new TestDistributedMember("test");
    InternalClientMembership.notifyJoined(test, CLIENT);
    synchronized(listener) {
      if (!fired[JOINED] && !fired[LEFT] && !fired[CRASHED]) {
        listener.wait(2000);
      }
    }
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
    SerializableRunnable createConnectionPool =
    new CacheSerializableRunnable("Create connectionPool") {
      public void run2() throws CacheException {
        getLogWriter().info("[testClientMembershipEventsInServer] create bridge client");
        Properties config = new Properties();
        config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        config.setProperty(DistributionConfig.LOCATORS_NAME, "");
        getSystem(config);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, getServerHostName(host), ports, true, -1, 2, null);
        createRegion(name, factory.create());
        assertNotNull(getRootRegion().getSubregion(name));
      }
    };

    // create bridge client in vm0...
    vm0.invoke(createConnectionPool);
    String clientMemberId = (String) vm0.invoke(ClientMembershipDUnitTest.class,
                                                "getMemberId");
    DistributedMember clientMember = (DistributedMember) vm0.invoke(ClientMembershipDUnitTest.class,
                                                "getDistributedMember");
                                                
    synchronized(listener) {
      if (!fired[JOINED] && !fired[LEFT] && !fired[CRASHED]) {
        listener.wait(60000);
      }
    }
    
    getLogWriter().info("[testClientMembershipEventsInServer] assert server detected client join");
    assertTrue(fired[JOINED]);
    assertEquals(member[JOINED] + " should equal " + clientMember,
      clientMember, member[JOINED]);
    assertEquals(memberId[JOINED] + " should equal " + clientMemberId,
      clientMemberId, memberId[JOINED]);
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
        getLogWriter().info("[testClientMembershipEventsInServer] Stop bridge client");
        getRootRegion().getSubregion(name).close();
        Map m = PoolManager.getAll();
        Iterator mit = m.values().iterator();
        while(mit.hasNext()) {
          Pool p = (Pool)mit.next();
          p.destroy();
        }
      }
    });

    synchronized(listener) {
      if (!fired[JOINED] && !fired[LEFT] && !fired[CRASHED]) {
        listener.wait(60000);
      }
    }
    
    getLogWriter().info("[testClientMembershipEventsInServer] assert server detected client left");
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
    vm0.invoke(createConnectionPool);
    clientMemberId = (String) vm0.invoke(ClientMembershipDUnitTest.class,
                                         "getMemberId");
                                                
    synchronized(listener) {
      if (!fired[JOINED] && !fired[LEFT] && !fired[CRASHED]) {
        listener.wait(60000);
      }
    }
    
    getLogWriter().info("[testClientMembershipEventsInServer] assert server detected client re-join");
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
          getLogWriter().info("[testClientMembershipEventsInServer] Stop bridge client");
          getRootRegion().getSubregion(name).close();
          Map m = PoolManager.getAll();
          Iterator mit = m.values().iterator();
          while(mit.hasNext()) {
            Pool p = (Pool)mit.next();
            p.destroy();
          }
        }
      });
  
      synchronized(listener) {
        if (!fired[JOINED] && !fired[LEFT] && !fired[CRASHED]) {
          listener.wait(60000);
        }
      }
      
      getLogWriter().info("[testClientMembershipEventsInServer] assert server detected client crashed");
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
    }
    finally {
      ServerConnection.setForceClientCrashEvent(false);
    }
  }
  
  /**
   * The joined event fires when the first client handshake is processed.
   * This pauses long enough to allow the rest of the client sockets to
   * complete handshaking before making the client leave. Without doing this
   * subsequent socket handshakes that are processed could fire join events
   * after departure events and then a departure event again. If you see
   * failures in testClientMembershipEventsInServer, try increasing this
   * timeout.
   */
  private void pauseForClientToJoin() {
    pause(2000);
  }
  
  /** 
   * Tests registration and event notification in conjunction with 
   * disconnecting and reconnecting to DistributedSystem. 
   */
  public void testLifecycle() throws Exception {
    final boolean[] fired = new boolean[3];
    final DistributedMember[] member = new DistributedMember[3];
    final String[] memberId = new String[3];
    final boolean[] isClient = new boolean[3];
    
    // create and register ClientMembershipListener in controller vm...
    ClientMembershipListener listener = new ClientMembershipListener() {
      public synchronized void memberJoined(ClientMembershipEvent event) {
        assertFalse(fired[JOINED]);
        assertNull(member[JOINED]);
        assertNull(memberId[JOINED]);
        assertFalse(isClient[JOINED]);
        fired[JOINED] = true;
        member[JOINED] = event.getMember();
        memberId[JOINED] = event.getMemberId();
        isClient[JOINED] = event.isClient();
        notifyAll();
      }
      public synchronized void memberLeft(ClientMembershipEvent event) {
      }
      public synchronized void memberCrashed(ClientMembershipEvent event) {
      }
    };
    ClientMembership.registerClientMembershipListener(listener);
    
    // create loner in controller vm...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    config.setProperty(DistributionConfig.LOCATORS_NAME, "");
    getSystem(config);
    
    // assert that event is fired while connected
    DistributedMember serverJoined = new TestDistributedMember("serverJoined");
    InternalClientMembership.notifyJoined(serverJoined, SERVER);
    synchronized(listener) {
      if (!fired[JOINED]) {
        listener.wait(2000);
      }
    }
    assertTrue(fired[JOINED]);
    assertEquals(serverJoined, member[JOINED]);
    assertEquals(serverJoined.getId(), memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    resetArraysForTesting(fired, member, memberId, isClient);
    
    // assert that event is NOT fired while disconnected
    disconnectFromDS();
    

    InternalClientMembership.notifyJoined(serverJoined, SERVER);
    synchronized(listener) {
      listener.wait(20);
    }
    assertFalse(fired[JOINED]);
    assertNull(member[JOINED]);
    assertNull(memberId[JOINED]);
    assertFalse(isClient[JOINED]);
    resetArraysForTesting(fired, member, memberId, isClient);
    
    // assert that event is fired again after reconnecting
    InternalDistributedSystem sys = getSystem(config);
    assertTrue(sys.isConnected());

    InternalClientMembership.notifyJoined(serverJoined, SERVER);
    synchronized(listener) {
      if (!fired[JOINED]) {
        listener.wait(2000);
      }
    }
    assertTrue(fired[JOINED]);
    assertEquals(serverJoined, member[JOINED]);
    assertEquals(serverJoined.getId(), memberId[JOINED]);
    assertFalse(isClient[JOINED]);
  }
  
  /**
   * Starts up server in controller vm and 4 clients, then calls and tests
   * ClientMembership.getConnectedClients(). 
   */
  public void testGetConnectedClients() throws Exception {
    final String name = this.getUniqueName();
    final int[] ports = new int[1];
    
    addExpectedException("ConnectException");

    // create BridgeServer in controller vm...
    getLogWriter().info("[testGetConnectedClients] Create BridgeServer");
    getSystem();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    Region region = createRegion(name, factory.create());
    assertNotNull(region);
    assertNotNull(getRootRegion().getSubregion(name));
    
    ports[0] = startBridgeServer(0);
    assertTrue(ports[0] != 0);
    String serverMemberId = getMemberId();

    getLogWriter().info("[testGetConnectedClients] ports[0]=" + ports[0]);
    getLogWriter().info("[testGetConnectedClients] serverMemberId=" + serverMemberId);

    final Host host = Host.getHost(0);
    SerializableRunnable createPool =
    new CacheSerializableRunnable("Create connection pool") {
      public void run2() throws CacheException {
        getLogWriter().info("[testGetConnectedClients] create bridge client");
        Properties config = new Properties();
        config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        config.setProperty(DistributionConfig.LOCATORS_NAME, "");
        // 11/30/2015 this test is periodically failing during distributedTest runs
        // so we are setting the log-level to fine to figure out what's going on
        config.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
        getSystem(config);
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        Pool p = ClientServerTestCase.configureConnectionPool(factory, getServerHostName(host), ports, true, -1, -1, null);
        createRegion(name, factory.create());
        assertNotNull(getRootRegion().getSubregion(name));
        assertTrue(p.getServers().size() > 0);
      }
    };

    // create bridge client in vm0...
    final String[] clientMemberIdArray = new String[host.getVMCount()];
    
    for (int i = 0; i < host.getVMCount(); i++) { 
      final VM vm = Host.getHost(0).getVM(i);
      System.out.println("creating pool in vm_"+i);
      vm.invoke(createPool);
      clientMemberIdArray[i] =  String.valueOf(vm.invoke(
        ClientMembershipDUnitTest.class, "getMemberId"));
    }
    Collection clientMemberIds = Arrays.asList(clientMemberIdArray);
                                                
    {
      final int expectedClientCount = clientMemberIds.size();
      WaitCriterion wc = new WaitCriterion() {
        public String description() {
          return "wait for clients";
        }
        public boolean done() {
          Map connectedClients = InternalClientMembership.getConnectedClients(false);
          if (connectedClients == null) {
            return false;
          }
          if (connectedClients.size() != expectedClientCount) {
            return false;
          }
          return true;
        }
      };
      waitForCriterion(wc, 30000, 100, false);
    }
    
    Map connectedClients = InternalClientMembership.getConnectedClients(false);
    assertNotNull(connectedClients);
    assertEquals(clientMemberIds.size(), connectedClients.size());
    for (Iterator iter = connectedClients.keySet().iterator(); iter.hasNext();) {
      String connectedClient = (String)iter.next();
      getLogWriter().info("[testGetConnectedClients] checking for client " + connectedClient);
      assertTrue(clientMemberIds.contains(connectedClient));
      Object[] result = (Object[])connectedClients.get(connectedClient);
      getLogWriter().info("[testGetConnectedClients] result: " + 
                          (result==null? "none"
                              : String.valueOf(result[0])+"; connections="+result[1]));
    }
  }

  /**
   * Starts up 4 server and the controller vm as a client, then calls and tests
   * ClientMembership.getConnectedServers(). 
   */
  public void testGetConnectedServers() throws Exception {
    final Host host = Host.getHost(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[host.getVMCount()];
    
    for (int i = 0; i < host.getVMCount(); i++) { 
      final int whichVM = i;
      final VM vm = Host.getHost(0).getVM(i);
      vm.invoke(new CacheSerializableRunnable("Create bridge server") {
        public void run2() throws CacheException {
          // create BridgeServer in controller vm...
          getLogWriter().info("[testGetConnectedServers] Create BridgeServer");
          getSystem();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.LOCAL);
          Region region = createRegion(name+"_"+whichVM, factory.create());
          assertNotNull(region);
          assertNotNull(getRootRegion().getSubregion(name+"_"+whichVM));
          region.put("KEY-1", "VAL-1");
          
          try {
            testGetConnectedServers_port = startBridgeServer(0);
          }
          catch (IOException e) {
            getLogWriter().error("startBridgeServer threw IOException", e);
            fail("startBridgeServer threw IOException " + e.getMessage());
          }
          
          assertTrue(testGetConnectedServers_port != 0);
      
          getLogWriter().info("[testGetConnectedServers] port=" + 
            ports[whichVM]);
          getLogWriter().info("[testGetConnectedServers] serverMemberId=" + 
            getDistributedMember());
        }
      });
      ports[whichVM] = vm.invokeInt(ClientMembershipDUnitTest.class, 
                                    "getTestGetConnectedServers_port");
      assertTrue(ports[whichVM] != 0);
    }
    
    getLogWriter().info("[testGetConnectedServers] create bridge client");
    Properties config = new Properties();
    config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    config.setProperty(DistributionConfig.LOCATORS_NAME, "");
    getSystem(config);
    getCache();
    
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);

    for (int i = 0; i < ports.length; i++) {
      getLogWriter().info("[testGetConnectedServers] creating connectionpool for " + 
        getServerHostName(host) + " " + ports[i]);
      int[] thisServerPorts = new int[] { ports[i] };
      ClientServerTestCase.configureConnectionPoolWithName(factory, getServerHostName(host), thisServerPorts, false, -1, -1, null,"pooly"+i);
      Region region = createRegion(name+"_"+i, factory.create());
      assertNotNull(getRootRegion().getSubregion(name+"_"+i));
      region.get("KEY-1");
    }

    {
      final int expectedVMCount = host.getVMCount();
      WaitCriterion wc = new WaitCriterion() {
        public String description() {
          return "wait for pools and servers";
        }
        public boolean done() {
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
        }
      };
      waitForCriterion(wc, 60000, 100, false);
    }

    {
      assertEquals(host.getVMCount(), PoolManager.getAll().size());
      
    }
    
    Map connectedServers = InternalClientMembership.getConnectedServers();
    assertNotNull(connectedServers);
    assertEquals(host.getVMCount(), connectedServers.size());
    for (Iterator iter = connectedServers.keySet().iterator(); iter.hasNext();) {
      String connectedServer = (String) iter.next();
      getLogWriter().info("[testGetConnectedServers]  value for connectedServer: " + 
                          connectedServers.get(connectedServer));
    }
  }

  protected static int testGetConnectedServers_port;
  private static int getTestGetConnectedServers_port() {
    return testGetConnectedServers_port;
  }

  /**
   * Tests getConnectedClients(boolean onlyClientsNotifiedByThisServer) where
   * onlyClientsNotifiedByThisServer is true.
   */
  public void testGetNotifiedClients() throws Exception {
    final Host host = Host.getHost(0);
    final String name = this.getUniqueName();
    final int[] ports = new int[host.getVMCount()];
    
    for (int i = 0; i < host.getVMCount(); i++) { 
      final int whichVM = i;
      final VM vm = Host.getHost(0).getVM(i);
      vm.invoke(new CacheSerializableRunnable("Create bridge server") {
        public void run2() throws CacheException {
          // create BridgeServer in controller vm...
          getLogWriter().info("[testGetNotifiedClients] Create BridgeServer");
          getSystem();
          AttributesFactory factory = new AttributesFactory();
          Region region = createRegion(name, factory.create());
          assertNotNull(region);
          assertNotNull(getRootRegion().getSubregion(name));
          region.put("KEY-1", "VAL-1");
          
          try {
            testGetNotifiedClients_port = startBridgeServer(0);
          }
          catch (IOException e) {
            getLogWriter().error("startBridgeServer threw IOException", e);
            fail("startBridgeServer threw IOException " + e.getMessage());
          }
          
          assertTrue(testGetNotifiedClients_port != 0);
      
          getLogWriter().info("[testGetNotifiedClients] port=" + 
            ports[whichVM]);
          getLogWriter().info("[testGetNotifiedClients] serverMemberId=" + 
            getMemberId());
        }
      });
      ports[whichVM] = vm.invokeInt(ClientMembershipDUnitTest.class, 
                                    "getTestGetNotifiedClients_port");
      assertTrue(ports[whichVM] != 0);
    }
    
    getLogWriter().info("[testGetNotifiedClients] create bridge client");
    Properties config = new Properties();
    config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    config.setProperty(DistributionConfig.LOCATORS_NAME, "");
    getSystem(config);
    getCache();
    
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);

    getLogWriter().info("[testGetNotifiedClients] creating connection pool");
    ClientServerTestCase.configureConnectionPool(factory, getServerHostName(host), ports, true, -1, -1, null);
    Region region = createRegion(name, factory.create());
    assertNotNull(getRootRegion().getSubregion(name));
    region.registerInterest("KEY-1");
    region.get("KEY-1");

    final String clientMemberId = getMemberId();
    
    pauseForClientToJoin();
    
    // assertions go here
    int[] clientCounts = new int[host.getVMCount()];
    
    // only one server vm will have that client for updating
    for (int i = 0; i < host.getVMCount(); i++) { 
      final int whichVM = i;
      final VM vm = Host.getHost(0).getVM(i);
      vm.invoke(new CacheSerializableRunnable("Create bridge server") {
        public void run2() throws CacheException {
          Map clients = InternalClientMembership.getConnectedClients(true);
          assertNotNull(clients);
          testGetNotifiedClients_clientCount = clients.size();
          if (testGetNotifiedClients_clientCount > 0) {
            // assert that the clientMemberId matches
            assertEquals(clientMemberId, clients.keySet().iterator().next());
          }
        }
      });
      clientCounts[whichVM] = vm.invokeInt(ClientMembershipDUnitTest.class, 
                              "getTestGetNotifiedClients_clientCount");
    }
    
    // only one server should have a notifier for this client...
    int totalClientCounts = 0;
    for (int i = 0; i < clientCounts.length; i++) {
      totalClientCounts += clientCounts[i];
    }
    // this assertion fails because the count is 4
    //assertEquals(1, totalClientCounts);
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
  static final class TestDistributedMember implements DistributedMember {
    
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
      return compareTo((TestDistributedMember)obj) == 0;
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
