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
package com.gemstone.gemfire.distributed.internal;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests the functionality of the LocatorLoadSnapshot, which
 * is the data structure that is used in the locator to compare
 * the load between multiple servers.
 * 
 */
@Category(UnitTest.class)
public class LocatorLoadSnapshotJUnitTest {
  
  /**
   * Test to make sure than an empty snapshot returns the 
   * correct values.
   */
  @Test
  public void testEmptySnapshot() {
    LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    assertNull(sn.getServerForConnection("group", Collections.EMPTY_SET));
    assertNull(sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(Collections.EMPTY_LIST, sn.getServersForQueue(null, Collections.EMPTY_SET, 5));
  }
  
  /**
   * Test a snapshot with two servers. The servers
   * are initialized with unequal load, and then
   * and then we test that after several requests, the
   * load balancer starts sending connections to the second
   * server.
   * 
   */
  @Test
  public void testTwoServers() {
    LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    ServerLocation l1 = new ServerLocation("localhost", 1);
    ServerLocation l2 = new ServerLocation("localhost", 2);
    ServerLoad ld1 = new ServerLoad(3, 1, 1.01f, 1);
    ServerLoad ld2 = new ServerLoad(5, .2f, 1f, .2f);
    sn.addServer(l1, new String[0], ld1);
    sn.addServer(l2, new String[0], ld2);
    
    HashMap expectedLoad = new HashMap();
    expectedLoad.put(l1, ld1);
    expectedLoad.put(l2, ld2);
    assertEquals(expectedLoad, sn.getLoadMap());
    
    assertNull(sn.getServerForConnection("group", Collections.EMPTY_SET));
    assertEquals(Collections.EMPTY_LIST, sn.getServersForQueue("group", Collections.EMPTY_SET, 5));
    
    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    //the load should be equal here, so we don't know which server to expect
    sn.getServerForConnection(null, Collections.EMPTY_SET);
    sn.getServerForConnection(null, Collections.EMPTY_SET);
    
    assertEquals(l2, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection(null, Collections.EMPTY_SET));
    
    assertEquals(Collections.singletonList(l2), sn.getServersForQueue(null, Collections.EMPTY_SET, 1));
    assertEquals(Collections.singletonList(l1), sn.getServersForQueue(null, Collections.EMPTY_SET, 1));
    assertEquals(Collections.singletonList(l2), sn.getServersForQueue(null, Collections.EMPTY_SET, 1));
    
    assertEquals(Arrays.asList(new ServerLocation[] {l2, l1} ), sn.getServersForQueue(null, Collections.EMPTY_SET, 5));
    assertEquals(Arrays.asList(new ServerLocation[] {l2, l1} ), sn.getServersForQueue(null, Collections.EMPTY_SET, -1));
  }
  
  /**
   * Test the updateLoad method. The snapshot should use the new
   * load when choosing a server.
   */
  @Test
  public void testUpdateLoad() {
    LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    ServerLocation l1 = new ServerLocation("localhost", 1);
    ServerLocation l2 = new ServerLocation("localhost", 2);
    sn.addServer(l1, new String[0], new ServerLoad(1, 1, 1, 1));
    sn.addServer(l2, new String[0], new ServerLoad(100, .2f, 1, .2f));
    
    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    sn.updateLoad(l1, new ServerLoad(200, 1, 1, 1));
    assertEquals(l2, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection(null, Collections.EMPTY_SET));
  }
  
  /**
   * Test that we can remove a server from the snapshot. It should not suggest
   * that server after it has been removed.
   */
  @Test
  public void testRemoveServer() {
    LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    ServerLocation l1 = new ServerLocation("localhost", 1);
    ServerLocation l2 = new ServerLocation("localhost", 2);
    sn.addServer(l1, new String[0], new ServerLoad(1, 1, 1, 1));
    sn.addServer(l2, new String[0], new ServerLoad(100, .2f, 10, .2f));
    
    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(Arrays.asList(new ServerLocation[] {l1, l2} ), sn.getServersForQueue(null, Collections.EMPTY_SET, -1));
    sn.removeServer(l1);
    assertEquals(l2, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(Collections.singletonList(l2), sn.getServersForQueue(null, Collections.EMPTY_SET, -1));
  }
  
  /**
   * Test of server groups. Make sure that the snapshot returns only servers from the correct
   * group. 
   */
  @Test
  public void testGroups() {
    LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    ServerLocation l1 = new ServerLocation("localhost", 1);
    ServerLocation l2 = new ServerLocation("localhost", 2);
    sn.addServer(l1, new String[] {"a", "b"}, new ServerLoad(1, 1, 1, 1));
    sn.addServer(l2, new String[]{"b", "c"}, new ServerLoad(1, 1, 1, 1));
    assertNotNull(sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l1, sn.getServerForConnection("a", Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection("c", Collections.EMPTY_SET));
    sn.updateLoad(l1, new ServerLoad(10,1,1,1));
    assertEquals(l2, sn.getServerForConnection("b", Collections.EMPTY_SET));
    sn.updateLoad(l2, new ServerLoad(100,1,1,1));
    assertEquals(l1, sn.getServerForConnection("b", Collections.EMPTY_SET));
    assertEquals(Arrays.asList(new ServerLocation[] {l1} ), sn.getServersForQueue("a", Collections.EMPTY_SET, -1));
    assertEquals(Arrays.asList(new ServerLocation[] {l2} ), sn.getServersForQueue("c", Collections.EMPTY_SET, -1));
    assertEquals(Arrays.asList(new ServerLocation[] {l1, l2} ), sn.getServersForQueue("b", Collections.EMPTY_SET, -1));
    assertEquals(Arrays.asList(new ServerLocation[] {l1, l2} ), sn.getServersForQueue(null, Collections.EMPTY_SET, -1));
    assertEquals(Arrays.asList(new ServerLocation[] {l1, l2} ), sn.getServersForQueue("b", Collections.EMPTY_SET, 5));
    
    sn.removeServer(l1);
    assertEquals(l2, sn.getServerForConnection("b", Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection("b", Collections.EMPTY_SET));
    assertNull(sn.getServerForConnection("a", Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection("c", Collections.EMPTY_SET));
    assertEquals(Arrays.asList(new ServerLocation[] {} ), sn.getServersForQueue("a", Collections.EMPTY_SET, -1));
    assertEquals(Arrays.asList(new ServerLocation[] {l2} ), sn.getServersForQueue("b", Collections.EMPTY_SET, 5));
  }
  
  /**
   * Test to make sure that we balancing three
   * servers with interecting groups correctly.
   */
  @Test
  public void testInterectingGroups() {
    LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    ServerLocation l1 = new ServerLocation("localhost", 1);
    ServerLocation l2 = new ServerLocation("localhost", 2);
    ServerLocation l3 = new ServerLocation("localhost", 3);
    sn.addServer(l1, new String[] {"a", }, new ServerLoad(0, 1, 0, 1));
    sn.addServer(l2, new String[]{"a", "b"}, new ServerLoad(0, 1, 0, 1));
    sn.addServer(l3, new String[]{"b"}, new ServerLoad(0, 1, 0, 1));
    
    //Test with interleaving requests for either group
    for(int i = 0; i < 60; i++) {
      ServerLocation l = sn.getServerForConnection("a", Collections.EMPTY_SET);
      assertTrue(l1.equals(l) || l2.equals(l));
      l = sn.getServerForConnection("b", Collections.EMPTY_SET);
      assertTrue(l2.equals(l) || l3.equals(l));
    }
    
    Map expected = new HashMap();
    ServerLoad expectedLoad = new ServerLoad(40f, 1f, 0f, 1f);
    expected.put(l1, expectedLoad);
    expected.put(l2, expectedLoad);
    expected.put(l3, expectedLoad);
    assertEquals(expected, sn.getLoadMap());
    
    sn.updateLoad(l1, new ServerLoad(0,1,0,1));
    sn.updateLoad(l2, new ServerLoad(0,1,0,1));
    sn.updateLoad(l3, new ServerLoad(0,1,0,1));
    
    
    //Now do the same test, but make all the requests for one group first,
    //then the second group.
    for(int i = 0; i < 60; i++) {
      ServerLocation l = sn.getServerForConnection("a", Collections.EMPTY_SET);
      assertTrue(l1.equals(l) || l2.equals(l));
    }
    
    expected = new HashMap();
    expected.put(l1, new ServerLoad(30f, 1f, 0f, 1f));
    expected.put(l2, new ServerLoad(30f, 1f, 0f, 1f));
    expected.put(l3, new ServerLoad(0f, 1f, 0f, 1f));
    assertEquals(expected, sn.getLoadMap());
    
    for(int i = 0; i < 60; i++) {
      ServerLocation l = sn.getServerForConnection("b", Collections.EMPTY_SET);
      assertTrue(l2.equals(l) || l3.equals(l));
    }
    
    //The load can't be completely balanced, because
    //We already had 30 connections from group a on server l2.
    //But we expect that l3 should have received most of the connections
    //for group b, because it started out with 0.
    expected = new HashMap();
    expected.put(l1, new ServerLoad(30f, 1f, 0f, 1f));
    expected.put(l2, new ServerLoad(45f, 1f, 0f, 1f));
    expected.put(l3, new ServerLoad(45f, 1f, 0f, 1f));
    assertEquals(expected, sn.getLoadMap());
    
  }
  
  /**
   * Test that we can specify a list of servers to exclude and the snapshot honors the request when
   * picking the best server.
   */
  @Test
  public void testExcludes() {
    LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    ServerLocation l1 = new ServerLocation("localhost", 1);
    ServerLocation l2 = new ServerLocation("localhost", 2);
    sn.addServer(l1, new String[0], new ServerLoad(1, 1, 1, 1));
    sn.addServer(l2, new String[0], new ServerLoad(100, 1, 100, 1));
    
    HashSet excludeAll = new HashSet();
    excludeAll.add(l1);
    excludeAll.add(l2);
    
    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection(null, Collections.singleton(l1)));
    
    assertEquals(null, sn.getServerForConnection(null, excludeAll));
    assertEquals(Arrays.asList(new ServerLocation[] {l2} ), sn.getServersForQueue(null, Collections.singleton(l1), 3));
    
    assertEquals(Arrays.asList(new ServerLocation[] {} ), sn.getServersForQueue(null, excludeAll, 3));
  }
  
  /**
   * A basic test of concurrent functionality. Starts a number of
   * threads making requests and expects the load to be balanced between
   * three servers.
   * @throws InterruptedException
   */
  @Test
  public void testConcurrentBalancing() throws InterruptedException {
    int NUM_THREADS = 50;
    final int NUM_REQUESTS = 10000;
    int ALLOWED_THRESHOLD = 50; //We should never be off by more than
    //the number of concurrent threads.
    
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    
    int initialLoad1 = (int) (Math.random() * (NUM_REQUESTS / 2));
    int initialLoad2 = (int) (Math.random() * (NUM_REQUESTS / 2));
    int initialLoad3 = (int) (Math.random() * (NUM_REQUESTS / 2));
    
    sn.addServer(l1, new String[0], new ServerLoad(initialLoad1, 1, 0, 1));
    sn.addServer(l2, new String[0], new ServerLoad(initialLoad2, 1, 0, 1));
    sn.addServer(l3, new String[0], new ServerLoad(initialLoad3, 1, 0, 1));
    
    final Map loadCounts = new HashMap();
    loadCounts.put(l1, new AtomicInteger(initialLoad1));
    loadCounts.put(l2, new AtomicInteger(initialLoad2));
    loadCounts.put(l3, new AtomicInteger(initialLoad3));
    
    Thread[] threads = new Thread[NUM_THREADS];
//    final Object lock = new Object();
    for(int i =0; i < NUM_THREADS; i++) {
      threads[i] = new Thread("Thread-" + i) {
        public void run() {
          for(int ii = 0; ii < NUM_REQUESTS; ii++) {
            ServerLocation location;
//            synchronized(lock) {
              location = sn.getServerForConnection(null, Collections.EMPTY_SET);
//            }
            AtomicInteger count = (AtomicInteger) loadCounts.get(location);
            count.incrementAndGet();
          }
        }
      };
    }
    
    for(int i =0; i < NUM_THREADS; i++) {
      threads[i].start();
    }
    
    for(int i =0; i < NUM_THREADS; i++) {
      Thread t = threads[i];
      long ms = 30 * 1000;
      t.join(30 * 1000);
      if (t.isAlive()) {
        for(int j =0; j < NUM_THREADS; j++) {
          threads[j].interrupt();
        }
        fail("Thread did not terminate after " + ms + " ms: " + t);
      }
    }
    
    double expectedPerServer = ( initialLoad1 + initialLoad2 + initialLoad3 + 
              NUM_REQUESTS * NUM_THREADS) / (double) loadCounts.size();
//    for(Iterator itr = loadCounts.entrySet().iterator(); itr.hasNext(); ) {
//      Map.Entry entry = (Entry) itr.next();
//      ServerLocation location = (ServerLocation) entry.getKey();
//      AI count= (AI) entry.getValue();
//    }
    
    for(Iterator itr = loadCounts.entrySet().iterator(); itr.hasNext(); ) {
      Map.Entry entry = (Entry) itr.next();
      ServerLocation location = (ServerLocation) entry.getKey();
      AtomicInteger count= (AtomicInteger) entry.getValue();
      int difference = (int) Math.abs(count.get() - expectedPerServer);
      assertTrue("Count " + count + " for server " + location + " is not within " + ALLOWED_THRESHOLD + " of " + expectedPerServer, difference < ALLOWED_THRESHOLD);
    }
  }
  
  @Test
  public void testAreBalanced() {
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    assertTrue(sn.hasBalancedConnections(null));
    assertTrue(sn.hasBalancedConnections("a"));
    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    
    sn.addServer(l1, new String[] {"a"}, new ServerLoad(0, 1, 0, 1));
    sn.addServer(l2, new String[] {"a", "b"}, new ServerLoad(0, 1, 0, 1));
    sn.addServer(l3, new String[] {"b"}, new ServerLoad(0, 1, 0, 1));
    
    assertTrue(sn.hasBalancedConnections(null));
    assertTrue(sn.hasBalancedConnections("a"));
    assertTrue(sn.hasBalancedConnections("b"));
    
    sn.updateLoad(l1, new ServerLoad(1,1,0,1));
    assertTrue(sn.hasBalancedConnections(null));
    assertTrue(sn.hasBalancedConnections("a"));
    assertTrue(sn.hasBalancedConnections("b"));
    
    sn.updateLoad(l2, new ServerLoad(2,1,0,1));
    assertFalse(sn.hasBalancedConnections(null));
    assertTrue(sn.hasBalancedConnections("a"));
    assertFalse(sn.hasBalancedConnections("b"));
  }
  
  public void _test2() { // delete this method?
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    sn.addServer(new ServerLocation("hs20h.gemstone.com",28543), new String[0], new ServerLoad(0.0f, 0.00125f, 0.0f, 1.0f));
    sn.addServer(new ServerLocation("hs20l.gemstone.com",22385), new String[0], new ServerLoad(0.0f, 0.00125f, 0.0f, 1.0f));
    sn.addServer(new ServerLocation("hs20n.gemstone.com",23482), new String[0], new ServerLoad(0.0f, 0.00125f, 0.0f, 1.0f));
    sn.addServer(new ServerLocation("hs20m.gemstone.com",23429), new String[0], new ServerLoad(0.0f, 0.00125f, 0.0f, 1.0f));
    sn.addServer(new ServerLocation("hs20e.gemstone.com",20154), new String[0],new ServerLoad(0.0f, 0.00125f, 0.0f, 1.0f));
    sn.addServer(new ServerLocation("hs20j.gemstone.com",24273), new String[0],new ServerLoad(0.0f, 0.00125f, 0.0f, 1.0f));
    sn.addServer(new ServerLocation("hs20g.gemstone.com",27125), new String[0],new ServerLoad(0.0f, 0.00125f, 0.0f, 1.0f));
    sn.addServer(new ServerLocation("hs20i.gemstone.com",25201), new String[0],new ServerLoad(0.0f, 0.00125f, 0.0f, 1.0f));
    sn.addServer(new ServerLocation("hs20k.gemstone.com",23711), new String[0],new ServerLoad(0.0f, 0.00125f, 0.0f, 1.0f));
    sn.addServer(new ServerLocation("hs20f.gemstone.com",21025), new String[0],new ServerLoad(0.0f, 0.00125f, 0.0f, 1.0f));
  }

}
