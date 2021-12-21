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
package org.apache.geode.distributed.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Tests the functionality of the LocatorLoadSnapshot, which is the data structure that is used in
 * the locator to compare the load between multiple servers.
 */
@Category({MembershipTest.class})
public class LocatorLoadSnapshotJUnitTest {

  final int LOAD_POLL_INTERVAL = 30000;

  /**
   * Test to make sure than an empty snapshot returns the correct values.
   */
  @Test
  public void testEmptySnapshot() {
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    assertNull(sn.getServerForConnection("group", Collections.EMPTY_SET));
    assertNull(sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(Collections.EMPTY_LIST, sn.getServersForQueue(null, Collections.EMPTY_SET, 5));
  }

  /**
   * Test a snapshot with two servers. The servers are initialized with unequal load, and then and
   * then we test that after several requests, the load balancer starts sending connections to the
   * second server.
   */
  @Test
  public void testTwoServers() {
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLoad ld1 = new ServerLoad(3, 1, 1.01f, 1);
    final ServerLoad ld2 = new ServerLoad(5, .2f, 1f, .2f);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    sn.addServer(l1, uniqueId1, new String[0], ld1, LOAD_POLL_INTERVAL);
    sn.addServer(l2, uniqueId2, new String[0], ld2, LOAD_POLL_INTERVAL);

    HashMap expectedLoad = new HashMap();
    expectedLoad.put(l1, ld1);
    expectedLoad.put(l2, ld2);
    assertEquals(expectedLoad, sn.getLoadMap());

    assertNull(sn.getServerForConnection("group", Collections.EMPTY_SET));
    assertEquals(Collections.EMPTY_LIST, sn.getServersForQueue("group", Collections.EMPTY_SET, 5));

    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    // the load should be equal here, so we don't know which server to expect
    sn.getServerForConnection(null, Collections.EMPTY_SET);
    sn.getServerForConnection(null, Collections.EMPTY_SET);

    assertEquals(l2, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection(null, Collections.EMPTY_SET));

    assertEquals(Collections.singletonList(l2),
        sn.getServersForQueue(null, Collections.EMPTY_SET, 1));
    assertEquals(Collections.singletonList(l1),
        sn.getServersForQueue(null, Collections.EMPTY_SET, 1));
    assertEquals(Collections.singletonList(l2),
        sn.getServersForQueue(null, Collections.EMPTY_SET, 1));

    assertEquals(Arrays.asList(l2, l1),
        sn.getServersForQueue(null, Collections.EMPTY_SET, 5));
    assertEquals(Arrays.asList(l2, l1),
        sn.getServersForQueue(null, Collections.EMPTY_SET, -1));
  }

  /**
   * Test the updateLoad method. The snapshot should use the new load when choosing a server.
   */
  @Test
  public void testUpdateLoad() {
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    sn.addServer(l1, uniqueId1, new String[0], new ServerLoad(1, 1, 1, 1), LOAD_POLL_INTERVAL);
    sn.addServer(l2, uniqueId2, new String[0], new ServerLoad(100, .2f, 1, .2f),
        LOAD_POLL_INTERVAL);

    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    sn.updateLoad(l1, uniqueId1, new ServerLoad(200, 1, 1, 1));
    assertEquals(l2, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection(null, Collections.EMPTY_SET));
  }

  /**
   * Test that we can remove a server from the snapshot. It should not suggest that server after it
   * has been removed.
   */
  @Test
  public void testRemoveServer() {
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    sn.addServer(l1, uniqueId1, new String[0], new ServerLoad(1, 1, 1, 1), LOAD_POLL_INTERVAL);
    sn.addServer(l2, uniqueId2, new String[0], new ServerLoad(100, .2f, 10, .2f),
        LOAD_POLL_INTERVAL);

    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(Arrays.asList(l1, l2),
        sn.getServersForQueue(null, Collections.EMPTY_SET, -1));
    sn.removeServer(l1, uniqueId1);
    assertEquals(l2, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(Collections.singletonList(l2),
        sn.getServersForQueue(null, Collections.EMPTY_SET, -1));
  }

  /**
   * Test of server groups. Make sure that the snapshot returns only servers from the correct group.
   */
  @Test
  public void testGroups() {
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    sn.addServer(l1, uniqueId1, new String[] {"a", "b"}, new ServerLoad(1, 1, 1, 1),
        LOAD_POLL_INTERVAL);
    sn.addServer(l2, uniqueId2, new String[] {"b", "c"}, new ServerLoad(1, 1, 1, 1),
        LOAD_POLL_INTERVAL);
    assertNotNull(sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l1, sn.getServerForConnection("a", Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection("c", Collections.EMPTY_SET));
    sn.updateLoad(l1, uniqueId1, new ServerLoad(10, 1, 1, 1));
    assertEquals(l2, sn.getServerForConnection("b", Collections.EMPTY_SET));
    sn.updateLoad(l2, uniqueId2, new ServerLoad(100, 1, 1, 1));
    assertEquals(l1, sn.getServerForConnection("b", Collections.EMPTY_SET));
    assertEquals(Arrays.asList(l1),
        sn.getServersForQueue("a", Collections.EMPTY_SET, -1));
    assertEquals(Arrays.asList(l2),
        sn.getServersForQueue("c", Collections.EMPTY_SET, -1));
    assertEquals(Arrays.asList(l1, l2),
        sn.getServersForQueue("b", Collections.EMPTY_SET, -1));
    assertEquals(Arrays.asList(l1, l2),
        sn.getServersForQueue(null, Collections.EMPTY_SET, -1));
    assertEquals(Arrays.asList(l1, l2),
        sn.getServersForQueue("b", Collections.EMPTY_SET, 5));

    sn.removeServer(l1, uniqueId1);
    assertEquals(l2, sn.getServerForConnection("b", Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection("b", Collections.EMPTY_SET));
    assertNull(sn.getServerForConnection("a", Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection("c", Collections.EMPTY_SET));
    assertEquals(Arrays.asList(),
        sn.getServersForQueue("a", Collections.EMPTY_SET, -1));
    assertEquals(Arrays.asList(l2),
        sn.getServersForQueue("b", Collections.EMPTY_SET, 5));
  }

  /**
   * Test to make sure that we balancing three servers with interecting groups correctly.
   */
  @Test
  public void testIntersectingGroups() {
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();
    sn.addServer(l1, uniqueId1, new String[] {"a",}, new ServerLoad(0, 1, 0, 1),
        LOAD_POLL_INTERVAL);
    sn.addServer(l2, uniqueId2, new String[] {"a", "b"}, new ServerLoad(0, 1, 0, 1),
        LOAD_POLL_INTERVAL);
    sn.addServer(l3, uniqueId3, new String[] {"b"}, new ServerLoad(0, 1, 0, 1), LOAD_POLL_INTERVAL);

    // Test with interleaving requests for either group
    for (int i = 0; i < 60; i++) {
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

    sn.updateLoad(l1, uniqueId1, new ServerLoad(0, 1, 0, 1));
    sn.updateLoad(l2, uniqueId2, new ServerLoad(0, 1, 0, 1));
    sn.updateLoad(l3, uniqueId3, new ServerLoad(0, 1, 0, 1));


    // Now do the same test, but make all the requests for one group first,
    // then the second group.
    for (int i = 0; i < 60; i++) {
      ServerLocation l = sn.getServerForConnection("a", Collections.EMPTY_SET);
      assertTrue(l1.equals(l) || l2.equals(l));
    }

    expected = new HashMap();
    expected.put(l1, new ServerLoad(30f, 1f, 0f, 1f));
    expected.put(l2, new ServerLoad(30f, 1f, 0f, 1f));
    expected.put(l3, new ServerLoad(0f, 1f, 0f, 1f));
    assertEquals(expected, sn.getLoadMap());

    for (int i = 0; i < 60; i++) {
      ServerLocation l = sn.getServerForConnection("b", Collections.EMPTY_SET);
      assertTrue(l2.equals(l) || l3.equals(l));
    }

    // The load can't be completely balanced, because
    // We already had 30 connections from group a on server l2.
    // But we expect that l3 should have received most of the connections
    // for group b, because it started out with 0.
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
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    sn.addServer(l1, uniqueId1, new String[0], new ServerLoad(1, 1, 1, 1), LOAD_POLL_INTERVAL);
    sn.addServer(l2, uniqueId2, new String[0], new ServerLoad(100, 1, 100, 1), LOAD_POLL_INTERVAL);

    HashSet excludeAll = new HashSet();
    excludeAll.add(l1);
    excludeAll.add(l2);

    assertEquals(l1, sn.getServerForConnection(null, Collections.EMPTY_SET));
    assertEquals(l2, sn.getServerForConnection(null, Collections.singleton(l1)));

    assertEquals(null, sn.getServerForConnection(null, excludeAll));
    assertEquals(Arrays.asList(l2),
        sn.getServersForQueue(null, Collections.singleton(l1), 3));

    assertEquals(Arrays.asList(),
        sn.getServersForQueue(null, excludeAll, 3));
  }

  @Test
  public void testAreBalanced() {
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    assertTrue(sn.hasBalancedConnections(null));
    assertTrue(sn.hasBalancedConnections("a"));
    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();
    sn.addServer(l1, uniqueId1, new String[] {"a"}, new ServerLoad(0, 1, 0, 1), LOAD_POLL_INTERVAL);
    sn.addServer(l2, uniqueId2, new String[] {"a", "b"}, new ServerLoad(0, 1, 0, 1),
        LOAD_POLL_INTERVAL);
    sn.addServer(l3, uniqueId3, new String[] {"b"}, new ServerLoad(0, 1, 0, 1), LOAD_POLL_INTERVAL);

    assertTrue(sn.hasBalancedConnections(null));
    assertTrue(sn.hasBalancedConnections("a"));
    assertTrue(sn.hasBalancedConnections("b"));

    sn.updateLoad(l1, uniqueId1, new ServerLoad(1, 1, 0, 1));
    assertTrue(sn.hasBalancedConnections(null));
    assertTrue(sn.hasBalancedConnections("a"));
    assertTrue(sn.hasBalancedConnections("b"));

    sn.updateLoad(l2, uniqueId2, new ServerLoad(2, 1, 0, 1));
    assertFalse(sn.hasBalancedConnections(null));
    assertTrue(sn.hasBalancedConnections("a"));
    assertFalse(sn.hasBalancedConnections("b"));
  }

  @Test
  public void testThatReplacementServerIsSelected() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();
    float defaultLoadImbalanceThreshold = LocatorLoadSnapshot.DEFAULT_LOAD_IMBALANCE_THRESHOLD;

    float l1ConnectionLoad = 50 + defaultLoadImbalanceThreshold;
    float l2ConnectionLoad = 50;
    float l3ConnectionLoad = 50 - defaultLoadImbalanceThreshold;
    loadSnapshot.addServer(l1, uniqueId1, new String[] {"a"},
        new ServerLoad(l1ConnectionLoad, 1, 0, 1),
        LOAD_POLL_INTERVAL);
    loadSnapshot.addServer(l2, uniqueId2, new String[] {"a", "b"},
        new ServerLoad(l2ConnectionLoad, 1, 0, 1), LOAD_POLL_INTERVAL);
    loadSnapshot.addServer(l3, uniqueId3, new String[] {"b"},
        new ServerLoad(l3ConnectionLoad, 1, 0, 1),
        LOAD_POLL_INTERVAL);

    // a new server should be selected until the load-imbalance-threshold is reached
    ServerLocation newServer = null;
    do {
      newServer = loadSnapshot.getReplacementServerForConnection(l1, "", Collections.EMPTY_SET);
      if (newServer == l3) {
        // the threshold check should have initiated client rebalancing
        assertTrue(loadSnapshot.isRebalancing());
      }
    } while (newServer == l3);

    // once balance is achieved we should have received the same server and
    // rebalancing should have ended
    assertEquals(l1, newServer);
    assertFalse(loadSnapshot.isRebalancing());

    // all load snapshots should now be balanced
    Map<ServerLocation, ServerLoad> loadMap = loadSnapshot.getLoadMap();
    ServerLoad l1Load = loadMap.get(l1);
    assertEquals(50, l1Load.getConnectionLoad(), 0.01);
    ServerLoad l2Load = loadMap.get(l2);
    assertEquals(50, l1Load.getConnectionLoad(), 0.01);
    ServerLoad l3Load = loadMap.get(l3);
    assertEquals(50, l3Load.getConnectionLoad(), 0.01);
  }

  @Test
  public void testThatReplacementServerIsNotSelectedIfThresholdNotReached() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();
    float defaultLoadImbalanceThreshold = LocatorLoadSnapshot.DEFAULT_LOAD_IMBALANCE_THRESHOLD;

    float l1ConnectionLoad = 50 + defaultLoadImbalanceThreshold - 1;
    float l2ConnectionLoad = 50;
    float l3ConnectionLoad = 50 + (defaultLoadImbalanceThreshold / 2);
    loadSnapshot.addServer(l1, uniqueId1, new String[] {"a"},
        new ServerLoad(l1ConnectionLoad, 1, 0, 1),
        LOAD_POLL_INTERVAL);
    loadSnapshot.addServer(l2, uniqueId2, new String[] {"a", "b"},
        new ServerLoad(l2ConnectionLoad, 1, 0, 1), LOAD_POLL_INTERVAL);
    loadSnapshot.addServer(l3, uniqueId3, new String[] {"b"},
        new ServerLoad(l3ConnectionLoad, 1, 0, 1),
        LOAD_POLL_INTERVAL);

    ServerLocation newServer =
        loadSnapshot.getReplacementServerForConnection(l1, "", Collections.EMPTY_SET);
    assertEquals(l1, newServer);
    Map<ServerLocation, ServerLoad> loadMap = loadSnapshot.getLoadMap();
    ServerLoad l1Load = loadMap.get(l1);
    assertEquals(l1ConnectionLoad, l1Load.getConnectionLoad(), 0.01);
  }

  @Test
  public void testisCurrentServerMostLoaded() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();

    final ServerLocationAndMemberId sli1 = new ServerLocationAndMemberId(l1, uniqueId1);
    final ServerLocationAndMemberId sli2 = new ServerLocationAndMemberId(l2, uniqueId2);
    final ServerLocationAndMemberId sli3 = new ServerLocationAndMemberId(l3, uniqueId3);

    float l1ConnectionLoad = 50;
    float l2ConnectionLoad = 40;
    float l3ConnectionLoad = 30;

    LocatorLoadSnapshot.LoadHolder loadHolder1 =
        new LocatorLoadSnapshot.LoadHolder(l1, l1ConnectionLoad, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder2 =
        new LocatorLoadSnapshot.LoadHolder(l2, l2ConnectionLoad, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder3 =
        new LocatorLoadSnapshot.LoadHolder(l3, l3ConnectionLoad, 1, LOAD_POLL_INTERVAL);

    Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();

    groupServers.put(sli1, loadHolder1);
    groupServers.put(sli2, loadHolder2);
    groupServers.put(sli3, loadHolder3);

    assertEquals(loadHolder1, loadSnapshot.isCurrentServerMostLoaded(l1, groupServers));
    assertNull(loadSnapshot.isCurrentServerMostLoaded(l2, groupServers));
    assertNull(loadSnapshot.isCurrentServerMostLoaded(l3, groupServers));
  }

  @Test
  public void testGetReplacementServerForConnection() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();

    float l1ConnectionLoad = 50;
    float l2ConnectionLoad = 40;
    float l3ConnectionLoad = 30;

    loadSnapshot.addServer(l1, uniqueId1, new String[] {"a"},
        new ServerLoad(l1ConnectionLoad, 1, 0, 1),
        LOAD_POLL_INTERVAL);
    loadSnapshot.addServer(l2, uniqueId2, new String[] {"a", "b"},
        new ServerLoad(l2ConnectionLoad, 1, 0, 1), LOAD_POLL_INTERVAL);
    loadSnapshot.addServer(l3, uniqueId3, new String[] {"b"},
        new ServerLoad(l3ConnectionLoad, 1, 0, 1),
        LOAD_POLL_INTERVAL);

    ServerLocation newServer1 =
        loadSnapshot.getReplacementServerForConnection(l1, "", Collections.EMPTY_SET);
    assertEquals(l3, newServer1);
    ServerLocation newServer2 =
        loadSnapshot.getReplacementServerForConnection(l1, "a", Collections.EMPTY_SET);
    assertEquals(l2, newServer2);
    ServerLocation newServer3 =
        loadSnapshot.getReplacementServerForConnection(l3, "b", Collections.EMPTY_SET);
    assertEquals(l3, newServer3);
    ServerLocation newServer4 =
        loadSnapshot.getReplacementServerForConnection(l2, "b", Collections.EMPTY_SET);
    assertEquals(l3, newServer4);
  }

  @Test
  public void testFindBestServersReturnOneServer() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();

    final ServerLocationAndMemberId sli1 = new ServerLocationAndMemberId(l1, uniqueId1);
    final ServerLocationAndMemberId sli2 = new ServerLocationAndMemberId(l2, uniqueId2);
    final ServerLocationAndMemberId sli3 = new ServerLocationAndMemberId(l3, uniqueId3);

    float l1ConnectionLoad = 50;
    float l2ConnectionLoad = 30;
    float l3ConnectionLoad = 40;

    LocatorLoadSnapshot.LoadHolder loadHolder1 =
        new LocatorLoadSnapshot.LoadHolder(l1, l1ConnectionLoad, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder2 =
        new LocatorLoadSnapshot.LoadHolder(l2, l2ConnectionLoad, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder3 =
        new LocatorLoadSnapshot.LoadHolder(l3, l3ConnectionLoad, 1, LOAD_POLL_INTERVAL);

    Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();

    groupServers.put(sli1, loadHolder1);
    groupServers.put(sli2, loadHolder2);
    groupServers.put(sli3, loadHolder3);

    List<LocatorLoadSnapshot.LoadHolder> result =
        loadSnapshot.findBestServers(groupServers, Collections.EMPTY_SET, 1);
    assertEquals(1, result.size());
    assertEquals(loadHolder2, result.get(0));
  }

  @Test
  public void testFindBestServersReturnMoreThanOneServer() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();

    final ServerLocationAndMemberId sli1 = new ServerLocationAndMemberId(l1, uniqueId1);
    final ServerLocationAndMemberId sli2 = new ServerLocationAndMemberId(l2, uniqueId2);
    final ServerLocationAndMemberId sli3 = new ServerLocationAndMemberId(l3, uniqueId3);

    float l1ConnectionLoad = 50;
    float l2ConnectionLoad = 30;
    float l3ConnectionLoad = 40;

    LocatorLoadSnapshot.LoadHolder loadHolder1 =
        new LocatorLoadSnapshot.LoadHolder(l1, l1ConnectionLoad, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder2 =
        new LocatorLoadSnapshot.LoadHolder(l2, l2ConnectionLoad, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder3 =
        new LocatorLoadSnapshot.LoadHolder(l3, l3ConnectionLoad, 1, LOAD_POLL_INTERVAL);

    Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();

    groupServers.put(sli1, loadHolder1);
    groupServers.put(sli2, loadHolder2);
    groupServers.put(sli3, loadHolder3);

    List<LocatorLoadSnapshot.LoadHolder> result =
        loadSnapshot.findBestServers(groupServers, Collections.EMPTY_SET, 2);
    assertEquals(2, result.size());
    assertEquals(loadHolder2, result.get(0));
    assertEquals(loadHolder3, result.get(1));

    result = loadSnapshot.findBestServers(groupServers, Collections.EMPTY_SET, 0);
    assertEquals(0, result.size());
  }

  @Test
  public void testFindBestServersCalledWithZeroCount() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();

    final ServerLocationAndMemberId sli1 = new ServerLocationAndMemberId(l1, uniqueId1);
    final ServerLocationAndMemberId sli2 = new ServerLocationAndMemberId(l2, uniqueId2);
    final ServerLocationAndMemberId sli3 = new ServerLocationAndMemberId(l3, uniqueId3);

    float l1ConnectionLoad = 50;
    float l2ConnectionLoad = 30;
    float l3ConnectionLoad = 40;

    LocatorLoadSnapshot.LoadHolder loadHolder1 =
        new LocatorLoadSnapshot.LoadHolder(l1, l1ConnectionLoad, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder2 =
        new LocatorLoadSnapshot.LoadHolder(l2, l2ConnectionLoad, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder3 =
        new LocatorLoadSnapshot.LoadHolder(l3, l3ConnectionLoad, 1, LOAD_POLL_INTERVAL);

    Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();

    groupServers.put(sli1, loadHolder1);
    groupServers.put(sli2, loadHolder2);
    groupServers.put(sli3, loadHolder3);

    List<LocatorLoadSnapshot.LoadHolder> result =
        loadSnapshot.findBestServers(groupServers, Collections.EMPTY_SET, 0);
    assertEquals(0, result.size());
  }

  @Test
  public void testFindBestServersCalledWithNegativeCount() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation l1 = new ServerLocation("localhost", 1);
    final ServerLocation l2 = new ServerLocation("localhost", 2);
    final ServerLocation l3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();

    final ServerLocationAndMemberId sli1 = new ServerLocationAndMemberId(l1, uniqueId1);
    final ServerLocationAndMemberId sli2 = new ServerLocationAndMemberId(l2, uniqueId2);
    final ServerLocationAndMemberId sli3 = new ServerLocationAndMemberId(l3, uniqueId3);

    float l1ConnectionLoad = 50;
    float l2ConnectionLoad = 30;
    float l3ConnectionLoad = 40;

    LocatorLoadSnapshot.LoadHolder loadHolder1 =
        new LocatorLoadSnapshot.LoadHolder(l1, l1ConnectionLoad, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder2 =
        new LocatorLoadSnapshot.LoadHolder(l2, l2ConnectionLoad, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder3 =
        new LocatorLoadSnapshot.LoadHolder(l3, l3ConnectionLoad, 1, LOAD_POLL_INTERVAL);

    Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();

    groupServers.put(sli1, loadHolder1);
    groupServers.put(sli2, loadHolder2);
    groupServers.put(sli3, loadHolder3);

    List<LocatorLoadSnapshot.LoadHolder> result =
        loadSnapshot.findBestServers(groupServers, Collections.EMPTY_SET, -1);
    assertEquals(3, result.size());
    assertEquals(loadHolder2, result.get(0));
    assertEquals(loadHolder3, result.get(1));
    assertEquals(loadHolder1, result.get(2));
  }

  @Test
  public void updateMapWithServerLocationAndMemberId() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation serverLocation = new ServerLocation("localhost", 1);
    final String uniqueId = new InternalDistributedMember("localhost", 1).getUniqueId();
    final ServerLocationAndMemberId sli = new ServerLocationAndMemberId(serverLocation, uniqueId);
    LocatorLoadSnapshot.LoadHolder loadHolder =
        new LocatorLoadSnapshot.LoadHolder(serverLocation, 50, 1, LOAD_POLL_INTERVAL);
    Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();
    groupServers.put(sli, loadHolder);
    Map<String, Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder>> map =
        new HashMap<>();
    map.put(null, groupServers);

    loadSnapshot.updateMap(map, serverLocation, uniqueId, 60, 2);

    LocatorLoadSnapshot.LoadHolder expectedLoadHolder =
        new LocatorLoadSnapshot.LoadHolder(serverLocation, 60, 2, LOAD_POLL_INTERVAL);

    assertEquals(expectedLoadHolder.getLoad(), groupServers.get(sli).getLoad(), 0);
    assertEquals(expectedLoadHolder.getLoadPerConnection(),
        groupServers.get(sli).getLoadPerConnection(), 0);
  }

  @Test
  public void updateMapWithServerLocationAndMemberIdKeyNotFound() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation serverLocation = new ServerLocation("localhost", 1);
    final String uniqueId = new InternalDistributedMember("localhost", 1).getUniqueId();
    final ServerLocationAndMemberId sli = new ServerLocationAndMemberId(serverLocation, uniqueId);
    Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();
    Map<String, Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder>> map =
        new HashMap<>();
    map.put(null, groupServers);

    loadSnapshot.updateMap(map, serverLocation, uniqueId, 50, 1);

    assertNull(groupServers.get(sli));
  }

  @Test
  public void updateMapWithServerLocation() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation serverLocation = new ServerLocation("localhost", 1);
    LocatorLoadSnapshot.LoadHolder loadHolder =
        new LocatorLoadSnapshot.LoadHolder(serverLocation, 50, 1, LOAD_POLL_INTERVAL);
    Map<ServerLocation, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();
    groupServers.put(serverLocation, loadHolder);
    Map<String, Map<ServerLocation, LocatorLoadSnapshot.LoadHolder>> map =
        new HashMap<>();
    map.put(null, groupServers);

    loadSnapshot.updateMap(map, serverLocation, 60, 2);

    LocatorLoadSnapshot.LoadHolder expectedLoadHolder =
        new LocatorLoadSnapshot.LoadHolder(serverLocation, 60, 2, LOAD_POLL_INTERVAL);

    assertEquals(expectedLoadHolder.getLoad(), groupServers.get(serverLocation).getLoad(), 0);
    assertEquals(expectedLoadHolder.getLoadPerConnection(),
        groupServers.get(serverLocation).getLoadPerConnection(), 0);
  }

  @Test
  public void updateMapWithServerLocationKeyNotFound() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation serverLocation = new ServerLocation("localhost", 1);
    Map<ServerLocation, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();
    Map<String, Map<ServerLocation, LocatorLoadSnapshot.LoadHolder>> map =
        new HashMap<>();
    map.put(null, groupServers);

    loadSnapshot.updateMap(map, serverLocation, 50, 1);

    assertNull(groupServers.get(serverLocation));
  }

  @Test
  public void testRemoveFromMapWithServerLocationAndMemberId() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation sl1 = new ServerLocation("localhost", 1);
    final ServerLocation sl2 = new ServerLocation("localhost", 2);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final ServerLocationAndMemberId sli1 = new ServerLocationAndMemberId(sl1, uniqueId1);
    final ServerLocationAndMemberId sli2 = new ServerLocationAndMemberId(sl2, uniqueId2);
    LocatorLoadSnapshot.LoadHolder loadHolder1 =
        new LocatorLoadSnapshot.LoadHolder(sl1, 50, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder2 =
        new LocatorLoadSnapshot.LoadHolder(sl2, 50, 1, LOAD_POLL_INTERVAL);

    Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();
    groupServers.put(sli1, loadHolder1);
    groupServers.put(sli2, loadHolder2);
    Map<String, Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder>> map =
        new HashMap<>();
    map.put(null, groupServers);

    loadSnapshot.removeFromMap(map, new String[] {""}, sl1, uniqueId1);

    assertEquals(1, groupServers.size());
    assertNull(groupServers.get(sli1));
  }

  @Test
  public void testRemoveFromMapWithServerLocationAndMemberIdAndGroups() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation sl1 = new ServerLocation("localhost", 1);
    final ServerLocation sl2 = new ServerLocation("localhost", 2);
    final ServerLocation sl3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();
    final ServerLocationAndMemberId sli1 = new ServerLocationAndMemberId(sl1, uniqueId1);
    final ServerLocationAndMemberId sli2 = new ServerLocationAndMemberId(sl2, uniqueId2);
    final ServerLocationAndMemberId sli3 = new ServerLocationAndMemberId(sl3, uniqueId3);
    LocatorLoadSnapshot.LoadHolder loadHolder1 =
        new LocatorLoadSnapshot.LoadHolder(sl1, 50, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder2 =
        new LocatorLoadSnapshot.LoadHolder(sl2, 50, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder3 =
        new LocatorLoadSnapshot.LoadHolder(sl2, 50, 1, LOAD_POLL_INTERVAL);

    Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();
    groupServers.put(sli1, loadHolder1);
    groupServers.put(sli2, loadHolder2);
    groupServers.put(sli3, loadHolder3);

    Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder> groupAServers = new HashMap<>();
    groupAServers.put(sli1, loadHolder1);
    groupAServers.put(sli2, loadHolder2);

    Map<String, Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder>> map =
        new HashMap<>();
    map.put(null, groupServers);
    map.put("a", groupAServers);

    loadSnapshot.removeFromMap(map, new String[] {"a"}, sl1, uniqueId1);

    assertEquals(2, groupServers.size());
    assertNull(groupServers.get(sli1));

    assertEquals(1, groupAServers.size());
    assertNull(groupAServers.get(sli1));
  }

  @Test
  public void testRemoveFromMapWithServerLocation() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation sl1 = new ServerLocation("localhost", 1);
    final ServerLocation sl2 = new ServerLocation("localhost", 2);

    LocatorLoadSnapshot.LoadHolder loadHolder1 =
        new LocatorLoadSnapshot.LoadHolder(sl1, 50, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder2 =
        new LocatorLoadSnapshot.LoadHolder(sl2, 50, 1, LOAD_POLL_INTERVAL);

    Map<ServerLocation, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();
    groupServers.put(sl1, loadHolder1);
    groupServers.put(sl2, loadHolder2);
    Map<String, Map<ServerLocation, LocatorLoadSnapshot.LoadHolder>> map =
        new HashMap<>();
    map.put(null, groupServers);

    loadSnapshot.removeFromMap(map, new String[] {""}, sl1);

    assertEquals(1, groupServers.size());
    assertNull(groupServers.get(sl1));
  }

  @Test
  public void testRemoveFromMapWithServerLocationAndGroups() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation sl1 = new ServerLocation("localhost", 1);
    final ServerLocation sl2 = new ServerLocation("localhost", 2);
    final ServerLocation sl3 = new ServerLocation("localhost", 3);

    LocatorLoadSnapshot.LoadHolder loadHolder1 =
        new LocatorLoadSnapshot.LoadHolder(sl1, 50, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder2 =
        new LocatorLoadSnapshot.LoadHolder(sl2, 50, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder3 =
        new LocatorLoadSnapshot.LoadHolder(sl3, 50, 1, LOAD_POLL_INTERVAL);

    Map<ServerLocation, LocatorLoadSnapshot.LoadHolder> groupServers = new HashMap<>();
    groupServers.put(sl1, loadHolder1);
    groupServers.put(sl2, loadHolder2);
    groupServers.put(sl3, loadHolder3);

    Map<ServerLocation, LocatorLoadSnapshot.LoadHolder> groupAServers = new HashMap<>();
    groupAServers.put(sl1, loadHolder1);
    groupAServers.put(sl2, loadHolder2);

    Map<String, Map<ServerLocation, LocatorLoadSnapshot.LoadHolder>> map =
        new HashMap<>();
    map.put(null, groupServers);
    map.put("a", groupAServers);

    loadSnapshot.removeFromMap(map, new String[] {"a"}, sl1);

    assertEquals(2, groupServers.size());
    assertNull(groupServers.get(sl1));

    assertEquals(1, groupAServers.size());
    assertNull(groupAServers.get(sl1));
  }

  @Test
  public void testAddGroupsWithServerLocationAndMemberId() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation sl1 = new ServerLocation("localhost", 1);
    final ServerLocation sl2 = new ServerLocation("localhost", 2);
    final ServerLocation sl3 = new ServerLocation("localhost", 3);
    final String uniqueId1 = new InternalDistributedMember("localhost", 1).getUniqueId();
    final String uniqueId2 = new InternalDistributedMember("localhost", 2).getUniqueId();
    final String uniqueId3 = new InternalDistributedMember("localhost", 3).getUniqueId();
    final ServerLocationAndMemberId sli1 = new ServerLocationAndMemberId(sl1, uniqueId1);
    final ServerLocationAndMemberId sli2 = new ServerLocationAndMemberId(sl2, uniqueId2);
    final ServerLocationAndMemberId sli3 = new ServerLocationAndMemberId(sl3, uniqueId3);
    LocatorLoadSnapshot.LoadHolder loadHolder1 =
        new LocatorLoadSnapshot.LoadHolder(sl1, 50, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder2 =
        new LocatorLoadSnapshot.LoadHolder(sl2, 50, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder3 =
        new LocatorLoadSnapshot.LoadHolder(sl3, 50, 1, LOAD_POLL_INTERVAL);

    Map<String, Map<ServerLocationAndMemberId, LocatorLoadSnapshot.LoadHolder>> map =
        new HashMap<>();

    loadSnapshot.addGroups(map, new String[] {"a"}, loadHolder1, uniqueId1);
    loadSnapshot.addGroups(map, new String[] {"a", "b"}, loadHolder2, uniqueId2);
    loadSnapshot.addGroups(map, new String[] {}, loadHolder3, uniqueId3);

    assertEquals(3, map.get(null).size());
    assertEquals(loadHolder1, map.get(null).get(sli1));
    assertEquals(loadHolder2, map.get(null).get(sli2));
    assertEquals(loadHolder3, map.get(null).get(sli3));

    assertEquals(2, map.get("a").size());
    assertEquals(loadHolder1, map.get("a").get(sli1));
    assertEquals(loadHolder2, map.get("a").get(sli2));

    assertEquals(1, map.get("b").size());
    assertEquals(loadHolder2, map.get("b").get(sli2));
  }

  @Test
  public void testAddGroupsWithServerLocation() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation sl1 = new ServerLocation("localhost", 1);
    final ServerLocation sl2 = new ServerLocation("localhost", 2);
    final ServerLocation sl3 = new ServerLocation("localhost", 3);

    LocatorLoadSnapshot.LoadHolder loadHolder1 =
        new LocatorLoadSnapshot.LoadHolder(sl1, 50, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder2 =
        new LocatorLoadSnapshot.LoadHolder(sl2, 50, 1, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder loadHolder3 =
        new LocatorLoadSnapshot.LoadHolder(sl3, 50, 1, LOAD_POLL_INTERVAL);

    Map<String, Map<ServerLocation, LocatorLoadSnapshot.LoadHolder>> map =
        new HashMap<>();

    loadSnapshot.addGroups(map, new String[] {"a"}, loadHolder1);
    loadSnapshot.addGroups(map, new String[] {"a", "b"}, loadHolder2);
    loadSnapshot.addGroups(map, new String[] {}, loadHolder3);

    assertEquals(3, map.get(null).size());
    assertEquals(loadHolder1, map.get(null).get(sl1));
    assertEquals(loadHolder2, map.get(null).get(sl2));
    assertEquals(loadHolder3, map.get(null).get(sl3));

    assertEquals(2, map.get("a").size());
    assertEquals(loadHolder1, map.get("a").get(sl1));
    assertEquals(loadHolder2, map.get("a").get(sl2));

    assertEquals(1, map.get("b").size());
    assertEquals(loadHolder2, map.get("b").get(sl2));
  }
}
