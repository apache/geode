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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.entry;
import static org.assertj.core.data.Offset.offset;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Tests the functionality of the LocatorLoadSnapshot, which is the data structure that is used in
 * the locator to compare the load between multiple servers.
 */
@Category({MembershipTest.class})
public class LocatorLoadSnapshotTest {

  final int LOAD_POLL_INTERVAL = 30000;

  /**
   * Test to make sure than an empty snapshot returns the correct values.
   */
  @Test
  public void testEmptySnapshot() {
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    assertThat(sn.getServerForConnection("group", Collections.emptySet())).isNull();
    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isNull();
    assertThat(sn.getServersForQueue(null, Collections.emptySet(), 5))
        .isEqualTo(Collections.EMPTY_LIST);
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

    Map<ServerLocation, ServerLoad> expectedLoad = new HashMap<>();
    expectedLoad.put(l1, ld1);
    expectedLoad.put(l2, ld2);
    assertThat(sn.getLoadMap()).isEqualTo(expectedLoad);

    assertThat(sn.getServerForConnection("group", Collections.emptySet())).isNull();
    assertThat(sn.getServersForQueue("group", Collections.emptySet(), 5))
        .isEqualTo(Collections.EMPTY_LIST);

    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isEqualTo(l1);
    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isEqualTo(l1);
    // the load should be equal here, so we don't know which server to expect
    sn.getServerForConnection(null, Collections.emptySet());
    sn.getServerForConnection(null, Collections.emptySet());

    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isEqualTo(l2);
    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isEqualTo(l2);

    assertThat(sn.getServersForQueue(null, Collections.emptySet(), 1))
        .isEqualTo(Collections.singletonList(l2));
    assertThat(sn.getServersForQueue(null, Collections.emptySet(), 1))
        .isEqualTo(Collections.singletonList(l1));
    assertThat(sn.getServersForQueue(null, Collections.emptySet(), 1))
        .isEqualTo(Collections.singletonList(l2));

    assertThat(sn.getServersForQueue(null, Collections.emptySet(), 5))
        .isEqualTo(Arrays.asList(l2, l1));
    assertThat(sn.getServersForQueue(null, Collections.emptySet(), -1))
        .isEqualTo(Arrays.asList(l2, l1));
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

    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isEqualTo(l1);
    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isEqualTo(l1);
    sn.updateLoad(l1, uniqueId1, new ServerLoad(200, 1, 1, 1));
    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isEqualTo(l2);
    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isEqualTo(l2);
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

    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isEqualTo(l1);

    assertThat(sn.getServersForQueue(null, Collections.emptySet(), -1))
        .isEqualTo(Arrays.asList(l1, l2));
    sn.removeServer(l1, uniqueId1);
    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isEqualTo(l2);
    assertThat(sn.getServersForQueue(null, Collections.emptySet(), -1))
        .isEqualTo(Collections.singletonList(l2));
    assertThat(sn.getServersForQueue(null, Collections.emptySet(), -1)).isEqualTo(
        Collections.singletonList(l2));
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
    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isNotNull();
    assertThat(sn.getServerForConnection("a", Collections.emptySet())).isEqualTo(l1);
    assertThat(sn.getServerForConnection("c", Collections.emptySet())).isEqualTo(l2);
    sn.updateLoad(l1, uniqueId1, new ServerLoad(10, 1, 1, 1));
    assertThat(sn.getServerForConnection("b", Collections.emptySet())).isEqualTo(l2);
    sn.updateLoad(l2, uniqueId2, new ServerLoad(100, 1, 1, 1));
    assertThat(sn.getServerForConnection("b", Collections.emptySet())).isEqualTo(l1);
    assertThat(sn.getServersForQueue("a", Collections.emptySet(), -1)).isEqualTo(
        Collections.singletonList(l1));
    assertThat(sn.getServersForQueue("c", Collections.emptySet(), -1)).isEqualTo(
        Collections.singletonList(l2));
    assertThat(sn.getServersForQueue("b", Collections.emptySet(), -1)).isEqualTo(
        Arrays.asList(l1, l2));
    assertThat(sn.getServersForQueue(null, Collections.emptySet(), -1)).isEqualTo(
        Arrays.asList(l1, l2));
    assertThat(sn.getServersForQueue("b", Collections.emptySet(), 5)).isEqualTo(
        Arrays.asList(l1, l2));

    sn.removeServer(l1, uniqueId1);
    assertThat(sn.getServerForConnection("b", Collections.emptySet())).isEqualTo(l2);
    assertThat(sn.getServerForConnection("b", Collections.emptySet())).isEqualTo(l2);
    assertThat(sn.getServerForConnection("a", Collections.emptySet())).isNull();
    assertThat(sn.getServerForConnection("c", Collections.emptySet())).isEqualTo(l2);
    assertThat(sn.getServersForQueue("a", Collections.emptySet(), -1)).isEqualTo(
        Collections.emptyList());
    assertThat(sn.getServersForQueue("b", Collections.emptySet(), 5)).isEqualTo(
        Collections.singletonList(l2));
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
      ServerLocation l = sn.getServerForConnection("a", Collections.emptySet());
      assertThat(l1.equals(l) || l2.equals(l)).isTrue();
      l = sn.getServerForConnection("b", Collections.emptySet());
      assertThat(l2.equals(l) || l3.equals(l)).isTrue();
    }

    Map<ServerLocation, ServerLoad> expected = new HashMap<>();
    ServerLoad expectedLoad = new ServerLoad(40f, 1f, 0f, 1f);
    expected.put(l1, expectedLoad);
    expected.put(l2, expectedLoad);
    expected.put(l3, expectedLoad);
    assertThat(sn.getLoadMap()).isEqualTo(expected);

    sn.updateLoad(l1, uniqueId1, new ServerLoad(0, 1, 0, 1));
    sn.updateLoad(l2, uniqueId2, new ServerLoad(0, 1, 0, 1));
    sn.updateLoad(l3, uniqueId3, new ServerLoad(0, 1, 0, 1));


    // Now do the same test, but make all the requests for one group first,
    // then the second group.
    for (int i = 0; i < 60; i++) {
      ServerLocation l = sn.getServerForConnection("a", Collections.emptySet());
      assertThat(l1.equals(l) || l2.equals(l)).isTrue();
    }

    expected = new HashMap<>();
    expected.put(l1, new ServerLoad(30f, 1f, 0f, 1f));
    expected.put(l2, new ServerLoad(30f, 1f, 0f, 1f));
    expected.put(l3, new ServerLoad(0f, 1f, 0f, 1f));
    assertThat(sn.getLoadMap()).isEqualTo(expected);

    for (int i = 0; i < 60; i++) {
      ServerLocation l = sn.getServerForConnection("b", Collections.emptySet());
      assertThat(l2.equals(l) || l3.equals(l)).isTrue();
    }

    // The load can't be completely balanced, because
    // We already had 30 connections from group a on server l2.
    // But we expect that l3 should have received most of the connections
    // for group b, because it started out with 0.
    expected = new HashMap<>();
    expected.put(l1, new ServerLoad(30f, 1f, 0f, 1f));
    expected.put(l2, new ServerLoad(45f, 1f, 0f, 1f));
    expected.put(l3, new ServerLoad(45f, 1f, 0f, 1f));
    assertThat(sn.getLoadMap()).isEqualTo(expected);

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

    Set<ServerLocation> excludeAll = new HashSet<>();
    excludeAll.add(l1);
    excludeAll.add(l2);

    assertThat(sn.getServerForConnection(null, Collections.emptySet())).isEqualTo(l1);
    assertThat(sn.getServerForConnection(null, Collections.singleton(l1))).isEqualTo(l2);

    assertThat(sn.getServerForConnection(null, excludeAll)).isEqualTo(null);
    assertThat(sn.getServersForQueue(null, Collections.singleton(l1), 3)).isEqualTo(
        Collections.singletonList(l2));

    assertThat(sn.getServersForQueue(null, excludeAll, 3)).isEqualTo(Collections.emptyList());
  }

  @Test
  public void testAreBalanced() {
    final LocatorLoadSnapshot sn = new LocatorLoadSnapshot();
    assertThat(sn.hasBalancedConnections(null)).isTrue();
    assertThat(sn.hasBalancedConnections("a")).isTrue();
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

    assertThat(sn.hasBalancedConnections(null)).isTrue();
    assertThat(sn.hasBalancedConnections("a")).isTrue();
    assertThat(sn.hasBalancedConnections("b")).isTrue();

    sn.updateLoad(l1, uniqueId1, new ServerLoad(1, 1, 0, 1));
    assertThat(sn.hasBalancedConnections(null)).isTrue();
    assertThat(sn.hasBalancedConnections("a")).isTrue();
    assertThat(sn.hasBalancedConnections("b")).isTrue();

    sn.updateLoad(l2, uniqueId2, new ServerLoad(2, 1, 0, 1));
    assertThat(sn.hasBalancedConnections(null)).isFalse();
    assertThat(sn.hasBalancedConnections("a")).isTrue();
    assertThat(sn.hasBalancedConnections("b")).isFalse();
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
    ServerLocation newServer;
    do {
      newServer = loadSnapshot.getReplacementServerForConnection(l1, "", Collections.emptySet());
      if (newServer == l3) {
        // the threshold check should have initiated client rebalancing
        assertThat(loadSnapshot.isRebalancing()).isTrue();
      }
    } while (newServer == l3);

    // once balance is achieved we should have received the same server and
    // rebalancing should have ended
    assertThat(newServer).isEqualTo(l1);
    assertThat(loadSnapshot.isRebalancing()).isFalse();

    // all load snapshots should now be balanced
    Map<ServerLocation, ServerLoad> loadMap = loadSnapshot.getLoadMap();
    ServerLoad l1Load = loadMap.get(l1);
    assertThat(l1Load.getConnectionLoad()).isCloseTo(50F, offset(0.01F));
    ServerLoad l2Load = loadMap.get(l2);
    assertThat(l2Load.getConnectionLoad()).isCloseTo(50F, offset(0.01F));
    ServerLoad l3Load = loadMap.get(l3);
    assertThat(l3Load.getConnectionLoad()).isCloseTo(50F, offset(0.01F));
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
        loadSnapshot.getReplacementServerForConnection(l1, "", Collections.emptySet());
    assertThat(newServer).isEqualTo(l1);
    Map<ServerLocation, ServerLoad> loadMap = loadSnapshot.getLoadMap();
    ServerLoad l1Load = loadMap.get(l1);
    assertThat(l1Load.getConnectionLoad()).isCloseTo(l1ConnectionLoad, offset(0.01F));
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

    assertThat(loadSnapshot.isCurrentServerMostLoaded(l1, groupServers)).isEqualTo(loadHolder1);
    assertThat(loadSnapshot.isCurrentServerMostLoaded(l2, groupServers)).isNull();
    assertThat(loadSnapshot.isCurrentServerMostLoaded(l3, groupServers)).isNull();
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
        loadSnapshot.getReplacementServerForConnection(l1, "", Collections.emptySet());
    assertThat(newServer1).isEqualTo(l3);
    ServerLocation newServer2 =
        loadSnapshot.getReplacementServerForConnection(l1, "a", Collections.emptySet());
    assertThat(newServer2).isEqualTo(l2);
    ServerLocation newServer3 =
        loadSnapshot.getReplacementServerForConnection(l3, "b", Collections.emptySet());
    assertThat(newServer3).isEqualTo(l3);
    ServerLocation newServer4 =
        loadSnapshot.getReplacementServerForConnection(l2, "b", Collections.emptySet());
    assertThat(newServer4).isEqualTo(l3);
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
        loadSnapshot.findBestServers(groupServers, Collections.emptySet(), 1);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)).isEqualTo(loadHolder2);
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
        loadSnapshot.findBestServers(groupServers, Collections.emptySet(), 2);
    assertThat(result).hasSize(2);
    assertThat(result.get(0)).isEqualTo(loadHolder2);
    assertThat(result.get(1)).isEqualTo(loadHolder3);

    result = loadSnapshot.findBestServers(groupServers, Collections.emptySet(), 0);
    assertThat(result).hasSize(0);
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
        loadSnapshot.findBestServers(groupServers, Collections.emptySet(), 0);
    assertThat(result).hasSize(0);
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
        loadSnapshot.findBestServers(groupServers, Collections.emptySet(), -1);
    assertThat(result).containsExactly(loadHolder2, loadHolder3, loadHolder1);
  }

  @Test
  public void testThatConnectionLoadIsCorrectlyUpdatedForTrafficConnectionGroup() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation serverLocation = new ServerLocation("localhost", 1);
    final String uniqueId = new InternalDistributedMember("localhost", 1).getUniqueId();

    loadSnapshot.addServer(serverLocation, uniqueId, new String[0], new ServerLoad(50, 1, 0, 1),
        LOAD_POLL_INTERVAL);

    loadSnapshot.updateConnectionLoadMap(serverLocation, uniqueId, 60, 2);

    LocatorLoadSnapshot.LoadHolder expectedLoadHolder =
        new LocatorLoadSnapshot.LoadHolder(serverLocation, 60, 2, LOAD_POLL_INTERVAL);

    Map<ServerLocation, ServerLoad> serverLoadMap = loadSnapshot.getLoadMap();
    assertThat(expectedLoadHolder.getLoad())
        .isEqualTo(serverLoadMap.get(serverLocation).getConnectionLoad());
    assertThat(expectedLoadHolder.getLoadPerConnection())
        .isEqualTo(serverLoadMap.get(serverLocation).getLoadPerConnection());
  }

  @Test
  public void testThatConnectionLoadIsCorrectlyUpdatedForGatewayReceiverGroup() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation serverLocation = new ServerLocation("localhost", 1);
    final String uniqueId = new InternalDistributedMember("localhost", 1).getUniqueId();
    final ServerLocationAndMemberId servLocAndMemberId =
        new ServerLocationAndMemberId(serverLocation, uniqueId);

    loadSnapshot.addServer(serverLocation, uniqueId, new String[] {GatewayReceiver.RECEIVER_GROUP},
        new ServerLoad(50, 1, 0, 1),
        LOAD_POLL_INTERVAL);

    LocatorLoadSnapshot.LoadHolder expectedLoadHolder =
        new LocatorLoadSnapshot.LoadHolder(serverLocation, 70, 8, LOAD_POLL_INTERVAL);

    loadSnapshot.updateConnectionLoadMap(serverLocation, uniqueId, expectedLoadHolder.getLoad(),
        expectedLoadHolder.getLoadPerConnection());

    Map<ServerLocationAndMemberId, ServerLoad> serverLoadMap =
        loadSnapshot.getGatewayReceiverLoadMap();
    assertThat(expectedLoadHolder.getLoad())
        .isEqualTo(serverLoadMap.get(servLocAndMemberId).getConnectionLoad());
    assertThat(expectedLoadHolder.getLoadPerConnection())
        .isEqualTo(serverLoadMap.get(servLocAndMemberId).getLoadPerConnection());
  }

  @Test
  public void testThatConnectionLoadIsCorrectlyUpdatedForBothGatewayReceiverGroupAndTrafficConnectionGroup() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation serverLocation = new ServerLocation("localhost", 1);
    final ServerLocation gatewayReceiverLocation = new ServerLocation("gatewayReciverHost", 111);
    final String uniqueId = new InternalDistributedMember("localhost", 1).getUniqueId();
    final ServerLocationAndMemberId servLocAndMemberId =
        new ServerLocationAndMemberId(gatewayReceiverLocation, uniqueId);

    loadSnapshot.addServer(gatewayReceiverLocation, uniqueId,
        new String[] {GatewayReceiver.RECEIVER_GROUP}, new ServerLoad(50, 1, 0, 1),
        LOAD_POLL_INTERVAL);

    loadSnapshot.addServer(serverLocation, uniqueId, new String[0], new ServerLoad(50, 1, 0, 1),
        LOAD_POLL_INTERVAL);

    LocatorLoadSnapshot.LoadHolder expectedLoadHolder =
        new LocatorLoadSnapshot.LoadHolder(serverLocation, 70, 8, LOAD_POLL_INTERVAL);
    LocatorLoadSnapshot.LoadHolder expectedGatewayLoad =
        new LocatorLoadSnapshot.LoadHolder(serverLocation, 90, 10, LOAD_POLL_INTERVAL);

    loadSnapshot.updateConnectionLoadMap(serverLocation, uniqueId, expectedLoadHolder.getLoad(),
        expectedLoadHolder.getLoadPerConnection());
    loadSnapshot.updateConnectionLoadMap(gatewayReceiverLocation, uniqueId,
        expectedGatewayLoad.getLoad(), expectedGatewayLoad.getLoadPerConnection());

    Map<ServerLocationAndMemberId, ServerLoad> gatewayReceiverLoadMap =
        loadSnapshot.getGatewayReceiverLoadMap();
    assertThat(expectedGatewayLoad.getLoad())
        .isEqualTo(gatewayReceiverLoadMap.get(servLocAndMemberId).getConnectionLoad());
    assertThat(expectedGatewayLoad.getLoadPerConnection())
        .isEqualTo(gatewayReceiverLoadMap.get(servLocAndMemberId).getLoadPerConnection());

    Map<ServerLocation, ServerLoad> serverLoadMap = loadSnapshot.getLoadMap();
    assertThat(expectedLoadHolder.getLoad())
        .isEqualTo(serverLoadMap.get(serverLocation).getConnectionLoad());
    assertThat(expectedLoadHolder.getLoadPerConnection())
        .isEqualTo(serverLoadMap.get(serverLocation).getLoadPerConnection());
  }

  @Test
  public void updateMapWithServerLocationAndMemberIdKeyNotFound() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation serverLocation = new ServerLocation("localhost", 1);
    final String uniqueId = new InternalDistributedMember("localhost", 1).getUniqueId();

    loadSnapshot.updateConnectionLoadMap(serverLocation, uniqueId, 50, 1);

    Map<ServerLocation, ServerLoad> serverLoadMap = loadSnapshot.getLoadMap();
    assertThat(serverLoadMap.isEmpty()).as("Expected connection map to be empty").isTrue();
  }

  @Test
  public void updateQueueMapWithServerLocation() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation serverLocation = new ServerLocation("localhost", 1);
    final String uniqueId = "memberId1";

    loadSnapshot.addServer(serverLocation, uniqueId, new String[0], new ServerLoad(50, 1, 0, 1),
        LOAD_POLL_INTERVAL);

    LocatorLoadSnapshot.LoadHolder expectedLoadHolder =
        new LocatorLoadSnapshot.LoadHolder(serverLocation, 60, 2, LOAD_POLL_INTERVAL);

    loadSnapshot.updateQueueLoadMap(serverLocation, expectedLoadHolder.getLoad(),
        expectedLoadHolder.getLoadPerConnection());

    Map<ServerLocation, ServerLoad> serverLoadMap = loadSnapshot.getLoadMap();
    ServerLoad queueServerLoad = serverLoadMap.get(serverLocation);
    assertThat(expectedLoadHolder.getLoad())
        .isEqualTo(queueServerLoad.getSubscriptionConnectionLoad());
    assertThat(expectedLoadHolder.getLoadPerConnection())
        .isEqualTo(queueServerLoad.getLoadPerSubscriptionConnection());
  }

  @Test
  public void updateQueueMapWithServerLocationKeyNotFound() {
    final LocatorLoadSnapshot loadSnapshot = new LocatorLoadSnapshot();

    final ServerLocation serverLocation = new ServerLocation("localhost", 1);

    loadSnapshot.updateQueueLoadMap(serverLocation, 70, 1);

    Map<ServerLocation, ServerLoad> serverLoadMap = loadSnapshot.getLoadMap();
    assertThat(serverLoadMap.isEmpty()).as("Expected connection map to be empty").isTrue();
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

    assertThat(groupServers).hasSize(1);
    assertThat(groupServers).doesNotContainKey(sli1);
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

    assertThat(groupServers).hasSize(2);
    assertThat(groupServers).doesNotContainKey(sli1);

    assertThat(groupAServers).hasSize(1);
    assertThat(groupServers).doesNotContainKey(sli1);
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

    assertThat(groupServers).hasSize(1);
    assertThat(groupServers).doesNotContainKey(sl1);
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

    assertThat(groupServers).hasSize(2);
    assertThat(groupServers).doesNotContainKey(sl1);

    assertThat(groupAServers).hasSize(1);
    assertThat(groupServers).doesNotContainKey(sl1);
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

    assertThat(map.get(null)).containsOnly(entry(sli1, loadHolder1), entry(sli2, loadHolder2),
        entry(sli3, loadHolder3));
    assertThat(map.get("a")).containsOnly(entry(sli1, loadHolder1), entry(sli2, loadHolder2));
    assertThat(map.get("b")).containsOnly(entry(sli2, loadHolder2));
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

    assertThat(map.get(null)).containsOnly(entry(sl1, loadHolder1), entry(sl2, loadHolder2),
        entry(sl3, loadHolder3));
    assertThat(map.get("a")).containsOnly(entry(sl1, loadHolder1), entry(sl2, loadHolder2));
    assertThat(map.get("b")).containsOnly(entry(sl2, loadHolder2));
  }
}
