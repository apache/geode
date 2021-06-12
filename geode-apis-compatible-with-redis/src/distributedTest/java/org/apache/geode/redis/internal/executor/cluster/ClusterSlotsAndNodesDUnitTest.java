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

package org.apache.geode.redis.internal.executor.cluster;

import static org.apache.geode.redis.internal.RegionProvider.REDIS_REGION_BUCKETS;
import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS_PER_BUCKET;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.assertj.core.data.Offset;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.redis.ClusterNode;
import org.apache.geode.redis.ClusterNodes;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class ClusterSlotsAndNodesDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final String LOCAL_HOST = "127.0.0.1";
  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;

  private static Jedis jedis1;
  private static Jedis jedis2;
  private static JedisCluster jedisCluster;
  private static int redisServerPort1;
  private static int redisServerPort2;

  @BeforeClass
  public static void classSetup() {
    locator = cluster.startLocatorVM(0);
    server1 = cluster.startRedisVM(1, locator.getPort());
    server2 = cluster.startRedisVM(2, locator.getPort());
  }

  @Before
  public void setup() {
    redisServerPort1 = cluster.getRedisPort(1);
    redisServerPort2 = cluster.getRedisPort(2);
    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);
    jedis2 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);

    jedisCluster = new JedisCluster(new HostAndPort("localhost", redisServerPort1), JEDIS_TIMEOUT);
  }

  @After
  public void cleanup() {
    jedis1.close();
    jedis2.close();
    jedisCluster.close();
  }

  @After
  public void testCleanup() {
    rebalanceAllRegions(server1);
  }

  @Test
  public void eachServerProducesTheSameNodeInformation() {
    List<ClusterNode> nodes1 = ClusterNodes.parseClusterNodes(jedis1.clusterNodes()).getNodes();
    assertThat(nodes1).hasSize(2);

    List<ClusterNode> nodes2 = ClusterNodes.parseClusterNodes(jedis2.clusterNodes()).getNodes();
    assertThat(nodes2).hasSize(2);

    assertThat(nodes1).containsExactlyInAnyOrderElementsOf(nodes2);
  }

  @Test
  public void eachServerProducesTheSameSlotInformation() {
    List<Object> slots1 = jedis1.clusterSlots();
    List<Object> slots2 = jedis2.clusterSlots();

    assertThat(slots1).usingRecursiveComparison().isEqualTo(slots2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void slotInformationIsCorrect() {
    List<Object> slots = jedis1.clusterSlots();

    assertThat(slots).hasSize(REDIS_REGION_BUCKETS);

    for (int i = 0; i < REDIS_REGION_BUCKETS; i++) {
      long slotStart = (long) ((List<Object>) slots.get(i)).get(0);
      long slotEnd = (long) ((List<Object>) slots.get(i)).get(1);

      assertThat(slotStart).isEqualTo((long) i * REDIS_REGION_BUCKETS);
      assertThat(slotEnd).isEqualTo((long) i * REDIS_REGION_BUCKETS + (REDIS_SLOTS_PER_BUCKET - 1));
    }
  }

  @Test
  public void slotsDistributionIsFairlyUniform() {
    List<Object> slots = jedis1.clusterSlots();
    List<ClusterNode> nodes = ClusterNodes.parseClusterSlots(slots).getNodes();

    assertThat(nodes.get(0).slots.size()).isCloseTo(REDIS_REGION_BUCKETS / 2, Offset.offset(2));
    assertThat(nodes.get(1).slots.size()).isCloseTo(REDIS_REGION_BUCKETS / 2, Offset.offset(2));
  }

  @Test
  public void whenAServerIsAddedOrRemoved_slotsAreRedistributed() {
    cluster.startRedisVM(3, locator.getPort());
    rebalanceAllRegions(server1);

    List<Object> slots = jedis1.clusterSlots();
    List<ClusterNode> nodes = ClusterNodes.parseClusterSlots(slots).getNodes();

    assertThat(nodes).as("incorrect number of nodes").hasSize(3);
    assertThat(nodes.get(0).slots.size()).isCloseTo(REDIS_REGION_BUCKETS / 3, Offset.offset(2));
    assertThat(nodes.get(1).slots.size()).isCloseTo(REDIS_REGION_BUCKETS / 3, Offset.offset(2));
    assertThat(nodes.get(2).slots.size()).isCloseTo(REDIS_REGION_BUCKETS / 3, Offset.offset(2));

    String info = jedis1.clusterInfo();
    assertThat(info).contains("cluster_known_nodes:3");
    assertThat(info).contains("cluster_size:3");

    cluster.crashVM(3);
    rebalanceAllRegions(server1);

    slots = jedis1.clusterSlots();
    nodes = ClusterNodes.parseClusterSlots(slots).getNodes();

    assertThat(nodes).as("incorrect number of nodes").hasSize(2);
    assertThat(nodes.get(0).slots.size()).isCloseTo(REDIS_REGION_BUCKETS / 2, Offset.offset(2));
    assertThat(nodes.get(1).slots.size()).isCloseTo(REDIS_REGION_BUCKETS / 2, Offset.offset(2));

    info = jedis1.clusterInfo();
    assertThat(info).contains("cluster_known_nodes:2");
    assertThat(info).contains("cluster_size:2");
  }

  @Test
  public void whenMultipleServersFail_bucketsAreRecreated() {
    int ENTRIES = 1000;

    cluster.startRedisVM(3, locator.getPort());
    cluster.startRedisVM(4, locator.getPort());
    rebalanceAllRegions(server1);

    for (int i = 0; i < ENTRIES; i++) {
      jedisCluster.set("key-" + i, "value-" + 1);
    }

    cluster.crashVM(3);
    cluster.crashVM(4);

    // Implicitly recreate missing buckets.
    jedis1.clusterSlots();
    rebalanceAllRegions(server1);

    int keysRemaining = 0;
    for (int i = 0; i < ENTRIES; i++) {
      keysRemaining += jedisCluster.get("key-" + i) != null ? 1 : 0;
    }

    assertThat(keysRemaining).isLessThan(ENTRIES);

    List<Object> slots = jedis1.clusterSlots();
    List<ClusterNode> nodes = ClusterNodes.parseClusterSlots(slots).getNodes();

    assertThat(nodes).as("incorrect number of nodes").hasSize(2);
    assertThat(nodes.get(0).slots.size()).isCloseTo(REDIS_REGION_BUCKETS / 2, Offset.offset(2));

    String info = jedis1.clusterInfo();
    assertThat(info).contains("cluster_known_nodes:2");
    assertThat(info).contains("cluster_size:2");
  }

  @Test
  public void slotsAreNotMissingOrDuplicatedWhenPrimariesAreMoving() throws Exception {
    AtomicBoolean done = new AtomicBoolean();
    CompletableFuture<Void> startupShutdownFuture = executor.runAsync(() -> {
      while (!done.get()) {
        MemberVM server3 = cluster.startRedisVM(3, locator.getPort());
        rebalanceAllRegions(server3);
        server3.stop();
      }
    });

    long endTime = System.currentTimeMillis() + 60_000;
    CompletableFuture<Integer> getSlotsFuture = executor.supplyAsync(() -> {
      int iterations = 0;

      while (System.currentTimeMillis() < endTime) {
        List<ClusterNode> nodes = ClusterNodes.parseClusterSlots(jedis1.clusterSlots()).getNodes();

        /// Ensure there is no missing or duplicate slot info
        BitSet missingSlots = new BitSet();
        // Set all bits as missing
        missingSlots.flip(0, 16384);
        nodes.stream()
            .flatMap(node -> node.slots.stream())
            .forEach(slot -> missingSlots.flip(slot.getLeft().intValue(),
                slot.getRight().intValue() + 1));

        assertThat(missingSlots.stream().toArray()).isEmpty();
        iterations++;
      }

      return iterations;
    });

    int iterations = getSlotsFuture.get();
    done.set(true);
    startupShutdownFuture.get();

    assertThat(iterations).isGreaterThan(0);
  }

  @Test
  public void hostAndPortInfoIsUnique_whenPrimariesAreMoving() throws Exception {
    AtomicBoolean done = new AtomicBoolean();
    CompletableFuture<Void> startupShutdownFuture = executor.runAsync(() -> {
      while (!done.get()) {
        server2.stop();
        server2 = cluster.startRedisVM(2, locator.getPort());
        rebalanceAllRegions(server1);
      }
    });

    long endTime = System.currentTimeMillis() + 60_000;
    CompletableFuture<Integer> getSlotsFuture = executor.supplyAsync(() -> {
      int iterations = 0;

      while (System.currentTimeMillis() < endTime) {
        List<ClusterNode> nodes = ClusterNodes.parseClusterNodes(jedis1.clusterNodes()).getNodes();

        if (nodes.size() != 2) {
          continue;
        }

        assertThat(nodes.get(0).port).isNotEqualTo(nodes.get(1).port);
        iterations++;
      }

      return iterations;
    });

    int iterations = getSlotsFuture.get();
    done.set(true);
    startupShutdownFuture.get();

    assertThat(iterations).isGreaterThan(0);
  }

  @Test
  public void clusterSlotsAndClusterNodesResponseIsEquivalent() {
    List<ClusterNode> nodesFromSlots =
        ClusterNodes.parseClusterSlots(jedis1.clusterSlots()).getNodes();
    List<ClusterNode> nodesFromNodes =
        ClusterNodes.parseClusterNodes(jedis1.clusterNodes()).getNodes();

    assertThat(nodesFromSlots).containsExactlyInAnyOrderElementsOf(nodesFromNodes);
  }

  private static void rebalanceAllRegions(MemberVM vm) {
    vm.invoke("Running rebalance", () -> {
      ResourceManager manager = ClusterStartupRule.getCache().getResourceManager();
      RebalanceFactory factory = manager.createRebalanceFactory();
      try {
        factory.start().getResults();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
