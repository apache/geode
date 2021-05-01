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

package org.apache.geode.redis.internal.executor.set;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class SremDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int SET_SIZE = 1000;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static JedisCluster jedis;

  private static Properties locatorProperties;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  private static int redisServerPort;

  @BeforeClass
  public static void classSetup() {
    locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = clusterStartUp.startLocatorVM(0, locatorProperties);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    redisServerPort = clusterStartUp.getRedisPort(1);

    jedis = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort), JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    try (Jedis conn = jedis.getConnectionFromSlot(0)) {
      conn.flushAll();
    }
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();

    server1.stop();
    server2.stop();
    server3.stop();
  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_thenRemoveHalfOfData() {

    String key = "key";

    List<String> members = makeMemberList(SET_SIZE, "member1-");
    List<String> halfOfMembers = makeMemberList(SET_SIZE / 2, "member1-");
    List<String> otherHalfOfMembers = new ArrayList<>(members);
    otherHalfOfMembers.removeAll(halfOfMembers);

    jedis.sadd(key, members.toArray(new String[] {}));
    jedis.srem(key, halfOfMembers.toArray(new String[] {}));

    Set<String> result = jedis.smembers(key);

    assertThat(result.toArray().length).isEqualTo(otherHalfOfMembers.size());
    assertThat(result.toArray()).containsExactlyInAnyOrder(otherHalfOfMembers.toArray());
  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_removingDifferentDataFromSameSetConcurrently() {

    String key = "key";

    List<String> members1 = makeMemberList(SET_SIZE, "member1-");
    List<String> members2 = makeMemberList(SET_SIZE, "member2-");

    List<String> allMembers = new ArrayList<>();
    allMembers.addAll(members1);
    allMembers.addAll(members2);

    jedis.sadd(key, allMembers.toArray(new String[] {}));

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.srem(key, members1.get(i)),
        (i) -> jedis.srem(key, members2.get(i))).run();

    Set<String> results = jedis.smembers(key);

    assertThat(results).isEmpty();
  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_givenMultipleClients_removingSameDataFromSameSetConcurrently() {

    String key = "key";

    List<String> members = makeMemberList(SET_SIZE, "member-");

    jedis.sadd(key, members.toArray(new String[] {}));

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.srem(key, members.get(i)),
        (i) -> jedis.srem(key, members.get(i))).run();

    Set<String> results = jedis.smembers(key);

    assertThat(results).isEmpty();
  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_removingFromDifferentSetsConcurrently() {

    String key1 = "key1";
    String key2 = "key2";

    List<String> members1 = makeMemberList(SET_SIZE, "member1-");
    List<String> members2 = makeMemberList(SET_SIZE, "member2-");

    jedis.sadd(key1, members1.toArray(new String[] {}));
    jedis.sadd(key2, members2.toArray(new String[] {}));

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.srem(key1, members1.get(i)),
        (i) -> jedis.srem(key2, members2.get(i))).run();

    Set<String> results1 = jedis.smembers(key1);
    Set<String> results2 = jedis.smembers(key2);

    assertThat(results1).isEmpty();
    assertThat(results2).isEmpty();

  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_givenMultipleClientsOnSameServer_removingSameDataFromSameSetConcurrently() {

    //Jedis jedis1B = new Jedis(LOCAL_HOST, redisServerPort1);
    //Jedis jedis2B = new Jedis(LOCAL_HOST, redisServerPort2);

    String key = "key";

    List<String> members = makeMemberList(SET_SIZE, "member1-");
    jedis.sadd(key, members.toArray(new String[] {}));

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.srem(key, members.get(i)),
        //(i) -> jedis1B.srem(key, members.get(i)),
        //(i) -> jedis2.srem(key, members.get(i)),
        (i) -> jedis.srem(key, members.get(i))).run();

    Set<String> results = jedis.smembers(key);

    assertThat(results).isEmpty();

    //jedis1B.disconnect();
    //jedis2B.disconnect();
  }

  @Test
  public void shouldDistributeDataAmongMultipleServers_givenMultipleClientsOnSameServer_removingDifferentDataFromSameSetConcurrently() {

    //Jedis jedis1B = new Jedis(LOCAL_HOST, redisServerPort1);
    //Jedis jedis2B = new Jedis(LOCAL_HOST, redisServerPort2);

    String key = "key1";

    List<String> members1 = makeMemberList(SET_SIZE, "member1-");
    List<String> members2 = makeMemberList(SET_SIZE, "member2-");

    List<String> allMembers = new ArrayList<>();
    allMembers.addAll(members1);
    allMembers.addAll(members2);

    jedis.sadd(key, allMembers.toArray(new String[] {}));

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.srem(key, members1.get(i)),
        //(i) -> jedis1B.srem(key, members1.get(i)),
        //(i) -> jedis2.srem(key, members2.get(i)),
        (i) -> jedis.srem(key, members2.get(i))).run();

    Set<String> results = jedis.smembers(key);

    assertThat(results).isEmpty();

    //jedis1B.disconnect();
    //jedis2B.disconnect();
  }

  private List<String> makeMemberList(int setSize, String baseString) {
    List<String> members = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      members.add(baseString + i);
    }
    return members;
  }
}
