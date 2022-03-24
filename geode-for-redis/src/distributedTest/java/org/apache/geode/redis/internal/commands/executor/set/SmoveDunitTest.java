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
package org.apache.geode.redis.internal.commands.executor.set;

import static org.apache.geode.redis.internal.services.RegionProvider.DEFAULT_REDIS_REGION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class SmoveDunitTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUp = new RedisClusterStartupRule(4);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int SET_SIZE = 1000;
  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static JedisCluster jedis;
  public static RedisSet srcValueReference;
  public static RedisSet destValueReference;

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  @BeforeClass
  public static void classSetup() {
    locator = clusterStartUp.startLocatorVM(0);
    server1 = clusterStartUp.startRedisVM(1, locator.getPort());
    server2 = clusterStartUp.startRedisVM(2, locator.getPort());
    server3 = clusterStartUp.startRedisVM(3, locator.getPort());

    int redisServerPort = clusterStartUp.getRedisPort(1);
    jedis = new JedisCluster(new HostAndPort(LOCAL_HOST, redisServerPort), JEDIS_TIMEOUT);
  }

  @Before
  public void testSetup() {
    clusterStartUp.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
  }

  @Test
  public void shouldDistributeDataAmongCluster() {
    String hashTag = "{" + clusterStartUp.getKeyOnServer("tag", 1) + "}";
    String srcKey = hashTag + "srcKey";
    String destKey = hashTag + "destKey";
    String memberToMove = "member1-20";

    List<String> members = makeMemberList(SET_SIZE, "member1-");

    jedis.sadd(srcKey, members.toArray(new String[] {}));
    jedis.smove(srcKey, destKey, memberToMove);

    Set<String> srcKeyResult = jedis.smembers(srcKey);
    Set<String> destKeyResult = jedis.smembers(destKey);

    assertThat(srcKeyResult.toArray()).doesNotContain(memberToMove);
    assertThat(destKeyResult.toArray()).contains(memberToMove);
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenConcurrentlyPerformingSmoveOnSameDestinationSet() {
    String hashTag = "{" + clusterStartUp.getKeyOnServer("tag", 1) + "}";
    String srcKey1 = hashTag + "srcKey1";
    String srcKey2 = hashTag + "srcKey2";
    String destKey = hashTag + "destKey";
    String memberToMove1 = "member1-20";
    String memberToMove2 = "member2-20";

    List<String> members1 = makeMemberList(SET_SIZE, "member1-");
    List<String> members2 = makeMemberList(SET_SIZE, "member2-");

    jedis.sadd(srcKey1, members1.toArray(new String[] {}));
    jedis.sadd(srcKey2, members2.toArray(new String[] {}));

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.smove(srcKey1, destKey, memberToMove1),
        (i) -> jedis.smove(srcKey2, destKey, memberToMove2)).runInLockstep();

    Set<String> srcKeyResult1 = jedis.smembers(srcKey1);
    Set<String> srcKeyResult2 = jedis.smembers(srcKey2);
    Set<String> destKeyResult = jedis.smembers(destKey);

    assertThat(srcKeyResult1.toArray()).doesNotContain(memberToMove1);
    assertThat(srcKeyResult2.toArray()).doesNotContain(memberToMove2);
    assertThat(destKeyResult.toArray()).contains(memberToMove1);
    assertThat(destKeyResult.toArray()).contains(memberToMove2);
  }

  @Test
  public void shouldDistributeDataAmongCluster_givenConcurrentlyPerformingSmoveOnSameSourceSet() {
    String hashTag = "{" + clusterStartUp.getKeyOnServer("tag", 1) + "}";
    String srcKey = hashTag + "srcKey";
    String destKey1 = hashTag + "destKey1";
    String destKey2 = hashTag + "destKey2";
    String memberToMove1 = "member1-20";
    String memberToMove2 = "member1-10";

    List<String> members1 = makeMemberList(SET_SIZE, "member1-");

    jedis.sadd(srcKey, members1.toArray(new String[] {}));

    new ConcurrentLoopingThreads(SET_SIZE,
        (i) -> jedis.smove(srcKey, destKey1, memberToMove1),
        (i) -> jedis.smove(srcKey, destKey2, memberToMove2)).runInLockstep();

    Set<String> srcKeyResult = jedis.smembers(srcKey);
    Set<String> destKeyResult1 = jedis.smembers(destKey1);
    Set<String> destKeyResult2 = jedis.smembers(destKey2);

    assertThat(srcKeyResult.toArray()).doesNotContain(memberToMove1);
    assertThat(srcKeyResult.toArray()).doesNotContain(memberToMove2);
    assertThat(destKeyResult1.toArray()).contains(memberToMove1);
    assertThat(destKeyResult2.toArray()).contains(memberToMove2);
  }

  @Test
  public void testSmove_isTransactional() {
    String hashTag = "{" + clusterStartUp.getKeyOnServer("tag", 1) + "}";
    String srcKey = hashTag + "srcKey";
    String destKey = hashTag + "destKey";
    String memberToMove = "member1-20";
    List<String> members = makeMemberList(SET_SIZE, "member1-");
    jedis.sadd(srcKey, members.toArray(new String[] {}));

    Set<String> srcKeyResult = jedis.smembers(srcKey);
    Set<String> destKeyResult = jedis.smembers(destKey);

    clusterStartUp.getMember(1).invoke(() -> {
      srcValueReference =
          (RedisSet) ClusterStartupRule.getCache().getRegion(DEFAULT_REDIS_REGION_NAME)
              .get(new RedisKey(srcKey.getBytes()));
      destValueReference =
          (RedisSet) ClusterStartupRule.getCache().getRegion(DEFAULT_REDIS_REGION_NAME)
              .get(new RedisKey(srcKey.getBytes()));
      assertThat(srcKeyResult.toArray()).hasSize(members.size());
      assertThat(destKeyResult.toArray()).isEmpty();
    });

    jedis.smove(srcKey, destKey, memberToMove);
    Set<String> updatedSrcKeyResult = jedis.smembers(srcKey);
    Set<String> updatedDestKeyResult = jedis.smembers(destKey);

    clusterStartUp.getMember(1).invoke(() -> {
      RedisSet newSrcValueReference =
          (RedisSet) ClusterStartupRule.getCache().getRegion(DEFAULT_REDIS_REGION_NAME)
              .get(new RedisKey(srcKey.getBytes()));
      RedisSet newDestValueReference =
          (RedisSet) ClusterStartupRule.getCache().getRegion(DEFAULT_REDIS_REGION_NAME)
              .get(new RedisKey(srcKey.getBytes()));
      assertThat(newSrcValueReference == srcValueReference)
          .as("References to old and updated source should be different").isFalse();
      assertThat(newDestValueReference == destValueReference)
          .as("References to old and updated destination should be different").isFalse();
      assertThat(updatedSrcKeyResult.toArray()).doesNotContain(memberToMove);
      assertThat(updatedDestKeyResult.toArray()).contains(memberToMove);
    });
  }

  private List<String> makeMemberList(int setSize, String baseString) {
    List<String> members = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      members.add(baseString + i);
    }
    return members;
  }
}
