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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RedisConstants;

public abstract class AbstractSMoveIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String nonExistentSetKey = "{user1}nonExistentSet";
  private static final String sourceKey = "{user1}sourceKey";
  private static final String[] sourceMembers = {"one", "two", "three", "four", "five"};
  private static final String destKey = "{user1}destKey";
  private static final String[] destMembers = {"a", "b", "c"};
  private static final String movedMember = "one";

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void smove_givenWrongNumberOfArguments_returnsError() {
    assertExactNumberOfArgs(jedis, Protocol.Command.SMOVE, 3);
  }

  @Test
  public void smove_withWrongTypeSource_returnsWrongTypeError() {
    jedis.set(sourceKey, "value");
    jedis.sadd(destKey, destMembers);

    assertThatThrownBy(() -> jedis.smove(sourceKey, destKey, movedMember))
        .hasMessageContaining(RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void smove_withWrongTypeDest_returnsWrongTypeError() {
    jedis.sadd(sourceKey, sourceMembers);
    jedis.set(destKey, "value");

    assertThatThrownBy(() -> jedis.smove(sourceKey, destKey, movedMember))
        .hasMessageContaining(RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void smove_withWrongTypeSourceAndDest_returnsWrongTypeError() {
    jedis.set(sourceKey, "sourceMember");
    jedis.set(destKey, "destMember");

    assertThatThrownBy(() -> jedis.smove(sourceKey, destKey, movedMember))
        .hasMessageContaining(RedisConstants.ERROR_WRONG_TYPE);
  }

  @Test
  public void smove_withNonExistentSource_returnsZero_sourceKeyDoesNotExist() {
    jedis.sadd(destKey, destMembers);

    assertThat(jedis.smove(nonExistentSetKey, destKey, movedMember))
        .isEqualTo(0);
    assertThat(jedis.exists(nonExistentSetKey)).isFalse();
  }

  @Test
  public void smove_withNonExistentMemberInSource_returnsZero_memberNotAddedToDest() {
    String nonExistentMember = "foo";
    jedis.sadd(sourceKey, sourceMembers);
    jedis.sadd(destKey, destMembers);

    assertThat(jedis.smove(nonExistentSetKey, destKey, nonExistentMember))
        .isEqualTo(0);
    assertThat(jedis.sismember(destKey, nonExistentMember)).isFalse();
  }

  @Test
  public void smove_withExistentSourceAndNonExistentDest_returnsOne_memberMovedFromSourceToCreatedDest() {
    jedis.sadd(sourceKey, sourceMembers);

    String[] sourceResult = ArrayUtils.remove(sourceMembers, 0);
    String[] destResult = new String[] {movedMember};

    assertThat(jedis.smove(sourceKey, destKey, movedMember))
        .isEqualTo(1);

    assertThat(jedis.smembers(sourceKey)).containsExactlyInAnyOrder(sourceResult);
    assertThat(jedis.smembers(destKey)).containsExactlyInAnyOrder(destResult);
  }

  @Test
  public void smove_withExistentSourceAndDest_returnsOne_memberMovedFromSourceToDest() {
    jedis.sadd(sourceKey, sourceMembers);
    jedis.sadd(destKey, destMembers);

    String[] sourceResult = ArrayUtils.remove(sourceMembers, 0);
    String[] destResult = ArrayUtils.add(destMembers, movedMember);

    assertThat(jedis.smove(sourceKey, destKey, movedMember))
        .isEqualTo(1);

    assertThat(jedis.smembers(sourceKey)).containsExactlyInAnyOrder(sourceResult);
    assertThat(jedis.smembers(destKey)).containsExactlyInAnyOrder(destResult);
  }

  @Test
  public void smove_withExistentSourceAndDest_withMemberInDest_returnsOne_memberRemovedFromSource() {
    jedis.sadd(sourceKey, sourceMembers);
    String[] newDestMembers = ArrayUtils.add(destMembers, movedMember);
    jedis.sadd(destKey, newDestMembers);

    String[] sourceResult = ArrayUtils.remove(sourceMembers, 0);

    assertThat(jedis.smove(sourceKey, destKey, movedMember))
        .isEqualTo(1);

    assertThat(jedis.smembers(sourceKey)).containsExactlyInAnyOrder(sourceResult);
    assertThat(jedis.smembers(destKey)).containsExactlyInAnyOrder(newDestMembers);
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    String[] sourceMemberRemoved = ArrayUtils.remove(sourceMembers, 0);
    String[] destMemberAdded = ArrayUtils.add(destMembers, movedMember);

    jedis.sadd(sourceKey, sourceMembers);
    jedis.sadd(destKey, destMembers);

    final AtomicLong moved = new AtomicLong(0);
    new ConcurrentLoopingThreads(1000,
        i -> jedis.srem(sourceKey, movedMember),
        i -> moved.set(jedis.smove(sourceKey, destKey, movedMember)))
            .runWithAction(() -> {
              // Check sdiffstore return size of diff
              assertThat(moved).satisfiesAnyOf(
                  smoveResult -> assertThat(smoveResult.get()).isEqualTo(0),
                  smoveResult -> assertThat(smoveResult.get()).isEqualTo(1));
              // Checks if values were moved or not from source key
              assertThat(sourceKey).satisfiesAnyOf(
                  source -> assertThat(jedis.smembers(source))
                      .containsExactlyInAnyOrder(sourceMembers),
                  source -> assertThat(jedis.smembers(source))
                      .containsExactlyInAnyOrder(sourceMemberRemoved));
              // Checks if values were moved or not to destination key
              assertThat(destKey).satisfiesAnyOf(
                  dest -> assertThat(jedis.smembers(dest))
                      .containsExactlyInAnyOrder(destMembers),
                  dest -> assertThat(jedis.smembers(dest))
                      .containsExactlyInAnyOrder(destMemberAdded));
              jedis.sadd(sourceKey, movedMember);
              jedis.srem(destKey, movedMember);
            });
  }
}
