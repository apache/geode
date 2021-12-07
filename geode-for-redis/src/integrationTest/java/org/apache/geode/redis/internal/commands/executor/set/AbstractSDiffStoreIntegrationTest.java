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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSDiffStoreIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;

  private static final String destinationKey = "{user1}destinationKey";
  private static final String[] destinationMembers =
      {"six", "seven", "eight", "nine", "ten", "one", "two"};
  private static final String setKey = "{user1}setKey";
  private static final String[] setMembers = {"one", "two", "three", "four", "five"};
  private static final String nonExistentSetKey = "{user1}nonExistentSet";

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
  public void sdiffstoreTooFewArguments_returnsError() {
    assertAtLeastNArgs(jedis, Protocol.Command.SDIFFSTORE, 2);
  }

  @Test
  public void sdiffstore_DifferentyKeyType_returnsWrongTypeError() {
    jedis.set("{user1}ding", "{user1}dong");
    assertThatThrownBy(() -> jedis.sdiffstore(destinationKey, "{user1}ding"))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  // Destination Key does not have members
  @Test
  public void sdiffstoreNonExistentDest_Set_returnsSetSize() {
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey)).isEqualTo(setMembers.length);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void sdiffstoreNonExistentDest_NonExistentSet_returnsZero() {
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, nonExistentSetKey)).isEqualTo(0);
    assertThat(jedis.smembers(destinationKey)).isEmpty();
  }

  @Test
  public void sdiffstoreNonExistentDest_SetAndNonExistent_returnsSetSize() {
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey, nonExistentSetKey))
        .isEqualTo(setMembers.length);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void sdiffstoreNonExistentDest_NonExistentAndSet_returnsZero() {
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, nonExistentSetKey, setKey)).isEqualTo(0);
    assertThat(jedis.smembers(destinationKey)).isEmpty();
  }

  @Test
  public void sdiffstoreNonExistentDest_MultipleSets_returnsSetSize() {
    String key = "{user1}key";
    String[] result = {"three", "four", "five"};
    jedis.sadd(setKey, setMembers);
    jedis.sadd(key, destinationMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey, key)).isEqualTo(3);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(result);
  }

  @Test
  public void sdiffstoreNonExistentDest_DifferentSets_returnsSetSize() {
    String key = "{user1}key";
    String[] members = {"nine", "twelve"};
    jedis.sadd(setKey, setMembers);
    jedis.sadd(key, members);
    assertThat(jedis.sdiffstore(destinationKey, setKey, key)).isEqualTo(setMembers.length);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void sdiffstoreNonExistentDest_SameSets_returnsZero() {
    String key = "{user1}key";
    jedis.sadd(setKey, setMembers);
    jedis.sadd(key, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey, key)).isEqualTo(0);
    assertThat(jedis.smembers(destinationKey)).isEmpty();
  }

  @Test
  public void sdiffstoreNonExistentDest_NonexistentSets_returnsZero() {
    assertThat(jedis.sdiffstore(destinationKey, nonExistentSetKey, "{user1}nonExistentKey2"))
        .isEqualTo(0);
    assertThat(jedis.smembers(destinationKey)).isEmpty();
  }

  // Destination Key has members
  @Test
  public void sdiffstoreDest_Set_returnsSetSize() {
    jedis.sadd(destinationKey, destinationMembers);
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey)).isEqualTo(setMembers.length);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void sdiffstoreDest_NonExistentSet_returnsZero() {
    jedis.sadd(destinationKey, destinationMembers);
    assertThat(jedis.sdiffstore(destinationKey, nonExistentSetKey)).isEqualTo(0);
    assertThat(jedis.smembers(destinationKey)).isEmpty();
  }

  @Test
  public void sdiffstoreDest_SetAndNonExistent_returnsSetSize() {
    jedis.sadd(destinationKey, destinationMembers);
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey, nonExistentSetKey))
        .isEqualTo(setMembers.length);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void sdiffstoreDest_NonExistentAndSet_returnsZero() {
    jedis.sadd(destinationKey, destinationMembers);
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, nonExistentSetKey, setKey)).isEqualTo(0);
    assertThat(jedis.smembers(destinationKey)).isEmpty();
  }

  @Test
  public void sdiffstoreDest_MultipleSets_returnsSetSize() {
    String key = "{user1}key";
    String[] result = {"three", "four", "five"};
    jedis.sadd(destinationKey, destinationMembers);
    jedis.sadd(setKey, setMembers);
    jedis.sadd(key, destinationMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey, key)).isEqualTo(3);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(result);
  }

  @Test
  public void sdiffstoreDest_DifferentSets_returnsSetSize() {
    String key = "{user1}key";
    String[] members = {"nine", "twelve"};
    jedis.sadd(destinationKey, destinationMembers);
    jedis.sadd(setKey, setMembers);
    jedis.sadd(key, members);
    assertThat(jedis.sdiffstore(destinationKey, setKey, key)).isEqualTo(setMembers.length);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void sdiffstoreDest_SameSets_returnsZero() {
    String key = "{user1}key";
    jedis.sadd(destinationKey, destinationMembers);
    jedis.sadd(setKey, setMembers);
    jedis.sadd(key, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey, key)).isEqualTo(0);
    assertThat(jedis.smembers(destinationKey)).isEmpty();
  }

  @Test
  public void sdiffstoreDest_NonexistentSets_returnsZero() {
    jedis.sadd(destinationKey, destinationMembers);
    assertThat(jedis.sdiffstore(destinationKey, nonExistentSetKey, "{user1}nonExistentKey2"))
        .isEqualTo(0);
    assertThat(jedis.smembers(destinationKey)).isEmpty();
  }

  @Test
  public void sdiffstore_DifferentDestKeyType_returnsSetSize() {
    jedis.set(destinationKey, "{user1}destMember");
    jedis.sadd(setKey, setMembers);
    assertThat(jedis.sdiffstore(destinationKey, setKey)).isEqualTo(setMembers.length);
    assertThat(jedis.smembers(destinationKey)).containsExactlyInAnyOrder(setMembers);
  }

  @Test
  public void ensureSetConsistency_whenRunningConcurrently() {
    jedis.sadd("{user1}firstset", setMembers);
    jedis.sadd("{user1}secondset", setMembers);

    final AtomicLong sdiffSize = new AtomicLong(0);
    final AtomicReference<Set<String>> sdiffstoreResultReference = new AtomicReference<>();

    new ConcurrentLoopingThreads(1000,
        i -> jedis.srem("{user1}secondset", setMembers),
        i -> sdiffSize.set(jedis.sdiffstore(destinationKey, "{user1}firstset", "{user1}secondset")))
            .runWithAction(() -> {
              // Check sdiffstore return size of diff
              assertThat(sdiffSize).satisfiesAnyOf(
                  sdiffstoreSize -> assertThat(sdiffstoreSize.get()).isEqualTo(0),
                  sdiffstoreSize -> assertThat(sdiffstoreSize.get()).isEqualTo(setMembers.length));
              // Checks if values were stored in destination key
              sdiffstoreResultReference.set(jedis.smembers(destinationKey));
              assertThat(sdiffstoreResultReference).satisfiesAnyOf(
                  output -> assertThat(output.get()).isEmpty(),
                  output -> assertThat(output.get()).containsExactlyInAnyOrder(setMembers));
              jedis.sadd("{user1}secondset", setMembers);
              jedis.srem(destinationKey, setMembers);
            });
  }
}
