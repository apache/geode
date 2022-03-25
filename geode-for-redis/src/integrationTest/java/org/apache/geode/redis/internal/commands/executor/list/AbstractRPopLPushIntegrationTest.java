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
package org.apache.geode.redis.internal.commands.executor.list;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_SLOT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.Protocol.Command.RPOPLPUSH;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractRPopLPushIntegrationTest implements RedisIntegrationTest {
  private static final String SOURCE_KEY = "{tag}source";
  private static final String DESTINATION_KEY = "{tag}destination";
  private JedisCluster jedis;

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
  public void rPopLPush_withWrongNumberOfArguments_returnsError() {
    assertExactNumberOfArgs(jedis, RPOPLPUSH, 2);
  }

  @Test
  public void rPopLPush_withKeysInDifferentSlots_returnsCrossSlotError() {
    final String key1 = "{1}key1";
    final String key2 = "{2}key2";

    // Neither key exists
    assertThatThrownBy(() -> jedis.sendCommand(RPOPLPUSH, key1, key2)).hasMessage(ERROR_WRONG_SLOT);

    jedis.lpush(key1, "1", "2", "3");

    // Source key exists
    assertThatThrownBy(() -> jedis.sendCommand(RPOPLPUSH, key1, key2)).hasMessage(ERROR_WRONG_SLOT);

    // Destination key exists
    assertThatThrownBy(() -> jedis.sendCommand(RPOPLPUSH, key2, key1)).hasMessage(ERROR_WRONG_SLOT);

    jedis.lpush(key2, "a", "b", "c");

    // Both keys exist
    assertThatThrownBy(() -> jedis.sendCommand(RPOPLPUSH, key1, key2)).hasMessage(ERROR_WRONG_SLOT);
  }

  @Test
  public void rPopLPush_withNonListSourceKey_returnsWrongTypeError() {
    jedis.set(SOURCE_KEY, "not_a_list");

    assertThatThrownBy(() -> jedis.rpoplpush(SOURCE_KEY, DESTINATION_KEY))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void rPopLPush_withNonListDestinationKey_returnsWrongTypeError() {
    jedis.lpush(SOURCE_KEY, "1", "2", "3");
    jedis.set(DESTINATION_KEY, "not_a_list");

    assertThatThrownBy(() -> jedis.rpoplpush(SOURCE_KEY, DESTINATION_KEY))
        .hasMessage(ERROR_WRONG_TYPE);
  }

  @Test
  public void rPopLPush_withNonexistentSourceKey_returnsNull() {
    assertThat(jedis.rpoplpush(SOURCE_KEY, DESTINATION_KEY)).isNull();
  }

  @Test
  public void rPopLPush_withNonexistentSourceKey_doesNotCreateListAtDestination() {
    jedis.rpoplpush(SOURCE_KEY, DESTINATION_KEY);
    assertThat(jedis.exists(DESTINATION_KEY)).isFalse();
  }

  @Test
  public void rPopLPush_withNonexistentSourceKey_doesNotModifyListAtDestination() {
    jedis.lpush(DESTINATION_KEY, "a", "b", "c");
    jedis.rpoplpush(SOURCE_KEY, DESTINATION_KEY);
    assertThat(jedis.lrange(DESTINATION_KEY, 0, -1)).containsExactly("c", "b", "a");
  }

  @Test
  public void rPopLPush_returnsPoppedElement() {
    jedis.lpush(SOURCE_KEY, "1", "2", "3");
    assertThat(jedis.rpoplpush(SOURCE_KEY, DESTINATION_KEY)).isEqualTo("1");
  }

  @Test
  public void rPopLPush_removesRightmostElementFromSource_andAddsToLeftOfDestination() {
    jedis.lpush(SOURCE_KEY, "1", "2", "3");
    jedis.lpush(DESTINATION_KEY, "a", "b", "c");
    jedis.rpoplpush(SOURCE_KEY, DESTINATION_KEY);

    assertThat(jedis.lrange(SOURCE_KEY, 0, -1)).containsExactly("3", "2");
    assertThat(jedis.lrange(DESTINATION_KEY, 0, -1)).containsExactly("1", "c", "b", "a");
  }

  @Test
  public void rPopLPush_createsListAtDestination_whenDestinationKeyIsEmpty() {
    jedis.lpush(SOURCE_KEY, "1", "2", "3");
    jedis.rpoplpush(SOURCE_KEY, DESTINATION_KEY);

    assertThat(jedis.lrange(DESTINATION_KEY, 0, -1)).containsExactly("1");
  }

  @Test
  public void rPopLPush_removesSourceList_whenLastElementIsPopped() {
    jedis.lpush(SOURCE_KEY, "1");
    jedis.rpoplpush(SOURCE_KEY, DESTINATION_KEY);

    assertThat(jedis.exists(SOURCE_KEY)).isFalse();
    assertThat(jedis.lrange(DESTINATION_KEY, 0, -1)).containsExactly("1");
  }

  @Test
  public void rPopLPush_rotatesList_whenSourceAndDestinationKeyAreTheSame() {
    jedis.lpush(SOURCE_KEY, "1", "2", "3");
    jedis.rpoplpush(SOURCE_KEY, SOURCE_KEY);

    assertThat(jedis.lrange(SOURCE_KEY, 0, -1)).containsExactly("1", "3", "2");
  }

  @Test
  public void rPopLPush_withConcurrentRPush_popsCorrectValue() {
    String[] initialElements = new String[] {"a", "b", "c"};
    String[] valuesToAdd = new String[] {"1", "2", "3"};
    jedis.rpush(SOURCE_KEY, initialElements);

    final AtomicReference<String> rPopLPushReference = new AtomicReference<>();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.rpush(SOURCE_KEY, valuesToAdd),
        i -> rPopLPushReference.set(jedis.rpoplpush(SOURCE_KEY, DESTINATION_KEY)))
            .runWithAction(() -> {
              assertThat(rPopLPushReference.get()).satisfiesAnyOf(
                  // RPOPLPUSH was first
                  rpopResult -> {
                    assertThat(rpopResult).isEqualTo("c");
                    assertThat(jedis.lrange(DESTINATION_KEY, 0, -1)).containsExactly("c");
                    assertThat(jedis.lrange(SOURCE_KEY, 0, -1))
                        .containsExactly("a", "b", "1", "2", "3");
                  },
                  // RPOP was first
                  rpopResult -> {
                    assertThat(rpopResult).isEqualTo("3");
                    assertThat(jedis.lrange(DESTINATION_KEY, 0, -1)).containsExactly("3");
                    assertThat(jedis.lrange(SOURCE_KEY, 0, -1))
                        .containsExactly("a", "b", "c", "1", "2");
                  });
              jedis.del(SOURCE_KEY);
              jedis.del(DESTINATION_KEY);
              jedis.rpush(SOURCE_KEY, initialElements);
            });
  }
}
