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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractSCardIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
  private static final String key = "{user1}setKey";

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
  public void scardWrongNumOfArgs_returnsError() {
    assertExactNumberOfArgs(jedis, Protocol.Command.SCARD, 1);
  }

  @Test
  public void scardSet_returnsSize() {
    String[] values = new String[] {"Jeff", "Natalie", "Michelle", "Joe", "Kelley"};
    jedis.sadd(key, values);
    assertThat(jedis.scard(key)).isEqualTo(values.length);
  }

  @Test
  public void scardNonExistentSet_returnsZero() {
    assertThat(jedis.scard("{user1}nonExistentSet")).isEqualTo(0);
  }

  @Test
  public void scardAfterSadd_returnsSize() {
    String[] valuesInitial = new String[] {"Jeff", "Natalie", "Michelle", "Joe", "Kelley"};
    String[] valuesToAdd = new String[] {"one", "two", "three"};

    jedis.sadd(key, valuesInitial);
    long size = jedis.scard(key);
    assertThat(size).isEqualTo(valuesInitial.length);
    jedis.sadd(key, valuesToAdd);
    assertThat(jedis.scard(key)).isEqualTo(size + valuesToAdd.length);
  }


  @Test
  public void scardConcurrentSAddSCard_sameKeyPerClient() {
    String[] valuesInitial = new String[] {"one", "two", "three"};
    String[] valuesToAdd = new String[] {"pear", "apple", "plum", "orange", "peach"};
    jedis.sadd(key, valuesInitial);

    final AtomicLong scardReference = new AtomicLong();
    new ConcurrentLoopingThreads(1000,
        i -> jedis.sadd(key, valuesToAdd),
        i -> scardReference.set(jedis.scard(key)))
            .runWithAction(() -> {
              assertThat(scardReference).satisfiesAnyOf(
                  scardResult -> assertThat(scardResult.get()).isEqualTo(valuesInitial.length),
                  scardResult -> assertThat(scardResult.get())
                      .isEqualTo(valuesInitial.length + valuesToAdd.length));
              jedis.srem(key, valuesToAdd);
              jedis.sadd(key, valuesToAdd);
            });
  }

  @Test
  public void scardWithWrongKeyType_returnsWrongTypeError() {
    jedis.set("ding", "dong");
    assertThatThrownBy(() -> jedis.scard("ding")).hasMessageContaining(ERROR_WRONG_TYPE);
  }
}
