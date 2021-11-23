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
package org.apache.geode.redis.internal.commands.executor.string;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractMSetNXIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private static final String HASHTAG = "{111}";

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
  public void givenKeyNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand("any", Protocol.Command.MSETNX))
        .hasMessageContaining("ERR wrong number of arguments for 'msetnx' command");
  }

  @Test
  public void givenValueNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", Protocol.Command.MSETNX, "key"))
        .hasMessageContaining("ERR wrong number of arguments for 'msetnx' command");
  }

  @Test
  public void givenEvenNumberOfArgumentsProvided_returnsWrongNumberOfArgumentsError() {
    // Redis returns this message in this scenario: "ERR wrong number of arguments for MSETNX"
    assertThatThrownBy(
        () -> jedis.sendCommand(HASHTAG, Protocol.Command.MSETNX, "key1" + HASHTAG, "value1",
            "key2" + HASHTAG, "value2", "key3" + HASHTAG))
                .hasMessageContaining("ERR wrong number of arguments");
  }

  @Test
  public void givenDifferentSlots_returnsError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("key1", Protocol.Command.MSETNX, "key1", "value1", "key2",
            "value2"))
                .hasMessageContaining("CROSSSLOT Keys in request don't hash to the same slot");
  }

  @Test
  public void testMSet_clearsExpiration() {
    jedis.setex("foo", 20L, "bar");
    jedis.mset("foo", "baz");

    assertThat(jedis.ttl("foo")).isEqualTo(-1);
  }

  @Test
  public void testDoesntSetAny_whenAnyTargetKeyExists() {
    String[] keys = new String[5];

    for (int i = 0; i < keys.length; i++) {
      keys[i] = HASHTAG + "key" + i;
    }
    String[] keysAndValues = makeKeysAndValues(keys, "valueOne");

    long response = jedis.msetnx(keysAndValues);
    assertThat(response).isOne();

    long response2 = jedis.msetnx(keysAndValues[0], randString());
    assertThat(response2).isZero();
    assertThat(keysAndValues[1]).isEqualTo(jedis.get(keysAndValues[0]));

    flushAll();
    jedis.set(keysAndValues[0], "foo");

    long response3 = jedis.msetnx(keysAndValues);
    assertThat(response3).isZero();
    List<String> values = jedis.mget(keys);
    assertThat(values).containsExactly("foo", null, null, null, null);
  }

  @Test
  public void testMSet_setsKeysAndReturnsCorrectValues() {
    int keyCount = 5;
    String[] keyvals = new String[(keyCount * 2)];
    String[] keys = new String[keyCount];
    String[] vals = new String[keyCount];
    for (int i = 0; i < keyCount; i++) {
      String key = randString() + HASHTAG;
      String val = randString();
      keyvals[2 * i] = key;
      keyvals[2 * i + 1] = val;
      keys[i] = key;
      vals[i] = val;
    }

    long result = jedis.msetnx(keyvals);
    assertThat(result).isEqualTo(1);

    assertThat(jedis.mget(keys)).containsExactly(vals);
  }

  @Test
  public void testMSet_concurrentInstances_mustBeAtomic() {
    int KEY_COUNT = 5000;
    String[] keys = new String[KEY_COUNT];

    for (int i = 0; i < keys.length; i++) {
      keys[i] = HASHTAG + "key" + i;
    }
    String[] keysAndValues1 = makeKeysAndValues(keys, "valueOne");
    String[] keysAndValues2 = makeKeysAndValues(keys, "valueTwo");

    new ConcurrentLoopingThreads(1000,
        i -> jedis.msetnx(keysAndValues1),
        i -> jedis.msetnx(keysAndValues2))
            .runWithAction(() -> {
              assertThat(jedis.mget(keys)).satisfiesAnyOf(
                  values -> assertThat(values)
                      .allSatisfy(value -> assertThat(value).startsWith("valueOne")),
                  values -> assertThat(values)
                      .allSatisfy(value -> assertThat(value).startsWith("valueTwo")));
              flushAll();
            });
  }

  private String[] makeKeysAndValues(String[] keys, String valueBase) {
    String[] keysValues = new String[keys.length * 2];
    for (int i = 0; i < keys.length * 2; i += 2) {
      keysValues[i] = keys[i / 2];
      keysValues[i + 1] = valueBase + i;
    }

    return keysValues;
  }


  private String randString() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }
}
