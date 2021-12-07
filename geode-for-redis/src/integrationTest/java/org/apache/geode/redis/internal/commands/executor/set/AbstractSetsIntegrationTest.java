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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractSetsIntegrationTest implements RedisIntegrationTest {

  private JedisCluster jedis;
  private static final ThreePhraseGenerator generator = new ThreePhraseGenerator();
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void saddErrors_givenTooFewArguments() {
    assertAtLeastNArgs(jedis, Protocol.Command.SADD, 2);
  }

  @Test
  public void testSAdd_withExistingKey_ofWrongType_shouldReturnError() {
    String key = "key";
    String stringValue = "preexistingValue";
    String[] setValue = new String[1];
    setValue[0] = "set value that should never get added";

    jedis.set(key, stringValue);
    assertThatThrownBy(() -> jedis.sadd(key, setValue))
        .hasMessageContaining("Operation against a key holding the wrong kind of value");
  }

  @Test
  public void testSAdd_withExistingKey_ofWrongType_shouldNotOverWriteExistingKey() {
    String key = "key";
    String stringValue = "preexistingValue";
    String[] setValue = new String[1];
    setValue[0] = "set value that should never get added";

    jedis.set(key, stringValue);

    assertThatThrownBy(() -> jedis.sadd(key, setValue)).isInstanceOf(JedisDataException.class);

    String result = jedis.get(key);

    assertThat(result).isEqualTo(stringValue);
  }

  @Test
  public void testSAdd_canStoreBinaryData() {
    byte[] blob = new byte[256];
    for (int i = 0; i < 256; i++) {
      blob[i] = (byte) i;
    }

    jedis.sadd("key".getBytes(), blob, blob);
    Set<byte[]> result = jedis.smembers("key".getBytes());

    assertThat(result).containsExactly(blob);
  }

  @Test
  public void srandmember_withStringFails() {
    jedis.set("string", "value");
    assertThatThrownBy(() -> jedis.srandmember("string")).hasMessageContaining("WRONGTYPE");
  }

  @Test
  public void srandmember_withNonExistentKeyReturnsNull() {
    assertThat(jedis.srandmember("non existent")).isNull();
  }

  @Test
  public void srandmemberCount_withNonExistentKeyReturnsEmptyArray() {
    assertThat(jedis.srandmember("non existent", 3)).isEmpty();
  }

  @Test
  public void srandmember_returnsOneMember() {
    jedis.sadd("key", "m1", "m2");
    String result = jedis.srandmember("key");
    assertThat(result).isIn("m1", "m2");
  }

  @Test
  public void srandmemberCount_returnsTwoUniqueMembers() {
    jedis.sadd("key", "m1", "m2", "m3");
    List<String> results = jedis.srandmember("key", 2);
    assertThat(results).hasSize(2);
    assertThat(results).containsAnyOf("m1", "m2", "m3");
    assertThat(results.get(0)).isNotEqualTo(results.get(1));
  }

  @Test
  public void srandmemberNegativeCount_returnsThreeMembers() {
    jedis.sadd("key", "m1", "m2", "m3");
    List<String> results = jedis.srandmember("key", -3);
    assertThat(results).hasSize(3);
    assertThat(results).containsAnyOf("m1", "m2", "m3");
  }

  @Test
  public void srandmemberNegativeCount_givenSmallSet_returnsThreeMembers() {
    jedis.sadd("key", "m1");
    List<String> results = jedis.srandmember("key", -3);
    assertThat(results).hasSize(3);
    assertThat(results).containsAnyOf("m1");
  }

  @Test
  public void smembers_givenKeyNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", Protocol.Command.SMEMBERS))
        .hasMessageContaining("ERR wrong number of arguments for 'smembers' command");
  }

  @Test
  public void smembers_givenMoreThanTwoArguments_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis
        .sendCommand("key", Protocol.Command.SMEMBERS, "key", "extraArg"))
            .hasMessageContaining("ERR wrong number of arguments for 'smembers' command");
  }

  @Test
  public void testSMembers() {
    int elements = 10;
    String key = generator.generate('x');

    String[] strings = generateStrings(elements, 'y');
    jedis.sadd(key, strings);

    Set<String> returnedSet = jedis.smembers(key);

    assertThat(returnedSet).containsExactlyInAnyOrder(strings);
  }

  @Test
  public void testSMembersWithNonexistentKey_returnsEmptySet() {
    assertThat(jedis.smembers("doesNotExist")).isEmpty();
  }

  private String[] generateStrings(int elements, char uniqueElement) {
    Set<String> strings = new HashSet<>();
    for (int i = 0; i < elements; i++) {
      String elem = generator.generate(uniqueElement);
      strings.add(elem);
    }
    return strings.toArray(new String[strings.size()]);
  }
}
