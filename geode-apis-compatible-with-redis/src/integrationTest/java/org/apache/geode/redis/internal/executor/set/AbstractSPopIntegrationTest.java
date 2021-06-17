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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractSPopIntegrationTest implements RedisIntegrationTest {
  private JedisCluster jedis;
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
  public void givenKeyNotProvided_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", Protocol.Command.SPOP))
        .hasMessageContaining("ERR wrong number of arguments for 'spop' command");
  }

  @Test
  public void givenMoreThanThreeArguments_returnsSyntaxError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.SPOP, "key", "NaN", "extraArg"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCountIsNotAnInteger_returnsNotIntegerError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", Protocol.Command.SPOP, "key", "NaN"))
        .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void testSPop() {
    int ENTRIES = 10;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));
    String poppy = jedis.spop("master");

    masterSet.remove(poppy);
    assertThat(jedis.smembers("master").toArray()).containsExactlyInAnyOrder(masterSet.toArray());

    assertThat(jedis.spop("spopnonexistent")).isNull();
  }

  @Test
  public void testSPopAll() {
    int ENTRIES = 10;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));
    Set<String> popped = jedis.spop("master", ENTRIES);

    assertThat(jedis.smembers("master").toArray()).isEmpty();
    assertThat(popped.toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }

  @Test
  public void testSPopAllPlusOne() {
    int ENTRIES = 10;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));
    Set<String> popped = jedis.spop("master", ENTRIES + 1);

    assertThat(jedis.smembers("master").toArray()).isEmpty();
    assertThat(popped.toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }

  @Test
  public void testSPopAllMinusOne() {
    int ENTRIES = 10;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));
    Set<String> popped = jedis.spop("master", ENTRIES - 1);

    assertThat(jedis.smembers("master").toArray()).hasSize(1);
    assertThat(popped).hasSize(ENTRIES - 1);
    assertThat(masterSet).containsAll(popped);
  }

  @Test
  public void testManySPops() {
    int ENTRIES = 100;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    List<String> popped = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      popped.add(jedis.spop("master"));
    }

    assertThat(jedis.smembers("master")).isEmpty();
    assertThat(popped.toArray()).containsExactlyInAnyOrder(masterSet.toArray());

    assertThat(jedis.spop("master")).isNull();
  }

  @Test
  public void testConcurrentSPops() throws InterruptedException {
    int ENTRIES = 1000;

    List<String> masterSet = new ArrayList<>();
    for (int i = 0; i < ENTRIES; i++) {
      masterSet.add("master-" + i);
    }

    jedis.sadd("master", masterSet.toArray(new String[] {}));

    List<String> popped1 = new ArrayList<>();
    Runnable runnable1 = () -> {
      for (int i = 0; i < ENTRIES / 2; i++) {
        popped1.add(jedis.spop("master"));
      }
    };

    List<String> popped2 = new ArrayList<>();
    Runnable runnable2 = () -> {
      for (int i = 0; i < ENTRIES / 2; i++) {
        popped2.add(jedis.spop("master"));
      }
    };

    Thread thread1 = new Thread(runnable1);
    Thread thread2 = new Thread(runnable2);

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();

    assertThat(jedis.smembers("master")).isEmpty();

    popped1.addAll(popped2);
    assertThat(popped1.toArray()).containsExactlyInAnyOrder(masterSet.toArray());
  }

  @Test
  public void testSPopWithOutCount_shouldReturnNil_givenEmptySet() {
    Object result = jedis.sendCommand("noneSuch", Protocol.Command.SPOP, "noneSuch");

    assertThat(result).isNull();
  }

  @Test
  public void testSPopWithCount_shouldReturnEmptyList_givenEmptySet() {
    Set<String> result = jedis.spop("noneSuch", 2);

    assertThat(result).isEmpty();
  }

  @Test
  public void testSPopWithCountOfOne_shouldReturnList() {
    jedis.sadd("set", "one");

    Object actual = jedis.sendCommand("set", Protocol.Command.SPOP, "set", "1");

    assertThat(actual).isInstanceOf(List.class);
  }

  @Test
  public void testSPopWithoutCount_shouldNotReturnList() {
    jedis.sadd("set", "one");

    Object actual = jedis.sendCommand("set", Protocol.Command.SPOP, "set");

    assertThat(actual).isNotInstanceOf(List.class);
  }

  @Test
  public void testSPopWithCountZero_shouldReturnEmptyList() {
    jedis.sadd("set", "one");

    Set<String> result = jedis.spop("set", 0);

    assertThat(result).isEmpty();
  }

  @Test
  public void testSPopWithoutArg_shouldReturnBulkString() throws Exception {
    jedis.sadd("set", "one");

    try (Socket redisSocket = new Socket("localhost", getPort())) {
      byte[] rawBytes = new byte[] {
          '*', '2', 0x0d, 0x0a,
          '$', '4', 0x0d, 0x0a,
          'S', 'P', 'O', 'P', 0x0d, 0x0a,
          '$', '3', 0x0d, 0x0a,
          's', 'e', 't', 0x0d, 0x0a,
      };

      redisSocket.getOutputStream().write(rawBytes);

      byte[] inputBuffer = new byte[1024];
      int n = redisSocket.getInputStream().read(inputBuffer);
      String result = bytesToString(Arrays.copyOfRange(inputBuffer, 0, n));

      assertThat(result).isEqualTo("$3\r\none\r\n");
    }
  }
}
