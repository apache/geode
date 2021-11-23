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

package org.apache.geode.redis.internal.commands.executor.key;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractKeysIntegrationTest implements RedisIntegrationTest {

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
  public void errors_givenWrongNumberOfArguments() {
    assertExactNumberOfArgs(jedis, Protocol.Command.KEYS, 1);
  }

  @Test
  public void givenSplat_withASCIIdata_returnsExpectedMatches() {
    jedis.set("{K}string1", "v1");
    jedis.sadd("{K}set1", "member1");
    jedis.hset("{K}hash1", "key1", "field1");
    assertThat(jedis.keys("{K}*")).containsExactlyInAnyOrder("{K}string1", "{K}set1", "{K}hash1");
    assertThat(jedis.keys("{K}s*")).containsExactlyInAnyOrder("{K}string1", "{K}set1");
    assertThat(jedis.keys("{K}h*")).containsExactlyInAnyOrder("{K}hash1");
    assertThat(jedis.keys("{K}foo*")).isEmpty();
  }

  @Test
  public void givenSplat_withBinaryData_returnsExpectedMatches() {
    byte[] stringKey =
        new byte[] {'{', 1, '}', (byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 't', 'r', 'i', 'n', 'g',
            '1'};
    byte[] value = new byte[] {'v', '1'};
    jedis.set(stringKey, value);
    byte[] setKey =
        new byte[] {'{', 1, '}', (byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 'e', 't', '1'};
    byte[] member = new byte[] {'m', '1'};
    jedis.sadd(setKey, member);
    byte[] hashKey =
        new byte[] {'{', 1, '}', (byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 'h', 'a', 's', 'h', '1'};
    byte[] key = new byte[] {'{', 1, '}', 'k', 'e', 'y', '1'};
    jedis.hset(hashKey, key, value);
    assertThat(jedis.exists(stringKey)).isTrue();
    assertThat(jedis.exists(setKey)).isTrue();
    assertThat(jedis.exists(hashKey)).isTrue();
    assertThat(jedis.keys(new byte[] {'{', 1, '}', '*'})).containsExactlyInAnyOrder(stringKey,
        setKey, hashKey);
    assertThat(jedis.keys(new byte[] {'{', 1, '}', (byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', '*'}))
        .containsExactlyInAnyOrder(stringKey, setKey);
    assertThat(jedis.keys(new byte[] {'{', 1, '}', (byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 'h', '*'}))
        .containsExactlyInAnyOrder(hashKey);
    assertThat(jedis.keys(new byte[] {'{', 1, '}', 'f', '*'})).isEmpty();
  }

  @Test
  public void givenBinaryValue_withExactMatch_preservesBinaryData() {
    String chineseHashTag = "{子}";
    byte[] utf8encodedBytes = chineseHashTag.getBytes();
    byte[] stringKey =
        new byte[] {(byte) 0xac, (byte) 0xed, 0, 4, 0, 5, 's', 't', 'r', 'i', 'n', 'g', '1'};
    byte[] allByteArray = new byte[utf8encodedBytes.length + stringKey.length];

    ByteBuffer buff = ByteBuffer.wrap(allByteArray);
    buff.put(utf8encodedBytes);
    buff.put(stringKey);
    byte[] combined = buff.array();

    jedis.set(combined, combined);
    assertThat(jedis.keys("{子}*".getBytes()))
        .containsExactlyInAnyOrder(combined);
  }

  @Test
  public void givenSplat_withCarriageReturnLineFeedAndTab_returnsExpectedMatches() {
    jedis.set("{K} foo bar ", "123");
    jedis.set("{K} foo\r\nbar\r\n ", "456");
    jedis.set("{K} \r\n\t\\x07\\x13 ", "789");

    assertThat(jedis.keys("{K}*")).containsExactlyInAnyOrder("{K} \r\n\t\\x07\\x13 ",
        "{K} foo\r\nbar\r\n ", "{K} foo bar ");
  }

  @Test
  public void givenMalformedGlobPattern_returnsEmptySet() {
    assertThat(jedis.keys("{K}[][]")).isEmpty();
  }
}
