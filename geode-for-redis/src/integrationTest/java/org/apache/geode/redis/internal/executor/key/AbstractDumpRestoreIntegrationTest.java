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

package org.apache.geode.redis.internal.executor.key;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertExactNumberOfArgs;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;

public abstract class AbstractDumpRestoreIntegrationTest implements RedisIntegrationTest {

  private RedisAdvancedClusterCommands<String, String> lettuce;
  protected JedisCluster jedis;
  private static String STRING_VALUE;
  private static byte[] RESTORE_BYTES;

  @Before
  public void setup() {
    jedis = new JedisCluster(new HostAndPort(BIND_ADDRESS, getPort()), REDIS_CLIENT_TIMEOUT);

    RedisClusterClient client =
        RedisClusterClient.create(String.format("redis://%s:%d", BIND_ADDRESS, getPort()));
    lettuce = client.connect().sync();

    STRING_VALUE = "It's a mad, mad, mad, mad, mad world";
    lettuce.set("set-dump-value", STRING_VALUE);
    RESTORE_BYTES = lettuce.dump("set-dump-value");
  }

  @After
  public void tearDown() {
    flushAll();
  }

  @Test
  public void dumpTakesExactlyOneArgument() {
    assertExactNumberOfArgs(jedis, Protocol.Command.DUMP, 1);
  }

  @Test
  public void restoreErrorsWithUnknownOption() {
    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.RESTORE, "key", "0", "", "FOO"))
            .hasMessageContaining("ERR syntax error");
  }

  @Test
  public void restoreFails_whenKeyAlreadyExists() {
    lettuce.set("restored", "already exists");

    assertThatThrownBy(() -> lettuce.restore("restored", 0, RESTORE_BYTES))
        .hasMessage("BUSYKEY Target key name already exists.");
  }

  @Test
  public void restoreFails_whenTTLisNegative() {
    assertThatThrownBy(() -> lettuce.restore("restored", -1, RESTORE_BYTES))
        .hasMessage("ERR Invalid TTL value, must be >= 0");
  }

  @Test
  public void restoreFails_whenTTLisNotANumber() {
    assertThatThrownBy(() -> jedis.sendCommand("restored".getBytes(), Protocol.Command.RESTORE,
        "restored".getBytes(), "not-an-integer".getBytes(), RESTORE_BYTES))
            .hasMessage("ERR value is not an integer or out of range");
  }

  @Test
  public void restoreFails_withInvalidBytes() {
    assertThatThrownBy(() -> lettuce.restore("restored", 0, new byte[] {0, 1, 2, 3}))
        .hasMessage("ERR DUMP payload version or checksum are wrong");
  }

  @Test
  public void dumpReturnsNull_whenKeyDoesNotExist() {
    assertThat(jedis.dump("unknown")).isNull();
  }

  @Test
  public void dumpAndRestoreString() {
    lettuce.set("dumped", STRING_VALUE);

    byte[] rawBytes = lettuce.dump("dumped");
    lettuce.restore("restored", 0, rawBytes);
    String response = lettuce.get("restored");

    assertThat(response).isEqualTo(STRING_VALUE);
    assertThat(lettuce.ttl("restored")).isEqualTo(-1);
  }

  @Test
  public void restore_withTTL_setsTTL() {
    lettuce.restore("restored", 2000, RESTORE_BYTES);
    String response = lettuce.get("restored");

    assertThat(response).isEqualTo(STRING_VALUE);
    assertThat(lettuce.pttl("restored"))
        .isLessThanOrEqualTo(2000)
        .isGreaterThan(0);
  }

  @Test
  public void restore_withReplace() {
    lettuce.set("restored", "already exists");
    lettuce.expire("restored", 10);

    lettuce.restore("restored", RESTORE_BYTES, new RestoreArgs().replace());
    String response = lettuce.get("restored");

    assertThat(response).isEqualTo(STRING_VALUE);
    assertThat(lettuce.pttl("restored")).isEqualTo(-1);
  }

  @Test
  public void restore_withAbsTTL() {
    long absttl = System.currentTimeMillis() + 10000;
    lettuce.restore("restored", RESTORE_BYTES, new RestoreArgs().ttl(absttl).absttl());

    String response = lettuce.get("restored");

    assertThat(response).isEqualTo(STRING_VALUE);
    assertThat(lettuce.ttl("restored"))
        .isLessThanOrEqualTo(10)
        .isGreaterThan(1);
  }

  @Test
  public void restore_withAbsTTL_inThePast_notReplacing() {
    long absttl = System.currentTimeMillis() - 10000;
    lettuce.restore("restored", RESTORE_BYTES, new RestoreArgs().ttl(absttl).absttl());

    String response = lettuce.get("restored");

    assertThat(response).isNull();
  }

  @Test
  public void restore_withAbsTTL_inThePast_andReplacing() {
    lettuce.set("restored", "already exists");
    long absttl = System.currentTimeMillis() - 10000;
    lettuce.restore("restored", RESTORE_BYTES, new RestoreArgs().ttl(absttl).absttl().replace());

    String response = lettuce.get("restored");

    assertThat(response).isNull();
  }

  @Test
  public void restore_withAbsTTL_ofZero() {
    lettuce.restore("restored", RESTORE_BYTES, new RestoreArgs().ttl(0).absttl());

    String response = lettuce.get("restored");

    assertThat(lettuce.ttl("restored")).isEqualTo(-1);
    assertThat(response).isEqualTo(STRING_VALUE);
  }

  @Test
  public void restore_withReplaceAndAbsttl() {
    lettuce.set("restored", "already exists");
    lettuce.expire("restored", 5);
    long absttl = System.currentTimeMillis() + 10000;
    lettuce.restore("restored", RESTORE_BYTES, new RestoreArgs().ttl(absttl).absttl().replace());

    String response = lettuce.get("restored");

    assertThat(response).isEqualTo(STRING_VALUE);
    assertThat(lettuce.ttl("restored")).isGreaterThan(5);
  }

  @Test
  public void dumpAndRestoreSet() {
    Set<String> smembers = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      lettuce.sadd("set", "member-" + i);
      smembers.add("member-" + i);
    }
    byte[] dump = lettuce.dump("set");

    lettuce.restore("restored", 0, dump);
    Set<String> result = lettuce.smembers("restored");

    assertThat(result).containsExactlyInAnyOrderElementsOf(smembers);
  }

  @Test
  public void dumpAndRestoreHash() {
    Map<String, String> hashy = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      lettuce.hset("hash", "field-" + i, "value-" + i);
      hashy.put("field-" + i, "value-" + i);
    }
    byte[] dump = lettuce.dump("hash");

    lettuce.restore("restored", 0, dump);
    String[] fields = Arrays.copyOf(hashy.keySet().toArray(), hashy.size(), String[].class);
    List<KeyValue<String, String>> restored = lettuce.hmget("restored", fields);
    Map<String, String> restoredMap = new HashMap<>();
    restored.forEach(e -> restoredMap.put(e.getKey(), e.getValue()));

    assertThat(restored.size()).isEqualTo(hashy.size());
    assertThat(restoredMap).containsAllEntriesOf(hashy);
  }

  @Test
  public void dumpAndRestoreSortedSet() {
    List<String> members = new ArrayList<>();
    List<Double> scores = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      lettuce.zadd("sorted-set", i, "member-" + i);
      members.add("member-" + i);
      scores.add((double) i);
    }
    byte[] dump = lettuce.dump("sorted-set");

    lettuce.restore("restored", 0, dump);

    for (int i = 0; i < 100; i++) {
      assertThat(lettuce.zscore("sorted-set", "member-" + i)).isEqualTo(i);
    }
  }

}
