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
package org.apache.geode.redis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class RedisStatsIntegrationTest {
  Jedis jedis;
  long initialKeyspaceHits;
  long initialKeyspaceMisses;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Before
  public void setup() {
    jedis = new Jedis("localhost", server.getPort(), 10000000);
    jedis.flushAll();
    jedis.set("Existing_Key", "A_Value");
    jedis.hset("Existing_Hash", "Field1", "Value1");
    jedis.sadd("Existing_Set", "m1", "m2", "m3");
    initialKeyspaceHits = server.getServer().getStats().getKeyspaceHits();
    initialKeyspaceMisses = server.getServer().getStats().getKeyspaceMisses();
  }

  @Test
  public void clientsStat_withConnectAndClose_isCorrect() {
    long initialClients = server.getServer().getStats().getClients();
    Jedis jedis = new Jedis("localhost", server.getPort(), 10000000);

    jedis.ping();
    assertThat(server.getServer().getStats().getClients()).isEqualTo(initialClients + 1);

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> assertThat(server.getServer().getStats().getClients()).isEqualTo(initialClients));
  }

  @Test
  public void keyspaceHitsStat_shouldIncrement_whenKeyAccessed() {
    jedis.get("Existing_Key");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 1);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses);
        });
  }

  @Test
  public void keyspaceHitsStat_shouldNotIncrement_whenNonexistentKeyAccessed() {
    jedis.get("Nonexistent_Key");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }

  // TODO: Set doesn't work like native Redis!
  @Test
  public void keyspaceStats_setCommand_existingKey() {
    jedis.set("Existing_Key", "New_Value");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 1);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses);
        });
  }

  // TODO: Set doesn't work like native Redis!
  @Test
  public void keyspaceStats_setCommand_nonexistentKey() {
    jedis.set("Another_Key", "Another_Value");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }

  @Test
  public void keyspaceStats_getBitCommand_existingKey() {
    jedis.getbit("Existing_Key", 0);

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 1);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses);
        });
  }

  @Test
  public void keyspaceStats_getBitCommand_nonexistentKey() {
    jedis.getbit("Nonexistent_Key", 0);

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }

  @Test
  public void keyspaceStats_getRangeCommand_existingKey() {
    jedis.getrange("Existing_Key", 0, 1);
    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 1);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses);
        });
  }

  @Test
  public void keyspaceStats_getRangeCommand_nonexistentKey() {
    jedis.getrange("Nonexistent_Key", 0, 1);
    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }

  @Test
  public void keyspaceStats_getSetCommand_existingKey() {
    jedis.getSet("Existing_Key", "New_Value");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 1);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses);
        });
  }

  @Test
  public void keyspaceStats_getSetCommand_nonexistentKey() {
    jedis.getSet("Nonexistent_Key", "FakeValue");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }

  @Test
  public void keyspaceStats_strlenCommand_existingKey() {
    jedis.strlen("Existing_Key");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 1);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses);
        });
  }

  @Test
  public void keyspaceStats_strlenCommand_nonexistentKey() {
    jedis.strlen("Nonexistent_Key");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }

  @Test
  public void keyspaceStats_mgetCommand() {
    jedis.mget("Existing_Key", "Nonexistent_Key");
    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 1);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }

  @Test
  public void keyspaceStats_bitopCommand() {
    jedis.bitop(BitOP.AND, "Existing_Key", "Existing_Key", "Nonexistent_Key");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 2);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }

  @Test
  public void keyspaceStats_bitcountCommand_existingKey() {
    jedis.bitcount("Existing_Key");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 1);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses);
        });
  }

  @Test
  public void keyspaceStats_bitcountCommand_nonexistentKey() {
    jedis.bitcount("Nonexistent_Key");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }

  @Test
  public void keyspaceStats_bitposCommand_existingKey() {
    jedis.bitpos("Existing_Key", true);
    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 1);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses);
        });
  }

  @Test
  public void keyspaceStats_bitposCommand_nonexistentKey() {
    jedis.bitpos("Nonexistent_Key", true);
    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }

  @Test
  public void keyspaceStats_hgetCommand_existingKey() {
    jedis.hget("Existing_Hash", "Field1");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 1);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses);
        });
  }

  @Test
  public void keyspaceStats_hgetCommand_nonexistentKey() {
    jedis.hget("Nonexistent_Hash", "Field1");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }

  @Test
  @Ignore("Not ready for prime time")
  public void keyspaceStats_smembersCommand_existingKey() {
    jedis.smembers("Existing_Set");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits + 1);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses);
        });
  }

  @Test
  @Ignore("Not ready for prime time")
  public void keyspaceStats_smembersCommand_nonexistentKey() {
    jedis.smembers("Nonexistent_Set");

    jedis.close();
    GeodeAwaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(
        () -> {
          assertThat(server.getServer().getStats().getKeyspaceHits())
              .isEqualTo(initialKeyspaceHits);
          assertThat(server.getServer().getStats().getKeyspaceMisses())
              .isEqualTo(initialKeyspaceMisses + 1);
        });
  }
}
