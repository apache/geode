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
  public static final String EXISTING_HASH_KEY = "Existing_Hash";
  public static final String EXISTING_STRING_KEY = "Existing_String";
  public static final String EXISTING_SET_KEY_1 = "Existing_Set_1";
  public static final String EXISTING_SET_KEY_2 = "Existing_Set_2";
  public static final String NONEXISTENT_KEY = "Nonexistent_Key";
  Jedis jedis;
  long initialKeyspaceHits;
  long initialKeyspaceMisses;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Before
  public void setup() {
    jedis = new Jedis("localhost", server.getPort(), 10000000);
    jedis.flushAll();
    jedis.set(EXISTING_STRING_KEY, "A_Value");
    jedis.hset(EXISTING_HASH_KEY, "Field1", "Value1");
    jedis.sadd(EXISTING_SET_KEY_1, "m1", "m2", "m3");
    jedis.sadd(EXISTING_SET_KEY_2, "m4", "m5", "m6");
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
    jedis.get(EXISTING_STRING_KEY);

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
    jedis.set(EXISTING_STRING_KEY, "New_Value");

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
    jedis.getbit(EXISTING_STRING_KEY, 0);

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
    jedis.getrange(EXISTING_STRING_KEY, 0, 1);
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
    jedis.getSet(EXISTING_STRING_KEY, "New_Value");

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
    jedis.strlen(EXISTING_STRING_KEY);

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
    jedis.strlen(NONEXISTENT_KEY);

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
    jedis.mget(EXISTING_STRING_KEY, "Nonexistent_Key");
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
    jedis.bitop(BitOP.AND, EXISTING_STRING_KEY, EXISTING_STRING_KEY, "Nonexistent_Key");

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
    jedis.bitcount(EXISTING_STRING_KEY);

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
    jedis.bitpos(EXISTING_STRING_KEY, true);
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
    jedis.hget(EXISTING_HASH_KEY, "Field1");

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
  public void keyspaceStats_smembersCommand_existingKey() {
    jedis.smembers(EXISTING_SET_KEY_1);

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

  @Test
  public void keyspaceStats_sunionstoreCommand_existingKey() {
    jedis.sunionstore("New_Set", EXISTING_SET_KEY_1, EXISTING_SET_KEY_2, "Nonexistent_Set");

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
  public void keyspaceStats_ExistsCommand_existingKey() {
    jedis.exists(EXISTING_STRING_KEY);
    jedis.close();

    GeodeAwaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(server.getServer().getStats().getKeyspaceHits())
                  .isEqualTo(initialKeyspaceHits + 1);
              assertThat(server.getServer().getStats().getKeyspaceMisses())
                  .isEqualTo(initialKeyspaceMisses);
            });
  }

  @Test
  public void keyspaceStats_ExistsCommand_nonexistentKey() {
    jedis.exists(NONEXISTENT_KEY);
    jedis.close();

    GeodeAwaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(server.getServer().getStats().getKeyspaceHits())
                  .isEqualTo(initialKeyspaceHits);
              assertThat(server.getServer().getStats().getKeyspaceMisses())
                  .isEqualTo(initialKeyspaceMisses + 1);
            });
  }

  @Test
  public void keyspaceStats_TypeCommand_existingKey() {
    jedis.type(EXISTING_STRING_KEY);
    jedis.close();

    GeodeAwaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(server.getServer().getStats().getKeyspaceHits())
                  .isEqualTo(initialKeyspaceHits + 1);
              assertThat(server.getServer().getStats().getKeyspaceMisses())
                  .isEqualTo(initialKeyspaceMisses);
            });
  }

  @Test
  public void keyspaceStats_TypeCommand_nonexistentKey() {
    jedis.type(NONEXISTENT_KEY);
    jedis.close();

    GeodeAwaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(server.getServer().getStats().getKeyspaceHits())
                  .isEqualTo(initialKeyspaceHits);
              assertThat(server.getServer().getStats().getKeyspaceMisses())
                  .isEqualTo(initialKeyspaceMisses + 1);
            });
  }


  @Test
  public void keyspaceStats_PTTL_TTLCommand_existingKey() {
    jedis.ttl(EXISTING_STRING_KEY);
    jedis.close();

    GeodeAwaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(server.getServer().getStats().getKeyspaceHits())
                  .isEqualTo(initialKeyspaceHits + 1);
              assertThat(server.getServer().getStats().getKeyspaceMisses())
                  .isEqualTo(initialKeyspaceMisses);
            });
  }

  @Test
  public void keyspaceStats_PTTL_TTL_Command_nonexistentKey() {
    jedis.ttl(NONEXISTENT_KEY);
    jedis.close();

    GeodeAwaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(server.getServer().getStats().getKeyspaceHits())
                  .isEqualTo(initialKeyspaceHits);
              assertThat(server.getServer().getStats().getKeyspaceMisses())
                  .isEqualTo(initialKeyspaceMisses + 1);
            });
  }

}
