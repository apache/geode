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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.AtomicDouble;
import org.assertj.core.data.Offset;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class RedisStatsIntegrationTest {

  public static final int TIMEOUT = (int) GeodeAwaitility.getTimeout().toMillis();
  public static final String EXISTING_HASH_KEY = "Existing_Hash";
  public static final String EXISTING_STRING_KEY = "Existing_String";
  public static final String EXISTING_SET_KEY_1 = "Existing_Set_1";
  public static final String EXISTING_SET_KEY_2 = "Existing_Set_2";
  public static final String NONEXISTENT_KEY = "Nonexistent_Key";
  private Jedis jedis;
  private RedisStats redisStats;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Before
  public void before() {
    jedis = new Jedis("localhost", server.getPort(), TIMEOUT);

    jedis.set(EXISTING_STRING_KEY, "A_Value");
    jedis.hset(EXISTING_HASH_KEY, "Field1", "Value1");
    jedis.sadd(EXISTING_SET_KEY_1, "m1", "m2", "m3");
    jedis.sadd(EXISTING_SET_KEY_2, "m4", "m5", "m6");

    redisStats = server.getServer().getStats();
    redisStats.clearAllStats();
  }

  @After
  public void after() {
    jedis.flushAll();
    jedis.close();
  }

  // #############Stats Section###################################

  @Test
  public void keyspaceHitsStat_shouldIncrement_whenKeyAccessed() {
    jedis.get(EXISTING_STRING_KEY);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);

    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceHitsStat_shouldNotIncrement_whenNonexistentKeyAccessed() {

    jedis.get("Nonexistent_Key");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  // TODO: Set doesn't work like native Redis!
  @Test
  public void keyspaceStats_setCommand_existingKey() {
    jedis.set(EXISTING_STRING_KEY, "New_Value");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  // TODO: Set doesn't work like native Redis!
  @Test
  public void keyspaceStats_setCommand_nonexistentKey() {
    jedis.set("Another_Key", "Another_Value");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_getBitCommand_existingKey() {
    jedis.getbit(EXISTING_STRING_KEY, 0);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceStats_getBitCommand_nonexistentKey() {
    jedis.getbit("Nonexistent_Key", 0);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_getRangeCommand_existingKey() {
    jedis.getrange(EXISTING_STRING_KEY, 0, 1);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceStats_getRangeCommand_nonexistentKey() {
    jedis.getrange("Nonexistent_Key", 0, 1);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_getSetCommand_existingKey() {
    jedis.getSet(EXISTING_STRING_KEY, "New_Value");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceStats_getSetCommand_nonexistentKey() {
    jedis.getSet("Nonexistent_Key", "FakeValue");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_strlenCommand_existingKey() {
    jedis.strlen(EXISTING_STRING_KEY);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceStats_strlenCommand_nonexistentKey() {
    jedis.strlen(NONEXISTENT_KEY);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_mgetCommand() {
    jedis.mget(EXISTING_STRING_KEY, "Nonexistent_Key");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_bitopCommand() {
    jedis.bitop(BitOP.AND, EXISTING_STRING_KEY, EXISTING_STRING_KEY, "Nonexistent_Key");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0 + 2);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_bitcountCommand_existingKey() {
    jedis.bitcount(EXISTING_STRING_KEY);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceStats_bitcountCommand_nonexistentKey() {
    jedis.bitcount("Nonexistent_Key");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_bitposCommand_existingKey() {
    jedis.bitpos(EXISTING_STRING_KEY, true);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceStats_bitposCommand_nonexistentKey() {
    jedis.bitpos("Nonexistent_Key", true);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_hgetCommand_existingKey() {
    jedis.hget(EXISTING_HASH_KEY, "Field1");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceStats_hgetCommand_nonexistentKey() {
    jedis.hget("Nonexistent_Hash", "Field1");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_smembersCommand_existingKey() {
    jedis.smembers(EXISTING_SET_KEY_1);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceStats_smembersCommand_nonexistentKey() {
    jedis.smembers("Nonexistent_Set");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_sunionstoreCommand_existingKey() {
    jedis.sunionstore(
        "New_Set",
        EXISTING_SET_KEY_1,
        EXISTING_SET_KEY_2,
        "Nonexistent_Set");

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0 + 2);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_ExistsCommand_existingKey() {
    jedis.exists(EXISTING_STRING_KEY);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceStats_ExistsCommand_nonexistentKey() {
    jedis.exists(NONEXISTENT_KEY);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_TypeCommand_existingKey() {
    jedis.type(EXISTING_STRING_KEY);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceStats_TypeCommand_nonexistentKey() {
    jedis.type(NONEXISTENT_KEY);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void keyspaceStats_PTTL_TTLCommand_existingKey() {
    jedis.ttl(EXISTING_STRING_KEY);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(1);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(0);
  }

  @Test
  public void keyspaceStats_PTTL_TTL_Command_nonexistentKey() {
    jedis.ttl(NONEXISTENT_KEY);

    assertThat(redisStats.getKeyspaceHits())
        .isEqualTo(0);
    assertThat(redisStats.getKeyspaceMisses())
        .isEqualTo(1);
  }

  @Test
  public void commandsProcessed_shouldIncrement_givenSuccessfulCommand() {
    long initialCommandsProcessed = redisStats.getCommandsProcessed();
    jedis.ttl("key");

    assertThat(redisStats.getCommandsProcessed())
        .isEqualTo(initialCommandsProcessed + 1);
  }

  @Test
  public void opsPerformedOverLastSecond_ShouldUpdate_givenOperationsOccurring() {

    int NUMBER_SECONDS_TO_RUN = 3;
    AtomicInteger numberOfCommandsExecuted = new AtomicInteger();
    AtomicDouble actual_commandsProcessed = new AtomicDouble();
    GeodeAwaitility
        .await()
        .during(Duration.ofSeconds(NUMBER_SECONDS_TO_RUN))
        .until(() -> {
          jedis.set("key", "value");
          numberOfCommandsExecuted.getAndIncrement();
          actual_commandsProcessed.set(redisStats.getOpsPerformedOverLastSecond());
          return true;
        });

    long expected =
        (numberOfCommandsExecuted.get() / NUMBER_SECONDS_TO_RUN);

    assertThat(actual_commandsProcessed.get())
        .isCloseTo(expected, Offset.offset(1d));

    // if time passes w/o operations
    GeodeAwaitility
        .await()
        .during(NUMBER_SECONDS_TO_RUN, TimeUnit.SECONDS)
        .until(() -> true);

    assertThat(redisStats.getOpsPerformedOverLastSecond())
        .isEqualTo(0);
  }

  @Test
  public void networkBytesRead_shouldIncrementBySizeOfCommandSent() {
    long initialNetworkBytesRead = redisStats.getTotalNetworkBytesRead();
    String respCommandString = "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";

    jedis.set("key", "value");

    assertThat(redisStats.getTotalNetworkBytesRead())
        .isEqualTo(initialNetworkBytesRead + respCommandString.length());
  }

  @Test
  public void NetworkKiloBytesReadOverLastSecond_shouldReturnCorrectData() {

    double REASONABLE_SOUNDING_OFFSET = .8;
    int NUMBER_SECONDS_TO_RUN = 2;
    String RESP_COMMAND_STRING = "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    int BYTES_SENT_PER_COMMAND = RESP_COMMAND_STRING.length();
    AtomicInteger totalBytesSent = new AtomicInteger();
    AtomicReference<Double> actual_kbs = new AtomicReference<>((double) 0);

    GeodeAwaitility
        .await()
        .during(Duration.ofSeconds(NUMBER_SECONDS_TO_RUN))
        .until(() -> {
          jedis.set("key", "value");
          totalBytesSent.addAndGet(BYTES_SENT_PER_COMMAND);
          actual_kbs.set(redisStats.getNetworkKiloBytesReadOverLastSecond());
          return true;
        });

    double expectedBytesReceived = totalBytesSent.get() / NUMBER_SECONDS_TO_RUN;
    double expected_kbs = expectedBytesReceived / 1000;

    assertThat(actual_kbs.get()).isCloseTo(expected_kbs,
        Offset.offset(REASONABLE_SOUNDING_OFFSET));

    // if time passes w/o operations
    GeodeAwaitility
        .await()
        .during(NUMBER_SECONDS_TO_RUN, TimeUnit.SECONDS)
        .until(() -> true);

    assertThat(redisStats.getNetworkKiloBytesReadOverLastSecond())
        .isEqualTo(0);

  }


  // ######################### Clients Section #################################

  @Test
  public void clientsStat_withConnectAndClose_isCorrect() {

    jedis = new Jedis("localhost", server.getPort(), TIMEOUT);
    jedis.ping();

    assertThat(redisStats.getConnectedClients()).isEqualTo(1);

    jedis.close();
    GeodeAwaitility.await().atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(redisStats.getConnectedClients()).isEqualTo(0));
  }

  @Test
  public void connectionsReceivedStat_shouldIncrement_WhenNewConnectionOccurs() {

    jedis = new Jedis("localhost", server.getPort(), TIMEOUT);
    jedis.ping();

    assertThat(redisStats.getConnectionsReceived()).isEqualTo(1);

    jedis.close();

    assertThat(redisStats.getConnectionsReceived()).isEqualTo(1);
  }

  // ######################## Server Section ################

  @Test
  public void uptimeInSeconds_ShouldReturnCorrectValue() {
    long serverUptimeAtStartOfTestInNanos = server.getCurrentTime();
    long statsUpTimeAtStartOfTest = redisStats.getUptimeInSeconds();

    GeodeAwaitility.await().during(Duration.ofSeconds(3)).until(() -> true);

    long expectedNanos = server.getCurrentTime() - serverUptimeAtStartOfTestInNanos;
    long expectedSeconds = TimeUnit.NANOSECONDS.toSeconds(expectedNanos);

    assertThat(redisStats.getUptimeInSeconds() - statsUpTimeAtStartOfTest)
        .isCloseTo(expectedSeconds, Offset.offset(1l));
  }

  @Test
  public void upTimeInDays_shouldReturnCorrectValue() {
    long startTimeInNanos = server.getStartTime();
    long currentTimeInNanos = server.getCurrentTime();

    long expectedNanos = currentTimeInNanos - startTimeInNanos;
    long expectedDays = TimeUnit.NANOSECONDS.toDays(expectedNanos);

    assertThat(redisStats.getUptimeInDays())
        .isEqualTo(expectedDays);
  }
}
