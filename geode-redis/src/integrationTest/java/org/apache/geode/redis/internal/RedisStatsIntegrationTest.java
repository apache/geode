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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.statistics.EnabledStatisticsClock;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.redis.internal.statistics.RedisStats;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class RedisStatsIntegrationTest {

  private static final int TIMEOUT = (int) GeodeAwaitility.getTimeout().toMillis();
  private static final String EXISTING_HASH_KEY = "Existing_Hash";
  private static final String EXISTING_STRING_KEY = "Existing_String";
  private static final String EXISTING_SET_KEY_1 = "Existing_Set_1";
  private static final String EXISTING_SET_KEY_2 = "Existing_Set_2";
  private static final String NONEXISTENT_KEY = "Nonexistent_Key";

  private Jedis jedis;
  private RedisStats redisStats;
  private static long START_TIME;
  private static StatisticsClock statisticsClock;

  private long preTestKeySpaceHits = 0;
  private long preTestKeySpaceMisses = 0;
  private long preTestConnectionsReceived = 0;
  private long preTestConnectedClients = 0;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  private static StatsValidator statsValidator;

  private static class StatsValidator {

    private final RedisStats redisStats;
    private final Statistics geodeStats;

    public StatsValidator(GeodeRedisServerRule serverRule) {
      redisStats = serverRule.getServer().getStats();

      InternalCache cache = (InternalCache) CacheFactory.getAnyInstance();
      InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();

      StatisticsType statSamplerType = system.findType("RedisStats");
      Statistics[] statsArray = system.findStatisticsByType(statSamplerType);
      assertThat(statsArray).hasSize(1);

      geodeStats = statsArray[0];
    }

    public void validateKeyspaceHits(long before, long delta) {
      assertThat(redisStats.getKeyspaceHits()).isEqualTo(before + delta);

      long geodeKeyspaceHits = geodeStats.getLong("keyspaceHits");
      assertThat(geodeKeyspaceHits).isEqualTo(before + delta);
    }

    public void validateKeyspaceMisses(long before, long delta) {
      assertThat(redisStats.getKeyspaceMisses()).isEqualTo(before + delta);

      long geodeKeyspaceMisses = geodeStats.getLong("keyspaceMisses");
      assertThat(geodeKeyspaceMisses).isEqualTo(before + delta);
    }

    public void validateCommandsProcessed(long before, long delta) {
      GeodeAwaitility.await().atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThat(redisStats.getCommandsProcessed())
              .isEqualTo(before + delta));

      GeodeAwaitility.await().atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> {
            long geodeCommandsProcessed = geodeStats.getLong("commandsProcessed");
            assertThat(geodeCommandsProcessed).isEqualTo(before + delta);
          });
    }

    public void validateNetworkBytesRead(long before, long delta) {
      assertThat(redisStats.getTotalNetworkBytesRead()).isEqualTo(before + delta);

      long geodeNetworkBytesRead = geodeStats.getLong("totalNetworkBytesRead");
      assertThat(geodeNetworkBytesRead).isEqualTo(before + delta);
    }

    public void validateConnectedClients(long before, long delta) {
      GeodeAwaitility.await().atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> {
            assertThat(redisStats.getConnectedClients()).isEqualTo(before + delta);
          });

      GeodeAwaitility.await().atMost(Duration.ofSeconds(2))
          .untilAsserted(() -> {
            long geodeConnectedClients = geodeStats.getLong("connectedClients");
            assertThat(geodeConnectedClients).isEqualTo(before + delta);
          });
    }

    public void validateConnectionsReceived(long before, long delta) {
      assertThat(redisStats.getTotalConnectionsReceived()).isEqualTo(before + delta);

      long geodeConnectionsReceived = geodeStats.getLong("totalConnectionsReceived");
      assertThat(geodeConnectionsReceived).isEqualTo(before + delta);
    }

  }

  @BeforeClass
  public static void beforeClass() {
    statisticsClock = new EnabledStatisticsClock();
    START_TIME = statisticsClock.getTime();

    statsValidator = new StatsValidator(server);
  }

  @Before
  public void before() {
    jedis = new Jedis("localhost", server.getPort(), TIMEOUT);

    redisStats = server.getServer().getStats();

    long preSetupCommandsProcessed = redisStats.getCommandsProcessed();

    jedis.set(EXISTING_STRING_KEY, "A_Value");
    jedis.hset(EXISTING_HASH_KEY, "Field1", "Value1");
    jedis.sadd(EXISTING_SET_KEY_1, "m1", "m2", "m3");
    jedis.sadd(EXISTING_SET_KEY_2, "m4", "m5", "m6");

    GeodeAwaitility.await().atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(redisStats.getCommandsProcessed())
            .isEqualTo(preSetupCommandsProcessed + 4));

    preTestKeySpaceHits = redisStats.getKeyspaceHits();
    preTestKeySpaceMisses = redisStats.getKeyspaceMisses();
    preTestConnectionsReceived = redisStats.getTotalConnectionsReceived();
    preTestConnectedClients = redisStats.getConnectedClients();
  }

  @After
  public void after() {
    jedis.flushAll();
    jedis.close();
    GeodeAwaitility.await().atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(redisStats.getConnectedClients())
            .isEqualTo(0));
  }

  // #############Stats Section###################################

  @Test
  public void keyspaceHitsStat_shouldIncrement_whenKeyAccessed() {
    jedis.get(EXISTING_STRING_KEY);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceHitsStat_shouldNotIncrement_whenNonexistentKeyAccessed() {
    jedis.get("Nonexistent_Key");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_setCommand_existingKey() {
    jedis.set(EXISTING_STRING_KEY, "New_Value");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_setCommand_nonexistentKey() {
    jedis.set("Another_Key", "Another_Value");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_getBitCommand_existingKey() {
    jedis.getbit(EXISTING_STRING_KEY, 0);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_getBitCommand_nonexistentKey() {
    jedis.getbit("Nonexistent_Key", 0);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_getRangeCommand_existingKey() {
    jedis.getrange(EXISTING_STRING_KEY, 0, 1);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_getRangeCommand_nonexistentKey() {
    jedis.getrange("Nonexistent_Key", 0, 1);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_getSetCommand_existingKey() {
    jedis.getSet(EXISTING_STRING_KEY, "New_Value");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_getSetCommand_nonexistentKey() {
    jedis.getSet("Nonexistent_Key", "FakeValue");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_strlenCommand_existingKey() {
    jedis.strlen(EXISTING_STRING_KEY);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_strlenCommand_nonexistentKey() {
    jedis.strlen(NONEXISTENT_KEY);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_mgetCommand() {
    jedis.mget(EXISTING_STRING_KEY, "Nonexistent_Key");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_bitopCommand_existingKey() {
    jedis.bitop(BitOP.AND, EXISTING_STRING_KEY, EXISTING_STRING_KEY, "Nonexistent_Key");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_bitopCommand_newKey() {
    jedis.bitop(BitOP.AND, "destination-key", EXISTING_STRING_KEY, "Nonexistent_Key");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_bitcountCommand_existingKey() {
    jedis.bitcount(EXISTING_STRING_KEY);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_bitcountCommand_nonexistentKey() {
    jedis.bitcount("Nonexistent_Key");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_bitposCommand_existingKey() {
    jedis.bitpos(EXISTING_STRING_KEY, true);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_bitposCommand_nonexistentKey() {
    jedis.bitpos("Nonexistent_Key", true);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_hgetCommand_existingKey() {
    jedis.hget(EXISTING_HASH_KEY, "Field1");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_hgetCommand_nonexistentKey() {
    jedis.hget("Nonexistent_Hash", "Field1");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_smembersCommand_existingKey() {
    jedis.smembers(EXISTING_SET_KEY_1);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_smembersCommand_nonexistentKey() {
    jedis.smembers("Nonexistent_Set");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_sunionstoreCommand_existingKey() {
    jedis.sunionstore(
        EXISTING_SET_KEY_1,
        EXISTING_SET_KEY_1,
        EXISTING_SET_KEY_2,
        "Nonexistent_Set");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_sunionstoreCommand_newKey() {
    jedis.sunionstore(
        "New_Set",
        EXISTING_SET_KEY_1,
        EXISTING_SET_KEY_2,
        "Nonexistent_Set");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_sdiffstoreCommand_newKey() {
    jedis.sdiffstore(
        "New_Set",
        EXISTING_SET_KEY_1,
        EXISTING_SET_KEY_2,
        "Nonexistent_Set");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_sinterstoreCommand_newKey() {
    jedis.sinterstore(
        "New_Set",
        EXISTING_SET_KEY_1,
        EXISTING_SET_KEY_2,
        "Nonexistent_Set");

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_ExistsCommand_existingKey() {
    jedis.exists(EXISTING_STRING_KEY);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_ExistsCommand_nonexistentKey() {
    jedis.exists(NONEXISTENT_KEY);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_TypeCommand_existingKey() {
    jedis.type(EXISTING_STRING_KEY);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_TypeCommand_nonexistentKey() {
    jedis.type(NONEXISTENT_KEY);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void keyspaceStats_PTTL_TTLCommand_existingKey() {
    jedis.ttl(EXISTING_STRING_KEY);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 1);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 0);
  }

  @Test
  public void keyspaceStats_PTTL_TTL_Command_nonexistentKey() {
    jedis.ttl(NONEXISTENT_KEY);

    statsValidator.validateKeyspaceHits(preTestKeySpaceHits, 0);
    statsValidator.validateKeyspaceMisses(preTestKeySpaceMisses, 1);
  }

  @Test
  public void commandsProcessed_shouldIncrement_givenSuccessfulCommand() {
    long initialCommandsProcessed = redisStats.getCommandsProcessed();
    jedis.ttl("key");

    statsValidator.validateCommandsProcessed(initialCommandsProcessed, 1);
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
        .isCloseTo(expected, Offset.offset(
            getTenPercentOf(actual_commandsProcessed.get())));

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

    statsValidator.validateNetworkBytesRead(initialNetworkBytesRead, respCommandString.length());
  }

  @Test
  public void networkKiloBytesReadOverLastSecond_shouldReturnCorrectData() {

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
    Jedis jedis2 = new Jedis("localhost", server.getPort(), TIMEOUT);
    jedis2.ping();

    statsValidator.validateConnectedClients(preTestConnectedClients, 1);

    jedis2.close();

    statsValidator.validateConnectedClients(preTestConnectedClients, 0);
  }

  @Test
  public void totalConnectionsReceivedStat_shouldIncrement_whenNewConnectionOccurs() {
    Jedis jedis2 = new Jedis("localhost", server.getPort(), TIMEOUT);
    jedis2.ping();

    statsValidator.validateConnectedClients(preTestConnectedClients, 1);

    jedis2.close();

    statsValidator.validateConnectionsReceived(preTestConnectionsReceived, 1);
  }

  // ######################## Server Section ################

  @Test
  public void uptimeInSeconds_shouldReturnCorrectValue() {
    long serverUptimeAtStartOfTestInNanos = getCurrentTime();
    long statsUpTimeAtStartOfTest = redisStats.getUptimeInSeconds();

    GeodeAwaitility.await().during(Duration.ofSeconds(3)).until(() -> true);

    long expectedNanos = getCurrentTime() - serverUptimeAtStartOfTestInNanos;
    long expectedSeconds = TimeUnit.NANOSECONDS.toSeconds(expectedNanos);

    assertThat(redisStats.getUptimeInSeconds() - statsUpTimeAtStartOfTest)
        .isCloseTo(expectedSeconds, Offset.offset(1l));
  }

  @Test
  public void upTimeInDays_shouldReturnCorrectValue() {
    long startTimeInNanos = getStartTime();
    long currentTimeInNanos = getCurrentTime();

    long expectedNanos = currentTimeInNanos - startTimeInNanos;
    long expectedDays = TimeUnit.NANOSECONDS.toDays(expectedNanos);

    assertThat(redisStats.getUptimeInDays())
        .isEqualTo(expectedDays);
  }

  public long getStartTime() {
    return START_TIME;
  }

  public long getCurrentTime() {
    return this.statisticsClock.getTime();
  }

  private double getTenPercentOf(Double value) {
    return Math.ceil(value * .1);
  }
}
