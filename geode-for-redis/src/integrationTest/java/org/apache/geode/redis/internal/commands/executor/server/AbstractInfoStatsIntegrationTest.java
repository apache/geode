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
package org.apache.geode.redis.internal.commands.executor.server;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.data.Offset;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.statistics.EnabledStatisticsClock;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.RedisTestHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractInfoStatsIntegrationTest implements RedisIntegrationTest {
  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final String EXISTING_HASH_KEY = "Existing_Hash";
  private static final String EXISTING_STRING_KEY = "Existing_String";
  private static final String EXISTING_SET_KEY_1 = "Existing_Set_1";
  private static final String EXISTING_SET_KEY_2 = "Existing_Set_2";

  private Jedis jedis;
  private static long startTime;
  private static StatisticsClock statisticsClock;

  private long preTestConnectionsReceived = 0;
  private long preTestConnectedClients = 0;

  private static final String COMMANDS_PROCESSED = "total_commands_processed";
  private static final String TOTAL_CONNECTIONS_RECEIVED = "total_connections_received";
  private static final String CONNECTED_CLIENTS = "connected_clients";
  private static final String OPS_PERFORMED_OVER_LAST_SECOND = "instantaneous_ops_per_sec";
  private static final String TOTAL_NETWORK_BYTES_READ = "total_net_input_bytes";
  private static final String NETWORK_KB_READ_OVER_LAST_SECOND = "instantaneous_input_kbps";
  private static final String UPTIME_IN_DAYS = "uptime_in_days";
  private static final String UPTIME_IN_SECONDS = "uptime_in_seconds";
  private static final String USED_MEMORY = "used_memory";
  private static final String MEMORY_FRAGMENTATION = "mem_fragmentation_ratio";
  private static final String TCP_PORT = "tcp_port";
  private static final String MAX_MEMORY = "maxmemory";

  private static final AtomicInteger numInfoCalled = new AtomicInteger(0);

  abstract int getExposedPort();

  abstract void configureMaxMemory(Jedis jedis);

  // ------------------- Setup -------------------------- //
  @BeforeClass
  public static void beforeClass() {
    statisticsClock = new EnabledStatisticsClock();
    startTime = statisticsClock.getTime();
  }

  @Before
  public void before() {
    jedis = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
    numInfoCalled.set(0);

    long preSetupCommandsProcessed = Long.parseLong(getInfo(jedis).get(COMMANDS_PROCESSED));

    jedis.set(EXISTING_STRING_KEY, "A_Value");
    jedis.hset(EXISTING_HASH_KEY, "Field1", "Value1");
    jedis.sadd(EXISTING_SET_KEY_1, "m1", "m2", "m3");
    jedis.sadd(EXISTING_SET_KEY_2, "m4", "m5", "m6");

    // the info command increments command processed, so we need to account for that.
    // the +1 is needed because info returns the number of commands processed before that call to
    // info
    await().atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertThat(
            Long.parseLong(getInfo(jedis).get(COMMANDS_PROCESSED)) - numInfoCalled.get() + 1)
                .isEqualTo(preSetupCommandsProcessed + 4));

    preTestConnectionsReceived = Long.parseLong(getInfo(jedis).get(TOTAL_CONNECTIONS_RECEIVED));
    preTestConnectedClients = Long.parseLong(getInfo(jedis).get(CONNECTED_CLIENTS));
    numInfoCalled.set(0);
  }

  @After
  public void after() {
    jedis.flushAll();
    await().atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> assertThat(Long.valueOf(getInfo(jedis).get(CONNECTED_CLIENTS))).isEqualTo(1));
    jedis.close();
  }

  // ------------------- Stats Section -------------------------- //

  // note: see AbstractHitsMissesIntegrationTest for testing of hits/misses
  // note: we are not testing hardcoded values at this time

  @Test
  public void keysSection_containsNumberOfSetKeys() {
    assertThat(jedis.info("keyspace")).contains("keys=4");
  }

  @Test
  public void maxMemoryIsNonZero_whenMaxMemoryIsSet() {
    configureMaxMemory(jedis);

    assertThat(Long.valueOf(getInfo(jedis).get(MAX_MEMORY))).isGreaterThan(0L);
  }

  @Test
  public void tcpPort_returnsExposedTCPPort() {
    assertThat(Integer.valueOf(getInfo(jedis).get(TCP_PORT))).isEqualTo(getExposedPort());
  }

  @Test
  public void usedMemory_shouldBeNonZeroWhenContainsData() {
    jedis.set("key", "value");

    assertThat(Long.valueOf(getInfo(jedis).get(USED_MEMORY))).isGreaterThan(0);
  }

  @Test
  public void memFragmentation_shouldBeGreaterThanOne() {
    for (int i = 0; i < 10000; i++) {
      jedis.set("key" + i, "value");
    }

    assertThat(Double.valueOf(getInfo(jedis).get(MEMORY_FRAGMENTATION))).isGreaterThan(1.0);
  }

  @Test
  public void commandsProcessed_shouldIncrement_givenSuccessfulCommand() {
    long initialCommandsProcessed = Long.parseLong(getInfo(jedis).get(COMMANDS_PROCESSED));
    jedis.ttl("key");

    validateCommandsProcessed(jedis, initialCommandsProcessed, 1);
  }

  @Test
  public void opsPerformedOverLastSecond_shouldUpdate_givenOperationsOccurring()
      throws InterruptedException, ExecutionException, TimeoutException {
    // Wait 2 seconds to allow the stat to return to zero following any set-up or previous tests
    Thread.sleep(2000);

    assertThat(Long.parseLong(getInfo(jedis).get(OPS_PERFORMED_OVER_LAST_SECOND))).isZero();

    final long numberSecondsToRun = 4;

    final long startTime = System.currentTimeMillis();
    final long endTime = startTime + Duration.ofSeconds(numberSecondsToRun).toMillis();

    // Take a sample in the middle of performing operations
    final long timeToSampleAt = startTime + Duration.ofSeconds(numberSecondsToRun / 2).toMillis();

    // Execute commands in the background
    Future<Void> executeCommands = executor.submit(() -> {
      Jedis jedis2 = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
      while (System.currentTimeMillis() < endTime) {
        jedis2.set("key", "value");
      }
      jedis2.close();
    });

    // Wait until we're in the middle of performing operations
    await().until(() -> System.currentTimeMillis() >= timeToSampleAt);

    assertThat(Long.parseLong(getInfo(jedis).get(OPS_PERFORMED_OVER_LAST_SECOND))).isGreaterThan(0);

    executeCommands.get(GeodeAwaitility.getTimeout().toMillis(), TimeUnit.MILLISECONDS);

    // Wait two seconds with no operations
    Thread.sleep(2000);

    // Confirm that instantaneous operations per second returns to zero when no operations are being
    // performed
    assertThat(Long.parseLong(getInfo(jedis).get(OPS_PERFORMED_OVER_LAST_SECOND))).isZero();
  }

  @Test
  public void networkBytesRead_shouldIncrementBySizeOfCommandSent() {
    long initialNetworkBytesRead = Long.parseLong(getInfo(jedis).get(TOTAL_NETWORK_BYTES_READ));
    String infoCommandString = "*3\r\n$3\r\ninfo\r\n";
    String respCommandString = "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";

    jedis.set("key", "value");

    validateNetworkBytesRead(jedis, initialNetworkBytesRead,
        respCommandString.length() + infoCommandString.length());
  }

  @Test
  public void networkKiloBytesReadOverLastSecond_shouldUpdate_givenOperationsOccurring()
      throws InterruptedException, ExecutionException, TimeoutException {
    // Wait 2 seconds to allow the stat to return to zero following any set-up or previous tests
    Thread.sleep(2000);

    // Confirm that instantaneous KB read per second returns to zero when no operations are being
    // performed, with a small offset to account for the info command being executed
    assertThat(Double.parseDouble(getInfo(jedis).get(NETWORK_KB_READ_OVER_LAST_SECOND)))
        .isCloseTo(0, offset(0.02));

    final int numberSecondsToRun = 4;
    final String key = "key";
    final String value = "value";

    final long startTime = System.currentTimeMillis();
    final long endTime = startTime + Duration.ofSeconds(numberSecondsToRun).toMillis();

    // Take a sample in the middle of performing operations
    final long timeToSampleAt = startTime + Duration.ofSeconds(numberSecondsToRun / 2).toMillis();

    // Execute commands in the background
    Future<Void> executeCommands = executor.submit(() -> {
      Jedis jedis2 = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
      while (System.currentTimeMillis() < endTime) {
        jedis2.set(key, value);
      }
      jedis2.close();
    });

    // Wait until we're in the middle of performing operations
    await().until(() -> System.currentTimeMillis() >= timeToSampleAt);

    assertThat(Double.parseDouble(getInfo(jedis).get(NETWORK_KB_READ_OVER_LAST_SECOND)))
        .isGreaterThan(0);

    executeCommands.get(GeodeAwaitility.getTimeout().toMillis(), TimeUnit.MILLISECONDS);

    // Wait two seconds with no operations
    Thread.sleep(2000);

    // Confirm that instantaneous KB read per second returns to zero when no operations are being
    // performed, with a small offset to account for the info command being executed
    assertThat(Double.parseDouble(getInfo(jedis).get(NETWORK_KB_READ_OVER_LAST_SECOND)))
        .isCloseTo(0, offset(0.02));
  }

  // ------------------- Clients Section -------------------------- //

  @Test
  public void connectedClients_incrAndDecrWhenClientConnectsAndDisconnects() {
    Jedis jedis2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    jedis2.ping();

    await().untilAsserted(() -> assertThat(Long.valueOf(getInfo(jedis).get(CONNECTED_CLIENTS)))
        .isEqualTo(preTestConnectedClients + 1));

    jedis2.close();

    await().untilAsserted(() -> assertThat(Long.valueOf(getInfo(jedis).get(CONNECTED_CLIENTS)))
        .isEqualTo(preTestConnectedClients));
  }

  @Test
  public void totalConnectionsReceivedStat_shouldIncrement_whenNewConnectionOccurs() {
    Jedis jedis2 = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    jedis2.ping();

    assertThat(Long.valueOf(getInfo(jedis).get(TOTAL_CONNECTIONS_RECEIVED)))
        .isEqualTo(preTestConnectionsReceived + 1);

    jedis2.close();
  }

  // ------------------- Server Section -------------------------- //

  @Test
  public void upTimeInDays_shouldBeEqualToTimeSinceStartInDays() {
    long startTimeInNanos = getStartTime();
    long currentTimeInNanos = getCurrentTime();

    long expectedNanos = currentTimeInNanos - startTimeInNanos;
    long expectedDays = TimeUnit.NANOSECONDS.toDays(expectedNanos);

    assertThat(Long.valueOf(getInfo(jedis).get(UPTIME_IN_DAYS))).isEqualTo(expectedDays);
  }

  @Test
  public void uptimeInSeconds_shouldReturnTimeSinceStartInSeconds() {
    long serverUptimeAtStartOfTestInNanos = getCurrentTime();
    long statsUpTimeAtStartOfTest = Long.parseLong(getInfo(jedis).get(UPTIME_IN_SECONDS));

    await().during(Duration.ofSeconds(3)).until(() -> true);

    long expectedNanos = getCurrentTime() - serverUptimeAtStartOfTestInNanos;
    long expectedSeconds = TimeUnit.NANOSECONDS.toSeconds(expectedNanos);

    assertThat(Long.parseLong(getInfo(jedis).get(UPTIME_IN_SECONDS)) - statsUpTimeAtStartOfTest)
        .isCloseTo(expectedSeconds, Offset.offset(1L));
  }

  // ------------------- Helper Methods ----------------------------- //
  public long getStartTime() {
    return startTime;
  }

  public long getCurrentTime() {
    return statisticsClock.getTime();
  }

  /**
   * Convert the values returned by the INFO command into a basic param:value map.
   */
  static synchronized Map<String, String> getInfo(Jedis jedis) {
    numInfoCalled.incrementAndGet();

    return RedisTestHelper.getInfo(jedis);
  }

  private void validateNetworkBytesRead(Jedis jedis, long initialNetworkBytesRead,
      int responseLength) {
    await().atMost(Duration.ofSeconds(2)).untilAsserted(
        () -> assertThat(Long.valueOf(getInfo(jedis).get(TOTAL_NETWORK_BYTES_READ)))
            .isEqualTo(initialNetworkBytesRead + responseLength));
  }

  private void validateCommandsProcessed(Jedis jedis, long initialCommandsProcessed, int diff) {
    await().atMost(Duration.ofSeconds(2)).untilAsserted(
        () -> assertThat(
            Long.parseLong(getInfo(jedis).get(COMMANDS_PROCESSED)) - numInfoCalled.get() + 1)
                .isEqualTo(initialCommandsProcessed + diff));
  }

}
