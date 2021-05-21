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
 *
 */


package org.apache.geode.redis.internal.data;

import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.DEFAULT_MAX_WAIT_TIME_RECONNECT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class PartitionedRegionStatsUpdateTest {

  @ClassRule
  public static RedisClusterStartupRule clusterStartUpRule = new RedisClusterStartupRule(3);

  private static MemberVM server1;
  private static MemberVM server2;

  private static Jedis jedis1;
  private static Jedis jedis2;

  private static final int JEDIS_TIMEOUT = Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final String LOCAL_HOST = "127.0.0.1";
  public static final String STRING_KEY = "string key";
  public static final String SET_KEY = "set key";
  public static final String HASH_KEY = "hash key";
  public static final String LONG_APPEND_VALUE = String.valueOf(Integer.MAX_VALUE);
  public static final String FIELD = "field";

  @BeforeClass
  public static void classSetup() {
    Properties locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, DEFAULT_MAX_WAIT_TIME_RECONNECT);

    MemberVM locator = clusterStartUpRule.startLocatorVM(0, locatorProperties);
    int locatorPort = locator.getPort();

    server1 = clusterStartUpRule.startRedisVM(1, locatorPort);
    int redisServerPort1 = clusterStartUpRule.getRedisPort(1);
    jedis1 = new Jedis(LOCAL_HOST, redisServerPort1, JEDIS_TIMEOUT);

    server2 = clusterStartUpRule.startRedisVM(2, locatorPort);
    int redisServerPort2 = clusterStartUpRule.getRedisPort(1);
    jedis2 = new Jedis(LOCAL_HOST, redisServerPort2, JEDIS_TIMEOUT);
  }

  @Before
  public void setup() {
    jedis1.flushAll();
  }

  @Test
  public void should_showIncreaseInDatastoreBytesInUse_givenStringValueSizeIncreases() {
    String LONG_APPEND_VALUE = String.valueOf(Integer.MAX_VALUE);
    jedis1.set(STRING_KEY, "value");

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis1.append(STRING_KEY, LONG_APPEND_VALUE);
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isGreaterThan(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showDecreaseInDatastoreBytesInUse_givenStringValueDeleted() {
    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    jedis1.set(STRING_KEY, "value");

    long intermediateDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    assertThat(intermediateDataStoreBytesInUse).isGreaterThan(initialDataStoreBytesInUse);

    jedis1.del(STRING_KEY);

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showDecreaseInDatastoreBytesInUse_givenStringValueShortened() {
    jedis1.set(STRING_KEY, "longer value");

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    jedis1.set(STRING_KEY, "value");

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isLessThan(initialDataStoreBytesInUse);
  }

  @Test
  public void should_resetMemoryUsage_givenFlushAllCommand() {
    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(initialDataStoreBytesInUse).isEqualTo(0L);

    jedis1.set(STRING_KEY, "value");

    jedis1.flushAll();

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showNoIncreaseInDatastoreBytesInUse_givenStringValueSizeDoesNotIncrease() {
    jedis1.set(STRING_KEY, "value");

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis1.set(STRING_KEY, "value");
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showIncreaseInDatastoreBytesInUse_givenSetValueSizeIncreases() {
    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis1.sadd(SET_KEY, "value" + i);
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isGreaterThan(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showNoIncreaseInDatastoreBytesInUse_givenSetValueSizeDoesNotIncrease() {
    jedis1.sadd(SET_KEY, "value");

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis1.sadd(SET_KEY, "value");
    }

    assertThat(jedis1.scard(SET_KEY)).isEqualTo(1);

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showDecreaseInDatastoreBytesInUse_givenSetValueDeleted() {
    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    jedis1.sadd(SET_KEY, "value");

    long intermediateDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    assertThat(intermediateDataStoreBytesInUse).isGreaterThan(initialDataStoreBytesInUse);

    jedis1.del(SET_KEY);

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showDecreaseInDatastoreBytesInUse_givenSetValueSizeDecreases() {
    for (int i = 0; i < 10; i++) {
      jedis1.sadd(SET_KEY, "value" + i);
    }

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 10; i++) {
      jedis1.srem(SET_KEY, "value" + i);
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isLessThan(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showIncreaseInDatastoreBytesInUse_givenHashValueSizeIncreases() {
    jedis1.hset(HASH_KEY, FIELD, "value");

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis1.hset(HASH_KEY, FIELD + i, LONG_APPEND_VALUE);
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isGreaterThan(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showDecreaseInDatastoreBytesInUse_givenHashValueDeleted() {
    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    jedis1.hset(HASH_KEY, FIELD, "value");

    long intermediateDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    assertThat(intermediateDataStoreBytesInUse).isGreaterThan(initialDataStoreBytesInUse);

    jedis1.del(HASH_KEY);

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showNoIncreaseInDatastoreBytesInUse_givenHSetDoesNotIncreaseHashSize() {
    jedis2.hset(HASH_KEY, FIELD, "initialvalue"); // two hsets are required to force
    jedis2.hset(HASH_KEY, FIELD, "value"); // deserialization on both servers
    // otherwise primary/secondary can disagree on size, and which server is primary varies

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    for (int i = 0; i < 10; i++) {
      jedis2.hset(HASH_KEY, FIELD, "value");
    }

    assertThat(jedis2.hgetAll(HASH_KEY).size()).isEqualTo(1);

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showIncreaseInDatastoreBytesInUse_givenHSetNXIncreasesHashSize() {
    jedis1.hset(HASH_KEY, FIELD, "value");

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis1.hsetnx(HASH_KEY, FIELD + i, "value");
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isGreaterThan(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showNoIncreaseInDatastoreBytesInUse_givenHSetNXDoesNotIncreaseHashSize() {
    jedis1.hset(HASH_KEY, FIELD, "value");

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 1000; i++) {
      jedis1.hsetnx(HASH_KEY, FIELD, "value");
    }

    long finalDataStoreBytesInUse = clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(finalDataStoreBytesInUse).isEqualTo(initialDataStoreBytesInUse);
  }

  /******* confirm that the other member agrees upon size *******/

  @Test
  public void should_showMembersAgreeUponUsedHashMemory_afterDeltaPropagation() {
    jedis1.hset(HASH_KEY, FIELD, "initialvalue"); // two hsets are required to force
    jedis1.hset(HASH_KEY, FIELD, "finalvalue"); // deserialization on both servers
    // otherwise primary/secondary can disagree on size, and which server is primary varies

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    for (int i = 0; i < 10; i++) {
      jedis1.hset(HASH_KEY, FIELD, "finalvalue");
    }

    assertThat(jedis1.hgetAll(HASH_KEY).size()).isEqualTo(1);

    long server2FinalDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);
    long server1FinalDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    assertThat(server1FinalDataStoreBytesInUse)
        .isEqualTo(server2FinalDataStoreBytesInUse)
        .isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showMembersAgreeUponUsedSetMemory_afterDeltaPropagationWhenAddingMembers() {
    jedis1.sadd(SET_KEY, "other"); // two sadds are required to force
    jedis1.sadd(SET_KEY, "value"); // deserialization on both servers
    // otherwise primary/secondary can disagree on size, and which server is primary varies

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    for (int i = 0; i < 10; i++) {
      jedis1.sadd(SET_KEY, "value");
    }

    assertThat(jedis1.scard(SET_KEY)).isEqualTo(2);

    long server1FinalDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long server2FinalDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(server1FinalDataStoreBytesInUse)
        .isEqualTo(server2FinalDataStoreBytesInUse)
        .isEqualTo(initialDataStoreBytesInUse);
  }

  @Test
  public void should_showMembersAgreeUponUsedSetMemory_afterDeltaPropagationWhenRemovingMembers() {
    String value1 = "value1";
    String value2 = "value2";
    jedis1.sadd(SET_KEY, value1); // two sadds are required to force
    jedis1.sadd(SET_KEY, value2); // deserialization on both servers
    // otherwise primary/secondary can disagree on size, and which server is primary varies

    jedis1.sadd(SET_KEY, "value3");

    jedis2.srem(SET_KEY, value1, value2);

    assertThat(jedis1.scard(SET_KEY)).isEqualTo(1);

    long server1DataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long server2DataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(server1DataStoreBytesInUse).isEqualTo(server2DataStoreBytesInUse);
  }

  @Test
  @Ignore("find a way to force deserialization on both members before enabling")
  public void should_showMembersAgreeUponUsedStringMemory_afterDeltaPropagation() {
    String value = "value";

    jedis1.set(STRING_KEY, "12345"); // two sets are required to force
    jedis1.set(STRING_KEY, value); // deserialization on both servers
    // otherwise primary/secondary can disagree on size, and which server is primary varies

    long initialDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);

    for (int i = 0; i < 10; i++) {
      jedis1.set(STRING_KEY, value);
    }

    assertThat(jedis1.exists(STRING_KEY)).isTrue();
    assertThat(jedis2.exists(STRING_KEY)).isTrue();

    assertThat(jedis1.get(STRING_KEY)).isEqualTo(value);
    assertThat(jedis2.get(STRING_KEY)).isEqualTo(value);

    long server1FinalDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server1);
    long server2FinalDataStoreBytesInUse =
        clusterStartUpRule.getDataStoreBytesInUseForDataRegion(server2);

    assertThat(server1FinalDataStoreBytesInUse)
        .isEqualTo(initialDataStoreBytesInUse);

    assertThat(server2FinalDataStoreBytesInUse)
        .isEqualTo(initialDataStoreBytesInUse);
  }
}
