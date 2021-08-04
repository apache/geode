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

package org.apache.geode.redis.internal.executor.server;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.logging.log4j.util.TriConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.RedisTestHelper;
import org.apache.geode.redis.internal.PassiveExpirationManager;
import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractHitsMissesIntegrationTest implements RedisIntegrationTest {

  private static final String HITS = "keyspace_hits";
  private static final String MISSES = "keyspace_misses";
  private static final String STRING_KEY = "string";
  private static final String STRING_INT_KEY = "int";
  private static final String SET_KEY = "set";
  private static final String HASH_KEY = "hash";
  private static final String SORTED_SET_KEY = "sortedSet";
  private static final String MAP_KEY_1 = "mapKey1";
  private static final String MAP_KEY_2 = "mapKey2";

  protected Jedis jedis;

  @BeforeClass
  public static void classSetup() {
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_HASH_ID,
        RedisHash.class);
  }

  @Before
  public void setup() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);

    jedis.set(STRING_KEY, "yarn");
    jedis.set(STRING_INT_KEY, "5");
    jedis.sadd(SET_KEY, "cotton");
    jedis.hset(HASH_KEY, "green", "eggs");
    jedis.zadd(SORTED_SET_KEY, -2.0, "almonds");
    jedis.mset(MAP_KEY_1, "fox", MAP_KEY_2, "box");
  }

  @After
  public void teardown() {
    jedis.flushAll();
    jedis.close();
  }

  /***********************************************
   ************* Supported Commands **************
   **********************************************/

  /************* Key related commands *************/
  @Test
  public void testKeys() {
    runCommandAndAssertNoStatUpdates("*", k -> jedis.keys(k));
  }

  @Test
  public void testExists() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.exists(k));
  }

  @Test
  public void testType() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.type(k));
  }

  @Test
  public void testTtl() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.ttl(k));
  }

  @Test
  public void testPttl() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.pttl(k));
  }

  @Test
  public void testRename() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, (k, v) -> jedis.rename(k, v));
  }

  @Test
  public void testDel() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, k -> jedis.del(k));
  }

  @Test
  public void testExpire() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, (k) -> jedis.expire(k, 5L));
  }

  @Test
  public void testPassiveExpiration() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, (k) -> {
      jedis.expire(k, 1L);
      GeodeAwaitility.await().atMost(Duration.ofMinutes(PassiveExpirationManager.INTERVAL * 2))
          .until(() -> jedis.keys(HASH_KEY).isEmpty());
    });
  }

  @Test
  public void testExpireAt() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, (k) -> jedis.expireAt(k, 2145916800));
  }

  @Test
  public void testPExpire() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, (k) -> jedis.pexpire(k, 1024));
  }

  @Test
  public void testPExpireAt() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, (k) -> jedis.pexpireAt(k, 1608247597));
  }

  @Test
  public void testPersist() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, (k) -> jedis.persist(k));
  }

  @Test
  public void testDump() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, (k) -> jedis.dump(k));
  }

  @Test
  public void testRestore() {
    byte[] data = jedis.dump(HASH_KEY);
    runCommandAndAssertNoStatUpdates("hash-2", (k) -> jedis.restore(k, 0L, data));
  }

  /************* String related commands *************/
  @Test
  public void testGet() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.get(k));
  }

  @Test
  public void testAppend() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, (k, v) -> jedis.append(k, v));
  }

  @Test
  public void testSet() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, (k, v) -> jedis.set(k, v));
  }

  @Test
  public void testSet_wrongType() {
    runCommandAndAssertNoStatUpdates(SET_KEY, (k, v) -> jedis.set(k, v));
  }

  @Test
  public void testStrlen() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.strlen(k));
  }

  @Test
  public void testDecr() {
    runCommandAndAssertNoStatUpdates(STRING_INT_KEY, k -> jedis.decr(k));
  }

  @Test
  public void testDecrby() {
    runCommandAndAssertNoStatUpdates(STRING_INT_KEY, k -> jedis.decrBy(k, 1));
  }

  @Test
  public void testGetrange() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.getrange(k, 1L, 2L));
  }

  @Test
  public void testIncr() {
    runCommandAndAssertNoStatUpdates(STRING_INT_KEY, k -> jedis.incr(k));
  }

  @Test
  public void testIncrby() {
    runCommandAndAssertNoStatUpdates(STRING_INT_KEY, k -> jedis.incrBy(k, 1L));
  }

  @Test
  public void testIncrbyfloat() {
    runCommandAndAssertNoStatUpdates(STRING_INT_KEY, k -> jedis.incrByFloat(k, 1.0));
  }

  @Test
  public void testMget() {
    runCommandAndAssertHitsAndMisses(MAP_KEY_1, MAP_KEY_2, (k1, k2) -> jedis.mget(k1, k2));
  }

  @Test
  public void testSetnx() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, (k, v) -> jedis.setnx(k, v));
  }

  /************* SortedSet related commands *************/
  @Test
  public void testZadd() {
    runCommandAndAssertNoStatUpdates(SORTED_SET_KEY, (k, v) -> jedis.zadd(k, 1.0, v));
  }

  @Test
  public void testZcard() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zcard(k));
  }

  @Test
  public void testZcount() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zcount(k, "-inf", "inf"));
  }

  @Test
  public void testZIncrBy() {
    runCommandAndAssertNoStatUpdates("key", (k, m) -> jedis.zincrby(k, 100.0, m));
  }

  @Test
  public void testZPopMax() {
    runCommandAndAssertNoStatUpdates(SORTED_SET_KEY, k -> jedis.zpopmax(k, 1));
  }

  @Test
  public void testZrange() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zrange(k, 0, 1));
  }

  @Test
  public void testZrange_withScores() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zrangeWithScores(k, 0, 1));
  }

  @Test
  public void testZrangeByLex() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zrangeByLex(k, "-", "+"));
  }

  @Test
  public void testZrangeByScore() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zrangeByScore(k, 0.0, 1.0));
  }

  @Test
  public void testZrank() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, (k, m) -> jedis.zrank(k, m));
  }

  @Test
  public void testZrem() {
    runCommandAndAssertNoStatUpdates(SORTED_SET_KEY, (k, v) -> jedis.zrem(k, v));
  }

  @Test
  public void testZrevrange() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zrevrange(k, 0, 1));
  }

  @Test
  public void testZrevrange_withScores() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zrevrangeWithScores(k, 0, 1));
  }

  @Test
  public void testZrevrank() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, (k, m) -> jedis.zrevrank(k, m));
  }

  @Test
  public void testZscore() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, (k, v) -> jedis.zscore(k, v));
  }

  /************* Set related commands *************/
  @Test
  public void testSadd() {
    runCommandAndAssertNoStatUpdates(SET_KEY, (k, v) -> jedis.sadd(k, v));
  }

  @Test
  public void testSrem() {
    runCommandAndAssertNoStatUpdates(SET_KEY, (k, v) -> jedis.srem(k, v));
  }

  @Test
  public void testSmembers() {
    runCommandAndAssertHitsAndMisses(SET_KEY, k -> jedis.smembers(k));
  }

  /************* Hash related commands *************/
  @Test
  public void testHset() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, (k, v, s) -> jedis.hset(k, v, s));
  }

  @Test
  public void testHgetall() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, k -> jedis.hgetAll(k));
  }

  @Test
  public void testHMSet() {
    Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");

    runCommandAndAssertNoStatUpdates("key", (k) -> jedis.hmset(k, map));
  }

  @Test
  public void testHdel() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, (k, v) -> jedis.hdel(k, v));
  }

  @Test
  public void testHget() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, (k, v) -> jedis.hget(k, v));
  }

  @Test
  public void testHkeys() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, k -> jedis.hkeys(k));
  }

  @Test
  public void testHlen() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, k -> jedis.hlen(k));
  }

  @Test
  public void testHvals() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, k -> jedis.hvals(k));
  }

  @Test
  public void testHmget() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, (k, v) -> jedis.hmget(k, v));
  }

  @Test
  public void testHexists() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, (k, v) -> jedis.hexists(k, v));
  }

  @Test
  public void testHstrlen() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, (k, v) -> jedis.hstrlen(k, v));
  }

  @Test
  public void testHscan() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, (k, v) -> jedis.hscan(k, v));
  }

  @Test
  public void testHincrby() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, (k, f) -> jedis.hincrBy(k, f, 1L));
  }

  @Test
  public void testHincrbyfloat() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, (k, f) -> jedis.hincrByFloat(k, f, 1.0));
  }

  @Test
  public void testHsetnx() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, (k, f, v) -> jedis.hsetnx(k, f, v));
  }

  /**********************************************
   ********** Unsupported Commands **************
   *********************************************/

  /************* Key related commands *************/
  @Test
  public void testScan() {
    runCommandAndAssertNoStatUpdates("0", k -> jedis.scan(k));
  }

  @Test
  public void testUnlink() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, k -> jedis.unlink(k));
  }

  /************* String related commands *************/
  @Test
  public void testGetset() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, (k, v) -> jedis.getSet(k, v));
  }

  @Test
  public void testMset() {
    runCommandAndAssertNoStatUpdates(MAP_KEY_1, (k, v) -> jedis.mset(k, v));
  }

  // todo updates stats when it shouldn't. not implemented in the function executor
  @Ignore
  @Test
  public void testMsetnx() {
    runCommandAndAssertNoStatUpdates(MAP_KEY_1, (k, v) -> jedis.msetnx(k, v));
  }

  @Test
  public void testSetex() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, (k, v) -> jedis.setex(k, 200L, v));
  }

  @Test
  public void testSetrange() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, (k, v) -> jedis.setrange(k, 1L, v));
  }

  /************* Bit related commands *************/
  @Test
  public void testBitcount() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.bitcount(k));
  }

  @Test
  public void testBitpos() {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    long currentHits = Long.parseLong(info.get(HITS));
    long currentMisses = Long.parseLong(info.get(MISSES));

    jedis.bitpos(STRING_KEY, true);
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses));

    jedis.bitpos("missed", true);
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1));
  }

  @Test
  public void testBitop() {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    long currentHits = Long.parseLong(info.get(HITS));
    long currentMisses = Long.parseLong(info.get(MISSES));

    jedis.bitop(BitOP.OR, "dest", STRING_KEY, STRING_KEY, "dest");
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 2));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1));

    jedis.bitop(BitOP.OR, "dest", STRING_KEY, "missed");
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 2 + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1 + 1));
  }

  @Test
  public void testGetbit() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.getbit(k, 1));
  }

  @Test
  public void testSetbit() {
    runCommandAndAssertNoStatUpdates(STRING_INT_KEY, (k, v) -> jedis.setbit(k, 0L, "1"));
  }

  /************* Set related commands *************/
  // FYI - In Redis 5.x SPOP produces inconsistent results depending on whether a count was given
  // or not. In Redis 6.x SPOP does not update any stats.
  @Test
  public void testSpop() {
    runCommandAndAssertNoStatUpdates(SET_KEY, k -> jedis.spop(k));
  }

  @Test
  public void testSismember() {
    runCommandAndAssertHitsAndMisses(SET_KEY, (k, v) -> jedis.sismember(k, v));
  }

  @Test
  public void testSrandmember() {
    runCommandAndAssertHitsAndMisses(SET_KEY, k -> jedis.srandmember(k));
  }

  @Test
  public void testScard() {
    runCommandAndAssertHitsAndMisses(SET_KEY, k -> jedis.scard(k));
  }

  @Test
  public void testSscan() {
    runCommandAndAssertHitsAndMisses(SET_KEY, (k, v) -> jedis.sscan(k, v));
  }

  @Test
  public void testSdiff() {
    runDiffCommandAndAssertHitsAndMisses(SET_KEY, (k, v) -> jedis.sdiff(k, v));
  }

  @Test
  public void testSdiffstore() {
    runDiffStoreCommandAndAssertNoStatUpdates(SET_KEY, (k, v, s) -> jedis.sdiffstore(k, v, s));
  }

  @Test
  public void testSinter() {
    runDiffCommandAndAssertHitsAndMisses(SET_KEY, (k, v) -> jedis.sinter(k, v));
  }

  @Test
  public void testSinterstore() {
    runDiffStoreCommandAndAssertNoStatUpdates(SET_KEY, (k, v, s) -> jedis.sinterstore(k, v, s));
  }

  @Test
  public void testSunion() {
    runDiffCommandAndAssertHitsAndMisses(SET_KEY, (k, v) -> jedis.sunion(k, v));
  }

  @Test
  public void testSunionstore() {
    runDiffStoreCommandAndAssertNoStatUpdates(SET_KEY, (k, v, s) -> jedis.sunionstore(k, v, s));
  }

  @Test
  public void testSmove() {
    runCommandAndAssertNoStatUpdates(SET_KEY, (k, d, m) -> jedis.smove(k, d, m));
  }

  /************* Helper Methods *************/
  private void runCommandAndAssertHitsAndMisses(String key, Consumer<String> command) {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    long currentHits = Long.parseLong(info.get(HITS));
    long currentMisses = Long.parseLong(info.get(MISSES));

    command.accept(key);
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses));

    command.accept("missed");
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1));
  }

  private void runCommandAndAssertHitsAndMisses(String key, BiConsumer<String, String> command) {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    long currentHits = Long.parseLong(info.get(HITS));
    long currentMisses = Long.parseLong(info.get(MISSES));

    command.accept(key, "42");
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses));

    command.accept("missed", "42");
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1));
  }

  private void runCommandAndAssertHitsAndMisses(String key1, String key2,
      BiConsumer<String, String> command) {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    long currentHits = Long.parseLong(info.get(HITS));
    long currentMisses = Long.parseLong(info.get(MISSES));

    command.accept(key1, key2);
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 2));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses));
  }

  private void runDiffCommandAndAssertHitsAndMisses(String key,
      BiConsumer<String, String> command) {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    long currentHits = Long.parseLong(info.get(HITS));
    long currentMisses = Long.parseLong(info.get(MISSES));

    command.accept(key, key);
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 2));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses));

    command.accept(key, "missed");
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 3));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1));
  }

  /**
   * When storing diff-ish results, hits and misses are never updated
   */
  private void runDiffStoreCommandAndAssertNoStatUpdates(String key,
      TriConsumer<String, String, String> command) {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    String currentHits = info.get(HITS);
    String currentMisses = info.get(MISSES);

    command.accept("destination", key, key);
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);

    command.accept("destination", key, "missed");
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);
  }

  private void runCommandAndAssertNoStatUpdates(String key, Consumer<String> command) {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    String currentHits = info.get(HITS);
    String currentMisses = info.get(MISSES);

    command.accept(key);
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);
  }

  private void runCommandAndAssertNoStatUpdates(String key, BiConsumer<String, String> command) {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    String currentHits = info.get(HITS);
    String currentMisses = info.get(MISSES);

    command.accept(key, "42");
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);
  }

  private void runCommandAndAssertNoStatUpdates(String key,
      TriConsumer<String, String, String> command) {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    String currentHits = info.get(HITS);
    String currentMisses = info.get(MISSES);

    command.accept(key, key, "42");
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);
  }
}
