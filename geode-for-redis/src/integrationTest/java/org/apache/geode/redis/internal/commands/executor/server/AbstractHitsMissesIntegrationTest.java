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

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.RedisTestHelper;

public abstract class AbstractHitsMissesIntegrationTest implements RedisIntegrationTest {

  private static final String HASHTAG = "{hashtag}";
  private static final String HITS = "keyspace_hits";
  private static final String MISSES = "keyspace_misses";
  private static final String STRING_KEY = HASHTAG + "string";
  private static final String STRING_INT_KEY = HASHTAG + "int";
  private static final String SET_KEY = HASHTAG + "set";
  private static final String HASH_KEY = HASHTAG + "hash";
  private static final String SORTED_SET_KEY = HASHTAG + "sortedSet";

  protected Jedis jedis;

  @Before
  public void setup() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);

    jedis.set(STRING_KEY, "yarn");
    jedis.set(STRING_INT_KEY, "5");
    jedis.sadd(SET_KEY, "cotton");
    jedis.hset(HASH_KEY, "green", "eggs");
    jedis.zadd(SORTED_SET_KEY, -2.0, "almonds");
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
    String key1 = HASHTAG + "key1";
    String key2 = HASHTAG + "key2";
    jedis.set(key1, "yarn");
    runCommandAndAssertNoStatUpdates(key1, k -> jedis.rename(k, key2));
  }

  @Test
  public void testDel() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, k -> jedis.del(k));
  }

  @Test
  public void testExpire() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, k -> jedis.expire(k, 5L));
  }

  @Test
  public void testExpireAt() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, k -> jedis.expireAt(k, 2145916800));
  }

  @Test
  public void testPExpire() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, k -> jedis.pexpire(k, 1024));
  }

  @Test
  public void testPExpireAt() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, k -> jedis.pexpireAt(k, 1608247597));
  }

  @Test
  public void testPersist() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, k -> jedis.persist(k));
  }

  @Test
  public void testDump() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, k -> jedis.dump(k));
  }

  @Test
  public void testRestore() {
    byte[] data = jedis.dump(HASH_KEY);
    runCommandAndAssertNoStatUpdates("hash-2", k -> jedis.restore(k, 0L, data));
  }

  /************* String related commands *************/
  @Test
  public void testGet() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.get(k));
  }

  @Test
  public void testAppend() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, k -> jedis.append(k, "value"));
  }

  @Test
  public void testSet() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, k -> jedis.set(k, "value"));
  }

  @Test
  public void testSet_wrongType() {
    runCommandAndAssertNoStatUpdates(SET_KEY, k -> jedis.set(k, "value"));
  }

  @Test
  public void testSetex() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, k -> jedis.setex(k, 200L, "value"));
  }

  @Test
  public void testPsetex() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, k -> jedis.psetex(k, 200000L, "value"));
  }

  @Test
  public void testGetset() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.getSet(k, "value"));
  }

  @Test
  public void testSetrange() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, k -> jedis.setrange(k, 1L, "value"));
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
    runMultiKeyCommandAndAssertHitsAndMisses(STRING_KEY, (k1, k2) -> jedis.mget(k1, k2));
  }

  @Test
  public void testMset() {
    runMultiKeyCommandAndAssertNoStatUpdates(STRING_KEY,
        (k1, k2) -> jedis.mset(k1, "value1", k2, "value2"));
  }

  @Test
  public void testMsetnx() {
    runMultiKeyCommandAndAssertNoStatUpdates(STRING_KEY,
        (k1, k2) -> jedis.msetnx(k1, "value1", k2, "value2"));
  }

  @Test
  public void testSetnx() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, k -> jedis.setnx(k, "value"));
  }

  /************* SortedSet related commands *************/
  @Test
  public void testZadd() {
    runCommandAndAssertNoStatUpdates(SORTED_SET_KEY, k -> jedis.zadd(k, 1.0, "member"));
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
    runCommandAndAssertNoStatUpdates(SORTED_SET_KEY, k -> jedis.zincrby(k, 100.0, "member"));
  }

  @Test
  public void testZInterStore() {
    runMultiKeyCommandAndAssertNoStatUpdates(SORTED_SET_KEY,
        (k1, k2) -> jedis.zinterstore(HASHTAG + "dest", k1, k2));
  }

  @Test
  public void testZLexCount() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zlexcount(k, "-", "+"));
  }

  @Test
  public void testZPopMax() {
    runCommandAndAssertNoStatUpdates(SORTED_SET_KEY, k -> jedis.zpopmax(k, 1));
  }

  @Test
  public void testZPopMin() {
    runCommandAndAssertNoStatUpdates(SORTED_SET_KEY, k -> jedis.zpopmin(k, 1));
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
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zrank(k, "member"));
  }

  @Test
  public void testZrem() {
    runCommandAndAssertNoStatUpdates(SORTED_SET_KEY, k -> jedis.zrem(k, "member"));
  }

  @Test
  public void testZremrangeByLex() {
    runCommandAndAssertNoStatUpdates(SORTED_SET_KEY, k -> jedis.zremrangeByLex(k, "-", "+"));
  }

  @Test
  public void testZremrangeByRank() {
    runCommandAndAssertNoStatUpdates(SORTED_SET_KEY, k -> jedis.zremrangeByRank(k, 0, 1));
  }

  @Test
  public void testZremrangeByScore() {
    runCommandAndAssertNoStatUpdates(SORTED_SET_KEY, k -> jedis.zremrangeByScore(k, 0.0, 1.0));
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
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zrevrank(k, "member"));
  }

  @Test
  public void testZscan() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zscan(k, "0"));
  }

  @Test
  public void testZscore() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zscore(k, "member"));
  }

  @Test
  public void testZrevrangeByLex() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zrevrangeByLex(k, "+", "-"));
  }

  @Test
  public void testZrevrangeByScore() {
    runCommandAndAssertHitsAndMisses(SORTED_SET_KEY, k -> jedis.zrevrangeByScore(k, 1, 0));
  }

  @Test
  public void testZUnionStore() {
    runMultiKeyCommandAndAssertNoStatUpdates(SORTED_SET_KEY,
        (k1, k2) -> jedis.zunionstore(HASHTAG + "dest", k1, k2));
  }

  /************* Set related commands *************/
  @Test
  public void testSadd() {
    runCommandAndAssertNoStatUpdates(SET_KEY, k -> jedis.sadd(k, "member"));
  }

  @Test
  public void testScard() {
    runCommandAndAssertHitsAndMisses(SET_KEY, k -> jedis.scard(k));
  }

  @Test
  public void testSdiff() {
    runMultiKeyCommandAndAssertHitsAndMisses(SET_KEY, (k1, k2) -> jedis.sdiff(k1, k2));
  }

  @Test
  public void testSdiffstore() {
    runMultiKeyCommandAndAssertNoStatUpdates(SET_KEY,
        (k1, k2) -> jedis.sdiffstore(HASHTAG + "dest", k1, k2));
  }

  @Test
  public void testSinter() {
    runMultiKeyCommandAndAssertHitsAndMisses(SET_KEY, (k1, k2) -> jedis.sinter(k1, k2));
  }

  @Test
  public void testSismember() {
    runCommandAndAssertHitsAndMisses(SET_KEY, k -> jedis.sismember(k, "member"));
  }

  @Test
  public void testSmembers() {
    runCommandAndAssertHitsAndMisses(SET_KEY, k -> jedis.smembers(k));
  }

  @Test
  public void testSmove() {
    runMultiKeyCommandAndAssertNoStatUpdates(SET_KEY, (k1, k2) -> jedis.smove(k1, k2, "member"));
  }

  @Test
  public void testSrem() {
    runCommandAndAssertNoStatUpdates(SET_KEY, k -> jedis.srem(k, "member"));
  }

  @Test
  public void testSunion() {
    runMultiKeyCommandAndAssertHitsAndMisses(SET_KEY, (k1, k2) -> jedis.sunion(k1, k2));
  }

  /************* Hash related commands *************/
  @Test
  public void testHset() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, k -> jedis.hset(k, "field", "value"));
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

    runCommandAndAssertNoStatUpdates(HASH_KEY, k -> jedis.hmset(k, map));
  }

  @Test
  public void testHdel() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, k -> jedis.hdel(k, "field"));
  }

  @Test
  public void testHget() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, k -> jedis.hget(k, "field"));
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
    runCommandAndAssertHitsAndMisses(HASH_KEY, k -> jedis.hmget(k, "field1", "field2"));
  }

  @Test
  public void testHexists() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, k -> jedis.hexists(k, "field"));
  }

  @Test
  public void testHstrlen() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, k -> jedis.hstrlen(k, "field"));
  }

  @Test
  public void testHscan() {
    runCommandAndAssertHitsAndMisses(HASH_KEY, k -> jedis.hscan(k, "0"));
  }

  @Test
  public void testHincrby() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, k -> jedis.hincrBy(k, "field", 1L));
  }

  @Test
  public void testHincrbyfloat() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, k -> jedis.hincrByFloat(k, "field", 1.0));
  }

  @Test
  public void testHsetnx() {
    runCommandAndAssertNoStatUpdates(HASH_KEY, k -> jedis.hsetnx(k, "field", "value"));
  }

  /**********************************************
   ********** Unsupported Commands **************
   *********************************************/

  /************* Key related commands *************/
  @Test
  public void testScan() {
    runCommandAndAssertNoStatUpdates("", unused -> jedis.scan("0"));
  }

  @Test
  public void testUnlink() {
    runCommandAndAssertNoStatUpdates(STRING_KEY, k -> jedis.unlink(k));
  }

  /************* Bit related commands *************/
  @Test
  public void testBitcount() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.bitcount(k));
  }

  @Test
  public void testBitpos() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.bitpos(k, true));
  }

  @Test
  public void testBitop() {
    runMultiKeyCommandAndAssertHitsAndMisses(STRING_KEY,
        (k1, k2) -> jedis.bitop(BitOP.OR, HASHTAG + "dest", k1, k2));
  }

  @Test
  public void testGetbit() {
    runCommandAndAssertHitsAndMisses(STRING_KEY, k -> jedis.getbit(k, 1));
  }

  @Test
  public void testSetbit() {
    runCommandAndAssertNoStatUpdates(STRING_INT_KEY, k -> jedis.setbit(k, 0L, "1"));
  }

  /************* Set related commands *************/
  // FYI - In Redis 5.x SPOP produces inconsistent results depending on whether a count was given
  // or not. In Redis 6.x SPOP does not update any stats.
  @Test
  public void testSpop() {
    runCommandAndAssertNoStatUpdates(SET_KEY, k -> jedis.spop(k));
  }

  @Test
  public void testSrandmember() {
    runCommandAndAssertHitsAndMisses(SET_KEY, k -> jedis.srandmember(k));
  }

  @Test
  public void testSscan() {
    runCommandAndAssertHitsAndMisses(SET_KEY, k -> jedis.sscan(k, "0"));
  }

  @Test
  public void testSinterstore() {
    runMultiKeyCommandAndAssertNoStatUpdates(SET_KEY,
        (k1, k2) -> jedis.sinterstore(HASHTAG + "dest", k1, k2));
  }

  @Test
  public void testSunionstore() {
    runMultiKeyCommandAndAssertNoStatUpdates(SET_KEY,
        (k1, k2) -> jedis.sunionstore(HASHTAG + "dest", k1, k2));
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

  private void runCommandAndAssertNoStatUpdates(String key, Consumer<String> command) {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    String currentHits = info.get(HITS);
    String currentMisses = info.get(MISSES);

    command.accept(key);
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);
  }

  private void runMultiKeyCommandAndAssertHitsAndMisses(String key,
      BiConsumer<String, String> command) {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    long currentHits = Long.parseLong(info.get(HITS));
    long currentMisses = Long.parseLong(info.get(MISSES));

    command.accept(key, key);
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 2));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses));

    command.accept(key, HASHTAG + "missed");
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 3));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1));
  }

  private void runMultiKeyCommandAndAssertNoStatUpdates(String key,
      BiConsumer<String, String> command) {
    Map<String, String> info = RedisTestHelper.getInfo(jedis);
    String currentHits = info.get(HITS);
    String currentMisses = info.get(MISSES);

    command.accept(key, key);
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);

    command.accept(key, HASHTAG + "missed");
    info = RedisTestHelper.getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);
  }
}
