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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.logging.log4j.util.TriConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;

import org.apache.geode.redis.internal.PassiveExpirationManager;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractHitsMissesIntegrationTest implements RedisPortSupplier {

  private static final String HITS = "keyspace_hits";
  private static final String MISSES = "keyspace_misses";

  protected Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void classSetup() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);

    jedis.set("string", "yarn");
    jedis.set("int", "5");
    jedis.sadd("set", "cotton");
    jedis.hset("hash", "green", "eggs");
    jedis.mset("mapKey1", "fox", "mapKey2", "box");
  }

  @After
  public void teardown() {
    jedis.flushAll();
    jedis.close();
  }

  /***********************************************
   ************* Supported Commands **************
   **********************************************/
  // ------------ Key related commands -----------

  @Test
  public void testKeys() {
    runCommandAndAssertNoStatUpdates("*", k -> jedis.keys(k));
  }

  @Test
  public void testExists() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.exists(k));
  }

  @Test
  public void testType() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.type(k));
  }

  @Test
  public void testTtl() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.ttl(k));
  }

  @Test
  public void testPttl() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.pttl(k));
  }

  @Test
  public void testRename() {
    runCommandAndAssertNoStatUpdates("string", (k, v) -> jedis.rename(k, v));
  }

  @Test
  public void testDel() {
    runCommandAndAssertNoStatUpdates("string", k -> jedis.del(k));
  }

  // ------------ String related commands -----------

  @Test
  public void testGet() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.get(k));
  }

  @Test
  public void testAppend() {
    runCommandAndAssertNoStatUpdates("string", (k, v) -> jedis.append(k, v));
  }

  @Test
  public void testSet() {
    runCommandAndAssertNoStatUpdates("string", (k, v) -> jedis.set(k, v));
  }

  @Test
  public void testSet_wrongType() {
    runCommandAndAssertNoStatUpdates("set", (k, v) -> jedis.set(k, v));
  }

  // ------------ Set related commands -----------
  @Test
  public void testSadd() {
    runCommandAndAssertNoStatUpdates("set", (k, v) -> jedis.sadd(k, v));
  }

  @Test
  public void testSrem() {
    runCommandAndAssertNoStatUpdates("set", (k, v) -> jedis.srem(k, v));
  }

  @Test
  public void testSmembers() {
    runCommandAndAssertHitsAndMisses("set", k -> jedis.smembers(k));
  }

  // ------------ Hash related commands -----------
  @Test
  public void testHset() {
    runCommandAndAssertNoStatUpdates("hash", (k, v, s) -> jedis.hset(k, v, s));
  }

  @Test
  public void testHgetall() {
    runCommandAndAssertHitsAndMisses("hash", k -> jedis.hgetAll(k));
  }

  @Test
  public void testHMSet() {
    Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");

    runCommandAndAssertNoStatUpdates("key", (k) -> jedis.hmset(k, map));
  }

  // ------------ Key related commands -----------

  @Test
  public void testExpire() {
    runCommandAndAssertNoStatUpdates("hash", (k) -> jedis.expire(k, 5));
  }

  @Test
  public void testPassiveExpiration() {
    runCommandAndAssertNoStatUpdates("hash", (k) -> {
      jedis.expire(k, 1);
      GeodeAwaitility.await().atMost(Duration.ofMinutes(PassiveExpirationManager.INTERVAL * 2))
          .until(() -> jedis.keys("hash").isEmpty());
    });
  }

  @Test
  public void testExpireAt() {
    runCommandAndAssertNoStatUpdates("hash", (k) -> jedis.expireAt(k, 2145916800));
  }

  @Test
  public void testPExpire() {
    runCommandAndAssertNoStatUpdates("hash", (k) -> jedis.pexpire(k, 1024));
  }

  @Test
  public void testPExpireAt() {
    runCommandAndAssertNoStatUpdates("hash", (k) -> jedis.pexpireAt(k, 1608247597));
  }

  @Test
  public void testPersist() {
    runCommandAndAssertNoStatUpdates("hash", (k) -> jedis.persist(k));
  }

  /**********************************************
   ********** Unsupported Commands **************
   *********************************************/
  // ---------- Key related commands -----------
  @Test
  public void testScan() {
    runCommandAndAssertNoStatUpdates("0", k -> jedis.scan(k));
  }

  @Test
  public void testUnlink() {
    runCommandAndAssertNoStatUpdates("string", k -> jedis.unlink(k));
  }

  // ------------ String related commands -----------
  @Test
  public void testGetset() {
    runCommandAndAssertHitsAndMisses("string", (k, v) -> jedis.getSet(k, v));
  }

  @Test
  public void testStrlen() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.strlen(k));
  }

  @Test
  public void testDecr() {
    runCommandAndAssertNoStatUpdates("int", k -> jedis.decr(k));
  }

  @Test
  public void testDecrby() {
    runCommandAndAssertNoStatUpdates("int", k -> jedis.decrBy(k, 1));
  }

  @Test
  public void testGetrange() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.getrange(k, 1l, 2l));
  }

  @Test
  public void testIncr() {
    runCommandAndAssertNoStatUpdates("int", k -> jedis.incr(k));
  }

  @Test
  public void testIncrby() {
    runCommandAndAssertNoStatUpdates("int", k -> jedis.incrBy(k, 1l));
  }

  @Test
  public void testIncrbyfloat() {
    runCommandAndAssertNoStatUpdates("int", k -> jedis.incrByFloat(k, 1.0));
  }

  @Test
  public void testMget() {
    runCommandAndAssertHitsAndMisses("mapKey1", "mapKey2", (k1, k2) -> jedis.mget(k1, k2));
  }

  @Test
  public void testMset() {
    runCommandAndAssertNoStatUpdates("mapKey1", (k, v) -> jedis.mset(k, v));
  }

  // todo updates stats when it shouldn't. not implemented in the function executor
  @Ignore
  @Test
  public void testMsetnx() {
    runCommandAndAssertNoStatUpdates("mapKey1", (k, v) -> jedis.msetnx(k, v));
  }

  @Test
  public void testSetex() {
    runCommandAndAssertNoStatUpdates("string", (k, v) -> jedis.setex(k, 200, v));
  }

  @Test
  public void testSetnx() {
    runCommandAndAssertNoStatUpdates("string", (k, v) -> jedis.setnx(k, v));
  }

  @Test
  public void testSetrange() {
    runCommandAndAssertNoStatUpdates("string", (k, v) -> jedis.setrange(k, 1l, v));
  }

  // ------------ Bit related commands -----------
  @Test
  public void testBitcount() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.bitcount(k));
  }

  @Test
  public void testBitpos() {
    Map<String, String> info = getInfo(jedis);
    Long currentHits = Long.parseLong(info.get(HITS));
    Long currentMisses = Long.parseLong(info.get(MISSES));

    jedis.bitpos("string", true);
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses));

    jedis.bitpos("missed", true);
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1));
  }

  @Test
  public void testBitop() {
    Map<String, String> info = getInfo(jedis);
    Long currentHits = Long.parseLong(info.get(HITS));
    Long currentMisses = Long.parseLong(info.get(MISSES));

    jedis.bitop(BitOP.OR, "dest", "string", "string", "dest");
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 2));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1));

    jedis.bitop(BitOP.OR, "dest", "string", "missed");
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 2 + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1 + 1));
  }

  @Test
  public void testGetbit() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.getbit(k, 1));
  }

  @Test
  public void testSetbit() {
    runCommandAndAssertNoStatUpdates("int", (k, v) -> jedis.setbit(k, 0l, "1"));
  }

  // ------------ Set related commands -----------
  // FYI - In Redis 5.x SPOP produces inconsistent results depending on whether a count was given
  // or not. In Redis 6.x SPOP does not update any stats.
  @Test
  public void testSpop() {
    runCommandAndAssertNoStatUpdates("set", k -> jedis.spop(k));
  }

  @Test
  public void testSismember() {
    runCommandAndAssertHitsAndMisses("set", (k, v) -> jedis.sismember(k, v));
  }

  @Test
  public void testSrandmember() {
    runCommandAndAssertHitsAndMisses("set", k -> jedis.srandmember(k));
  }

  @Test
  public void testScard() {
    runCommandAndAssertHitsAndMisses("set", k -> jedis.scard(k));
  }

  @Test
  public void testSscan() {
    runCommandAndAssertHitsAndMisses("set", (k, v) -> jedis.sscan(k, v));
  }

  @Test
  public void testSdiff() {
    runDiffCommandAndAssertHitsAndMisses("set", (k, v) -> jedis.sdiff(k, v));
  }

  @Test
  public void testSdiffstore() {
    runDiffStoreCommandAndAssertNoStatUpdates("set", (k, v, s) -> jedis.sdiffstore(k, v, s));
  }

  @Test
  public void testSinter() {
    runDiffCommandAndAssertHitsAndMisses("set", (k, v) -> jedis.sinter(k, v));
  }

  @Test
  public void testSinterstore() {
    runDiffStoreCommandAndAssertNoStatUpdates("set", (k, v, s) -> jedis.sinterstore(k, v, s));
  }

  @Test
  public void testSunion() {
    runDiffCommandAndAssertHitsAndMisses("set", (k, v) -> jedis.sunion(k, v));
  }

  @Test
  public void testSunionstore() {
    runDiffStoreCommandAndAssertNoStatUpdates("set", (k, v, s) -> jedis.sunionstore(k, v, s));
  }

  @Test
  public void testSmove() {
    runCommandAndAssertNoStatUpdates("set", (k, d, m) -> jedis.smove(k, d, m));
  }

  // ------------ Hash related commands -----------
  @Test
  public void testHdel() {
    runCommandAndAssertNoStatUpdates("hash", (k, v) -> jedis.hdel(k, v));
  }

  @Test
  public void testHget() {
    runCommandAndAssertHitsAndMisses("hash", (k, v) -> jedis.hget(k, v));
  }

  @Test
  public void testHkeys() {
    runCommandAndAssertHitsAndMisses("hash", k -> jedis.hkeys(k));
  }

  @Test
  public void testHlen() {
    runCommandAndAssertHitsAndMisses("hash", k -> jedis.hlen(k));
  }

  @Test
  public void testHvals() {
    runCommandAndAssertHitsAndMisses("hash", k -> jedis.hvals(k));
  }

  @Test
  public void testHmget() {
    runCommandAndAssertHitsAndMisses("hash", (k, v) -> jedis.hmget(k, v));
  }

  @Test
  public void testHexists() {
    runCommandAndAssertHitsAndMisses("hash", (k, v) -> jedis.hexists(k, v));
  }

  @Test
  public void testHstrlen() {
    runCommandAndAssertHitsAndMisses("hash", (k, v) -> jedis.hstrlen(k, v));
  }

  @Test
  public void testHscan() {
    runCommandAndAssertHitsAndMisses("hash", (k, v) -> jedis.hscan(k, v));
  }

  @Test
  public void testHincrby() {
    runCommandAndAssertNoStatUpdates("hash", (k, f) -> jedis.hincrBy(k, f, 1l));
  }

  @Test
  public void testHincrbyfloat() {
    runCommandAndAssertNoStatUpdates("hash", (k, f) -> jedis.hincrByFloat(k, f, 1.0));
  }

  @Test
  public void testHsetnx() {
    runCommandAndAssertNoStatUpdates("hash", (k, f, v) -> jedis.hsetnx(k, f, v));
  }

  // ------------ Helper Methods -----------

  private void runCommandAndAssertHitsAndMisses(String key, Consumer<String> command) {
    Map<String, String> info = getInfo(jedis);
    Long currentHits = Long.parseLong(info.get(HITS));
    Long currentMisses = Long.parseLong(info.get(MISSES));

    command.accept(key);
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses));

    command.accept("missed");
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1));
  }

  private void runCommandAndAssertHitsAndMisses(String key, BiConsumer<String, String> command) {
    Map<String, String> info = getInfo(jedis);
    Long currentHits = Long.parseLong(info.get(HITS));
    Long currentMisses = Long.parseLong(info.get(MISSES));

    command.accept(key, "42");
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses));

    command.accept("missed", "42");
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 1));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1));
  }

  private void runCommandAndAssertHitsAndMisses(String key1, String key2,
      BiConsumer<String, String> command) {
    Map<String, String> info = getInfo(jedis);
    Long currentHits = Long.parseLong(info.get(HITS));
    Long currentMisses = Long.parseLong(info.get(MISSES));

    command.accept(key1, key2);
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 2));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses));
  }

  private void runDiffCommandAndAssertHitsAndMisses(String key,
      BiConsumer<String, String> command) {
    Map<String, String> info = getInfo(jedis);
    Long currentHits = Long.parseLong(info.get(HITS));
    Long currentMisses = Long.parseLong(info.get(MISSES));

    command.accept(key, key);
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 2));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses));

    command.accept(key, "missed");
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(String.valueOf(currentHits + 3));
    assertThat(info.get(MISSES)).isEqualTo(String.valueOf(currentMisses + 1));
  }

  /**
   * When storing diff-ish results, hits and misses are never updated
   */
  private void runDiffStoreCommandAndAssertNoStatUpdates(String key,
      TriConsumer<String, String, String> command) {
    Map<String, String> info = getInfo(jedis);
    String currentHits = info.get(HITS);
    String currentMisses = info.get(MISSES);

    command.accept("destination", key, key);
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);

    command.accept("destination", key, "missed");
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);
  }

  private void runCommandAndAssertNoStatUpdates(String key, Consumer<String> command) {
    Map<String, String> info = getInfo(jedis);
    String currentHits = info.get(HITS);
    String currentMisses = info.get(MISSES);

    command.accept(key);
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);
  }

  private void runCommandAndAssertNoStatUpdates(String key, BiConsumer<String, String> command) {
    Map<String, String> info = getInfo(jedis);
    String currentHits = info.get(HITS);
    String currentMisses = info.get(MISSES);

    command.accept(key, "42");
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);
  }

  private void runCommandAndAssertNoStatUpdates(String key,
      TriConsumer<String, String, String> command) {
    Map<String, String> info = getInfo(jedis);
    String currentHits = info.get(HITS);
    String currentMisses = info.get(MISSES);

    command.accept(key, key, "42");
    info = getInfo(jedis);

    assertThat(info.get(HITS)).isEqualTo(currentHits);
    assertThat(info.get(MISSES)).isEqualTo(currentMisses);
  }

  /**
   * Convert the values returned by the INFO command into a basic param:value map.
   */
  static Map<String, String> getInfo(Jedis jedis) {
    Map<String, String> results = new HashMap<>();
    String rawInfo = jedis.info();

    for (String line : rawInfo.split("\r\n")) {
      int colonIndex = line.indexOf(":");
      if (colonIndex > 0) {
        String key = line.substring(0, colonIndex);
        String value = line.substring(colonIndex + 1);
        results.put(key, value);
      }
    }

    return results;
  }
}
