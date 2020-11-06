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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.logging.log4j.util.TriConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractHitsMissesIntegrationTest implements RedisPortSupplier {

  private static final String HITS = "keyspace_hits";
  private static final String MISSES = "keyspace_misses";

  protected Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  abstract void resetStats();

  @Before
  public void classSetup() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);

    resetStats();

    jedis.set("string", "yarn");
    jedis.sadd("set", "cotton");
    jedis.hset("hash", "green", "eggs");
  }

  @After
  public void teardown() {
    jedis.flushAll();
    jedis.close();
  }

  // ------------ Key related commands -----------

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

  // ------------ String related commands -----------

  @Test
  public void testGet() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.get(k));
  }

  @Test
  public void testGetset() {
    runCommandAndAssertHitsAndMisses("string", (k, v) -> jedis.getSet(k, v));
  }

  @Test
  public void testStrlen() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.strlen(k));
  }

  // ------------ Bit related commands -----------

  @Test
  public void testBitcount() {
    runCommandAndAssertHitsAndMisses("string", k -> jedis.bitcount(k));
  }

  @Test
  public void testBitpos() {
    jedis.bitpos("string", true);
    Map<String, String> info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("1");
    assertThat(info.get(MISSES)).isEqualTo("0");

    jedis.bitpos("missed", true);
    info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("1");
    assertThat(info.get(MISSES)).isEqualTo("1");
  }

  @Test
  public void testBitop() {
    jedis.bitop(BitOP.OR, "dest", "string", "string");
    Map<String, String> info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("2");
    assertThat(info.get(MISSES)).isEqualTo("0");

    jedis.bitop(BitOP.OR, "dest", "string", "missed");
    info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("3");
    assertThat(info.get(MISSES)).isEqualTo("1");
  }

  // ------------ Set related commands -----------
  // FYI - In Redis 5.x SPOP produces inconsistent results depending on whether a count was given
  // or not. In Redis 6.x SPOP does not update any stats.

  @Test
  public void testSmembers() {
    runCommandAndAssertHitsAndMisses("set", k -> jedis.smembers(k));
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
    runDiffStoreCommandAndAssertHitsAndMisses("set", (k, v, s) -> jedis.sdiffstore(k, v, s));
  }

  @Test
  public void testSinter() {
    runDiffCommandAndAssertHitsAndMisses("set", (k, v) -> jedis.sinter(k, v));
  }

  @Test
  public void testSinterstore() {
    runDiffStoreCommandAndAssertHitsAndMisses("set", (k, v, s) -> jedis.sinterstore(k, v, s));
  }

  @Test
  public void testSunion() {
    runDiffCommandAndAssertHitsAndMisses("set", (k, v) -> jedis.sunion(k, v));
  }

  @Test
  public void testSunionstore() {
    runDiffStoreCommandAndAssertHitsAndMisses("set", (k, v, s) -> jedis.sunionstore(k, v, s));
  }

  // ------------ Hash related commands -----------

  @Test
  public void testHget() {
    runCommandAndAssertHitsAndMisses("hash", (k, v) -> jedis.hget(k, v));
  }

  @Test
  public void testHgetall() {
    runCommandAndAssertHitsAndMisses("hash", k -> jedis.hgetAll(k));
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

  private void runCommandAndAssertHitsAndMisses(String key, Consumer<String> command) {
    command.accept(key);
    Map<String, String> info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("1");
    assertThat(info.get(MISSES)).isEqualTo("0");

    command.accept("missed");
    info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("1");
    assertThat(info.get(MISSES)).isEqualTo("1");
  }

  private void runCommandAndAssertHitsAndMisses(String key, BiConsumer<String, String> command) {
    command.accept(key, "42");
    Map<String, String> info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("1");
    assertThat(info.get(MISSES)).isEqualTo("0");

    command.accept("missed", "42");
    info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("1");
    assertThat(info.get(MISSES)).isEqualTo("1");
  }

  private void runDiffCommandAndAssertHitsAndMisses(String key,
      BiConsumer<String, String> command) {
    command.accept(key, key);
    Map<String, String> info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("2");
    assertThat(info.get(MISSES)).isEqualTo("0");

    command.accept(key, "missed");
    info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("3");
    assertThat(info.get(MISSES)).isEqualTo("1");
  }

  /**
   * When storing diff-ish results, hits and misses are never updated
   */
  private void runDiffStoreCommandAndAssertHitsAndMisses(String key,
      TriConsumer<String, String, String> command) {
    command.accept("destination", key, key);
    Map<String, String> info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("0");
    assertThat(info.get(MISSES)).isEqualTo("0");

    command.accept("destination", key, "missed");
    info = getInfo();

    assertThat(info.get(HITS)).isEqualTo("0");
    assertThat(info.get(MISSES)).isEqualTo("0");
  }

  /**
   * Convert the values returned by the INFO command into a basic param:value map.
   */
  private Map<String, String> getInfo() {
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
