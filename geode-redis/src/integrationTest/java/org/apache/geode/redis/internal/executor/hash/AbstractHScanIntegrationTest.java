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
package org.apache.geode.redis.internal.executor.hash;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.RedisPortSupplier;

public abstract class AbstractHScanIntegrationTest implements RedisPortSupplier {

  protected Jedis jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @After
  public void tearDown() {
    jedis.close();
  }

  @Test
  public void givenNoKeyArgument_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN))
        .hasMessageContaining("ERR wrong number of arguments for 'hscan' command");
  }

  @Test
  public void givenNoCursorArgument_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "key!"))
        .hasMessageContaining("ERR wrong number of arguments for 'hscan' command");
  }

  @Test
  public void givenArgumentsAreNotOddAndKeyExists_returnsSyntaxError() {
    jedis.hset("a", "b", "1");

    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "a", "0", "a*"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenArgumentsAreNotOddAndKeyDoesNotExist_returnsEmptyArray() {
    List<Object> result =
        (List<Object>) jedis.sendCommand(Protocol.Command.HSCAN, "key!", "0", "a*");

    assertThat((byte[]) result.get(0)).isEqualTo("0".getBytes());
    assertThat((List<Object>) result.get(1)).isEmpty();
  }

  @Test
  public void givenMatchOrCountKeywordNotSpecified_returnsSyntaxError() {
    jedis.hset("a", "b", "1");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "a", "0", "a*", "1"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.hset("a", "b", "1");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "a", "0", "COUNT", "MATCH"))
        .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenCount_whenCountParameterIsZero_returnsSyntaxError() {
    jedis.hset("a", "b", "1");

    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "a", "0", "COUNT", "0"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNegative_returnsSyntaxError() {
    jedis.hset("a", "b", "1");

    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "a", "0", "COUNT", "-37"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.hset("a", "b", "1");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "a", "0", "COUNT", "3",
        "COUNT", "sjlfs", "COUNT", "1"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsLessThanOne_returnsSyntaxError() {
    jedis.hset("a", "b", "1");
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "a", "0", "COUNT", "3",
        "COUNT", "0", "COUNT", "1"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenKeyIsNotAHash_returnsWrongTypeError() {
    jedis.sadd("a", "1");

    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "a", "0", "COUNT", "-37"))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void givenKeyIsNotAHash_andCursorIsNotAnInteger_returnsCursorError() {
    jedis.sadd("a", "b");

    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "a", "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_andCursorIsNotAnInteger_returnsCursorError() {
    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "notReal", "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenExistentHashKey_andCursorIsNotAnInteger_returnsCursorError() {
    jedis.hset("a", "b", "1");

    assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.HSCAN, "a", "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_returnsEmptyArray() {
    ScanResult<Map.Entry<String, String>> result = jedis.hscan("nonexistent", "0");

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenHashWithOneEntry_returnsEntry() {
    jedis.hset("a", "1", "2");
    ScanResult<Map.Entry<String, String>> result = jedis.hscan("a", "0");

    Map<String, String> expected = new HashMap<>();
    expected.put("1", "2");
    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsExactly(expected.entrySet().iterator().next());
  }

  @Test
  public void givenHashWithMultipleEntries_returnsAllEntries() {
    Map<String, String> entryMap = new HashMap<>();
    entryMap.put("1", "yellow");
    entryMap.put("2", "green");
    entryMap.put("3", "orange");
    jedis.hmset("colors", entryMap);

    ScanResult<Map.Entry<String, String>> result = jedis.hscan("colors", "0");

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(new HashSet<>(result.getResult())).isEqualTo(entryMap.entrySet());
  }

  @Test
  public void givenCount_returnsAllEntriesWithoutDuplicates() {
    Map<String, String> entryMap = new HashMap<>();
    entryMap.put("1", "yellow");
    entryMap.put("2", "green");
    entryMap.put("3", "orange");
    jedis.hmset("colors", entryMap);

    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    ScanResult<Map.Entry<String, String>> result;
    List<Map.Entry<String, String>> allEntries = new ArrayList<>();
    String cursor = "0";

    do {
      result = jedis.hscan("colors", cursor, scanParams);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(allEntries).hasSize(3);
    assertThat(new HashSet<>(allEntries)).isEqualTo(entryMap.entrySet());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenMultipleCounts_returnsAllEntriesWithoutDuplicates() {
    Map<String, String> entryMap = new HashMap<>();
    entryMap.put("1", "yellow");
    entryMap.put("12", "green");
    entryMap.put("3", "grey");
    jedis.hmset("colors", entryMap);
    List<Object> result;

    List<byte[]> allEntries = new ArrayList<>();
    String cursor = "0";

    do {
      result = (List<Object>) jedis.sendCommand(Protocol.Command.HSCAN, "colors", cursor, "COUNT",
          "2", "COUNT", "1");
      allEntries.addAll((List<byte[]>) result.get(1));
      cursor = new String((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), "0".getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo("0".getBytes());
    assertThat(allEntries).containsExactlyInAnyOrder("1".getBytes(), "yellow".getBytes(),
        "12".getBytes(), "green".getBytes(), "3".getBytes(), "grey".getBytes());
  }

  @Test
  public void givenMatch_returnsAllMatchingEntriesWithoutDuplicates() {
    Map<String, String> entryMap = new HashMap<>();
    entryMap.put("1", "yellow");
    entryMap.put("12", "green");
    entryMap.put("3", "grey");
    jedis.hmset("colors", entryMap);

    ScanParams scanParams = new ScanParams();
    scanParams.match("1*");

    ScanResult<Map.Entry<String, String>> result = jedis.hscan("colors", "0", scanParams);

    entryMap.remove("3");
    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(new HashSet<>(result.getResult())).isEqualTo(entryMap.entrySet());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenMultipleMatches_returnsEntriesMatchingLastMatchParameter() {
    Map<String, String> entryMap = new HashMap<>();
    entryMap.put("1", "yellow");
    entryMap.put("12", "green");
    entryMap.put("3", "grey");
    jedis.hmset("colors", entryMap);

    List<Object> result =
        (List<Object>) jedis.sendCommand(Protocol.Command.HSCAN, "colors", "0", "MATCH", "3*",
            "MATCH", "1*");

    assertThat((byte[]) result.get(0)).isEqualTo("0".getBytes());
    assertThat((List<Object>) result.get(1)).containsExactlyInAnyOrder("1".getBytes(),
        "yellow".getBytes(), "12".getBytes(), "green".getBytes());
  }

  @Test
  public void givenMatchAndCount_returnsAllMatchingKeysWithoutDuplicates() {
    Map<String, String> entryMap = new HashMap<>();
    entryMap.put("1", "yellow");
    entryMap.put("12", "green");
    entryMap.put("3", "orange");
    jedis.hmset("colors", entryMap);

    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    scanParams.match("1*");
    ScanResult<Map.Entry<String, String>> result;
    List<Map.Entry<String, String>> allEntries = new ArrayList<>();
    String cursor = "0";

    do {
      result = jedis.hscan("colors", cursor, scanParams);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    entryMap.remove("3");
    assertThat(allEntries).hasSize(2);
    assertThat(new HashSet<>(allEntries)).isEqualTo(entryMap.entrySet());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenMultipleCountsAndMatches_returnsEntriesMatchingLastMatchParameter() {
    Map<String, String> entryMap = new HashMap<>();
    entryMap.put("1", "yellow");
    entryMap.put("12", "green");
    entryMap.put("3", "grey");
    jedis.hmset("colors", entryMap);
    List<Object> result;

    List<byte[]> allEntries = new ArrayList<>();
    String cursor = "0";

    do {
      result = (List<Object>) jedis.sendCommand(Protocol.Command.HSCAN, "colors", cursor, "COUNT",
          "37", "MATCH", "3*", "COUNT", "2", "COUNT", "1", "MATCH", "1*");
      allEntries.addAll((List<byte[]>) result.get(1));
      cursor = new String((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), "0".getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo("0".getBytes());
    assertThat(allEntries).containsExactlyInAnyOrder("1".getBytes(), "yellow".getBytes(),
        "12".getBytes(), "green".getBytes());
  }

  @Test
  public void givenNegativeCursor_returnsEntriesUsingAbsoluteValueOfCursor() {
    Map<String, String> entryMap = new HashMap<>();
    entryMap.put("1", "yellow");
    entryMap.put("2", "green");
    entryMap.put("3", "orange");
    jedis.hmset("colors", entryMap);

    String cursor = "-100";
    ScanResult<Map.Entry<String, String>> result;
    List<Map.Entry<String, String>> allEntries = new ArrayList<>();

    do {
      result = jedis.hscan("colors", cursor);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(allEntries).hasSize(3);
    assertThat(new HashSet<>(allEntries)).isEqualTo(entryMap.entrySet());
  }

  @Test
  public void givenCursorGreaterThanUnsignedLongCapacity_returnsCursorError() {
    assertThatThrownBy(() -> jedis.hscan("a", "18446744073709551616"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNegativeCursorGreaterThanUnsignedLongCapacity_returnsCursorError() {
    assertThatThrownBy(() -> jedis.hscan("a", "-18446744073709551616"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenInvalidRegexSyntax_returnsEmptyArray() {
    jedis.hset("a", "1", "green");
    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    scanParams.match("\\p");

    ScanResult<Map.Entry<String, String>> result = jedis.hscan("a", "0", scanParams);

    assertThat(result.getResult()).isEmpty();
  }
}
