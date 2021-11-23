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
package org.apache.geode.redis.internal.commands.executor.set;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractSScanIntegrationTest implements RedisIntegrationTest {
  protected JedisCluster jedis;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @Before
  public void setUp() {
    jedis = new JedisCluster(new HostAndPort("localhost", getPort()), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    flushAll();
    jedis.close();
  }

  @Test
  public void givenNoKeyArgument_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", Protocol.Command.SSCAN))
        .hasMessageContaining("ERR wrong number of arguments for 'sscan' command");
  }

  @Test
  public void givenNoCursorArgument_returnsWrongNumberOfArgumentsError() {
    assertThatThrownBy(() -> jedis.sendCommand("key", Protocol.Command.SSCAN, "key"))
        .hasMessageContaining("ERR wrong number of arguments for 'sscan' command");
  }

  @Test
  public void givenArgumentsAreNotOddAndKeyExists_returnsSyntaxError() {
    jedis.sadd("a", "1");
    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.SSCAN, "a", "0", "a*"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenArgumentsAreNotOddAndKeyDoesNotExist_returnsEmptyArray() {
    List<Object> result =
        (List<Object>) jedis.sendCommand("key!", Protocol.Command.SSCAN, "key!", "0", "a*");

    assertThat((byte[]) result.get(0)).isEqualTo("0".getBytes());
    assertThat((List<Object>) result.get(1)).isEmpty();
  }

  @Test
  public void givenMatchOrCountKeywordNotSpecified_returnsSyntaxError() {
    jedis.sadd("a", "1");
    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.SSCAN, "a", "0", "a*", "1"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.sadd("a", "1");
    assertThatThrownBy(
        () -> jedis.sendCommand("a", Protocol.Command.SSCAN, "a", "0", "COUNT", "MATCH"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.sadd("a", "1");
    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.SSCAN, "a", "0", "COUNT", "2",
        "COUNT", "sjlfs", "COUNT", "1"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsLessThanOne_returnsSyntaxError() {
    jedis.sadd("a", "1");
    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.SSCAN, "a", "0", "COUNT", "2",
        "COUNT", "0", "COUNT", "1"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsZero_returnsSyntaxError() {
    jedis.sadd("a", "1");

    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.SSCAN, "a", "0", "COUNT", "0"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNegative_returnsSyntaxError() {
    jedis.sadd("a", "1");

    assertThatThrownBy(
        () -> jedis.sendCommand("a", Protocol.Command.SSCAN, "a", "0", "COUNT", "-37"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenKeyIsNotASet_returnsWrongTypeError() {
    jedis.hset("a", "b", "1");

    assertThatThrownBy(
        () -> jedis.sendCommand("a", Protocol.Command.SSCAN, "a", "0", "COUNT", "-37"))
            .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void givenKeyIsNotASet_andCursorIsNotAnInteger_returnsInvalidCursorError() {
    jedis.hset("a", "b", "1");

    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.SSCAN, "a", "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_andCursorIsNotInteger_returnsInvalidCursorError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("notReal", Protocol.Command.SSCAN, "notReal", "notReal", "sjfls"))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenExistentSetKey_andCursorIsNotAnInteger_returnsInvalidCursorError() {
    jedis.set("a", "b");

    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.SSCAN, "a", "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_returnsEmptyArray() {
    ScanResult<String> result = jedis.sscan("nonexistent", "0");

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).isEmpty();
  }

  @Test
  public void givenSetWithOneMember_returnsMember() {
    jedis.sadd("a", "1");
    ScanResult<String> result = jedis.sscan("a", "0");

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsExactly("1");
  }

  @Test
  public void givenSetWithMultipleMembers_returnsAllMembers() {
    jedis.sadd("a", "1", "2", "3");
    ScanResult<String> result = jedis.sscan("a", "0");

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsExactlyInAnyOrder("1", "2", "3");
  }

  @Test
  public void givenCount_returnsAllMembersWithoutDuplicates() {
    jedis.sadd("a", "1", "2", "3");

    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    String cursor = "0";
    ScanResult<byte[]> result;
    List<byte[]> allMembersFromScan = new ArrayList<>();

    do {
      result = jedis.sscan("a".getBytes(), cursor.getBytes(), scanParams);
      allMembersFromScan.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(allMembersFromScan).containsExactlyInAnyOrder("1".getBytes(),
        "2".getBytes(),
        "3".getBytes());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenMultipleCounts_returnsAllEntriesWithoutDuplicates() {
    jedis.sadd("a", "1", "12", "3");

    List<Object> result;

    List<byte[]> allEntries = new ArrayList<>();
    String cursor = "0";

    do {
      result =
          (List<Object>) jedis.sendCommand("a", Protocol.Command.SSCAN, "a", cursor, "COUNT", "2",
              "COUNT", "1");
      allEntries.addAll((List<byte[]>) result.get(1));
      cursor = new String((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), "0".getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo("0".getBytes());
    assertThat(allEntries).containsExactlyInAnyOrder("1".getBytes(),
        "12".getBytes(),
        "3".getBytes());
  }

  @Test
  public void givenMatch_returnsAllMatchingMembersWithoutDuplicates() {
    jedis.sadd("a", "1", "12", "3");

    ScanParams scanParams = new ScanParams();
    scanParams.match("1*");

    ScanResult<byte[]> result =
        jedis.sscan("a".getBytes(), "0".getBytes(), scanParams);

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).containsExactlyInAnyOrder("1".getBytes(),
        "12".getBytes());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenMultipleMatches_returnsMembersMatchingLastMatchParameter() {
    jedis.sadd("a", "1", "12", "3");

    List<Object> result = (List<Object>) jedis.sendCommand("a", Protocol.Command.SSCAN, "a", "0",
        "MATCH", "3*", "MATCH", "1*");

    assertThat((byte[]) result.get(0)).isEqualTo("0".getBytes());
    assertThat((List<byte[]>) result.get(1)).containsExactlyInAnyOrder("1".getBytes(),
        "12".getBytes());
  }

  @Test
  public void givenMatchAndCount_returnsAllMembersWithoutDuplicates() {
    jedis.sadd("a", "1", "12", "3");

    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    scanParams.match("1*");
    ScanResult<byte[]> result;
    List<byte[]> allMembersFromScan = new ArrayList<>();
    String cursor = "0";

    do {
      result = jedis.sscan("a".getBytes(), cursor.getBytes(), scanParams);
      allMembersFromScan.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(allMembersFromScan).containsExactlyInAnyOrder("1".getBytes(),
        "12".getBytes());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenMultipleCountsAndMatches_returnsAllEntriesWithoutDuplicates() {
    jedis.sadd("a", "1", "12", "3");

    List<Object> result;
    List<byte[]> allEntries = new ArrayList<>();
    String cursor = "0";

    do {
      result =
          (List<Object>) jedis.sendCommand("a", Protocol.Command.SSCAN, "a", cursor, "COUNT", "37",
              "MATCH", "3*", "COUNT", "2", "COUNT", "1", "MATCH", "1*");
      allEntries.addAll((List<byte[]>) result.get(1));
      cursor = new String((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), "0".getBytes()));

    assertThat((byte[]) result.get(0)).isEqualTo("0".getBytes());
    assertThat(allEntries).containsExactlyInAnyOrder("1".getBytes(),
        "12".getBytes());
  }

  @Test
  public void givenNegativeCursor_returnsMembersUsingAbsoluteValueOfCursor() {
    jedis.sadd("b", "green", "orange", "yellow");

    List<String> allEntries = new ArrayList<>();

    String cursor = "-100";
    ScanResult<String> result;
    do {
      result = jedis.sscan("b", cursor);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(allEntries).containsExactlyInAnyOrder("green", "orange", "yellow");
  }

  @Test
  public void givenCursorGreaterThanUnsignedLongCapacity_returnsCursorError() {
    assertThatThrownBy(() -> jedis.sscan("a", "18446744073709551616"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNegativeCursorGreaterThanUnsignedLongCapacity_returnsCursorError() {
    assertThatThrownBy(() -> jedis.sscan("a", "-18446744073709551616"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenInvalidRegexSyntax_returnsEmptyArray() {
    jedis.sadd("a", "1");
    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    scanParams.match("\\p");

    ScanResult<byte[]> result =
        jedis.sscan("a".getBytes(), "0".getBytes(), scanParams);

    assertThat(result.getResult()).isEmpty();
  }
}
