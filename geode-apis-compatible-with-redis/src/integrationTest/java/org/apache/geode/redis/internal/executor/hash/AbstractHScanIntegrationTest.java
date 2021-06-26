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

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_CURSOR;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_SYNTAX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_WRONG_TYPE;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.cluster.CRC16;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractHScanIntegrationTest implements RedisIntegrationTest {

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

  /********* Parameter Checks **************/

  @Test
  public void givenLessThanTwoArguments_returnsWrongNumberOfArgumentsError() {
    assertAtLeastNArgs(jedis, Protocol.Command.HSCAN, 2);
  }

  @Test
  public void givenMatchArgumentWithoutPatternOnExistingKey_returnsSyntaxError() {
    jedis.hset("key", "b", "1");

    assertThatThrownBy(() -> jedis.sendCommand("key", Protocol.Command.HSCAN, "key", "0", "Match"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenMatchArgumentWithoutPatternOnNonExistentKey_returnsEmptyArray() {

    List<Object> result =
        (List<Object>) jedis.sendCommand("key1", Protocol.Command.HSCAN, "key1", "0", "Match");

    assertThat((List<String>) result.get(1)).isEmpty();
  }

  @Test
  public void givenCountArgumentWithoutNumberOnExistingKey_returnsSyntaxError() {
    jedis.hset("a", "b", "1");

    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.HSCAN, "a", "0", "Count"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void givenCountArgumentWithoutNumberOnNonExistentKey_returnsEmptyArray() {
    List<Object> result =
        (List<Object>) jedis.sendCommand("b", Protocol.Command.HSCAN, "b", "0", "Count");

    assertThat((List<String>) result.get(1)).isEmpty();
  }

  @Test
  public void givenMatchOrCountKeywordNotSpecified_returnsSyntaxError() {
    jedis.hset("a", "b", "1");
    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.HSCAN, "a", "0", "a*", "1"))
        .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.hset("a", "b", "1");
    assertThatThrownBy(
        () -> jedis.sendCommand("a", Protocol.Command.HSCAN, "a", "0", "COUNT", "MATCH"))
            .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsNotAnInteger_returnsNotIntegerError() {
    jedis.hset("a", "b", "1");
    assertThatThrownBy(
        () -> jedis.sendCommand("a", Protocol.Command.HSCAN, "a", "0", "COUNT", "3",
            "COUNT", "sjlfs", "COUNT", "1"))
                .hasMessageContaining(ERROR_NOT_INTEGER);
  }

  @Test
  public void givenCount_whenCountParameterIsZero_returnsSyntaxError() {
    jedis.hset("a", "b", "1");

    assertThatThrownBy(
        () -> jedis.sendCommand("a", Protocol.Command.HSCAN, "a", "0", "COUNT", "0"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenCount_whenCountParameterIsNegative_returnsSyntaxError() {
    jedis.hset("a", "b", "1");

    assertThatThrownBy(
        () -> jedis.sendCommand("a", Protocol.Command.HSCAN, "a", "0", "COUNT", "-37"))
            .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenMultipleCounts_whenAnyCountParameterIsLessThanOne_returnsSyntaxError() {
    jedis.hset("key", "b", "1");

    assertThatThrownBy(
        () -> jedis.sendCommand("key", Protocol.Command.HSCAN, "key", "0", "COUNT", "3",
            "COUNT", "0", "COUNT", "1"))
                .hasMessageContaining(ERROR_SYNTAX);
  }

  @Test
  public void givenKeyIsNotAHash_returnsWrongTypeError() {
    jedis.sadd("a", "1");

    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.HSCAN, "a", "0"))
        .hasMessageContaining(ERROR_WRONG_TYPE);
  }

  @Test
  public void givenKeyIsNotAHash_andCursorIsNotAnInteger_returnsCursorError() {
    jedis.sadd("a", "b");

    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.HSCAN, "a", "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_andCursorIsNotAnInteger_returnsCursorError() {
    assertThatThrownBy(
        () -> jedis.sendCommand("notReal", Protocol.Command.HSCAN, "notReal", "sjfls"))
            .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenExistentHashKey_andCursorIsNotAnInteger_returnsCursorError() {
    jedis.hset("a", "b", "1");

    assertThatThrownBy(() -> jedis.sendCommand("a", Protocol.Command.HSCAN, "a", "sjfls"))
        .hasMessageContaining(ERROR_CURSOR);
  }

  @Test
  public void givenNonexistentKey_returnsEmptyArray() {
    ScanResult<Map.Entry<String, String>> result = jedis.hscan("nonexistent", "0");

    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(result.getResult()).isEmpty();
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
  public void givenInvalidRegexSyntax_returnsEmptyArray() {
    jedis.hset("a", "1", "green");
    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    scanParams.match("\\p");

    ScanResult<Map.Entry<byte[], byte[]>> result =
        jedis.hscan(stringToBytes("a"), stringToBytes("0"), scanParams);

    assertThat(result.getResult()).isEmpty();
  }

  /*** Happy path behavior checks start here **/

  @Test
  public void givenHashWithOneEntry_returnsEntry() {
    Map<String, String> expected = new HashMap<>();
    expected.put("1", "2");
    jedis.hset("a", "1", "2");

    ScanResult<Map.Entry<String, String>> result = jedis.hscan("a", "0");

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
  @SuppressWarnings("unchecked")
  public void givenMultipleCounts_DoesNotFail() {
    Map<String, String> entryMap = new HashMap<>();
    entryMap.put("1", "yellow");
    entryMap.put("12", "green");
    entryMap.put("3", "grey");
    jedis.hmset("colors", entryMap);
    List<Object> result;

    List<byte[]> allEntries = new ArrayList<>();
    String cursor = "0";

    do {
      result = (List<Object>) jedis.sendCommand("colors", Protocol.Command.HSCAN,
          "colors",
          cursor,
          "COUNT", "2",
          "COUNT", "1");

      allEntries.addAll((List<byte[]>) result.get(1));
      cursor = bytesToString((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), stringToBytes("0")));

    assertThat((byte[]) result.get(0)).isEqualTo(stringToBytes("0"));
  }

  @Test
  public void givenCompleteIteration_shouldReturnCursorWithValueOfZero() {
    Map<String, String> entryMap = new HashMap<>();
    entryMap.put("1", "yellow");
    entryMap.put("2", "green");
    entryMap.put("3", "orange");
    jedis.hmset("colors", entryMap);

    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    ScanResult<Map.Entry<byte[], byte[]>> result;
    List<Map.Entry<byte[], byte[]>> allEntries = new ArrayList<>();
    String cursor = "0";

    do {
      result = jedis.hscan(stringToBytes("colors"), stringToBytes(cursor), scanParams);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    assertThat(result.getCursor()).isEqualTo("0");
  }

  @Test
  public void givenMatch_returnsAllMatchingEntries() {
    Map<byte[], byte[]> entryMap = new HashMap<>();
    byte[] field3 = stringToBytes("3");
    entryMap.put(stringToBytes("1"), stringToBytes("yellow"));
    entryMap.put(stringToBytes("12"), stringToBytes("green"));
    entryMap.put(field3, stringToBytes("grey"));
    jedis.hmset(stringToBytes("colors"), entryMap);

    ScanParams scanParams = new ScanParams();
    scanParams.match("1*");

    ScanResult<Map.Entry<byte[], byte[]>> result =
        jedis.hscan(stringToBytes("colors"), stringToBytes("0"), scanParams);

    entryMap.remove(field3);
    assertThat(result.isCompleteIteration()).isTrue();
    assertThat(new HashSet<>(result.getResult()))
        .usingElementComparator(new MapEntryWithByteArraysComparator())
        .containsExactlyInAnyOrderElementsOf(entryMap.entrySet());
  }

  private static class MapEntryWithByteArraysComparator
      implements Comparator<Map.Entry<byte[], byte[]>> {
    @Override
    public int compare(Map.Entry<byte[], byte[]> o1, Map.Entry<byte[], byte[]> o2) {
      return Arrays.equals(o1.getKey(), o2.getKey()) &&
          Arrays.equals(o1.getValue(), o2.getValue()) ? 0 : 1;
    }
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
        (List<Object>) jedis.sendCommand(stringToBytes("colors"), Protocol.Command.HSCAN,
            stringToBytes("colors"), stringToBytes("0"), stringToBytes("MATCH"),
            stringToBytes("3*"),
            stringToBytes("MATCH"), stringToBytes("1*"));

    assertThat((byte[]) result.get(0)).isEqualTo(stringToBytes("0"));
    assertThat((List<Object>) result.get(1)).containsAll(
        Arrays.asList(stringToBytes("1"), stringToBytes("yellow"),
            stringToBytes("12"), stringToBytes("green")));
  }

  @Test
  public void givenMatchAndCount_returnsAllMatchingKeys() {
    Map<byte[], byte[]> entryMap = new HashMap<>();
    byte[] field3 = stringToBytes("3");
    entryMap.put(stringToBytes("1"), stringToBytes("yellow"));
    entryMap.put(stringToBytes("12"), stringToBytes("green"));
    entryMap.put(field3, stringToBytes("orange"));
    jedis.hmset(stringToBytes("colors"), entryMap);

    ScanParams scanParams = new ScanParams();
    scanParams.count(1);
    scanParams.match("1*");
    ScanResult<Map.Entry<byte[], byte[]>> result;
    List<Map.Entry<byte[], byte[]>> allEntries = new ArrayList<>();
    String cursor = "0";

    do {
      result = jedis.hscan(stringToBytes("colors"), stringToBytes(cursor), scanParams);
      allEntries.addAll(result.getResult());
      cursor = result.getCursor();
    } while (!result.isCompleteIteration());

    entryMap.remove(field3);

    assertThat(new HashSet<>(allEntries))
        .usingElementComparator(new MapEntryWithByteArraysComparator())
        .containsAll(entryMap.entrySet());
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
      result =
          (List<Object>) jedis.sendCommand(stringToBytes("colors"), Protocol.Command.HSCAN,
              stringToBytes("colors"), stringToBytes(cursor),
              stringToBytes("COUNT"), stringToBytes("37"),
              stringToBytes("MATCH"), stringToBytes("3*"), stringToBytes("COUNT"),
              stringToBytes("2"),
              stringToBytes("COUNT"), stringToBytes("1"), stringToBytes("MATCH"),
              stringToBytes("1*"));
      allEntries.addAll((List<byte[]>) result.get(1));
      cursor = bytesToString((byte[]) result.get(0));
    } while (!Arrays.equals((byte[]) result.get(0), stringToBytes("0")));

    assertThat((byte[]) result.get(0)).isEqualTo(stringToBytes("0"));
    assertThat(allEntries).containsExactlyInAnyOrder(stringToBytes("1"),
        stringToBytes("yellow"),
        stringToBytes("12"), stringToBytes("green"));
  }

  @Test
  public void should_notReturnValue_givenValueWasRemovedBeforeHSCANISCalled() {

    Map<String, String> data = new HashMap<>();
    data.put("field_1", "yellow");
    data.put("field_2", "green");
    data.put("field_3", "grey");
    jedis.hmset("colors", data);

    jedis.hdel("colors", "field_3");
    data.remove("field_3");

    GeodeAwaitility.await().untilAsserted(
        () -> assertThat(jedis.hget("colors", "field_3")).isNull());

    ScanResult<Map.Entry<String, String>> result = jedis.hscan("colors", "0");

    assertThat(new HashSet<>(result.getResult()))
        .containsExactlyInAnyOrderElementsOf(data.entrySet());
  }

  @Test
  public void should_notErrorGivenNonzeroCursorOnFirstCall() {

    Map<String, String> data = new HashMap<>();
    data.put("field_1", "yellow");
    data.put("field_2", "green");
    data.put("field_3", "grey");
    jedis.hmset("colors", data);

    ScanResult<Map.Entry<String, String>> result = jedis.hscan("colors", "5");

    assertThat(new HashSet<>(result.getResult()))
        .isSubsetOf(data.entrySet());
  }

  /**** Concurrency ***/

  private final int SIZE_OF_INITIAL_HASH_DATA = 100;
  final String HASH_KEY = "key";
  final String BASE_FIELD = "baseField_";

  @Test
  public void should_notLoseFields_givenConcurrentThreadsDoingHScansAndChangingValues() {
    final Map<String, String> INITIAL_HASH_DATA = makeEntrySet(SIZE_OF_INITIAL_HASH_DATA);
    jedis.hset(HASH_KEY, INITIAL_HASH_DATA);
    final int ITERATION_COUNT = 500;

    int slot = getSlotForKey(HASH_KEY);
    Jedis jedis1 = jedis.getConnectionFromSlot(slot);
    Jedis jedis2 = jedis.getConnectionFromSlot(slot);

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> multipleHScanAndAssertOnSizeOfResultSet(jedis1, INITIAL_HASH_DATA),
        (i) -> multipleHScanAndAssertOnSizeOfResultSet(jedis2, INITIAL_HASH_DATA),
        (i) -> {
          int fieldSuffix = i % SIZE_OF_INITIAL_HASH_DATA;
          jedis.hset(HASH_KEY, BASE_FIELD + fieldSuffix, "new_value_" + i);
        }).run();

    jedis1.close();
    jedis2.close();
  }

  @Test
  public void should_notLoseKeysForConsistentlyPresentFields_givenConcurrentThreadsAddingAndRemovingFields() {
    final Map<String, String> INITIAL_HASH_DATA = makeEntrySet(SIZE_OF_INITIAL_HASH_DATA);
    jedis.hset(HASH_KEY, INITIAL_HASH_DATA);
    final int ITERATION_COUNT = 500;

    int slot = getSlotForKey(HASH_KEY);
    Jedis jedis1 = jedis.getConnectionFromSlot(slot);
    Jedis jedis2 = jedis.getConnectionFromSlot(slot);

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> multipleHScanAndAssertOnContentOfResultSet(i, jedis1, INITIAL_HASH_DATA),
        (i) -> multipleHScanAndAssertOnContentOfResultSet(i, jedis2, INITIAL_HASH_DATA),
        (i) -> {
          String field = "new_" + BASE_FIELD + i;
          jedis.hset(HASH_KEY, field, "whatever");
          jedis.hdel(HASH_KEY, field);
        }).run();

    jedis1.close();
    jedis2.close();
  }

  @Test
  public void should_notAlterUnderlyingData_givenMultipleConcurrentHscans() {
    final Map<String, String> INITIAL_HASH_DATA = makeEntrySet(SIZE_OF_INITIAL_HASH_DATA);
    jedis.hset(HASH_KEY, INITIAL_HASH_DATA);
    final int ITERATION_COUNT = 500;

    int slot = getSlotForKey(HASH_KEY);
    Jedis jedis1 = jedis.getConnectionFromSlot(slot);
    Jedis jedis2 = jedis.getConnectionFromSlot(slot);

    new ConcurrentLoopingThreads(ITERATION_COUNT,
        (i) -> multipleHScanAndAssertOnContentOfResultSet(i, jedis1, INITIAL_HASH_DATA),
        (i) -> multipleHScanAndAssertOnContentOfResultSet(i, jedis2, INITIAL_HASH_DATA))
            .run();

    INITIAL_HASH_DATA
        .forEach((field, value) -> assertThat(jedis.hget(HASH_KEY, field).equals(value)));

    jedis1.close();
    jedis2.close();
  }

  private void multipleHScanAndAssertOnContentOfResultSet(int iteration, Jedis jedis,
      final Map<String, String> initialHashData) {

    List<String> allEntries = new ArrayList<>();
    ScanResult<Map.Entry<String, String>> result;
    String cursor = "0";

    do {
      result = jedis.hscan(HASH_KEY, cursor);
      cursor = result.getCursor();
      List<Map.Entry<String, String>> resultEntries = result.getResult();
      resultEntries
          .forEach((entry) -> allEntries.add(entry.getKey()));
    } while (!result.isCompleteIteration());

    assertThat(allEntries).as("failed on iteration " + iteration)
        .containsAll(initialHashData.keySet());
  }

  private void multipleHScanAndAssertOnSizeOfResultSet(Jedis jedis,
      final Map<String, String> initialHashData) {
    List<Map.Entry<String, String>> allEntries = new ArrayList<>();
    ScanResult<Map.Entry<String, String>> result;
    String cursor = "0";

    do {
      result = jedis.hscan(HASH_KEY, cursor);
      cursor = result.getCursor();
      allEntries.addAll(result.getResult());
    } while (!result.isCompleteIteration());

    List<Map.Entry<String, String>> allDistinctEntries =
        allEntries
            .stream()
            .distinct()
            .collect(Collectors.toList());

    assertThat(allDistinctEntries.size())
        .isEqualTo(initialHashData.size());
  }

  private Map<String, String> makeEntrySet(int sizeOfDataSet) {
    Map<String, String> dataSet = new HashMap<>();
    for (int i = 0; i < sizeOfDataSet; i++) {
      dataSet.put(BASE_FIELD + i, "value_" + i);
    }
    return dataSet;
  }

  private int getSlotForKey(String key) {
    int crc = CRC16.calculate(key);
    return crc % RegionProvider.REDIS_SLOTS;
  }
}
